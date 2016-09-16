{-# LANGUAGE DataKinds                  #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase                 #-}
{-# LANGUAGE RankNTypes                 #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE TypeApplications           #-}
{-# LANGUAGE TypeFamilies               #-}
{-# LANGUAGE TypeInType                 #-}
{-# LANGUAGE TypeOperators              #-}
{-# LANGUAGE UndecidableInstances       #-}
{-# OPTIONS_GHC -fno-warn-redundant-constraints       #-}

module Database.LMDB.Safe
  ( Transaction, RunTransaction, liftReadOnlyTransaction
  , TransactionError (..)
  , ReadOnly (ReadOnly), ReadWrite (ReadWrite)
  , DB, DBDef, (:&)((:&)), DefaultDatabase
  , NoFlag, ReverseKey, DupSort, IntegerKey
  , DupFixed, IntegerDup, ReverseDup, Create
  , withLMDB, Config (..)
  , module Database.LMDB.Safe.Instances
  , get, put, putWith, modify
  , dropDB, clearDB
  -- * Cursors
  , withCursor
  , cursorFirst, cursorLast, cursorNext, cursorPrev
  , cursorGet, cursorGetKey
  -- * Unsafe
  , unsafeGetEnv
  , unsafeExtractDbi
  , unsafeGet, unsafePut, unsafePutWith
  , unsafeCursorGet, unsafeCursorGetKey
  -- * Internals
  , DBNames, DBSchema, DBSchemaHandles
  -- * Re-exports
  , module Database.LMDB.Raw
  , liftIO, Proxy (Proxy)
  ) where

--------------------------------------------------------------------------------
import           Control.Concurrent           (runInBoundThread, threadDelay)
import           Control.Exception
import           Control.Monad.Except
import           Control.Monad.IO.Class       (MonadIO, liftIO)
import           Control.Monad.Reader         (ReaderT, ask, lift, runReaderT)
import qualified Data.ByteString              as BS
import           Data.Functor                 (($>))
import           Data.Proxy                   (Proxy (Proxy))
import           Database.LMDB.Raw
import           Foreign.Marshal.Alloc        (alloca)
import           Foreign.Ptr                  (castPtr, nullPtr)
import           Foreign.Storable             (peek)
import           GHC.TypeLits                 (KnownSymbol, symbolVal)
--------------------------------------------------------------------------------
import           Database.LMDB.Safe.Instances
import           Type.And
--------------------------------------------------------------------------------

newtype Transaction rw a = Transaction (ExceptT TransactionError (ReaderT (MDB_env, MDB_txn) IO) a)
  deriving (Functor, Applicative, Monad, MonadError TransactionError)
data ReadOnly  = ReadOnly
data ReadWrite = ReadWrite

instance MonadIO (Transaction rw) where
  liftIO act = (Transaction . lift . lift . try . try $ act) >>= \case
    Right (Right r) -> return r
    Right (Left r)  -> throwError (LMDBError r)
    Left r          -> liftIO $ throwIO (r :: IOException)

class    TransactionType a         where isReadOnly :: a -> Bool
instance TransactionType ReadOnly  where isReadOnly = const True
instance TransactionType ReadWrite where isReadOnly = const False

unsafeGetEnv :: Transaction rw (MDB_env, MDB_txn)
unsafeGetEnv = Transaction ask

withTransaction :: TransactionType rw => MDB_env -> rw -> Transaction rw a -> IO (Either TransactionError a)
withTransaction env trtype (Transaction tr) = runInBoundThread $ do
  tx <- mdb_txn_begin env Nothing (isReadOnly trtype)
  try (runReaderT (runExceptT tr) (env, tx)) >>= \case
    Right (Right r) -> mdb_txn_commit tx $> Right r
    Right (Left er) -> mdb_txn_abort  tx $> Left er
    Left l          -> mdb_txn_abort  tx *> throwIO (l :: IOException)

liftReadOnlyTransaction :: Transaction ReadOnly a -> Transaction rw a
liftReadOnlyTransaction (Transaction a) = Transaction a

data TransactionError
  = ParseError BS.ByteString
  | LMDBError  LMDB_Error
  deriving (Show, Eq)

-- instance Exception LMDBError

-- catchTransaction :: Transaction rw a -> (LMDBError -> a) -> Transaction rw a
-- catchTransaction (Transaction tr) f = Transaction $ do
--   env <- ask
--   lift $ runReaderT tr env `catch` (return . f)

--------------------------------------------------------------------------------

newtype DB key val = DB { unsafeExtractDbi :: MDB_dbi' }

data DBDef name flags key val

data DefaultDatabase

class IsDBName a where dbNameToString :: Proxy a -> Maybe String
instance KnownSymbol sym => IsDBName sym where dbNameToString = Just . symbolVal
instance IsDBName DefaultDatabase        where dbNameToString = const Nothing

class IsDBFlags a where
  toDBFlags :: Proxy a -> [MDB_DbFlag]

data NoFlag
data ReverseKey
data DupSort
data IntegerKey
data DupFixed
data IntegerDup
data ReverseDup
data Create

instance IsDBFlags NoFlag     where toDBFlags _ = []
instance IsDBFlags ReverseKey where toDBFlags _ = [MDB_REVERSEKEY]
instance IsDBFlags DupSort    where toDBFlags _ = [MDB_DUPSORT]
instance IsDBFlags IntegerKey where toDBFlags _ = [MDB_INTEGERKEY]
instance IsDBFlags DupFixed   where toDBFlags _ = [MDB_DUPFIXED]
instance IsDBFlags IntegerDup where toDBFlags _ = [MDB_INTEGERDUP]
instance IsDBFlags ReverseDup where toDBFlags _ = [MDB_REVERSEDUP]
instance IsDBFlags Create     where toDBFlags _ = [MDB_CREATE]
instance (IsDBFlags a , IsDBFlags b) => IsDBFlags (a :& b) where
  toDBFlags (Proxy :: Proxy (a :& b))
    = toDBFlags (Proxy :: Proxy a) ++ toDBFlags (Proxy :: Proxy b)

class DBSchema ref where
  type DBSchemaHandles ref
  openSchema :: Proxy ref -> Transaction ReadWrite (DBSchemaHandles ref)

instance (IsDBFlags flags, IsDBName name) => DBSchema (DBDef name flags key val) where
  type DBSchemaHandles (DBDef name flags key val) = DB key val
  openSchema (Proxy :: Proxy (DBDef name flags key val))
    = Transaction $ do
        (_, tx) <- ask
        liftIO $ DB <$> mdb_dbi_open' tx n f
    where n = dbNameToString (Proxy :: Proxy name)
          f = toDBFlags (Proxy :: Proxy flags)

instance
  ( IsUnique (DBNames (head :& rest))
  , DBSchema head, DBSchema rest
  ) => DBSchema (head :& rest) where
  type DBSchemaHandles (head :& rest)
    = DBSchemaHandles head :& DBSchemaHandles rest
  openSchema (Proxy :: Proxy (head :& rest))
    = (:&) <$> openSchema (Proxy :: Proxy head)
           <*> openSchema (Proxy :: Proxy rest)

--------------------------------------------------------------------------------

data Config
  = Config { lmdbPath    :: FilePath
           , lmdbMapSize :: Int
           , lmdbMaxDBs  :: Int
           , lmdbFlags   :: [MDB_EnvFlag]
           }

type RunTransaction = forall a. forall m. forall rw. (TransactionType rw, MonadIO m)
                   => rw -> Transaction rw a -> m (Either TransactionError a)

withLMDB :: DBSchema def
         => Config
         -> Proxy def
         -> (RunTransaction -> DBSchemaHandles def -> IO a)
         -> IO a
withLMDB cfg defs act = bracket acq des go
  where
    acq = runInBoundThread $ do
      env <- mdb_env_create
      mdb_env_set_mapsize env (lmdbMapSize cfg)
      mdb_env_set_maxdbs env (lmdbMaxDBs cfg)
      mdb_env_open env (lmdbPath cfg) (lmdbFlags cfg)
      return env
    des = runInBoundThread . mdb_env_close
    go env = withTransaction env ReadWrite (openSchema defs) >>= \case
      Left  err -> error $ "Could not open databases: " ++ show err
      Right dbs -> act (\a b -> liftIO $ withTransaction env a b) dbs

--------------------------------------------------------------------------------

get :: (ToLMDB k, FromLMDB v) => DB k v -> k -> Transaction readOnly (Maybe v)
get = unsafeGet

unsafeGet :: (ToLMDB k, FromLMDB v) => DB a b -> k -> Transaction readOnly (Maybe v)
unsafeGet (DB db) k = Transaction $ do
  (_, txn) <- ask
  ExceptT . lift . withMDBVal k $ \kval ->
    mdb_get' txn db kval >>= \case
      Nothing -> return (Right Nothing)
      Just v  ->
        mdbValFromLMDB v >>= \case
          Right val -> val `seq` (return . Right $ Just val)
          Left err  -> return . Left $ ParseError err

put :: (ToLMDB k, ToLMDB v) => DB k v -> k -> v -> Transaction ReadWrite Bool
put = unsafePut

putWith :: (ToLMDB k, ToLMDB v)
        => MDB_WriteFlags
        -> DB k v
        -> k
        -> v
        -> Transaction ReadWrite Bool
putWith = unsafePutWith

unsafePut :: (ToLMDB k, ToLMDB v) => DB a b -> k -> v -> Transaction ReadWrite Bool
unsafePut = unsafePutWith defaultWriteFlags

unsafePutWith :: (ToLMDB k, ToLMDB v)
              => MDB_WriteFlags
              -> DB a b
              -> k
              -> v
              -> Transaction ReadWrite Bool
unsafePutWith fs (DB db) k v = Transaction $ do
  (_, txn) <- ask
  liftIO $ withMDBVal k $ \km ->
    withMDBVal v $ \vm ->
      mdb_put' fs txn db km vm

defaultWriteFlags :: MDB_WriteFlags
defaultWriteFlags = compileWriteFlags []

modify :: (ToLMDB k, FromLMDB v, ToLMDB v)
       => DB k v
       -> k
       -> (Maybe v -> v)
       -> Transaction ReadWrite Bool
modify db k f = get db k >>= put db k . f

dropDB :: DB k v -> Transaction ReadWrite ()
dropDB = liftTxnDbi mdb_drop'

clearDB :: DB k v -> Transaction ReadWrite ()
clearDB = liftTxnDbi mdb_clear'

--------------------------------------------------------------------------------

newtype Cursor k v = Cursor MDB_cursor'

withCursor :: DB k v -> (Cursor k v -> Transaction ReadOnly a) -> Transaction rw a
withCursor db act = do
  (_, txn) <- unsafeGetEnv
  let dbi = unsafeExtractDbi db
  curr <- liftIO $ mdb_cursor_open' txn dbi
  r <- liftReadOnlyTransaction . act $ Cursor curr
  liftIO $ mdb_cursor_close' curr
  return r

cursorFirst, cursorLast, cursorNext, cursorPrev :: Cursor k v -> Transaction ro Bool
cursorFirst = positionCursor MDB_FIRST
cursorLast  = positionCursor MDB_LAST
cursorNext  = positionCursor MDB_NEXT
cursorPrev  = positionCursor MDB_PREV

cursorGet :: (FromLMDB k, FromLMDB v) => Cursor k v -> Transaction ro (k, v)
cursorGet = unsafeCursorGet

cursorGetKey :: FromLMDB k => Cursor k v -> Transaction ro k
cursorGetKey = unsafeCursorGetKey

unsafeCursorGet :: (FromLMDB k, FromLMDB v) => Cursor a b -> Transaction ro (k, v)
unsafeCursorGet (Cursor cur) = do
  r <- liftIO $ do
    alloca $ \kptr -> alloca $ \vptr -> do
      _ <- mdb_cursor_get' MDB_GET_CURRENT cur kptr vptr
      (,) <$> (peek kptr >>= mdbValFromLMDB)
          <*> (peek vptr >>= mdbValFromLMDB)
  case r of
    (Right a1, Right a2) -> return (a1, a2)
    ret                  -> undefined


unsafeCursorGetKey :: FromLMDB k => Cursor a b -> Transaction ro k
unsafeCursorGetKey (Cursor cur) = do
  r <- liftIO $ alloca $ \kptr -> alloca $ \vptr -> do
    _ <- mdb_cursor_get' MDB_GET_CURRENT cur kptr vptr
    peek kptr >>= mdbValFromLMDB
  case r of
    Right xs -> return xs
    Left bs  -> throwError $ ParseError bs

positionCursor :: MDB_cursor_op -> Cursor k v -> Transaction ro Bool
positionCursor op (Cursor cur) = liftIO $ mdb_cursor_get' op cur nullPtr nullPtr

--------------------------------------------------------------------------------

mdbValFromLMDB :: FromLMDB s => MDB_val -> IO (Either BS.ByteString s)
mdbValFromLMDB (MDB_val s d)
  = fromLMDB (castPtr d, fromIntegral s) >>= \case
      Nothing -> Left <$> BS.packCStringLen (castPtr d, fromIntegral s)
      Just xs -> xs `seq` return (Right xs)

withMDBVal :: ToLMDB s => s -> (MDB_val -> IO a) -> IO a
withMDBVal s act
  = toLMDB s $ \(ptr, len) ->
      act $ MDB_val (fromIntegral len) (castPtr ptr)

liftTxnDbi :: (MDB_txn -> MDB_dbi' -> IO a) -> DB k v -> Transaction rw a
liftTxnDbi f (DB db) = do
  (_, txn) <- unsafeGetEnv
  liftIO $ f txn db

--------------------------------------------------------------------------------

type family DBNames a where
  DBNames (DBDef name _ _ _) = Proxy name
  DBNames (a :& b)           = DBNames a :& DBNames b

--------------------------------------------------------------------------------
