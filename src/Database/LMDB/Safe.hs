{-# LANGUAGE DataKinds                  #-}
{-# LANGUAGE TypeApplications                  #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase                 #-}
{-# LANGUAGE RankNTypes                 #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE TypeFamilies               #-}
{-# LANGUAGE TypeInType                 #-}
{-# LANGUAGE TypeOperators              #-}
{-# LANGUAGE UndecidableInstances       #-}

module Database.LMDB.Safe
  ( Transaction, RunTransaction, liftReadOnlyTransaction
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
  , unsafeTransaction
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
import           Control.Concurrent     (runInBoundThread)
import           Control.Exception      (SomeException, bracket, throwIO, try)
import           Control.Monad.IO.Class (MonadIO, liftIO)
import           Control.Monad.Reader   (ReaderT, ask, lift, runReaderT)
import           Data.ByteString        (ByteString)
import qualified Data.ByteString        as BS
import Data.Maybe (fromMaybe)
import Data.Typeable
import Data.SafeCopy
import qualified Data.ByteString.Unsafe as BS
import           Data.Functor           (void, ($>))
import           Data.Kind
import           Data.Proxy             (Proxy (Proxy))
import           Data.Type.Bool
import           Database.LMDB.Raw
import           Foreign.C.Types        (CChar)
import           Foreign.Marshal.Alloc  (alloca)
import           Foreign.Marshal.Utils  (with)
import           Foreign.Ptr            (Ptr, castPtr, nullPtr)
import           Foreign.Storable       (Storable, peek, sizeOf)
import Data.Serialize hiding (get, put)
import           GHC.TypeLits (symbolVal, KnownSymbol, TypeError, ErrorMessage (..), Symbol)
--------------------------------------------------------------------------------
import Type.And
import Database.LMDB.Safe.Instances
--------------------------------------------------------------------------------

newtype Transaction rw a = Transaction { unTransaction :: ReaderT (MDB_env, MDB_txn) IO a }
  deriving (Functor, Applicative, Monad, MonadIO)
data ReadOnly  = ReadOnly
data ReadWrite = ReadWrite

class TransactionType a where isReadOnly :: a -> Bool
instance TransactionType ReadOnly  where isReadOnly = const True
instance TransactionType ReadWrite where isReadOnly = const False

unsafeTransaction :: (MDB_env -> MDB_txn -> IO a) -> Transaction rw a
unsafeTransaction f = Transaction $ ask >>= lift . uncurry f

withTransaction :: TransactionType rw => MDB_env -> rw -> Transaction rw a -> IO a
withTransaction env trtype (Transaction tr) = runInBoundThread $ do
  tx <- mdb_txn_begin env Nothing (isReadOnly trtype)
  r <- try $ runReaderT tr (env, tx)
  case r of
    Left  l -> mdb_txn_abort tx  *> throwIO (l :: SomeException)
    Right r -> mdb_txn_commit tx $> r

liftReadOnlyTransaction :: Transaction ReadOnly a -> Transaction ReadWrite a
liftReadOnlyTransaction (Transaction a) = Transaction a

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
    = unsafeTransaction $ \_ tx -> do
        DB <$> mdb_dbi_open' tx n flags
    where n = dbNameToString (Proxy :: Proxy name)
          flags = toDBFlags (Proxy :: Proxy flags)

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
  = Config { path    :: FilePath
           , mapsize :: Int
           , maxdbs  :: Int
           , flags   :: [MDB_EnvFlag]
           }

type RunTransaction = forall a. forall rw. TransactionType rw
                   => rw -> Transaction rw a -> IO a

withLMDB :: DBSchema def
         => Config
         -> Proxy def
         -> (RunTransaction -> DBSchemaHandles def -> IO a)
         -> IO a
withLMDB cfg defs act = bracket acq des go
  where
    acq = runInBoundThread $ do
      env <- mdb_env_create
      mdb_env_set_mapsize env (mapsize cfg)
      mdb_env_set_maxdbs env (maxdbs cfg)
      mdb_env_open env (path cfg) (flags cfg)
      return env
    des = runInBoundThread . mdb_env_close
    go env = withTransaction env ReadWrite (openSchema defs)
          >>= act (withTransaction env)

--------------------------------------------------------------------------------

get :: (ToLMDB k, FromLMDB v) => DB k v -> k -> Transaction readOnly (Maybe v)
get = unsafeGet

unsafeGet :: (ToLMDB k, FromLMDB v) => DB a b -> k -> Transaction readOnly (Maybe v)
unsafeGet (DB db) k = unsafeTransaction $ \_ txn ->
  withMDBVal k $ \kval ->
    mdb_get' txn db kval >>= \case
      Nothing -> return Nothing
      Just v  -> do
        val <- mdbValFromLMDB v
        val `seq` (return $ Just val)

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
unsafePutWith flags (DB db) k v = unsafeTransaction $ \_ txn ->
  withMDBVal k $ \km ->
    withMDBVal v $ \vm ->
      mdb_put' flags txn db km vm

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

withCursor :: DB k v -> (Cursor k v -> Transaction ro a) -> Transaction rw a
withCursor db act = unsafeTransaction $ \env txn -> do
  let dbi = unsafeExtractDbi db
      acq = mdb_cursor_open' txn dbi
      des = mdb_cursor_close'
  bracket acq des $ \cur -> runReaderT (unTransaction . act $ Cursor cur) (env, txn)

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
unsafeCursorGet (Cursor cur) = unsafeTransaction $ \_ _ ->
  alloca $ \kptr -> alloca $ \vptr -> do
    mdb_cursor_get' MDB_GET_CURRENT cur kptr vptr
    (,) <$> (peek kptr >>= mdbValFromLMDB)
        <*> (peek vptr >>= mdbValFromLMDB)

unsafeCursorGetKey :: FromLMDB k => Cursor a b -> Transaction ro k
unsafeCursorGetKey (Cursor cur) = unsafeTransaction $ \_ _ ->
  alloca $ \kptr -> alloca $ \vptr -> do
    mdb_cursor_get' MDB_GET_CURRENT cur kptr vptr
    peek kptr >>= mdbValFromLMDB

positionCursor :: MDB_cursor_op -> Cursor k v -> Transaction ro Bool
positionCursor op (Cursor cur)
  = unsafeTransaction $ \_ _ -> mdb_cursor_get' op cur nullPtr nullPtr

--------------------------------------------------------------------------------

mdbValFromLMDB :: FromLMDB s => MDB_val -> IO s
mdbValFromLMDB v@(MDB_val s d)
  = fromLMDB (castPtr d, fromIntegral s) >>= \case
      Nothing -> showMDBVal v >>= \i -> error ("Failed decoding: " ++ i)
      Just xs -> return xs

withMDBVal :: ToLMDB s => s -> (MDB_val -> IO a) -> IO a
withMDBVal s act
  = toLMDB s $ \(ptr, len) ->
      act (MDB_val (fromIntegral len) (castPtr ptr))

liftTxnDbi :: (MDB_txn -> MDB_dbi' -> IO a) -> DB k v -> Transaction rw a
liftTxnDbi f (DB db) = unsafeTransaction $ \_ txn -> f txn db

showMDBVal :: MDB_val -> IO String
showMDBVal (MDB_val s d)
  = (return . show :: Maybe BS.ByteString -> IO String) =<< fromLMDB (castPtr d, fromIntegral s)

--------------------------------------------------------------------------------

type family DBNames a where
  DBNames (DBDef name _ _ _) = Proxy name
  DBNames (a :& b)           = DBNames a :& DBNames b

--------------------------------------------------------------------------------
