{-# LANGUAGE DataKinds                  #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase                 #-}
{-# LANGUAGE RankNTypes                 #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE TypeFamilies               #-}
{-# LANGUAGE TypeOperators              #-}

module Database.LMDB.Safe
  ( Transaction, rawTransaction
  , ReadOnly (ReadOnly), ReadWrite (ReadWrite)
  , DbDef, (:&)((:&))
  , openLMDB
  , ToLMDB(toLMDB), FromLMDB(fromLMDB)
  , storableToLMDB, storableFromLMDB
  , get, unsafeGet, put, putWith, unsafePut, unsafePutWith
  , dropDB, clearDB
  , Config (..)
  -- * Re-exports
  , module Database.LMDB.Raw
  , Proxy (Proxy)
  ) where

--------------------------------------------------------------------------------
import           Control.Concurrent     (runInBoundThread)
import           Control.Exception      (SomeException, bracket, throwIO, try)
import           Control.Monad.Reader   (ReaderT, ask, lift, runReaderT)
import           Data.ByteString        (ByteString)
import qualified Data.ByteString        as BS
import qualified Data.ByteString.Unsafe as BS
import           Data.Functor           (($>))
import           Data.Proxy             (Proxy (Proxy))
import           Database.LMDB.Raw
import           Foreign.C.Types        (CChar)
import           Foreign.Marshal.Utils  (with)
import           Foreign.Ptr            (Ptr, castPtr)
import           Foreign.Storable       (Storable, peek, sizeOf)
import           GHC.TypeLits           (KnownSymbol, Symbol, symbolVal)
--------------------------------------------------------------------------------

newtype Transaction rw a = Transaction (ReaderT MDB_txn IO a)
  deriving (Functor, Applicative, Monad)
data ReadOnly  = ReadOnly
data ReadWrite = ReadWrite

class TransactionType a where isReadOnly :: a -> Bool
instance TransactionType ReadOnly  where isReadOnly = const True
instance TransactionType ReadWrite where isReadOnly = const False

rawTransaction :: (MDB_txn -> IO a) -> Transaction rw a
rawTransaction f = Transaction $ ask >>= lift . f

withTransaction :: TransactionType rw => MDB_env -> rw -> Transaction rw a -> IO a
withTransaction env trtype (Transaction tr) = runInBoundThread $ do
  tx <- mdb_txn_begin env Nothing (isReadOnly trtype)
  r <- try $ runReaderT tr tx
  case r of
    Left  l -> mdb_txn_abort tx  >> throwIO (l :: SomeException)
    Right r -> mdb_txn_commit tx $> r

--------------------------------------------------------------------------------

newtype DB key val = DB MDB_dbi'

data DbDef (sym :: Symbol) key val

data a :& b = a :& b

class LMDBDef ref where
  type Res ref
  open :: Proxy ref -> Transaction ReadWrite (Res ref)

instance (ToLMDB key, KnownSymbol sym, FromLMDB val)
       => LMDBDef (DbDef sym key val) where
  type Res (DbDef sym key val) = DB key val
  open (Proxy :: Proxy (DbDef sym key val))
    = rawTransaction $ \tx -> DB <$> mdb_dbi_open' tx name [MDB_CREATE]
    where name = Just $ symbolVal (Proxy :: Proxy sym)

instance (LMDBDef head, LMDBDef rest) => LMDBDef (head :& rest) where
  type Res (head :& rest) = Res head :& Res rest
  open (Proxy :: Proxy (head :& rest))
    = (:&) <$> open (Proxy :: Proxy head) <*> open (Proxy :: Proxy rest)

--------------------------------------------------------------------------------

data Config
  = Config { path    :: FilePath
           , mapsize :: Int
           , maxdbs  :: Int
           }

type RunTransaction = forall a. forall rw. TransactionType rw
                   => rw -> Transaction rw a -> IO a

openLMDB :: LMDBDef def
         => Config
         -> Proxy def
         -> (RunTransaction -> Res def -> IO a)
         -> IO a
openLMDB cfg defs act = bracket acq des go
  where
    acq = runInBoundThread $ do
      env <- mdb_env_create
      mdb_env_set_mapsize env (mapsize cfg)
      mdb_env_set_maxdbs env (maxdbs cfg)
      mdb_env_open env (path cfg) []
      return env
    des = runInBoundThread . mdb_env_close
    go env = withTransaction env ReadWrite (open defs)
          >>= act (withTransaction env)

--------------------------------------------------------------------------------

class ToLMDB   a where toLMDB   :: a -> ((Ptr CChar, Int) -> IO b) -> IO b
class FromLMDB a where fromLMDB :: (Ptr CChar, Int) -> IO a

instance ToLMDB   ByteString where toLMDB   = BS.unsafeUseAsCStringLen
instance FromLMDB ByteString where fromLMDB = BS.packCStringLen

storableToLMDB :: Storable a => a -> ((Ptr CChar, Int) -> IO b) -> IO b
storableToLMDB s act = with s $ \ptr -> act (castPtr ptr, sizeOf s)

storableFromLMDB :: Storable a => (Ptr CChar, Int) -> IO a
storableFromLMDB = peek . castPtr . fst

get :: (ToLMDB k, FromLMDB v) => DB k v -> k -> Transaction readOnly (Maybe v)
get = unsafeGet

unsafeGet :: (ToLMDB k, FromLMDB v) => DB a b -> k -> Transaction readOnly (Maybe v)
unsafeGet (DB db) k = rawTransaction $ \txn ->
  withMDBVal k $ \kval ->
    mdb_get' txn db kval >>= \case
      Nothing            -> return Nothing
      Just (MDB_val s d) -> do
        val <- fromLMDB (castPtr d, fromIntegral s)
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
unsafePutWith flags (DB db) k v = rawTransaction $ \txn ->
  withMDBVal k $ \km ->
    withMDBVal v $ \vm ->
      mdb_put' flags txn db km vm

defaultWriteFlags :: MDB_WriteFlags
defaultWriteFlags = compileWriteFlags []

dropDB :: DB k v -> Transaction ReadWrite ()
dropDB = liftTxnDbi mdb_drop'

clearDB :: DB k v -> Transaction ReadWrite ()
clearDB = liftTxnDbi mdb_clear'

--------------------------------------------------------------------------------

withMDBVal :: ToLMDB s => s -> (MDB_val -> IO a) -> IO a
withMDBVal s act
  = toLMDB s $ \(ptr, len) ->
      act (MDB_val (fromIntegral len) (castPtr ptr))

liftTxnDbi :: (MDB_txn -> MDB_dbi' -> IO a) -> DB k v -> Transaction rw a
liftTxnDbi f (DB db) = rawTransaction $ \txn -> f txn db

--------------------------------------------------------------------------------

