{-# LANGUAGE FlexibleInstances    #-}
{-# LANGUAGE TypeApplications     #-}
{-# LANGUAGE TypeSynonymInstances #-}

module Database.LMDB.Safe.Instances where

--------------------------------------------------------------------------------
import qualified Data.ByteString        as BS
import qualified Data.ByteString.Lazy   as BL
import qualified Data.ByteString.Unsafe as BS
import           Data.Copointed
import           Data.Int
import           Data.SafeCopy
import           Data.Serialize
import           Data.Word
import           Foreign.C.Types
import           Foreign.Marshal.Utils
import           Foreign.Ptr
import           Foreign.Storable
--------------------------------------------------------------------------------

class ToLMDB   a where toLMDB   :: a -> ((Ptr CChar, Int) -> IO b) -> IO b
class FromLMDB a where fromLMDB :: (Ptr CChar, Int) -> IO (Maybe a)

instance ToLMDB   BS.ByteString where toLMDB   = BS.unsafeUseAsCStringLen
instance FromLMDB BS.ByteString where fromLMDB = fmap Just . BS.packCStringLen

instance ToLMDB   BL.ByteString where toLMDB   = toLMDB . BL.toStrict
instance FromLMDB BL.ByteString where fromLMDB = fmap (fmap BL.fromStrict) . fromLMDB

newtype SafeCopyLMDB a = SafeCopyLMDB a
instance Copointed SafeCopyLMDB where copoint (SafeCopyLMDB a) = a

instance SafeCopy a => ToLMDB (SafeCopyLMDB a) where
  toLMDB = safeCopyToLMDB . copoint

instance SafeCopy a => FromLMDB (SafeCopyLMDB a) where
  fromLMDB = fmap (fmap SafeCopyLMDB) . safeCopyFromLMDB

-- not using default storable instance here, LMDB doesn't accept empty keys
instance ToLMDB   () where toLMDB   () = toLMDB True
instance FromLMDB () where fromLMDB    = fmap (fmap $ const ()) . fromLMDB @Bool

storableToLMDB :: Storable a => a -> ((Ptr CChar, Int) -> IO b) -> IO b
storableToLMDB s act = with s $ \ptr -> act (castPtr ptr, sizeOf s)

storableFromLMDB :: Storable a => (Ptr CChar, Int) -> IO (Maybe a)
storableFromLMDB = fmap Just . peek . castPtr . fst

serializeToLMDB :: Serialize a => a -> ((Ptr CChar, Int) -> IO b) -> IO b
serializeToLMDB = toLMDB . encode

serializeFromLMDB :: Serialize a => (Ptr CChar, Int) -> IO (Maybe a)
serializeFromLMDB a = (either (const Nothing) Just . decode =<<) <$> fromLMDB a

safeCopyToLMDB :: SafeCopy a => a -> ((Ptr CChar, Int) -> IO b) -> IO b
safeCopyToLMDB = toLMDB . runPut . safePut

safeCopyFromLMDB :: SafeCopy a => (Ptr CChar, Int) -> IO (Maybe a)
safeCopyFromLMDB a = (either (const Nothing) Just . runGet safeGet =<<) <$> fromLMDB a

instance ToLMDB   Bool    where toLMDB   = storableToLMDB
instance FromLMDB Bool    where fromLMDB = storableFromLMDB

instance ToLMDB   String  where toLMDB   = serializeToLMDB
instance FromLMDB String  where fromLMDB = serializeFromLMDB

instance ToLMDB   Integer where toLMDB   = serializeToLMDB
instance FromLMDB Integer where fromLMDB = serializeFromLMDB

instance ToLMDB   Float   where toLMDB   = storableToLMDB
instance FromLMDB Float   where fromLMDB = storableFromLMDB

instance ToLMDB   Double  where toLMDB   = storableToLMDB
instance FromLMDB Double  where fromLMDB = storableFromLMDB

instance ToLMDB   Int     where toLMDB   = storableToLMDB
instance FromLMDB Int     where fromLMDB = storableFromLMDB

instance ToLMDB   Int8    where toLMDB   = storableToLMDB
instance FromLMDB Int8    where fromLMDB = storableFromLMDB

instance ToLMDB   Int16   where toLMDB   = storableToLMDB
instance FromLMDB Int16   where fromLMDB = storableFromLMDB

instance ToLMDB   Int32   where toLMDB   = storableToLMDB
instance FromLMDB Int32   where fromLMDB = storableFromLMDB

instance ToLMDB   Int64   where toLMDB   = storableToLMDB
instance FromLMDB Int64   where fromLMDB = storableFromLMDB

instance ToLMDB   Word8   where toLMDB   = storableToLMDB
instance FromLMDB Word8   where fromLMDB = storableFromLMDB

instance ToLMDB   Word16  where toLMDB   = storableToLMDB
instance FromLMDB Word16  where fromLMDB = storableFromLMDB

instance ToLMDB   Word32  where toLMDB   = storableToLMDB
instance FromLMDB Word32  where fromLMDB = storableFromLMDB

instance ToLMDB   Word64  where toLMDB   = storableToLMDB
instance FromLMDB Word64  where fromLMDB = storableFromLMDB

