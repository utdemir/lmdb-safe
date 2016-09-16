{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeOperators     #-}

module Main where

--------------------------------------------------------------------------------
import           Control.Concurrent
import           Control.Exception                (bracket)
import           Control.Monad
import           Data.Bool
import qualified Data.ByteString                  as BS
import qualified Data.ByteString.Char8            as BS8
import           Data.Int
import           Data.Time.Clock
import           Foreign.Marshal.Utils
import           Foreign.Ptr
import           Foreign.Storable
import           Pipes                            hiding (Proxy)
import qualified Pipes.Prelude                    as P
import           System.Directory
import           System.Posix.Temp
import           Test.Hspec
import           Test.HUnit
--------------------------------------------------------------------------------
import           Database.LMDB.Safe
import           Database.LMDB.Safe.EventSourcing
--------------------------------------------------------------------------------

type MySimpleDatabase = DBDef "simple" Create BS.ByteString BS.ByteString
mySimpleDatabase :: Proxy MySimpleDatabase
mySimpleDatabase = Proxy

type MyIntDatabase = DBDef "simple" (Create :& IntegerKey) Int64 BS.ByteString
myIntDatabase :: Proxy MyIntDatabase
myIntDatabase = Proxy

type ManyDatabases = DBDef "db1" (Create :& IntegerKey) Int64 BS.ByteString
                  :& DBDef "db2" (Create :& IntegerKey) Int64 BS.ByteString
                  :& DBDef "db3" (Create :& IntegerKey) Int64 BS.ByteString
                  :& DBDef "db4" (Create :& IntegerKey) Int64 BS.ByteString
                  :& DBDef "db5" (Create :& IntegerKey) Int64 BS.ByteString
manyDatabases :: Proxy ManyDatabases
manyDatabases = Proxy

data MalformedData = MalformedData deriving Show
instance FromLMDB MalformedData where
  fromLMDB = const $ return Nothing

main :: IO ()
main = hspec . parallel $ do
  describe "Database.LMDB.Safe" $ do
    specify "create" $ withTmpDir $ \tmp ->
      withLMDB (Config tmp (5*1000*1000) 10 []) mySimpleDatabase $ \_ _ ->
        return ()
    specify "put" $ withTmpDir  $ \tmp ->
      withLMDB (Config tmp (5*1000*1000) 10 []) mySimpleDatabase $ \transaction db -> do
        ret <- transaction ReadWrite $ put db "haskell" "curry"
        ret @?= Right True
    specify "get == Nothing" $ withTmpDir  $ \tmp ->
      withLMDB (Config tmp (5*1000*1000) 10 []) mySimpleDatabase $ \transaction db -> do
        ret <- transaction ReadOnly $ get db "idontexist"
        ret @?= Right Nothing
    specify "put >> get (same transaction)" $ withTmpDir  $ \tmp ->
      withLMDB (Config tmp (5*1000*1000) 10 []) mySimpleDatabase $ \transaction db -> do
        r <- transaction ReadWrite $ (,) <$> put db "haskell" "curry"
                                         <*> get db "haskell"
        r @?= Right (True, Just "curry")
    specify "put >> get (two transactions)" $ withTmpDir  $ \tmp ->
      withLMDB (Config tmp (5*1000*1000) 10 []) mySimpleDatabase $ \transaction db -> do
        r <- (,) <$> transaction ReadWrite (put db "haskell" "curry")
                 <*> transaction ReadOnly  (get db "haskell")
        r @?= (Right True, Right (Just "curry"))
    specify "put >> get (re-open database)" $ withTmpDir  $ \tmp -> do
      r1 <- withLMDB (Config tmp (5*1000*1000) 10 []) mySimpleDatabase $ \transaction db ->
        transaction ReadWrite $ put db "haskell" "curry"
      r2 <- withLMDB (Config tmp (5*1000*1000) 10 []) mySimpleDatabase $ \transaction db ->
        transaction ReadOnly $ get db "haskell"
      (r1, r2) @?= (Right True, Right (Just "curry"))
    specify "parse error" $ withTmpDir $ \tmp -> do
      withLMDB (Config tmp (5*1000*1000) 10 []) mySimpleDatabase $ \transaction db -> do
        r <- transaction ReadWrite $ do
          put db "haskell" "curry"
          unsafeGet db ("haskell" :: BS.ByteString) :: Transaction ReadWrite (Maybe MalformedData)
        case r of
          Left (ParseError _) -> return ()
          _ -> assertFailure $ "Expecting parse error, but got" ++ show r
    specify "1200*put >> 1200*get (one transaction)" $ withTmpDir  $ \tmp ->
      withLMDB (Config tmp (5*1000*1000) 10 []) myIntDatabase $ \transaction db -> do
        Right _ <- transaction ReadWrite $ forM_ [1..1200] $ \i -> put db i (BS8.pack $ show i)
        Right _ <- transaction ReadOnly  $
          forM_ [1..1200] $ \i ->
            get db i >>= liftIO . assertEqual "get failed" (Just $ BS8.pack $ show i)
        return ()
    specify "1200*put >> 1200*get (many transactions)" $ withTmpDir  $ \tmp ->
      withLMDB (Config tmp (5*1000*1000) 10 []) myIntDatabase $ \transaction db -> do
        forM_ [1..1200] $ \i ->
          transaction ReadWrite $
            put db i (BS8.pack $ show i) >>= liftIO . assertBool "put returned False"
        Right _ <- transaction ReadOnly  $
          forM_ [1..1200] $ \i ->
            get db i >>= liftIO . assertEqual "get failed" (Just $ BS8.pack $ show i)
        return ()
    specify "1200*put >> 1200*get (many transactions, MDB_APPEND)" $ withTmpDir  $ \tmp ->
      withLMDB (Config tmp (5*1000*1000) 10 []) myIntDatabase $ \transaction db -> do
        let mdbAppend = compileWriteFlags [MDB_APPEND]
        forM_ [1..1200] $ \i ->
          transaction ReadWrite $
            putWith mdbAppend db i (BS8.pack $ show i) >>= liftIO . assertBool "put returned False"
        Right _ <- transaction ReadOnly  $
          forM_ [1..1200] $ \i ->
            get db i >>= liftIO . assertEqual "get failed" (Just $ BS8.pack $ show i)
        return ()
    specify "many databases" $ withTmpDir  $ \tmp ->
      withLMDB (Config tmp (5*1000*1000) 10 []) manyDatabases
      $ \transaction (db1 :& db2 :& db3 :& db4 :& db5) -> do
        Right _ <- transaction ReadWrite $
          put db1 0 "1" >> put db2 0 "2" >> put db3 0 "3" >> put db4 0 "4" >> put db5 0 "5"
        Right _ <- transaction ReadOnly $ do
          get db1 0 >>= liftIO . assertEqual "" (Just "1")
          get db2 0 >>= liftIO . assertEqual "" (Just "2")
          get db3 0 >>= liftIO . assertEqual "" (Just "3")
          get db4 0 >>= liftIO . assertEqual "" (Just "4")
          get db5 0 >>= liftIO . assertEqual "" (Just "5")
        return ()
    specify "cursors on empty database" $ withTmpDir $ \tmp ->
      withLMDB (Config tmp (5*1000*1000) 10 []) myIntDatabase $ \transaction db -> do
        Right _ <- transaction ReadOnly $
          withCursor db $ \curr -> do
            cursorFirst curr >>= liftIO . assertEqual "cursorFirst" False
            cursorLast curr  >>= liftIO . assertEqual "cursorLast" False
        return ()
    specify "cursors" $ withTmpDir $ \tmp ->
      withLMDB (Config tmp (5*1000*1000) 10 []) myIntDatabase $ \transaction db -> do
        Right _ <- transaction ReadWrite $
          forM_ [1200,1199..1] $ \i ->
            put db i (BS8.pack $ show i) >>= liftIO . assertBool "put returned False"
        Right _ <- transaction ReadOnly $
          withCursor db $ \curr -> do
            cursorFirst  curr >>= liftIO . assertBool  "cursorFirst"
            cursorGet    curr >>= liftIO . assertEqual "get first value" (1, "1")
            cursorNext   curr >>= liftIO . assertBool  "cursorNext"
            cursorGet    curr >>= liftIO . assertEqual "get next value" (2, "2")
            cursorLast   curr >>= liftIO . assertBool  "cursorLast"
            cursorGet    curr >>= liftIO . assertEqual "get last value" (1200, "1200")
            cursorGetKey curr >>= liftIO . assertEqual "getKey last value" 1200
            cursorNext   curr >>= liftIO . assertEqual "cursorNext after cursorLast" False
        return ()

    specify "Killing in front of a data load shouldn't break consistency" $ withTmpDir $ \tmp -> do
      worker <- forkIO $ do
        withLMDB (Config tmp (5*1000*1000) 10 []) myIntDatabase $ \transaction db ->
          forM_ [1..100000] $ \i -> transaction ReadWrite $
            put db i "consistent" >>= liftIO . assertBool "put returned False"
      threadDelay $ 1*1000*1000
      killThread worker
      _ <- withLMDB (Config tmp (5*1000*1000) 10 []) myIntDatabase $ \transaction db -> do
        transaction ReadOnly $
          withCursor db $ \curr -> do
            cursorFirst curr >>= \case
              False -> liftIO $ assertFailure "failed to get first pos"
              True -> go
                where
                   go = do
                     cursorGet curr >>= liftIO . assertEqual "value is not consistent" "consistent" . snd
                     cursorNext curr >>= bool (return ()) go
                     -- TODO: NOSYNC  should fail
      return ()

  describe "Database.LMDB.Safe.EventSourcing" $ do
    specify "publish events" $ withTmpDir $ \tmp -> do
      withEventSource tmp (0 :: Integer) (\st (eid, i) -> st + i) $ \es -> do
        r <- runEffect $ (undefined <$> each [1..]) >-> es >-> replicateM 5 await
        r @?= [0, 1, 3, 6, 10]
      withEventSource tmp (0 :: Integer) (\st (eid, i) -> st + i) $ \es -> do
        r <- runEffect $ (undefined <$> each [10..]) >-> es >-> replicateM 5 await
        r @?= [10, 20, 31, 43, 56]
    specify "stored state" $ withTmpDir $ \tmp -> do
      withEventSource tmp (0 :: Integer) (\st (eid, i) -> st + i) $ \es -> do
        r <- P.last $ each [1..5050] >-> es
        r @?= Just 12753775
        return ()
      withEventSource tmp (0 :: Integer) (\st (eid, i) -> st + i) $ \es -> do
        r <- P.last $ return () >-> es
        r @?= Just 12753775
        return ()


  describe "Database.LMDB.Raw" $ do
    specify "raw - many transactions" $ withTmpDir $ \tmp -> runInBoundThread $ do
      env <- mdb_env_create
      mdb_env_set_mapsize env $ 5*1000*1000*1000
      mdb_env_open env tmp []

      let writeFlags = compileWriteFlags []

      txn1 <- mdb_txn_begin env Nothing False
      dbi <- mdb_dbi_open' txn1 Nothing [MDB_CREATE, MDB_INTEGERKEY]
      mdb_txn_commit txn1

      forM_ [1..1200] $ \i -> do
        txn2 <- mdb_txn_begin env Nothing False
        _ <- withMDBVal i $ \kval -> withMDBVal i $ \vval ->
          mdb_put' writeFlags txn2 dbi kval vval
        mdb_txn_commit txn2

withMDBVal :: Int64 -> (MDB_val -> IO a) -> IO a
withMDBVal i act
  = with i $ \ptr -> act (MDB_val (fromIntegral $ sizeOf i) (castPtr ptr))

-- failed transactions shouldnt commit

withTmpDir :: (FilePath -> IO a) -> IO a
withTmpDir = bracket acquire destroy
  where
    acquire :: IO FilePath
    acquire = mkdtemp "lmdb-safe-test-"
    destroy :: FilePath -> IO ()
    destroy = removeDirectoryRecursive
