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
import           Data.Maybe
import           Foreign.Marshal.Utils
import           Foreign.Ptr
import           Foreign.Storable
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

-- myEventSourcingDatabase
--   :: QueryTable Bool "trues"  0 () Int64
--   :& QueryTable Bool "falses" 0 () Int64
-- myEventSourcingDatabase
--    = MkQueryTable trueCount
--   :& MkQueryTable falseCount
--   where
--     trueCount  me ev = when   ev . void $ modify me () (maybe 1 succ)
--     falseCount me ev = unless ev . void $ modify me () (maybe 1 succ)

main :: IO ()
main = hspec . parallel $ do
  describe "Database.LMDB.Safe" $ do
    specify "create" $ withTmpDir $ \tmp ->
      withLMDB (Config tmp (5*1000*1000) 10 []) mySimpleDatabase $ \transaction db ->
        return ()
    specify "put" $ withTmpDir  $ \tmp ->
      withLMDB (Config tmp (5*1000*1000) 10 []) mySimpleDatabase $ \transaction db -> do
        ret <- transaction ReadWrite $ put db "haskell" "curry"
        assertBool "put returned False" ret
    specify "get == Nothing" $ withTmpDir  $ \tmp ->
      withLMDB (Config tmp (5*1000*1000) 10 []) mySimpleDatabase $ \transaction db -> do
        ret <- transaction ReadOnly $ get db "idontexist"
        ret @?= Nothing
    specify "put >> get (same transaction)" $ withTmpDir  $ \tmp ->
      withLMDB (Config tmp (5*1000*1000) 10 []) mySimpleDatabase $ \transaction db ->
        transaction ReadWrite $ do
          put db "haskell" "curry" >>= liftIO . assertBool "put returned False"
          get db "haskell" >>= liftIO . assertEqual "get failed" (Just "curry")
    specify "put >> get (two transactions)" $ withTmpDir  $ \tmp ->
      withLMDB (Config tmp (5*1000*1000) 10 []) mySimpleDatabase $ \transaction db -> do
        transaction ReadWrite $
          put db "haskell" "curry" >>= liftIO . assertBool "put returned False"
        transaction ReadOnly  $
          get db "haskell" >>= liftIO . assertEqual "get failed" (Just "curry")
    specify "put >> get (re-open database)" $ withTmpDir  $ \tmp -> do
      withLMDB (Config tmp (5*1000*1000) 10 []) mySimpleDatabase $ \transaction db -> do
        transaction ReadWrite $
          put db "haskell" "curry" >>= liftIO . assertBool "put returned False"
      withLMDB (Config tmp (5*1000*1000) 10 []) mySimpleDatabase $ \transaction db -> do
        transaction ReadOnly  $
          get db "haskell" >>= liftIO . assertEqual "get failed" (Just "curry")
    specify "1200*put >> 1200*get (one transaction)" $ withTmpDir  $ \tmp ->
      withLMDB (Config tmp (5*1000*1000) 10 []) myIntDatabase $ \transaction db -> do
        transaction ReadWrite $
          forM_ [1..1200] $ \i ->
            put db i (BS8.pack $ show i) >>= liftIO . assertBool "put returned False"
        transaction ReadOnly  $
          forM_ [1..1200] $ \i ->
            get db i >>= liftIO . assertEqual "get failed" (Just $ BS8.pack $ show i)
    specify "1200*put >> 1200*get (many transactions)" $ withTmpDir  $ \tmp ->
      withLMDB (Config tmp (5*1000*1000) 10 []) myIntDatabase $ \transaction db -> do
        forM_ [1..1200] $ \i ->
          transaction ReadWrite $
            put db i (BS8.pack $ show i) >>= liftIO . assertBool "put returned False"
        transaction ReadOnly  $
          forM_ [1..1200] $ \i ->
            get db i >>= liftIO . assertEqual "get failed" (Just $ BS8.pack $ show i)
    specify "1200*put >> 1200*get (many transactions, MDB_APPEND)" $ withTmpDir  $ \tmp ->
      withLMDB (Config tmp (5*1000*1000) 10 []) myIntDatabase $ \transaction db -> do
        let mdbAppend = compileWriteFlags [MDB_APPEND]
        forM_ [1..1200] $ \i ->
          transaction ReadWrite $
            putWith mdbAppend db i (BS8.pack $ show i) >>= liftIO . assertBool "put returned False"
        transaction ReadOnly  $
          forM_ [1..1200] $ \i ->
            get db i >>= liftIO . assertEqual "get failed" (Just $ BS8.pack $ show i)
    specify "many databases" $ withTmpDir  $ \tmp ->
      withLMDB (Config tmp (5*1000*1000) 10 []) manyDatabases
      $ \transaction (db1 :& db2 :& db3 :& db4 :& db5) -> do
        transaction ReadWrite $
          put db1 0 "1" >> put db2 0 "2" >> put db3 0 "3" >> put db4 0 "4" >> put db5 0 "5"
        transaction ReadOnly $ do
          get db1 0 >>= liftIO . assertEqual "" (Just "1")
          get db2 0 >>= liftIO . assertEqual "" (Just "2")
          get db3 0 >>= liftIO . assertEqual "" (Just "3")
          get db4 0 >>= liftIO . assertEqual "" (Just "4")
          get db5 0 >>= liftIO . assertEqual "" (Just "5")
    specify "cursors on empty database" $ withTmpDir $ \tmp ->
      withLMDB (Config tmp (5*1000*1000) 10 []) myIntDatabase $ \transaction db -> do
        transaction ReadOnly $
          withCursor db $ \curr -> do
            cursorFirst curr >>= liftIO . assertEqual "cursorFirst" False
            cursorLast curr  >>= liftIO . assertEqual "cursorLast" False
    specify "cursors" $ withTmpDir $ \tmp ->
      withLMDB (Config tmp (5*1000*1000) 10 []) myIntDatabase $ \transaction db -> do
        transaction ReadWrite $
          forM_ [1200,1199..1] $ \i ->
            put db i (BS8.pack $ show i) >>= liftIO . assertBool "put returned False"
        transaction ReadOnly $
          withCursor db $ \curr -> do
            cursorFirst  curr >>= liftIO . assertBool  "cursorFirst"
            cursorGet    curr >>= liftIO . assertEqual "get first value" (1, "1")
            cursorNext   curr >>= liftIO . assertBool  "cursorNext"
            cursorGet    curr >>= liftIO . assertEqual "get next value" (2, "2")
            cursorLast   curr >>= liftIO . assertBool  "cursorLast"
            cursorGet    curr >>= liftIO . assertEqual "get last value" (1200, "1200")
            cursorGetKey curr >>= liftIO . assertEqual "getKey last value" 1200
            cursorNext   curr >>= liftIO . assertEqual "cursorNext after cursorLast" False

    specify "Killing in front of a data load shouldn't break consistency" $ withTmpDir $ \tmp -> do
      worker <- forkIO $ do
        withLMDB (Config tmp (5*1000*1000) 10 []) myIntDatabase $ \transaction db ->
          forM_ [1..100000] $ \i -> transaction ReadWrite $
            put db i "consistent" >>= liftIO . assertBool "put returned False"
      threadDelay $ 1*1000*1000
      killThread worker
      withLMDB (Config tmp (5*1000*1000) 10 []) myIntDatabase $ \transaction db -> do
        transaction ReadOnly $
          withCursor db $ \curr -> do
            cursorFirst curr >>= \case
              False -> liftIO $ assertFailure "failed to get first pos"
              True -> go curr
                where
                   go curr = do
                     cursorGet curr >>= liftIO . assertEqual "value is not consistent" "consistent" . snd
                     cursorNext curr >>= bool (return ()) (go curr)
                     -- TODO: NOSYNC  should fail

  -- describe "Database.LMDB.EventSourcing" $ do
  --   specify "publish events" $ withTmpDir $ \tmp -> do
  --     withEventSource tmp myEventSourcingDatabase
  --     $ \pub transaction (trues :& falses) -> do
  --       forM_ [True, True, True, False, False] pub
  --       threadDelay $ 2*1000*1000
  --       transaction (get trues  ()) >>= assertEqual "trues"  (Just 3)
  --       transaction (get falses ()) >>= assertEqual "falses" (Just 2)

  -- describe "Database.LMDB.Raw" $ do
  --   specify "raw - many transactions" $ withTmpDir $ \tmp -> runInBoundThread $ do
  --     env <- mdb_env_create
  --     mdb_env_set_mapsize env $ 5*1000*1000*1000
  --     mdb_env_open env tmp []

  --     let writeFlags = compileWriteFlags []

  --     txn <- mdb_txn_begin env Nothing False
  --     dbi <- mdb_dbi_open' txn Nothing [MDB_CREATE, MDB_INTEGERKEY]
  --     mdb_txn_commit txn

  --     forM_ [1..1200] $ \i -> do
  --       txn <- mdb_txn_begin env Nothing False
  --       withMDBVal i $ \kval -> withMDBVal i $ \vval ->
  --         mdb_put' writeFlags txn dbi kval vval
  --       mdb_txn_commit txn

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
