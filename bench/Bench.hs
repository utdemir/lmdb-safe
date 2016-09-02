{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeOperators     #-}

module Main where

--------------------------------------------------------------------------------
import           Control.Monad
import           Criterion.Main
import qualified Data.ByteString.Char8 as BS8
import           Data.Int
--------------------------------------------------------------------------------
import           Database.LMDB.Safe
--------------------------------------------------------------------------------

type TestDB = DbDef DefaultDatabase Int64 BS8.ByteString IntegerKey

testDB :: Proxy TestDB
testDB = Proxy

instance ToLMDB   Int64 where toLMDB   = storableToLMDB
instance ToLMDB   Bool  where toLMDB   = storableToLMDB
instance FromLMDB Bool  where fromLMDB = storableFromLMDB

main :: IO ()
main = do
  withLMDB (Config "/home/utdemir/bench" (5*1000*1000*1000) 200) testDB
    $ \transaction db -> do
      defaultMain
        [
        --   bench "100 put - one transaction" $
        --     nfIO . transaction ReadWrite $ forM_ [1..100] $ \i -> put db i True
        -- bench "1000 put - many transactions" $
        --     nfIO . forM_ [1..1000] $ \i -> transaction ReadWrite $ put db i (BS8.pack . take 1024 $ cycle "a")
        bench "1000 put - many transactions - MDB_APPEND" $
            nfIO . forM_ [1..1000] $ \i -> transaction ReadWrite $ putWith mdbAppend db i (BS8.pack . take 1024 $ cycle "a")
        -- , bench "100 get - one transaction" $
        --     nfIO . transaction ReadOnly $ forM_ [1..100] $ \i -> get db 1
        -- , bench "100 get - many transactions" $
        --     nfIO . forM_ [1..100] $ \i -> transaction ReadOnly $ get db 1
        ]

mdbAppend :: MDB_WriteFlags
mdbAppend = compileWriteFlags [MDB_APPEND]
