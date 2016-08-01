{-# LANGUAGE DataKinds     #-}
{-# LANGUAGE TypeOperators #-}

module Main where

--------------------------------------------------------------------------------
import           Control.Monad
import           Criterion.Main
--------------------------------------------------------------------------------
import           Database.LMDB.Safe
--------------------------------------------------------------------------------

type TestDB = DbDef "mydb" Int Bool

testDB :: Proxy TestDB
testDB = Proxy

instance ToLMDB   Int  where toLMDB   = storableToLMDB
instance ToLMDB   Bool where toLMDB   = storableToLMDB
instance FromLMDB Bool where fromLMDB = storableFromLMDB

main :: IO ()
main = do
  openLMDB (Config "/home/utdemir/test" (5*1000*1000*1000) 200) testDB
    $ \transaction db -> do
      defaultMain
        [ bench "100 put - one transaction" $
            nfIO . transaction ReadWrite $ forM_ [1..100] $ \i -> put db i True
        , bench "100 put - many transactions" $
            nfIO . forM_ [1..100] $ \i -> transaction ReadWrite $ put db i True
        , bench "100 put - many transactions - MDB_APPEND" $
            nfIO . forM_ [1..100] $ \i -> transaction ReadWrite $ putWith mdbAppend db i True
        , bench "100 get - one transaction" $
            nfIO . transaction ReadOnly $ forM_ [1..100] $ \i -> get db 1
        , bench "100 get - many transactions" $
            nfIO . forM_ [1..100] $ \i -> transaction ReadOnly $ get db 1
        ]

mdbAppend :: MDB_WriteFlags
mdbAppend = compileWriteFlags [MDB_APPEND]
