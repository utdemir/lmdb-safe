{-# LANGUAGE DataKinds                  #-}
{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase                 #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE TemplateHaskell            #-}
{-# LANGUAGE TupleSections              #-}
{-# LANGUAGE TypeFamilies               #-}
{-# LANGUAGE TypeOperators              #-}

module Database.LMDB.Safe.EventSourcing
  ( module Database.LMDB.Safe
  , withEventSource
  , EventId
  ) where

--------------------------------------------------------------------------------
import           Control.Monad
import qualified Control.Monad.State.Strict as S
import           Data.Bool
import           Data.Functor
import           Data.Int
import           Data.Proxy
import           Data.SafeCopy
import           Pipes                      hiding (Proxy)
import           Prelude                    hiding (init)
--------------------------------------------------------------------------------
import           Database.LMDB.Safe
--------------------------------------------------------------------------------

newtype EventId = EventId Int64
  deriving (Show, Eq, Ord, ToLMDB, FromLMDB)
deriveSafeCopy 1 'base ''EventId

-- we can't make EventId an Enum because it requires conversion to Int,
-- which is smaller than Int64.
firstEventId :: EventId
firstEventId = EventId 1

succEventId :: EventId -> EventId
succEventId (EventId i) = EventId (i+1)

fromToEventId :: EventId -> EventId -> [EventId]
fromToEventId f s = takeWhile (<= s) $ iterate succEventId f

data StoredState st
  = StoredState EventId st

deriveSafeCopy 1 'base ''StoredState
instance SafeCopy st => FromLMDB (StoredState st) where
  fromLMDB = safeCopyFromLMDB
instance SafeCopy st => ToLMDB (StoredState st) where
  toLMDB = safeCopyToLMDB

type EventSourceSchema ev st
  = DBDef "events" (Create :& IntegerKey) EventId (SafeCopyLMDB ev)
 :& DBDef "state"  Create                 ()      (StoredState st)

withEventSource
  :: (SafeCopy event, SafeCopy state)
  => FilePath
  -> state
  -> (state -> (EventId, event) -> state)
  -> (Pipe event state IO bottom -> IO b)
  -> IO b
withEventSource path init step act
  = withLMDB
      (Config path (5*1000*1000*1000) 2 [])
      (Proxy :: Proxy (EventSourceSchema event state))
    $ \transaction (events :& storedState) ->
        let apply eid ev = S.modify' (\s -> step s (eid, ev)) >> S.get
            pipe = flip S.evalStateT init $ do
               -- at startup, fold all events on database
              liftIO (transaction ReadOnly $ lastEventId events) >>= \case
                Left  err        -> fail $ "Unexpected error from DB: " ++ show err
                Right Nothing    -> return ()
                Right (Just eid) -> do
                  first <- transaction ReadOnly (get storedState ()) >>= \case
                    Right (Just (StoredState seid ss)) | seid <= eid
                      -> S.put ss $> succEventId seid
                    _ -> return firstEventId

                  forM_ (fromToEventId first eid) $ \i ->
                    liftIO (transaction ReadOnly $ get events i) >>= \case
                      Left  err     -> fail $ "Unexpected error from DB: " ++ show err
                      Right Nothing -> return ()
                      Right (Just (SafeCopyLMDB ev))
                        -> void $ apply i ev

              -- yield the state initially
              lift . yield =<< S.get

              -- main loop, wait for an event, persist it and yield state
              forever $ do
                ev <- lift await
                eid' <- liftIO . transaction ReadOnly $
                   maybe firstEventId (\(EventId i) -> EventId $ succ i)
                     <$> lastEventId events
                eid <- case eid' of
                  Left err -> fail $ "Unexpected error from DB: " ++ show err
                  Right xs -> return xs

                apply eid ev
                _ <- transaction ReadWrite $ put events eid (SafeCopyLMDB ev)

                st <- S.get
                lift $ yield st

                -- for every 1000th event, persist state
                case eid of
                  EventId i | (i `mod` 1000) == 0
                    -> void $ transaction ReadWrite (put storedState () (StoredState eid st))
                  _ -> return ()

                return ()


        in act pipe

lastEventId :: DB EventId a -> Transaction ro (Maybe EventId)
lastEventId events
  = withCursor events $ \curr ->
      bool (return Nothing) (Just <$> cursorGetKey curr) =<< cursorLast curr
