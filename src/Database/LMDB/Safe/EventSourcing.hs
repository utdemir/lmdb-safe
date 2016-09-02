{-# LANGUAGE DataKinds                  #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase                 #-}
{-# LANGUAGE TupleSections              #-}
{-# LANGUAGE TypeFamilies               #-}
{-# LANGUAGE TypeOperators              #-}

module Database.LMDB.Safe.EventSourcing
  ( module Database.LMDB.Safe
  , withEventSource
  , EventId

  , Fold (..)
  ) where

--------------------------------------------------------------------------------
import           Control.Foldl
import           Control.Monad
import qualified Control.Monad.State.Strict as S
import           Data.Bool
import           Data.Int
import           Data.Proxy
import           GHC.TypeLits
import           Pipes                      hiding (Proxy)
import qualified Pipes.Concurrent           as P
--------------------------------------------------------------------------------
import           Database.LMDB.Safe
import           Type.And
--------------------------------------------------------------------------------

data Event a = Event a

newtype EventId = EventId Int64
  deriving (Show, Eq, Ord, ToLMDB, FromLMDB)

-- we can't make EventId an Enum because it requires conversion to Int,
-- which is smaller than Int64.

firstEventId :: EventId
firstEventId = EventId 1

succEventId :: EventId -> EventId
succEventId (EventId i) = EventId (i+1)

fromToEventId :: EventId -> EventId -> [EventId]
fromToEventId (EventId f) (EventId s)
  = fmap EventId $ takeWhile (<= s) $ iterate (+1) f

type EventSourceSchema a
  = DBDef "events" (Create :& IntegerKey) EventId (Event a)

instance FromLMDB a => FromLMDB (Event a) where
  fromLMDB = fmap (fmap Event) . fromLMDB
instance ToLMDB a => ToLMDB (Event a) where
  toLMDB (Event a) = toLMDB a

withEventSource
  :: (FromLMDB event, ToLMDB event)
  => FilePath
  -> Fold (EventId, event) state
  -> (  Pipe event state IO ()
     -> IO b
     )
  -> IO b
withEventSource path (Fold step init extract) act
  = withLMDB
      (Config path (5*1000*1000*1000) 1 [])
      (Proxy :: Proxy (EventSourceSchema a))
    $ \transaction events ->
        let apply eid ev = S.modify' (\s -> step s (eid, ev)) >> S.get
            pipe = flip S.evalStateT init $ do
              -- at startup, fold all events on database
              eid <- liftIO $ transaction ReadOnly $ lastEventId events
              case eid of
                Nothing  -> return ()
                Just eid -> forM_ (fromToEventId firstEventId eid) $ \i ->
                  liftIO (transaction ReadOnly $ get events i) >>= \case
                    Nothing         -> return ()
                    Just (Event ev) -> void $ apply i ev

              -- yield the state initially
              lift . yield . extract =<< S.get

              -- main loop, wait for an event, persist it and yield state
              forever $ do
                ev <- lift await
                eid <- liftIO . transaction ReadOnly $
                   maybe firstEventId (\(EventId i) -> EventId $ succ i)
                     <$> lastEventId events
                apply eid ev >>= lift . yield . extract
                liftIO . transaction ReadWrite $ put events eid (Event ev)
        in act pipe

-- calculateProgress :: EventId -> EventId -> Double
-- calculateProgress (EventId tot) (EventId cur)
--   | tot == 0 || cur == 0 = 0
--   | otherwise            = fromIntegral cur / fromIntegral tot

-- withEventSource
--   :: ( FromLMDB ev, ToLMDB ev
--      , QueryTables queryTables, EvTypeOf queryTables ~ ev
--      , DBSchema (QueryTableSchema queryTables)
--      , IsUnique (DBNames (EventSourceSchema ev :& QueryTableSchema queryTables))
--      , Length (EventSourceSchema ev :& QueryTableSchema queryTables) ~ len, KnownNat len
--      )
--   => FilePath
--   -> queryTables
--   -> ( PublishEvent ev
--      -> (Transaction ReadOnly a -> IO a)
--      -> DBSchemaHandles (QueryTableSchema queryTables)
--      -> IO b
--      )
--   -> IO b
-- withEventSource path (queryTables :: queryTables) act
--   = withLMDB
--       (Config path (5*1000*1000*1000) tableCount [MDB_NOSYNC])
--       (Proxy :: Proxy (EventSourceSchema a :& QueryTableSchema queryTables))
--     $ \transaction ((meta :& events) :& qs) ->
--       bracket
--         (spawn meta events qs transaction queryTables)
--         (mapM_ killThread)
--         $ \_ -> do
--           let publishEvent = \ev ->
--                 mdb_env_sync =<< transaction ReadWrite (do
--                   eid <- maybe 1 succ <$> lastEventId events
--                   put events eid (Event ev)
--                   unsafeTransaction $ \env _ -> return env
--                 )
--           act publishEvent (transaction ReadOnly) qs
--   where
--     tableCount = fromIntegral $ natVal (Proxy :: Proxy (Length (EventSourceSchema ev :& QueryTableSchema queryTables)))

lastEventId :: DB EventId (Event a) -> Transaction ro (Maybe EventId)
lastEventId events
  = withCursor events $ \curr ->
      bool (return Nothing) (Just <$> cursorGetKey curr) =<< cursorLast curr

-- --------------------------------------------------------------------------------

-- data Aggr ev a
--   = MkAggr a (a -> ev -> a)

-- class Aggrs a where
--   type AggrsInputs a
--   type AggrsEventType a
--   spawn :: DB EventId (Event (AggrsEventType a))
--         -> RunTransaction
--         -> a
--         -> IO (AggrsInputs a)

-- instance FromLMDB ev => Aggrs (Aggr ev a) where
--   type AggrsInputs (Aggr ev a) = P.Input a
--   type AggrsEventType (Aggr ev a) = ev
--   spawn eventsDB transaction (MkAggr init step) = do
--     (output, input) <- P.spawn $ P.bounded 16
--     return input

--     transaction ReadWrite $ do
--       get metaDB myName >>= \case
--         Just (TableMeta teid tver) | tver == myVersion ->
--           return ()
--         _ ->
--           void $ clearDB myDb >> put metaDB myName (TableMeta Nothing myVersion)

--     fmap pure . forkIO $ do
--       forever $ do
--         eid <- transaction ReadOnly $ lastEventId eventsDB

--         hasMore <- transaction ReadWrite $ do
--           myeid <- get metaDB myName >>= \case
--             Just (TableMeta teid tver) | tver == myVersion ->
--               return teid
--             _ ->
--               clearDB myDb >> return Nothing

--           case compare myeid eid of
--             LT -> do
--               let newEid = fromMaybe 0 myeid + 1
--               get eventsDB newEid >>= \case
--                 Nothing         -> return ()
--                 Just (Event ev) -> def myDb ev
--               put metaDB myName (TableMeta (Just newEid) myVersion)
--               return True
--             EQ -> return False
--             GT -> error "invariant violation: query table is newer than event log!"

--         unless hasMore $ threadDelay (1*1000*1000)

--     where
--       myName    = symbolVal (Proxy :: Proxy name)
--       myVersion = fromIntegral $ natVal (Proxy :: Proxy ver)

-- instance (QueryTables a, QueryTables b, EvTypeOf a ~ EvTypeOf b) => QueryTables (a :& b) where
--   type QueryTableSchema (a :& b) = QueryTableSchema a :& QueryTableSchema b
--   type EvTypeOf (a :& b) = EvTypeOf a
--   spawn metaDB eventsDB (ta :& tb) transaction (a :& b)
--     = (++) <$> spawn metaDB eventsDB ta transaction a
--            <*> spawn metaDB eventsDB tb transaction b

-- data TableMeta
--   = TableMeta { lastAppliedEvent :: Maybe Int64
--               , version          :: Int64
--               }

-- deriveSafeCopy 1 'base ''TableMeta

-- instance ToLMDB   TableMeta where toLMDB   = safeCopyToLMDB
-- instance FromLMDB TableMeta where fromLMDB = safeCopyFromLMDB
