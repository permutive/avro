{-# LANGUAGE NumDecimals         #-}
{-# LANGUAGE OverloadedLists     #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell     #-}
{-# LANGUAGE TypeApplications    #-}

module Bench.Deconflict
( only
, notOnly
)
where

import Data.Avro            (decode, encode, toAvro)
import Data.Avro.Deconflict
import Data.Avro.Deriving
import Data.Avro.FromAvro   (fromAvro)
import Data.Avro.Schema     (Result)
import Data.Vector          (Vector)
import Text.RawString.QQ

import qualified Bench.Deconflict.Reader as R
import qualified Bench.Deconflict.Writer as W
import qualified Data.Vector             as Vector
import qualified System.Random           as Random

import Gauge

newOuter :: IO W.Outer
newOuter = do
  i1 <- Random.randomRIO (minBound, maxBound)
  i2 <- Random.randomRIO (minBound, maxBound)
  pure $ W.Outer "Written" (W.Inner i1) (W.Inner i2)

many :: Int -> IO a -> IO (Vector a)
many = Vector.replicateM

-- | Only deconflicts values without actually decoding into generated types
only :: Benchmark
only = env (many 1e5 $ toAvro <$> newOuter) $ \ values ->
  bgroup "Encoded: Value"
    [ bgroup "No Deconflict"
        [ bench "Decode from value" $ nf (fmap (fromAvro @W.Outer)) values
        ]
    -- , bgroup "deconflict"
    --     [ bench "plain"     $ nf (fmap (deconflict          W.schema'Outer R.schema'Outer)) $ values
    --     , bench "noResolve" $ nf (fmap (deconflictNoResolve W.schema'Outer R.schema'Outer)) $ values
    --     ]
    ]

notOnly :: Benchmark
notOnly = env (many 1e5 $ encode <$> newOuter) $ \ values ->
  bgroup "Encoded: ByteString"
    [ bgroup "No Deconflict"
        [ bench "Read via current values"  $ nf (fmap (decode @W.Outer)) values
        , bench "Read via new values"      $ nf (fmap W.ggOuter) values
        ]
    ]

