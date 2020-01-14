{-# LANGUAGE DeriveAnyClass      #-}
{-# LANGUAGE DeriveFoldable      #-}
{-# LANGUAGE DeriveFunctor       #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE DeriveTraversable   #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE QuasiQuotes         #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving  #-}
{-# LANGUAGE StrictData          #-}
{-# LANGUAGE TemplateHaskell     #-}
{-# LANGUAGE TupleSections       #-}
{-# LANGUAGE TypeApplications    #-}

module Bench.Deconflict.Writer
where

import Data.Avro.Deriving
import Text.RawString.QQ

import           Control.Monad       (replicateM, when)
import           Data.Avro.Schema    (Field, Schema, TypeName)
import           Data.Binary.Get
import           Data.ByteString     (ByteString)
import           Data.Foldable
import           Data.Text           (Text)
import           Data.Traversable
import           Data.Vector         (Vector)
import qualified Data.Vector         as V
import qualified Data.Vector.Mutable as MV
import           GHC.Int             (Int32, Int64)

import qualified Data.Avro.Decode.Get as Get
import qualified Data.Avro.Schema     as S
import qualified Data.Binary.Get      as Get

import qualified Data.ByteString.Lazy as LBS
import           Data.HashMap.Strict  (HashMap)
import qualified Data.HashMap.Strict  as HashMap
import qualified Data.Text            as Text

import Control.Monad.ST (ST)
import Data.Dynamic

import Control.DeepSeq

deriveAvroFromByteString [r|
{
  "type": "record",
  "name": "Outer",
  "fields": [
    { "name": "name", "type": "string" },
    { "name": "inner", "type": {
        "type": "record",
        "name": "Inner",
        "fields": [
          { "name": "id", "type": "int" }
        ]
      }
    },
    { "name": "other", "type": "Inner" }
  ]
}
|]

deriving instance NFData Inner
deriving instance NFData Outer

data VValue
      = Null
      | Boolean Bool
      | Int Int32
      | Long Int64
      | Float Float
      | Double Double
      | Bytes ByteString
      | String Text
      | Array (Vector VValue)
      | Map (HashMap Text VValue)
      | Record (Vector VValue)
      | Union Int VValue
      | Fixed ByteString
      | Enum Int Text
  deriving (Eq, Show)

data R a = Success LBS.ByteString a | Failure String
  deriving (Show, Traversable, Functor, Eq, Foldable)

toEither :: R a -> Either String a
toEither (Success _ a) = Right a
toEither (Failure err) = Left err

getInnerR :: LBS.ByteString -> R (Vector VValue)
getInnerR bs =
  V.createT $ do
    vals <- MV.unsafeNew 1

    case runGetOrFail (Get.getAvro @Int32) bs of
      Left (_, _, err) -> pure $ Failure err
      Right (bs', _, val) -> do
        MV.write vals 0 (Int val)
        pure $ Success bs' vals

writeByPositions :: MV.MVector s VValue -> [(Int, VValue)] -> ST s ()
writeByPositions mv writes = foldl (>>) (return ()) (fmap (go mv) writes)
  where go :: MV.MVector s VValue ->  (Int, VValue) -> ST s ()
        go mv (n, v) = MV.write mv n v

getValue :: Schema -> Get VValue
getValue sch =
  let env = S.extractBindings sch
  in getField env sch


getRecord :: HashMap TypeName Schema -> [Field] -> Get (Vector VValue)
getRecord env fs = do
  moos <- forM fs $ \f ->
    case S.fldStatus f of
      S.Ignored   -> getField env (S.fldType f) >> pure []
      S.AsIs i    -> fmap ((:[]) . (i, )) (getField env (S.fldType f))
      S.Defaulted -> undefined

  return $ V.create $ do
    vals <- MV.unsafeNew (length fs)
    writeByPositions vals (mconcat moos)
    return vals

getField :: HashMap TypeName Schema -> Schema -> Get VValue
getField env sch = case sch of
  S.Boolean               -> fmap Boolean Get.getAvro
  S.Int                   -> fmap Int     Get.getAvro
  S.String                -> fmap String  Get.getAvro
  S.Record _ _ _ _ fields -> fmap Record  (getRecord env fields)
  S.Bytes                 -> fmap Bytes   Get.getAvro

  S.NamedType tn          ->
    case HashMap.lookup tn env of
      Nothing -> fail $ "Unable to resolve type name " <> show tn
      Just r  -> getField env r

  S.Enum _ _ _ symbs      -> do
    i <- Get.getLong
    case symbs V.!? fromIntegral i of
      Nothing -> fail $ "Enum " <> show symbs <> " doesn't contain value at position " <> show i
      Just v  -> pure $ Enum (fromIntegral i) v

  S.Union opts            -> do
    i <- Get.getLong
    case opts V.!? fromIntegral i of
      Nothing -> fail $ "Decoded Avro tag is outside the expected range for a Union. Tag: " <> show i <> " union of: " <> show (V.map S.typeName opts)
      Just t  -> Union (fromIntegral i) <$> getField env t

  S.Fixed _ _ size -> Fixed <$> Get.getByteString (fromIntegral size)

  S.Array t -> do
    vals <- getBlocksOf env t
    pure $ Array (V.fromList $ mconcat vals)

  S.Map  t  -> do
    kvs <- getKVBlocks env t
    return $ Map (HashMap.fromList $ mconcat kvs)


getKVBlocks :: HashMap TypeName Schema -> Schema -> Get [[(Text, VValue)]]
getKVBlocks env t = do
  blockLength <- abs <$> Get.getLong
  if blockLength == 0
  then return []
  else do vs <- replicateM (fromIntegral blockLength) ((,) <$> Get.getString <*> getField env t)
          (vs:) <$> getKVBlocks env t
{-# INLINE getKVBlocks #-}

getBlocksOf :: HashMap TypeName Schema -> Schema -> Get [[VValue]]
getBlocksOf env t = do
  blockLength <- abs <$> Get.getLong
  if blockLength == 0
  then return []
  else do
    vs <- replicateM (fromIntegral blockLength) (getField env t)
    (vs:) <$> getBlocksOf env t
{-# INLINE getBlocksOf #-}
--------------------------------------------------------------------------
class FromVValue a where
  fromVValue :: VValue -> Either String a

instance FromVValue Int where
  fromVValue (Int x)  = Right (fromIntegral x)
  fromVValue (Long x) = Right (fromIntegral x)
  fromVValue x        = Left ("Unable to use as Int: " <> show x)
  {-# INLINE fromVValue #-}

instance FromVValue Int32 where
  fromVValue (Int x) = Right x
  fromVValue x       = Left ("Unable to use as Int32: " <> show x)
  {-# INLINE fromVValue #-}

instance FromVValue Double where
  fromVValue (Double x) = Right x
  fromVValue (Float x)  = Right (realToFrac x)
  fromVValue (Long x)   = Right (fromIntegral x)
  fromVValue (Int x)    = Right (fromIntegral x)
  fromVValue x          = Left ("Unable to use as Double: " <> show x)
  {-# INLINE fromVValue #-}

instance FromVValue Bool where
  fromVValue (Boolean x) = Right x
  fromVValue x           = Left ("Unable to use as Bool: " <> show x)
  {-# INLINE fromVValue #-}

instance FromVValue Text where
  fromVValue (String x) = Right x
  fromVValue x          = Left ("Unable to use as Text: " <> show x)
  {-# INLINE fromVValue #-}

instance FromVValue Inner where
  fromVValue (Record vals) = do
    v1 <- fromVValue (vals V.! 0)
    pure $ Inner v1
  fromVValue x = Left ("Unable to use as Inner: " <> show x)
  {-# INLINE fromVValue #-}

instance FromVValue Outer where
  fromVValue (Record vals) = do
    v1 <- fromVValue (vals V.! 0)
    v2 <- fromVValue (vals V.! 1)
    v3 <- fromVValue (vals V.! 2)
    pure $ Outer v1 v2 v3
  fromVValue x = Left ("Unable to use as Outer: " <> show x)
  {-# INLINE fromVValue #-}

--------------------------- GENERATED? -----------------------------------

ggOuter :: LBS.ByteString -> Either String Outer
ggOuter bs = case runGetOrFail (getValue schema'Outer) bs of
    Right (_, _, Record v) -> fromVValue (Record v)
    Right (_, _, s)        -> Left "Illegal type"
    Left (_, _, e)         -> Left e
