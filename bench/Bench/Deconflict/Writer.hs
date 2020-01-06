{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes       #-}
{-# LANGUAGE TemplateHaskell   #-}
{-# LANGUAGE StandaloneDeriving   #-}
{-# LANGUAGE DeriveAnyClass   #-}
{-# LANGUAGE TypeApplications   #-}
{-# LANGUAGE DeriveTraversable   #-}
{-# LANGUAGE DeriveFoldable   #-}
{-# LANGUAGE DeriveFunctor   #-}
module Bench.Deconflict.Writer
where

import Data.Avro.Deriving
import Text.RawString.QQ

import Data.Vector (Vector)
import qualified Data.Vector as V
import qualified Data.Vector.Mutable as MV
import Data.ByteString.Lazy (ByteString)
import Data.Avro.Schema (Schema)
import GHC.Int (Int32, Int64)
import Data.Binary.Get
import Data.Text (Text)
import Data.Traversable
import Data.Foldable

import qualified Data.Avro.Decode.Get as Get

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

-- getRecordValues :: Schema -> ByteString -> Vector Dynamic
-- getRecordValues (Record _ _ _ _ fs) =
--   let 
--   in undefined

data VValue
      = Null
      | Boolean Bool
      | Int Int32
      | Long Int64
      | Float Float
      | Double Double
      | Bytes ByteString
      | String Text
      | Array [VValue]
      -- | Map (HashMap Text (VValue f))
      | Record (Vector VValue)
      | Union Int VValue
      | Fixed ByteString
      | Enum Int Text
  deriving (Eq, Show)

data R a = Success ByteString a | Failure String 
  deriving (Show, Traversable, Functor, Eq, Foldable)

toEither :: R a -> Either String a
toEither (Success _ a) = Right a
toEither (Failure err) = Left err

getInnerR :: ByteString -> R (Vector VValue)
getInnerR bs = 
  V.createT $ do
    vals <- MV.unsafeNew 1

    case runGetOrFail (Get.getAvro @Int32) bs of
      Left (_, _, err) -> pure $ Failure err
      Right (bs', _, val) -> do
        MV.write vals 0 (Int val)
        pure $ Success bs' vals

getOuterR :: ByteString -> R (Vector VValue)
getOuterR bs = 
  V.createT $ do
    vals <- MV.unsafeNew 3

    case runGetOrFail (Get.getAvro @Text) bs of
      Left (_, _, err) -> pure $ Failure err
      Right (bs', _, val) -> do
        MV.write vals 0 (String val)
        case getInnerR bs' of
          Failure err -> pure $ Failure err
          Success bs'' val2 -> do
            MV.write vals 1 (Record val2)
            case getInnerR bs'' of
              Failure err -> pure $ Failure err
              Success bs''' val3 -> do
                MV.write vals 2 (Record val3)  
                pure $ Success bs''' vals    

getInner :: Vector VValue -> Inner
getInner vals =
  let
    Int v1 = vals V.! 0
  in Inner v1

getOuter :: Vector VValue -> Outer
getOuter vals =
  let
    String v1 = vals V.! 0
    Record v2 = vals V.! 1
    Record v3 = vals V.! 2
  in Outer v1 (getInner v2) (getInner v3)

getOuter' :: ByteString -> Either String Outer
getOuter' bs = 
  case getOuterR bs of
    Failure err -> Left err
    Success _ val -> Right $ getOuter val