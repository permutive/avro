{-# LANGUAGE FlexibleInstances   #-}
{-# LANGUAGE MultiWayIf          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | An internal module that contains common decoding functionality
-- that is shared between Lazy and Strict decoders, as well as
-- generic 'Get' monad helpers.
module Data.Avro.Decode.Get
where

import qualified Codec.Compression.Zlib     as Z
import           Control.Monad              (replicateM, when)
import qualified Data.Aeson                 as A
import qualified Data.Array                 as Array
import           Data.Binary.Get            (Get)
import qualified Data.Binary.Get            as G
import           Data.Binary.IEEE754        as IEEE
import           Data.Bits
import           Data.ByteString            (ByteString)
import qualified Data.ByteString.Lazy       as BL
import qualified Data.ByteString.Lazy.Char8 as BC
import           Data.Int
import qualified Data.Map                   as Map
import           Data.Maybe
import           Data.Monoid                ((<>))
import qualified Data.Set                   as Set
import           Data.Text                  (Text)
import qualified Data.Text                  as Text
import qualified Data.Text.Encoding         as Text
import qualified Data.Vector                as V
import           Prelude                    as P

import Data.Avro.Codec
import Data.Avro.DecodeRaw
import Data.Avro.Schema    as S

import Data.Avro.Internal.Get

class GetAvro a where
  getAvro :: Get a

instance GetAvro ty => GetAvro (Map.Map Text ty) where
  getAvro = getMap
instance GetAvro Bool where
  getAvro = getBoolean
instance GetAvro Int32 where
  getAvro = getInt
instance GetAvro Int64 where
  getAvro = getLong
instance GetAvro BL.ByteString where
  getAvro = BL.fromStrict <$> getBytes
instance GetAvro ByteString where
  getAvro = getBytes
instance GetAvro Text where
  getAvro = getString
instance GetAvro Float where
  getAvro = getFloat
instance GetAvro Double where
  getAvro = getDouble
instance GetAvro String where
  getAvro = Text.unpack <$> getString
instance GetAvro a => GetAvro [a] where
  getAvro = getArray
instance GetAvro a => GetAvro (Maybe a) where
  getAvro =
    do t <- getLong
       case t of
        0 -> return Nothing
        1 -> Just <$> getAvro
        n -> fail $ "Invalid tag for expected {null,a} Avro union, received: " <> show n

instance GetAvro a => GetAvro (Array.Array Int a) where
  getAvro =
    do ls <- getAvro
       return $ Array.listArray (0,length ls - 1) ls
instance GetAvro a => GetAvro (V.Vector a) where
  getAvro = V.fromList <$> getAvro
instance (GetAvro a, Ord a) => GetAvro (Set.Set a) where
  getAvro = Set.fromList <$> getAvro


data ContainerHeader = ContainerHeader
  { syncBytes       :: !BL.ByteString
  , decompress      :: forall a. Decompress a
  , containedSchema :: !Schema
  }

nrSyncBytes :: Integral sb => sb
nrSyncBytes = 16

instance GetAvro ContainerHeader where
  getAvro =
   do magic <- getFixed avroMagicSize
      when (BL.fromStrict magic /= avroMagicBytes)
           (fail "Invalid magic number at start of container.")
      metadata <- getMap :: Get (Map.Map Text BL.ByteString) -- avro.schema, avro.codec
      sync  <- BL.fromStrict <$> getFixed nrSyncBytes
      codec <- getCodec (Map.lookup "avro.codec" metadata)
      schema <- case Map.lookup "avro.schema" metadata of
                  Nothing -> fail "Invalid container object: no schema."
                  Just s  -> case A.eitherDecode' s of
                                Left e  -> fail ("Can not decode container schema: " <> e)
                                Right x -> return x
      return ContainerHeader { syncBytes = sync
                             , decompress = codecDecompress codec
                             , containedSchema = schema
                             }
   where avroMagicSize :: Integral a => a
         avroMagicSize = 4

         avroMagicBytes :: BL.ByteString
         avroMagicBytes = BC.pack "Obj" <> BL.pack [1]

         getFixed :: Int -> Get ByteString
         getFixed = G.getByteString


getCodec :: Monad m => Maybe BL.ByteString -> m Codec
getCodec (Just "null")    = pure nullCodec
getCodec (Just "deflate") = pure deflateCodec
getCodec (Just x)         = error $ "Unrecognized codec: " <> BC.unpack x
getCodec Nothing          = pure nullCodec


getArray :: GetAvro ty => Get [ty]
getArray = decodeBlocks getAvro

getMap :: GetAvro ty => Get (Map.Map Text ty)
getMap = Map.fromList <$> decodeBlocks keyValue
  where keyValue = (,) <$> getString <*> getAvro

