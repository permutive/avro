{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StrictData          #-}
module Data.Avro.Internal.Container
where

import           Control.Monad              (when)
import qualified Data.Aeson                 as Aeson
import           Data.Avro.Codec            (Codec, Decompress)
import qualified Data.Avro.Codec            as Codec
import           Data.Avro.Schema           (Schema)
import           Data.Binary.Get            (Get)
import qualified Data.Binary.Get            as Get
import           Data.ByteString            (ByteString)
import qualified Data.ByteString.Lazy       as BL
import qualified Data.ByteString.Lazy.Char8 as BLC
import qualified Data.Map.Strict            as Map
import           Data.Text                  (Text)

import qualified Data.Avro.Internal.Get as AGet

data ContainerHeader = ContainerHeader
  { syncBytes       :: BL.ByteString
  , decompress      :: forall a. Decompress a
  , containedSchema :: Schema
  }

nrSyncBytes :: Integral sb => sb
nrSyncBytes = 16

getContainerHeader :: Get ContainerHeader
getContainerHeader = do
  magic <- getFixed avroMagicSize
  when (BL.fromStrict magic /= avroMagicBytes)
        (fail "Invalid magic number at start of container.")
  metadata <- getMeta
  sync  <- BL.fromStrict <$> getFixed nrSyncBytes
  codec <- parseCodec (Map.lookup "avro.codec" metadata)
  schema <- case Map.lookup "avro.schema" metadata of
              Nothing -> fail "Invalid container object: no schema."
              Just s  -> case Aeson.eitherDecode' s of
                            Left e  -> fail ("Can not decode container schema: " <> e)
                            Right x -> return x
  return ContainerHeader  { syncBytes = sync
                          , decompress = Codec.codecDecompress codec
                          , containedSchema = schema
                          }
  where avroMagicSize :: Integral a => a
        avroMagicSize = 4

        avroMagicBytes :: BL.ByteString
        avroMagicBytes = BLC.pack "Obj" <> BL.pack [1]

        getFixed :: Int -> Get ByteString
        getFixed = Get.getByteString

        getMeta :: Get (Map.Map Text BL.ByteString)
        getMeta =
          let keyValue = (,) <$> AGet.getString <*> AGet.getBytesLazy
          in Map.fromList <$> AGet.decodeBlocks keyValue

----------------------------------------------------------------
parseCodec :: Monad m => Maybe BL.ByteString -> m Codec
parseCodec (Just "null")    = pure Codec.nullCodec
parseCodec (Just "deflate") = pure Codec.deflateCodec
parseCodec (Just x)         = error $ "Unrecognized codec: " <> BLC.unpack x
parseCodec Nothing          = pure Codec.nullCodec
