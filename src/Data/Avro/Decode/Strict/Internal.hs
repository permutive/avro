{-# LANGUAGE FlexibleInstances   #-}
{-# LANGUAGE MultiWayIf          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections       #-}
{-# LANGUAGE TypeApplications    #-}
module Data.Avro.Decode.Strict.Internal
where

import qualified Codec.Compression.Zlib     as Z
import           Control.Monad              (replicateM, when)
import qualified Data.Aeson                 as A
import qualified Data.Array                 as Array
import           Data.Avro.Internal.Get
import           Data.Binary.Get            (Get, runGetOrFail)
import qualified Data.Binary.Get            as G
import           Data.Binary.IEEE754        as IEEE
import           Data.Bits
import           Data.ByteString            (ByteString)
import qualified Data.ByteString.Lazy       as BL
import qualified Data.ByteString.Lazy.Char8 as BC
import qualified Data.HashMap.Strict        as HashMap
import           Data.Int
import           Data.List                  (foldl')
import qualified Data.List.NonEmpty         as NE
import qualified Data.Map                   as Map
import           Data.Maybe
import           Data.Monoid                ((<>))
import qualified Data.Set                   as Set
import           Data.Text                  (Text)
import qualified Data.Text                  as Text
import qualified Data.Text.Encoding         as Text
import qualified Data.Vector                as V
import           Prelude                    as P

import           Data.Avro.Decode.Get
import           Data.Avro.DecodeRaw
import           Data.Avro.Schema     as S
import qualified Data.Avro.Types      as T
import           Data.Avro.Zag

{-# INLINABLE getAvroOf #-}
getAvroOf :: Schema -> Get (T.Value Schema)
getAvroOf ty0 = go ty0
 where
 env = S.buildTypeEnvironment envFail ty0
 envFail t = fail $ "Named type not in schema: " <> show t

 go :: Schema -> Get (T.Value Schema)
 go ty =
  case ty of
    Null    -> return T.Null
    Boolean -> T.Boolean <$> getAvro
    Int     -> T.Int     <$> getAvro
    Long    -> T.Long    <$> getAvro
    Float   -> T.Float   <$> getAvro
    Double  -> T.Double  <$> getAvro
    Bytes   -> T.Bytes   <$> getAvro
    String  -> T.String  <$> getAvro
    Array t ->
      do vals <- getBlocksOf t
         return $ T.Array (V.fromList $ mconcat vals)
    Map  t  ->
      do kvs <- getKVBlocks t
         return $ T.Map (HashMap.fromList $ mconcat kvs)
    NamedType tn -> env tn >>= go
    Record {..} ->
      T.Record ty . HashMap.fromList . catMaybes <$> mapM getField fields
    Enum {..} ->
      do i <- getLong
         let sym = fromMaybe "" (symbols V.!? (fromIntegral i)) -- empty string for 'missing' symbols (alternative is an error or exception)
         pure (T.Enum ty (fromIntegral i) sym)
    Union ts ->
      do i <- getLong
         case ts V.!? (fromIntegral i) of
          Nothing -> fail $ "Decoded Avro tag is outside the expected range for a Union. Tag: " <> show i <> " union of: " <> show (V.map typeName ts)
          Just t  -> T.Union ts t <$> go t
    Fixed {..} -> T.Fixed ty <$> G.getByteString (fromIntegral size)
    IntLongCoercion     -> T.Long   . fromIntegral <$> getAvro @Int32
    IntFloatCoercion    -> T.Float  . fromIntegral <$> getAvro @Int32
    IntDoubleCoercion   -> T.Double . fromIntegral <$> getAvro @Int32
    LongFloatCoercion   -> T.Float  . fromIntegral <$> getAvro @Int64
    LongDoubleCoercion  -> T.Double . fromIntegral <$> getAvro @Int64
    FloatDoubleCoercion -> T.Double . realToFrac   <$> getAvro @Float
    FreeUnion _ ty -> T.Union (V.singleton ty) ty <$> go ty

 getField :: Field -> Get (Maybe (Text, T.Value Schema))
 getField Field{..} =
  case (fldStatus, fldDefault) of
    (AsIs _, _)          -> Just . (fldName,) <$> go fldType
    (Defaulted, Just v)  -> pure $ Just (fldName, v)
    (Ignored,   Nothing) -> go fldType >> pure Nothing

 getKVBlocks :: Schema -> Get [[(Text,T.Value Schema)]]
 getKVBlocks t =
  do blockLength <- abs <$> getLong
     if blockLength == 0
      then return []
      else do vs <- replicateM (fromIntegral blockLength) ((,) <$> getString <*> go t)
              (vs:) <$> getKVBlocks t
 {-# INLINE getKVBlocks #-}

 getBlocksOf :: Schema -> Get [[T.Value Schema]]
 getBlocksOf t =
  do blockLength <- abs <$> getLong
     if blockLength == 0
      then return []
      else do vs <- replicateM (fromIntegral blockLength) (go t)
              (vs:) <$> getBlocksOf t
 {-# INLINE getBlocksOf #-}
