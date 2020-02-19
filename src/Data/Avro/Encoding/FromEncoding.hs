{-# LANGUAGE TupleSections #-}
module Data.Avro.Encoding.FromEncoding
where

import           Control.Monad            (forM, replicateM)
import           Control.Monad.ST         (ST)
import qualified Data.Avro.Decode.Get     as Get
import           Data.Avro.Encoding.Value
import           Data.Avro.Schema         (Field, Schema, TypeName)
import qualified Data.Avro.Schema         as Schema
import           Data.Binary.Get          (Get)
import qualified Data.Binary.Get          as Get
import qualified Data.ByteString.Lazy     as BL
import           Data.HashMap.Strict      (HashMap)
import qualified Data.HashMap.Strict      as HashMap
import           Data.Text                (Text)
import           Data.Vector              (Vector)
import qualified Data.Vector              as V
import qualified Data.Vector.Mutable      as MV

decodeValueWithSchema :: FromValue a => Schema -> BL.ByteString -> Either String a
decodeValueWithSchema deconflictedSchema payload =
  case Get.runGetOrFail (getValue deconflictedSchema) payload of
    Right (_, _, v) -> fromValue v
    Left (_, _, e)  -> Left e

getValue :: Schema -> Get Value
getValue sch =
  let env = Schema.extractBindings sch
  in getField env sch

getField :: HashMap TypeName Schema -> Schema -> Get Value
getField env sch = case sch of
  Schema.Null                  -> pure Null
  Schema.Boolean               -> fmap Boolean Get.getAvro
  Schema.Int                   -> fmap Int     Get.getAvro
  Schema.Long                  -> fmap Long    Get.getAvro
  Schema.String                -> fmap String  Get.getAvro
  Schema.Record _ _ _ _ fields -> fmap Record  (getRecord env fields)
  Schema.Bytes                 -> fmap Bytes   Get.getAvro

  Schema.NamedType tn          ->
    case HashMap.lookup tn env of
      Nothing -> fail $ "Unable to resolve type name " <> show tn
      Just r  -> getField env r

  Schema.Enum _ _ _ symbs      -> do
    i <- Get.getLong
    case symbs V.!? fromIntegral i of
      Nothing -> fail $ "Enum " <> show symbs <> " doesn't contain value at position " <> show i
      Just v  -> pure $ Enum (fromIntegral i) v

  Schema.Union opts            -> do
    i <- Get.getLong
    case opts V.!? fromIntegral i of
      Nothing -> fail $ "Decoded Avro tag is outside the expected range for a Union. Tag: " <> show i <> " union of: " <> show (V.map Schema.typeName opts)
      Just t  -> Union (fromIntegral i) <$> getField env t

  Schema.Fixed _ _ size -> Fixed <$> Get.getByteString (fromIntegral size)

  Schema.Array t -> do
    vals <- getBlocksOf env t
    pure $ Array (V.fromList $ mconcat vals)

  Schema.Map  t  -> do
    kvs <- getKVBlocks env t
    return $ Map (HashMap.fromList $ mconcat kvs)

  Schema.FreeUnion ix t -> do
    v <- getField env t
    pure $ Union ix v

  s -> error ("Not expected: " <> show s)

getKVBlocks :: HashMap TypeName Schema -> Schema -> Get [[(Text, Value)]]
getKVBlocks env t = do
  blockLength <- abs <$> Get.getLong
  if blockLength == 0
  then return []
  else do vs <- replicateM (fromIntegral blockLength) ((,) <$> Get.getString <*> getField env t)
          (vs:) <$> getKVBlocks env t
{-# INLINE getKVBlocks #-}

getBlocksOf :: HashMap TypeName Schema -> Schema -> Get [[Value]]
getBlocksOf env t = do
  blockLength <- abs <$> Get.getLong
  if blockLength == 0
  then return []
  else do
    vs <- replicateM (fromIntegral blockLength) (getField env t)
    (vs:) <$> getBlocksOf env t

writeByPositions :: MV.MVector s Value -> [(Int, Value)] -> ST s ()
writeByPositions mv writes = foldl (>>) (return ()) (fmap (go mv) writes)
  where go :: MV.MVector s Value ->  (Int, Value) -> ST s ()
        go mv (n, v) = MV.write mv n v

getRecord :: HashMap TypeName Schema -> [Field] -> Get (Vector Value)
getRecord env fs = do
  moos <- forM fs $ \f ->
    case Schema.fldStatus f of
      Schema.Ignored   -> getField env (Schema.fldType f) >> pure []
      Schema.AsIs i    -> fmap ((:[]) . (i, )) (getField env (Schema.fldType f))
      Schema.Defaulted -> undefined

  return $ V.create $ do
    vals <- MV.unsafeNew (length fs)
    writeByPositions vals (mconcat moos)
    return vals

