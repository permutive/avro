{-# LANGUAGE FlexibleInstances   #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}
{-# LANGUAGE TypeFamilies        #-}

module Data.Avro.Encoding.ToEncoding
where

import qualified Data.Array              as Ar
import           Data.Avro.EncodeRaw
import           Data.Avro.Schema        as S
import           Data.Avro.Types         as T
import qualified Data.Binary.IEEE754     as IEEE
import qualified Data.ByteString         as B
import           Data.ByteString.Builder
import           Data.ByteString.Lazy    as BL
import qualified Data.Foldable           as F
import           Data.HashMap.Strict     (HashMap)
import qualified Data.HashMap.Strict     as HashMap
import           Data.Int
import           Data.Ix                 (Ix)
import           Data.List               as DL
import qualified Data.Map.Strict         as Map
import           Data.Text               (Text)
import qualified Data.Text               as T
import qualified Data.Text.Encoding      as T
import qualified Data.Text.Lazy          as TL
import qualified Data.Text.Lazy.Encoding as TL
import qualified Data.Vector             as V
import qualified Data.Vector.Unboxed     as U
import           Data.Word

class ToEncoding a where
  toEncoding :: Schema -> a -> Builder

instance ToEncoding Int where
  toEncoding S.Long i = encodeRaw @Int64 (fromIntegral i)
  toEncoding S.Int i  = encodeRaw @Int32 (fromIntegral i)
  toEncoding s _      = error ("Unable to encode Int as: " <> show s)
  {-# INLINE toEncoding #-}

instance ToEncoding Int32 where
  toEncoding S.Long i = encodeRaw @Int64 (fromIntegral i)
  toEncoding S.Int i  = encodeRaw @Int32 i
  toEncoding s _      = error ("Unable to encode Int32 as: " <> show s)
  {-# INLINE toEncoding #-}

instance ToEncoding Int64 where
  toEncoding S.Long i = encodeRaw @Int64 i
  toEncoding s _      = error ("Unable to encode Int64 as: " <> show s)
  {-# INLINE toEncoding #-}

instance ToEncoding Word8 where
  toEncoding S.Int i  = encodeRaw @Word8 i
  toEncoding S.Long i = encodeRaw @Word64 (fromIntegral i)
  toEncoding s _      = error ("Unable to encode Word8 as: " <> show s)
  {-# INLINE toEncoding #-}

instance ToEncoding Word16 where
  toEncoding S.Int i  = encodeRaw @Word16 i
  toEncoding S.Long i = encodeRaw @Word64 (fromIntegral i)
  toEncoding s _      = error ("Unable to encode Word16 as: " <> show s)
  {-# INLINE toEncoding #-}

instance ToEncoding Word32 where
  toEncoding S.Int i  = encodeRaw @Word32 i
  toEncoding S.Long i = encodeRaw @Word64 (fromIntegral i)
  toEncoding s _      = error ("Unable to encode Word32 as: " <> show s)
  {-# INLINE toEncoding #-}

instance ToEncoding Word64 where
  toEncoding S.Long i = encodeRaw @Word64 i
  toEncoding s _      = error ("Unable to encode Word64 as: " <> show s)
  {-# INLINE toEncoding #-}

instance ToEncoding Double where
  toEncoding S.Double i = word64LE (IEEE.doubleToWord i)
  toEncoding s _        = error ("Unable to encode Double as: " <> show s)
  {-# INLINE toEncoding #-}

instance ToEncoding Float where
  toEncoding S.Float i  = word32LE (IEEE.floatToWord i)
  toEncoding S.Double i = word64LE (IEEE.doubleToWord $ realToFrac i)
  toEncoding s _        = error ("Unable to encode Float as: " <> show s)
  {-# INLINE toEncoding #-}

instance ToEncoding Bool where
  toEncoding S.Boolean v = word8 $ fromIntegral (fromEnum v)
  toEncoding s _         = error ("Unable to encode Bool as: " <> show s)
  {-# INLINE toEncoding #-}

instance ToEncoding B.ByteString where
  toEncoding s bs = case s of
    S.Bytes                          -> encodeRaw (B.length bs) <> byteString bs
    S.Fixed _ _ l | l == B.length bs -> byteString bs
    S.Fixed _ _ l                    -> error ("Unable to encode ByteString as Fixed(" <> show l <> ") because its length is " <> show (B.length bs))
    _                                -> error ("Unable to encode ByteString as: " <> show s)
  {-# INLINE toEncoding #-}

instance ToEncoding BL.ByteString where
  toEncoding s bs = toEncoding s (BL.toStrict bs)
  {-# INLINE toEncoding #-}

instance ToEncoding Text where
  toEncoding s v =
    let
      bs = T.encodeUtf8 v
      res = encodeRaw (B.length bs) <> byteString bs
    in case s of
      S.Bytes  -> res
      S.String -> res
      _        -> error ("Unable to encode Text as: " <> show s)
  {-# INLINE toEncoding #-}

instance ToEncoding TL.Text where
  toEncoding s v = toEncoding s (TL.toStrict v)
  {-# INLINE toEncoding #-}

instance ToEncoding a => ToEncoding [a] where
  toEncoding (S.Array s) as =
    if DL.null as then long0 else encodeRaw (F.length as) <> foldMap (toEncoding s) as <> long0
  toEncoding s _         = error ("Unable to encode Haskell list as: " <> show s)

instance ToEncoding a => ToEncoding (V.Vector a) where
  toEncoding (S.Array s) as =
    if V.null as then long0 else encodeRaw (V.length as) <> foldMap (toEncoding s) as <> long0
  toEncoding s _         = error ("Unable to encode Vector list as: " <> show s)

instance (Ix i, ToEncoding a) => ToEncoding (Ar.Array i a) where
  toEncoding (S.Array s) as =
    if F.length as == 0 then long0 else encodeRaw (F.length as) <> foldMap (toEncoding s) as <> long0
  toEncoding s _         = error ("Unable to encode indexed Array list as: " <> show s)

instance (U.Unbox a, ToEncoding a) => ToEncoding (U.Vector a) where
  toEncoding (S.Array s) as =
    if U.null as then long0 else encodeRaw (U.length as) <> foldMap (toEncoding s) (U.toList as) <> long0
  toEncoding s _         = error ("Unable to encode Vector list as: " <> show s)

instance ToEncoding a => ToEncoding (Map.Map Text a) where
  toEncoding (S.Map s) hm =
    if Map.null hm then long0 else putI (F.length hm) <> foldMap putKV (Map.toList hm) <> long0
    where putKV (k,v) = toEncoding S.String k <> toEncoding s v
  toEncoding s _         = error ("Unable to encode HashMap as: " <> show s)

instance ToEncoding a => ToEncoding (HashMap Text a) where
  toEncoding (S.Map s) hm =
    if HashMap.null hm then long0 else putI (F.length hm) <> foldMap putKV (HashMap.toList hm) <> long0
    where putKV (k,v) = toEncoding S.String k <> toEncoding s v
  toEncoding s _         = error ("Unable to encode HashMap as: " <> show s)

instance ToEncoding a => ToEncoding (Maybe a) where
  toEncoding (S.Union opts) v =
    case V.toList opts of
      [S.Null, s] -> maybe (putI 0) (\a -> putI 1 <> toEncoding s a) v
      wrongOpts   -> error ("Unable to encode Maybe as " <> show wrongOpts)
  toEncoding s _ = error ("Unable to encode Maybe as " <> show s)

instance (ToEncoding a, ToEncoding b) => ToEncoding (Either a b) where
  toEncoding (S.Union opts) v =
    case V.toList opts of
      [sa, sb] -> case v of
        Left a  -> putI 0 <> toEncoding sa a
        Right b -> putI 1 <> toEncoding sb b
      wrongOpts   -> error ("Unable to encode Either as " <> show wrongOpts)
  toEncoding s _ = error ("Unable to encode Either as " <> show s)


-- Put a Haskell Int.
putI :: Int -> Builder
putI = encodeRaw
{-# INLINE putI #-}

-- Terminating word for array and map types.
long0 :: Builder
long0 = encodeRaw (0 :: Word64)
{-# INLINE long0 #-}
