{-# LANGUAGE DeriveAnyClass    #-}
{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE StrictData        #-}

module Data.Avro.Encoding.Value where

import Control.DeepSeq     (NFData)
import Data.HashMap.Strict (HashMap)
import Data.Int
import Data.List.NonEmpty  (NonEmpty)
import Data.Text
import Data.Vector         (Vector)
import GHC.Generics        (Generic)

import qualified Data.ByteString      as BS
import qualified Data.ByteString.Lazy as BL
import qualified Data.HashMap.Strict  as HashMap
import qualified Data.Map             as Map
import qualified Data.Text            as Text
import qualified Data.Text.Encoding   as Text
import qualified Data.Vector          as V

data Value
      = Null
      | Boolean Bool
      | Int     {-# UNPACK #-} Int32
      | Long    {-# UNPACK #-} Int64
      | Float   {-# UNPACK #-} Float
      | Double  {-# UNPACK #-} Double
      | Bytes   {-# UNPACK #-} BS.ByteString
      | String  {-# UNPACK #-} Text
      | Array   (Vector Value)
      | Map     (HashMap Text Value)
      | Record  (Vector Value)
      | Union   {-# UNPACK #-} Int Value
      | Fixed   {-# UNPACK #-} BS.ByteString
      | Enum    {-# UNPACK #-} Int {-# UNPACK #-} Text
  deriving (Eq, Show, Generic, NFData)

--------------------------------------------------------------------------
class FromValue a where
  fromValue :: Value -> Either String a

instance FromValue Int where
  fromValue (Int x)  = Right (fromIntegral x)
  fromValue (Long x) = Right (fromIntegral x)
  fromValue x        = Left ("Unable to use as Int: " <> show x)
  {-# INLINE fromValue #-}

instance FromValue Int32 where
  fromValue (Int x) = Right x
  fromValue x       = Left ("Unable to use as Int32: " <> show x)
  {-# INLINE fromValue #-}

instance FromValue Int64 where
  fromValue (Long x) = Right x
  fromValue (Int x)  = Right (fromIntegral x)
  fromValue x        = Left ("Unable to use as Int64: " <> show x)
  {-# INLINE fromValue #-}

instance FromValue Double where
  fromValue (Double x) = Right x
  fromValue (Float x)  = Right (realToFrac x)
  fromValue (Long x)   = Right (fromIntegral x)
  fromValue (Int x)    = Right (fromIntegral x)
  fromValue x          = Left ("Unable to use as Double: " <> show x)
  {-# INLINE fromValue #-}

instance FromValue Float where
  fromValue (Float x) = Right x
  fromValue (Long x)  = Right (fromIntegral x)
  fromValue (Int x)   = Right (fromIntegral x)
  fromValue x         = Left ("Unable to use as Double: " <> show x)
  {-# INLINE fromValue #-}

instance FromValue Bool where
  fromValue (Boolean x) = Right x
  fromValue x           = Left ("Unable to use as Bool: " <> show x)
  {-# INLINE fromValue #-}

instance FromValue Text where
  fromValue (String x) = Right x
  fromValue (Bytes x) = case Text.decodeUtf8' x of
    Left unicodeExc -> Left (show unicodeExc)
    Right text      -> Right text
  fromValue x          = Left ("Unable to use as Text: " <> show x)
  {-# INLINE fromValue #-}

instance FromValue BS.ByteString where
  fromValue (Bytes bs) = Right bs
  fromValue (String x) = Right (Text.encodeUtf8 x)
  fromValue x          = Left ("Unable to use as Bytes: " <> show x)

instance FromValue BL.ByteString where
  fromValue (Bytes bs) = Right (BL.fromStrict bs)
  fromValue (String x) = Right (BL.fromStrict $ Text.encodeUtf8 x)
  fromValue x          = Left ("Unable to use as Bytes: " <> show x)

instance FromValue a => FromValue [a] where
  fromValue (Array vec) = mapM fromValue $ V.toList vec
  fromValue x           = Left ("Expected Array, but got: " <> show x)
  {-# INLINE fromValue #-}

instance FromValue a => FromValue (Maybe a) where
  fromValue (Union 0 Null) = Right Nothing
  fromValue (Union 1 v)    = Just <$> fromValue v

instance (FromValue a, FromValue b) => FromValue (Either a b) where
  fromValue (Union 0 a) = Left <$> fromValue a
  fromValue (Union 1 b) = Right <$> fromValue b
  fromValue (Union n _) = Left ("Unable to decode union value with a position #" <> show n)

instance FromValue a => FromValue (Map.Map Text a) where
  fromValue (Map mp) = traverse fromValue (Map.fromList (HashMap.toList mp))
  fromValue x        = Left ("Expected Map, but got: " <> show x)

instance FromValue a => FromValue (HashMap.HashMap Text a) where
  fromValue (Map mp) = traverse fromValue mp
  fromValue x        = Left ("Expected Map, but got: " <> show x)
