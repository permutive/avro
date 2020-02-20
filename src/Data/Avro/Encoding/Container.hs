module Data.Avro.Encoding.Container
where

import           Data.Avro.Encoding.FromEncoding (getValue)
import           Data.Avro.Schema                (Schema)
import qualified Data.Avro.Schema                as Schema
import           Data.Binary.Get                 (Get)
import qualified Data.Binary.Get                 as Get
import qualified Data.ByteString.Lazy            as BL

