{-# LANGUAGE DeriveFoldable      #-}
{-# LANGUAGE DeriveFunctor       #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE DeriveTraversable   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Data.Avro.EitherN where

import           Data.Avro
import           Data.Avro.Decode.Lazy as AL
import           Data.Avro.Encode      (ToEncoding (..), putI)
import           Data.Avro.Schema      as S
import qualified Data.Avro.Types       as T
import           Data.Avro.Value       (FromValue (..))
import qualified Data.Avro.Value       as AV
import           Data.Bifoldable       (Bifoldable (..))
import           Data.Bifunctor        (Bifunctor (..))
import           Data.Bitraversable    (Bitraversable (..))
import           Data.List.NonEmpty
import           Data.Tagged
import qualified Data.Vector           as V
import           GHC.Generics          (Generic)

data Either3 a b c = E3_1 a | E3_2 b | E3_3 c deriving (Eq, Ord, Show, Generic, Functor, Foldable, Traversable)

data Either4 a b c d = E4_1 a | E4_2 b | E4_3 c | E4_4 d deriving (Eq, Ord, Show, Generic, Functor, Foldable, Traversable)

data Either5 a b c d e = E5_1 a | E5_2 b | E5_3 c | E5_4 d | E5_5 e deriving (Eq, Ord, Show, Generic, Functor, Foldable, Traversable)

instance Applicative (Either3 a b) where
  pure = E3_3
  E3_1 a <*> _ = E3_1 a
  E3_2 a <*> _ = E3_2 a
  E3_3 f <*> r = fmap f r

instance Applicative (Either4 a b c) where
  pure = E4_4
  E4_1 a <*> _ = E4_1 a
  E4_2 a <*> _ = E4_2 a
  E4_3 a <*> _ = E4_3 a
  E4_4 f <*> r = fmap f r

instance Applicative (Either5 a b c d) where
  pure = E5_5
  E5_1 a <*> _ = E5_1 a
  E5_2 a <*> _ = E5_2 a
  E5_3 a <*> _ = E5_3 a
  E5_4 a <*> _ = E5_4 a
  E5_5 f <*> r = fmap f r

instance Bifunctor (Either3 a) where
  bimap _ _ (E3_1 a) = E3_1 a
  bimap f _ (E3_2 a) = E3_2 (f a)
  bimap _ g (E3_3 a) = E3_3 (g a)

instance Bifunctor (Either4 a b) where
  bimap _ _ (E4_1 a) = E4_1 a
  bimap _ _ (E4_2 a) = E4_2 a
  bimap f _ (E4_3 a) = E4_3 (f a)
  bimap _ g (E4_4 a) = E4_4 (g a)

instance Bifunctor (Either5 a b c) where
  bimap _ _ (E5_1 a) = E5_1 a
  bimap _ _ (E5_2 a) = E5_2 a
  bimap _ _ (E5_3 a) = E5_3 a
  bimap f _ (E5_4 a) = E5_4 (f a)
  bimap _ g (E5_5 a) = E5_5 (g a)

instance Monad (Either3 a b) where
  E3_1 a >>= _ = E3_1 a
  E3_2 a >>= _ = E3_2 a
  E3_3 a >>= f = f a

instance Monad (Either4 a b c) where
  E4_1 a >>= _ = E4_1 a
  E4_2 a >>= _ = E4_2 a
  E4_3 a >>= _ = E4_3 a
  E4_4 a >>= f = f a

instance Monad (Either5 a b c d) where
  E5_1 a >>= _ = E5_1 a
  E5_2 a >>= _ = E5_2 a
  E5_3 a >>= _ = E5_3 a
  E5_4 a >>= _ = E5_4 a
  E5_5 a >>= f = f a

instance Bifoldable (Either3 a) where
  bifoldMap f _ (E3_2 a) = f a
  bifoldMap _ g (E3_3 a) = g a
  bifoldMap _ _ _        = mempty

instance Bifoldable (Either4 a b) where
  bifoldMap f _ (E4_3 a) = f a
  bifoldMap _ g (E4_4 a) = g a
  bifoldMap _ _ _        = mempty

instance Bifoldable (Either5 a b c) where
  bifoldMap f _ (E5_4 a) = f a
  bifoldMap _ g (E5_5 a) = g a
  bifoldMap _ _ _        = mempty

instance Bitraversable (Either3 a) where
  bitraverse _ _ (E3_1 a) = pure (E3_1 a)
  bitraverse f _ (E3_2 a) = E3_2 <$> f a
  bitraverse _ g (E3_3 a) = E3_3 <$> g a

instance Bitraversable (Either4 a b) where
  bitraverse _ _ (E4_1 a) = pure (E4_1 a)
  bitraverse _ _ (E4_2 a) = pure (E4_2 a)
  bitraverse f _ (E4_3 a) = E4_3 <$> f a
  bitraverse _ g (E4_4 a) = E4_4 <$> g a

instance Bitraversable (Either5 a b c) where
  bitraverse _ _ (E5_1 a) = pure (E5_1 a)
  bitraverse _ _ (E5_2 a) = pure (E5_2 a)
  bitraverse _ _ (E5_3 a) = pure (E5_3 a)
  bitraverse f _ (E5_4 a) = E5_4 <$> f a
  bitraverse _ g (E5_5 a) = E5_5 <$> g a

instance (HasAvroSchema a, HasAvroSchema b, HasAvroSchema c) => HasAvroSchema (Either3 a b c) where
  schema = Tagged $ mkUnion (untag (schema :: Tagged a Schema) :| [
                             untag (schema :: Tagged b Schema),
                             untag (schema :: Tagged c Schema)
                            ])

instance (HasAvroSchema a, HasAvroSchema b, HasAvroSchema c, HasAvroSchema d) => HasAvroSchema (Either4 a b c d) where
  schema = Tagged $ mkUnion (untag (schema :: Tagged a Schema) :| [
                             untag (schema :: Tagged b Schema),
                             untag (schema :: Tagged c Schema),
                             untag (schema :: Tagged d Schema)
                            ])

instance (HasAvroSchema a, HasAvroSchema b, HasAvroSchema c, HasAvroSchema d, HasAvroSchema e) => HasAvroSchema (Either5 a b c d e) where
  schema = Tagged $ mkUnion (untag (schema :: Tagged a Schema) :| [
                             untag (schema :: Tagged b Schema),
                             untag (schema :: Tagged c Schema),
                             untag (schema :: Tagged d Schema),
                             untag (schema :: Tagged e Schema)
                            ])

instance (FromAvro a, FromAvro b, FromAvro c) => FromAvro (Either3 a b c) where
  fromAvro e@(T.Union _ branch x)
    | matches branch schemaA = E3_1 <$> fromAvro x
    | matches branch schemaB = E3_2 <$> fromAvro x
    | matches branch schemaC = E3_3 <$> fromAvro x
    | otherwise              = badValue e "Either3"
    where Tagged schemaA = schema :: Tagged a Schema
          Tagged schemaB = schema :: Tagged b Schema
          Tagged schemaC = schema :: Tagged c Schema
  fromAvro x = badValue x "Either3"

instance (FromAvro a, FromAvro b, FromAvro c, FromAvro d) => FromAvro (Either4 a b c d) where
  fromAvro e@(T.Union _ branch x)
    | matches branch schemaA = E4_1 <$> fromAvro x
    | matches branch schemaB = E4_2 <$> fromAvro x
    | matches branch schemaC = E4_3 <$> fromAvro x
    | matches branch schemaD = E4_4 <$> fromAvro x
    | otherwise              = badValue e "Either4"
    where Tagged schemaA = schema :: Tagged a Schema
          Tagged schemaB = schema :: Tagged b Schema
          Tagged schemaC = schema :: Tagged c Schema
          Tagged schemaD = schema :: Tagged d Schema
  fromAvro x = badValue x "Either4"

instance (FromAvro a, FromAvro b, FromAvro c, FromAvro d, FromAvro e) => FromAvro (Either5 a b c d e) where
  fromAvro e@(T.Union _ branch x)
    | matches branch schemaA = E5_1 <$> fromAvro x
    | matches branch schemaB = E5_2 <$> fromAvro x
    | matches branch schemaC = E5_3 <$> fromAvro x
    | matches branch schemaD = E5_4 <$> fromAvro x
    | matches branch schemaE = E5_5 <$> fromAvro x
    | otherwise              = badValue e "Either5"
    where Tagged schemaA = schema :: Tagged a Schema
          Tagged schemaB = schema :: Tagged b Schema
          Tagged schemaC = schema :: Tagged c Schema
          Tagged schemaD = schema :: Tagged d Schema
          Tagged schemaE = schema :: Tagged e Schema
  fromAvro x = badValue x "Either5"

instance (FromLazyAvro a, FromLazyAvro b, FromLazyAvro c) => FromLazyAvro (Either3 a b c) where
  fromLazyAvro e@(AL.Union _ branch x)
    | matches branch schemaA = E3_1 <$> fromLazyAvro x
    | matches branch schemaB = E3_2 <$> fromLazyAvro x
    | matches branch schemaC = E3_3 <$> fromLazyAvro x
    | otherwise              = badValue e "Either3"
    where Tagged schemaA = schema :: Tagged a Schema
          Tagged schemaB = schema :: Tagged b Schema
          Tagged schemaC = schema :: Tagged c Schema
  fromLazyAvro x = badValue x "Either3"

instance (FromLazyAvro a, FromLazyAvro b, FromLazyAvro c, FromLazyAvro d) => FromLazyAvro (Either4 a b c d) where
  fromLazyAvro e@(AL.Union _ branch x)
    | matches branch schemaA = E4_1 <$> fromLazyAvro x
    | matches branch schemaB = E4_2 <$> fromLazyAvro x
    | matches branch schemaC = E4_3 <$> fromLazyAvro x
    | matches branch schemaD = E4_4 <$> fromLazyAvro x
    | otherwise              = badValue e "Either4"
    where Tagged schemaA = schema :: Tagged a Schema
          Tagged schemaB = schema :: Tagged b Schema
          Tagged schemaC = schema :: Tagged c Schema
          Tagged schemaD = schema :: Tagged d Schema
  fromLazyAvro x = badValue x "Either4"

instance (FromLazyAvro a, FromLazyAvro b, FromLazyAvro c, FromLazyAvro d, FromLazyAvro e) => FromLazyAvro (Either5 a b c d e) where
  fromLazyAvro e@(AL.Union _ branch x)
    | matches branch schemaA = E5_1 <$> fromLazyAvro x
    | matches branch schemaB = E5_2 <$> fromLazyAvro x
    | matches branch schemaC = E5_3 <$> fromLazyAvro x
    | matches branch schemaD = E5_4 <$> fromLazyAvro x
    | matches branch schemaE = E5_5 <$> fromLazyAvro x
    | otherwise              = badValue e "Either5"
    where Tagged schemaA = schema :: Tagged a Schema
          Tagged schemaB = schema :: Tagged b Schema
          Tagged schemaC = schema :: Tagged c Schema
          Tagged schemaD = schema :: Tagged d Schema
          Tagged schemaE = schema :: Tagged e Schema
  fromLazyAvro x = badValue x "Either5"

instance (ToAvro a, ToAvro b, ToAvro c) => ToAvro (Either3 a b c) where
  toAvro e =
    let sch = options (schemaOf e)
    in case e of
      E3_1 a -> T.Union sch (schemaOf a) (toAvro a)
      E3_2 b -> T.Union sch (schemaOf b) (toAvro b)
      E3_3 c -> T.Union sch (schemaOf c) (toAvro c)

instance (ToAvro a, ToAvro b, ToAvro c, ToAvro d) => ToAvro (Either4 a b c d) where
  toAvro e =
    let sch = options (schemaOf e)
    in case e of
      E4_1 a -> T.Union sch (schemaOf a) (toAvro a)
      E4_2 b -> T.Union sch (schemaOf b) (toAvro b)
      E4_3 c -> T.Union sch (schemaOf c) (toAvro c)
      E4_4 d -> T.Union sch (schemaOf d) (toAvro d)

instance (ToAvro a, ToAvro b, ToAvro c, ToAvro d, ToAvro e) => ToAvro (Either5 a b c d e) where
  toAvro e =
    let sch = options (schemaOf e)
    in case e of
      E5_1 a -> T.Union sch (schemaOf a) (toAvro a)
      E5_2 b -> T.Union sch (schemaOf b) (toAvro b)
      E5_3 c -> T.Union sch (schemaOf c) (toAvro c)
      E5_4 d -> T.Union sch (schemaOf d) (toAvro d)
      E5_5 e -> T.Union sch (schemaOf e) (toAvro e)

------------ DATA.AVRO.VALUE --------------------------------
instance (FromValue a, FromValue b, FromValue c) => FromValue (Either3 a b c) where
  fromValue (AV.Union 0 a) = E3_1 <$> fromValue a
  fromValue (AV.Union 1 b) = E3_2 <$> fromValue b
  fromValue (AV.Union 2 c) = E3_3 <$> fromValue c
  fromValue (AV.Union n _) = Left ("Unable to decode Either3 from a position #" <> show n)

instance (FromValue a, FromValue b, FromValue c, FromValue d) => FromValue (Either4 a b c d) where
  fromValue (AV.Union 0 a) = E4_1 <$> fromValue a
  fromValue (AV.Union 1 b) = E4_2 <$> fromValue b
  fromValue (AV.Union 2 c) = E4_3 <$> fromValue c
  fromValue (AV.Union 3 d) = E4_4 <$> fromValue d
  fromValue (AV.Union n _) = Left ("Unable to decode Either4 from a position #" <> show n)

instance (FromValue a, FromValue b, FromValue c, FromValue d, FromValue e) => FromValue (Either5 a b c d e) where
  fromValue (AV.Union 0 a) = E5_1 <$> fromValue a
  fromValue (AV.Union 1 b) = E5_2 <$> fromValue b
  fromValue (AV.Union 2 c) = E5_3 <$> fromValue c
  fromValue (AV.Union 3 d) = E5_4 <$> fromValue d
  fromValue (AV.Union 4 e) = E5_5 <$> fromValue e
  fromValue (AV.Union n _) = Left ("Unable to decode Either5 from a position #" <> show n)

instance (ToEncoding a, ToEncoding b, ToEncoding c) => ToEncoding (Either3 a b c) where
  toEncoding (S.Union opts) v =
    case V.toList opts of
      [sa, sb, sc] -> case v of
        E3_1 a -> putI 0 <> toEncoding sa a
        E3_2 b -> putI 1 <> toEncoding sb b
        E3_3 c -> putI 2 <> toEncoding sc c
      wrongOpts   -> error ("Unable to encode Either3 as " <> show wrongOpts)
  toEncoding s _ = error ("Unable to encode Either3 as " <> show s)

instance (ToEncoding a, ToEncoding b, ToEncoding c, ToEncoding d) => ToEncoding (Either4 a b c d) where
  toEncoding (S.Union opts) v =
    case V.toList opts of
      [sa, sb, sc, sd] -> case v of
        E4_1 a -> putI 0 <> toEncoding sa a
        E4_2 b -> putI 1 <> toEncoding sb b
        E4_3 c -> putI 2 <> toEncoding sc c
        E4_4 d -> putI 3 <> toEncoding sd d
      wrongOpts   -> error ("Unable to encode Either4 as " <> show wrongOpts)
  toEncoding s _ = error ("Unable to encode Either4 as " <> show s)

instance (ToEncoding a, ToEncoding b, ToEncoding c, ToEncoding d, ToEncoding e) => ToEncoding (Either5 a b c d e) where
  toEncoding (S.Union opts) v =
    case V.toList opts of
      [sa, sb, sc, sd, se] -> case v of
        E5_1 a -> putI 0 <> toEncoding sa a
        E5_2 b -> putI 1 <> toEncoding sb b
        E5_3 c -> putI 2 <> toEncoding sc c
        E5_4 d -> putI 3 <> toEncoding sd d
        E5_5 e -> putI 4 <> toEncoding se e
      wrongOpts   -> error ("Unable to encode Either5 as " <> show wrongOpts)
  toEncoding s _ = error ("Unable to encode Either5 as " <> show s)
