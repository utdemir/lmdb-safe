{-# LANGUAGE ConstraintKinds      #-}
{-# LANGUAGE DataKinds            #-}
{-# LANGUAGE KindSignatures       #-}
{-# LANGUAGE TypeFamilies         #-}
{-# LANGUAGE TypeOperators        #-}
{-# LANGUAGE UndecidableInstances #-}

module Type.And where

--------------------------------------------------------------------------------
import           Data.Kind
import           Data.Proxy
import           Data.Type.Bool
import           Data.Type.Equality
import           GHC.TypeLits
--------------------------------------------------------------------------------

data a :& b = a :& b
infixr 6 :&

--------------------------------------------------------------------------------

type family NotContains a b :: Constraint where
  NotContains a (b :& c) = (NotContains a b, NotContains a c)
  NotContains (a :& b) c = (NotContains a b, NotContains a c)
  NotContains a b        = (a == b) ~ False

type family IsUnique (a :: *) :: Constraint where
  IsUnique (a :& b) = (NotContains a b, IsUnique b)
  IsUnique a        = ()

type family Length (a :: *) :: Nat where
  Length (a :& b) = Length a + Length b
  Length a        = 1

--------------------------------------------------------------------------------

requiresUnique :: IsUnique a => Proxy a -> ()
requiresUnique _ = ()

tests = requiresUnique (Proxy :: Proxy ((Proxy "utku") :& (Proxy "demir")))

