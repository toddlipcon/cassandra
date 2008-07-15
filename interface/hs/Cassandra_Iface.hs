module Cassandra_Iface where
import Thrift
import Data.Generics
import Control.Exception
import qualified Data.Map as Map
import qualified Data.Set as Set
import Data.Int
import Cassandra_Types

import FacebookService_Iface
class FacebookService_Iface a => Cassandra_Iface a where
  get_slice :: a -> Maybe ([Char]) -> Maybe ([Char]) -> Maybe ([Char]) -> Maybe (Int) -> Maybe (Int) -> IO ([Column_t])
  get_column :: a -> Maybe ([Char]) -> Maybe ([Char]) -> Maybe ([Char]) -> IO (Column_t)
  get_column_count :: a -> Maybe ([Char]) -> Maybe ([Char]) -> Maybe ([Char]) -> IO (Int)
  insert :: a -> Maybe ([Char]) -> Maybe ([Char]) -> Maybe ([Char]) -> Maybe ([Char]) -> Maybe (Int) -> IO (())
  batch_insert :: a -> Maybe (Batch_mutation_t) -> IO (())
  batch_insert_blocking :: a -> Maybe (Batch_mutation_t) -> IO (Bool)
  remove :: a -> Maybe ([Char]) -> Maybe ([Char]) -> Maybe ([Char]) -> IO (())
  get_slice_super :: a -> Maybe ([Char]) -> Maybe ([Char]) -> Maybe ([Char]) -> Maybe (Int) -> Maybe (Int) -> IO ([SuperColumn_t])
  get_superColumn :: a -> Maybe ([Char]) -> Maybe ([Char]) -> Maybe ([Char]) -> IO (SuperColumn_t)
  batch_insert_superColumn :: a -> Maybe (Batch_mutation_super_t) -> IO (())
  batch_insert_superColumn_blocking :: a -> Maybe (Batch_mutation_super_t) -> IO (Bool)
