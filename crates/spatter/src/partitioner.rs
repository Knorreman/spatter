use std::hash::Hash;

use crate::shuffle::hash_bucket;
use spatter_core::{Error, Result};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HashPartitioner {
    n: usize,
}

impl HashPartitioner {
    pub fn new(n: usize) -> Result<Self> {
        if n == 0 {
            return Err(Error::InvalidParallelism(0));
        }
        Ok(Self { n })
    }

    pub fn num_partitions(self) -> usize {
        self.n
    }

    pub fn get_partition<K: Hash>(self, key: &K) -> usize {
        hash_bucket(key, self.n)
    }
}
