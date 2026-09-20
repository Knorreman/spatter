//! Experimental Spark-style RDDs with local threads and same-binary clusters.
//!
//! ```
//! use spatter::prelude::*;
//!
//! fn main() -> Result<()> {
//!     let sc = SpatterContext::builder().master("local[2]").get_or_create()?;
//!     let mut counts = sc.parallelize(vec!["hello".to_owned(), "hello".to_owned(), "world".to_owned()])
//!         .map(|word| (word, 1usize))
//!         .reduce_by_key(|a, b| a + b)
//!         .collect()?;
//!     counts.sort();
//!     assert_eq!(counts, vec![("hello".to_owned(), 2), ("world".to_owned(), 1)]);
//!     Ok(())
//! }
//! ```
//!
//! Cluster ranks currently execute the same application main function.
//! Use [`Rdd::collect_to_driver`] for gathered results; cluster execution and
//! failure recovery are experimental. See the repository's execution notes
//! before using chained shuffles or side-effecting closures across ranks.

mod bootstrap;
mod cluster;
mod context;
mod dag;
mod exec;
mod lineage;
mod partitioner;
mod profile;
mod rdd;
mod shuffle;
mod source;

pub use bootstrap::{launch_mode, run_executor, LaunchMode};
pub use context::{ContextBuilder, SpatterContext};
pub use dag::{Stage, StageKind};
pub use partitioner::HashPartitioner;
pub use rdd::Rdd;
pub use spatter_core::{Dependency, Error, RddId, Result, ShuffleId};

pub mod prelude {
    pub use crate::{
        launch_mode, run_executor, Dependency, Error, HashPartitioner, LaunchMode, Rdd, RddId,
        Result, ShuffleId, SpatterContext, Stage, StageKind,
    };
}
