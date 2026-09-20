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
