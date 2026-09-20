mod error;
mod ids;
mod partition;

pub use error::{Error, Result};
pub use ids::{Dependency, RddId, ShuffleId};
pub use partition::{default_parallelism, parse_local_threads, split_contiguous};
