use std::sync::Arc;

use spatter_core::{Dependency, RddId};

use crate::partitioner::HashPartitioner;

#[derive(Clone, Debug)]
pub struct LineageNode {
    pub id: RddId,
    pub deps: Arc<[Dependency]>,
    pub n_partitions: usize,
    pub parents: Arc<[Arc<LineageNode>]>,
    pub partitioner: Option<HashPartitioner>,
}
