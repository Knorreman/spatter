use std::collections::HashSet;
use std::sync::Arc;

use spatter_core::{RddId, Result, ShuffleId};

use crate::exec::ActionCtx;
use crate::lineage::LineageNode;
use crate::rdd::Rdd;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StageKind {
    ShuffleMap { shuffle: ShuffleId },
    Result,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Stage {
    pub id: usize,
    pub rdd_id: RddId,
    pub n_partitions: usize,
    pub kind: StageKind,
}

pub fn cut_stages(root: &LineageNode) -> Vec<Stage> {
    let mut stages = Vec::new();
    let mut seen_shuffle = HashSet::new();
    let mut seen_nodes = HashSet::new();
    walk(root, &mut stages, &mut seen_shuffle, &mut seen_nodes);
    stages.push(Stage {
        id: stages.len(),
        rdd_id: root.id,
        n_partitions: root.n_partitions,
        kind: StageKind::Result,
    });
    stages
}

fn walk(
    node: &LineageNode,
    stages: &mut Vec<Stage>,
    seen_shuffle: &mut HashSet<ShuffleId>,
    seen_nodes: &mut HashSet<RddId>,
) {
    if !seen_nodes.insert(node.id) {
        return;
    }
    for parent in node.parents.iter() {
        walk(parent, stages, seen_shuffle, seen_nodes);
    }
    for dep in node.deps.iter() {
        if let spatter_core::Dependency::Shuffle { parent, shuffle } = *dep {
            if seen_shuffle.insert(shuffle) {
                let n = node
                    .parents
                    .iter()
                    .find(|p| p.id == parent)
                    .map(|p| p.n_partitions)
                    .unwrap_or(node.n_partitions);
                stages.push(Stage {
                    id: stages.len(),
                    rdd_id: parent,
                    n_partitions: n,
                    kind: StageKind::ShuffleMap { shuffle },
                });
            }
        }
    }
}

pub fn run_job<T, R, F>(rdd: &Rdd<T>, f: F) -> Result<R>
where
    T: Send + 'static,
    F: FnOnce(Arc<ActionCtx>) -> Result<R>,
{
    let _stages = cut_stages(rdd.lineage());
    let action = ActionCtx::create(rdd.cluster());
    rdd.prepare(&action)?;
    f(action)
}
