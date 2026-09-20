use std::hash::Hash;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use serde::de::DeserializeOwned;
use serde::Serialize;
use spatter_core::{Dependency, Error, RddId, Result};

use crate::context::ContextInner;
use crate::dag::{self, Stage};
use crate::exec::{catch_compute_result, catch_driver, run_partitions, ActionCtx};
use crate::lineage::LineageNode;
use crate::partitioner::HashPartitioner;
use crate::shuffle::{
    parallel_reduce_buckets, store_blocks, write_map_combine, IncrementalBuckets, ShuffleBlocks,
};

pub(crate) type PartIter<T> = Box<dyn Iterator<Item = T> + Send>;
pub(crate) type ComputeFn<T> = Arc<dyn Fn(usize, &ActionCtx) -> Result<PartIter<T>> + Send + Sync>;
pub(crate) type PrepFn = Arc<dyn Fn(&ActionCtx) -> Result<()> + Send + Sync>;

pub struct Rdd<T> {
    ctx: Arc<ContextInner>,
    id: RddId,
    deps: Arc<[Dependency]>,
    n_partitions: usize,
    compute: ComputeFn<T>,
    prep: PrepFn,
    lineage: Arc<LineageNode>,
}

impl<T> Clone for Rdd<T> {
    fn clone(&self) -> Self {
        Self {
            ctx: Arc::clone(&self.ctx),
            id: self.id,
            deps: Arc::clone(&self.deps),
            n_partitions: self.n_partitions,
            compute: Arc::clone(&self.compute),
            prep: Arc::clone(&self.prep),
            lineage: Arc::clone(&self.lineage),
        }
    }
}

impl<T> Rdd<T>
where
    T: Send + 'static,
{
    pub(crate) fn new<F>(
        ctx: Arc<ContextInner>,
        n_partitions: usize,
        deps: Vec<Dependency>,
        parents: Vec<Arc<LineageNode>>,
        partitioner: Option<HashPartitioner>,
        prep: PrepFn,
        compute: F,
    ) -> Self
    where
        F: Fn(usize, &ActionCtx) -> Result<PartIter<T>> + Send + Sync + 'static,
    {
        let id = ctx.alloc_id();
        let deps: Arc<[Dependency]> = deps.into();
        let lineage = Arc::new(LineageNode {
            id,
            deps: Arc::clone(&deps),
            n_partitions,
            parents: parents.into(),
            partitioner,
        });
        let inner_prep = prep;
        let prep: PrepFn = Arc::new(move |ctx| ctx.prep_once(id, || inner_prep(ctx)));
        Self {
            ctx,
            id,
            deps,
            n_partitions,
            compute: Arc::new(compute),
            prep,
            lineage,
        }
    }

    pub fn id(&self) -> RddId {
        self.id
    }

    pub fn dependencies(&self) -> &[Dependency] {
        &self.deps
    }

    pub fn get_num_partitions(&self) -> usize {
        self.n_partitions
    }

    pub fn partitioner(&self) -> Option<HashPartitioner> {
        self.lineage.partitioner
    }

    pub fn stages(&self) -> Vec<Stage> {
        dag::cut_stages(&self.lineage)
    }

    pub(crate) fn lineage(&self) -> &LineageNode {
        &self.lineage
    }

    pub(crate) fn cluster(&self) -> Option<Arc<crate::cluster::Cluster>> {
        self.ctx.cluster.clone()
    }

    pub(crate) fn prepare(&self, ctx: &ActionCtx) -> Result<()> {
        (self.prep)(ctx)
    }

    pub fn map<U, F>(self, f: F) -> Rdd<U>
    where
        U: Send + 'static,
        F: Fn(T) -> U + Send + Sync + 'static,
    {
        let prev = self.compute;
        let f = Arc::new(f);
        let parent = Arc::clone(&self.lineage);
        Rdd::new(
            self.ctx,
            self.n_partitions,
            vec![Dependency::Narrow { parent: self.id }],
            vec![parent],
            None,
            Arc::clone(&self.prep),
            move |p, ctx| {
                let f = Arc::clone(&f);
                Ok(Box::new(prev(p, ctx)?.map(move |x| f.as_ref()(x))) as PartIter<U>)
            },
        )
    }

    pub fn filter<F>(self, pred: F) -> Rdd<T>
    where
        F: Fn(&T) -> bool + Send + Sync + 'static,
    {
        let prev = self.compute;
        let pred = Arc::new(pred);
        let parent = Arc::clone(&self.lineage);
        Rdd::new(
            self.ctx,
            self.n_partitions,
            vec![Dependency::Narrow { parent: self.id }],
            vec![parent],
            None,
            Arc::clone(&self.prep),
            move |p, ctx| {
                let pred = Arc::clone(&pred);
                Ok(Box::new(prev(p, ctx)?.filter(move |x| pred.as_ref()(x))) as PartIter<T>)
            },
        )
    }

    pub fn flat_map<U, I, F>(self, f: F) -> Rdd<U>
    where
        U: Send + 'static,
        I: IntoIterator<Item = U> + 'static,
        I::IntoIter: Send,
        F: Fn(T) -> I + Send + Sync + 'static,
    {
        let prev = self.compute;
        let f = Arc::new(f);
        let parent = Arc::clone(&self.lineage);
        Rdd::new(
            self.ctx,
            self.n_partitions,
            vec![Dependency::Narrow { parent: self.id }],
            vec![parent],
            None,
            Arc::clone(&self.prep),
            move |p, ctx| {
                let f = Arc::clone(&f);
                Ok(Box::new(prev(p, ctx)?.flat_map(move |x| f.as_ref()(x))) as PartIter<U>)
            },
        )
    }

    pub fn union(self, other: Rdd<T>) -> Rdd<T> {
        assert!(
            Arc::ptr_eq(&self.ctx, &other.ctx),
            "union requires RDDs from the same SpatterContext"
        );
        let n1 = self.n_partitions;
        let left = self.compute;
        let right = other.compute;
        let left_id = self.id;
        let right_id = other.id;
        let left_prep = Arc::clone(&self.prep);
        let right_prep = Arc::clone(&other.prep);
        let prep: PrepFn = Arc::new(move |ctx| {
            left_prep(ctx)?;
            right_prep(ctx)
        });
        Rdd::new(
            self.ctx,
            n1 + other.n_partitions,
            vec![
                Dependency::Narrow { parent: left_id },
                Dependency::Narrow { parent: right_id },
            ],
            vec![self.lineage, other.lineage],
            None,
            prep,
            move |p, ctx| {
                if p < n1 {
                    left(p, ctx)
                } else {
                    right(p - n1, ctx)
                }
            },
        )
    }

    pub fn distinct(self) -> Rdd<T>
    where
        T: Clone + Eq + Hash + Serialize + DeserializeOwned + Send + Sync + 'static,
    {
        self.map(|x| (x, ()))
            .reduce_by_key(|_, _| ())
            .map(|(x, _)| x)
    }

    pub fn collect(&self) -> Result<Vec<T>> {
        dag::run_job(self, |action| {
            let compute = Arc::clone(&self.compute);
            let parts = run_partitions(
                self.n_partitions,
                self.ctx.parallelism,
                &action,
                move |p, ctx| {
                    if !ctx.owns(p) {
                        return Ok(Vec::new());
                    }
                    drain(compute(p, ctx)?, ctx)
                },
            )?;
            Ok(parts.into_iter().flatten().collect())
        })
    }

    pub fn collect_to_driver(&self) -> Result<Vec<T>>
    where
        T: Serialize + DeserializeOwned + Send + 'static,
    {
        let local = self.collect()?;
        match self.cluster() {
            Some(c) => c.gather(local),
            None => Ok(local),
        }
    }

    pub fn count(&self) -> Result<usize> {
        dag::run_job(self, |action| {
            let compute = Arc::clone(&self.compute);
            let sizes = run_partitions(
                self.n_partitions,
                self.ctx.parallelism,
                &action,
                move |p, ctx| {
                    if !ctx.owns(p) {
                        return Ok(0);
                    }
                    let mut n = 0usize;
                    for _ in compute(p, ctx)? {
                        if ctx.cancel.load(Ordering::Relaxed) {
                            return Err(Error::Cancelled);
                        }
                        n = n.checked_add(1).ok_or(Error::CountOverflow)?;
                    }
                    Ok(n)
                },
            )?;
            sizes
                .into_iter()
                .try_fold(0usize, |a, b| a.checked_add(b).ok_or(Error::CountOverflow))
        })
    }

    pub fn take(&self, n: usize) -> Result<Vec<T>> {
        if n == 0 {
            return Ok(Vec::new());
        }
        dag::run_job(self, |action| {
            if action.cluster.as_ref().is_some_and(|c| !c.is_driver()) {
                return Ok(Vec::new());
            }
            let mut out = Vec::new();
            for p in 0..self.n_partitions {
                let remaining = n - out.len();
                let compute = Arc::clone(&self.compute);
                let action = Arc::clone(&action);
                let chunk = catch_compute_result(p, move || {
                    let iter = compute(p, &action)?;
                    Ok(iter.take(remaining).collect::<Vec<T>>())
                })?;
                out.extend(chunk);
                if out.len() == n {
                    return Ok(out);
                }
            }
            Ok(out)
        })
    }

    pub fn reduce<F>(&self, f: F) -> Result<T>
    where
        F: Fn(T, T) -> T + Send + Sync + 'static,
    {
        dag::run_job(self, |action| {
            let compute = Arc::clone(&self.compute);
            let fold = Arc::new(f);
            let worker_fold = Arc::clone(&fold);
            let partials = run_partitions(
                self.n_partitions,
                self.ctx.parallelism,
                &action,
                move |p, ctx| {
                    if !ctx.owns(p) {
                        return Ok(None);
                    }
                    let mut iter = compute(p, ctx)?;
                    let Some(first) = iter.next() else {
                        return Ok(None);
                    };
                    let mut acc = first;
                    for x in iter {
                        if ctx.cancel.load(Ordering::Relaxed) {
                            return Err(Error::Cancelled);
                        }
                        acc = worker_fold.as_ref()(acc, x);
                    }
                    Ok(Some(acc))
                },
            )?;
            catch_driver(move || {
                let mut iter = partials.into_iter().flatten();
                let first = iter.next().ok_or(Error::EmptyRdd)?;
                Ok(iter.fold(first, |a, b| fold.as_ref()(a, b)))
            })
        })
    }
}

impl<K, V> Rdd<(K, V)>
where
    K: Clone + Eq + Hash + Serialize + DeserializeOwned + Send + Sync + 'static,
    V: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub fn combine_by_key<C, Create, MergeV, MergeC>(
        self,
        create: Create,
        merge_value: MergeV,
        merge_combiners: MergeC,
    ) -> Rdd<(K, C)>
    where
        C: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
        Create: Fn(V) -> C + Send + Sync + 'static,
        MergeV: Fn(C, V) -> C + Send + Sync + 'static,
        MergeC: Fn(C, C) -> C + Send + Sync + 'static,
    {
        let n_out = self.n_partitions;
        let parallelism = self.ctx.parallelism;
        let parent_n = self.n_partitions;
        let parent_compute = self.compute;
        let create = Arc::new(create);
        let merge_value = Arc::new(merge_value);
        let merge_combiners = Arc::new(merge_combiners);
        let shuffle = self.ctx.alloc_shuffle();
        let parent = Arc::clone(&self.lineage);
        let partitioner = HashPartitioner::new(n_out).ok();
        let write = {
            let create = Arc::clone(&create);
            let merge_value = Arc::clone(&merge_value);
            let merge_combiners = Arc::clone(&merge_combiners);
            let parent_compute = Arc::clone(&parent_compute);
            Arc::new(move |ctx: &ActionCtx| {
                let create = Arc::clone(&create);
                let merge_value = Arc::clone(&merge_value);
                let merge_combiners = Arc::clone(&merge_combiners);
                let parent_compute = Arc::clone(&parent_compute);
                let acc = std::sync::Mutex::new(IncrementalBuckets::new(n_out, shuffle.0));
                if let Some(cluster) = ctx.cluster.clone() {
                    cluster.dispatch_each(
                        parent_n,
                        |mp| {
                            write_map_combine(
                                n_out,
                                parent_compute(mp, ctx)?,
                                create.as_ref(),
                                merge_value.as_ref(),
                                &ctx.cancel,
                            )
                        },
                        |buckets| acc.lock().expect("spill acc").push(buckets),
                    )?;
                } else {
                    run_partitions(parent_n, parallelism, ctx, |mp, ctx| {
                        let buckets = write_map_combine(
                            n_out,
                            parent_compute(mp, ctx)?,
                            create.as_ref(),
                            merge_value.as_ref(),
                            &ctx.cancel,
                        )?;
                        acc.lock().expect("spill acc").push(buckets)
                    })?;
                }
                let mut blocks = acc.into_inner().expect("spill acc").finish()?;
                match &mut blocks {
                    ShuffleBlocks::Memory(merged) => {
                        let merged = std::mem::take(merged);
                        let reduced =
                            parallel_reduce_buckets(merged, merge_combiners.as_ref(), parallelism);
                        store_blocks(shuffle.0, reduced)
                    }
                    ShuffleBlocks::Spilled(paths) => {
                        let paths = std::mem::take(paths);
                        struct SpillGuard(Vec<std::path::PathBuf>);
                        impl Drop for SpillGuard {
                            fn drop(&mut self) {
                                for p in &self.0 {
                                    let _ = std::fs::remove_file(p);
                                }
                            }
                        }
                        let guard = SpillGuard(paths);
                        let mut reduced = Vec::with_capacity(n_out);
                        for path in &guard.0 {
                            reduced.push(crate::shuffle::reduce_spilled_path(
                                path,
                                merge_combiners.as_ref(),
                            )?);
                        }
                        store_blocks(shuffle.0, reduced)
                    }
                }
            })
        };
        let parent_prep = Arc::clone(&self.prep);
        let write_prep = Arc::clone(&write);
        let prep: PrepFn = Arc::new(move |ctx| {
            parent_prep(ctx)?;
            ctx.shuffle(shuffle, || write_prep(ctx)).map(|_| ())
        });
        let write_compute = Arc::clone(&write);
        Rdd::new(
            self.ctx,
            n_out,
            vec![Dependency::Shuffle {
                parent: self.id,
                shuffle,
            }],
            vec![parent],
            partitioner,
            prep,
            move |p, ctx| {
                let blocks = ctx.shuffle(shuffle, || write_compute(ctx))?;
                let part = blocks.partition(p)?;
                Ok(Box::new(part.into_iter()) as PartIter<(K, C)>)
            },
        )
    }

    pub fn reduce_by_key<F>(self, f: F) -> Rdd<(K, V)>
    where
        F: Fn(V, V) -> V + Send + Sync + 'static,
    {
        let f = Arc::new(f);
        let merge_value = Arc::clone(&f);
        let merge_combiners = Arc::clone(&f);
        self.combine_by_key(
            |v| v,
            move |c, v| merge_value.as_ref()(c, v),
            move |a, b| merge_combiners.as_ref()(a, b),
        )
    }

    pub fn aggregate_by_key<U, Seq, Comb>(self, zero: U, seq_op: Seq, comb_op: Comb) -> Rdd<(K, U)>
    where
        U: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
        Seq: Fn(U, V) -> U + Send + Sync + 'static,
        Comb: Fn(U, U) -> U + Send + Sync + 'static,
    {
        let seq = Arc::new(seq_op);
        let seq_merge = Arc::clone(&seq);
        self.combine_by_key(
            move |v| seq.as_ref()(zero.clone(), v),
            move |c, v| seq_merge.as_ref()(c, v),
            comb_op,
        )
    }

    pub fn group_by_key(self) -> Rdd<(K, Vec<V>)> {
        self.map(|(k, v)| (k, vec![v]))
            .reduce_by_key(|mut a, mut b| {
                a.append(&mut b);
                a
            })
    }
}

fn drain<T>(iter: impl Iterator<Item = T>, ctx: &ActionCtx) -> Result<Vec<T>> {
    let mut out = Vec::new();
    for item in iter {
        if ctx.cancel.load(Ordering::Relaxed) {
            return Err(Error::Cancelled);
        }
        out.push(item);
    }
    Ok(out)
}
