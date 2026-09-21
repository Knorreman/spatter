use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use spatter_core::{parse_local_threads, split_contiguous, RddId, Result, ShuffleId};

use crate::source::{text_file_meta, FileLines};

use crate::bootstrap::{launch_mode, run_executor, LaunchMode};
use crate::rdd::Rdd;

#[derive(Clone)]
pub struct SpatterContext {
    pub(crate) inner: Arc<ContextInner>,
}

pub(crate) struct ContextInner {
    _metrics: Option<crate::metrics::MetricsServer>,
    pub parallelism: usize,
    pub cluster: Option<std::sync::Arc<crate::cluster::Cluster>>,
    next_id: AtomicU64,
    next_shuffle: AtomicU64,
}

pub struct ContextBuilder {
    master: String,
}

impl ContextInner {
    pub(crate) fn alloc_id(&self) -> RddId {
        RddId(self.next_id.fetch_add(1, Ordering::Relaxed))
    }

    pub(crate) fn alloc_shuffle(&self) -> ShuffleId {
        ShuffleId(self.next_shuffle.fetch_add(1, Ordering::Relaxed))
    }
}

impl SpatterContext {
    pub fn builder() -> ContextBuilder {
        ContextBuilder {
            master: "local[*]".to_string(),
        }
    }

    pub fn parallelism(&self) -> usize {
        self.inner.parallelism
    }

    pub fn rank(&self) -> usize {
        self.inner.cluster.as_ref().map(|c| c.rank).unwrap_or(0)
    }

    pub fn world_size(&self) -> usize {
        self.inner.cluster.as_ref().map(|c| c.n).unwrap_or(1)
    }

    pub fn is_driver(&self) -> bool {
        self.rank() == 0
    }

    pub fn net_bytes(&self) -> (u64, u64) {
        self.inner
            .cluster
            .as_ref()
            .map(|c| (c.bytes_sent(), c.bytes_recv()))
            .unwrap_or((0, 0))
    }

    pub fn file_partition_opens(&self) -> u64 {
        crate::source::FILE_PARTITION_OPENS.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn gather_u64(&self, value: u64) -> Result<Vec<u64>> {
        match &self.inner.cluster {
            Some(c) => c.gather(vec![value]),
            None => Ok(vec![value]),
        }
    }

    pub fn parallelize<T>(&self, data: Vec<T>) -> Rdd<T>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.parallelize_partitions(data, self.inner.parallelism)
            .expect("context parallelism is >= 1")
    }

    pub fn parallelize_partitions<T>(&self, data: Vec<T>, n_partitions: usize) -> Result<Rdd<T>>
    where
        T: Clone + Send + Sync + 'static,
    {
        let parts = Arc::new(split_contiguous(data, n_partitions)?);
        let n_parts = parts.len();
        Ok(Rdd::new(
            Arc::clone(&self.inner),
            n_parts,
            Vec::new(),
            Vec::new(),
            None,
            std::sync::Arc::new(|_| Ok(())),
            move |p, _ctx| {
                let part = parts[p].clone();
                Ok(Box::new(part.into_iter()))
            },
        ))
    }

    pub fn read_text_file(&self, path: impl AsRef<Path>) -> Result<Rdd<String>> {
        self.read_text_file_partitions(path, self.inner.parallelism)
    }

    pub fn read_text_file_partitions(
        &self,
        path: impl AsRef<Path>,
        n_partitions: usize,
    ) -> Result<Rdd<String>> {
        let (path, splits) = text_file_meta(path, n_partitions)?;
        let path = Arc::new(path);
        let splits = Arc::new(splits);
        let n_parts = splits.len();
        Ok(Rdd::new(
            Arc::clone(&self.inner),
            n_parts,
            Vec::new(),
            Vec::new(),
            None,
            Arc::new(|_| Ok(())),
            move |p, _ctx| {
                let (start, end) = splits[p];
                Ok(Box::new(FileLines::open(&path, start, end)?) as crate::rdd::PartIter<String>)
            },
        ))
    }
}

impl ContextBuilder {
    pub fn master(mut self, master: impl Into<String>) -> Self {
        self.master = master.into();
        self
    }

    pub fn build(self) -> Result<SpatterContext> {
        let children = crate::cluster::maybe_spawn_cluster()?;
        let cluster = crate::cluster::join_if_configured(children)?;
        let parallelism = parse_local_threads(&self.master)?;
        let metrics = match std::env::var("SPATTER_METRICS_ADDR") {
            Ok(value) => {
                let mut addr: std::net::SocketAddr = value
                    .parse()
                    .map_err(|e| spatter_core::Error::Io(format!("metrics address: {e}")))?;
                let rank = cluster.as_ref().map(|c| c.rank).unwrap_or(0);
                if addr.port() != 0 {
                    let port = usize::from(addr.port())
                        .checked_add(rank)
                        .and_then(|p| u16::try_from(p).ok())
                        .ok_or_else(|| spatter_core::Error::Io("metrics port overflow".into()))?;
                    addr.set_port(port);
                }
                let server = crate::metrics::MetricsServer::bind(addr)
                    .map_err(|e| spatter_core::Error::Io(e.to_string()))?;
                crate::metrics::log_line(format!(
                    "METRICS_LISTEN rank={rank} address={}",
                    server.local_addr()
                ));
                Some(server)
            }
            Err(_) => None,
        };
        Ok(SpatterContext {
            inner: Arc::new(ContextInner {
                _metrics: metrics,
                parallelism,
                cluster,
                next_id: AtomicU64::new(0),
                next_shuffle: AtomicU64::new(0),
            }),
        })
    }

    pub fn get_or_create(self) -> Result<SpatterContext> {
        if launch_mode() == LaunchMode::Executor {
            run_executor()?;
            std::process::exit(0);
        }
        self.build()
    }
}
