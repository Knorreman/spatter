use std::any::Any;
use std::collections::HashMap;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Mutex, OnceLock};
use std::thread;

use crate::cluster::Cluster;
use spatter_core::{Error, RddId, Result, ShuffleId};

pub fn panic_message(payload: Box<dyn Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "Box<dyn Any>".to_string()
    }
}

type ShuffleCell = Arc<OnceLock<Result<Arc<dyn Any + Send + Sync>>>>;
type PrepCell = Arc<OnceLock<Result<()>>>;

pub struct ActionCtx {
    pub cancel: AtomicBool,
    pub cluster: Option<Arc<Cluster>>,
    shuffles: Mutex<HashMap<ShuffleId, ShuffleCell>>,
    preps: Mutex<HashMap<RddId, PrepCell>>,
}

impl ActionCtx {
    #[allow(dead_code)]
    pub fn new() -> Arc<Self> {
        Self::create(None)
    }

    pub fn create(cluster: Option<Arc<Cluster>>) -> Arc<Self> {
        Arc::new(Self {
            cancel: AtomicBool::new(false),
            cluster,
            shuffles: Mutex::new(HashMap::new()),
            preps: Mutex::new(HashMap::new()),
        })
    }

    pub fn owns(&self, _partition: usize) -> bool {
        self.cluster.as_ref().map(|c| c.is_driver()).unwrap_or(true)
    }

    pub fn shuffle<T, F>(&self, id: ShuffleId, init: F) -> Result<Arc<T>>
    where
        T: Send + Sync + 'static,
        F: FnOnce() -> Result<T>,
    {
        let cell = {
            let mut guard = self.shuffles.lock().expect("shuffle cache poisoned");
            Arc::clone(guard.entry(id).or_insert_with(|| Arc::new(OnceLock::new()))) as ShuffleCell
        };
        let stored = cell.get_or_init(|| match catch_unwind(AssertUnwindSafe(init)) {
            Ok(Ok(value)) => Ok(Arc::new(value) as Arc<dyn Any + Send + Sync>),
            Ok(Err(e)) => Err(e),
            Err(payload) => Err(Error::Panic {
                message: panic_message(payload),
            }),
        });
        match stored {
            Ok(any) => Arc::downcast::<T>(Arc::clone(any)).map_err(|_| Error::Panic {
                message: "shuffle cache type mismatch".into(),
            }),
            Err(e) => Err(e.clone()),
        }
    }

    pub fn prep_once<F>(&self, id: RddId, f: F) -> Result<()>
    where
        F: FnOnce() -> Result<()>,
    {
        let cell = {
            let mut guard = self.preps.lock().expect("prep cache poisoned");
            Arc::clone(guard.entry(id).or_insert_with(|| Arc::new(OnceLock::new())))
        };
        cell.get_or_init(|| match catch_unwind(AssertUnwindSafe(f)) {
            Ok(r) => r,
            Err(payload) => Err(Error::Panic {
                message: panic_message(payload),
            }),
        })
        .clone()
    }
}

pub fn catch_compute_result<T, F>(partition: usize, f: F) -> Result<T>
where
    F: FnOnce() -> Result<T>,
{
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(r) => r,
        Err(payload) => Err(Error::PartitionPanic {
            partition,
            message: panic_message(payload),
        }),
    }
}

pub fn catch_driver<T, F>(f: F) -> Result<T>
where
    F: FnOnce() -> Result<T>,
{
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(r) => r,
        Err(payload) => Err(Error::Panic {
            message: panic_message(payload),
        }),
    }
}

pub fn run_partitions<T, F>(
    n_partitions: usize,
    parallelism: usize,
    action: &ActionCtx,
    compute: F,
) -> Result<Vec<T>>
where
    T: Send + 'static,
    F: Fn(usize, &ActionCtx) -> Result<T> + Sync,
{
    if n_partitions == 0 {
        return Ok(Vec::new());
    }
    let threads = parallelism.max(1).min(n_partitions);
    let job = crate::metrics::job_id();
    let (tx, rx) = mpsc::channel();
    thread::scope(|scope| {
        for t in 0..threads {
            let tx = tx.clone();
            let compute = &compute;
            thread::Builder::new()
                .name(format!("spatter-{t}"))
                .spawn_scoped(scope, move || {
                    let mut p = t;
                    while p < n_partitions {
                        if action.cancel.load(Ordering::Relaxed) {
                            break;
                        }
                        let mut attempts = 0u32;
                        let msg = loop {
                            crate::metrics::start_task("local", job, p, attempts);
                            let started = std::time::Instant::now();
                            let part = catch_unwind(AssertUnwindSafe(|| compute(p, action)));
                            crate::metrics::observe_task(
                                "local",
                                job,
                                p,
                                attempts,
                                started.elapsed(),
                                matches!(&part, Ok(Ok(_))),
                            );
                            match part {
                                Ok(Ok(v)) => break Ok((p, v)),
                                Ok(Err(e)) => break Err(e),
                                Err(_payload) if attempts < 2 => {
                                    attempts += 1;
                                    continue;
                                }
                                Err(payload) => {
                                    break Err(Error::PartitionPanic {
                                        partition: p,
                                        message: panic_message(payload),
                                    })
                                }
                            }
                        };
                        if tx.send(msg).is_err() {
                            break;
                        }
                        p += threads;
                    }
                })
                .map_err(|e| Error::Panic {
                    message: format!("failed to spawn worker: {e}"),
                })?;
        }
        drop(tx);
        let mut slots: Vec<Option<T>> = (0..n_partitions).map(|_| None).collect();
        for msg in rx {
            match msg {
                Ok((p, part)) => slots[p] = Some(part),
                Err(e) => {
                    action.cancel.store(true, Ordering::Relaxed);
                    return Err(e);
                }
            }
        }
        slots
            .into_iter()
            .enumerate()
            .map(|(i, slot)| slot.ok_or(Error::MissingPartition(i)))
            .collect()
    })
}
