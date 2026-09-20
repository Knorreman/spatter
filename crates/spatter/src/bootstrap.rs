use std::env;

use spatter_core::{Error, Result};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LaunchMode {
    Driver,
    Executor,
}

pub fn launch_mode() -> LaunchMode {
    if env::args().any(|a| a == "--spatter-executor") {
        LaunchMode::Executor
    } else {
        LaunchMode::Driver
    }
}

pub fn run_executor() -> Result<()> {
    if std::env::var("SPATTER_RANK").is_err() {
        return Err(Error::Cluster(
            "executor requires SPATTER_RANK, SPATTER_N, SPATTER_MASTER".into(),
        ));
    }
    let cluster = crate::cluster::join_if_configured(Vec::new())?
        .ok_or_else(|| Error::Cluster("executor failed to join cluster".into()))?;
    cluster.dispatch_each::<Vec<u8>, _, _>(
        0,
        |_| Err(Error::Cluster("executor has no registered task".into())),
        |_| Ok(()),
    )
}
