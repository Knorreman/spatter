# Execution and configuration

Transformations build a lineage graph. An action creates an action-local cache,
prepares parent shuffles and executes partitions. Shuffle data can be reused
within that action; the next action recomputes it.

## Cluster path

All ranks must construct the same program/lineage and enter actions in the same
order. Rank 0 dispatches shuffle-map tasks over a star TCP topology. Windows
contain at most two partitions per rank; workers use at most two compute threads
and bounded queues. Replies are checked against the sending rank and outstanding
partition. Driver-local partitions are also computed.

Combined map outputs travel to the driver, which spills/reduces and stores the
result. Other ranks do not have equivalent post-shuffle caches: do not assume
arbitrary chained distributed shuffles are supported. Non-shuffle cluster
actions currently execute on the driver rather than distributing all work.
Worker-local `collect`/`count` results are not global; `reduce` can return
`EmptyRdd` on workers. `collect_to_driver` is the tested gathered-result path.

On worker compute failure or a read disconnect, pending dispatch work can be
recomputed on the driver. A write failure or later gather can still fail the job.
Closures must tolerate replay; external writes may occur more than once. Local
partition panic retries and cluster replay do not provide exactly-once effects.

## Environment

| Variable | Default / meaning |
|---|---|
| `SPATTER_N` | Number of ranks including driver; unset means local |
| `SPATTER_RANK` | Rank 0 is driver; required for manual launch (StatefulSet hostname ordinal can supply it) |
| `SPATTER_PORT` | Driver listener port, `18741` |
| `SPATTER_MASTER` | Worker destination `host:port`; defaults to `127.0.0.1:PORT` |
| `SPATTER_HOSTS` | Fallback when MASTER is unset; only its first comma-separated host/address is used |
| `SPATTER_BIND` | Driver bind host, `0.0.0.0`; port comes from PORT |
| `SPATTER_CLUSTER_TIMEOUT_MS` | Startup/socket timeout, `30000`; not a whole-job deadline |
| `SPATTER_SPILL_MB` | Spill threshold, `64`; zero forces spill paths, not zero heap use |
| `SPATTER_REDUCE_THREADS` | Spilled-bucket reduction threads, `2`, capped by context parallelism |
| `SPATTER_PROFILE` | Set to enable stderr profiling spans |

Local `--cluster N` spawning explicitly uses a localhost master address. For
manual multi-host launch, set N/RANK/MASTER on each process instead of using
`--cluster`. Supply the same compiled binary and readable file path on every
host; hostnames must resolve and the driver's listener must be reachable.

Text input splits align to newline boundaries. The source still expects a local
filesystem path on each rank; there is no shared-file distribution service.
Network frames have a 256 MiB cap. Large partition replies can fail despite
spill settings. Collecting all records on the driver can exceed memory.
