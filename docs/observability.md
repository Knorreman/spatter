# Operational observability

## HTTP metrics

Set `SPATTER_METRICS_ADDR` to opt in when building a context:

```sh
SPATTER_METRICS_ADDR=127.0.0.1:19000 SPATTER_TASK_LOG=1 \
  cargo run --release --locked --example wordcount -- --cluster 3 /path/to/input.txt
```

Rank 0 binds port 19000, rank 1 binds 19001, rank 2 binds 19002. Use
`curl http://127.0.0.1:19000/metrics` while the job is running. For ephemeral
ports set `127.0.0.1:0`; each process logs `METRICS_LISTEN rank=... address=...`.
The listener lives as long as the context's shared inner state, including RDDs
that retain it. Short jobs may finish before a manual scrape.

Addresses are numeric socket addresses; IPv6 uses `[::1]:19000`. Port overflow
or bind failure returns an error from context construction. No listener is
started by default. Each rank exposes its own counters; this is not a driver
aggregation endpoint. Avoid exposing the unauthenticated endpoint publicly.

Applications can instead use `spatter::metrics::MetricsServer::bind(addr)` and
keep the returned handle alive, or call `spatter::metrics::render()` for a final
text snapshot. All contexts/servers in one process share counters. Endpoint
threads terminate on handle drop; reads/writes and request length are bounded.
Only `GET /metrics` is supported. This is a small diagnostic server, not a
general HTTP service.

Counters (all prefixed `spatter_`):

| Name | Meaning |
|---|---|
| `task_attempts_total` | Started instrumented task attempts |
| `task_failures_total` | Completed attempts returning an error or caught panic |
| `task_retries_total` | Started attempts with attempt number greater than zero |
| `task_duration_microseconds_total` | Sum of completed task wall times, including failed attempts |
| `dispatch_disconnects_total` | Dispatch reply-stream read failures, including timeouts |
| `network_sent_bytes_total` / `network_received_bytes_total` | Successful framed transport bytes, including headers and control/gather traffic |
| `shuffle_map_records_total` | Partial combined records emitted by map combiners |
| `spill_written_bytes_total` / `spill_read_bytes_total` | Successfully written/read spill chunks, including length headers |

Snapshots are approximate under concurrent updates. Summed task duration is
not job elapsed time. Killed processes cannot emit a completion event or retain
in-memory counters; scrape periodically. Transport bytes are not exclusively
shuffle bytes. No CPU/RSS gauge or histogram is provided in this first version.

## Structured task events

Set `SPATTER_TASK_LOG=1` for JSON-line `task_started` and `task_finished` events
on stderr. Completion adds `duration_us` and `success`. Every event includes
`pid`, `rank`, `scope`, `job`, `partition`, and `attempt`.

- `scope=dispatch`: job IDs identify one dispatch invocation. The driver sends
  the identifier and absolute partition index to workers, so the same
  `(job, partition)` identifies the initial attempt and driver replay across
  window boundaries. Initial attempt is 0; driver replay is 1.
- `scope=local`: job IDs identify one `run_partitions` invocation in a process,
  including local shuffle/reduction/result stages. Include PID when grouping
  these events. Panic retries increment the attempt number.

IDs reset on process restart; they are diagnostic stage-invocation IDs, not a
durable global application job catalog. Multiple dispatched shuffles have
different job IDs. Cluster result-stage no-op partitions can be counted as
local task attempts. Direct `take()` and driver-only final reductions are not
covered by task counters. No per-partition labels are attached to Prometheus
counters; high-cardinality identifiers remain in logs.

To diagnose worker loss, locate a worker `task_started` without a finish,
then look for driver replay with the same dispatch job/partition and attempt 1.
A write failure can abort before replay; the application's final error remains
authoritative. Later gathers may fail even when dispatch replay succeeded.

To diagnose skew/spill, compare task durations across partitions and inspect
spill counters on the driver. Enable `SPATTER_PROFILE=1` for deeper spans
(`map_combine`, `spill_read_reduce`, transport encode/decode and I/O). Weighted
skew in `bench` does not by itself guarantee spill; a small
`SPATTER_SPILL_MB` threshold exercises that path.

## Verification

```sh
cargo test --test metrics
SPATTER_SPILL_MB=0 cargo test --test metrics
cargo build --release --locked --example wordcount
python3 tests/cluster_process.py
```

Tests scrape a live endpoint, verify counters from actual computations and panic
recovery, verify listener shutdown with an idle client, and check structured
events from repeated three-rank runs and a SIGKILL during active input.
The internal dispatch wire format now includes diagnostic IDs: all ranks must
run the same build, as required by the same-binary cluster model.
