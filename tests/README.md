# Cluster regression tests

`cargo test --workspace --all-targets --locked` includes loopback TCP tests for:

- outstanding-partition replay after disconnect;
- duplicate, wrong-owner and out-of-range reply rejection;
- repeated dispatch windows followed by gathers on the same connections.

On Linux, run the real-process test with:

```sh
cargo build --release --locked --example wordcount
python3 tests/cluster_process.py
```

It checks two successful three-rank runs, then sends SIGKILL to a worker
after observing its input file open. The interrupted job must return exact
totals or an explicit error within 20 seconds. Full-job recovery is not
guaranteed: later gather operations still require all ranks.

Dispatch now submits windows of two partitions per rank, with at most two
compute threads per worker and two slots in each task/reply channel. Windows
are drained before reuse. These are count bounds, not a total heap budget:
individual partition computations can still allocate large results.
