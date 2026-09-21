# Spatter

An **experimental Spark-style RDD engine in Rust**: lazy transformations,
local thread execution, shuffle aggregation and same-binary TCP clusters.
There is no JVM or WASM runtime. This is not Spark API compatibility or a
production-ready distributed scheduler.

Source: <https://github.com/Knorreman/spatter>. License: Apache-2.0.
The workspace declares Rust 1.74; CI tests MSRV and stable.

## Run from source

Requires Rust/Cargo; cluster examples and process regression tests target Linux.

```sh
git clone https://github.com/Knorreman/spatter.git
cd spatter
printf 'hello world hello\nfoo bar foo\n' > /tmp/spatter-input.txt
cargo run --release --locked --example wordcount -- /tmp/spatter-input.txt
cargo run --release --locked --example wordcount -- --cluster 3 /tmp/spatter-input.txt
```

Both runs should report `keys=4 sum=6`. Cluster mode starts three processes:
rank 0 is the driver and also computes partitions; ranks 1–2 are workers.
The driver prints the result. All ranks need the same input at the same path.
`--cluster` is an argument handled by the application context, not by Cargo.

## Use as a library

Until registry publication is verified, use a local path dependency:

```toml
[dependencies]
spatter = { path = "../spatter/crates/spatter" }
```

```rust
use spatter::prelude::*;

fn main() -> Result<()> {
    let sc = SpatterContext::builder().master("local[2]").get_or_create()?;
    let total = sc.parallelize(vec![1, 2, 3, 4])
        .map(|x| x * 2)
        .filter(|x| *x > 4)
        .reduce(|a, b| a + b)?;
    assert_eq!(total, 14);
    Ok(())
}
```

### API overview

- Sources: `parallelize`, `parallelize_partitions`, `read_text_file`,
  `read_text_file_partitions`.
- Transformations: `map`, `filter`, `flat_map`, `union`, `distinct`.
- Keyed aggregation: `reduce_by_key`, `combine_by_key`, `aggregate_by_key`,
  `group_by_key`.
- Actions: `collect`, `collect_to_driver`, `count`, `take`, `reduce`.
- Inspection: `dependencies`, `stages`, `partitioner`, `get_num_partitions`.

Local masters are `local` (one thread), `local[N]`, and `local[*]`.
Shuffle keys/values require Serde serialization plus the API's thread-safety,
ownership and hashing bounds. Prefer owned `String` keys over borrowed strings.
Reducers used for parallel aggregation should be associative and commutative.

## Current limits

- Cluster ranks run the same `main`; closures are compiled into the same binary,
  not serialized. The executor-only entry is a placeholder without a registered
  task implementation, not a standalone distributed runtime.
- Distributed computation is currently concentrated in shuffle-map dispatch.
  Cluster actions do not all have Spark's global semantics. Use the tested
  single-shuffle `collect_to_driver` pattern; cluster chained-shuffle correctness
  is not established. Local chained shuffles are tested.
- Shuffle output/reduction remains centralized on the driver. A lost worker's
  pending dispatch partitions can be replayed, but later gathers still require
  all ranks. Full-job recovery and exactly-once side effects are not guaranteed.
- Repeated actions recompute; there is no public persist/checkpoint API.
- Spill thresholds and bounded queues are not hard process-memory limits.
  Combiners, reducers, individual records and collected results can be large.
- TCP has no authentication, TLS or binary-version negotiation. Run matching
  binaries on a trusted network; the default driver bind is `0.0.0.0`.

See [execution/configuration](docs/execution.md),
[deployment](deploy/README.md), [observability](docs/observability.md), and [release checklist](docs/releasing.md).
Repository-relative documentation is also available on GitHub.

## Development

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --all-targets --locked
cargo test --workspace --doc --locked
SPATTER_SPILL_MB=0 cargo test --locked --test shuffle --test dag
cargo doc --workspace --no-deps --locked
```

Develop on a topic branch and open a PR for review. See
[process/failure tests](tests/README.md) and [benchmarks](bench/README.md).
The benchmark suite has local and localhost-cluster measurements; Spark
baselines and genuine multi-host benchmark results remain unfinished.
