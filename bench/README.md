# Reproducible measurements

```sh
bench/run.sh INPUT 3 > results.csv 2> details.log
BENCH_RANKS=16 BENCH_MODES=cluster bench/run.sh INPUT 3
BENCH_SPILL_MB=0 bench/run.sh SMALL_INPUT 1
```

The runner uses `--locked`, propagates nonzero exits, and applies a per-process
deadline (`BENCH_TIMEOUT`, default 300 seconds). Python computes expected counts
using whitespace splitting. CSV rows match their header; stderr preserves
per-rank peak RSS and network totals. `peak_kb` is the maximum rank peak, and
`sum_rank_peaks_kb` is a sum of individual high-water marks, not simultaneous
cluster memory. Input preparation and compilation are outside execution time.

The default `BENCH_MASTER=local[16]` fixes source partitions at 16. Cluster
workers remain subject to the dispatch concurrency bounds. For a shared CPU
budget, use the Linux harness below, which inherits one affinity mask across
all child ranks. No multi-host or Spark baseline is claimed by these runs.

```sh
cargo build --release --locked --example bench
python3 bench/measure.py LABEL /tmp/gospark-wc-big.txt
```

This comparison harness expects the 200,000-key/60,000,000-word corpus,
prints machine/configuration metadata and three runs each at 1/4/16 ranks.

## Scenarios and profiling

- Wordcount checks key count and sum.
- Skew emits a weighted `hot=9` pair plus a prefixed original key with weight
  one for each input token: 90% of aggregate weight goes to one key. This is
  weighted aggregation, not a claim of 90% physical input records. Empty input
  stays empty. Checks include exact hot weight, total weight and key count.
- Reuse checks three actions on the same lineage; Spatter recomputes each action.

`BENCH_SPILL_MB` is forwarded as `SPATTER_SPILL_MB`. Skew alone does not prove
spill; set a small threshold and inspect the profile. With `SPATTER_PROFILE=1`,
stderr includes PID/rank/stage/microsecond spans for map/combine, memory reduce,
spill read/reduce, spill encode/write, transport encoding/decoding and network
I/O. Concurrent/nested spans are not additive wall time; network reads include
waiting. Profile separately from final timing runs to avoid logging overhead.

`SPATTER_REDUCE_THREADS` controls spilled-bucket reduction (default 2, capped
by context parallelism). More threads can increase peak heap use substantially.
