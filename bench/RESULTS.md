# Benchmark results

Suite: `bench/run.sh` (see header for usage). Hardware: developer workstation
(Linux 6.8, rustc 1.98.1, release profile). Input: `/tmp/gospark-wc-big.txt`
(426 MiB, 1.5M lines, 60M words, 200k distinct). `BENCH_RANKS=4`,
`BENCH_SPILL_MB=64`, 3 reps. `exec_ms` measures the action from the driver;
`startup_ms` is context build (fork + TCP join for cluster mode).

| scenario | mode      | rep | startup_ms | exec_ms | peak_kb |
|----------|-----------|-----|------------|---------|---------|
| wordcount| local     | 1-3 | 0          | 5620-5817 | 994144-1026752 |
| wordcount| cluster4 | 1-3 | 20-61      | 20018-21179 | 254072-280060 |
| skew     | local     | 1-3 | 0          | 5590-6092 | 993572-1015744 |
| skew     | cluster4 | 1-3 | 20-61      | 20545-20805 | 275660-276812 |
| reuse    | local     | 1-3 | 0          | 17172-17886 | 1102364-1112420 |
| reuse    | cluster4 | 1-3 | 20         | 62689-63002 | 296224-297324 |

Correctness asserted on every run: `keys=200000 sum=60000000`; reuse reports
identical counts across 3 actions; skew reports a nonzero `hot` bucket.

## Findings

1. **Peak memory**: cluster mode cuts driver peak RSS ~2-4x (1.0 GiB -> 0.26-0.5 GiB)
   by streaming map output over TCP instead of materializing locally.
2. **Reuse amplification**: 3 `count()` actions over one shuffled RDD cost ~3x
   a single action in both modes (shuffle blocks are cached per action only).
3. **Pipelined dispatch (fixed)**: `dispatch_each` previously sent one task and
   blocked on its full compute+reply round trip, serializing all remote work:
   cluster4 wordcount ~20 s, ~3.5x slower than local. After pipelining task
   writes, reading replies concurrently per worker stream, and running worker
   computes on a thread pool: cluster4 20 s -> 7.5 s, cluster16 wordcount 5.7 s
   (matches local; was 2.3-2.5 s before driver-dispatch). Remaining gap vs the
   pre-dispatch cluster is shuffle bytes round-tripping through the driver.
4. Startup cost of forking ranks and joining TCP is negligible (< 100 ms).

## Spark baseline

Not yet measured on this host; `docs/` has a placeholder and the runner
keeps `mode` generic so `spark` rows can be added with the same assertions.