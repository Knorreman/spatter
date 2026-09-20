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

1. **Peak memory**: cluster mode cuts driver peak RSS ~4x (1.0 GiB -> 0.26 GiB)
   by streaming map output over TCP instead of materializing locally.
2. **Reuse amplification**: 3 `count()` actions over one shuffled RDD cost ~3x
   a single action in both modes (shuffle blocks are cached per action only).
3. **REGRESSION vs pre-dispatch cluster**: before driver-dispatch, `--cluster 16`
   wordcount ran in 2.3-2.5 s; the current star-topology `dispatch_each` sends
   tasks one at a time, blocking on each reply (serial per partition), and
   clusters all shuffle bytes through the driver. Result: cluster4 is ~20 s,
   ~3.5x slower than local. Follow-up work: pipeline task dispatch (in-flight
   window > 1) and per-rank shuffle write to unblock cluster scaling.
4. Startup cost of forking ranks and joining TCP is negligible (< 100 ms).

## Spark baseline

Not yet measured on this host; `docs/` has a placeholder and the runner
keeps `mode` generic so `spark` rows can be added with the same assertions.