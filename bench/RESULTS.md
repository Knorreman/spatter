# Benchmark results

## Corrected measurements after dispatch hardening

The older results below used driver-only RSS and a skew transformation that
did not produce a hot key on this corpus. Their cluster-wide memory and skew
claims are withdrawn. The runner also previously failed to pass the spill
setting to the engine. See `bench/README.md` for corrected semantics.

Controlled wordcount runs: 446,667,000-byte corpus, 200,000 keys, 60,000,000
words; Linux 6.8, release build, CPU affinity 0–15 shared by all ranks,
16 partitions, 64 MiB spill threshold, three fresh processes per mode.
No cache dropping; sequential runs, not randomized trials.

| Mode | Serial spilled reduce, ms | Default two-thread reduce, ms | Median improvement |
|---|---|---|---|
| local | 5343, 5359, 5347 | 4756, 4748, 4815 | 11.1% |
| 4 ranks | 8579, 8717, 8559 | 8025, 8208, 8227 | 4.3% |
| 16 ranks | 5832, 5940, 5897 | 5397, 5492, 5497 | 6.9% |

All runs matched the expected key count and total. Four-rank maximum per-rank
RSS rose from 304,552–314,352 KiB to 319,488–323,944 KiB; sums of rank peaks
were 822,824–855,332 versus 829,180–861,500 KiB. Sixteen-rank sums remained
about 1.64 million KiB—more than local execution's roughly 1 million KiB.
Sum of rank peaks is not simultaneous aggregate RSS.

An exploratory 16-reducer configuration gave medians of 4147/7498/4732 ms
(local/4/16 ranks), but four-rank driver peak RSS reached 539,372–545,444 KiB.
The default is therefore two reducer threads, capped by context parallelism;
`SPATTER_REDUCE_THREADS` allows explicit tuning.

An opt-in profile before optimization showed 1.45 s in serial spilled
reduction, 0.33 s in spill encode/write and about 0.63 s each in transport
encode/decode summed across ranks. Map/combine spans totaled 16.06 s across
concurrent ranks; these sums are not critical-path wall time. Network reads
include waiting for computation. This supports optimizing spilled reduction,
not attributing the whole regression to network bandwidth.

Forced-spill tests additionally exposed and fixed a map/output spill filename
collision. The full shuffle and DAG suites now pass with a zero spill threshold.

## Historical measurements (limitations above apply)

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
