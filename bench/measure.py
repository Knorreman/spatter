"""Build bench, then run: python3 bench/measure.py LABEL INPUT.

Uses the 200k-key/60M-word corpus and one shared 16-CPU affinity budget.
"""
import json
import os
import platform
import re
import subprocess
import sys

label, path = sys.argv[1:]
cpus = sorted(os.sched_getaffinity(0))[:16]
os.sched_setaffinity(0, cpus)
env = {k: v for k, v in os.environ.items() if not k.startswith(("SPATTER_", "BENCH_"))}
env.update(BENCH_MASTER="local[16]", SPATTER_SPILL_MB="64")
print(json.dumps(dict(label=label, cpus=cpus, platform=platform.platform(),
                     input_bytes=os.path.getsize(path), partitions=16, spill_mb=64)), flush=True)
for ranks in [1, 4, 16]:
    for repeat in range(3):
        cmd = ["target/release/examples/bench"]
        if ranks > 1:
            cmd += ["--cluster", str(ranks)]
        result = subprocess.run(cmd + [path], env=env, capture_output=True, text=True, timeout=180, check=True)
        assert "keys=200000 sum=60000000" in result.stderr, result.stderr
        metrics = dict(re.findall(r"(\w+)=(\d+)\b", result.stderr))
        peaks = re.search(r"rank_peaks_kb=(\[[^\]]*\])", result.stderr)
        assert peaks is not None, result.stderr
        print(json.dumps(dict(label=label, ranks=ranks, repeat=repeat + 1,
                             metrics=metrics, rank_peaks_kb=json.loads(peaks[1]))), flush=True)
