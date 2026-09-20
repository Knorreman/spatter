#!/usr/bin/env bash
# Spatter benchmark suite: one command, pinned configs, correctness checks.
#
# Usage:
#   bench/run.sh [input] [reps]
#
# Environment:
#   BENCH_INPUT  input text file (default /tmp/gospark-wc-big.txt)
#   BENCH_REPS   repetitions per scenario/mode (default 3)
#   BENCH_MODES  subset of "local cluster" (default "local cluster")
#
# Scenarios: wordcount, skew, reuse. Modes: local, cluster N (N=BENCH_RANKS, default 4).
# Prints CSV rows: scenario,mode,rep,startup_ms,exec_ms,peak_kb and asserts outputs.

set -euo pipefail
cd "$(dirname "$0")/.."

# cargo may live in ~/.cargo/bin (not on PATH in minimal shells)
export PATH="$HOME/.cargo/bin:$PATH"

BENCH_INPUT="${1:-${BENCH_INPUT:-/tmp/gospark-wc-big.txt}}"
BENCH_REPS="${2:-${BENCH_REPS:-3}}"
BENCH_MODES="${BENCH_MODES:-local cluster}"
BENCH_RANKS="${BENCH_RANKS:-4}"
BENCH_SPILL_MB="${BENCH_SPILL_MB:-64}"

if [[ ! -r "$BENCH_INPUT" ]]; then
    echo "input not readable: $BENCH_INPUT" >&2
    exit 1
fi

read -r EXPECTED_KEYS EXPECTED_SUM < <(python3 - "$BENCH_INPUT" <<'PY'
import sys
keys, total = set(), 0
with open(sys.argv[1]) as source:
    for line in source:
        words = line.split()
        keys.update(words)
        total += len(words)
print(len(keys), total)
PY
)

cargo build --release --locked --example bench

run_case() {
    local scenario="$1" mode="$2" rep="$3"
    local extra=()
    local mode_name="$mode"
    if [[ "$mode" == cluster* ]]; then
        extra=(--cluster "$BENCH_RANKS")
        mode_name="cluster$BENCH_RANKS"
    fi
    local out
    if ! out="$(BENCH_SCENARIO="$scenario" SPATTER_SPILL_MB="$BENCH_SPILL_MB" \
        timeout "${BENCH_TIMEOUT:-300}" ./target/release/examples/bench "${extra[@]}" "$BENCH_INPUT" 2>&1)"; then
        echo "$out" >&2
        exit 1
    fi
    echo "$out" >&2
    local startup exec_ms peak
    startup="$(grep -o 'startup_ms=[0-9]*' <<<"$out" | cut -d= -f2 | tail -1)"
    exec_ms="$(grep -o 'exec_ms=[0-9]*' <<<"$out" | cut -d= -f2 | head -1)"
    peak="$(grep -o 'peak_kb=[0-9]*' <<<"$out" | cut -d= -f2 | tail -1)"
    if [[ -z "$startup" || -z "$exec_ms" || -z "$peak" ]]; then
        echo "FAILED scenario=$scenario mode=$mode_name rep=$rep (missing metrics)" >&2
        echo "$out" >&2
        exit 1
    fi
    if [[ "$scenario" == wordcount ]]; then
        grep -q "keys=$EXPECTED_KEYS sum=$EXPECTED_SUM" <<<"$out" \
            || { echo "FAILED scenario=$scenario mode=$mode_name rep=$rep (output mismatch)" >&2
                 grep 'BENCH' <<<"$out" >&2; exit 1; }
    elif [[ "$scenario" == reuse ]]; then
        grep -q "runs=\[$EXPECTED_KEYS, $EXPECTED_KEYS, $EXPECTED_KEYS\]" <<<"$out" \
            || { echo "FAILED scenario=$scenario mode=$mode_name rep=$rep (reuse mismatch)" >&2
                 grep 'BENCH' <<<"$out" >&2; exit 1; }
    elif [[ "$scenario" == skew ]]; then
        grep -q "keys=$((EXPECTED_KEYS + (EXPECTED_SUM > 0))) hot=$((EXPECTED_SUM * 9)) sum=$((EXPECTED_SUM * 10))$" <<<"$out" \
            || { echo "FAILED scenario=$scenario mode=$mode_name rep=$rep (skew mismatch)" >&2
                 grep 'BENCH' <<<"$out" >&2; exit 1; }
    fi
    echo "${scenario},${mode_name},${rep},${startup},${exec_ms},${peak}"
}

echo "scenario,mode,rep,startup_ms,exec_ms,peak_kb"
for scenario in wordcount skew reuse; do
    for mode in $BENCH_MODES; do
        for rep in $(seq 1 "$BENCH_REPS"); do
            run_case "$scenario" "$mode" "$rep"
        done
    done
done
echo "BENCH OK input=$BENCH_INPUT keys=$EXPECTED_KEYS sum=$EXPECTED_SUM" >&2
