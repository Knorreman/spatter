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
BENCH_REPS="${BENCH_REPS:-3}"
BENCH_MODES="${BENCH_MODES:-local cluster}"
BENCH_RANKS="${BENCH_RANKS:-4}"
BENCH_SPILL_MB="${BENCH_SPILL_MB:-64}"

if [[ ! -r "$BENCH_INPUT" ]]; then
    echo "input not readable: $BENCH_INPUT" >&2
    exit 1
fi

EXPECTED_KEYS="$(tr ' ' '\n' < "$BENCH_INPUT" | sed '/^$/d' | sort -u | wc -l)"
EXPECTED_SUM="$(tr ' ' '\n' < "$BENCH_INPUT" | sed '/^$/d' | wc -l)"

cargo build --release --example bench

run_case() {
    local scenario="$1" mode="$2" rep="$3"
    local extra=()
    local mode_name="$mode"
    if [[ "$mode" == cluster* ]]; then
        extra=(--cluster "$BENCH_RANKS")
        mode_name="cluster$BENCH_RANKS"
    fi
    local out
    out="$(BENCH_SCENARIO="$scenario" BENCH_SPILL_MB="$BENCH_SPILL_MB" \
        ./target/release/examples/bench "${extra[@]}" "$BENCH_INPUT" 2>&1 || true)"
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
        grep -q "keys=$EXPECTED_KEYS hot=[0-9]*" <<<"$out" \
            || { echo "FAILED scenario=$scenario mode=$mode_name rep=$rep (skew mismatch)" >&2
                 grep 'BENCH' <<<"$out" >&2; exit 1; }
    fi
    echo "bench,${scenario},${mode_name},${rep},${startup},${exec_ms},${peak}"
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