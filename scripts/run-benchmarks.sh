#!/usr/bin/env bash
#
# run-benchmarks.sh — Run BRZ broker performance benchmarks manually.
#
# Orchestrates broker(s) and test services directly, collecting JSON result
# files into a results directory.  Useful when `zig build perf` output is
# not easily inspectable (the test runner compacts passing test output).
#
# Usage:
#   ./scripts/run-benchmarks.sh [--local-only] [--remote-only] [--output-dir DIR]
#
# Options:
#   --local-only    Run only the local (single-broker) latency benchmarks.
#   --remote-only   Run only the cross-broker latency benchmarks.
#   --output-dir    Directory for result JSON files (default: /tmp/brz-bench-results).
#
# Prerequisites:
#   zig build install && zig build test-bins

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BIN="$PROJECT_ROOT/zig-out/bin"

# Defaults.
RUN_LOCAL=true
RUN_REMOTE=true
RESULTS_DIR="/tmp/brz-bench-results"

# ── Parse arguments ───────────────────────────────────────────────────

while [[ $# -gt 0 ]]; do
    case "$1" in
        --local-only)   RUN_LOCAL=true; RUN_REMOTE=false; shift ;;
        --remote-only)  RUN_LOCAL=false; RUN_REMOTE=true; shift ;;
        --output-dir)   RESULTS_DIR="$2"; shift 2 ;;
        -h|--help)
            sed -n '2,/^$/p' "$0" | sed 's/^# \?//'
            exit 0 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

# ── Verify binaries exist ────────────────────────────────────────────

for bin in brz-broker brz-test-echo-service brz-test-ping-service; do
    if [[ ! -x "$BIN/$bin" ]]; then
        echo "ERROR: $BIN/$bin not found.  Run:  zig build install && zig build test-bins"
        exit 1
    fi
done

# ── Workspace setup ──────────────────────────────────────────────────

WORK_DIR=$(mktemp -d /tmp/brz-bench-XXXXXX)
STORAGE="$WORK_DIR/storage"
CONFIGS="$WORK_DIR/config"
LOGS="$WORK_DIR/logs"
mkdir -p "$STORAGE/brz-test/services" "$CONFIGS" "$LOGS" "$RESULTS_DIR"

PIDS=()

cleanup() {
    for pid in "${PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
    wait 2>/dev/null || true
    rm -rf "$WORK_DIR"
}
trap cleanup EXIT

# ── Helpers ───────────────────────────────────────────────────────────

wait_for_ready() {
    local log_file="$1"
    local timeout="${2:-10}"
    local deadline=$((SECONDS + timeout))
    while [[ $SECONDS -lt $deadline ]]; do
        if grep -q "broker started\|service ready" "$log_file" 2>/dev/null; then
            return 0
        fi
        sleep 0.1
    done
    echo "  WARNING: timed out waiting for readiness in $log_file"
    return 1
}

stop_pid() {
    local pid="$1"
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
    # Remove from PIDS array.
    local new_pids=()
    for p in "${PIDS[@]}"; do
        [[ "$p" != "$pid" ]] && new_pids+=("$p")
    done
    PIDS=("${new_pids[@]+"${new_pids[@]}"}")
}

# ── Local latency benchmark ──────────────────────────────────────────

run_local_bench() {
    local SIZE=$1
    local TAG="${SIZE}B"
    local WARMUP=10000
    local COUNT=100000

    if [[ $SIZE -eq 4096 ]]; then
        WARMUP=5000
        COUNT=50000
    fi

    echo "  local-latency-$TAG ..."

    # Single broker.
    cat > "$CONFIGS/broker_local.properties" << EOF
broker.node.id=1
broker.local.host.port=127.0.0.1:19010
broker.group.name=brz-test
broker.storage.path=$STORAGE
broker.control.buffer.size=65536
broker.messages.buffer.size=1048576
broker.threading.mode=dedicated
broker.idle.strategy=yielding
broker.sender.cpu.affinity=2
broker.receiver.cpu.affinity=3
broker.io.uring.sqpoll=true
EOF

    "$BIN/brz-broker" --config "$CONFIGS/broker_local.properties" \
        > "$LOGS/broker_local_$TAG.log" 2>&1 &
    PIDS+=($!)
    local BROKER_PID=${PIDS[-1]}
    wait_for_ready "$LOGS/broker_local_$TAG.log" 5

    # Echo service with latency measurement.
    "$BIN/brz-test-echo-service" \
        --storage-path "$STORAGE" \
        --group brz-test \
        --service-name echo \
        --broker-node-id 1 \
        --quiet \
        --idle-strategy yielding \
        --result-file "$RESULTS_DIR/local-latency-echo-$TAG.json" \
        > "$LOGS/echo_local_$TAG.log" 2>&1 &
    PIDS+=($!)
    local ECHO_PID=${PIDS[-1]}
    wait_for_ready "$LOGS/echo_local_$TAG.log" 5

    # Ping service.
    "$BIN/brz-test-ping-service" \
        --storage-path "$STORAGE" \
        --group brz-test \
        --service-name ping \
        --broker-node-id 1 \
        --target-service echo \
        --message-count "$COUNT" \
        --message-size "$SIZE" \
        --warmup-count "$WARMUP" \
        --idle-strategy yielding \
        --result-file "$RESULTS_DIR/local-latency-ping-$TAG.json" \
        --spin-timeout-ms 100 \
        > "$LOGS/ping_local_$TAG.log" 2>&1

    # Drain and stop.
    sleep 2
    stop_pid "$ECHO_PID"
    stop_pid "$BROKER_PID"
    sleep 1

    # Clean storage for next run.
    rm -rf "$STORAGE/brz-test"
    mkdir -p "$STORAGE/brz-test/services"
}

# ── Cross-broker latency benchmark ───────────────────────────────────

write_two_broker_configs() {
    cat > "$CONFIGS/broker_1.properties" << EOF
broker.node.id=1
broker.local.host.port=127.0.0.1:19001
broker.member.host.ports=2@127.0.0.1:19002
broker.group.name=brz-test
broker.storage.path=$STORAGE
broker.control.buffer.size=65536
broker.messages.buffer.size=1048576
broker.threading.mode=dedicated
broker.idle.strategy=yielding
broker.sender.cpu.affinity=2
broker.receiver.cpu.affinity=3
broker.io.uring.sqpoll=true
EOF

    cat > "$CONFIGS/broker_2.properties" << EOF
broker.node.id=2
broker.local.host.port=127.0.0.1:19002
broker.member.host.ports=1@127.0.0.1:19001
broker.group.name=brz-test
broker.storage.path=$STORAGE
broker.control.buffer.size=65536
broker.messages.buffer.size=1048576
broker.threading.mode=dedicated
broker.idle.strategy=yielding
broker.sender.cpu.affinity=4
broker.receiver.cpu.affinity=5
broker.io.uring.sqpoll=true
EOF
}

run_remote_bench() {
    local SIZE=$1
    local TAG="${SIZE}B"
    local WARMUP=10000
    local COUNT=100000

    if [[ $SIZE -eq 4096 ]]; then
        WARMUP=5000
        COUNT=50000
    fi

    echo "  remote-latency-$TAG ..."

    write_two_broker_configs

    "$BIN/brz-broker" --config "$CONFIGS/broker_1.properties" \
        > "$LOGS/broker_a_$TAG.log" 2>&1 &
    PIDS+=($!)
    local BA_PID=${PIDS[-1]}

    "$BIN/brz-broker" --config "$CONFIGS/broker_2.properties" \
        > "$LOGS/broker_b_$TAG.log" 2>&1 &
    PIDS+=($!)
    local BB_PID=${PIDS[-1]}

    wait_for_ready "$LOGS/broker_a_$TAG.log" 5
    wait_for_ready "$LOGS/broker_b_$TAG.log" 5
    sleep 2  # cluster settle

    # Echo on broker B.
    "$BIN/brz-test-echo-service" \
        --storage-path "$STORAGE" \
        --group brz-test \
        --service-name echo \
        --broker-node-id 2 \
        --quiet \
        --idle-strategy yielding \
        --result-file "$RESULTS_DIR/remote-latency-echo-$TAG.json" \
        > "$LOGS/echo_remote_$TAG.log" 2>&1 &
    PIDS+=($!)
    local ECHO_PID=${PIDS[-1]}
    wait_for_ready "$LOGS/echo_remote_$TAG.log" 5

    # Ping on broker A.
    "$BIN/brz-test-ping-service" \
        --storage-path "$STORAGE" \
        --group brz-test \
        --service-name ping \
        --broker-node-id 1 \
        --target-service echo \
        --message-count "$COUNT" \
        --message-size "$SIZE" \
        --warmup-count "$WARMUP" \
        --idle-strategy yielding \
        --result-file "$RESULTS_DIR/remote-latency-ping-$TAG.json" \
        --spin-timeout-ms 100 \
        > "$LOGS/ping_remote_$TAG.log" 2>&1

    sleep 2
    stop_pid "$ECHO_PID"
    stop_pid "$BB_PID"
    stop_pid "$BA_PID"
    sleep 1

    rm -rf "$STORAGE/brz-test"
    mkdir -p "$STORAGE/brz-test/services"
}

# ── Main ──────────────────────────────────────────────────────────────

echo "BRZ Broker Benchmark Suite"
echo "=========================="
echo "Binaries:   $BIN"
echo "Results:    $RESULTS_DIR"
echo ""

if [[ "$RUN_LOCAL" == true ]]; then
    echo "Running local (single-broker) latency benchmarks..."
    for SIZE in 32 128 512 1024 4096; do
        run_local_bench $SIZE
    done
    echo ""
fi

if [[ "$RUN_REMOTE" == true ]]; then
    echo "Running cross-broker latency benchmarks..."
    for SIZE in 32 128 512 1024 4096; do
        run_remote_bench $SIZE
    done
    echo ""
fi

# ── Print summary ────────────────────────────────────────────────────

echo "Results:"
echo "--------"
for f in "$RESULTS_DIR"/*.json; do
    [[ -e "$f" ]] || continue
    echo ""
    echo "$(basename "$f"):"
    cat "$f"
done

echo ""
echo "Done. JSON results are in: $RESULTS_DIR"
