#!/usr/bin/env bash
#
# bench-single-size.sh — Run a cross-broker benchmark for a single message
# size, repeating N times and keeping the best result.
#
# Useful for isolating benchmark noise: run one size at a time with no other
# CPU-intensive processes (IDEs, agents, browsers) competing for cores.
#
# Usage:
#   ./scripts/bench-single-size.sh <size> [runs] [--local] [--output-dir DIR]
#
# Arguments:
#   size          Message payload size in bytes (32, 128, 512, 1024, 4096)
#   runs          Number of iterations (default: 5)
#
# Options:
#   --local       Run a single-broker (local IPC) benchmark instead of cross-broker.
#   --output-dir  Directory for best-of-N result JSON files (default: /tmp/brz-bench-best).
#
# Prerequisites:
#   zig build install -Doptimize=ReleaseFast && zig build test-bins -Doptimize=ReleaseFast
#
# Example:
#   # Best-of-10 for 128 B cross-broker
#   ./scripts/bench-single-size.sh 128 10
#
#   # Best-of-5 for 512 B local IPC
#   ./scripts/bench-single-size.sh 512 5 --local
#
#   # Custom output directory
#   ./scripts/bench-single-size.sh 32 5 --output-dir ./my-results

set -euo pipefail

# ── Parse arguments ───────────────────────────────────────────────────

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <size> [runs] [--local] [--output-dir DIR]"
    echo "  size: message payload in bytes (32, 128, 512, 1024, 4096)"
    echo "  runs: number of iterations (default: 5)"
    exit 1
fi

SIZE=$1; shift
RUNS=5
MODE="remote"
BEST_DIR="/tmp/brz-bench-best"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --local)      MODE="local"; shift ;;
        --output-dir) BEST_DIR="$2"; shift 2 ;;
        [0-9]*)       RUNS="$1"; shift ;;
        *)            echo "Unknown option: $1"; exit 1 ;;
    esac
done

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BIN="$PROJECT_ROOT/zig-out/bin"

# ── Verify binaries exist ────────────────────────────────────────────

for bin in brz-broker brz-test-echo-service brz-test-ping-service; do
    if [[ ! -x "$BIN/$bin" ]]; then
        echo "ERROR: $BIN/$bin not found."
        echo "Run:  zig build install -Doptimize=ReleaseFast && zig build test-bins -Doptimize=ReleaseFast"
        exit 1
    fi
done

# ── Workspace setup ──────────────────────────────────────────────────

WORK_DIR=$(mktemp -d /tmp/brz-bench-single-XXXXXX)
STORAGE="$WORK_DIR/storage"
CONFIGS="$WORK_DIR/config"
LOGS="$WORK_DIR/logs"
RESULTS="$WORK_DIR/results"
mkdir -p "$STORAGE/brz-test/services" "$CONFIGS" "$LOGS" "$RESULTS" "$BEST_DIR"

PIDS=()

cleanup() {
    for pid in "${PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
    wait 2>/dev/null || true
    rm -rf "$WORK_DIR"
}
trap cleanup EXIT

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
    local new_pids=()
    for p in "${PIDS[@]}"; do
        [[ "$p" != "$pid" ]] && new_pids+=("$p")
    done
    PIDS=("${new_pids[@]+"${new_pids[@]}"}")
}

# ── Test parameters ──────────────────────────────────────────────────

TAG="${SIZE}B"
WARMUP=10000
COUNT=100000
[[ $SIZE -eq 4096 ]] && WARMUP=5000 && COUNT=50000

PREFIX="$MODE-latency"

echo "BRZ Single-Size Benchmark"
echo "========================="
echo "Mode:     $MODE"
echo "Size:     $TAG"
echo "Runs:     $RUNS"
echo "Best dir: $BEST_DIR"
echo ""

# ── Run iterations ───────────────────────────────────────────────────

best_tput=0

for i in $(seq 1 "$RUNS"); do
    # Clean shared memory from previous iteration.
    rm -rf "$STORAGE/brz-test"
    mkdir -p "$STORAGE/brz-test/services"

    if [[ "$MODE" == "remote" ]]; then
        # ── Two-broker setup ─────────────────────────────────────────
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
EOF

        "$BIN/brz-broker" --config "$CONFIGS/broker_1.properties" \
            > "$LOGS/broker_a.log" 2>&1 &
        PIDS+=($!)
        local_BA_PID=${PIDS[-1]}

        "$BIN/brz-broker" --config "$CONFIGS/broker_2.properties" \
            > "$LOGS/broker_b.log" 2>&1 &
        PIDS+=($!)
        local_BB_PID=${PIDS[-1]}

        wait_for_ready "$LOGS/broker_a.log" 5
        wait_for_ready "$LOGS/broker_b.log" 5
        sleep 2  # cluster settle

        ECHO_NODE=2
    else
        # ── Single-broker setup ──────────────────────────────────────
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
EOF

        "$BIN/brz-broker" --config "$CONFIGS/broker_local.properties" \
            > "$LOGS/broker_local.log" 2>&1 &
        PIDS+=($!)
        local_BA_PID=${PIDS[-1]}
        local_BB_PID=""

        wait_for_ready "$LOGS/broker_local.log" 5

        ECHO_NODE=1
    fi

    # Echo service.
    "$BIN/brz-test-echo-service" \
        --storage-path "$STORAGE" \
        --group brz-test \
        --service-name echo \
        --broker-node-id "$ECHO_NODE" \
        --quiet \
        --idle-strategy yielding \
        --result-file "$RESULTS/$PREFIX-echo-$TAG.json" \
        > "$LOGS/echo.log" 2>&1 &
    PIDS+=($!)
    local_ECHO_PID=${PIDS[-1]}
    wait_for_ready "$LOGS/echo.log" 5

    # Ping service (foreground — blocks until done).
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
        --result-file "$RESULTS/$PREFIX-ping-$TAG.json" \
        --spin-timeout-ms 100 \
        > "$LOGS/ping.log" 2>&1

    sleep 1

    # Stop processes.
    stop_pid "$local_ECHO_PID"
    [[ -n "${local_BB_PID:-}" ]] && stop_pid "$local_BB_PID"
    stop_pid "$local_BA_PID"
    sleep 1

    # Extract throughput and compare.
    tput=$(python3 -c "import json; print(json.load(open('$RESULTS/$PREFIX-ping-$TAG.json'))['throughput_msgs_per_sec'])")
    echo "  Run $i/$RUNS: ${TAG} throughput = $tput msgs/sec"

    if [[ "$tput" -gt "$best_tput" ]] 2>/dev/null; then
        best_tput=$tput
        cp "$RESULTS/$PREFIX-ping-$TAG.json" "$BEST_DIR/$PREFIX-ping-$TAG.json"
        cp "$RESULTS/$PREFIX-echo-$TAG.json" "$BEST_DIR/$PREFIX-echo-$TAG.json"
    fi
done

# ── Summary ──────────────────────────────────────────────────────────

echo ""
echo "Best $TAG throughput: $best_tput msgs/sec"
echo ""
echo "Best results saved to:"
echo "  $BEST_DIR/$PREFIX-ping-$TAG.json"
echo "  $BEST_DIR/$PREFIX-echo-$TAG.json"
echo ""
echo "Ping (send-side) details:"
python3 -c "
import json, sys
d = json.load(open('$BEST_DIR/$PREFIX-ping-$TAG.json'))
print(f'  Throughput:  {d[\"throughput_msgs_per_sec\"]:>12,} msgs/sec')
print(f'  Send p50:    {d[\"send_latency_p50_ns\"]:>12,} ns')
print(f'  Send p95:    {d[\"send_latency_p95_ns\"]:>12,} ns')
print(f'  Send p99:    {d[\"send_latency_p99_ns\"]:>12,} ns')
print(f'  Send p99.9:  {d[\"send_latency_p99_9_ns\"]:>12,} ns')
"
echo ""
echo "Echo (end-to-end latency) details:"
python3 -c "
import json, sys
d = json.load(open('$BEST_DIR/$PREFIX-echo-$TAG.json'))
print(f'  Measured:    {d[\"total_measured\"]:>12,} msgs')
print(f'  Echo p50:    {d[\"latency_p50_ns\"]/1e6:>12.2f} ms')
print(f'  Echo p95:    {d[\"latency_p95_ns\"]/1e6:>12.2f} ms')
print(f'  Echo p99:    {d[\"latency_p99_ns\"]/1e6:>12.2f} ms')
print(f'  Echo p99.9:  {d[\"latency_p99_9_ns\"]/1e6:>12.2f} ms')
"
