#!/usr/bin/env bash
#
# bench-single-size.sh — Run a latency benchmark for a single message size,
# repeating N times and keeping the best result.
#
# Useful for isolating benchmark noise: run one size at a time with no other
# CPU-intensive processes (IDEs, agents, browsers) competing for cores.
#
# Usage:
#   ./scripts/bench-single-size.sh <size> [runs] [--local]
#       [--latency-mode transit|saturated] [--send-interval-ns NS]
#       [--output-dir DIR]
#
# Arguments:
#   size          Message payload size in bytes (32, 128, 512, 1024, 4096)
#   runs          Number of iterations (default: 5)
#
# Options:
#   --local             Run a single-broker (local IPC) benchmark instead of cross-broker.
#   --latency-mode      "transit" (paced, unloaded latency) or
#                       "saturated" (queueing latency under load). Default: transit.
#   --send-interval-ns  Override pacing interval for transit mode (default: 10000 ns).
#   --output-dir        Directory for best-of-N result JSON files (default: /tmp/ringloom-bench-best).
#
# Prerequisites:
#   zig build install -Doptimize=ReleaseFast && zig build test-bins -Doptimize=ReleaseFast
#
# io_uring receiver variant:
#   RINGLOOM_BENCH_IOURING_RECEIVER=true ./scripts/bench-single-size.sh 128 5
#   Optional tuning: RINGLOOM_BENCH_IOURING_SQPOLL,
#   RINGLOOM_BENCH_IOURING_RECV_BUFFER_SIZE,
#   RINGLOOM_BENCH_IOURING_RECV_BUFFER_COUNT, RINGLOOM_BENCH_IOURING_CQ_DEPTH,
#   RINGLOOM_BENCH_IOURING_RECEIVER_CQE_BATCH
# io_uring sender variant:
#   RINGLOOM_BENCH_IOURING_SENDER=true ./scripts/bench-single-size.sh 128 5
#   Optional tuning: RINGLOOM_BENCH_SENDER_WRITEV_BATCH_SIZE,
#   RINGLOOM_BENCH_SENDER_WRITE_BUDGET,
#   RINGLOOM_BENCH_IOURING_SENDER_CQE_BATCH
#
# Examples:
#   # Best-of-10 for 128 B unloaded cross-broker latency
#   ./scripts/bench-single-size.sh 128 10
#
#   # Saturated queueing latency, matching the automated multi-size benchmark
#   ./scripts/bench-single-size.sh 128 5 --latency-mode saturated
#
#   # Best-of-5 for 512 B local IPC
#   ./scripts/bench-single-size.sh 512 5 --local
#
#   # Custom output directory
#   ./scripts/bench-single-size.sh 32 5 --output-dir ./my-results

set -euo pipefail

# ── Parse arguments ───────────────────────────────────────────────────

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <size> [runs] [--local] [--latency-mode transit|saturated] [--send-interval-ns NS] [--output-dir DIR]"
    echo "  size: message payload in bytes (32, 128, 512, 1024, 4096)"
    echo "  runs: number of iterations (default: 5)"
    exit 1
fi

SIZE=$1; shift
RUNS=5
MODE="remote"
BEST_DIR="/tmp/ringloom-bench-best"
LATENCY_MODE="transit"
SEND_INTERVAL_NS=""
IOURING_RECEIVER_ENABLED="${RINGLOOM_BENCH_IOURING_RECEIVER:-false}"
IOURING_SENDER_ENABLED="${RINGLOOM_BENCH_IOURING_SENDER:-false}"
IOURING_SQPOLL="${RINGLOOM_BENCH_IOURING_SQPOLL:-true}"
IOURING_RECV_BUFFER_SIZE="${RINGLOOM_BENCH_IOURING_RECV_BUFFER_SIZE:-16384}"
IOURING_RECV_BUFFER_COUNT="${RINGLOOM_BENCH_IOURING_RECV_BUFFER_COUNT:-256}"
IOURING_CQ_DEPTH="${RINGLOOM_BENCH_IOURING_CQ_DEPTH:-1024}"
IOURING_RECEIVER_CQE_BATCH="${RINGLOOM_BENCH_IOURING_RECEIVER_CQE_BATCH:-256}"
IOURING_SENDER_CQE_BATCH="${RINGLOOM_BENCH_IOURING_SENDER_CQE_BATCH:-64}"
SENDER_WRITEV_BATCH_SIZE="${RINGLOOM_BENCH_SENDER_WRITEV_BATCH_SIZE:-64}"
SENDER_WRITE_BUDGET="${RINGLOOM_BENCH_SENDER_WRITE_BUDGET:-256}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --local)            MODE="local"; shift ;;
        --latency-mode)     LATENCY_MODE="$2"; shift 2 ;;
        --send-interval-ns) SEND_INTERVAL_NS="$2"; shift 2 ;;
        --output-dir)       BEST_DIR="$2"; shift 2 ;;
        [0-9]*)             RUNS="$1"; shift ;;
        *)                  echo "Unknown option: $1"; exit 1 ;;
    esac
done

if [[ "$LATENCY_MODE" != "transit" && "$LATENCY_MODE" != "saturated" ]]; then
    echo "ERROR: --latency-mode must be 'transit' or 'saturated'"
    exit 1
fi

if [[ -z "$SEND_INTERVAL_NS" ]]; then
    if [[ "$LATENCY_MODE" == "transit" ]]; then
        SEND_INTERVAL_NS=10000
    else
        SEND_INTERVAL_NS=0
    fi
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BIN="$PROJECT_ROOT/zig-out/bin"

# ── Verify binaries exist ────────────────────────────────────────────

for bin in ringloom-broker ringloom-test-echo-service ringloom-test-ping-service; do
    if [[ ! -x "$BIN/$bin" ]]; then
        echo "ERROR: $BIN/$bin not found."
        echo "Run:  zig build install -Doptimize=ReleaseFast && zig build test-bins -Doptimize=ReleaseFast"
        exit 1
    fi
done

# ── Workspace setup ──────────────────────────────────────────────────

WORK_DIR=$(mktemp -d /tmp/ringloom-bench-single-XXXXXX)
STORAGE="$WORK_DIR/storage"
CONFIGS="$WORK_DIR/config"
LOGS="$WORK_DIR/logs"
RESULTS="$WORK_DIR/results"
mkdir -p "$STORAGE/ringloom-test/services" "$CONFIGS" "$LOGS" "$RESULTS" "$BEST_DIR"

PIDS=()
TOTAL_CPUS=$(nproc)
ISOLATED_CPUS=$(cat /sys/devices/system/cpu/isolated 2>/dev/null || true)
SMT_ACTIVE=$(cat /sys/devices/system/cpu/smt/active 2>/dev/null || true)
BOOST_STATE=$(cat /sys/devices/system/cpu/cpufreq/boost 2>/dev/null || true)
HAS_TASKSET=0
command -v taskset >/dev/null 2>&1 && HAS_TASKSET=1

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

wait_for_pid_exit() {
    local pid="$1"
    local timeout="${2:-30}"
    local label="${3:-process}"
    local deadline=$((SECONDS + timeout))

    while kill -0 "$pid" 2>/dev/null; do
        if [[ $SECONDS -ge $deadline ]]; then
            echo "  ERROR: timed out waiting for $label to exit"
            return 1
        fi
        sleep 0.1
    done

    wait "$pid" 2>/dev/null || true
    return 0
}

warn_if_untuned() {
    local warned=0

    if [[ -z "$ISOLATED_CPUS" ]]; then
        echo "WARNING: no isolated CPUs configured; benchmark jitter can be severe."
        warned=1
    fi
    if [[ "$SMT_ACTIVE" == "1" ]]; then
        echo "WARNING: SMT is enabled; disable it in BIOS for cleaner latency numbers."
        warned=1
    fi
    if [[ "$BOOST_STATE" != "" && "$BOOST_STATE" != "0" ]]; then
        echo "WARNING: turbo boost is enabled; results may be less deterministic."
        warned=1
    fi
    if [[ $warned -eq 1 ]]; then
        echo "         See docs/testing.md and ./scripts/tune-system.sh --verify before benchmarking."
        echo ""
    fi
}

pin_service_core() {
    local role="$1"

    if [[ $HAS_TASKSET -ne 1 || -z "$ISOLATED_CPUS" ]]; then
        return
    fi

    if [[ "$MODE" == "remote" && $TOTAL_CPUS -ge 8 ]]; then
        [[ "$role" == "echo" ]] && printf "6" || printf "7"
        return
    fi

    if [[ "$MODE" == "local" && $TOTAL_CPUS -ge 6 ]]; then
        [[ "$role" == "echo" ]] && printf "4" || printf "5"
    fi
}

start_bg_process() {
    local log_file="$1"
    local core="$2"
    shift 2

    if [[ -n "$core" ]]; then
        taskset -c "$core" "$@" > "$log_file" 2>&1 &
    else
        "$@" > "$log_file" 2>&1 &
    fi
    PIDS+=($!)
}

run_fg_process() {
    local log_file="$1"
    local core="$2"
    shift 2

    if [[ -n "$core" ]]; then
        taskset -c "$core" "$@" > "$log_file" 2>&1
    else
        "$@" > "$log_file" 2>&1
    fi
}

# ── Test parameters ──────────────────────────────────────────────────

TAG="${SIZE}B"
WARMUP=10000
COUNT=100000
[[ $SIZE -eq 4096 ]] && WARMUP=5000 && COUNT=50000

if [[ "$LATENCY_MODE" == "transit" ]]; then
    PREFIX="$MODE-transit-latency"
else
    PREFIX="$MODE-saturated-latency"
fi

echo "RingLoom Single-Size Benchmark"
echo "========================="
echo "Mode:     $MODE"
echo "Size:     $TAG"
echo "Runs:     $RUNS"
echo "Latency:  $LATENCY_MODE"
echo "Pacing:   ${SEND_INTERVAL_NS} ns"
echo "Best dir: $BEST_DIR"
echo ""

warn_if_untuned

# ── Run iterations ───────────────────────────────────────────────────

best_tput=0
best_latency=0

for i in $(seq 1 "$RUNS"); do
    # Clean shared memory from previous iteration.
    rm -rf "$STORAGE/ringloom-test"
    mkdir -p "$STORAGE/ringloom-test/services"

    if [[ "$MODE" == "remote" ]]; then
        # ── Two-broker setup ─────────────────────────────────────────
        cat > "$CONFIGS/broker_1.properties" << EOF
broker.node.id=1
broker.local.host.port=127.0.0.1:19001
broker.member.host.ports=2@127.0.0.1:19002
broker.group.name=ringloom-test
broker.storage.path=$STORAGE
broker.control.buffer.size=65536
broker.messages.buffer.size=1048576
broker.threading.mode=dedicated
broker.idle.strategy=yielding
broker.sender.cpu.affinity=2
broker.receiver.cpu.affinity=3
broker.io.uring.sqpoll=$IOURING_SQPOLL
broker.io.uring.sender.enabled=$IOURING_SENDER_ENABLED
broker.io.uring.sender.cqe.batch.size=$IOURING_SENDER_CQE_BATCH
broker.io.uring.receiver.enabled=$IOURING_RECEIVER_ENABLED
broker.io.uring.receiver.cqe.batch.size=$IOURING_RECEIVER_CQE_BATCH
broker.io.uring.cq.depth=$IOURING_CQ_DEPTH
broker.io.uring.recv.buffer.size=$IOURING_RECV_BUFFER_SIZE
broker.io.uring.recv.buffer.count=$IOURING_RECV_BUFFER_COUNT
broker.sender.writev.batch.size=$SENDER_WRITEV_BATCH_SIZE
broker.sender.write.budget.per.peer=$SENDER_WRITE_BUDGET
broker.benchmark.latency.tracing.enabled=true
EOF

        cat > "$CONFIGS/broker_2.properties" << EOF
broker.node.id=2
broker.local.host.port=127.0.0.1:19002
broker.member.host.ports=1@127.0.0.1:19001
broker.group.name=ringloom-test
broker.storage.path=$STORAGE
broker.control.buffer.size=65536
broker.messages.buffer.size=1048576
broker.threading.mode=dedicated
broker.idle.strategy=yielding
broker.sender.cpu.affinity=4
broker.receiver.cpu.affinity=5
broker.io.uring.sqpoll=$IOURING_SQPOLL
broker.io.uring.sender.enabled=$IOURING_SENDER_ENABLED
broker.io.uring.sender.cqe.batch.size=$IOURING_SENDER_CQE_BATCH
broker.io.uring.receiver.enabled=$IOURING_RECEIVER_ENABLED
broker.io.uring.receiver.cqe.batch.size=$IOURING_RECEIVER_CQE_BATCH
broker.io.uring.cq.depth=$IOURING_CQ_DEPTH
broker.io.uring.recv.buffer.size=$IOURING_RECV_BUFFER_SIZE
broker.io.uring.recv.buffer.count=$IOURING_RECV_BUFFER_COUNT
broker.sender.writev.batch.size=$SENDER_WRITEV_BATCH_SIZE
broker.sender.write.budget.per.peer=$SENDER_WRITE_BUDGET
broker.benchmark.latency.tracing.enabled=true
EOF

        start_bg_process "$LOGS/broker_a.log" "" \
            "$BIN/ringloom-broker" --config "$CONFIGS/broker_1.properties"
        local_BA_PID=${PIDS[-1]}

        start_bg_process "$LOGS/broker_b.log" "" \
            "$BIN/ringloom-broker" --config "$CONFIGS/broker_2.properties"
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
broker.group.name=ringloom-test
broker.storage.path=$STORAGE
broker.control.buffer.size=65536
broker.messages.buffer.size=1048576
broker.threading.mode=dedicated
broker.idle.strategy=yielding
broker.sender.cpu.affinity=2
broker.receiver.cpu.affinity=3
broker.io.uring.sqpoll=$IOURING_SQPOLL
broker.io.uring.sender.enabled=$IOURING_SENDER_ENABLED
broker.io.uring.sender.cqe.batch.size=$IOURING_SENDER_CQE_BATCH
broker.io.uring.receiver.enabled=$IOURING_RECEIVER_ENABLED
broker.io.uring.receiver.cqe.batch.size=$IOURING_RECEIVER_CQE_BATCH
broker.io.uring.cq.depth=$IOURING_CQ_DEPTH
broker.io.uring.recv.buffer.size=$IOURING_RECV_BUFFER_SIZE
broker.io.uring.recv.buffer.count=$IOURING_RECV_BUFFER_COUNT
broker.sender.writev.batch.size=$SENDER_WRITEV_BATCH_SIZE
broker.sender.write.budget.per.peer=$SENDER_WRITE_BUDGET
EOF

        start_bg_process "$LOGS/broker_local.log" "" \
            "$BIN/ringloom-broker" --config "$CONFIGS/broker_local.properties"
        local_BA_PID=${PIDS[-1]}
        local_BB_PID=""

        wait_for_ready "$LOGS/broker_local.log" 5

        ECHO_NODE=1
    fi

    ECHO_CORE=$(pin_service_core echo)
    PING_CORE=$(pin_service_core ping)

    # Echo service.
    start_bg_process "$LOGS/echo.log" "$ECHO_CORE" \
        "$BIN/ringloom-test-echo-service" \
        --storage-path "$STORAGE" \
        --group ringloom-test \
        --service-name echo \
        --broker-node-id "$ECHO_NODE" \
        --quiet \
        --idle-strategy yielding \
        --max-measured-messages "$COUNT" \
        --result-file "$RESULTS/$PREFIX-echo-$TAG.json"
    local_ECHO_PID=${PIDS[-1]}
    wait_for_ready "$LOGS/echo.log" 5

    # Ping service (foreground — blocks until done).
    run_fg_process "$LOGS/ping.log" "$PING_CORE" \
        "$BIN/ringloom-test-ping-service" \
        --storage-path "$STORAGE" \
        --group ringloom-test \
        --service-name ping \
        --broker-node-id 1 \
        --target-service echo \
        --message-count "$COUNT" \
        --message-size "$SIZE" \
        --warmup-count "$WARMUP" \
        --idle-strategy yielding \
        --result-file "$RESULTS/$PREFIX-ping-$TAG.json" \
        --spin-timeout-ms 100 \
        --send-interval-ns "$SEND_INTERVAL_NS"

    if ! wait_for_pid_exit "$local_ECHO_PID" 30 "echo service"; then
        echo "  ERROR: echo did not receive all expected messages"
        tail -n 40 "$LOGS/ping.log" || true
        tail -n 40 "$LOGS/echo.log" || true
        exit 1
    fi

    # Stop processes.
    [[ -n "${local_BB_PID:-}" ]] && stop_pid "$local_BB_PID"
    stop_pid "$local_BA_PID"
    sleep 1

    sent=$(python3 -c "import json; print(json.load(open('$RESULTS/$PREFIX-ping-$TAG.json'))['sent'])")
    failed=$(python3 -c "import json; print(json.load(open('$RESULTS/$PREFIX-ping-$TAG.json'))['failed'])")
    measured=$(python3 -c "import json; print(json.load(open('$RESULTS/$PREFIX-echo-$TAG.json'))['total_measured'])")
    if [[ "$sent" -ne "$COUNT" || "$failed" -ne 0 || "$measured" -ne "$COUNT" ]]; then
        echo "  ERROR: incomplete benchmark run (sent=$sent failed=$failed measured=$measured expected=$COUNT)"
        tail -n 40 "$LOGS/ping.log" || true
        tail -n 40 "$LOGS/echo.log" || true
        exit 1
    fi

    tput=$(python3 -c "import json; print(json.load(open('$RESULTS/$PREFIX-ping-$TAG.json'))['throughput_msgs_per_sec'])")
    latency_ns=$(python3 -c "import json; print(json.load(open('$RESULTS/$PREFIX-echo-$TAG.json'))['latency_p50_ns'])")

    if [[ "$LATENCY_MODE" == "transit" ]]; then
        echo "  Run $i/$RUNS: ${TAG} p50 = $latency_ns ns, throughput = $tput msgs/sec"
        if [[ "$best_latency" -eq 0 || "$latency_ns" -lt "$best_latency" ]]; then
            best_latency=$latency_ns
            best_tput=$tput
            cp "$RESULTS/$PREFIX-ping-$TAG.json" "$BEST_DIR/$PREFIX-ping-$TAG.json"
            cp "$RESULTS/$PREFIX-echo-$TAG.json" "$BEST_DIR/$PREFIX-echo-$TAG.json"
        fi
    else
        echo "  Run $i/$RUNS: ${TAG} throughput = $tput msgs/sec"
        if [[ "$tput" -gt "$best_tput" ]] 2>/dev/null; then
            best_tput=$tput
            cp "$RESULTS/$PREFIX-ping-$TAG.json" "$BEST_DIR/$PREFIX-ping-$TAG.json"
            cp "$RESULTS/$PREFIX-echo-$TAG.json" "$BEST_DIR/$PREFIX-echo-$TAG.json"
        fi
    fi
done

# ── Summary ──────────────────────────────────────────────────────────

echo ""
if [[ "$LATENCY_MODE" == "transit" ]]; then
    echo "Best $TAG transit p50: $best_latency ns"
    echo "Best $TAG throughput:  $best_tput msgs/sec"
else
    echo "Best $TAG throughput: $best_tput msgs/sec"
fi
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
print(f'  Pacing:      {d.get(\"send_interval_ns\", 0):>12,} ns')
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
if d['latency_p50_ns'] >= 1_000_000:
    fmt = lambda v: f'{v/1e6:>12.2f} ms'
else:
    fmt = lambda v: f'{v/1e3:>12.2f} us'
print(f'  Echo p50:    {fmt(d[\"latency_p50_ns\"])}')
print(f'  Echo p95:    {fmt(d[\"latency_p95_ns\"])}')
print(f'  Echo p99:    {fmt(d[\"latency_p99_ns\"])}')
print(f'  Echo p99.9:  {fmt(d[\"latency_p99_9_ns\"])}')
if d.get('stage_breakdown_measured', 0) > 0:
    print('  Stage breakdown:')
    print(f'    broker A queue p50:    {d[\"broker_a_queue_p50_ns\"]/1e3:>10.2f} us')
    print(f'    transport p50:         {d[\"transport_p50_ns\"]/1e3:>10.2f} us')
    print(f'    broker B delivery p50: {d[\"broker_b_delivery_p50_ns\"]/1e3:>10.2f} us')
"
