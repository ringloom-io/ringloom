#!/usr/bin/env bash
#
# bench-topic.sh — Benchmark persistent topics: measure Aeron topic publish
# throughput to a running broker.  Builds RingLoomDataHeader(flag_topic) +
# TopicPublishHeader + payload, offers via Aeron IPC, and measures end-to-end
# throughput exactly as a real producer service does.
#
# Usage:
#   ./scripts/bench-topic.sh [message_size] [runs]
#
# Prerequisites:
#   zig build install -Doptimize=ReleaseFast && zig build test-bins -Doptimize=ReleaseFast

set -euo pipefail

MSG_COUNT=${TOPIC_BENCH_MSG_COUNT:-500000}
MSG_SIZE=${1:-128}
RUNS=${2:-5}
ACK_MODE=0  # 0=fire_and_forget, 1=replicate_once

while [[ $# -gt 0 ]]; do
    case "$1" in
        --ack-mode) ACK_MODE="$2"; shift 2 ;;
        --ack)      ACK_MODE=1; shift ;;
        [0-9]*)     if [[ -z "${_saw_size:-}" ]]; then MSG_SIZE="$1"; _saw_size=1; else RUNS="$1"; fi; shift ;;
        *)          shift ;;
    esac
done

ACK_TAG=$([ "$ACK_MODE" == "1" ] && echo "replicate_once" || echo "fire_and_forget")

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BIN="$PROJECT_ROOT/zig-out/bin"
BENCH_BIN="$BIN/ringloom-test-topic-aeron-bench"
BROKER="$BIN/ringloom-broker"
BEST_DIR="/tmp/ringloom-topic-bench"

for bin in "$BENCH_BIN" "$BROKER"; do
    [[ -x "$bin" ]] || { echo "ERROR: $bin missing. Run: zig build install -Doptimize=ReleaseFast && zig build test-bins -Doptimize=ReleaseFast"; exit 1; }
done

SIZE_TAG="${MSG_SIZE}B"

echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║     RingLoom Topic Publish Benchmark (Aeron IPC)            ║"
echo "╠══════════════════════════════════════════════════════════════╣"
printf "║  %-56s ║\n" "Payload:        $SIZE_TAG"
printf "║  %-56s ║\n" "Messages:       $MSG_COUNT (+ 1000 warmup)"
printf "║  %-56s ║\n" "Iterations:     $RUNS (best kept)"
printf "║  %-56s ║\n" "Ack mode:       $ACK_TAG"
printf "║  %-56s ║\n" "Path:           Aeron IPC offer() → broker receiver"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

ISOLATED_CPUS=$(cat /sys/devices/system/cpu/isolated 2>/dev/null || true)
SMT_ACTIVE=$(cat /sys/devices/system/cpu/smt/active 2>/dev/null || true)
[[ -z "$ISOLATED_CPUS" ]] && echo "NOTE: no isolated CPUs; jitter may be higher than optimal."
[[ "$SMT_ACTIVE" == "1" ]]   && echo "NOTE: SMT active; disable in BIOS for cleaner numbers."
HAS_TASKSET=0; command -v taskset >/dev/null 2>&1 && HAS_TASKSET=1
echo ""

best_tput=0; best_json=""; best_run=0
mkdir -p "$BEST_DIR"

for run in $(seq 1 "$RUNS"); do
    WORK_DIR=$(mktemp -d /tmp/ringloom-bench-topic-XXXXXX)
    STORAGE="$WORK_DIR/storage"; AERON="$WORK_DIR/aeron"; LOGS="$WORK_DIR/logs"
    mkdir -p "$STORAGE/ringloom-test/topics" "$AERON" "$LOGS"
    trap "rm -rf $WORK_DIR" EXIT

    # Broker with topics enabled.
    cat > "$WORK_DIR/broker.properties" << EOF
broker.node.id=1
broker.local.host.port=127.0.0.1:19040
broker.group.name=ringloom-test
broker.storage.path=$STORAGE
broker.control.buffer.size=65536
broker.messages.buffer.size=1048576
broker.threading.mode=shared
broker.idle.strategy=yielding
broker.aeron.directory=$AERON
broker.aeron.delete.directory.on.start=true
broker.aeron.delete.directory.on.shutdown=true
broker.topics.enabled=true
broker.topics.path=$STORAGE/ringloom-test/topics
broker.topics.pub.stream.base=50000
EOF

    BROKER_CORE=""; [[ $HAS_TASKSET -eq 1 ]] && BROKER_CORE="2"
    if [[ -n "$BROKER_CORE" ]]; then
        taskset -c "$BROKER_CORE" "$BROKER" --config "$WORK_DIR/broker.properties" > "$LOGS/broker.log" 2>&1 &
    else
        "$BROKER" --config "$WORK_DIR/broker.properties" > "$LOGS/broker.log" 2>&1 &
    fi
    BROKER_PID=$!
    sleep 3

    # Verify broker is alive.
    if ! kill -0 "$BROKER_PID" 2>/dev/null; then
        echo "  Run $run/$RUNS: broker failed to start"; kill "$BROKER_PID" 2>/dev/null || true; rm -rf "$WORK_DIR"; continue
    fi

    echo -n "  Run $run/$RUNS  ${SIZE_TAG} ... "

    JSON=$(timeout 30 "$BENCH_BIN" \
        --aeron-dir "$AERON" \
        --pub-stream-id 50001 \
        --topic-id 0xABCD \
        --message-count "$MSG_COUNT" \
        --message-size "$MSG_SIZE" \
        --warmup-count 1000 \
        --ack-mode "$ACK_MODE" 2>/dev/null || true)

    kill "$BROKER_PID" 2>/dev/null || true; wait "$BROKER_PID" 2>/dev/null || true

    if echo "$JSON" | grep -q "TOPIC_AERON_BENCH_JSON"; then
        J=$(echo "$JSON" | sed -n '/TOPIC_AERON_BENCH_JSON<<EOF/,/EOF/p' | grep -v 'TOPIC_AERON_BENCH_JSON\|EOF')
    else
        echo "FAIL (no JSON)"; rm -rf "$WORK_DIR"; trap - EXIT; continue
    fi

    TPUT=$(echo "$J"  | python3 -c "import sys,json; print(json.load(sys.stdin)['tput'])"  2>/dev/null || echo "0")
    DT_US=$(echo "$J"  | python3 -c "import sys,json; print(json.load(sys.stdin)['dt_us'])" 2>/dev/null || echo "0")

    echo "${TPUT} msgs/sec  (${DT_US} us)"

    TPUT_NUM=$(echo "$TPUT" | sed 's/\..*//')
    if [[ "$TPUT_NUM" -gt "$best_tput" ]] 2>/dev/null; then
        best_tput=$TPUT_NUM; best_json="$J"; best_run=$run; echo "    -> new best"
    fi

    rm -rf "$WORK_DIR"; trap - EXIT
done

[[ -z "$best_json" ]] && { echo "No successful runs."; exit 1; }

TPUT_FMT=$(echo "$best_json" | python3 -c "import sys,json; d=json.load(sys.stdin); n=d['tput']; print(f'{n:,}')" 2>/dev/null || echo "$best_tput")
DT_US_FMT=$(echo "$best_json" | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'{d[\"dt_us\"]}')" 2>/dev/null || echo "0")
COST_NS=$(echo "$best_json"  | python3 -c "import sys,json; d=json.load(sys.stdin); cost=d['dt_ns']/d['mc']; print(f'{cost:.0f}')" 2>/dev/null || echo "0")
RATE_MB=$(echo "$best_json"  | python3 -c "import sys,json; d=json.load(sys.stdin); rate=d['tput']*d['ms']/(1024*1024); print(f'{rate:.2f}')" 2>/dev/null || echo "0")

echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║               Results (best of $RUNS)                          ║"
echo "╠══════════════════════════════════════════════════════════════╣"
printf "║  %-56s ║\n" "Best run:       #$best_run"
printf "║  %-56s ║\n" "Messages:       $MSG_COUNT (+ 1000 warmup)"
printf "║  %-56s ║\n" "Payload:        $SIZE_TAG"
printf "║  %-56s ║\n" "Throughput:     ${TPUT_FMT} msgs/sec"
printf "║  %-56s ║\n" "Data rate:      ${RATE_MB} MiB/sec"
printf "║  %-56s ║\n" "Elapsed:        ${DT_US_FMT} us"
printf "║  %-56s ║\n" "Per-message:    ${COST_NS} ns"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""
echo "$best_json" | python3 -m json.tool > "$BEST_DIR/topic-publish-${MSG_SIZE}B.json" 2>/dev/null || true
echo "JSON → $BEST_DIR/topic-publish-${MSG_SIZE}B.json"
echo ""
echo "Compare sizes:"
echo "  $0 32  $RUNS"
echo "  $0 128 $RUNS"
echo "  $0 512 $RUNS"
echo "  $0 1024 $RUNS"
echo "  $0 4096 $RUNS"
echo ""
