#!/usr/bin/env bash
#
# bench-topic.sh — Persistent topic publish benchmark (single broker).
#
# Runs ringloom-test-topic-publisher-service against a fresh topics-enabled
# broker: it registers the topic, publishes through the leader's Aeron IPC
# publication, and times until the broker has appended everything (see that
# binary's header). Each run writes one JSON file:
#
#   <output-dir>/run-<N>/topic-publish-<ack_mode>-<size>B.json
#
# Fields: throughput_msgs_per_sec (appended by the broker), offer_msgs_per_sec
# (frames into the publication), ack_wait_ns (last publish → final ack).
#
# Usage:
#   ./scripts/bench-topic.sh [--sizes "32 128 1024"] [--runs N] [--count N]
#                            [--ack] [--output-dir DIR]
#
# Defaults: sizes "128", 3 runs, up to 100000 messages (+1000 warmup; capped at 64 MiB total payload), fire_and_forget,
# output /tmp/ringloom-topic-bench. Summarise with:
#   scripts/bench-report.py <output-dir>
#
# Prerequisites:
#   zig build install test-bins -Doptimize=ReleaseFast

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BIN="$PROJECT_ROOT/zig-out/bin"
PUB_BIN="$BIN/ringloom-test-topic-publisher-service"

SIZES="128"
RUNS=3
COUNT=auto
WARMUP=1000
ACK_MODE=0
OUT_DIR="/tmp/ringloom-topic-bench"
FAILED=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --sizes)      SIZES="$2"; shift 2 ;;
        --runs)       RUNS="$2"; shift 2 ;;
        --count)      COUNT="$2"; shift 2 ;;
        --ack)        ACK_MODE=1; shift ;;
        --output-dir) OUT_DIR="$2"; shift 2 ;;
        -h|--help)    sed -n '2,/^$/p' "$0" | sed 's/^# \?//'; exit 0 ;;
        *) echo "Unknown option: $1 (see --help)"; exit 1 ;;
    esac
done

[[ "$RUNS" =~ ^[1-9][0-9]*$ && ( "$COUNT" == auto || "$COUNT" =~ ^[1-9][0-9]*$ ) ]] || { echo "runs and count must be positive integers"; exit 1; }
for size in $SIZES; do
    [[ "$size" =~ ^[1-9][0-9]*$ ]] || { echo "invalid payload size: $size"; exit 1; }
done

for bin in "$PUB_BIN" "$BIN/ringloom-broker"; do
    [[ -x "$bin" ]] || { echo "ERROR: $bin missing. Run: zig build install test-bins -Doptimize=ReleaseFast"; exit 1; }
done

MODE_TAG=$([[ "$ACK_MODE" == "1" ]] && echo replicate_once || echo fire_and_forget)
echo "Topic publish benchmark: sizes=[$SIZES] runs=$RUNS count=$COUNT warmup=$WARMUP mode=$MODE_TAG"
echo "Results: $OUT_DIR"

WORK_DIR=""
cleanup() {
    if [[ -n "$WORK_DIR" ]]; then
        bash "$SCRIPT_DIR/start-test-broker.sh" --workspace "$WORK_DIR" --node-id 1 --stop >/dev/null 2>&1 || true
        rm -rf "$WORK_DIR"
    fi
}
trap cleanup EXIT

for run in $(seq 1 "$RUNS"); do
    mkdir -p "$OUT_DIR/run-$run"
    for size in $SIZES; do
        # The queue dependency has a fixed ~80 MiB daily cycle file. Keep
        # default runs below 64 MiB including warmup and record overhead.
        run_count="$COUNT"
        if [[ "$run_count" == auto ]]; then
            run_count=$((67108864 / (size + 8) - WARMUP - 1))
            ((run_count > 100000)) && run_count=100000
            ((run_count > 0)) || { echo "Payload too large for benchmark budget"; exit 1; }
        fi
        WORK_DIR=$(mktemp -d /tmp/ringloom-bench-topic-XXXXXX)
        env_out=$(bash "$SCRIPT_DIR/start-test-broker.sh" --workspace "$WORK_DIR" --node-id 1 \
            --port 19040 --group ringloom-bench --daemon --bin-dir "$BIN" --topics)
        storage=$(echo "$env_out" | sed -n 's/^.*RINGLOOM_STORAGE_PATH=//p' | tr -d "'\"")
        group=$(echo "$env_out" | sed -n 's/^.*RINGLOOM_GROUP=//p' | tr -d "'\"")

        result="$OUT_DIR/run-$run/topic-publish-$MODE_TAG-${size}B.json"
        [[ ! -e "$result" ]] || { echo "Refusing to overwrite $result"; exit 1; }
        if timeout 120 "$PUB_BIN" --storage-path "$storage" --group "$group" \
            --service-name topic-bench --broker-node-id 1 --topic bench \
            --count "$run_count" --warmup "$WARMUP" --size "$size" --ack-mode "$ACK_MODE" \
            --result-file "$result" > "$WORK_DIR/publisher.log" 2>&1; then
            python3 -c "import json,sys; d=json.load(open(sys.argv[1])); print(f\"  run {sys.argv[2]} {d['message_size']} B: {d['throughput_msgs_per_sec']:,} msgs/s appended, ack wait {d['ack_wait_ns']/1000:.0f} us\")" "$result" "$run"
        else
            FAILED=1
            echo "  run $run ${size} B: FAILED (log below)"; tail -5 "$WORK_DIR/publisher.log"
        fi
        log_dir="$OUT_DIR/run-$run/logs-$MODE_TAG-${size}B"
        mkdir -p "$log_dir"
        cp "$WORK_DIR/publisher.log" "$log_dir/"
        cp -R "$WORK_DIR/logs" "$WORK_DIR/config" "$log_dir/"
        cleanup
        WORK_DIR=""
    done
done

echo ""
python3 "$SCRIPT_DIR/bench-report.py" "$OUT_DIR"

exit "$FAILED"
