#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SAMPLE_DIR=$(cd "$SCRIPT_DIR/.." && pwd)
ROOT_DIR=$(cd "$SCRIPT_DIR/../../.." && pwd)

PROFILE=default
ORDERS=1000
RATE_PER_SEC=10000
BURST_SIZE=4
MESSAGE_SIZE=128
DURATION_SEC=0
OPTIMIZE=Debug
NO_BUILD=0
BIN_DIR="$ROOT_DIR/zig-out/bin"
WORKSPACE=""

PIDS=()
LABELS=()
LOGS=()
LAST_PID=""
LAST_LOG=""

usage() {
    cat <<EOF
Usage: samples/order-management/scripts/run.sh [options]

Options:
  --profile default|full
  --orders N
  --rate-per-sec N
  --burst-size N
  --message-size N
  --duration-sec N
  --optimize Debug|ReleaseSafe|ReleaseFast|ReleaseSmall
  --workspace PATH
  --no-build
  --bin-dir PATH
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --profile) PROFILE="$2"; shift 2 ;;
        --orders) ORDERS="$2"; shift 2 ;;
        --rate-per-sec) RATE_PER_SEC="$2"; shift 2 ;;
        --burst-size) BURST_SIZE="$2"; shift 2 ;;
        --message-size) MESSAGE_SIZE="$2"; shift 2 ;;
        --duration-sec) DURATION_SEC="$2"; shift 2 ;;
        --optimize) OPTIMIZE="$2"; shift 2 ;;
        --workspace) WORKSPACE="$2"; shift 2 ;;
        --no-build) NO_BUILD=1; shift ;;
        --bin-dir) BIN_DIR="$2"; shift 2 ;;
        --help|-h) usage; exit 0 ;;
        *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
    esac
done

if [[ "$PROFILE" != "default" && "$PROFILE" != "full" ]]; then
    echo "--profile must be 'default' or 'full'" >&2
    exit 2
fi

cd "$ROOT_DIR"

if [[ "$NO_BUILD" -eq 0 ]]; then
    zig build sample-order-management -Doptimize="$OPTIMIZE"
fi

if [[ -z "$WORKSPACE" ]]; then
    WORKSPACE=$(mktemp -d "${TMPDIR:-/tmp}/ringloom-order-management-XXXXXX")
else
    mkdir -p "$WORKSPACE"
fi

CONFIG_DIR="$WORKSPACE/config"
LOG_DIR="$WORKSPACE/logs"
RESULT_DIR="$WORKSPACE/results"
STORAGE_DIR="$WORKSPACE/storage"
mkdir -p "$CONFIG_DIR" "$LOG_DIR" "$RESULT_DIR" "$STORAGE_DIR"

write_broker_config() {
    local node_id=$1
    local bind=$2
    local peers=$3
    local file="$CONFIG_DIR/broker_${node_id}.properties"
    cat >"$file" <<EOF
broker.node.id=$node_id
broker.local.host.port=$bind
broker.member.host.ports=$peers
broker.group.name=order-management
broker.storage.path=$STORAGE_DIR
broker.control.buffer.size=65536
broker.messages.buffer.size=1048576
broker.threading.mode=dedicated
broker.idle.strategy=backoff
broker.flow.control.enabled=true
broker.flow.control.peer.send.counters.enabled=true
broker.benchmark.latency.tracing.enabled=true
EOF
}

start_proc() {
    local label=$1
    shift
    local log="$LOG_DIR/${label}.log"
    echo "starting $label"
    "$@" >"$log" 2>&1 &
    LAST_PID=$!
    LAST_LOG=$log
    PIDS+=("$LAST_PID")
    LABELS+=("$label")
    LOGS+=("$log")
}

wait_for_log() {
    local label=$1
    local pid=$2
    local log=$3
    local marker=$4
    local timeout_sec=${5:-15}
    local deadline=$((SECONDS + timeout_sec))
    while (( SECONDS < deadline )); do
        if grep -q "$marker" "$log" 2>/dev/null; then
            return 0
        fi
        if ! kill -0 "$pid" 2>/dev/null; then
            echo "$label exited before marker '$marker'" >&2
            sed -n '1,160p' "$log" >&2 || true
            return 1
        fi
        sleep 0.05
    done
    echo "timed out waiting for $label marker '$marker'" >&2
    sed -n '1,160p' "$log" >&2 || true
    return 1
}

stop_pid() {
    local label=$1
    local pid=$2
    if kill -0 "$pid" 2>/dev/null; then
        echo "stopping $label"
        kill -TERM "$pid" 2>/dev/null || true
        local deadline=$((SECONDS + 5))
        while kill -0 "$pid" 2>/dev/null && (( SECONDS < deadline )); do
            sleep 0.05
        done
        if kill -0 "$pid" 2>/dev/null; then
            kill -KILL "$pid" 2>/dev/null || true
        fi
        wait "$pid" 2>/dev/null || true
    fi
}

stop_all() {
    local i
    for (( i=${#PIDS[@]}-1; i>=0; i-- )); do
        stop_pid "${LABELS[$i]}" "${PIDS[$i]}"
    done
}
trap stop_all EXIT INT TERM

write_broker_config 1 "127.0.0.1:19101" "2@127.0.0.1:19102"
write_broker_config 2 "127.0.0.1:19102" "1@127.0.0.1:19101"

start_proc broker-1 "$BIN_DIR/ringloom-broker" --config "$CONFIG_DIR/broker_1.properties"
BROKER1_PID=$LAST_PID
BROKER1_LOG=$LAST_LOG
start_proc broker-2 "$BIN_DIR/ringloom-broker" --config "$CONFIG_DIR/broker_2.properties"
BROKER2_PID=$LAST_PID
BROKER2_LOG=$LAST_LOG
wait_for_log broker-1 "$BROKER1_PID" "$BROKER1_LOG" "broker started" 15
wait_for_log broker-2 "$BROKER2_PID" "$BROKER2_LOG" "broker started" 15

service_common_args=(--storage-path "$STORAGE_DIR" --group order-management --idle-strategy backoff)

start_service() {
    local label=$1
    local exe=$2
    local node_id=$3
    local result_file=$4
    shift 4
    start_proc "$label" "$BIN_DIR/$exe" "${service_common_args[@]}" \
        --broker-node-id "$node_id" \
        --result-file "$result_file" \
        "$@"
    wait_for_log "$label" "$LAST_PID" "$LAST_LOG" "service ready" 15
}

start_service portfolio-service ringloom-sample-portfolio-service 1 "$RESULT_DIR/portfolio-service.json"
start_service execution-service ringloom-sample-execution-service 2 "$RESULT_DIR/execution-service.json"
if [[ "$PROFILE" == "full" ]]; then
    start_service matching-engine-node2 ringloom-sample-matching-engine 2 "$RESULT_DIR/matching-engine-node2.json" \
        --service-name matching-engine --leader-election
else
    start_service matching-engine-node2 ringloom-sample-matching-engine 2 "$RESULT_DIR/matching-engine-node2.json" \
        --service-name matching-engine
fi
MATCHING2_PID=$LAST_PID

if [[ "$PROFILE" == "full" ]]; then
    start_service matching-engine-node1 ringloom-sample-matching-engine 1 "$RESULT_DIR/matching-engine-node1.json" \
        --service-name matching-engine-extra --leader-election
fi

start_service risk-service-node1 ringloom-sample-risk-service 1 "$RESULT_DIR/risk-service-node1.json" \
    --service-name risk-service

if [[ "$PROFILE" == "full" ]]; then
    start_service risk-service-node2 ringloom-sample-risk-service 2 "$RESULT_DIR/risk-service-node2.json" \
        --service-name risk-service-extra --leader-routing
    RISK2_PID=$LAST_PID
fi

start_service order-gateway ringloom-sample-order-gateway 1 "$RESULT_DIR/order-gateway.json"

if [[ "$PROFILE" == "full" ]]; then
    stop_pid risk-service-node2 "$RISK2_PID"
    sleep 0.5
    start_service risk-service-node2-restarted ringloom-sample-risk-service 2 "$RESULT_DIR/risk-service-node2-restarted.json" \
        --service-name risk-service-extra --leader-routing
fi

start_service order-simulator ringloom-sample-order-simulator 1 "$RESULT_DIR/order-simulator.json" \
    --orders "$ORDERS" \
    --rate-per-sec "$RATE_PER_SEC" \
    --burst-size "$BURST_SIZE" \
    --message-size "$MESSAGE_SIZE" \
    --duration-sec "$DURATION_SEC"
SIM_PID=$LAST_PID

wait "$SIM_PID"
sleep 1

stop_all
trap - EXIT INT TERM

echo "order-management sample complete"
echo "workspace: $WORKSPACE"
echo "logs: $LOG_DIR"
echo "results: $RESULT_DIR"
echo "storage: $STORAGE_DIR"
if [[ -x "$BIN_DIR/ringloom-stat" ]]; then
    echo "monitor: $BIN_DIR/ringloom-stat --storage-path \"$STORAGE_DIR\" --group order-management"
else
    echo "monitor: zig build stat -- --storage-path \"$STORAGE_DIR\" --group order-management"
fi
