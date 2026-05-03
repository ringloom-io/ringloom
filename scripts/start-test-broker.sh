#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

workspace=""
group="ringloom-java-test"
node_id="1"
host="127.0.0.1"
port="19001"
bin_dir="zig-out/bin"
timeout_sec="10"
mode=""

usage() {
    cat <<'EOF'
Usage:
  ./scripts/start-test-broker.sh [--workspace DIR] [--group NAME] [--node-id N]
                                 [--host HOST] [--port PORT] [--bin-dir DIR]
                                 [--timeout SEC] --foreground
  ./scripts/start-test-broker.sh [--workspace DIR] [--group NAME] [--node-id N]
                                 [--host HOST] [--port PORT] [--bin-dir DIR]
                                 [--timeout SEC] --daemon
  ./scripts/start-test-broker.sh --workspace DIR --stop
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --workspace)
            workspace="$2"
            shift 2
            ;;
        --group)
            group="$2"
            shift 2
            ;;
        --node-id)
            node_id="$2"
            shift 2
            ;;
        --host)
            host="$2"
            shift 2
            ;;
        --port)
            port="$2"
            shift 2
            ;;
        --bin-dir)
            bin_dir="$2"
            shift 2
            ;;
        --timeout)
            timeout_sec="$2"
            shift 2
            ;;
        --foreground)
            mode="foreground"
            shift
            ;;
        --daemon)
            mode="daemon"
            shift
            ;;
        --stop)
            mode="stop"
            shift
            ;;
        --help|-h)
            usage
            exit 0
            ;;
        *)
            echo "unknown option: $1" >&2
            usage >&2
            exit 1
            ;;
    esac
done

if [[ -z "$mode" ]]; then
    echo "one of --foreground, --daemon, or --stop is required" >&2
    exit 1
fi

if [[ -z "$workspace" ]]; then
    workspace="$(mktemp -d /tmp/ringloom-java-it-XXXXXX)"
fi

config_dir="$workspace/config"
logs_dir="$workspace/logs"
storage_dir="$workspace/storage"
pid_file="$workspace/broker.pid"
config_file="$config_dir/broker_${node_id}.properties"
log_file="$logs_dir/broker_${node_id}.log"
broker_bin="$bin_dir/ringloom-broker"

ensure_workspace() {
    mkdir -p "$config_dir" "$logs_dir" "$storage_dir"
}

write_config() {
    cat >"$config_file" <<EOF
broker.node.id=$node_id
broker.local.host.port=$host:$port
broker.group.name=$group
broker.storage.path=$storage_dir
broker.control.buffer.size=65536
broker.messages.buffer.size=1048576
broker.threading.mode=dedicated
broker.idle.strategy=backoff
EOF
}

print_env() {
    local pid="$1"
    printf 'RINGLOOM_BROKER_PID=%s\n' "$pid"
    printf 'RINGLOOM_BROKER_CONFIG=%s\n' "$config_file"
    printf 'RINGLOOM_BROKER_LOG=%s\n' "$log_file"
    printf 'RINGLOOM_STORAGE_PATH=%s\n' "$storage_dir"
    printf 'RINGLOOM_GROUP=%s\n' "$group"
}

wait_for_ready() {
    local pid="$1"
    local deadline=$((SECONDS + timeout_sec))

    while (( SECONDS < deadline )); do
        if [[ -f "$log_file" ]] && grep -Eq 'broker started|broker ready' "$log_file"; then
            return 0
        fi

        if ! kill -0 "$pid" 2>/dev/null; then
            echo "broker process $pid exited before becoming ready" >&2
            if [[ -f "$log_file" ]]; then
                cat "$log_file" >&2
            fi
            return 1
        fi

        sleep 0.1
    done

    echo "timed out waiting for broker readiness marker" >&2
    if [[ -f "$log_file" ]]; then
        cat "$log_file" >&2
    fi
    return 1
}

stop_broker() {
    if [[ ! -f "$pid_file" ]]; then
        echo "pid file not found: $pid_file" >&2
        exit 1
    fi

    local pid
    pid="$(cat "$pid_file")"
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
        kill "$pid"
        for _ in $(seq 1 100); do
            if ! kill -0 "$pid" 2>/dev/null; then
                rm -f "$pid_file"
                exit 0
            fi
            sleep 0.1
        done
        kill -9 "$pid"
    fi

    rm -f "$pid_file"
}

start_broker() {
    if [[ ! -x "$broker_bin" ]]; then
        echo "broker executable not found: $broker_bin" >&2
        exit 1
    fi

    ensure_workspace
    write_config
    : >"$log_file"

    "$broker_bin" --config "$config_file" >>"$log_file" 2>&1 &
    local pid=$!
    echo "$pid" >"$pid_file"

    wait_for_ready "$pid"
    print_env "$pid"

    if [[ "$mode" == "foreground" ]]; then
        trap 'if kill -0 "$pid" 2>/dev/null; then kill "$pid"; wait "$pid" || true; fi' EXIT INT TERM
        wait "$pid"
    fi
}

if [[ "$mode" == "stop" ]]; then
    stop_broker
else
    start_broker
fi
