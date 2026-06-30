#!/usr/bin/env bash
#
# bench-topic-cross.sh — Cross-broker topic replication latency benchmark.
#
# Architecture:
#   1. Publisher registers topic on Broker A (leader) — creates master queue
#      and triggers full-mesh replication to Broker B.
#   2. Wait for Broker B's replica dir.
#   3. Start subscriber on Broker B reading from replica.
#   4. Publish from Broker A with embedded timestamps.
#   5. Measure end-to-end latency: send timestamp → subscriber receive.
#
# Usage:
#   ./scripts/bench-topic-cross.sh [message_count] [message_size] [runs] [pace_us]
#
#   pace_us — inter-message delay in microseconds (default 20).
#             Pacing the sender keeps it from outrunning the receiver and
#             flooding the IPC term buffer, so measured latency reflects the
#             true per-message pipeline cost rather than term-buffer queueing.
#             Try 5–20 for a latency-focused run (50k–200k msgs/s), 0 for burst.

set -euo pipefail

MSG_COUNT=10000; MSG_SIZE=128; RUNS=1; PACE_US=20
[[ $# -ge 1 ]] && { MSG_COUNT="$1"; shift; }
[[ $# -ge 1 ]] && { MSG_SIZE="$1"; shift; }
[[ $# -ge 1 ]] && { RUNS="$1"; shift; }
[[ $# -ge 1 ]] && { PACE_US="$1"; shift; }

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BIN="$PROJECT_ROOT/zig-out/bin"
PUB_BIN="$BIN/ringloom-test-topic-aeron-bench"
SUB_BIN="$BIN/ringloom-test-topic-sub-bench"
BROKER="$BIN/ringloom-broker"
BEST_DIR="/tmp/ringloom-topic-cross-bench"

for b in "$PUB_BIN" "$SUB_BIN" "$BROKER"; do
    [[ -x "$b" ]] || { echo "ERROR: $b missing."; exit 1; }
done

TOPIC_NAME="bench-topic"
SIZE_TAG="${MSG_SIZE}B"

echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║  RingLoom Cross-Broker Topic Replication Benchmark           ║"
echo "╠══════════════════════════════════════════════════════════════╣"
printf "║  %-59s ║\n" "Payload:        $SIZE_TAG"
printf "║  %-59s ║\n" "Messages:       $MSG_COUNT (+ 5000 warmup)"
if [[ "$PACE_US" -gt 0 ]]; then
    RATE=$(awk "BEGIN {printf \"%.0f\", 1000000/$PACE_US}")
    printf "║  %-59s ║\n" "Sender pace:    ${PACE_US}us/msg (~${RATE} msgs/s)"
else
    printf "║  %-59s ║\n" "Sender pace:    burst (no pacing)"
fi
printf "║  %-59s ║\n" "Iterations:     $RUNS (best kept)"
echo "║  Path:   Pub→BrokerA (leader)→repl→BrokerB (replica)→Sub     ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

best_lat=999999999; best_json=""; best_run=0
mkdir -p "$BEST_DIR"

for run in $(seq 1 "$RUNS"); do
    WORK_DIR=$(mktemp -d /tmp/ringloom-cross-bench-XXXXXX)
    S1="$WORK_DIR/s1"; S2="$WORK_DIR/s2"
    A1="$WORK_DIR/a1"; A2="$WORK_DIR/a2"; LOGS="$WORK_DIR/logs"
    mkdir -p "$S1/ringloom-test/topics" "$S2/ringloom-test/topics" "$A1" "$A2" "$LOGS"
    trap "rm -rf $WORK_DIR" EXIT

    for node in 1 2; do
        if [[ $node -eq 1 ]]; then s="$S1"; a="$A1"; port=19301; peer_port=19302; peer_id=2
        else s="$S2"; a="$A2"; port=19302; peer_port=19301; peer_id=1; fi
        cat > "$WORK_DIR/broker_${node}.properties" << EOF
broker.node.id=$node
broker.local.host.port=127.0.0.1:$port
broker.member.host.ports=${peer_id}@127.0.0.1:${peer_port}
broker.group.name=ringloom-test
broker.storage.path=$s
broker.control.buffer.size=65536
broker.messages.buffer.size=1048576
broker.threading.mode=dedicated
broker.idle.strategy=yielding
broker.aeron.directory=$a
broker.aeron.delete.directory.on.start=true
broker.aeron.delete.directory.on.shutdown=true
broker.topics.enabled=true
broker.topics.path=$s/ringloom-test/topics
broker.topics.pub.stream.base=50000
broker.topics.repl.stream.base=40000
broker.benchmark.latency.tracing.enabled=true
EOF
    done

    echo "── Run $run/$RUNS ───────────────────────────────────────────"
    echo -n "  Starting brokers... "
    "$BROKER" --config "$WORK_DIR/broker_1.properties" > "$LOGS/b1.log" 2>&1 & P1=$!
    "$BROKER" --config "$WORK_DIR/broker_2.properties" > "$LOGS/b2.log" 2>&1 & P2=$!
    sleep 4

    if ! kill -0 $P1 2>/dev/null || ! kill -0 $P2 2>/dev/null; then
        echo "FAIL (broker crashed)"; kill $P1 $P2 2>/dev/null || true
        rm -rf "$WORK_DIR"; trap - EXIT; continue
    fi
    echo "started"

    # ── Step 1: Publisher registers topic (creates master + full-mesh replica) ─
    # The broker assigns topic_id = Wyhash(0, name); the bench binary derives the
    # same id and prints TOPIC_ID/TOPIC_DIR, which we parse to locate the queue.
    echo -n "  Registering topic via publisher... "
    REG_OUT=$(timeout 15 "$PUB_BIN" \
        --aeron-dir "$A1" --pub-stream-id 50001 \
        --message-count 1 --warmup-count 0 \
        --message-size "$MSG_SIZE" \
        --storage-path "$S1" --register-topic "$TOPIC_NAME" \
        2> "$LOGS/reg.err" || true)
    TOPIC_ID=$(echo "$REG_OUT" | sed -n 's/^TOPIC_ID=0x\([0-9a-f]*\).*/\1/p' | head -1)
    TOPIC_DIR=$(echo "$REG_OUT" | sed -n 's/^TOPIC_DIR=\(.*\)$/\1/p' | head -1)
    if [[ -z "$TOPIC_ID" || -z "$TOPIC_DIR" ]]; then
        echo "FAIL (no topic id)"; cat "$LOGS/reg.err" 2>/dev/null | tail -3
        kill $P1 $P2 2>/dev/null || true; rm -rf "$WORK_DIR"; trap - EXIT; continue
    fi
    echo "registered (topic_id=0x$TOPIC_ID)"

    # ── Step 2: Wait for replica dir on Broker B ────────────────────────
    REPLICA_DIR="$S2/ringloom-test/topics/$TOPIC_DIR"
    echo -n "  Waiting for replica on Broker B... "
    for i in $(seq 1 40); do
        if [[ -d "$REPLICA_DIR" ]]; then echo "ready (${i}x250ms)"; break; fi
        sleep 0.25
    done
    if [[ ! -d "$REPLICA_DIR" ]]; then
        echo "FAIL (timeout)"; kill $P1 $P2 2>/dev/null || true
        rm -rf "$WORK_DIR"; trap - EXIT; continue
    fi

    # ── Step 3: Start subscriber on Broker B FIRST ───────────────────────
    # The subscriber tails the replica queue live (from index 0) so it measures
    # true end-to-end replication latency: send-timestamp (embedded by the
    # publisher) → subscriber-receive. Starting it before publishing avoids
    # measuring only the backlog-drain time.
    echo -n "  Starting subscriber... "
    "$SUB_BIN" --queue-dir "$REPLICA_DIR" --expected-count "$MSG_COUNT" \
        --timeout-sec 30 > "$LOGS/sub.log" 2>&1 & SP=$!
    echo "started (pid=$SP)"
    # Give the subscriber a moment to open the tailer before we flood the path.
    sleep 0.5

    # ── Step 4: Publish from Broker A with embedded send timestamps ──────
    # A generous warmup primes the receiver's drain loop into steady state
    # before the measured run, so the measured messages don't inherit cold-start
    # term-buffer queueing delay. Warmup messages carry the warmup phase flag
    # and are drained (not counted) by the subscriber.
    echo -n "  Publishing $MSG_COUNT msgs... "
    PACE_ARG=()
    [[ "$PACE_US" -gt 0 ]] && PACE_ARG=(--pace-us "$PACE_US")
    PUB_OUT=$(timeout 30 "$PUB_BIN" \
        --aeron-dir "$A1" --pub-stream-id 50001 \
        --topic-id "0x$TOPIC_ID" --message-count "$MSG_COUNT" \
        --message-size "$MSG_SIZE" --warmup-count 5000 --latency 1 \
        "${PACE_ARG[@]}" \
        2>/dev/null || true)

    if echo "$PUB_OUT" | grep -q "TOPIC_AERON_BENCH_JSON"; then
        PUB_J=$(echo "$PUB_OUT" | sed -n '/TOPIC_AERON_BENCH_JSON<<EOF/,/EOF/p' | grep -v 'TOPIC_AERON_BENCH_JSON\|EOF')
        TPUT=$(echo "$PUB_J" | python3 -c "import sys,json; print(json.load(sys.stdin)['tput'])" 2>/dev/null || echo "0")
        echo "done ($TPUT msgs/sec)"
    else
        echo "FAIL"; PUB_J=""
    fi

    # ── Step 5: Wait for subscriber to receive all messages ──────────────
    echo -n "  Waiting for subscriber... "
    wait $SP 2>/dev/null || true

    if [[ -f "$LOGS/sub.log" ]] && grep -q "TOPIC_SUB_JSON" "$LOGS/sub.log"; then
        SUB_J=$(sed -n '/TOPIC_SUB_JSON<<EOF/,/EOF/p' "$LOGS/sub.log" | grep -v 'TOPIC_SUB_JSON\|EOF')
        RECV=$(echo "$SUB_J" | python3 -c "import sys,json; print(json.load(sys.stdin)['received'])" 2>/dev/null || echo "0")
        LAT_AVG=$(echo "$SUB_J" | python3 -c "import sys,json; print(json.load(sys.stdin)['lat_avg_ns'])" 2>/dev/null || echo "0")
        LAT_MIN=$(echo "$SUB_J" | python3 -c "import sys,json; print(json.load(sys.stdin)['lat_min_ns'])" 2>/dev/null || echo "0")
        LAT_MAX=$(echo "$SUB_J" | python3 -c "import sys,json; print(json.load(sys.stdin)['lat_max_ns'])" 2>/dev/null || echo "0")
        P2A_AVG=$(echo "$SUB_J" | python3 -c "import sys,json; print(json.load(sys.stdin).get('pub_to_a_avg_ns',0))" 2>/dev/null || echo "0")
        A2S_AVG=$(echo "$SUB_J" | python3 -c "import sys,json; print(json.load(sys.stdin).get('a_to_sub_avg_ns',0))" 2>/dev/null || echo "0")
        LAT_US=$(awk "BEGIN {printf \"%.2f\", $LAT_AVG/1000}")
        P2A_US=$(awk "BEGIN {printf \"%.2f\", $P2A_AVG/1000}")
        A2S_US=$(awk "BEGIN {printf \"%.2f\", $A2S_AVG/1000}")
        echo "rcvd=$RECV e2e=${LAT_US}us (pub->A=${P2A_US}us A->sub=${A2S_US}us) min=$LAT_MIN max=$LAT_MAX"

        if [[ "$RECV" -gt 0 && "$LAT_AVG" -lt "$best_lat" ]]; then
            best_lat=$LAT_AVG; best_json="$SUB_J"; best_run=$run
            echo "    -> new best"
        fi
    else
        echo "FAIL (no subscriber output)"
    fi

    kill $P1 $P2 2>/dev/null || true; wait 2>/dev/null || true
    rm -rf "$WORK_DIR"; trap - EXIT
    echo ""
done

[[ -z "$best_json" ]] && { echo "No successful runs."; exit 1; }

LAT_AVG=$(echo "$best_json" | python3 -c "import sys,json; print(json.load(sys.stdin)['lat_avg_ns'])" 2>/dev/null || echo "0")
LAT_MIN=$(echo "$best_json" | python3 -c "import sys,json; print(json.load(sys.stdin)['lat_min_ns'])" 2>/dev/null || echo "0")
LAT_MAX=$(echo "$best_json" | python3 -c "import sys,json; print(json.load(sys.stdin)['lat_max_ns'])" 2>/dev/null || echo "0")
P2A_AVG=$(echo "$best_json" | python3 -c "import sys,json; print(json.load(sys.stdin).get('pub_to_a_avg_ns',0))" 2>/dev/null || echo "0")
P2A_MIN=$(echo "$best_json" | python3 -c "import sys,json; print(json.load(sys.stdin).get('pub_to_a_min_ns',0))" 2>/dev/null || echo "0")
P2A_MAX=$(echo "$best_json" | python3 -c "import sys,json; print(json.load(sys.stdin).get('pub_to_a_max_ns',0))" 2>/dev/null || echo "0")
A2S_AVG=$(echo "$best_json" | python3 -c "import sys,json; print(json.load(sys.stdin).get('a_to_sub_avg_ns',0))" 2>/dev/null || echo "0")
A2S_MIN=$(echo "$best_json" | python3 -c "import sys,json; print(json.load(sys.stdin).get('a_to_sub_min_ns',0))" 2>/dev/null || echo "0")
A2S_MAX=$(echo "$best_json" | python3 -c "import sys,json; print(json.load(sys.stdin).get('a_to_sub_max_ns',0))" 2>/dev/null || echo "0")
RECV=$(echo "$best_json" | python3 -c "import sys,json; print(json.load(sys.stdin)['received'])" 2>/dev/null || echo "0")

echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║            Results (best of $RUNS)                               ║"
echo "╠══════════════════════════════════════════════════════════════╣"
printf "║  %-59s ║\n" "Best run:       #$best_run"
printf "║  %-59s ║\n" "Messages sent:  $MSG_COUNT"
printf "║  %-59s ║\n" "Messages rcvd:  $RECV"
printf "║  %-59s ║\n" "Payload:        $SIZE_TAG"
printf "║  %-59s ║\n" "Avg latency:    $(awk "BEGIN {printf \"%.2f\", $LAT_AVG/1000}") us (${LAT_AVG} ns)"
printf "║  %-59s ║\n" "Min latency:    ${LAT_MIN} ns"
printf "║  %-59s ║\n" "Max latency:    ${LAT_MAX} ns"
echo "╠══════════════════════════════════════════════════════════════╣"
echo "║  Stage breakdown (send -> broker-A ingress -> sub receive)   ║"
printf "║  %-59s ║\n" "pub->A: avg=$(awk "BEGIN {printf \"%.2f\", $P2A_AVG/1000}")us min=$P2A_MIN max=$P2A_MAX ns"
printf "║  %-59s ║\n" "A->sub: avg=$(awk "BEGIN {printf \"%.2f\", $A2S_AVG/1000}")us min=$A2S_MIN max=$A2S_MAX ns"
echo "╚══════════════════════════════════════════════════════════════╝"
echo "$best_json" | python3 -m json.tool > "$BEST_DIR/cross-broker-${MSG_SIZE}B.json" 2>/dev/null || true
echo "JSON → $BEST_DIR/cross-broker-${MSG_SIZE}B.json"
