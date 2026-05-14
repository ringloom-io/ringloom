# Step 6: Sender Reliability and Scheduler

## Objective

Replace the TCP sender path with a reliable UDP sender that drains per-destination buffers fairly, writes to term logs, sends DATA frames, processes STATUS/NAK/RTT control frames, and retransmits from retained logs.

## Source touchpoints

| File/dir | Change |
|---|---|
| `src/broker/sender/sender_event_loop.zig` | Replace single-ring TCP loop with destination scheduler and UDP stream sender |
| `src/broker/sender/peer_sender.zig` | Replace TCP connection state with UDP peer/session state |
| `src/broker/sender/write_queue.zig` | Remove or repurpose; UDP uses term logs, not TCP write queues |
| `src/udp/*` | Use term log, frame codecs, endpoint |
| `src/common/memory/*` | Read send-buffer directory and update pressure states |
| `src/broker/cluster/*` | Admin messages use UDP DATA/admin frames |

## Sender state

Per peer:

```
PeerUdpSender
  node_id
  remote_addr
  session_id
  epoch
  setup_state
  last_frame_sent_ns
  last_frame_received_ns
```

Per stream/destination:

```
StreamSender
  stream_key
  stream_id
  destination_entry
  term_log
  sender_position
  sender_limit
  congestion_window
  retransmit_actions
  next_heartbeat_ns
  next_setup_ns
  pressure_state
```

## Duty cycle

1. Drain sender command queue.
2. Poll endpoint for sender-side control frames:
   - `STATUS` updates sender limit,
   - `NAK` schedules retransmit action,
   - `RTTM` updates RTT,
   - `ERROR` transitions session/stream state.
3. Process retransmit timers before new data.
4. Send SETUP for streams without confirmed setup.
5. Run destination scheduler:
   - choose next active destination,
   - skip if flow/congestion/term-log blocked,
   - read bounded messages from destination ring,
   - append to stream term log,
   - send DATA frames within MTU and window.
6. Send HEARTBEAT for idle streams.
7. Publish destination and stream counters.

## Scheduler

Use deficit round-robin:

```
for each active destination:
  if blocked: continue
  deficit += quantum
  while deficit >= next_message_cost and budgets allow:
    read one message
    append/send fragments
    deficit -= bytes_sent
```

Budgets:

- max destinations per cycle,
- max messages per destination per cycle,
- max bytes per stream per cycle,
- max retransmits per cycle,
- max endpoint packets per batch.

## Backpressure rules

The sender must not consume from a destination send buffer if:

1. stream setup is not confirmed and setup retry budget is exhausted,
2. `sender_position >= sender_limit`,
3. congestion window has no room,
4. active term cannot rotate because retained data is still unacknowledged,
5. endpoint reports persistent send pressure for that peer,
6. destination entry is draining/closed.

Other destination buffers remain eligible.

## Tests

### Unit tests

1. Scheduler visits active destinations fairly.
2. Flow-blocked destination is skipped without advancing ring head.
3. Congestion-blocked destination is skipped without advancing ring head.
4. Term-log-blocked destination is skipped without advancing ring head.
5. STATUS frame increases sender limit.
6. NAK schedules retransmit and duplicate overlapping NAK is suppressed.
7. Retransmit action sends from term log and then lingers.
8. HEARTBEAT is sent for idle stream only after interval.
9. SETUP retry backoff works and stops allocating new state.

### Integration tests

1. Two destination buffers, one blocked by zero receiver window, one open: open stream continues.
2. Drop first DATA packet and verify NAK-triggered retransmit.
3. Reorder DATA packets and verify sender handles delayed STATUS without corrupting position.
4. Endpoint send `WouldBlock` increments pressure counter and retries later.

### E2E tests

1. Cross-broker POSIX UDP route succeeds.
2. Cross-broker route continues to service B while service A's destination stream is flow-blocked.
3. Broker heartbeat remains healthy while one destination stream is blocked.

## Done criteria

- Sender no longer uses TCP sockets, TCP write queues, or TCP frame headers.
- Per-destination buffer consumption is independently gated.
- Retransmission uses term logs, not an ad hoc packet cache.
- Tests demonstrate loss recovery and destination isolation.

