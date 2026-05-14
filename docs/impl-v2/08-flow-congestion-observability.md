# Step 8: Flow Control, Congestion Control, and Observability

## Objective

Complete the control loop around reliable UDP: receiver windows, sender limits, congestion windows, pressure propagation into destination send-buffer entries, and monitoring/counters.

## Source touchpoints

| File/dir | Change |
|---|---|
| `src/udp/flow_control.zig` | Position-based flow-control strategies |
| `src/udp/congestion_control.zig` | Static congestion control and RTT hooks |
| `src/common/memory/*` | Pressure state and counters in send-buffer directory/transport region |
| `src/common/monitoring/*` | Metadata reader and Prometheus exporter for UDP counters |
| `src/service/flow_control_config.zig` | Update service-facing backpressure config |
| `src/service/service_client.zig` | Read per-destination pressure instead of global send-buffer state |
| `src/broker/control/*` | Refresh remote service capacity and stream pressure state |

## Flow control

Default: unicast position-based receiver window.

```
receiver_position = consumed_position
receiver_edge = receiver_position + receiver_window
sender_limit = min(receiver_edge, congestion_limit)
```

Receiver window is based on:

- configured maximum,
- target service ring remaining capacity,
- receive reassembly retention,
- stream liveness,
- admin/heartbeat priority.

## Congestion control

Initial strategy: static window with RTT measurement.

State:

```
StaticCongestionControl
  initial_window
  max_window
  rtt_ewma_ns
  loss_observed
```

Rules:

1. Start with `min(initial_window, term_length / 2)`.
2. Keep window fixed unless configured lower by receiver pressure.
3. Record RTTM samples.
4. On loss, force STATUS and count loss but do not shrink below configured minimum in the initial strategy.

CUBIC can be introduced later behind the same interface.

## Pressure propagation

Destination `SendBufferEntry.pressure_state` should reflect the stream's limiting condition:

| State | Meaning |
|---|---|
| `normal` | Sender may consume this destination buffer |
| `flow_blocked` | Receiver window exhausted |
| `congested` | Congestion window exhausted |
| `term_blocked` | Retained term cannot rotate |
| `peer_down` | Peer/session not established |
| `draining` | Control plane is closing buffer |

ServiceClient reads this state before send if flow control is enabled.

## Counters

Add counters at three scopes:

1. Endpoint counters:
   - packets/bytes sent and received,
   - invalid frames,
   - MTU errors,
   - endpoint send pressure,
   - AF_XDP fallback/drops.
2. Stream counters:
   - sender position,
   - sender limit,
   - receiver position,
   - high-water mark,
   - NAKs sent/received,
   - retransmits sent,
   - duplicate packets,
   - stale session packets,
   - RTT EWMA.
3. Destination buffer counters:
   - bytes/messages pending,
   - full events,
   - flow-blocked cycles,
   - congestion-blocked cycles,
   - peer-down drops,
   - service backpressure events.

## Tests

### Unit tests

1. STATUS updates sender limit from receiver position/window.
2. Sender limit never moves backward unless session resets.
3. Receiver window becomes zero or small when target service capacity is low.
4. Pressure state transitions are correct for flow, congestion, term, peer, and draining states.
5. ServiceClient maps pressure states to existing send errors:
   - flow/congestion -> `BackPressure` or `PeerCongested`,
   - peer down -> `PeerDisconnected`,
   - destination full -> `SendBufferFull`.
6. RTT EWMA updates from RTTM samples.
7. Counters increment for NAK, retransmit, duplicate, stale session, and reassembly drop events.

### Integration tests

1. Fill target service ring and verify receiver advertises reduced window.
2. Sender observes zero window and stops consuming only that destination.
3. After target service drains, STATUS opens window and sender resumes.
4. Counter snapshots expose stream and destination pressure.

### Observability tests

1. Metadata reader can read v2 send-buffer directory entries.
2. Prometheus exporter emits UDP and destination-buffer metrics.
3. Existing observability e2e is updated to assert representative v2 counters.

## Done criteria

- Backpressure is per destination/stream, not global.
- Service-facing flow control reads v2 pressure and capacity state.
- Monitoring replaces TCP counters with UDP, stream, and destination counters.
- Tests prove pressure closes and reopens one stream without affecting another.

