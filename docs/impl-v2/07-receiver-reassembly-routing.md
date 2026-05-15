# Step 7: Receiver Reassembly and Routing

## Objective

Replace the TCP receiver path with a UDP receiver that validates sessions, inserts DATA into stream windows, detects gaps, sends STATUS/NAK frames, reassembles fragmented messages, and routes complete messages to local services.

## Source touchpoints

| File/dir | Change |
|---|---|
| `src/broker/receiver/receiver_event_loop.zig` | Replace TCP accept/read/frame parser with UDP packet poll/dispatch |
| `src/broker/receiver/peer_receiver.zig` | Replace TCP read state with UDP peer/session/stream receive state |
| `src/broker/receiver/message_router.zig` | Route reassembled UDP DATA payloads and admin frames |
| `src/udp/*` | Use codecs, loss detector, reassembly state |
| `src/broker/cluster/admin_dispatch.zig` | Admin messages arrive as UDP admin DATA frames |
| `src/common/message/message_assembler.zig` | Reuse or replace with stream-aware reassembly |

## Receiver state

Per peer/session:

```
PeerUdpReceiver
  node_id
  remote_addr
  session_id
  epoch
  last_valid_frame_ns
  streams
```

Per stream:

```
StreamReceiver
  stream_id
  stream_key
  receive_window
  receive_buffer
  frame_meta
  high_water_mark
  rebuild_position
  consumed_position
  gap_tracker
  pending_message_state
  next_status_ns
  next_nak_ns
```

The receiver event loop still uses one shared RX staging buffer for packet polling, but committed packet retention is per stream in `receive_buffer`.

## Packet handling

1. Validate common header.
2. Validate source address and configured peer.
3. Validate session ID and epoch.
4. Dispatch:
   - `SETUP`: validate and create/refresh stream.
   - `DATA`: validate route and insert into stream.
   - `RTTM`: reply or update RTT.
   - `HEARTBEAT`: update liveness and position.
   - `ERROR`: notify control loop.
5. Invalid packets increment typed counters and do not crash the loop.

## DATA insertion

1. Reject packet beyond flow-control overrun guard.
2. Drop packet below stale/dedup window.
3. Copy DATA payload into the stream receive buffer if the target slot is empty.
4. Ignore duplicate if already present.
5. Advance high-water mark.
6. Track gaps between rebuild position and high-water mark.
7. Publish frame metadata only after payload copy completes.
8. Schedule delayed NAK for persistent gaps.

## Reassembly and delivery

1. Rebuild contiguous DATA frames in term order.
2. For unfragmented messages, route directly when target service buffer has capacity.
3. For fragmented messages:
   - verify route metadata consistency while rebuilding contiguous fragments,
   - rebuild directly from the stream receive buffer by default,
   - use a bounded per-message scratch/reassembly arena only when direct rebuild is impractical,
   - route only after all bytes are present.
4. If target service buffer is full:
   - keep the complete message pinned in the stream receive buffer or bounded pending-message state if retention allows,
   - advertise reduced receiver window,
   - do not block other streams.
5. If retention is exhausted:
   - drop according to policy,
   - increment `reassembly_retention_drops` or `service_backpressure_drops`,
   - advance or close stream according to configured reliability policy.

## STATUS and NAK sending

Send STATUS when:

- consumed position advances by at least `window / 4`,
- receiver window changes from pressured to normal or normal to pressured,
- setup requested status,
- heartbeat/status interval expires,
- loss/congestion strategy forces status.

Send NAK when:

- gap remains after initial NAK delay,
- retry delay expires and gap still exists,
- NAK rate limit permits.

## Tests

### Unit tests

1. SETUP creates stream only after validation.
2. DATA from wrong source node/session is rejected.
3. In-order DATA advances rebuild and consumed positions.
4. Out-of-order DATA creates gap and schedules NAK.
5. Duplicate DATA is counted and ignored.
6. Stale session DATA is dropped.
7. Fragmented message reassembles only after all fragments arrive.
8. Inconsistent route metadata across fragments drops the message.
9. Pending-message exhaustion drops oldest or configured victim.
10. Target service full reduces advertised receiver window.
11. STATUS scheduling follows window advancement and interval rules.

### Integration tests

1. Inject drop: receiver sends NAK; retransmit fills gap; message routes once.
2. Inject reorder: receiver waits/rebuilds without duplicate delivery.
3. Inject duplicate: receiver delivers message once.
4. Fill one target service ring: only that stream's window closes.
5. Admin heartbeat/election frames continue while application stream is pressured.

### E2E tests

1. Cross-broker fragmented messages of 4 KiB, 64 KiB, and mixed sizes round trip.
2. Slow consumer on broker B backpressures only its stream; another service on B remains responsive.
3. Broker restart creates new epoch; old-session packets are ignored.

## Done criteria

- Receiver no longer accepts TCP connections or parses TCP byte streams.
- Complete messages are delivered in stream order and at most once within a session.
- Receiver emits STATUS/NAK frames and updates per-stream pressure.
- Loss, reorder, duplication, and slow target service tests pass.
