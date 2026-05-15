# RingLoom Broker Architecture v2

**Reliable UDP transport, optional AF_XDP kernel bypass, and per-remote-service send buffers**

This proposal replaces the current broker-to-broker TCP transport with a reliable UDP transport inspired by Aeron. It also replaces the broker-wide outbound send ring buffer with independent send buffers per remote service destination, so congestion for one destination does not stop unrelated destinations from making progress.

This is a breaking architecture. Backward compatibility with the TCP protocol, TCP config keys, or the v1 broker metadata layout is not a design goal.

---

## 1. Goals and non-goals

### Goals

1. **Reliable UDP instead of TCP for cross-host transport.** Reliability, ordering, retransmission, flow control, and congestion control move into RingLoom.
2. **Aeron-style transport mechanics where they fit RingLoom.** Use position-based term logs, SETUP/DATA/status/NAK/RTT frames, receiver windows, loss detection, and retransmission from retained sender logs.
3. **RingLoom-native routing.** Keep `(node_id, service_id, template_id, correlation_id)` routing in the wire protocol instead of exposing Aeron channels to services.
4. **Optional AF_XDP/eBPF kernel bypass.** Redirect only configured RingLoom UDP port traffic into AF_XDP sockets. All other traffic must return `XDP_PASS` to the kernel.
5. **POSIX UDP fallback.** If AF_XDP is unavailable, not configured, unsupported by the NIC/kernel, or fails startup validation, the broker uses non-blocking POSIX UDP.
6. **Per-remote-service send buffers.** A remote service destination has an independent shared-memory outbound buffer. Sender backpressure pauses only that destination buffer.
7. **Bounded hot-path allocation.** All transport buffers, term logs, retransmit state, receive-window state, pending-message state, and scheduler entries are preallocated or allocated during control-plane setup.
8. **Detailed observability.** Packet loss, retransmission, flow-control stalls, congestion windows, AF_XDP fallback, and per-destination send-buffer pressure must be visible through counters.

### Non-goals

1. **TCP compatibility.** The removed v1 TCP transport is not preserved as a compatibility layer.
2. **General Aeron compatibility.** The protocol borrows Aeron concepts but is not wire-compatible with Aeron.
3. **Multicast in the initial design.** The first v2 transport is unicast broker-to-broker. The protocol leaves room for multicast-style NAK suppression later.
4. **End-to-end exactly-once delivery across broker restart.** The transport provides reliable ordered delivery within a live broker session. Broker restart creates a new session epoch; unacknowledged data from the previous session is abandoned.
5. **Runtime hot-swap between AF_XDP and POSIX on the same port.** Fallback is a startup-time decision for the first implementation. Runtime fallback requires transport teardown and session re-establishment.

---

## 2. Key design decisions

### 2.1 Reliable UDP model

RingLoom v2 adopts Aeron's **term-log and position-based flow-control model**:

- Each unidirectional stream has a sender term log.
- Sent data remains in the term log until receivers acknowledge a position beyond it or the stream/session is closed.
- Retransmissions copy packets from the term log; there is no separate packet cache that can disagree with the publication log.
- The receiver advertises a consumed position plus a receiver window.
- The sender may transmit only while `sender_position < sender_limit`, where `sender_limit` is derived from status messages and congestion control.
- Loss is detected by gaps in received term positions and repaired by NAKs.

This avoids mixing TCP-like byte streams with UDP datagrams and keeps retransmission storage, flow control, and NAK addressing based on the same position arithmetic.

### 2.2 Meaning of "single send/receive buffer"

The v2 transport uses one **I/O staging buffer** per event-loop direction:

- Sender event loop: one reusable scratch buffer for encoding control frames, headers, and small packet batches before handoff to POSIX UDP or AF_XDP TX descriptors.
- Receiver event loop: one reusable scratch buffer for decoding packets and dispatching frame handlers.

Reliability state is separate and bounded:

- Send retransmission state lives in per-stream term logs.
- Receive ordering and backpressure state lives in per-stream bounded receive windows, gap trackers, and bounded pending-message state.
- ACK/status/NAK bookkeeping lives in per-stream control state.

The staging buffers must not be confused with durable in-flight storage. Retransmission and reassembly require retained per-stream state.

### 2.3 Send-buffer key

A broker metadata send buffer is keyed by **remote destination service**:

```
SendBufferKey = {
    target_node_id: i16,
    target_service_id: i32,
}
```

All local services sending to the same remote service share that destination buffer. This matches the user goal of isolating remote-service backpressure:

- If remote service `(node=2, service=7)` is slow, the sender stops consuming only that destination buffer.
- Buffers for `(node=2, service=8)` and `(node=3, service=4)` continue to be drained.

This does not isolate multiple local producers that target the same slow remote service from each other. Full source/destination pair isolation can be added later with:

```
SendBufferKey = {
    source_service_id,
    target_node_id,
    target_service_id,
}
```

The default remote-destination key is chosen to avoid an unbounded `local_services x remote_services` buffer explosion while satisfying the main head-of-line blocking problem.

---

## 3. System overview

```
                         Host A                                Host B
  +------------------------------------------------+   +------------------------------+
  | Services                                       |   | Services                     |
  |                                                |   |                              |
  |   Service 1 ----+                              |   |       +---- Service 7        |
  |   Service 2 ----+                              |   |       |                      |
  |                 |                              |   |       |                      |
  |                 v                              |   |       v                      |
  |  +------------------------------+              |   |  +------------------------+  |
  |  | Broker metadata              |              |   |  | Broker metadata        |  |
  |  |                              |              |   |  |                        |  |
  |  | control ring                 |              |   |  | control ring           |  |
  |  | send-buffer directory        |              |   |  | send-buffer directory  |  |
  |  |   dst 2:7 -> ring buffer ----+----+         |   |  |                        |  |
  |  |   dst 2:8 -> ring buffer ----+--+ |         |   |  +------------------------+  |
  |  +------------------------------+  | |         |   |                              |
  |                                    | |         |   |                              |
  |  +---------------------------------v-v------+  |   |  +------------------------+  |
  |  | Sender event loop                         |  |   |  | Receiver event loop    |  |
  |  | - fair scan active destination buffers    |  |   |  | - UDP/AF_XDP RX poll   |  |
  |  | - skip flow/congestion-blocked streams    |  |   |  | - loss detection       |  |
  |  | - encode DATA, SETUP, HEARTBEAT           |  |   |  | - reassembly           |  |
  |  | - retransmit from term logs               |  |   |  | - route complete msgs  |  |
  |  +------------------+------------------------+  |   |  +-----------+------------+  |
  |                     |                           |   |              |               |
  |       POSIX UDP or AF_XDP/eBPF                  |   |       target service RB      |
  +---------------------+---------------------------+---+--------------+---------------+
                        | Reliable UDP frames
                        v
```

The local same-host IPC path remains shared-memory direct delivery from service to service. The broker is still only involved in cross-host messages and control-plane tasks.

---

## 4. Shared memory layout v2

### 4.1 Broker metadata file

The v1 broker metadata contains one control ring buffer and one broker-wide send ring buffer. V2 replaces the single send buffer with a directory plus variable-sized per-destination send buffers.

```
+------------------------------------------------+
| Metadata Header (512 bytes)                    |
| - metadata_version = 2                         |
| - node_id, pid, timestamps                     |
| - control_buffer_length                        |
| - send_directory_offset/length                 |
| - send_region_offset/length                    |
| - flow_control_region offsets                  |
| - monitoring tail offsets                      |
+------------------------------------------------+
| Control Ring Buffer                            |
| Service -> broker control messages             |
+------------------------------------------------+
| Send Buffer Directory                          |
| Fixed-size entries, cache-line aligned         |
+------------------------------------------------+
| Per-Destination Send Buffer Region             |
| Entry 0 ring buffer data + trailer             |
| Entry 1 ring buffer data + trailer             |
| ...                                            |
+------------------------------------------------+
| Transport State Region                         |
| Optional shared counters visible to services   |
+------------------------------------------------+
| Monitoring Tail                                |
| counters, counter metadata, error log          |
+------------------------------------------------+
```

### 4.2 Send buffer directory

Each directory entry describes one remote destination buffer:

```
SendBufferEntry (128 bytes)
  state: u8                 // free, provisioning, active, draining, closed
  pressure_state: u8        // unknown, normal, flow_blocked, congested, closed
  generation: u16
  target_node_id: i16
  target_service_id: i32
  stream_id: u32
  ring_offset: u64
  ring_capacity: u32
  max_message_length: u32
  producer_count: u32
  last_activity_ns: i64
  bytes_pending: u64
  messages_pending: u64
  lifetime_messages_written: u64
  lifetime_messages_dropped: u64
  padding...
```

State transitions:

```
free -> provisioning -> active -> draining -> closed -> free
```

- `provisioning`: control loop owns creation and initializes the ring buffer.
- `active`: services may write; sender may consume.
- `draining`: no new service handles are issued; sender may drain or drop according to shutdown policy.
- `closed`: sender stops consuming; control loop can reclaim after generation bump.

### 4.3 Service access

Services do not write to a global send buffer. A service client resolves a remote service instance to a send-buffer entry:

1. Discovery reports remote service `(target_node_id, target_service_id)`.
2. ServiceClient asks the broker control plane for a destination send-buffer handle, or uses a handle already attached to the discovery response.
3. The handle contains directory index, generation, ring offset, capacity, and stream ID.
4. On send, ServiceClient validates that the directory entry is still `active` and generation matches.
5. ServiceClient writes a RingLoom application envelope into that destination ring buffer.

If no destination send buffer exists:

- For non-blocking send APIs, return `NoAvailableInstance` or `BackPressure` depending on discovery state.
- For spin/backpressure strategies, spin only until the provisioning deadline expires.
- Do not enqueue into a hidden global fallback buffer.

### 4.4 Buffer sizing and caps

V2 adds explicit limits:

| Limit | Purpose |
|---|---|
| `broker.send.buffers.max.entries` | Maximum active destination buffers |
| `broker.send.buffers.default.size` | Capacity per destination buffer |
| `broker.send.buffers.max.total.bytes` | Shared-memory budget guard |
| `broker.send.buffers.idle.timeout.ms` | Reclaim inactive buffers |
| `broker.send.buffers.drain.timeout.ms` | Max time in draining before forced close |

If the directory is full, control-plane provisioning fails and the service sees a send failure. The broker must not evict an active destination buffer without moving it through `draining`.

---

## 5. Reliable UDP wire protocol

All fields are little-endian. Packet sizes must fit the configured path MTU after Ethernet/IP/UDP headers. The broker sets DF for POSIX UDP where supported and treats IP fragmentation as a transport error.

### 5.1 Frame types

| Type | Name | Direction | Purpose |
|---:|---|---|---|
| 1 | `SETUP` | sender -> receiver | Establish stream/session parameters |
| 2 | `SETUP_RESPONSE` | receiver -> sender | Validate peer and confirm stream |
| 3 | `DATA` | sender -> receiver | Application/admin data fragment |
| 4 | `STATUS` | receiver -> sender | ACK/flow-control position and window |
| 5 | `NAK` | receiver -> sender | Request retransmission of a missing range |
| 6 | `RTTM` | bidirectional | RTT measurement |
| 7 | `HEARTBEAT` | bidirectional | Liveness and idle position advertisement |
| 8 | `ERROR` | bidirectional | Protocol or session error |

### 5.2 Common header

```
CommonHeader (16 bytes)
  u32 magic              // "RUD2"
  u8  version            // 2
  u8  frame_type
  u16 flags
  u16 header_length
  u16 frame_length       // complete UDP payload length
  u32 session_id
```

`session_id` is derived from the sender node, target node, and startup epoch. It changes after broker restart.

### 5.3 DATA header

```
DataHeader (64 bytes total, includes CommonHeader)
  CommonHeader common
  u32 stream_id
  i32 term_id
  u32 term_offset
  u64 message_id
  u32 fragment_offset
  u32 message_length
  u8  source_node_id
  u8  target_node_id
  u16 route_flags        // admin, begin, end, unfragmented
  u16 source_service_id
  u16 target_service_id
  u16 template_id
  u16 reserved
  i64 correlation_id
```

The DATA payload follows the 64-byte header.

Routing fields remain RingLoom-native. The receiver routes a complete reassembled message to `target_service_id` on `target_node_id`. Admin messages use `route_flags.admin` and are dispatched to cluster/control handlers.

### 5.4 SETUP

SETUP establishes or refreshes stream state:

```
SetupHeader
  CommonHeader common
  u8  source_node_id
  u8  target_node_id
  u16 reserved
  u32 stream_id
  i32 initial_term_id
  i32 active_term_id
  u32 term_length
  u32 mtu
  u64 sender_epoch
  u32 group_name_hash
  u32 token_length
  bytes token            // optional authentication or anti-amplification token
```

The receiver validates:

1. Magic and protocol version.
2. `target_node_id` equals local node.
3. Source node is a configured peer.
4. Group hash matches.
5. Source address and port match the configured peer endpoint unless an explicit dynamic endpoint mode is added.
6. Epoch is newer than any existing stream for that peer or matches the active stream.

Until validation completes, the receiver must not allocate large term or reassembly state. It may send only bounded `SETUP_RESPONSE` or `ERROR` frames to prevent amplification.

### 5.5 STATUS

STATUS combines ACK and flow-control advertisement:

```
StatusHeader
  CommonHeader common
  u32 stream_id
  i32 consumption_term_id
  u32 consumption_term_offset
  u32 receiver_window
  u64 receiver_id
  u64 highest_contiguous_message_id
```

The sender computes:

```
receiver_position = position(consumption_term_id, consumption_term_offset)
sender_limit = receiver_position + min(receiver_window, congestion_window)
```

### 5.6 NAK

```
NakHeader
  CommonHeader common
  u32 stream_id
  i32 term_id
  u32 term_offset
  u32 length
```

The sender retransmits the requested range, limited by:

- term boundary,
- configured max retransmit length,
- receiver window,
- retransmit rate limit,
- duplicate NAK suppression state.

### 5.7 HEARTBEAT

HEARTBEAT is a zero-payload position update. It carries stream ID and current sender or receiver position. A broker sends heartbeats when no DATA or STATUS has been sent within the heartbeat interval.

Unlike TCP v1, heartbeat liveness no longer depends on a byte stream staying readable. Each session tracks last valid frame time per peer and per stream.

---

## 6. Term logs, fragmentation, and reassembly

### 6.1 Sender term log

Each active stream has three term partitions:

```
StreamTermLog
  term[0]: term_length bytes
  term[1]: term_length bytes
  term[2]: term_length bytes
  metadata: 4096 bytes
```

Metadata includes:

- `term_tail_counters[3]`: packed `(term_id, term_offset)`,
- `active_term_count`,
- `sender_position`,
- `sender_limit`,
- `initial_term_id`,
- `term_length`,
- `mtu`,
- retransmit action slots,
- connection/session state.

DATA frames are written with release ordering on `frame_length` as the final commit field. Sender retransmission reads only committed frames.

### 6.2 Fragmentation

Application messages larger than `mtu - DataHeader.size` are split into fragments:

- All fragments share `message_id`.
- `fragment_offset` is the byte offset in the original message.
- `message_length` is the full application message length.
- `route_flags.begin` and `route_flags.end` mark message boundaries.

Fragments may be retransmitted independently. The receiver delivers a message only when all fragments for a `message_id` are present and route metadata is consistent.

### 6.3 Receiver reassembly

The receiver uses one shared RX staging buffer for packet I/O, but durable receive state is **per stream**, not global.

Each active stream has a bounded receive window sized to the advertised flow-control window plus bounded slack for reorder and target-service backpressure:

```
StreamReceiveWindow
  data: receive_window_length bytes
  frame_meta: fixed-size presence/length metadata
  high_water_mark
  rebuild_position
  consumed_position
  gap_tracker
  pending_message_state
```

Key properties:

- The receiver does **not** mirror the sender's three-term retransmit log by default.
- It retains only enough committed DATA to cover the active receive window and bounded pending delivery.
- Packets are copied from the shared RX staging buffer into the stream-local receive window at the absolute position derived from `(term_id, term_offset)`.
- Frame metadata is published only after payload bytes are copied, so rebuild logic sees committed frames only.
- Rebuild walks forward from `rebuild_position` in position order and stops at the first gap or undeliverable complete message.

For delivery:

- Unfragmented messages are routed directly from the receive window to the target service ring when capacity exists.
- Fragmented messages are rebuilt from contiguous frames in the receive window.
- A separate per-message reassembly arena is optional, not required by default. It should be used only when message layout or lifetime makes direct rebuild from the receive window impractical.

If bounded pending-message state is exhausted, the receiver drops the oldest incomplete or blocked message for that stream, increments the appropriate drop counter, and may NAK gaps only if they are still inside the receive window.

### 6.4 Duplicate and stale packet handling

The receiver maintains a dedup window per stream:

- Packets below `consumed_position - dedup_window` are stale and dropped.
- Duplicate committed fragments inside the window are ignored.
- Packets from an older session epoch are dropped and counted.

---

## 7. Send path v2

### 7.1 Duty cycle

```
sender_do_work:
  1. drain control commands
  2. poll inbound control frames on transport endpoint (STATUS, NAK, RTTM, ERROR)
  3. process retransmit timers
  4. scan destination send buffers in fair order
  5. for each destination:
       if stream flow/congestion blocked: skip buffer
       else read bounded messages from its ring buffer
       append to stream term log
       send DATA frames up to MTU/window/budget
  6. send SETUP/HEARTBEAT/RTTM as needed
  7. publish counters and pressure states
```

The sender never uses the minimum available capacity across all peers or destinations. Each stream's pressure state gates only its destination buffer.

### 7.2 Fair scheduling

The default scheduler is deficit round-robin over active destination buffers:

- Each active buffer gets a byte quantum per cycle.
- A skipped buffer keeps or caps its deficit according to config.
- Flow-blocked and congestion-blocked streams are skipped without consuming from their shared-memory ring.
- Empty buffers are deprioritized until a producer writes again.

### 7.3 Backpressure behavior

| Condition | Behavior |
|---|---|
| Destination send buffer full | Service send fails or applies configured spin/drop strategy for that destination |
| Stream flow-control window exhausted | Sender stops consuming that destination buffer |
| Stream congestion window exhausted | Sender stops consuming that destination buffer |
| Term log cannot rotate because old term is unacked | Sender stops consuming that destination buffer |
| Remote service ring buffer full | Receiver withholds consumed-position advancement and advertises a smaller window; sender eventually stops consuming only that destination |
| Peer/session down | Destination buffers for that peer enter peer-down pressure; other peers continue |

---

## 8. Receive path v2

### 8.1 Duty cycle

```
receiver_do_work:
  1. drain control commands
  2. poll UDP/AF_XDP packets into receive staging buffer
  3. validate common header and session
  4. dispatch by frame type
       SETUP -> validate/create stream
       DATA -> insert into stream receive-window state
       RTTM -> update RTT or reply
       HEARTBEAT -> liveness update
       ERROR -> notify control loop
  5. track rebuild/loss for active streams
  6. send STATUS and NAK frames
  7. route complete messages to local service ring buffers
  8. publish counters and pressure states
```

### 8.2 Receiver backpressure

The receiver should no longer always drop application messages immediately when a target service ring buffer is full. Instead:

1. If the target service buffer has capacity, route the complete message and advance consumed position.
2. If the target service buffer is full, keep the complete message pinned in that stream's receive window or bounded pending-message state.
3. Advertise reduced or zero receiver window for that stream.
4. If receive-side retention is exhausted, drop the message, advance according to the configured loss policy, and increment explicit counters.

The receiver must not acknowledge past blocked data by moving it into one global cross-stream queue. This converts remote-service backpressure into per-stream flow-control pressure rather than global connection pressure.

---

## 9. Transport engines

### 9.1 Transport interface

Both POSIX UDP and AF_XDP implement the same broker-facing interface:

```
UdpEndpoint:
  init(config) -> endpoint
  bind(local_addr, port)
  send(frame, remote_addr)
  sendBatch(frames, remote_addr)
  poll(receive_buffer, packet_limit) -> packets
  close()
```

The sender and receiver event loops own their endpoint state. No transport engine may allocate on the packet hot path.

### 9.2 POSIX UDP

The POSIX implementation uses:

- non-blocking UDP sockets,
- `sendmmsg`/`recvmmsg` on Linux where available,
- `sendmsg`/`recvmsg` fallback,
- `SO_SNDBUF` and `SO_RCVBUF`,
- `IP_MTU_DISCOVER`/DF where supported,
- optional `SO_REUSEPORT` only if multiple receive queues are introduced later.

### 9.3 AF_XDP/eBPF

AF_XDP is selected only when all configured prerequisites pass.

#### XDP filter

The eBPF program must:

1. Parse Ethernet, IPv4/IPv6, and UDP headers.
2. If packet is not UDP, return `XDP_PASS`.
3. If destination port is not configured for RingLoom, return `XDP_PASS`.
4. If IP packet is fragmented, return `XDP_PASS` or drop according to config; the default is `XDP_PASS` plus a counter because non-first fragments do not contain UDP ports.
5. If the configured XSK map entry exists for the RX queue, redirect to AF_XDP.
6. Otherwise return `XDP_PASS`.

This guarantees that only RingLoom's configured UDP port traffic bypasses the kernel.

#### Operational constraints

AF_XDP requires:

- Linux with XDP and AF_XDP support,
- sufficient privileges (`CAP_NET_ADMIN`, `CAP_BPF`, and related kernel settings),
- NIC and driver support for native or zero-copy mode if requested,
- one XSK per bound interface/RX queue,
- RX queue steering so the RingLoom UDP port lands on the XSK queue,
- UMEM fill/completion ring management,
- user-space Ethernet/IP/UDP header construction and checksum handling on TX,
- neighbor resolution for TX destination MAC addresses.

If native zero-copy mode is requested but only generic XDP is available, the default policy is startup fallback to POSIX UDP. Config can allow generic XDP for diagnostics.

#### POSIX coexistence

When XDP redirects a UDP port to AF_XDP, a POSIX socket bound to that same port will not receive redirected packets. The broker therefore chooses one active engine per port at startup:

```
prefer_af_xdp:
  if AF_XDP setup succeeds -> use AF_XDP
  else -> log fallback and use POSIX UDP
require_af_xdp:
  if AF_XDP setup fails -> broker startup fails
posix:
  skip AF_XDP setup
```

---

## 10. Flow control and congestion control

### 10.1 Flow control

The default flow control is Aeron-style max/unicast flow control:

```
receiver_position = position(consumption_term_id, consumption_term_offset)
receiver_edge = receiver_position + receiver_window
sender_limit = max(sender_limit, receiver_edge)
```

For unicast broker-to-broker streams, there is one receiver, so max and receiver edge are equivalent. A future multicast mode can add min/tagged strategies.

### 10.2 Congestion control

The initial congestion control is static-window with RTT measurement:

- `congestion_window = min(configured_initial_window, term_length / 2)`
- RTTM frames maintain RTT EWMA for observability and future adaptive algorithms.
- On loss, the receiver forces a STATUS update and sends bounded NAKs.
- CUBIC can be added as a pluggable strategy after the static implementation is correct.

### 10.3 NAK policy

The receiver delays first NAK slightly to allow reordering to resolve naturally. The sender suppresses duplicate NAKs that overlap an active retransmit action. Config controls:

- initial NAK delay,
- retry NAK delay,
- maximum retransmit length,
- maximum active retransmit actions per stream,
- retransmit linger duration,
- NAK rate limit per stream.

---

## 11. Configuration v2

Representative config keys:

| Key | Meaning |
|---|---|
| `broker.transport` | `udp` |
| `broker.udp.local.host.port` | UDP bind address |
| `broker.udp.member.host.ports` | Peer UDP endpoints |
| `broker.udp.mtu` | Payload MTU for RingLoom UDP frames |
| `broker.udp.term.length` | Per-stream term partition length |
| `broker.udp.receiver.window.length` | Receiver flow-control window |
| `broker.udp.heartbeat.interval.ms` | Peer/session heartbeat |
| `broker.udp.session.timeout.ms` | Peer/session timeout |
| `broker.udp.nak.initial.delay.us` | Initial NAK delay |
| `broker.udp.nak.retry.delay.us` | Retry NAK delay |
| `broker.send.buffers.max.entries` | Max destination send buffers |
| `broker.send.buffers.default.size` | Destination send buffer capacity |
| `broker.send.buffers.max.total.bytes` | Total send-buffer region budget |
| `broker.transport.engine` | `posix`, `prefer_af_xdp`, or `require_af_xdp` |
| `broker.af_xdp.interfaces` | Interface names eligible for XDP attach |
| `broker.af_xdp.ports` | UDP ports eligible for redirect |
| `broker.af_xdp.mode` | `zero_copy`, `copy`, or `generic_allowed` |
| `broker.af_xdp.rx.queue` | RX queue to bind |
| `broker.af_xdp.umem.frame.count` | UMEM frame count |
| `broker.af_xdp.umem.frame.size` | UMEM frame size |

TCP and TCP io_uring keys are removed during the v2 cutover.

---

## 12. Observability

V2 counters replace TCP counters with transport-native counters:

| Counter | Scope |
|---|---|
| `udp_bytes_sent`, `udp_bytes_received` | endpoint |
| `udp_packets_sent`, `udp_packets_received` | endpoint |
| `udp_invalid_frames` | endpoint |
| `udp_setup_sent`, `udp_setup_received` | endpoint/stream |
| `udp_status_sent`, `udp_status_received` | stream |
| `udp_naks_sent`, `udp_naks_received` | stream |
| `udp_retransmits_sent` | stream |
| `udp_duplicate_packets` | stream |
| `udp_stale_session_packets` | peer |
| `udp_reassembly_slot_drops` | stream |
| `udp_reassembly_timeout_drops` | stream |
| `udp_flow_control_blocked_cycles` | destination buffer/stream |
| `udp_congestion_blocked_cycles` | destination buffer/stream |
| `udp_sender_position`, `udp_sender_limit` | stream |
| `udp_receiver_position`, `udp_receiver_hwm` | stream |
| `udp_rtt_ewma_ns` | stream |
| `af_xdp_enabled`, `af_xdp_fallbacks` | endpoint |
| `af_xdp_rx_drops`, `af_xdp_tx_drops` | endpoint |
| `send_buffer_full_by_destination` | destination buffer |

Counters exposed to services should use the send-buffer directory entry and per-stream transport state, not a single peer-level pending count.

---

## 13. Security posture

The first v2 transport assumes a trusted broker network, but still includes basic anti-spoofing and amplification controls:

- peers must be configured or authorized by control-plane membership,
- source address/port must match the configured peer unless dynamic endpoints are explicitly enabled,
- SETUP must validate group hash and node identity,
- receiver must not allocate large state before SETUP validation,
- response size before validation must be bounded.

Payload encryption/authentication is out of scope for the first implementation. If untrusted networks are required, add AEAD-protected sessions with pre-shared keys or a certificate-based handshake before enabling production use.

---

## 14. Testing strategy

V2 requires tests beyond TCP-era cross-broker success cases:

1. **Protocol unit tests:** header sizes, field offsets, endian encode/decode, invalid frame rejection.
2. **Term log tests:** append, rotate, retransmit range scan, max-position guard.
3. **Loss tests:** packet drop, reorder, duplication, delayed NAK, duplicate NAK suppression.
4. **Flow-control tests:** receiver window stalls only the affected stream.
5. **Per-destination buffer tests:** full buffer for one remote service does not stop another destination.
6. **Receiver backpressure tests:** slow target service reduces stream window and does not block other streams.
7. **POSIX UDP e2e tests:** cross-broker routing, restart, heartbeat timeout, fragmentation, discovery, leader election.
8. **AF_XDP tests:** unit-test BPF filter decisions; capability-gated integration test for AF_XDP RX/TX or fallback behavior.
9. **Interop mode tests:** one broker using POSIX and one broker using AF_XDP when the environment supports it.
10. **Operational tests:** startup fallback, `require_af_xdp` startup failure, invalid port filter config, PMTUD/oversized payload behavior.

---

## 15. Implementation touchpoints

The completed v2 implementation is centered on these surfaces:

- Broker metadata owns destination send-buffer, flow-control, and peer counter regions in `src/common/memory/broker_metadata.zig`.
- ServiceClient routes remote sends into per-destination buffers with transport-neutral message envelopes in `src/service/service_client.zig`.
- SenderEventLoop drains per-destination buffers and sends reliable UDP SETUP/DATA/HEARTBEAT frames in `src/broker/sender/sender_event_loop.zig`.
- ReceiverEventLoop polls UDP packets, reassembles DATA frames, emits SETUP_RESPONSE/STATUS/NAK, and routes complete messages in `src/broker/receiver/receiver_event_loop.zig`.
- Config exposes UDP/send-buffer/AF_XDP keys in `src/common/config/broker_config.zig` and `src/common/config/config_loader.zig`.
- E2E coverage exercises UDP cross-broker routing, fragmentation, backpressure, and observability in `src/e2e/`.
