# Implementation Task: TCP Transport for BRZ Broker

You are implementing the TCP transport layer for the BRZ broker, a high-performance IPC and cross-host message routing system written in Zig. The architecture has been redesigned to replace a custom Aeron-like reliable UDP protocol with TCP connections between brokers.

## Context

The full architecture and implementation documents are in `docs/`:

- `docs/architecture.md` — The primary architecture document (22 sections + appendices)
- `docs/impl/00-overview.md` — Implementation guide overview with build order
- `docs/impl/01-platform-abstraction.md` — OS abstractions, constants
- `docs/impl/02-memory-layout-and-shared-memory.md` — Metadata files, SHM layout
- `docs/impl/03-concurrent-data-structures.md` — MPSC ring buffer, counters
- `docs/impl/04-tcp-transport-library.md` — **`brz_tcp` library specification** (NEW)
- `docs/impl/05-send-path.md` — TCP send path (REWRITTEN)
- `docs/impl/06-receive-path.md` — TCP receive path (REWRITTEN)
- `docs/impl/08-service-ipc.md` — Service ↔ broker IPC
- `docs/impl/09-control-plane.md` — Control messages
- `docs/impl/10-threading-model.md` — Event loop architecture
- `docs/impl/11-cluster-management.md` — Leader election, peer management
- `docs/impl/12-configuration-and-monitoring.md` — Config, counters
- `docs/impl/13-library-split-and-packaging.md` — Package boundaries

**Read the architecture document and all relevant impl docs before writing any code.** The docs contain complete wire format specifications, Zig struct definitions, state machine diagrams, and pseudocode that should be followed precisely.

## Current State of the Codebase

The existing code implements the **old UDP-based architecture**. Key files that need to change:

### Source layout

```
src/
├── bin/                    # Executables (broker main, test services)
├── broker/
│   ├── app/                # Broker application factory, runtime
│   ├── cluster/            # Cluster management, leader election, admin messages
│   ├── control/            # Control loop, service registry, heartbeat checker
│   ├── flow_control/       # ❌ DELETE ENTIRELY — UDP flow control (NAK, Status Messages, back-pressure, receiver flow control, sender flow control, zero-window probe)
│   ├── receiver/           # ❌ REWRITE — fragment assembler, loss detector, receive log buffer → TCP read state machine, message routing
│   ├── sender/             # ❌ REWRITE — message fragmenter, retransmit buffer/handler → per-peer write queues, TCP write mechanics
│   ├── threading/          # UPDATE — event loop integration with brz_tcp
│   └── transport/          # ❌ REWRITE — udp_socket.zig, io_uring.zig, kqueue.zig, network_io.zig → becomes thin wrapper or replaced by brz_tcp imports
├── common/
│   ├── concurrent/         # Ring buffer, counters, error log — KEEP (minor updates)
│   ├── config/             # BrokerConfig — UPDATE for TCP params
│   ├── memory/             # Constants, metadata, receive_log.zig — UPDATE (remove receive_log, update constants)
│   ├── message/            # Message header, fragmenting producer, assembler — UPDATE (remove UDP fragmentation for cross-host)
│   ├── monitoring/         # System counters — UPDATE counter IDs
│   ├── platform/           # Atomics, clock, mmap, process sync — KEEP
│   └── protocol/           # Frame parser, frames — ❌ REWRITE for TCP framing
├── e2e/                    # End-to-end tests — UPDATE for TCP
├── perf/                   # Benchmarks — UPDATE for TCP
├── service/                # Service client library — minor updates
└── testing/                # Test harness — minor updates
```

### Build system (`build.zig`)

Currently defines modules: `brz_common`, `brz_broker`, `brz_service`, `brz_testing`. You need to add a `brz_tcp` module and wire it as a dependency of `brz_broker`.

## What Needs to Be Implemented

### Phase 1: `brz_tcp` Library (doc 04)

Create the standalone TCP transport library under `src/tcp/`. This is the foundation — nothing else can proceed without it.

**Files to create:**

| File | Description |
|------|-------------|
| `src/tcp/tcp.zig` | Public root module — re-exports all public types |
| `src/tcp/io_engine.zig` | `IoEngine` type alias (compile-time backend selection) |
| `src/tcp/io_uring_engine.zig` | Linux io_uring backend |
| `src/tcp/kqueue_engine.zig` | macOS kqueue backend |
| `src/tcp/connection.zig` | `Connection` struct, `ConnectionState` enum, `ConnectionRegistry` |
| `src/tcp/listener.zig` | TCP listener (accept loop) |
| `src/tcp/connector.zig` | Outbound TCP connector |
| `src/tcp/framing.zig` | `FrameHeader` (24-byte packed struct), `ReadState`, `buildHeader`, `buildHeartbeat` |
| `src/tcp/handshake.zig` | `HandshakeFrame` (24-byte packed struct), validation, FNV-1a hash |
| `src/tcp/socket_config.zig` | TCP socket options (TCP_NODELAY, SO_KEEPALIVE, buffer sizes) |
| `src/tcp/buffer_pool.zig` | Pre-allocated slab buffer pool |

**Key specifications (from architecture.md sections 7 and 11):**

Wire format — Message Frame Header (24 bytes, little-endian):
```
Offset  Size  Type    Field
0       4     u32     frame_length      (total frame including header)
4       1     u8      flags             (0x01 = ADMIN)
5       1     u8      source_node_id
6       1     u8      target_node_id
7       1     u8      reserved
8       2     u16     source_service_id
10      2     u16     target_service_id
12      2     u16     template_id       (message type, 0 = raw app message)
14      2     u16     reserved
16      8     i64     correlation_id    (for request-response matching)
24+     ...   bytes   payload
```

Connection Handshake (24 bytes):
```
Offset  Size  Type    Field
0       4     u32     magic             (0x42525A00 = "BRZ\0")
4       1     u8      protocol_version  (1)
5       1     u8      source_node_id
6       1     u8      target_node_id
7       1     u8      direction         (0x01=sender, 0x02=receiver)
8       8     u64     session_epoch
16      4     u32     group_name_hash   (FNV-1a of group name)
20      4     u32     reserved
```

IoEngine interface — compile-time dispatch, no vtable:
- `init(max_connections)`, `deinit()`
- `submitAccept()`, `submitConnect()`, `submitRead()`, `submitWrite()`, `submitWritev()`, `submitClose()`
- `pollCompletions(out: []Completion) u32`, `flush()`

### Phase 2: Delete UDP Code

Remove these files entirely:
- `src/broker/flow_control/` — entire directory (back_pressure.zig, counters.zig, receiver_flow_control.zig, sender_flow_control.zig, status_message.zig, test_flow_control.zig, zero_window_probe.zig)
- `src/broker/flow_control.zig` — module root
- `src/broker/receiver/fragment_assembler.zig`
- `src/broker/receiver/loss_detector.zig`
- `src/broker/receiver/receive_log_buffer.zig`
- `src/broker/sender/message_fragmenter.zig`
- `src/broker/sender/retransmit_buffer.zig`
- `src/broker/sender/retransmit_handler.zig`
- `src/broker/transport/udp_socket.zig`
- `src/common/memory/receive_log.zig`
- `src/common/protocol/frames.zig` (old UDP frame types)

Update module root files (`src/broker/receiver.zig`, `src/broker/sender.zig`, `src/broker/transport.zig`, `src/broker/root.zig`, `src/common/memory.zig`, `src/common/protocol.zig`) to remove imports of deleted files.

### Phase 3: Rewrite Sender (doc 05)

Rewrite `src/broker/sender/` for TCP:

| File | Change |
|------|--------|
| `sender_event_loop.zig` | Rewrite duty cycle: drain send ring buffer → enqueue to per-peer write queues → flush TCP writes via brz_tcp → send heartbeats → check connection health. Round-robin fairness with WRITE_BUDGET_PER_PEER=16. |
| `peer_sender.zig` | Rewrite: bounded write queue (ring buffer, drop-oldest on overflow), TCP write submission via IoEngine, connection state tracking, heartbeat sending (500ms interval), reconnection with exponential backoff (100ms→1000ms cap). |
| `send_buffer_pool.zig` | May be simplified or replaced — evaluate whether brz_tcp's buffer pool suffices. |
| `sender_command.zig` | Update command types for TCP (add/remove peer, connection state changes). |

### Phase 4: Rewrite Receiver (doc 06)

Rewrite `src/broker/receiver/` for TCP:

| File | Change |
|------|--------|
| `receiver_event_loop.zig` | Rewrite duty cycle: poll I/O completions → process completed reads per peer (READ_BUDGET_PER_PEER=16) → accept new connections → check peer liveness. Always-read model — never pause socket reads. |
| `peer_receiver.zig` | New: per-peer TCP read state, framing (ReadState from brz_tcp), handshake validation, heartbeat timeout tracking (suspect at 1500ms, dead at 2000ms). |
| `message_router.zig` | Update: route complete framed messages to service ring buffers. Drop on full + increment counter. Admin messages dispatched by template_id. |

### Phase 5: Update Transport Layer

`src/broker/transport/` currently has UDP-specific code. Replace with brz_tcp integration:

| File | Change |
|------|--------|
| `io_uring.zig` | DELETE — functionality moves to `src/tcp/io_uring_engine.zig` |
| `kqueue.zig` | DELETE — functionality moves to `src/tcp/kqueue_engine.zig` |
| `udp_socket.zig` | DELETE |
| `network_io.zig` | REWRITE or DELETE — replaced by brz_tcp's IoEngine |
| `buffer_pool.zig` | EVALUATE — may be replaced by brz_tcp's BufferPool |
| `transport.zig` | UPDATE module root |

### Phase 6: Update Supporting Code

**Configuration (`src/common/config/broker_config.zig`):**
- Remove: `recv_log_buffer_size`, `retransmit_buffer_size`, `mtu_length`
- Add: `tcp_send_buffer_size` (default 262144), `tcp_recv_buffer_size` (default 262144), `max_frame_length` (default 65536), `peer_write_queue_capacity` (default 4096), `heartbeat_interval_ms` (default 500), `heartbeat_timeout_ms` (default 2000), `reconnect_initial_delay_ms` (default 100), `reconnect_max_delay_ms` (default 1000)

**Constants (`src/common/memory/constants.zig` and `src/common/platform/constants.zig`):**
- Remove: `frame_type_nak`, `frame_type_sm`, `frame_type_setup`, `flag_begin`, `flag_end`, `flag_unfragmented`, NAK/SM/retransmit timer constants
- Add: `protocol_version = 1`, `handshake_magic = 0x42525A00`, `heartbeat_template_id = 0xFFFF`, `direction_sender = 0x01`, `direction_receiver = 0x02`, `header_length = 24`, `handshake_length = 24`

**System Counters (`src/common/monitoring/system_counter.zig`):**
- Remove: `naks_sent`, `naks_received`, `retransmits_sent`, `status_messages_sent`, `status_messages_received`, `flow_control_under_runs`, `flow_control_over_runs`, `invalid_packets`
- Add: `tcp_connections_accepted`, `tcp_connection_errors`, `tcp_handshake_failures`, `tcp_reconnect_attempts`, `heartbeat_timeouts`, `service_full_drops`, `peer_queue_overflow_drops`, `peer_not_connected_drops`, `invalid_frames`

**Protocol (`src/common/protocol/`):**
- `frame_parser.zig`: Rewrite for TCP framing — parse length-prefixed frames from byte stream (or delegate to brz_tcp's ReadState)
- `frames.zig`: DELETE old UDP frame types, replace with imports from brz_tcp's framing.zig

**Cluster management (`src/broker/cluster/`):**
- Update connection setup to use TCP handshake instead of SETUP/SM frames
- Update `ConnectionState` enum: `disconnected`, `handshake_sent`, `connected` (was: `disconnected`, `setup_sent`, `connected`)
- Peer addresses resolve to TCP host:port

**Threading (`src/broker/threading/`):**
- Update `broker_threads.zig` to initialize brz_tcp IoEngine and pass to sender/receiver event loops
- Update `composite_event_loop.zig` for TCP integration

**Build system (`build.zig`):**
- Add `brz_tcp` module: `b.addModule("brz_tcp", .{ .root_source_file = b.path("src/tcp/tcp.zig"), ... })`
- Add `brz_tcp` as import dependency for `brz_broker`
- Add build option for I/O backend selection (auto-detect from target OS)
- Add test step for brz_tcp unit tests

### Phase 7: Update Tests

- Unit tests in `src/tcp/` — framing, handshake, buffer pool, io_engine (see doc 04 section 14)
- Update `src/e2e/` tests — replace UDP transport references with TCP
- Update `src/perf/` benchmarks — replace UDP transport references with TCP
- Remove tests that reference deleted modules (flow control, fragment assembler, loss detector, retransmit)

## Critical Design Constraints

1. **Always-read model**: The receiver event loop MUST always read from TCP. Never pause socket reads. If a service ring buffer is full, drop the message and increment a counter. This prevents head-of-line blocking.

2. **Per-peer fairness**: Both sender and receiver enforce per-peer budgets per duty cycle (READ_BUDGET_PER_PEER=16, WRITE_BUDGET_PER_PEER=16). Round-robin across peers.

3. **No allocations on hot path**: All buffers are pre-allocated during initialization. Message routing uses flyweight overlays. No `allocator.alloc()` in the event loop.

4. **Single-writer principle**: Each mutable field has exactly one writer. Sender event loop owns outbound connections. Receiver event loop owns inbound connections.

5. **Dual unidirectional connections**: Each broker pair uses TWO TCP connections — sender event loop owns outgoing, receiver event loop owns incoming. If either fails, both are recycled.

6. **Heartbeats bypass write queues**: Heartbeat frames are written directly to TCP, not enqueued in the per-peer write queue. They must never be dropped due to queue overflow.

7. **Session epoch for stale detection**: Each broker restart increments session_epoch. Handshake validation rejects connections with stale epochs.

8. **Exponential backoff on reconnect**: 100ms → 200ms → 400ms → 800ms → 1000ms cap. Reset to 100ms on successful connection.

9. **Best-effort delivery across disconnects**: No application-level sequence numbers or replay. Messages in flight during disconnect are lost.

10. **Compile-time backend selection**: The IoEngine type is selected at compile time via `build.zig` option or auto-detection from target OS. No runtime dispatch.

## Zig-Specific Requirements

- Target **Zig 0.15.x** stable
- Use `extern struct` for wire format structs (not `packed struct`) — 24-byte FrameHeader with `comptime { std.debug.assert(@sizeOf(FrameHeader) == 24); }`
- Use `@atomicLoad`/`@atomicStore` with explicit ordering for shared state
- All error handling via Zig error unions — no panics on I/O errors
- `comptime` assertions for struct sizes, alignment, and buffer constraints

## Implementation Order

Follow the document numbering. Each phase depends only on previous phases:

1. Create `src/tcp/` — the `brz_tcp` library (doc 04)
2. Update `build.zig` — add `brz_tcp` module
3. Delete UDP code (Phase 2 above)
4. Update constants, config, counters (Phase 6 — these are leaf changes)
5. Rewrite sender (doc 05)
6. Rewrite receiver (doc 06)
7. Update cluster management, threading
8. Update and fix tests
9. Build and verify: `zig build test`

## Verification

After implementation, the following should pass:

```bash
# Build everything
zig build

# Run unit tests (including brz_tcp tests)
zig build test

# Build test service binaries
zig build test-bins
```

No UDP-related code should remain in the source tree. Grep for these terms and ensure zero matches in `src/`:
- `sendmsg`, `recvmsg` (except in historical comments)
- `retransmit`, `recv_log`, `loss_detect`, `fragment_assembler`
- `NAK`, `Status.Message`, `StatusMessage` (as types/functions)
- `frame_type_nak`, `frame_type_sm`, `frame_type_setup`
- `udp_socket`, `UdpSocket`
