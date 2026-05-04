# 06 — Receive Path (TCP)

> **Depends on:** [02 — Memory Layout & Shared Memory](02-memory-layout-and-shared-memory.md) (service metadata files, ring buffer layout),
> [03 — Concurrent Data Structures](03-concurrent-data-structures.md) (MPSC ring buffer for routing to services),
> [04 — TCP Transport Library](04-tcp-transport-library.md) (`FrameHeader`, handshake frame, `TcpIo`, `ringloom_tcp` library)
>
> **Depended on by:** [08 — Service IPC](08-service-ipc.md) (cross-host messages land in service ring buffers via the routing described here)

This document describes the **TCP receive path** — the subsystem that accepts incoming
TCP connections from peer brokers, reads length-prefixed message frames from the TCP
stream, validates frame headers, and routes complete messages to target service ring
buffers.

The receiver reads complete frames from the TCP stream, validates them, and routes
them directly to the target service ring buffers.

All code targets **Zig 0.14.x** stable.

---

## Table of Contents

1.  [Overview](#1-overview)
2.  [TCP Listener and Connection Acceptance](#2-tcp-listener-and-connection-acceptance)
3.  [Per-Connection Read State Machine](#3-per-connection-read-state-machine)
4.  [Frame Validation](#4-frame-validation)
5.  [Receiver Event Loop Duty Cycle](#5-receiver-event-loop-duty-cycle)
6.  [Message Routing to Service Ring Buffers](#6-message-routing-to-service-ring-buffers)
7.  [Admin Message Handling](#7-admin-message-handling)
8.  [Heartbeat Processing](#8-heartbeat-processing)
9.  [Connection Failure Handling](#9-connection-failure-handling)
10. [Buffer Management](#10-buffer-management)
11. [Fairness and Budgets](#11-fairness-and-budgets)
12. [Counters and Monitoring](#12-counters-and-monitoring)
13. [Configuration Parameters](#13-configuration-parameters)
14. [Error Handling](#14-error-handling)
15. [Testing Strategy](#15-testing-strategy)

---

## 1. Overview

```
TCP Listener ──► Accept ──► Handshake Validation
                                    │
                                    ▼
                     ┌──────────────────────────────┐
                     │  Per-Peer TCP Connection      │
                     │  io_uring/kqueue read          │
                     └──────────┬───────────────────┘
                                │
                                ▼
                     ┌──────────────────────────────┐
                     │  Read State Machine           │
                     │  (header → payload → done)    │
                     └──────────┬───────────────────┘
                                │
                                ▼
                     ┌──────────────────────────────┐
                     │  Frame Validation             │
                     │  (length, node IDs)           │
                     └──────────┬───────────────────┘
                                │
                    ┌───────────┼───────────┐
                    ▼           ▼           ▼
              ┌──────────┐ ┌────────┐ ┌──────────┐
              │ Data Msg │ │ Admin  │ │Heartbeat │
              │          │ │  Msg   │ │          │
              └────┬─────┘ └───┬────┘ └────┬─────┘
                   │           │           │
                   ▼           ▼           ▼
              Service Ring   Admin     Update peer
              Buffer write   Handler   last_recv_ts
```

The receiver event loop is a **single thread** that owns all incoming TCP connections
from peer brokers and the TCP listener socket. It runs as a duty-cycle event loop
(spin on work count, idle strategy when no work), following the same pattern as every
other event loop in RingLoom (see doc 01, §5).

### Data Flow Summary

| Step | Actor | Operation |
|------|-------|-----------|
| 1 | TCP listener | Accept incoming connection from peer |
| 2 | Receiver event loop | Read 24-byte handshake frame, validate |
| 3 | Receiver event loop | Register connection in peer table |
| 4 | io_uring/kqueue | Complete read on connected socket |
| 5 | Read state machine | Accumulate header bytes (24 bytes) |
| 6 | Read state machine | Accumulate payload bytes (frame_length − 24) |
| 7 | Frame validation | Validate frame_length, source/target node IDs |
| 8 | Receiver event loop | Route to service ring buffer (or admin handler) |

---

## 2. TCP Listener and Connection Acceptance

The receiver owns the TCP listener socket — one per broker. Each peer broker opens
one TCP connection *to* this broker's listener. From the peer's perspective, this is
its "send connection"; from ours, it is the "receive connection" for that peer.

### 2.1 Wire Format: Handshake Frame

The handshake frame is a 24-byte fixed-size message sent by the connecting peer
immediately after the TCP connection is established. It uses a packed struct for
zero-overhead parsing.

```zig
const HandshakeFrame = packed struct {
    /// Magic bytes: 0x474E4952 ("RING" in little-endian).
    magic: u32,

    /// Protocol version. Must match PROTOCOL_VERSION for compatibility.
    protocol_version: u8,

    /// The node ID of the connecting peer (the sender).
    source_node_id: u8,

    /// The node ID of this broker (the receiver). The peer must set this
    /// correctly; a mismatch means the peer connected to the wrong broker.
    target_node_id: u8,

    /// Connection direction from the peer's perspective.
    /// SEND (0x01) = peer is sending to us (expected on receiver side).
    direction: u8,

    /// Hash of the cluster group name. Must match our configured group.
    /// Prevents cross-cluster connections.
    group_name_hash: u32,

    /// Session epoch — monotonically increasing per peer restart.
    /// If higher than our stored epoch for this source_node_id, this is
    /// a new session and the old connection should be replaced.
    session_epoch: u32,

    /// Reserved for future use. Must be zero.
    reserved: u64,

    comptime {
        std.debug.assert(@sizeOf(HandshakeFrame) == 24);
    }
};

const HANDSHAKE_MAGIC: u32 = 0x474E4952;
const PROTOCOL_VERSION: u8 = 1;
const DIRECTION_SEND: u8 = 0x01;
```

### 2.2 Listener Setup

On startup, the receiver creates a TCP listener socket bound to the configured port
and registers it with the I/O backend for accept notifications.

```zig
fn setupListener(self: *ReceiverEventLoop) !void {
    const addr = try std.net.Address.resolveIp("0.0.0.0", self.config.tcp_listen_port);

    self.listener_fd = try std.posix.socket(
        addr.any.family,
        std.posix.SOCK.STREAM | std.posix.SOCK.NONBLOCK | std.posix.SOCK.CLOEXEC,
        std.posix.IPPROTO.TCP,
    );
    errdefer std.posix.close(self.listener_fd);

    // Allow rapid rebind after restart.
    try std.posix.setsockopt(self.listener_fd, std.posix.SOL.SOCKET, std.posix.SO.REUSEADDR, &std.mem.toBytes(@as(c_int, 1)));

    try std.posix.bind(self.listener_fd, &addr.any, addr.getOsSockLen());
    try std.posix.listen(self.listener_fd, self.config.tcp_listen_backlog);

    // Register for accept notifications via io_uring multishot accept
    // or kqueue EVFILT_READ on the listener fd.
    try self.tcp_io.registerAccept(self.listener_fd);
}
```

### 2.3 Accept Loop

On Linux, the receiver uses `IORING_OP_ACCEPT` with the `IORING_ACCEPT_MULTISHOT`
flag. This tells io_uring to continuously accept new connections without
re-submitting the SQE after each accept. On macOS, `EVFILT_READ` on the listener
fd fires whenever new connections are pending; the receiver calls `accept4` in a
loop until `EAGAIN`.

```zig
fn processAccepts(self: *ReceiverEventLoop) u32 {
    var work_count: u32 = 0;

    while (self.tcp_io.pollAccept()) |accepted| {
        const conn_fd = accepted.fd;

        // Set TCP_NODELAY to avoid Nagle buffering on this connection.
        std.posix.setsockopt(conn_fd, std.posix.IPPROTO.TCP, std.posix.TCP.NODELAY, &std.mem.toBytes(@as(c_int, 1))) catch {
            std.posix.close(conn_fd);
            self.counters.increment(.handshake_failures);
            continue;
        };

        // Start reading the 24-byte handshake frame.
        const pending = self.pending_handshakes.acquire() orelse {
            // Too many pending handshakes — reject.
            std.posix.close(conn_fd);
            self.counters.increment(.handshake_failures);
            continue;
        };
        pending.* = .{
            .fd = conn_fd,
            .buf = std.mem.zeroes([24]u8),
            .bytes_read = 0,
        };

        // Register for read on the handshake fd.
        self.tcp_io.registerRead(conn_fd, &pending.buf, @ptrCast(pending)) catch {
            std.posix.close(conn_fd);
            self.pending_handshakes.release(pending);
            self.counters.increment(.handshake_failures);
            continue;
        };

        self.counters.increment(.connections_accepted);
        work_count += 1;
    }

    return work_count;
}
```

### 2.4 Handshake Validation

Once all 24 bytes of the handshake frame have been read, the receiver validates the
handshake. If any check fails, the connection is closed immediately — no error
response is sent.

```zig
fn validateHandshake(
    self: *ReceiverEventLoop,
    buf: *const [24]u8,
    conn_fd: std.posix.fd_t,
) ?*PeerConnection {
    const handshake: *const HandshakeFrame = @ptrCast(buf);

    // ── Check 1: Magic bytes ──────────────────────────────────────────
    if (handshake.magic != HANDSHAKE_MAGIC) {
        self.counters.increment(.handshake_failures);
        std.posix.close(conn_fd);
        return null;
    }

    // ── Check 2: Protocol version ─────────────────────────────────────
    if (handshake.protocol_version != PROTOCOL_VERSION) {
        self.counters.increment(.handshake_failures);
        std.posix.close(conn_fd);
        return null;
    }

    // ── Check 3: Target node ID must match this broker ────────────────
    if (handshake.target_node_id != self.local_node_id) {
        self.counters.increment(.handshake_failures);
        std.posix.close(conn_fd);
        return null;
    }

    // ── Check 4: Direction must be SEND (peer sends to us) ────────────
    if (handshake.direction != DIRECTION_SEND) {
        self.counters.increment(.handshake_failures);
        std.posix.close(conn_fd);
        return null;
    }

    // ── Check 5: Group name hash must match our cluster ───────────────
    if (handshake.group_name_hash != self.group_name_hash) {
        self.counters.increment(.handshake_failures);
        std.posix.close(conn_fd);
        return null;
    }

    // ── Check 6: Source node ID must be valid (1..max_nodes) ──────────
    const source_id = handshake.source_node_id;
    if (source_id == 0 or source_id > constants.max_nodes) {
        self.counters.increment(.handshake_failures);
        std.posix.close(conn_fd);
        return null;
    }

    // ── Check 7: Session epoch — handle reconnection ──────────────────
    // If we already have a connection from this peer, compare epochs.
    // A higher epoch means the peer restarted; replace the old connection.
    if (self.peers.getPtr(source_id)) |existing| {
        if (handshake.session_epoch > existing.session_epoch) {
            // New session supersedes old. Close old connection.
            self.closePeerConnection(existing);
        } else {
            // Stale or duplicate connection attempt. Reject.
            self.counters.increment(.handshake_failures);
            std.posix.close(conn_fd);
            return null;
        }
    }

    // ── All checks passed — register the connection ───────────────────
    const peer_conn = self.createPeerConnection(source_id, conn_fd, handshake.session_epoch);
    return peer_conn;
}
```

### 2.5 Connection Registration

After a successful handshake, the connection is added to the peer table and read
operations begin immediately.

```zig
fn createPeerConnection(
    self: *ReceiverEventLoop,
    source_node_id: u8,
    conn_fd: std.posix.fd_t,
    session_epoch: u32,
) ?*PeerConnection {
    const peer = self.peer_pool.acquire() orelse {
        std.posix.close(conn_fd);
        return null;
    };

    peer.* = PeerConnection.init(source_node_id, conn_fd, session_epoch, self.config.read_buffer_size);
    self.peers.put(source_node_id, peer) catch {
        self.peer_pool.release(peer);
        std.posix.close(conn_fd);
        return null;
    };

    // Start reading frames from this connection.
    self.tcp_io.registerRead(
        conn_fd,
        peer.read_state.currentBuffer(),
        @ptrCast(peer),
    ) catch {
        self.peers.remove(source_node_id);
        self.peer_pool.release(peer);
        std.posix.close(conn_fd);
        return null;
    };

    peer.last_recv_ns = Clock.monotonicNanos();
    return peer;
}
```

---

## 3. Per-Connection Read State Machine

TCP is a byte stream — it does not preserve message boundaries. A single `read()`
call may return a partial header, a complete frame, or multiple frames concatenated
together. The per-connection read state machine handles this reassembly.

### 3.1 State Structure

Each `PeerConnection` maintains a `ReadState` that tracks progress through the
current frame.

```zig
const ReadState = struct {
    /// Current phase of the read state machine.
    phase: Phase,

    /// Buffer for accumulating the 24-byte frame header.
    header_buf: [24]u8,

    /// Number of header bytes read so far (0..24).
    header_bytes_read: u8,

    /// Pre-allocated buffer for accumulating the payload (frame body after header).
    /// Sized to max_frame_length at connection creation.
    payload_buf: []u8,

    /// Number of payload bytes read so far.
    payload_bytes_read: u32,

    /// Total frame length from the header (includes the 24-byte header).
    frame_length: u32,

    const Phase = enum {
        /// Reading the 24-byte frame header.
        reading_header,
        /// Reading the payload (frame_length - 24 bytes).
        reading_payload,
    };

    pub fn init(payload_buf: []u8) ReadState {
        return .{
            .phase = .reading_header,
            .header_buf = std.mem.zeroes([24]u8),
            .header_bytes_read = 0,
            .payload_buf = payload_buf,
            .payload_bytes_read = 0,
            .frame_length = 0,
        };
    }

    /// Reset to begin reading the next frame.
    pub fn reset(self: *ReadState) void {
        self.phase = .reading_header;
        self.header_bytes_read = 0;
        self.payload_bytes_read = 0;
        self.frame_length = 0;
    }

    /// Returns the buffer slice where the next read should deposit bytes.
    pub fn currentBuffer(self: *ReadState) []u8 {
        return switch (self.phase) {
            .reading_header => self.header_buf[self.header_bytes_read..],
            .reading_payload => self.payload_buf[self.payload_bytes_read..self.payloadLength()],
        };
    }

    /// Payload length = frame_length - header size.
    pub fn payloadLength(self: *const ReadState) u32 {
        return self.frame_length - frame_header_length;
    }
};
```

### 3.2 State Machine Diagram

```
                    ┌─────────────────────────┐
                    │    reading_header        │
      ┌────────────│  (0..24 bytes buffered)  │◄──────────────┐
      │ read()     └────────────┬─────────────┘               │
      │ partial                 │ 24 bytes complete            │
      └─────────────────────►   │                              │
                                ▼                              │
                    ┌─────────────────────────┐               │
                    │  Parse header            │               │
                    │  Validate frame_length   │               │
                    └────────────┬─────────────┘               │
                                │                              │
                     ┌──────────┴──────────┐                   │
                     │                     │                   │
               payload_len > 0       payload_len == 0          │
                     │                     │                   │
                     ▼                     │                   │
          ┌──────────────────────┐         │                   │
          │  reading_payload     │         │                   │
          │  (0..N bytes)        │         │                   │
          └──────────┬───────────┘         │                   │
                     │ complete            │                   │
                     ▼                     ▼                   │
          ┌──────────────────────────────────┐                │
          │  Frame complete                   │                │
          │  → Validate → Route → Reset       │────────────────┘
          └───────────────────────────────────┘
```

### 3.3 Processing Received Bytes

When io_uring or kqueue reports that bytes have been read on a connection, the
receiver feeds them into the read state machine. Because a single read may contain
data for multiple frames, this function loops until all received bytes are consumed.

```zig
const frame_header_length: u32 = 24;

/// Process bytes received on a peer connection. Returns the number of
/// complete frames extracted. May consume partial data and leave the
/// read state mid-frame for the next read completion.
fn processReceivedBytes(
    self: *ReceiverEventLoop,
    peer: *PeerConnection,
    data: []const u8,
) ProcessResult {
    var offset: usize = 0;
    var frames_processed: u32 = 0;
    var state = &peer.read_state;

    while (offset < data.len) {
        switch (state.phase) {
            .reading_header => {
                const needed = frame_header_length - @as(u32, state.header_bytes_read);
                const available = data.len - offset;
                const to_copy = @min(needed, available);

                @memcpy(
                    state.header_buf[state.header_bytes_read..][0..to_copy],
                    data[offset..][0..to_copy],
                );
                state.header_bytes_read += @intCast(to_copy);
                offset += to_copy;

                // Header not yet complete — wait for more data.
                if (state.header_bytes_read < frame_header_length) continue;

                // ── Header complete: extract frame_length ─────────────
                const header: *const FrameHeader = @ptrCast(&state.header_buf);
                state.frame_length = header.frame_length;

                // Validate frame_length before proceeding.
                if (state.frame_length < frame_header_length or
                    state.frame_length > self.config.max_frame_length)
                {
                    // Protocol error: cannot recover framing. Close connection.
                    return .{ .frames = frames_processed, .err = .protocol_error };
                }

                const payload_len = state.frame_length - frame_header_length;
                if (payload_len == 0) {
                    // Header-only frame (e.g., heartbeat). Process immediately.
                    self.processCompleteFrame(peer, &state.header_buf, &.{});
                    frames_processed += 1;
                    state.reset();
                } else {
                    // Transition to payload reading phase.
                    state.phase = .reading_payload;
                    state.payload_bytes_read = 0;
                }
            },

            .reading_payload => {
                const needed = state.payloadLength() - state.payload_bytes_read;
                const available = data.len - offset;
                const to_copy = @min(needed, available);

                @memcpy(
                    state.payload_buf[state.payload_bytes_read..][0..to_copy],
                    data[offset..][0..to_copy],
                );
                state.payload_bytes_read += @intCast(to_copy);
                offset += to_copy;

                // Payload not yet complete — wait for more data.
                if (state.payload_bytes_read < state.payloadLength()) continue;

                // ── Frame complete: validate and route ─────────────────
                self.processCompleteFrame(
                    peer,
                    &state.header_buf,
                    state.payload_buf[0..state.payloadLength()],
                );
                frames_processed += 1;
                state.reset();
            },
        }

        // Enforce per-peer budget within a single read completion.
        if (frames_processed >= self.config.read_budget_per_peer) break;
    }

    return .{ .frames = frames_processed, .err = null };
}

const ProcessResult = struct {
    frames: u32,
    err: ?ProcessError,

    const ProcessError = enum {
        protocol_error,
    };
};
```

### 3.4 Handling Partial Reads

TCP partial reads are the normal case, not an exception. The state machine handles
them naturally:

| Scenario | State machine behavior |
|----------|----------------------|
| Read returns 10 bytes (partial header) | `header_bytes_read` advances to 10, stays in `reading_header` |
| Next read returns 14 bytes (rest of header) | Header completes, transition to `reading_payload` or process if payload_len == 0 |
| Read returns header + partial payload | Header processed, payload partially filled, stays in `reading_payload` |
| Read returns last N bytes of payload + next header start | Payload completes, frame processed, remaining bytes start next header |
| Read returns multiple complete frames | Loop processes each frame, respecting per-peer budget |

---

## 4. Frame Validation

After the read state machine assembles a complete frame (header + payload), the
frame is validated before routing.

### 4.1 Wire Format: Frame Header

The frame header is a 24-byte packed struct. All fields are little-endian.

```zig
const FrameHeader = packed struct {
    /// Total frame length in bytes, including this header.
    /// Minimum: 24 (header only). Maximum: MAX_FRAME_LENGTH.
    frame_length: u32,

    /// Frame flags. Bit 0: ADMIN_FLAG (frame is an admin/control message).
    flags: u8,

    /// Node ID of the broker that sent this frame.
    source_node_id: u8,

    /// Node ID of the broker this frame is addressed to.
    target_node_id: u8,

    /// Reserved byte, must be zero.
    reserved_0: u8,

    /// Service ID of the sending service (on the source broker).
    source_service_id: u16,

    /// Service ID of the target service (on this broker).
    target_service_id: u16,

    /// SBE template ID identifying the message schema.
    template_id: u16,

    /// Reserved, must be zero.
    reserved_1: u16,

    /// Application-level correlation ID for request-response matching.
    correlation_id: i64,

    comptime {
        std.debug.assert(@sizeOf(FrameHeader) == 24);
    }
};

const ADMIN_FLAG: u8 = 0x01;
const HEARTBEAT_TEMPLATE: u16 = 0xFFFF;
```

### 4.2 Validation Steps

```zig
/// Validate a complete frame. Returns true if the frame should be routed,
/// false if it was dropped or handled as an error.
fn validateFrame(
    self: *ReceiverEventLoop,
    peer: *PeerConnection,
    header: *const FrameHeader,
) FrameAction {
    // ── Check 1: source_node_id matches the peer ──────────────────────
    // The source_node_id in the frame must match the node_id established
    // during the handshake for this connection. A mismatch indicates
    // either a bug in the sender or a corrupted frame. This is non-fatal
    // because framing is still intact (we read frame_length bytes).
    if (header.source_node_id != peer.node_id) {
        self.counters.increment(.invalid_frame_drops);
        peer.counters.invalid_frame_drops += 1;
        return .drop;
    }

    // ── Check 2: target_node_id matches this broker ───────────────────
    // A frame addressed to a different broker should never arrive on our
    // connection, but we check defensively.
    if (header.target_node_id != self.local_node_id) {
        self.counters.increment(.invalid_frame_drops);
        peer.counters.invalid_frame_drops += 1;
        return .drop;
    }

    // ── Check 3: Classify frame type ──────────────────────────────────
    if (header.template_id == HEARTBEAT_TEMPLATE) {
        return .heartbeat;
    }

    if (header.flags & ADMIN_FLAG != 0) {
        return .admin;
    }

    return .route_to_service;
}

const FrameAction = enum {
    route_to_service,
    admin,
    heartbeat,
    drop,
};
```

### 4.3 Frame Length Validation

Frame length is validated during header parsing (see §3.3) *before* payload bytes
are read. This is critical because an invalid frame length means the byte stream
is desynchronized — we cannot determine where the next frame starts.

```
Frame length validation matrix:

  frame_length < 24                → Protocol error. Close connection.
                                     Cannot be a valid frame.

  frame_length > MAX_FRAME_LENGTH  → Protocol error. Close connection.
                                     Either corrupted stream or misbehaving peer.

  24 ≤ frame_length ≤ MAX          → Valid. Payload length = frame_length - 24.

  frame_length == 24               → Header-only frame (heartbeat, ack).
                                     payload_length = 0. Process immediately.
```

**Why close on invalid frame length:** Unlike node ID mismatches (where we can
skip `frame_length` bytes and find the next frame), an invalid frame length
means we don't know how many bytes to skip. The stream is irrecoverably
desynchronized. The only safe action is to close the connection and let the
peer reconnect.

---

## 5. Receiver Event Loop Duty Cycle

The receiver event loop follows the standard duty-cycle pattern described in
[doc 00](00-overview.md). Each iteration returns a work count that drives the idle
strategy. If no work was done across all phases, the idle strategy decides whether
to spin, yield, or park.

### 5.1 State

```zig
const platform = @import("../platform.zig");
const constants = platform.constants;
const Clock = platform.Clock;

const ReceiverEventLoop = struct {
    /// TCP I/O backend (io_uring on Linux, kqueue on macOS).
    tcp_io: *TcpIo,

    /// Listener socket file descriptor.
    listener_fd: std.posix.fd_t,

    /// Connected peers, keyed by node ID.
    peers: IntHashMap(*PeerConnection),

    /// Pool of pre-allocated PeerConnection structs.
    peer_pool: PeerConnectionPool,

    /// Pending handshake connections (not yet validated).
    pending_handshakes: PendingHandshakePool,

    /// Service table: maps service_id → service ring buffer.
    service_table: *ServiceTable,

    /// Admin message handler (heartbeats, cluster state, leader election).
    admin_handler: *AdminHandler,

    /// MPSC command queue from the control loop (add/remove peer, etc.).
    cmd_queue: *CommandQueue,

    /// Shared counters manager for observability.
    counters: *CountersManager,

    /// This broker's node ID.
    local_node_id: u8,

    /// Hash of the configured cluster group name.
    group_name_hash: u32,

    /// Configuration parameters.
    config: ReceiverConfig,

    /// Whether this event loop is running.
    running: platform.AtomicBool,

    const Self = @This();

    pub fn init(
        tcp_io: *TcpIo,
        service_table: *ServiceTable,
        admin_handler: *AdminHandler,
        cmd_queue: *CommandQueue,
        counters: *CountersManager,
        local_node_id: u8,
        group_name_hash: u32,
        config: ReceiverConfig,
        allocator: std.mem.Allocator,
    ) !Self {
        var self = Self{
            .tcp_io = tcp_io,
            .listener_fd = undefined,
            .peers = IntHashMap(*PeerConnection).init(allocator),
            .peer_pool = try PeerConnectionPool.init(constants.max_nodes, allocator),
            .pending_handshakes = try PendingHandshakePool.init(constants.max_pending_handshakes, allocator),
            .service_table = service_table,
            .admin_handler = admin_handler,
            .cmd_queue = cmd_queue,
            .counters = counters,
            .local_node_id = local_node_id,
            .group_name_hash = group_name_hash,
            .config = config,
            .running = platform.AtomicBool.init(true),
        };

        try self.setupListener();
        return self;
    }
};
```

### 5.2 PeerConnection

One `PeerConnection` exists per connected peer broker. It holds the TCP connection
state, the read state machine, per-peer counters, and liveness tracking.

```zig
const PeerConnection = struct {
    /// The peer broker's node ID (established during handshake).
    node_id: u8,

    /// TCP socket file descriptor for this connection.
    fd: std.posix.fd_t,

    /// Session epoch from the handshake. Used to detect peer restarts.
    session_epoch: u32,

    /// Read state machine for TCP stream reassembly.
    read_state: ReadState,

    /// Pre-allocated read buffer for io_uring reads.
    read_buffer: []u8,

    /// Monotonic timestamp (ns) of the last frame received from this peer.
    /// Updated on every frame, including heartbeats.
    last_recv_ns: i64,

    /// Per-peer counters.
    counters: PeerCounters,

    /// Remaining read budget for the current duty cycle iteration.
    budget_remaining: u16,

    const Self = @This();

    pub fn init(
        node_id: u8,
        fd: std.posix.fd_t,
        session_epoch: u32,
        read_buffer_size: u32,
    ) Self {
        return .{
            .node_id = node_id,
            .fd = fd,
            .session_epoch = session_epoch,
            .read_state = ReadState.init(/* payload_buf from pool */),
            .read_buffer = undefined, // Set by caller from buffer pool
            .last_recv_ns = 0,
            .counters = PeerCounters.init(),
            .budget_remaining = 0,
        };
    }
};

const PeerCounters = struct {
    bytes_received: u64,
    frames_received: u64,
    invalid_frame_drops: u64,
    connection_errors: u64,
    heartbeats_received: u64,
    heartbeat_timeouts: u64,

    pub fn init() PeerCounters {
        return .{
            .bytes_received = 0,
            .frames_received = 0,
            .invalid_frame_drops = 0,
            .connection_errors = 0,
            .heartbeats_received = 0,
            .heartbeat_timeouts = 0,
        };
    }
};
```

### 5.3 Duty Cycle

```zig
/// Called by the ThreadRunner on every iteration.
/// Returns the total number of items processed (work count).
pub fn doWork(self: *ReceiverEventLoop) u32 {
    var work_count: u32 = 0;
    const now_ns = Clock.monotonicNanos();

    // ── Phase 1: Drain inter-event-loop command queue ────────────────
    // Process commands from the control loop: add peer, remove peer,
    // update configuration. Limited to command_drain_limit per cycle.
    work_count += self.cmd_queue.drain(
        dispatchCommand,
        @ptrCast(self),
        constants.command_drain_limit,
    );

    // ── Phase 2: Process I/O completions from io_uring/kqueue ────────
    // This drains completed read and accept operations. Each completion
    // delivers bytes from a connected peer or a newly accepted socket.
    work_count += self.tcp_io.pollCompletions(
        onIoCompletion,
        @ptrCast(self),
        constants.recv_batch_limit,
    );

    // ── Phase 3: Process pending accepts ─────────────────────────────
    // Complete handshake validation for connections that have finished
    // reading their 24-byte handshake frame.
    work_count += self.processAccepts();

    // ── Phase 4: Read frames from connected peers ────────────────────
    // Round-robin across all connected peers. Each peer gets up to
    // READ_BUDGET_PER_PEER frames read per iteration.
    work_count += self.readFromPeers(now_ns);

    // ── Phase 5: Check heartbeat timeouts ────────────────────────────
    // If no data (including heartbeats) received from a peer within
    // HEARTBEAT_TIMEOUT, mark the peer as suspect. After
    // PEER_LIVENESS_TIMEOUT, consider the peer dead.
    work_count += self.checkHeartbeatTimeouts(now_ns);

    return work_count;
}
```

**Phase ordering rationale:**

1. **Commands first** — peer additions/removals must be visible before we read data
   from those peers or accept new connections for them.
2. **I/O completions second** — drain kernel-delivered bytes. This populates the read
   buffers that subsequent phases will consume.
3. **Accepts third** — finalize new connections so they are available for reading
   in phase 4 of the *next* iteration.
4. **Peer reads fourth** — the core work: extract frames from buffered data, validate,
   and route to service ring buffers.
5. **Heartbeat timeouts last** — low-frequency housekeeping. Runs every iteration but
   only takes action when a timeout threshold is crossed.

### 5.4 Reading from Peers

The receiver reads from all connected peers in round-robin order, up to a per-peer
budget, to ensure fairness.

```zig
fn readFromPeers(self: *ReceiverEventLoop, now_ns: i64) u32 {
    var work_count: u32 = 0;

    var peer_iter = self.peers.valueIterator();
    while (peer_iter.next()) |peer| {
        peer.budget_remaining = self.config.read_budget_per_peer;

        while (peer.budget_remaining > 0) {
            const bytes_read = self.tcp_io.readAvailable(peer.fd, peer.read_buffer) catch |err| {
                switch (err) {
                    error.WouldBlock => break, // No more data available.
                    else => {
                        self.handleConnectionError(peer, err);
                        break;
                    },
                }
            };

            if (bytes_read == 0) {
                // EOF — peer closed the connection.
                self.handleConnectionClosed(peer);
                break;
            }

            // Feed received bytes into the read state machine.
            const result = self.processReceivedBytes(peer, peer.read_buffer[0..bytes_read]);

            peer.counters.bytes_received += bytes_read;
            peer.counters.frames_received += result.frames;
            self.counters.add(.bytes_received, @intCast(bytes_read));
            work_count += result.frames;

            if (result.frames > 0) {
                peer.last_recv_ns = now_ns;
            }

            peer.budget_remaining -|= @intCast(result.frames);

            // Protocol error — close connection.
            if (result.err) |_| {
                self.handleProtocolError(peer);
                break;
            }
        }
    }

    return work_count;
}
```

---

## 6. Message Routing to Service Ring Buffers

Once a frame passes validation, the receiver routes it to the target service's ring
buffer. This is the final step of the receive path for data messages.

### 6.1 Routing Logic

```zig
fn routeToService(
    self: *ReceiverEventLoop,
    header: *const FrameHeader,
    payload: []const u8,
) void {
    const target_service_id = header.target_service_id;
    const msg_type_id: i32 = if (header.template_id == 0)
        constants.application_msg_type_id
    else
        @intCast(header.template_id);

    // ── Look up the service ───────────────────────────────────────────
    const service = self.service_table.get(target_service_id) orelse {
        self.counters.increment(.unknown_service_drops);
        return;
    };

    // ── Write to the service's ring buffer ────────────────────────────
    const ring_buf = service.ring_buffer;
    ring_buf.write(msg_type_id, payload) catch {
        // Ring buffer full — DROP the message.
        // CRITICAL: Never block. The always-read model prevents
        // head-of-line blocking across services.
        self.counters.increment(.service_full_drops);
        service.counters.full_drops += 1;
        return;
    };

    self.counters.increment(.frames_routed);
}
```

### 6.2 The Always-Read Model

The receiver **always** reads from all TCP connections, regardless of whether any
service ring buffer is full. This is a fundamental design principle.

**Why always-read matters:**

```
Scenario: Service A's ring buffer is full.

  ✗ Pause-read model (NOT used):
    Stop reading from the TCP socket.
    TCP receive window fills up.
    Kernel stops ACKing data from the peer.
    Peer's TCP send buffer fills up.
    ALL messages from that peer are blocked — including messages
    for Service B, Service C, etc. (head-of-line blocking).

  ✓ Always-read model (RingLoom approach):
    Keep reading from the TCP socket.
    Messages for Service A are dropped (counter incremented).
    Messages for Service B, Service C flow normally.
    No head-of-line blocking.
```

The tradeoff is clear: dropped messages for a slow service vs. head-of-line
blocking that affects all services on that connection. RingLoom chooses drops.

### 6.3 Ring Buffer Write Details

The service ring buffer uses the standard MPSC ring buffer from
[doc 03](03-concurrent-data-structures.md). The receiver is one of potentially
multiple writers (local services also write to the same ring buffer for
same-host messages). Each write is atomic at the record level — other readers
see either the complete record or nothing.

```
Ring buffer record layout:

┌──────────────────────────────────────────────────────────────┐
│  Record Header (8 bytes)                                     │
│  +0: length (i32) — total record length including header     │
│  +4: msg_type_id (i32) — template_id, or application type if │
│      the TCP template_id is 0                                │
├──────────────────────────────────────────────────────────────┤
│  Application payload (frame_length - 24 bytes)               │
├──────────────────────────────────────────────────────────────┤
│  Padding (to alignment boundary)                             │
└──────────────────────────────────────────────────────────────┘
```

The TCP frame header is not delivered to services. It remains a broker-to-broker
transport header. The application-visible contract is the same as local IPC:
handlers receive a `msg_type_id` and an application payload slice.

---

## 7. Admin Message Handling

Admin messages are control-plane frames processed by the broker itself rather than
routed to application services. They are identified by the `ADMIN_FLAG` bit in the
frame header's `flags` field.

### 7.1 Admin Message Classification

```zig
fn processAdminMessage(
    self: *ReceiverEventLoop,
    peer: *PeerConnection,
    header: *const FrameHeader,
    payload: []const u8,
) void {
    // Admin messages are dispatched by template_id.
    switch (header.template_id) {
        constants.TEMPLATE_CLUSTER_STATE_SYNC => {
            self.admin_handler.onClusterStateSync(peer.node_id, payload);
        },
        constants.TEMPLATE_LEADER_ELECTION => {
            self.admin_handler.onLeaderElection(peer.node_id, payload);
        },
        constants.TEMPLATE_CONTROL_MESSAGE => {
            self.admin_handler.onControlMessage(peer.node_id, payload);
        },
        constants.TEMPLATE_PEER_DISCOVERY => {
            self.admin_handler.onPeerDiscovery(peer.node_id, payload);
        },
        else => {
            // Unknown admin template — drop silently.
            self.counters.increment(.unknown_admin_drops);
        },
    }
}
```

### 7.2 Admin Message Routing

Admin messages bypass the service ring buffer entirely:

```
                    ┌──────────────┐
  flags & ADMIN ──►│ Admin Handler │──► Cluster state machine
                    │              │──► Leader election logic
                    │              │──► Control plane responses
                    └──────────────┘

                    ┌──────────────┐
  flags == 0    ──►│ Service Table │──► Service ring buffer
                    └──────────────┘
```

Even admin messages are subject to the always-read model. The admin handler must
not block — if it needs to do expensive work (e.g., cluster state recalculation),
it enqueues the work for the control loop and returns immediately.

---

## 8. Heartbeat Processing

Heartbeat frames are lightweight keep-alive messages exchanged between brokers. On
the receive side, they serve one purpose: prove that the peer is still alive and
the TCP connection is healthy.

### 8.1 Heartbeat Frame Format

A heartbeat is a header-only frame (no payload) with `template_id = HEARTBEAT_TEMPLATE`.

```
Heartbeat frame (24 bytes):

┌─────────────────────────────────────────────┐
│  frame_length   = 24  (header only)         │
│  flags          = 0                         │
│  source_node_id = peer's node ID            │
│  target_node_id = this broker's node ID     │
│  reserved_0     = 0                         │
│  source_service_id = 0                      │
│  target_service_id = 0                      │
│  template_id    = 0xFFFF (HEARTBEAT)        │
│  reserved_1     = 0                         │
│  correlation_id = 0                         │
└─────────────────────────────────────────────┘
```

### 8.2 Heartbeat Reception

```zig
fn processHeartbeat(
    self: *ReceiverEventLoop,
    peer: *PeerConnection,
    now_ns: i64,
) void {
    peer.last_recv_ns = now_ns;
    peer.counters.heartbeats_received += 1;
    self.counters.increment(.heartbeats_received);
}
```

### 8.3 Heartbeat Timeout Detection

The receiver checks heartbeat timeouts for all connected peers on every duty cycle
iteration. Two thresholds are used:

| Threshold | Default | Action |
|-----------|---------|--------|
| `heartbeat_timeout_ms` | 2000 ms | Mark peer as **suspect** — log warning, increment counter |
| `peer_liveness_timeout_ms` | 5000 ms | Mark peer as **dead** — close connection, notify sender |

```zig
fn checkHeartbeatTimeouts(self: *ReceiverEventLoop, now_ns: i64) u32 {
    var work_count: u32 = 0;

    const heartbeat_timeout_ns: i64 = @as(i64, self.config.heartbeat_timeout_ms) * std.time.ns_per_ms;
    const liveness_timeout_ns: i64 = @as(i64, self.config.peer_liveness_timeout_ms) * std.time.ns_per_ms;

    var peer_iter = self.peers.valueIterator();
    while (peer_iter.next()) |peer| {
        if (peer.last_recv_ns == 0) continue; // Not yet received first frame.

        const elapsed_ns = now_ns - peer.last_recv_ns;

        if (elapsed_ns >= liveness_timeout_ns) {
            // ── Peer is dead ──────────────────────────────────────────
            // Close connection and notify the sender loop.
            peer.counters.heartbeat_timeouts += 1;
            self.counters.increment(.heartbeat_timeouts);
            self.handlePeerDead(peer);
            work_count += 1;
        } else if (elapsed_ns >= heartbeat_timeout_ns) {
            // ── Peer is suspect ───────────────────────────────────────
            // Log warning but keep the connection open. The peer may
            // recover (e.g., GC pause, network hiccup).
            // Only log once per timeout period to avoid log spam.
            if (!peer.suspect_logged) {
                peer.suspect_logged = true;
                peer.counters.heartbeat_timeouts += 1;
                self.counters.increment(.heartbeat_timeouts);
                work_count += 1;
            }
        } else {
            // Peer is healthy — clear suspect flag.
            peer.suspect_logged = false;
        }
    }

    return work_count;
}
```

### 8.4 Liveness State Diagram

```
                                   heartbeat/data received
                                ┌──────────────────────────┐
                                │                          │
                                ▼                          │
  ┌───────────┐  timeout_ms  ┌─────────┐  liveness_ms  ┌──┴──┐
  │  Healthy   │────────────►│ Suspect  │──────────────►│ Dead │
  └───────────┘              └─────────┘               └──────┘
       ▲                         │                        │
       │    heartbeat/data       │                        │
       └─────────────────────────┘                        │
                                                          ▼
                                               Close connection,
                                               notify sender loop,
                                               wait for reconnect
```

---

## 9. Connection Failure Handling

TCP connections can fail for many reasons: peer crash, network partition, connection
reset, or the peer intentionally closing the connection. The receiver handles all
failure modes through a single cleanup path.

### 9.1 Failure Modes

| Error | Source | Severity |
|-------|--------|----------|
| `ConnectionResetByPeer` | Peer process crashed or forcefully closed | Fatal to connection |
| EOF (read returns 0) | Peer gracefully closed the connection | Fatal to connection |
| `BrokenPipe` | Write to a closed connection (during handshake response) | Fatal to connection |
| `ConnectionRefused` | Should not happen on accepted connections | Fatal to connection |
| `TimedOut` | TCP keepalive timeout (if enabled) | Fatal to connection |

### 9.2 Connection Cleanup

```zig
fn handleConnectionError(self: *ReceiverEventLoop, peer: *PeerConnection, err: anyerror) void {
    peer.counters.connection_errors += 1;
    self.counters.increment(.connection_errors);
    self.closePeerConnection(peer);
}

fn handleConnectionClosed(self: *ReceiverEventLoop, peer: *PeerConnection) void {
    self.counters.increment(.connection_errors);
    self.closePeerConnection(peer);
}

fn handleProtocolError(self: *ReceiverEventLoop, peer: *PeerConnection) void {
    self.counters.increment(.invalid_frame_drops);
    self.closePeerConnection(peer);
}

fn handlePeerDead(self: *ReceiverEventLoop, peer: *PeerConnection) void {
    self.closePeerConnection(peer);
}

/// Central cleanup for all connection closures.
fn closePeerConnection(self: *ReceiverEventLoop, peer: *PeerConnection) void {
    const node_id = peer.node_id;

    // ── Step 1: Close the TCP socket ──────────────────────────────────
    std.posix.close(peer.fd);

    // ── Step 2: Clean up read state ───────────────────────────────────
    // Return the payload buffer to the buffer pool.
    self.payload_buffer_pool.release(peer.read_state.payload_buf);

    // Return the read buffer to the buffer pool.
    self.read_buffer_pool.release(peer.read_buffer);

    // ── Step 3: Remove from peer table ────────────────────────────────
    _ = self.peers.remove(node_id);

    // ── Step 4: Notify sender event loop ──────────────────────────────
    // The sender needs to close its outgoing connection to this peer
    // and stop sending data. This is done via the inter-loop command queue.
    self.notifySenderPeerDisconnected(node_id);

    // ── Step 5: Return PeerConnection to pool ─────────────────────────
    self.peer_pool.release(peer);
}

fn notifySenderPeerDisconnected(self: *ReceiverEventLoop, node_id: u8) void {
    const cmd = Command{ .peer_disconnected = .{ .node_id = node_id } };
    self.sender_cmd_queue.push(cmd) catch {
        // If the command queue is full, the sender will detect the dead
        // peer via its own heartbeat timeout. Not ideal but safe.
    };
}
```

### 9.3 Reconnection Protocol

The receiver does **not** initiate reconnection. The reconnection flow is:

```
  Receiver detects failure → closes connection → notifies sender
                                                        │
  Sender closes its outgoing connection ◄───────────────┘
                                                        │
  Sender reconnects (outgoing) ─────────────────────────┘
                                                        │
  Peer's receiver accepts the new connection ◄──────────┘
                                                        │
  Peer's sender reconnects to us ───────────────────────┘
                                                        │
  Our receiver accepts the new incoming connection ◄────┘
```

This asymmetric model — sender reconnects, receiver waits — avoids connection
storms and simplifies the state machine. Each side has a single responsibility.

### 9.4 Coordinated Connection Recycling

When the sender loop decides to recycle a connection (e.g., session epoch change),
it sends a command to the receiver via the inter-loop command queue:

```zig
fn dispatchCommand(context: *anyopaque, cmd: Command) void {
    const self: *ReceiverEventLoop = @ptrCast(@alignCast(context));

    switch (cmd) {
        .close_peer_connection => |close| {
            if (self.peers.getPtr(close.node_id)) |peer| {
                self.closePeerConnection(peer);
            }
        },
        .update_config => |config| {
            self.config = config;
        },
        .shutdown => {
            self.running.store(false, .release);
        },
    }
}
```

---

## 10. Buffer Management

The receive path is designed for zero allocations on the hot path. All buffers are
pre-allocated at startup and recycled during operation.

### 10.1 Buffer Architecture

```
                    ┌─────────────────────────────────────┐
                    │  Read Buffer Pool                    │
                    │  (one per connection, 64KB each)     │
                    │                                     │
                    │  ┌────────┐ ┌────────┐ ┌────────┐  │
                    │  │ 64KB   │ │ 64KB   │ │ 64KB   │  │
                    │  └────────┘ └────────┘ └────────┘  │
                    └─────────────────────────────────────┘
                                    │
                                    │ io_uring read deposits
                                    │ bytes here
                                    ▼
                    ┌─────────────────────────────────────┐
                    │  Payload Buffer Pool                 │
                    │  (one per connection, MAX_FRAME_LEN) │
                    │                                     │
                    │  ┌──────────┐ ┌──────────┐          │
                    │  │ 1MB max  │ │ 1MB max  │          │
                    │  └──────────┘ └──────────┘          │
                    └─────────────────────────────────────┘
                                    │
                                    │ state machine copies
                                    │ payload bytes here
                                    ▼
                    ┌─────────────────────────────────────┐
                    │  Service Ring Buffer                  │
                    │  (shared memory, per-service)         │
                    │                                     │
                    │  header + payload copied here         │
                    └─────────────────────────────────────┘
```

### 10.2 Read Buffer Pool

Each connection has a dedicated read buffer where the kernel (via io_uring or kqueue)
deposits bytes from `read()` or `recv()` calls. The buffer is pre-allocated and
registered with io_uring for zero-copy reads.

```zig
const ReadBufferPool = struct {
    buffers: [][]align(64) u8,
    free_stack: []u32,
    free_count: u32,
    capacity: u32,

    pub fn init(count: u32, buf_size: u32, allocator: std.mem.Allocator) !ReadBufferPool {
        const buffers = try allocator.alloc([]align(64) u8, count);
        const free_stack = try allocator.alloc(u32, count);

        for (0..count) |i| {
            buffers[i] = try allocator.alignedAlloc(u8, 64, buf_size);
            free_stack[i] = @intCast(i);
        }

        return .{
            .buffers = buffers,
            .free_stack = free_stack,
            .free_count = count,
            .capacity = count,
        };
    }

    pub fn acquire(self: *ReadBufferPool) ?[]align(64) u8 {
        if (self.free_count == 0) return null;
        self.free_count -= 1;
        return self.buffers[self.free_stack[self.free_count]];
    }

    pub fn release(self: *ReadBufferPool, buf: []align(64) u8) void {
        const idx = self.findIndex(buf);
        self.free_stack[self.free_count] = idx;
        self.free_count += 1;
    }

    fn findIndex(self: *ReadBufferPool, buf: []align(64) u8) u32 {
        for (self.buffers, 0..) |b, i| {
            if (b.ptr == buf.ptr) return @intCast(i);
        }
        unreachable;
    }
};
```

### 10.3 Payload Buffer Pool

Payload buffers hold the message payload while the read state machine accumulates
bytes across multiple TCP reads. Each payload buffer is sized to `max_frame_length`
to handle the largest possible frame without reallocation.

```zig
const PayloadBufferPool = struct {
    buffers: [][]u8,
    free_stack: []u32,
    free_count: u32,
    capacity: u32,

    pub fn init(count: u32, max_frame_length: u32, allocator: std.mem.Allocator) !PayloadBufferPool {
        const payload_size = max_frame_length - frame_header_length;
        const buffers = try allocator.alloc([]u8, count);
        const free_stack = try allocator.alloc(u32, count);

        for (0..count) |i| {
            buffers[i] = try allocator.alloc(u8, payload_size);
            free_stack[i] = @intCast(i);
        }

        return .{
            .buffers = buffers,
            .free_stack = free_stack,
            .free_count = count,
            .capacity = count,
        };
    }

    pub fn acquire(self: *PayloadBufferPool) ?[]u8 {
        if (self.free_count == 0) return null;
        self.free_count -= 1;
        return self.buffers[self.free_stack[self.free_count]];
    }

    pub fn release(self: *PayloadBufferPool, buf: []u8) void {
        for (self.buffers, 0..) |b, i| {
            if (b.ptr == buf.ptr) {
                self.free_stack[self.free_count] = @intCast(i);
                self.free_count += 1;
                return;
            }
        }
        unreachable;
    }
};
```

### 10.4 io_uring Registered Buffers

On Linux, the receiver registers its read buffers with io_uring at startup using
`IORING_REGISTER_BUFFERS`. This allows the kernel to skip the `copy_from_user` /
`copy_to_user` step and read directly into the pre-registered buffer pages.

```zig
fn registerBuffersWithIoUring(self: *ReceiverEventLoop) !void {
    var iovecs: [constants.max_nodes]std.posix.iovec = undefined;
    var count: u32 = 0;

    var pool_iter = self.read_buffer_pool.buffers;
    for (pool_iter) |buf| {
        if (count >= constants.max_nodes) break;
        iovecs[count] = .{
            .base = buf.ptr,
            .len = buf.len,
        };
        count += 1;
    }

    try self.tcp_io.registerBuffers(iovecs[0..count]);
}
```

### 10.5 Buffer Lifecycle

```
  Connection accepted
       │
       ▼
  Acquire read_buffer from ReadBufferPool
  Acquire payload_buf from PayloadBufferPool
       │
       ▼
  ┌─── io_uring reads into read_buffer ◄──────────────────────┐
  │    State machine copies to header_buf / payload_buf        │
  │         │                                                  │
  │         ▼                                                  │
  │    Frame complete → copy to service ring buffer            │
  │    State machine reset → ready for next frame              │
  │         │                                                  │
  └─────────┘                                                  │
                                                               │
  Connection closed                                            │
       │                                                       │
       ▼                                                       │
  Release read_buffer back to ReadBufferPool                   │
  Release payload_buf back to PayloadBufferPool                │
```

---

## 11. Fairness and Budgets

Without fairness controls, a single peer sending at high rate could monopolize the
receiver's CPU time, starving reads from other peers. RingLoom uses per-peer budgets to
prevent this.

### 11.1 Per-Peer Read Budget

Each peer is allowed to have at most `READ_BUDGET_PER_PEER` (default: 16) frames
processed per duty cycle iteration. After a peer's budget is exhausted, the receiver
moves on to the next peer, even if more data is available on the socket.

```zig
const READ_BUDGET_PER_PEER: u16 = 16;
```

### 11.2 Budget Accounting

```zig
fn readFromPeerWithBudget(self: *ReceiverEventLoop, peer: *PeerConnection, now_ns: i64) u32 {
    var frames_this_peer: u32 = 0;
    peer.budget_remaining = self.config.read_budget_per_peer;

    while (peer.budget_remaining > 0) {
        const bytes_read = self.tcp_io.readAvailable(peer.fd, peer.read_buffer) catch |err| {
            switch (err) {
                error.WouldBlock => break,
                else => {
                    self.handleConnectionError(peer, err);
                    return frames_this_peer;
                },
            }
        };

        if (bytes_read == 0) {
            self.handleConnectionClosed(peer);
            return frames_this_peer;
        }

        const result = self.processReceivedBytes(peer, peer.read_buffer[0..bytes_read]);
        frames_this_peer += result.frames;
        peer.budget_remaining -|= @intCast(result.frames);

        if (result.frames > 0) {
            peer.last_recv_ns = now_ns;
        }

        if (result.err != null) {
            self.handleProtocolError(peer);
            return frames_this_peer;
        }

        // If processReceivedBytes consumed all data without hitting budget,
        // the state machine is mid-frame. We'll pick up more bytes next time
        // io_uring delivers a completion.
    }

    return frames_this_peer;
}
```

### 11.3 Fairness Properties

| Property | Guarantee |
|----------|-----------|
| Bounded latency per peer | At most `READ_BUDGET_PER_PEER` frames processed before switching |
| Round-robin scheduling | Every connected peer is visited once per duty cycle |
| No starvation | Even if one peer sends 10,000 frames/s, others get their budget |
| Predictable worst case | Max frames per iteration = `READ_BUDGET_PER_PEER × num_peers` |

### 11.4 Total Budget per Iteration

The total number of frames the receiver will process in one duty cycle iteration is
bounded by:

```
  total_budget = READ_BUDGET_PER_PEER × num_connected_peers
```

For example, with the default budget of 16 and 8 connected peers:

```
  total_budget = 16 × 8 = 128 frames per iteration
```

This bounds the worst-case latency of a single duty cycle iteration and ensures the
receiver returns to the idle strategy promptly when work is done.

---

## 12. Counters and Monitoring

The receiver maintains counters at two levels: per-peer (in `PeerCounters`) and global
(in the shared `CountersManager`). All counters are monotonically increasing.

### 12.1 Global Counters

| Counter | Scope | Description |
|---------|-------|-------------|
| `bytes_received` | per peer | Total bytes read from TCP sockets |
| `frames_received` | per peer | Total complete frames extracted from the stream |
| `frames_routed` | total | Frames successfully written to service ring buffers |
| `service_full_drops` | per service, total | Frames dropped because the target service ring buffer is full |
| `unknown_service_drops` | total | Frames dropped because `target_service_id` is not in the service table |
| `invalid_frame_drops` | per peer | Frames dropped due to node ID mismatch or other validation failure |
| `connection_errors` | per peer | Read errors (reset, EOF, broken pipe) that closed a connection |
| `connections_accepted` | total | TCP connections accepted by the listener |
| `handshake_failures` | total | Handshake validations that failed (bad magic, version, etc.) |
| `heartbeats_received` | per peer | Heartbeat frames received |
| `heartbeat_timeouts` | per peer | Times a peer crossed the heartbeat timeout threshold |
| `read_completions_processed` | total | io_uring/kqueue read completions drained |
| `unknown_admin_drops` | total | Admin frames with unrecognized template_id |

### 12.2 Counter Implementation

```zig
const ReceiverCounterId = enum {
    bytes_received,
    frames_received,
    frames_routed,
    service_full_drops,
    unknown_service_drops,
    invalid_frame_drops,
    connection_errors,
    connections_accepted,
    handshake_failures,
    heartbeats_received,
    heartbeat_timeouts,
    read_completions_processed,
    unknown_admin_drops,
};
```

Per-peer counters are aggregated into the global counters when a peer disconnects or
when the monitoring system reads counters. This avoids atomic operations on per-peer
counters (since only the receiver thread writes them) while still providing a global
view.

---

## 13. Configuration Parameters

All receiver configuration is set at startup and can be updated at runtime via the
command queue (where noted).

| Parameter | Default | Description | Runtime update |
|-----------|---------|-------------|----------------|
| `tcp_listen_port` | 9100 | TCP port the listener binds to | No |
| `tcp_listen_backlog` | 128 | `listen()` backlog for pending connections | No |
| `read_budget_per_peer` | 16 | Maximum frames read from one peer per duty cycle | Yes |
| `max_frame_length` | 1,048,576 (1 MB) | Maximum allowed frame_length in header. Frames exceeding this cause a protocol error. | No |
| `heartbeat_timeout_ms` | 2000 | Time without data before peer is marked suspect | Yes |
| `peer_liveness_timeout_ms` | 5000 | Time without data before peer is considered dead and connection is closed | Yes |
| `read_buffer_size` | 65,536 (64 KB) | Size of the pre-allocated read buffer per connection | No |

### 13.1 Configuration Struct

```zig
const ReceiverConfig = struct {
    tcp_listen_port: u16 = 9100,
    tcp_listen_backlog: u31 = 128,
    read_budget_per_peer: u16 = 16,
    max_frame_length: u32 = 1_048_576,
    heartbeat_timeout_ms: u32 = 2000,
    peer_liveness_timeout_ms: u32 = 5000,
    read_buffer_size: u32 = 65_536,
};
```

### 13.2 Configuration Validation

```zig
fn validateConfig(config: ReceiverConfig) !void {
    if (config.max_frame_length < frame_header_length) {
        return error.InvalidConfig; // Must fit at least one header.
    }
    if (config.heartbeat_timeout_ms >= config.peer_liveness_timeout_ms) {
        return error.InvalidConfig; // Suspect must fire before dead.
    }
    if (config.read_budget_per_peer == 0) {
        return error.InvalidConfig; // Must process at least one frame.
    }
    if (config.read_buffer_size < frame_header_length) {
        return error.InvalidConfig; // Must fit at least one header.
    }
}
```

---

## 14. Error Handling

All errors in the receive path are non-fatal to the event loop itself. Individual
connections may be closed, but the receiver continues operating for all other peers.

### 14.1 Error Classification

```
  ┌──────────────────────────────────────────────────────────────┐
  │                    Error Classification                      │
  ├──────────────────────┬───────────────────────────────────────┤
  │ Error                │ Action                                │
  ├──────────────────────┼───────────────────────────────────────┤
  │ Read error (TCP)     │ Close connection, clean up state,     │
  │ (reset, EOF, pipe)   │ wait for peer to reconnect.           │
  │                      │ Counter: connection_errors             │
  ├──────────────────────┼───────────────────────────────────────┤
  │ Invalid frame_length │ Protocol error — close connection.    │
  │ (< 24 or > max)      │ Stream is corrupted; cannot recover   │
  │                      │ framing. Counter: invalid_frame_drops  │
  ├──────────────────────┼───────────────────────────────────────┤
  │ Node ID mismatch     │ Drop frame, increment counter.        │
  │ (source or target)   │ Non-fatal: framing is intact, we      │
  │                      │ read exactly frame_length bytes.       │
  │                      │ Counter: invalid_frame_drops            │
  ├──────────────────────┼───────────────────────────────────────┤
  │ Service ring full    │ Drop message, increment counter.      │
  │                      │ Non-fatal: always-read model.          │
  │                      │ Counter: service_full_drops             │
  ├──────────────────────┼───────────────────────────────────────┤
  │ Unknown service      │ Drop message, increment counter.      │
  │                      │ Non-fatal: service may not be          │
  │                      │ registered yet.                        │
  │                      │ Counter: unknown_service_drops          │
  ├──────────────────────┼───────────────────────────────────────┤
  │ Handshake failure    │ Close connection immediately.          │
  │                      │ Counter: handshake_failures             │
  └──────────────────────┴───────────────────────────────────────┘
```

### 14.2 Fatal vs. Non-Fatal Decision Tree

```zig
fn processCompleteFrame(
    self: *ReceiverEventLoop,
    peer: *PeerConnection,
    header_buf: *const [24]u8,
    payload: []const u8,
) void {
    const header: *const FrameHeader = @ptrCast(header_buf);

    // Update last_recv_ns on every frame (including heartbeats).
    peer.last_recv_ns = Clock.monotonicNanos();
    peer.counters.frames_received += 1;

    const action = self.validateFrame(peer, header);

    switch (action) {
        .route_to_service => {
            self.routeToService(header, payload);
        },
        .admin => {
            self.processAdminMessage(peer, header, payload);
        },
        .heartbeat => {
            self.processHeartbeat(peer, Clock.monotonicNanos());
        },
        .drop => {
            // Already counted in validateFrame.
        },
    }
}
```

### 14.3 Event Loop Resilience

The receiver event loop itself never terminates due to a connection error. Only an
explicit shutdown command (from the control loop) stops the event loop:

```zig
pub fn run(self: *ReceiverEventLoop) void {
    while (self.running.load(.acquire)) {
        const work_count = self.doWork();

        if (work_count == 0) {
            self.idle_strategy.idle();
        } else {
            self.idle_strategy.reset();
        }
    }

    self.shutdown();
}

fn shutdown(self: *ReceiverEventLoop) void {
    // Close all peer connections.
    var peer_iter = self.peers.valueIterator();
    while (peer_iter.next()) |peer| {
        std.posix.close(peer.fd);
        self.payload_buffer_pool.release(peer.read_state.payload_buf);
        self.read_buffer_pool.release(peer.read_buffer);
    }
    self.peers.clearAndFree();

    // Close the listener socket.
    std.posix.close(self.listener_fd);
}
```

---

## 15. Testing Strategy

### 15.1 Unit Tests: Frame Parsing

```zig
test "parse complete frame from single read" {
    // Given: a complete frame (header + 100 bytes payload) in one buffer.
    var header_buf: [24]u8 = undefined;
    const header: *FrameHeader = @ptrCast(&header_buf);
    header.* = .{
        .frame_length = 124, // 24 + 100
        .flags = 0,
        .source_node_id = 2,
        .target_node_id = 1,
        .reserved_0 = 0,
        .source_service_id = 10,
        .target_service_id = 20,
        .template_id = 100,
        .reserved_1 = 0,
        .correlation_id = 42,
    };

    var payload: [100]u8 = undefined;
    @memset(&payload, 0xAA);

    var data: [124]u8 = undefined;
    @memcpy(data[0..24], &header_buf);
    @memcpy(data[24..124], &payload);

    // When: processed through the state machine.
    var peer = createTestPeer(2);
    var loop = createTestReceiverLoop(1);
    const result = loop.processReceivedBytes(&peer, &data);

    // Then: one frame processed, no errors.
    try std.testing.expectEqual(@as(u32, 1), result.frames);
    try std.testing.expect(result.err == null);
}

test "parse frame split across two reads" {
    // Given: a 124-byte frame split into two chunks.
    var frame_data: [124]u8 = undefined;
    buildTestFrame(&frame_data, 124, 2, 1);

    // When: first 16 bytes (partial header), then remaining 108 bytes.
    var peer = createTestPeer(2);
    var loop = createTestReceiverLoop(1);

    const result1 = loop.processReceivedBytes(&peer, frame_data[0..16]);
    try std.testing.expectEqual(@as(u32, 0), result1.frames); // No complete frame yet.

    const result2 = loop.processReceivedBytes(&peer, frame_data[16..]);
    try std.testing.expectEqual(@as(u32, 1), result2.frames); // Frame complete.
}

test "parse frame split mid-payload" {
    // Given: a 1024-byte frame (24 header + 1000 payload).
    var frame_data: [1024]u8 = undefined;
    buildTestFrame(&frame_data, 1024, 2, 1);

    // When: header arrives complete, but payload arrives in 3 chunks.
    var peer = createTestPeer(2);
    var loop = createTestReceiverLoop(1);

    // Chunk 1: full header + 200 bytes payload.
    const result1 = loop.processReceivedBytes(&peer, frame_data[0..224]);
    try std.testing.expectEqual(@as(u32, 0), result1.frames);

    // Chunk 2: 300 bytes more payload.
    const result2 = loop.processReceivedBytes(&peer, frame_data[224..524]);
    try std.testing.expectEqual(@as(u32, 0), result2.frames);

    // Chunk 3: remaining 500 bytes.
    const result3 = loop.processReceivedBytes(&peer, frame_data[524..]);
    try std.testing.expectEqual(@as(u32, 1), result3.frames);
}

test "multiple complete frames in single read" {
    // Given: three 48-byte frames concatenated in one buffer.
    var data: [144]u8 = undefined;
    buildTestFrame(data[0..48], 48, 2, 1);
    buildTestFrame(data[48..96], 48, 2, 1);
    buildTestFrame(data[96..144], 48, 2, 1);

    // When: all bytes arrive in a single read.
    var peer = createTestPeer(2);
    var loop = createTestReceiverLoop(1);
    const result = loop.processReceivedBytes(&peer, &data);

    // Then: three frames processed.
    try std.testing.expectEqual(@as(u32, 3), result.frames);
}
```

### 15.2 Unit Tests: Handshake Validation

```zig
test "valid handshake accepted" {
    var loop = createTestReceiverLoop(1);
    var buf: [24]u8 = undefined;
    writeTestHandshake(&buf, .{
        .magic = HANDSHAKE_MAGIC,
        .protocol_version = PROTOCOL_VERSION,
        .source_node_id = 2,
        .target_node_id = 1,
        .direction = DIRECTION_SEND,
        .group_name_hash = loop.group_name_hash,
        .session_epoch = 1,
        .reserved = 0,
    });

    const result = loop.validateHandshake(&buf, createTestFd());
    try std.testing.expect(result != null);
}

test "handshake with wrong magic rejected" {
    var loop = createTestReceiverLoop(1);
    var buf: [24]u8 = undefined;
    writeTestHandshake(&buf, .{
        .magic = 0xDEADBEEF,
        .protocol_version = PROTOCOL_VERSION,
        .source_node_id = 2,
        .target_node_id = 1,
        .direction = DIRECTION_SEND,
        .group_name_hash = loop.group_name_hash,
        .session_epoch = 1,
        .reserved = 0,
    });

    const result = loop.validateHandshake(&buf, createTestFd());
    try std.testing.expect(result == null);
}

test "handshake with wrong target_node_id rejected" {
    var loop = createTestReceiverLoop(1);
    var buf: [24]u8 = undefined;
    writeTestHandshake(&buf, .{
        .magic = HANDSHAKE_MAGIC,
        .protocol_version = PROTOCOL_VERSION,
        .source_node_id = 2,
        .target_node_id = 99, // Wrong — we are node 1.
        .direction = DIRECTION_SEND,
        .group_name_hash = loop.group_name_hash,
        .session_epoch = 1,
        .reserved = 0,
    });

    const result = loop.validateHandshake(&buf, createTestFd());
    try std.testing.expect(result == null);
}

test "handshake with newer epoch replaces old connection" {
    var loop = createTestReceiverLoop(1);

    // First connection from node 2 with epoch 1.
    var buf1: [24]u8 = undefined;
    writeTestHandshake(&buf1, .{
        .magic = HANDSHAKE_MAGIC,
        .protocol_version = PROTOCOL_VERSION,
        .source_node_id = 2,
        .target_node_id = 1,
        .direction = DIRECTION_SEND,
        .group_name_hash = loop.group_name_hash,
        .session_epoch = 1,
        .reserved = 0,
    });
    const peer1 = loop.validateHandshake(&buf1, createTestFd());
    try std.testing.expect(peer1 != null);

    // Second connection from node 2 with epoch 2 (peer restarted).
    var buf2: [24]u8 = undefined;
    writeTestHandshake(&buf2, .{
        .magic = HANDSHAKE_MAGIC,
        .protocol_version = PROTOCOL_VERSION,
        .source_node_id = 2,
        .target_node_id = 1,
        .direction = DIRECTION_SEND,
        .group_name_hash = loop.group_name_hash,
        .session_epoch = 2,
        .reserved = 0,
    });
    const peer2 = loop.validateHandshake(&buf2, createTestFd());
    try std.testing.expect(peer2 != null);
    try std.testing.expectEqual(@as(u32, 2), peer2.?.session_epoch);
}

test "handshake with stale epoch rejected" {
    var loop = createTestReceiverLoop(1);

    // First connection with epoch 5.
    var buf1: [24]u8 = undefined;
    writeTestHandshake(&buf1, .{
        .magic = HANDSHAKE_MAGIC,
        .protocol_version = PROTOCOL_VERSION,
        .source_node_id = 2,
        .target_node_id = 1,
        .direction = DIRECTION_SEND,
        .group_name_hash = loop.group_name_hash,
        .session_epoch = 5,
        .reserved = 0,
    });
    _ = loop.validateHandshake(&buf1, createTestFd());

    // Second connection with epoch 3 (stale).
    var buf2: [24]u8 = undefined;
    writeTestHandshake(&buf2, .{
        .magic = HANDSHAKE_MAGIC,
        .protocol_version = PROTOCOL_VERSION,
        .source_node_id = 2,
        .target_node_id = 1,
        .direction = DIRECTION_SEND,
        .group_name_hash = loop.group_name_hash,
        .session_epoch = 3,
        .reserved = 0,
    });
    const result = loop.validateHandshake(&buf2, createTestFd());
    try std.testing.expect(result == null);
}
```

### 15.3 Unit Tests: Routing Logic

```zig
test "route frame to existing service with capacity" {
    var loop = createTestReceiverLoop(1);
    var ring_buf = try createTestRingBuffer(4096);
    defer ring_buf.deinit();
    loop.service_table.put(20, &ring_buf);

    var header = createTestFrameHeader(.{
        .target_service_id = 20,
        .source_node_id = 2,
        .target_node_id = 1,
    });
    const payload = [_]u8{0x01} ** 100;

    loop.routeToService(&header, &payload);

    // Service ring buffer should contain the routed message.
    try std.testing.expectEqual(@as(u64, 1), loop.counters.get(.frames_routed));
    try std.testing.expectEqual(@as(u64, 0), loop.counters.get(.service_full_drops));
}

test "route frame to full service ring buffer drops message" {
    var loop = createTestReceiverLoop(1);
    var ring_buf = try createTestRingBuffer(64); // Tiny buffer — will be full.
    defer ring_buf.deinit();
    loop.service_table.put(20, &ring_buf);

    // Fill the ring buffer first.
    fillRingBuffer(&ring_buf);

    var header = createTestFrameHeader(.{
        .target_service_id = 20,
        .source_node_id = 2,
        .target_node_id = 1,
    });
    const payload = [_]u8{0x01} ** 100;

    loop.routeToService(&header, &payload);

    try std.testing.expectEqual(@as(u64, 0), loop.counters.get(.frames_routed));
    try std.testing.expectEqual(@as(u64, 1), loop.counters.get(.service_full_drops));
}

test "route frame to unknown service drops message" {
    var loop = createTestReceiverLoop(1);
    // No service registered for ID 99.

    var header = createTestFrameHeader(.{
        .target_service_id = 99,
        .source_node_id = 2,
        .target_node_id = 1,
    });
    const payload = [_]u8{0x01} ** 100;

    loop.routeToService(&header, &payload);

    try std.testing.expectEqual(@as(u64, 0), loop.counters.get(.frames_routed));
    try std.testing.expectEqual(@as(u64, 1), loop.counters.get(.unknown_service_drops));
}
```

### 15.4 Unit Tests: Frame Validation

```zig
test "frame with invalid frame_length triggers protocol error" {
    var peer = createTestPeer(2);
    var loop = createTestReceiverLoop(1);

    // Frame with frame_length = 10 (less than header size of 24).
    var bad_header: [24]u8 = undefined;
    const h: *FrameHeader = @ptrCast(&bad_header);
    h.frame_length = 10; // Invalid — minimum is 24.

    const result = loop.processReceivedBytes(&peer, &bad_header);
    try std.testing.expect(result.err != null);
    try std.testing.expectEqual(ProcessResult.ProcessError.protocol_error, result.err.?);
}

test "frame with frame_length exceeding max triggers protocol error" {
    var peer = createTestPeer(2);
    var loop = createTestReceiverLoop(1);

    var bad_header: [24]u8 = undefined;
    const h: *FrameHeader = @ptrCast(&bad_header);
    h.frame_length = loop.config.max_frame_length + 1; // Too large.

    const result = loop.processReceivedBytes(&peer, &bad_header);
    try std.testing.expect(result.err != null);
}

test "frame with wrong source_node_id is dropped (non-fatal)" {
    var peer = createTestPeer(2); // Peer is node 2.
    var loop = createTestReceiverLoop(1);

    var header = createTestFrameHeader(.{
        .source_node_id = 5, // Mismatch — peer is node 2.
        .target_node_id = 1,
        .target_service_id = 20,
    });

    const action = loop.validateFrame(&peer, &header);
    try std.testing.expectEqual(FrameAction.drop, action);
}

test "frame with wrong target_node_id is dropped (non-fatal)" {
    var peer = createTestPeer(2);
    var loop = createTestReceiverLoop(1);

    var header = createTestFrameHeader(.{
        .source_node_id = 2,
        .target_node_id = 99, // Wrong — we are node 1.
        .target_service_id = 20,
    });

    const action = loop.validateFrame(&peer, &header);
    try std.testing.expectEqual(FrameAction.drop, action);
}

test "heartbeat frame classified correctly" {
    var peer = createTestPeer(2);
    var loop = createTestReceiverLoop(1);

    var header = createTestFrameHeader(.{
        .source_node_id = 2,
        .target_node_id = 1,
        .template_id = HEARTBEAT_TEMPLATE,
    });

    const action = loop.validateFrame(&peer, &header);
    try std.testing.expectEqual(FrameAction.heartbeat, action);
}

test "admin frame classified correctly" {
    var peer = createTestPeer(2);
    var loop = createTestReceiverLoop(1);

    var header = createTestFrameHeader(.{
        .source_node_id = 2,
        .target_node_id = 1,
        .flags = ADMIN_FLAG,
    });

    const action = loop.validateFrame(&peer, &header);
    try std.testing.expectEqual(FrameAction.admin, action);
}
```

### 15.5 Integration Tests

```zig
test "receiver loop routes frames from mock TCP transport to ring buffers" {
    // Given: a mock TCP transport with two pre-loaded frames from peer 2.
    var mock_io = MockTcpIo.init();
    mock_io.enqueueFrame(2, buildFrame(2, 1, 20, "hello"));
    mock_io.enqueueFrame(2, buildFrame(2, 1, 30, "world"));

    // Given: two service ring buffers.
    var ring20 = try createTestRingBuffer(4096);
    defer ring20.deinit();
    var ring30 = try createTestRingBuffer(4096);
    defer ring30.deinit();

    var service_table = ServiceTable.init(std.testing.allocator);
    defer service_table.deinit();
    service_table.put(20, &ring20);
    service_table.put(30, &ring30);

    // Given: a receiver loop with the mock transport.
    var loop = try ReceiverEventLoop.initForTest(&mock_io, &service_table, 1);
    defer loop.deinit();

    // Simulate handshake for peer 2.
    loop.addTestPeer(2);

    // When: one duty cycle.
    const work = loop.doWork();

    // Then: both frames routed to their respective services.
    try std.testing.expect(work >= 2);
    try std.testing.expectEqual(@as(u64, 2), loop.counters.get(.frames_routed));
    try std.testing.expect(ring20.hasData());
    try std.testing.expect(ring30.hasData());
}

test "receiver loop handles rapid connect/disconnect" {
    var mock_io = MockTcpIo.init();
    var loop = try ReceiverEventLoop.initForTest(&mock_io, &ServiceTable.empty(), 1);
    defer loop.deinit();

    // Simulate 100 rapid connect/disconnect cycles from the same peer.
    for (0..100) |epoch| {
        var buf: [24]u8 = undefined;
        writeTestHandshake(&buf, .{
            .magic = HANDSHAKE_MAGIC,
            .protocol_version = PROTOCOL_VERSION,
            .source_node_id = 2,
            .target_node_id = 1,
            .direction = DIRECTION_SEND,
            .group_name_hash = loop.group_name_hash,
            .session_epoch = @intCast(epoch + 1),
            .reserved = 0,
        });

        const peer = loop.validateHandshake(&buf, createTestFd());
        try std.testing.expect(peer != null);

        // Simulate disconnect.
        loop.closePeerConnection(peer.?);
    }

    try std.testing.expectEqual(@as(u64, 100), loop.counters.get(.connections_accepted));
}

test "receiver loop enforces per-peer read budget" {
    var mock_io = MockTcpIo.init();

    // Peer 2 sends 50 small frames.
    for (0..50) |_| {
        mock_io.enqueueFrame(2, buildFrame(2, 1, 20, "x"));
    }

    var ring20 = try createTestRingBuffer(65536);
    defer ring20.deinit();
    var service_table = ServiceTable.init(std.testing.allocator);
    defer service_table.deinit();
    service_table.put(20, &ring20);

    var loop = try ReceiverEventLoop.initForTest(&mock_io, &service_table, 1);
    defer loop.deinit();
    loop.addTestPeer(2);

    // When: one duty cycle with budget = 16.
    const work = loop.doWork();

    // Then: at most READ_BUDGET_PER_PEER frames processed.
    try std.testing.expect(work <= 16);

    // Second duty cycle processes more.
    const work2 = loop.doWork();
    try std.testing.expect(work2 <= 16);
}

test "maximum frame size handled correctly" {
    var mock_io = MockTcpIo.init();

    // Build a frame at the maximum allowed size (1 MB).
    const max_payload_len = 1_048_576 - 24;
    const large_payload = try std.testing.allocator.alloc(u8, max_payload_len);
    defer std.testing.allocator.free(large_payload);
    @memset(large_payload, 0xBB);

    mock_io.enqueueFrame(2, buildFrameWithPayload(2, 1, 20, large_payload));

    var ring20 = try createTestRingBuffer(2 * 1_048_576);
    defer ring20.deinit();
    var service_table = ServiceTable.init(std.testing.allocator);
    defer service_table.deinit();
    service_table.put(20, &ring20);

    var loop = try ReceiverEventLoop.initForTest(&mock_io, &service_table, 1);
    defer loop.deinit();
    loop.addTestPeer(2);

    const work = loop.doWork();
    try std.testing.expect(work >= 1);
    try std.testing.expectEqual(@as(u64, 1), loop.counters.get(.frames_routed));
}
```

### 15.6 Edge Case Tests

```zig
test "header-only frame (heartbeat) processed without payload read phase" {
    var peer = createTestPeer(2);
    var loop = createTestReceiverLoop(1);

    // Heartbeat: frame_length = 24 (header only), template_id = HEARTBEAT_TEMPLATE.
    var frame_data: [24]u8 = undefined;
    const h: *FrameHeader = @ptrCast(&frame_data);
    h.* = .{
        .frame_length = 24,
        .flags = 0,
        .source_node_id = 2,
        .target_node_id = 1,
        .reserved_0 = 0,
        .source_service_id = 0,
        .target_service_id = 0,
        .template_id = HEARTBEAT_TEMPLATE,
        .reserved_1 = 0,
        .correlation_id = 0,
    };

    const result = loop.processReceivedBytes(&peer, &frame_data);

    try std.testing.expectEqual(@as(u32, 1), result.frames);
    try std.testing.expect(result.err == null);
    try std.testing.expectEqual(@as(u64, 1), peer.counters.heartbeats_received);
}

test "single byte reads still assemble complete frame" {
    var peer = createTestPeer(2);
    var loop = createTestReceiverLoop(1);

    var frame_data: [48]u8 = undefined; // 24 header + 24 payload.
    buildTestFrame(&frame_data, 48, 2, 1);

    // Feed one byte at a time.
    var total_frames: u32 = 0;
    for (0..48) |i| {
        const result = loop.processReceivedBytes(&peer, frame_data[i .. i + 1]);
        total_frames += result.frames;
        try std.testing.expect(result.err == null);
    }

    try std.testing.expectEqual(@as(u32, 1), total_frames);
}

test "two frames arrive with boundary exactly between them" {
    var peer = createTestPeer(2);
    var loop = createTestReceiverLoop(1);

    // Two 48-byte frames.
    var data: [96]u8 = undefined;
    buildTestFrame(data[0..48], 48, 2, 1);
    buildTestFrame(data[48..96], 48, 2, 1);

    // First read: exactly the first frame.
    const r1 = loop.processReceivedBytes(&peer, data[0..48]);
    try std.testing.expectEqual(@as(u32, 1), r1.frames);

    // Second read: exactly the second frame.
    const r2 = loop.processReceivedBytes(&peer, data[48..96]);
    try std.testing.expectEqual(@as(u32, 1), r2.frames);
}

test "frame boundary straddles two reads (header split)" {
    var peer = createTestPeer(2);
    var loop = createTestReceiverLoop(1);

    // 48-byte frame followed by 48-byte frame.
    var data: [96]u8 = undefined;
    buildTestFrame(data[0..48], 48, 2, 1);
    buildTestFrame(data[48..96], 48, 2, 1);

    // First read: first frame + 10 bytes of second frame's header.
    const r1 = loop.processReceivedBytes(&peer, data[0..58]);
    try std.testing.expectEqual(@as(u32, 1), r1.frames);

    // Second read: remaining 38 bytes of second frame.
    const r2 = loop.processReceivedBytes(&peer, data[58..96]);
    try std.testing.expectEqual(@as(u32, 1), r2.frames);
}
```

### 15.7 Performance Tests

```zig
test "throughput: small frames (64B)" {
    // Measure: frames/second for 64-byte frames across 4 peers.
    var mock_io = MockTcpIo.init();
    const frame_count = 100_000;
    const peers = [_]u8{ 2, 3, 4, 5 };

    for (peers) |peer_id| {
        for (0..frame_count / peers.len) |_| {
            mock_io.enqueueFrame(peer_id, buildFrame(peer_id, 1, 20, &[_]u8{0} ** 40));
        }
    }

    var ring20 = try createTestRingBuffer(16 * 1_048_576);
    defer ring20.deinit();
    var service_table = ServiceTable.init(std.testing.allocator);
    defer service_table.deinit();
    service_table.put(20, &ring20);

    var loop = try ReceiverEventLoop.initForTest(&mock_io, &service_table, 1);
    defer loop.deinit();
    for (peers) |peer_id| loop.addTestPeer(peer_id);

    const start_ns = Clock.monotonicNanos();
    var total_work: u64 = 0;
    while (total_work < frame_count) {
        total_work += loop.doWork();
    }
    const elapsed_ns = Clock.monotonicNanos() - start_ns;

    const frames_per_sec = @as(f64, @floatFromInt(total_work)) /
        (@as(f64, @floatFromInt(elapsed_ns)) / 1_000_000_000.0);

    // Log performance result (no hard assertion — depends on hardware).
    std.debug.print("\nThroughput: {d:.0} frames/sec ({d} frames in {d}ms)\n", .{
        frames_per_sec,
        total_work,
        @divFloor(elapsed_ns, std.time.ns_per_ms),
    });
}

test "throughput: large frames (64KB)" {
    var mock_io = MockTcpIo.init();
    const frame_count = 10_000;
    const payload = [_]u8{0} ** (65536 - 24);

    for (0..frame_count) |_| {
        mock_io.enqueueFrame(2, buildFrameWithPayload(2, 1, 20, &payload));
    }

    var ring20 = try createTestRingBuffer(128 * 1_048_576);
    defer ring20.deinit();
    var service_table = ServiceTable.init(std.testing.allocator);
    defer service_table.deinit();
    service_table.put(20, &ring20);

    var loop = try ReceiverEventLoop.initForTest(&mock_io, &service_table, 1);
    defer loop.deinit();
    loop.addTestPeer(2);

    const start_ns = Clock.monotonicNanos();
    var total_work: u64 = 0;
    while (total_work < frame_count) {
        total_work += loop.doWork();
    }
    const elapsed_ns = Clock.monotonicNanos() - start_ns;

    const bytes_per_sec = @as(f64, @floatFromInt(total_work * 65536)) /
        (@as(f64, @floatFromInt(elapsed_ns)) / 1_000_000_000.0);
    const gbps = bytes_per_sec / (1024.0 * 1024.0 * 1024.0);

    std.debug.print("\nThroughput: {d:.2} GB/s ({d} frames in {d}ms)\n", .{
        gbps,
        total_work,
        @divFloor(elapsed_ns, std.time.ns_per_ms),
    });
}
```

---

*Previous: [05 — Send Path](05-send-path.md)*
*Next: [08 — Service IPC](08-service-ipc.md)*
