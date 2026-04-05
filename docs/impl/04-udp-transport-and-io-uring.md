# 04 — UDP Transport & io_uring Integration

> **Depends on:** [01 — Platform Abstraction](01-platform-abstraction.md) (atomics, clocks, threads),
> [02 — Memory Layout & Shared Memory](02-memory-layout-and-shared-memory.md) (buffer sizing constants),
> [03 — Concurrent Data Structures](03-concurrent-data-structures.md) (ring buffer for send path)
>
> **Depended on by:** [05 — Send Path](05-send-path.md), [06 — Receive Path](06-receive-path.md),
> [07 — Flow Control](07-flow-control.md)

This document specifies the UDP wire protocol, the `io_uring` integration that replaces
`epoll` on Linux, the platform-specific I/O backends for macOS and Windows, and the
socket and buffer management layers that tie everything together.

This is a **key architectural document** because the move from `epoll` + per-call
`sendmsg`/`recvmsg` to `io_uring` with batched submissions, registered buffers, and
optional kernel-side polling fundamentally changes the I/O model. Every subsequent
document that touches the network path builds on the abstractions defined here.

All code targets **Zig 0.15.x** stable.

---

## Table of Contents

1. [Overview](#1-overview)
2. [UDP Wire Protocol](#2-udp-wire-protocol)
   - [Common Frame Header](#21-common-frame-header-8-bytes)
   - [Frame Types](#22-frame-types)
   - [Data Frame Header](#23-data-frame-header-40-bytes)
   - [Setup Frame](#24-setup-frame-24-bytes)
   - [Status Message](#25-status-message-28-bytes)
   - [NAK Frame](#26-nak-frame-24-bytes)
   - [Heartbeat](#27-heartbeat)
   - [Serialization Helpers](#28-serialization-helpers-flyweight-pattern)
3. [io_uring Integration (Linux)](#3-io_uring-integration-linux)
   - [Why io_uring Instead of epoll](#31-why-io_uring-instead-of-epoll)
   - [io_uring Overview](#32-io_uring-overview)
   - [IoUring Wrapper](#33-iouring-wrapper)
   - [Registered Buffers](#34-registered-buffers)
   - [Buffer Pool](#35-buffer-pool)
   - [Sender Integration](#36-sender-integration-with-io_uring)
   - [Receiver Integration](#37-receiver-integration-with-io_uring)
   - [Multishot Receive](#38-multishot-receive)
   - [SQPOLL Mode](#39-sqpoll-mode-ultra-low-latency)
   - [Error Handling](#310-error-handling)
4. [Platform-Specific I/O Backends](#4-platform-specific-io-backends)
   - [NetworkIo Union](#41-networkio-union)
   - [kqueue Backend (macOS)](#42-kqueue-backend-macos)
   - [IOCP Backend (Windows)](#43-iocp-backend-windows)
5. [Socket Management](#5-socket-management)
6. [Testing](#6-testing)
7. [File Structure](#7-file-structure)
8. [Implementation Steps](#8-implementation-steps)

---

## 1. Overview

The UDP transport layer serves a single purpose: move frames between broker processes
running on different hosts. Same-host services communicate via shared-memory ring
buffers and never touch this layer.

The transport has three responsibilities:

1. **Framing.** Define a compact, 4-byte-aligned, little-endian wire format that carries
   both transport metadata (sequence numbers, term offsets) and BRZ routing fields
   (node IDs, service IDs, template IDs) in a single header — avoiding double-parsing.

2. **I/O Submission.** Submit UDP sends and receives to the kernel efficiently. On Linux
   this means `io_uring`; on macOS, `kqueue` + `sendmsg`/`recvmsg`; on Windows, IOCP
   with overlapped I/O.

3. **Buffer Management.** Pre-allocate and register fixed-size buffers so that the hot
   path never allocates and, on Linux, the kernel never re-maps buffer pages.

The transport layer does **not** own the send or receive event loops — those are defined
in [05 — Send Path](05-send-path.md) and [06 — Receive Path](06-receive-path.md). This
document provides the building blocks they compose.

```
┌────────────────────────────────────────────────────────────────────┐
│                        Broker Process                              │
│                                                                    │
│   ┌─────────────┐     ┌─────────────┐     ┌─────────────────────┐ │
│   │ Sender      │     │ Receiver    │     │ Control Loop        │ │
│   │ Event Loop  │     │ Event Loop  │     │ (Step 09)           │ │
│   │ (Step 05)   │     │ (Step 06)   │     │                     │ │
│   └──────┬──────┘     └──────┬──────┘     └─────────────────────┘ │
│          │                   │                                     │
│   ┌──────▼───────────────────▼──────┐  ◄── THIS DOCUMENT          │
│   │        Transport Layer          │                              │
│   │                                 │                              │
│   │  ┌───────────┐ ┌─────────────┐  │                              │
│   │  │ NetworkIo │ │ BufferPool  │  │                              │
│   │  │ (io_uring │ │ (registered │  │                              │
│   │  │  /kqueue  │ │  buffers)   │  │                              │
│   │  │  /iocp)   │ │             │  │                              │
│   │  └─────┬─────┘ └─────────────┘  │                              │
│   │        │                        │                              │
│   │  ┌─────▼─────┐                  │                              │
│   │  │ UdpSocket │                  │                              │
│   │  └─────┬─────┘                  │                              │
│   └────────┼────────────────────────┘                              │
│            │                                                       │
└────────────┼───────────────────────────────────────────────────────┘
             │
             ▼
        UDP Datagrams
        (to/from peer brokers)
```

---

## 2. UDP Wire Protocol

All on-wire frames use **little-endian** byte order and are aligned to **4 bytes**.
Every frame begins with a common 8-byte header that identifies the frame type and total
length. Specific frame types extend this header with additional fields.

### 2.1 Common Frame Header (8 bytes)

```
Offset  Size  Type    Field
──────────────────────────────────
0       4     i32     frame_length   (total frame size including header)
4       1     u8      version        (always 0)
5       1     u8      flags
6       2     u16     frame_type
```

**File: `src/protocol/frames.zig`**

```zig
const std = @import("std");
const constants = @import("../platform/constants.zig");

/// Common header shared by all frame types. 8 bytes, 4-byte aligned.
pub const FrameHeader = packed struct {
    /// Total frame size in bytes, including this header and any payload.
    /// Must be >= 8 (header only) and <= MTU.
    frame_length: i32,

    /// Protocol version. Always 0 for current protocol.
    version: u8 = constants.frame_header_version,

    /// Flags. Interpretation depends on frame_type.
    flags: u8 = 0,

    /// Discriminates the frame type. See FrameType enum.
    frame_type: u16,

    comptime {
        std.debug.assert(@sizeOf(FrameHeader) == 8);
        std.debug.assert(@alignOf(FrameHeader) == 1); // packed → byte-aligned
    }
};
```

The `comptime` block ensures the struct is exactly 8 bytes. Because it's `packed`, Zig
lays out fields with no padding, and `@sizeOf` reflects the true wire size.

### 2.2 Frame Types

| Value  | Name        | Description                                     |
|--------|-------------|-------------------------------------------------|
| `0x00` | `PAD`       | Padding frame — receiver skips                  |
| `0x01` | `DATA`      | Data frame carrying a BRZ message               |
| `0x02` | `NAK`       | Negative acknowledgement (retransmit request)   |
| `0x03` | `SM`        | Status message (flow control + receiver window) |
| `0x04` | `SETUP`     | Connection establishment                        |
| `0x05` | `HEARTBEAT` | Keepalive (zero-length data frame)              |

```zig
pub const FrameType = enum(u16) {
    pad = 0x00,
    data = 0x01,
    nak = 0x02,
    sm = 0x03,
    setup = 0x04,
    heartbeat = 0x05,
    _,  // allow unknown values for forward compatibility

    pub fn fromU16(value: u16) FrameType {
        return @enumFromInt(value);
    }
};
```

The `_` catch-all allows the receiver to parse future frame types without crashing.
Unknown types are silently dropped.

### 2.3 Data Frame Header (40 bytes)

The most important frame type. It combines transport fields (sequence number, term
offset) with BRZ routing fields (node IDs, service IDs, template ID, correlation ID)
into a single header. This eliminates the need for a separate BRZ message header inside
the payload, avoiding double-parsing on the receive path.

```
Offset  Size  Type    Field
──────────────────────────────────────────────────
0       4     i32     frame_length
4       1     u8      version (0)
5       1     u8      flags
6       2     u16     frame_type (DATA = 0x01)
── transport fields ──────────────────────────────
8       4     i32     term_offset           position within send buffer
12      1     u8      source_node_id
13      1     u8      target_node_id
14      2     u16     source_service_id
16      2     u16     target_service_id
── routing fields ────────────────────────────────
18      2     u16     template_id           message type (0 = raw user message)
20      4     i32     correlation_id        request-response matching
24      1     u8      msg_flags             BRZ-specific flags (chunked, etc.)
25      7     u8[7]   reserved              zero-filled, future use
── ordering ──────────────────────────────────────
32      8     i64     sequence_number       monotonic per source→target link
──────────────────────────────────────────────────
40      ...   bytes   payload
```

**Total header: 40 bytes**, aligned to 8 bytes.

```zig
/// Data frame header — 40 bytes. Carries a BRZ message between brokers.
///
/// Layout is designed so the receiver can route a message with a single
/// read of this header: source/target node and service IDs are at fixed
/// offsets, and the payload immediately follows at byte 40.
pub const DataFrameHeader = packed struct {
    // --- common header (8 bytes) ---
    frame_length: i32,
    version: u8 = constants.frame_header_version,
    flags: u8 = 0,
    frame_type: u16 = @intFromEnum(FrameType.data),

    // --- transport fields ---
    term_offset: i32 = 0,
    source_node_id: u8 = 0,
    target_node_id: u8 = 0,
    source_service_id: u16 = 0,
    target_service_id: u16 = 0,

    // --- routing fields ---
    template_id: u16 = 0,
    correlation_id: i32 = 0,
    msg_flags: u8 = 0,
    reserved: [7]u8 = [_]u8{0} ** 7,

    // --- ordering ---
    sequence_number: i64 = 0,

    comptime {
        std.debug.assert(@sizeOf(DataFrameHeader) == constants.data_frame_header_length);
        std.debug.assert(@sizeOf(DataFrameHeader) == 40);
    }

    /// Returns the payload portion of a frame buffer, assuming the buffer
    /// starts at the beginning of this header.
    pub fn payloadSlice(buf: []const u8) []const u8 {
        const header_len = @sizeOf(DataFrameHeader);
        if (buf.len <= header_len) return buf[0..0];
        return buf[header_len..];
    }

    /// Returns the frame length from raw bytes without constructing the full header.
    pub fn peekFrameLength(buf: []const u8) ?i32 {
        if (buf.len < 4) return null;
        return std.mem.readInt(i32, buf[0..4], .little);
    }

    /// Returns true if this is an unfragmented (complete) message.
    pub fn isUnfragmented(self: DataFrameHeader) bool {
        return (self.flags & constants.flag_unfragmented) == constants.flag_unfragmented;
    }

    /// Returns true if this is the first fragment (BEGIN set).
    pub fn isBegin(self: DataFrameHeader) bool {
        return (self.flags & constants.flag_begin) != 0;
    }

    /// Returns true if this is the last fragment (END set).
    pub fn isEnd(self: DataFrameHeader) bool {
        return (self.flags & constants.flag_end) != 0;
    }

    /// Returns true if this is an admin/cluster message.
    pub fn isAdmin(self: DataFrameHeader) bool {
        return (self.flags & constants.flag_admin) != 0;
    }
};
```

**Flags field (byte 5):**

| Flag           | Bit    | Meaning                                        |
|----------------|--------|------------------------------------------------|
| `BEGIN`        | `0x80` | First fragment of a fragmented message         |
| `END`          | `0x40` | Last fragment of a fragmented message          |
| `UNFRAGMENTED` | `0xC0` | `BEGIN \| END` — complete message in one frame |
| `ADMIN`        | `0x20` | Admin/cluster message (not routed to services) |

**Key design choice:** Integrating `nodeId`/`serviceId` into the transport header
(instead of keeping them in a separate BRZ header inside the payload) means the
receiver can route a message with a single 40-byte header read. No secondary parsing
step, no offset arithmetic into the payload.

### 2.4 Setup Frame (24 bytes)

Sent by a broker when it first connects to a peer, or when re-establishing after a
timeout. The peer uses `log_buffer_length` and `mtu_length` to configure its receiver
state for this link.

```
Offset  Size  Type    Field
──────────────────────────────────
0       8     ...     frame_header (frame_type = SETUP)
8       1     u8      source_node_id
9       1     u8      reserved
10      2     u16     reserved
12      4     i32     log_buffer_length     receiver's log buffer size
16      4     i32     mtu_length            maximum transmission unit
20      4     i32     initial_sequence      starting sequence number
```

```zig
/// Setup frame — 24 bytes. Sent to establish a connection with a peer.
pub const SetupFrame = packed struct {
    // --- common header ---
    frame_length: i32 = @sizeOf(SetupFrame),
    version: u8 = constants.frame_header_version,
    flags: u8 = 0,
    frame_type: u16 = @intFromEnum(FrameType.setup),

    // --- setup fields ---
    source_node_id: u8 = 0,
    _reserved0: u8 = 0,
    _reserved1: u16 = 0,
    log_buffer_length: i32 = 0,
    mtu_length: i32 = 0,
    initial_sequence: i32 = 0,

    comptime {
        std.debug.assert(@sizeOf(SetupFrame) == 24);
    }
};
```

### 2.5 Status Message (28 bytes)

Sent by receivers back to senders for flow control. Carries the receiver's current
consumption position and how far ahead the sender is permitted to write (the receiver
window).

```
Offset  Size  Type    Field
──────────────────────────────────
0       8     ...     frame_header (frame_type = SM)
8       1     u8      node_id                 receiver's node ID
9       1     u8      reserved
10      2     u16     reserved
12      8     i64     consumption_position    how far the receiver has consumed
20      4     i32     receiver_window         how far ahead the sender may write
24      4     i32     reserved
```

```zig
/// Status Message — 28 bytes. Receiver → Sender flow control.
pub const StatusMessage = packed struct {
    // --- common header ---
    frame_length: i32 = @sizeOf(StatusMessage),
    version: u8 = constants.frame_header_version,
    flags: u8 = 0,
    frame_type: u16 = @intFromEnum(FrameType.sm),

    // --- status fields ---
    node_id: u8 = 0,
    _reserved0: u8 = 0,
    _reserved1: u16 = 0,
    consumption_position: i64 = 0,
    receiver_window: i32 = 0,
    _reserved2: i32 = 0,

    comptime {
        std.debug.assert(@sizeOf(StatusMessage) == 28);
    }
};
```

### 2.6 NAK Frame (24 bytes)

Sent by receivers requesting retransmission of a contiguous range of missing data.

```
Offset  Size  Type    Field
──────────────────────────────────
0       8     ...     frame_header (frame_type = NAK)
8       1     u8      node_id       sender of the NAK (receiver's perspective)
9       1     u8      reserved
10      2     u16     reserved
12      8     i64     position      start of missing data
20      4     i32     length        length of missing data in bytes
```

```zig
/// NAK frame — 24 bytes. Receiver → Sender retransmit request.
pub const NakFrame = packed struct {
    // --- common header ---
    frame_length: i32 = @sizeOf(NakFrame),
    version: u8 = constants.frame_header_version,
    flags: u8 = 0,
    frame_type: u16 = @intFromEnum(FrameType.nak),

    // --- nak fields ---
    node_id: u8 = 0,
    _reserved0: u8 = 0,
    _reserved1: u16 = 0,
    position: i64 = 0,
    length: i32 = 0,

    comptime {
        std.debug.assert(@sizeOf(NakFrame) == 24);
    }
};
```

### 2.7 Heartbeat

A heartbeat is a **zero-length data frame** — a `DataFrameHeader` with no payload:

- `frame_length = 40` (header only)
- `flags = UNFRAGMENTED` (0xC0)
- `frame_type = DATA` (0x01)
- `sequence_number = current_sequence` (no increment — heartbeats don't consume sequence space)

The receiver uses heartbeats to:
1. Confirm the sender is alive
2. Track the sender's current sequence position (for gap detection)

```zig
/// Create a heartbeat frame for the given peer link.
pub fn makeHeartbeat(
    source_node_id: u8,
    target_node_id: u8,
    current_sequence: i64,
) DataFrameHeader {
    return .{
        .frame_length = @sizeOf(DataFrameHeader),
        .flags = constants.flag_unfragmented,
        .frame_type = @intFromEnum(FrameType.data),
        .source_node_id = source_node_id,
        .target_node_id = target_node_id,
        .sequence_number = current_sequence,
    };
}
```

### 2.8 Serialization Helpers (Flyweight Pattern)

All frame structs are `packed`, which means they can be overlaid directly onto byte
buffers with zero-copy pointer casting. This is the Zig equivalent of the SBE flyweight
pattern used in the Java BRZ.

**File: `src/protocol/frame_parser.zig`**

```zig
const std = @import("std");
const frames = @import("frames.zig");
const constants = @import("../platform/constants.zig");

/// Overlay a DataFrameHeader onto a byte slice (read-only, zero-copy).
/// Returns null if the buffer is too small or frame_length is invalid.
pub fn readDataFrame(buf: []const u8) ?*const frames.DataFrameHeader {
    if (buf.len < @sizeOf(frames.DataFrameHeader)) return null;

    const header: *const frames.DataFrameHeader = @ptrCast(@alignCast(buf.ptr));

    // Validate frame_length is at least the header size
    if (header.frame_length < @as(i32, @intCast(@sizeOf(frames.DataFrameHeader)))) return null;

    return header;
}

/// Overlay a DataFrameHeader onto a mutable byte slice (write, zero-copy).
/// The caller writes fields directly through the returned pointer.
pub fn writeDataFrame(buf: []u8) ?*frames.DataFrameHeader {
    if (buf.len < @sizeOf(frames.DataFrameHeader)) return null;

    const header: *frames.DataFrameHeader = @ptrCast(@alignCast(buf.ptr));
    header.* = .{
        .frame_length = 0, // caller must set
    };

    return header;
}

/// Parse just the common frame header to determine the frame type.
/// This is the first step in the receive path dispatch.
pub fn readFrameHeader(buf: []const u8) ?*const frames.FrameHeader {
    if (buf.len < @sizeOf(frames.FrameHeader)) return null;
    return @ptrCast(@alignCast(buf.ptr));
}

/// Overlay a SetupFrame onto a byte slice.
pub fn readSetupFrame(buf: []const u8) ?*const frames.SetupFrame {
    if (buf.len < @sizeOf(frames.SetupFrame)) return null;
    return @ptrCast(@alignCast(buf.ptr));
}

/// Overlay a StatusMessage onto a byte slice.
pub fn readStatusMessage(buf: []const u8) ?*const frames.StatusMessage {
    if (buf.len < @sizeOf(frames.StatusMessage)) return null;
    return @ptrCast(@alignCast(buf.ptr));
}

/// Overlay a NakFrame onto a byte slice.
pub fn readNakFrame(buf: []const u8) ?*const frames.NakFrame {
    if (buf.len < @sizeOf(frames.NakFrame)) return null;
    return @ptrCast(@alignCast(buf.ptr));
}

/// Dispatch on frame_type and return the specific frame type.
pub const ParsedFrame = union(enum) {
    data: *const frames.DataFrameHeader,
    setup: *const frames.SetupFrame,
    sm: *const frames.StatusMessage,
    nak: *const frames.NakFrame,
    pad: *const frames.FrameHeader,
    unknown: u16,
};

pub fn parseFrame(buf: []const u8) ?ParsedFrame {
    const header = readFrameHeader(buf) orelse return null;
    const frame_type = frames.FrameType.fromU16(header.frame_type);

    return switch (frame_type) {
        .data, .heartbeat => if (readDataFrame(buf)) |f| .{ .data = f } else null,
        .setup => if (readSetupFrame(buf)) |f| .{ .setup = f } else null,
        .sm => if (readStatusMessage(buf)) |f| .{ .sm = f } else null,
        .nak => if (readNakFrame(buf)) |f| .{ .nak = f } else null,
        .pad => .{ .pad = header },
        _ => .{ .unknown = header.frame_type },
    };
}
```

**Usage example — receiver dispatch:**

```zig
fn onPacketReceived(buf: []const u8) void {
    const parsed = frame_parser.parseFrame(buf) orelse return;

    switch (parsed) {
        .data => |header| {
            if (header.isAdmin()) {
                handleAdminMessage(header, frames.DataFrameHeader.payloadSlice(buf));
            } else {
                routeToService(header, buf);
            }
        },
        .setup => |setup| handleSetup(setup),
        .sm => |sm| handleStatusMessage(sm),
        .nak => |nak| handleNak(nak),
        .pad => {},
        .unknown => {},
    }
}
```

**Byte-level serialization for sends:**

When constructing a frame for sending, write the struct directly into a buffer slot:

```zig
fn encodeDataFrame(
    buf: []u8,
    payload: []const u8,
    source_node_id: u8,
    target_node_id: u8,
    source_service_id: u16,
    target_service_id: u16,
    template_id: u16,
    correlation_id: i32,
    msg_flags: u8,
    sequence_number: i64,
) ?usize {
    const header_len = @sizeOf(frames.DataFrameHeader);
    const total_len = header_len + payload.len;
    if (buf.len < total_len) return null;

    const header: *frames.DataFrameHeader = @ptrCast(@alignCast(buf.ptr));
    header.* = .{
        .frame_length = @intCast(total_len),
        .flags = if (payload.len > 0) constants.flag_unfragmented else constants.flag_unfragmented,
        .source_node_id = source_node_id,
        .target_node_id = target_node_id,
        .source_service_id = source_service_id,
        .target_service_id = target_service_id,
        .template_id = template_id,
        .correlation_id = correlation_id,
        .msg_flags = msg_flags,
        .sequence_number = sequence_number,
    };

    // Copy payload immediately after header
    @memcpy(buf[header_len..][0..payload.len], payload);

    return total_len;
}
```

---

## 3. io_uring Integration (Linux)

### 3.1 Why io_uring Instead of epoll

The original architecture specifies `epoll` + `sendmsg`/`recvmmsg`. We replace this
with `io_uring` because the broker's workload — many small UDP sends and receives per
duty cycle — is precisely the pattern where `io_uring` provides the greatest advantage.

**Head-to-head comparison:**

| Dimension | `epoll` + `sendmsg`/`recvmmsg` | `io_uring` |
|---|---|---|
| **Syscalls per batch of N sends** | N × `sendmsg` + 1 × `epoll_wait` = N+1 | 1 × `io_uring_enter` (or 0 with SQPOLL) |
| **Syscalls per batch of M recvs** | 1 × `recvmmsg` + 1 × `epoll_wait` = 2 | 1 × `io_uring_enter` (or 0 with SQPOLL + multishot) |
| **Context switches per I/O op** | 1 per syscall | 0 — kernel drains SQ in batch |
| **Buffer mapping** | Kernel maps user buffers on each call | `IORING_REGISTER_BUFFERS` pins once at init |
| **Receive re-arming** | Must call `recvmmsg` again after each batch | Multishot: one SQE → continuous CQEs |
| **Timer management** | Separate `timerfd` + epoll registration | `IORING_OP_TIMEOUT` — unified in same ring |
| **Kernel-side polling** | Not available | `SQPOLL` mode — zero submission syscalls |

**Concrete impact for the broker:**

1. **Fewer syscalls.** A broker sending 10 frames to different peers can submit all 10
   as SQEs and call `io_uring_enter()` once. With `sendmsg`, that's 10 separate syscalls.

2. **No user↔kernel context switch per I/O.** The kernel drains the submission queue in
   batch. The CPU never transitions between user and kernel mode per-packet.

3. **Registered buffers eliminate per-call page table walks.** With
   `IORING_REGISTER_BUFFERS`, the kernel pins our send and receive buffers at init time.
   Each subsequent I/O references the buffer by index — no `get_user_pages()` overhead.

4. **Natural batching aligns with duty-cycle loops.** The SQ/CQ ring structure is a
   perfect match: the sender queues all SQEs during one duty cycle, then submits them
   all at once. The receiver drains all CQEs in one loop iteration.

5. **SQPOLL mode for ultra-low latency.** In high-throughput scenarios, a dedicated
   kernel thread polls the SQ continuously. The broker never calls `io_uring_enter()`
   at all — it writes SQEs and the kernel picks them up.

6. **Multishot receive reduces submission overhead to near-zero on the receive path.**
   One SQE generates a CQE for every received packet, without re-submission.

### 3.2 io_uring Overview

io_uring uses two ring buffers shared between user-space and the kernel:

```
User-space                              Kernel
┌──────────────────┐                   ┌──────────────────┐
│                  │                   │                  │
│  Write SQEs ─────┼──────────────────►│  Process SQEs    │
│  (submissions)   │  Submission Queue │  (execute I/O)   │
│                  │  (shared memory)  │                  │
│                  │                   │                  │
│  Read CQEs  ◄────┼──────────────────┤│  Write CQEs      │
│  (completions)   │  Completion Queue │  (I/O results)   │
│                  │  (shared memory)  │                  │
└──────────────────┘                   └──────────────────┘
```

- **Submission Queue (SQ):** User-space writes Submission Queue Entries (SQEs)
  describing I/O operations. Each SQE specifies an opcode (`SENDMSG`, `RECVMSG`,
  `TIMEOUT`, `NOP`), a file descriptor, buffer pointers, and user-data for correlation.

- **Completion Queue (CQ):** Kernel writes Completion Queue Entries (CQEs) containing
  the result of each completed operation. The CQE carries `user_data` (matching the
  SQE), `res` (result or errno), and `flags`.

**Key operations for the broker:**

| Operation             | SQE opcode            | Use                                             |
|-----------------------|-----------------------|-------------------------------------------------|
| `IORING_OP_SENDMSG`  | Send UDP datagram     | Sender event loop transmitting to peers         |
| `IORING_OP_RECVMSG`  | Receive UDP datagram  | Receiver event loop receiving from peers        |
| `IORING_OP_TIMEOUT`  | Timer                 | Heartbeat and status message timing             |
| `IORING_OP_NOP`      | No-op                 | Wake up from idle / drain SQPOLL                |

**CQ size:** We configure the CQ to be at least 2× the SQ depth. This ensures
completions are never dropped even if user-space is temporarily slow to drain them.

### 3.3 IoUring Wrapper

**File: `src/transport/io_uring.zig`**

The `IoUring` struct wraps the Zig standard library's `std.os.linux.IoUring` and adds:
- Ergonomic methods for UDP send/recv with `msghdr` setup
- Registered buffer integration
- Pending-submission tracking for batched submit
- Completion polling with a callback interface

```zig
const std = @import("std");
const linux = std.os.linux;
const constants = @import("../platform/constants.zig");

pub const IoUring = struct {
    ring: linux.IoUring,
    pending_submissions: u32 = 0,
    registered_buffers: bool = false,

    const Self = @This();

    // ── Lifecycle ──────────────────────────────────────────────

    /// Initialize the io_uring instance.
    ///
    /// `queue_depth` — number of SQE slots (must be power of two).
    /// `flags`       — io_uring setup flags (e.g. IORING_SETUP_SQPOLL).
    pub fn init(queue_depth: u32, flags: u32) !IoUring {
        var params = std.mem.zeroes(linux.io_uring_params);
        params.flags = flags;

        // CQ size = 2× SQ depth to avoid completion drops
        params.flags |= linux.IORING_SETUP_CQSIZE;
        params.cq_entries = queue_depth * 2;

        const ring = try linux.IoUring.init(queue_depth, params);
        return .{
            .ring = ring,
        };
    }

    /// Clean up all io_uring resources.
    pub fn deinit(self: *Self) void {
        self.ring.deinit();
    }

    // ── Submission: UDP Send ───────────────────────────────────

    /// Queue a UDP sendmsg operation. Does NOT submit to kernel yet —
    /// call `submit()` or `submitAndWait()` after batching all SQEs.
    ///
    /// `user_data` is an opaque tag returned in the CQE for correlation.
    pub fn prepareSend(
        self: *Self,
        socket_fd: i32,
        buf: []const u8,
        dest_addr: *const std.posix.sockaddr,
        dest_addr_len: std.posix.socklen_t,
        msghdr_buf: *linux.msghdr,
        iov: *linux.iovec,
        user_data: u64,
    ) !void {
        // Set up iovec
        iov.* = .{
            .base = @constCast(buf.ptr),
            .len = buf.len,
        };

        // Set up msghdr
        msghdr_buf.* = std.mem.zeroes(linux.msghdr);
        msghdr_buf.name = @constCast(dest_addr);
        msghdr_buf.namelen = dest_addr_len;
        msghdr_buf.iov = @ptrCast(iov),
        msghdr_buf.iovlen = 1;

        var sqe = try self.ring.get_sqe();
        sqe.prep_sendmsg(socket_fd, msghdr_buf, 0);
        sqe.user_data = user_data;

        self.pending_submissions += 1;
    }

    // ── Submission: UDP Receive ────────────────────────────────

    /// Queue a UDP recvmsg operation.
    pub fn prepareRecv(
        self: *Self,
        socket_fd: i32,
        buf: []u8,
        src_addr: *std.posix.sockaddr,
        src_addr_len: *std.posix.socklen_t,
        msghdr_buf: *linux.msghdr,
        iov: *linux.iovec,
        user_data: u64,
    ) !void {
        iov.* = .{
            .base = buf.ptr,
            .len = buf.len,
        };

        msghdr_buf.* = std.mem.zeroes(linux.msghdr);
        msghdr_buf.name = src_addr;
        msghdr_buf.namelen = src_addr_len.*;
        msghdr_buf.iov = @ptrCast(iov);
        msghdr_buf.iovlen = 1;

        var sqe = try self.ring.get_sqe();
        sqe.prep_recvmsg(socket_fd, msghdr_buf, 0);
        sqe.user_data = user_data;

        self.pending_submissions += 1;
    }

    // ── Submission: Timeout ────────────────────────────────────

    /// Queue a timeout. The CQE fires after `ns` nanoseconds.
    pub fn prepareTimeout(self: *Self, ns: u64, user_data: u64) !void {
        var sqe = try self.ring.get_sqe();

        var ts: linux.kernel_timespec = .{
            .sec = @intCast(ns / std.time.ns_per_s),
            .nsec = @intCast(ns % std.time.ns_per_s),
        };

        sqe.prep_timeout(&ts, 0, 0);
        sqe.user_data = user_data;

        self.pending_submissions += 1;
    }

    // ── Submission: NOP ────────────────────────────────────────

    /// Queue a no-op. Useful for waking up a blocked ring or
    /// flushing SQPOLL.
    pub fn prepareNop(self: *Self, user_data: u64) !void {
        var sqe = try self.ring.get_sqe();
        sqe.prep_nop();
        sqe.user_data = user_data;

        self.pending_submissions += 1;
    }

    // ── Submit ─────────────────────────────────────────────────

    /// Submit all pending SQEs to the kernel. Returns the number submitted.
    /// This is a single `io_uring_enter()` call regardless of how many SQEs
    /// were queued.
    pub fn submit(self: *Self) !u32 {
        if (self.pending_submissions == 0) return 0;

        const submitted = try self.ring.submit();
        self.pending_submissions = 0;
        return submitted;
    }

    /// Submit all pending SQEs and wait for at least `min_complete`
    /// completions. Combines submit + wait into a single syscall.
    pub fn submitAndWait(self: *Self, min_complete: u32) !u32 {
        const submitted = try self.ring.submit_and_wait(min_complete);
        self.pending_submissions = 0;
        return submitted;
    }

    // ── Completion Polling ─────────────────────────────────────

    /// The result of a single completed I/O operation.
    pub const Completion = struct {
        user_data: u64,
        result: i32,       // >=0 on success (bytes transferred), <0 on error (-errno)
        flags: u32,
    };

    /// Poll for completed I/O operations. Calls `handler` for each CQE,
    /// up to `limit` completions. Returns number of completions processed.
    ///
    /// This does NOT block. If no completions are ready, returns 0.
    pub fn pollCompletions(
        self: *Self,
        comptime handler: fn (completion: Completion) void,
        limit: u32,
    ) u32 {
        var count: u32 = 0;

        while (count < limit) {
            const cqe = self.ring.peek_cqe() catch break;
            if (cqe == null) break;

            const c = cqe.?;
            handler(.{
                .user_data = c.user_data,
                .result = c.res,
                .flags = c.flags,
            });

            self.ring.cqe_seen(c);
            count += 1;
        }

        return count;
    }

    /// Poll completions using a runtime function pointer (for cases where
    /// comptime dispatch is not possible).
    pub fn pollCompletionsDynamic(
        self: *Self,
        context: *anyopaque,
        handler: *const fn (context: *anyopaque, completion: Completion) void,
        limit: u32,
    ) u32 {
        var count: u32 = 0;

        while (count < limit) {
            const cqe = self.ring.peek_cqe() catch break;
            if (cqe == null) break;

            const c = cqe.?;
            handler(context, .{
                .user_data = c.user_data,
                .result = c.res,
                .flags = c.flags,
            });

            self.ring.cqe_seen(c);
            count += 1;
        }

        return count;
    }

    // ── Registered Buffers ─────────────────────────────────────

    /// Register a set of fixed buffers with the kernel. Must be called once
    /// at init time. After registration, SQEs can reference buffers by index
    /// instead of pointer, avoiding per-I/O page table walks.
    pub fn registerBuffers(self: *Self, iovecs: []const linux.iovec) !void {
        try self.ring.register_buffers(iovecs);
        self.registered_buffers = true;
    }

    /// Unregister previously registered buffers.
    pub fn unregisterBuffers(self: *Self) !void {
        try self.ring.unregister_buffers();
        self.registered_buffers = false;
    }
};
```

### 3.4 Registered Buffers

Pre-registering send and receive buffers with the kernel avoids per-I/O buffer mapping
overhead. The kernel pins the physical pages at registration time, so subsequent I/O
operations reference them by index — no `get_user_pages()` call, no page table walk.

**Registration flow:**

```
Startup:
  1. Allocate N send buffers × MTU bytes each (cache-line aligned)
  2. Allocate M recv buffers × MTU bytes each (cache-line aligned)
  3. Build iovec array pointing to each buffer
  4. Call io_uring.registerBuffers(iovecs)

Hot path (send):
  1. Acquire buffer from pool → get index
  2. Encode frame into buffer[index]
  3. Submit SQE with IOSQE_FIXED_FILE flag and buffer_index

Hot path (recv):
  1. Submit recv SQE with buffer group selection (IOSQE_BUFFER_SELECT)
  2. On CQE: buffer_index is in cqe.flags >> IORING_CQE_BUFFER_SHIFT
  3. Process received packet
  4. Return buffer to pool

Shutdown:
  5. Call io_uring.unregisterBuffers()
  6. Free all buffer memory
```

**Why this matters:** Without registered buffers, the kernel must call `get_user_pages()`
on every `sendmsg`/`recvmsg` to translate user-space virtual addresses to physical
pages. For the broker, which may execute tens of thousands of I/O operations per second,
this overhead accumulates significantly. Registering buffers once at startup eliminates
it entirely.

### 3.5 Buffer Pool

**File: `src/transport/buffer_pool.zig`**

The buffer pool manages a fixed set of pre-allocated, cache-line-aligned buffers. It is
used by both the sender (to stage outbound frames) and the receiver (to provide buffers
for incoming packets).

Design constraints:
- **Single-threaded access.** Each pool belongs to one event loop thread. No
  synchronization needed.
- **Fixed capacity.** All buffers allocated at init, never grown. Exhaustion is handled
  by the caller (back-pressure or drop).
- **Cache-line aligned.** Each buffer starts at a 64-byte boundary to avoid false sharing
  and to satisfy io_uring's alignment preferences for registered buffers.

```zig
const std = @import("std");
const constants = @import("../platform/constants.zig");
const Allocator = std.mem.Allocator;

pub const BufferSlot = struct {
    /// Index of this buffer in the pool. Used as the registered buffer index
    /// for io_uring operations.
    index: u16,

    /// The buffer memory itself.
    buffer: []align(constants.cache_line_length) u8,
};

pub const BufferPool = struct {
    /// All buffers, indexed by slot index.
    buffers: [][]align(constants.cache_line_length) u8,

    /// Stack of free buffer indices. Pop to acquire, push to release.
    /// Using a simple stack (array + top pointer) for O(1) acquire/release
    /// with no branching.
    free_indices: []u16,
    free_top: u16,

    /// Total number of buffers in the pool.
    count: u16,

    /// Size of each individual buffer in bytes.
    buf_size: usize,

    allocator: Allocator,

    const Self = @This();

    /// Create a buffer pool with `count` buffers, each `buf_size` bytes.
    /// Each buffer is cache-line aligned (64 bytes).
    pub fn init(allocator: Allocator, count: u16, buf_size: usize) !BufferPool {
        const buffers = try allocator.alloc(
            []align(constants.cache_line_length) u8,
            count,
        );
        errdefer allocator.free(buffers);

        const free_indices = try allocator.alloc(u16, count);
        errdefer allocator.free(free_indices);

        // Allocate each individual buffer
        for (0..count) |i| {
            buffers[i] = try allocator.alignedAlloc(
                u8,
                constants.cache_line_length,
                buf_size,
            );
            // Initialize free stack (all buffers start free)
            free_indices[i] = @intCast(i);
        }

        return .{
            .buffers = buffers,
            .free_indices = free_indices,
            .free_top = count,
            .count = count,
            .buf_size = buf_size,
            .allocator = allocator,
        };
    }

    /// Acquire a free buffer. Returns null if the pool is exhausted.
    /// O(1) — pops from the free stack.
    pub fn acquire(self: *Self) ?BufferSlot {
        if (self.free_top == 0) return null;

        self.free_top -= 1;
        const index = self.free_indices[self.free_top];

        return .{
            .index = index,
            .buffer = self.buffers[index],
        };
    }

    /// Release a buffer back to the pool. O(1) — pushes onto the free stack.
    /// The caller must not use the buffer after releasing it.
    pub fn release(self: *Self, slot: BufferSlot) void {
        std.debug.assert(self.free_top < self.count);
        self.free_indices[self.free_top] = slot.index;
        self.free_top += 1;
    }

    /// Return the number of currently available (free) buffers.
    pub fn available(self: *const Self) u16 {
        return self.free_top;
    }

    /// Return the total capacity of the pool.
    pub fn capacity(self: *const Self) u16 {
        return self.count;
    }

    /// Build an iovec array suitable for io_uring buffer registration.
    /// The caller must free the returned slice.
    pub fn toIovecs(self: *const Self, allocator: Allocator) ![]std.os.linux.iovec {
        const iovecs = try allocator.alloc(std.os.linux.iovec, self.count);
        for (0..self.count) |i| {
            iovecs[i] = .{
                .base = self.buffers[i].ptr,
                .len = self.buf_size,
            };
        }
        return iovecs;
    }

    /// Free all buffers and the pool metadata.
    pub fn deinit(self: *Self) void {
        for (0..self.count) |i| {
            self.allocator.free(self.buffers[i]);
        }
        self.allocator.free(self.buffers);
        self.allocator.free(self.free_indices);
    }
};
```

**Usage in sender event loop init:**

```zig
// At startup:
const send_pool = try BufferPool.init(allocator, 64, mtu_length);
const recv_pool = try BufferPool.init(allocator, 64, mtu_length);

// Register with io_uring for zero-copy sends
const send_iovecs = try send_pool.toIovecs(allocator);
defer allocator.free(send_iovecs);
try io_ring.registerBuffers(send_iovecs);
```

### 3.6 Sender Integration with io_uring

The sender event loop duty cycle changes fundamentally with io_uring. Instead of calling
`sendmsg()` inline for each outbound message, the sender queues SQEs and submits them
in one batch at the end of each duty cycle.

```
┌─────────────────────────────────────────────────┐
│  Sender Duty Cycle (one iteration)              │
│                                                 │
│  1. Poll io_uring completions                   │
│     → release send buffers back to pool         │
│     → handle send errors (log, retry)           │
│                                                 │
│  2. Drain send ring buffer                      │
│     → for each outbound message:                │
│        a. acquire buffer from pool              │
│        b. encode frame into buffer              │
│        c. queue io_uring SQE (SENDMSG)          │
│                                                 │
│  3. Queue heartbeat SQEs (if timer expired)     │
│                                                 │
│  4. Submit all queued SQEs → ONE syscall         │
│                                                 │
│  5. Process retransmit timeouts                 │
│                                                 │
│  Return: total work count                       │
└─────────────────────────────────────────────────┘
```

**Pseudocode:**

```zig
/// Tag values embedded in SQE user_data to identify completions.
const CompletionTag = enum(u64) {
    /// Bits 0-15: buffer pool index. Bits 16-31: peer node id.
    send_complete = 0x0001_0000_0000_0000,
    heartbeat_send = 0x0002_0000_0000_0000,
    timeout = 0x0003_0000_0000_0000,
    _,

    /// Encode a send completion tag with the buffer index for reclamation.
    fn encodeSend(buffer_index: u16) u64 {
        return @intFromEnum(CompletionTag.send_complete) | @as(u64, buffer_index);
    }

    /// Extract the buffer index from a send completion tag.
    fn decodeSendBufferIndex(user_data: u64) u16 {
        return @truncate(user_data & 0xFFFF);
    }
};

fn senderDoWork(sender: *SenderEventLoop) u32 {
    var work_count: u32 = 0;
    const now_ns = platform.Clock.monotonicNanos();

    // 1. Process io_uring completions (completed sends)
    //    Each completion returns a buffer to the pool.
    work_count += sender.io_ring.pollCompletions(sender.onSendComplete, constants.send_batch_limit);

    // 2. Drain send ring buffer → queue io_uring SQEs
    work_count += sender.send_ring_buffer.read(struct {
        fn onMessage(ctx: *SenderEventLoop, buf: []const u8) void {
            const header = frame_parser.readDataFrame(buf) orelse return;
            const target_node = header.target_node_id;
            const peer = ctx.peers.get(target_node) orelse return;

            // Acquire a send buffer from the pool
            const slot = ctx.send_pool.acquire() orelse {
                ctx.counters.increment(.send_buffer_exhausted);
                return; // back-pressure: drop this message
            };

            // Copy frame into the registered send buffer
            @memcpy(slot.buffer[0..buf.len], buf);

            // Queue the io_uring SQE
            ctx.io_ring.prepareSend(
                peer.socket_fd,
                slot.buffer[0..buf.len],
                &peer.address,
                peer.address_len,
                &ctx.msghdr_pool[slot.index],
                &ctx.iov_pool[slot.index],
                CompletionTag.encodeSend(slot.index),
            ) catch {
                ctx.send_pool.release(slot);
                return;
            };
        }
    }.onMessage, sender, constants.send_batch_limit);

    // 3. Queue heartbeat sends (if interval elapsed)
    if (now_ns >= sender.next_heartbeat_ns) {
        sender.queueHeartbeats();
        sender.next_heartbeat_ns = now_ns + constants.udp_heartbeat_interval_ns;
    }

    // 4. Submit all queued SQEs in one batch
    if (sender.io_ring.pending_submissions > 0) {
        const submitted = sender.io_ring.submit() catch 0;
        work_count += submitted;
    }

    // 5. Process retransmit timeouts
    work_count += sender.retransmit_handler.processTimeouts(now_ns);

    return work_count;
}

fn onSendComplete(completion: IoUring.Completion) void {
    const buffer_index = CompletionTag.decodeSendBufferIndex(completion.user_data);

    if (completion.result < 0) {
        // Send failed — log the error, buffer is still returned to pool
        sender.counters.increment(.send_errors);
    }

    // Return the buffer to the pool regardless of success/failure
    sender.send_pool.release(.{
        .index = buffer_index,
        .buffer = sender.send_pool.buffers[buffer_index],
    });
}
```

**Key difference from `sendmsg` approach:** With direct `sendmsg`, the send is
synchronous — the function returns and the buffer can be reused immediately. With
io_uring, the send is asynchronous — the buffer must remain valid until the CQE arrives.
This is why we need the `BufferPool`: each in-flight send holds one buffer slot, and the
slot is released only when the CQE confirms completion.

### 3.7 Receiver Integration with io_uring

The receiver event loop mirrors the sender pattern. It pre-submits receive SQEs, then
polls completions to process incoming packets.

```
┌─────────────────────────────────────────────────┐
│  Receiver Duty Cycle (one iteration)            │
│                                                 │
│  1. Poll io_uring completions (received packets)│
│     → parse frame type                          │
│     → DATA: insert into recv log, route to svc  │
│     → SETUP: handle connection establishment    │
│     → SM: update sender flow control state      │
│     → NAK: trigger retransmit                   │
│     → re-submit recv SQE for consumed buffer    │
│                                                 │
│  2. Scan for losses → queue NAK SQEs            │
│                                                 │
│  3. Queue Status Message SQEs (rate-limited)    │
│                                                 │
│  4. Submit all queued SQEs → ONE syscall         │
│                                                 │
│  Return: total work count                       │
└─────────────────────────────────────────────────┘
```

**Pseudocode:**

```zig
fn receiverDoWork(receiver: *ReceiverEventLoop) u32 {
    var work_count: u32 = 0;
    const now_ns = platform.Clock.monotonicNanos();

    // 1. Process io_uring completions (received packets)
    work_count += receiver.io_ring.pollCompletions(
        receiver.onPacketReceived,
        constants.recv_batch_limit,
    );

    // 2. Scan for losses and queue NAK SQEs
    for (receiver.peers.values()) |peer| {
        const naks_sent = peer.loss_detector.scan(peer.recv_log, now_ns);
        if (naks_sent > 0) {
            receiver.queueNakFrames(peer, naks_sent);
        }
        work_count += naks_sent;
    }

    // 3. Queue Status Message SQEs (rate-limited)
    if (now_ns >= receiver.next_sm_ns) {
        receiver.queueStatusMessages();
        receiver.next_sm_ns = now_ns + constants.sm_timeout_ns;
    }

    // 4. Submit all queued SQEs in one batch
    if (receiver.io_ring.pending_submissions > 0) {
        const submitted = receiver.io_ring.submit() catch 0;
        work_count += submitted;
    }

    return work_count;
}

fn onPacketReceived(completion: IoUring.Completion) void {
    const buffer_index = CompletionTag.decodeRecvBufferIndex(completion.user_data);
    const slot = receiver.recv_pool.getByIndex(buffer_index);

    if (completion.result <= 0) {
        // Receive error or zero-length — re-submit recv SQE
        receiver.resubmitRecv(slot);
        return;
    }

    const bytes_received: usize = @intCast(completion.result);
    const buf = slot.buffer[0..bytes_received];

    // Parse and dispatch
    const parsed = frame_parser.parseFrame(buf);
    if (parsed) |frame| {
        switch (frame) {
            .data => |header| {
                const peer = receiver.peers.get(header.source_node_id);
                if (peer) |p| {
                    p.recv_log.insertPacket(buf);
                    if (header.isAdmin()) {
                        receiver.handleAdminMessage(header, frames.DataFrameHeader.payloadSlice(buf));
                    } else {
                        receiver.routeToService(header, buf);
                    }
                }
            },
            .setup => |setup| receiver.handleSetup(setup),
            .sm => |sm| receiver.handleStatusMessage(sm),
            .nak => |nak| receiver.handleNak(nak),
            .pad => {},
            .unknown => {},
        }
    }

    // Re-submit recv SQE for this buffer
    receiver.resubmitRecv(slot);
}
```

**Critical detail — recv SQE resubmission:** After processing each received packet, we
must re-submit a recv SQE for the same buffer. This is analogous to calling `recvmsg()`
again in the `epoll` model. The resubmission is queued and batched with other SQEs in
step 4.

### 3.8 Multishot Receive

io_uring supports **multishot receive** (`IORING_RECV_MULTISHOT`), where a single SQE
generates multiple CQEs — one for each received packet. This eliminates the need to
re-submit recv SQEs after each packet.

**How it works:**

```
Traditional (one-shot):                 Multishot:

  SQE ──► CQE (1 packet)                SQE ──► CQE (packet 1)
  SQE ──► CQE (1 packet)                     ──► CQE (packet 2)
  SQE ──► CQE (1 packet)                     ──► CQE (packet 3)
  SQE ──► CQE (1 packet)                     ──► CQE (packet 4)
  ...                                         ──► ...
                                              (continues until cancelled)
  N SQEs for N packets                   1 SQE for ∞ packets
```

**Integration with buffer rings (provided buffers):**

Multishot receive works with `io_uring` provided buffers (`IOSQE_BUFFER_SELECT`). The
kernel automatically selects a buffer from a registered buffer group for each incoming
packet:

1. At init: set up a buffer group via `IORING_REGISTER_PBUF_RING`
2. Submit one multishot recvmsg SQE per socket, with `IOSQE_BUFFER_SELECT` flag
3. Each CQE carries the buffer group ID and buffer index in `cqe.flags`
4. Process the packet, then return the buffer to the group
5. The multishot SQE remains active until explicitly cancelled

```zig
/// Set up a provided buffer ring for multishot receives.
pub fn setupProvidedBufferRing(
    self: *Self,
    group_id: u16,
    buffers: [][]align(constants.cache_line_length) u8,
) !void {
    // Register the buffer ring with the kernel
    var reg: linux.io_uring_buf_reg = std.mem.zeroes(linux.io_uring_buf_reg);
    reg.ring_addr = 0; // kernel allocates
    reg.ring_entries = @intCast(buffers.len);
    reg.bgid = group_id;

    try self.ring.register_buf_ring(&reg);

    // Add each buffer to the ring
    for (buffers, 0..) |buf, i| {
        self.ring.buf_ring_add(
            reg.ring_addr,
            buf.ptr,
            @intCast(buf.len),
            @intCast(i),
            @intCast(buffers.len - 1),
            @intCast(i),
        );
    }

    // Make all buffers visible to the kernel
    self.ring.buf_ring_advance(reg.ring_addr, @intCast(buffers.len));
}

/// Submit a multishot recvmsg SQE. This single SQE will generate one CQE
/// per received packet until cancelled.
pub fn prepareMultishotRecv(
    self: *Self,
    socket_fd: i32,
    msghdr_buf: *linux.msghdr,
    group_id: u16,
    user_data: u64,
) !void {
    var sqe = try self.ring.get_sqe();

    sqe.prep_recvmsg(socket_fd, msghdr_buf, 0);
    sqe.user_data = user_data;
    sqe.flags |= linux.IOSQE_BUFFER_SELECT;
    sqe.buf_group = group_id;
    // Set multishot flag
    sqe.ioprio |= linux.IORING_RECV_MULTISHOT;

    self.pending_submissions += 1;
}
```

**Extracting buffer index from CQE:**

```zig
/// Extract the provided buffer index from a multishot CQE.
fn extractBufferIndex(cqe_flags: u32) u16 {
    // Buffer index is stored in the upper 16 bits of cqe.flags
    return @truncate(cqe_flags >> linux.IORING_CQE_BUFFER_SHIFT);
}

/// Check if a multishot CQE indicates more data is coming.
fn isMultishotMore(cqe_flags: u32) bool {
    return (cqe_flags & linux.IORING_CQE_F_MORE) != 0;
}
```

**When to use multishot vs. one-shot:**

| Mode | Pros | Cons | Use when |
|---|---|---|---|
| One-shot | Simpler buffer management, explicit control | Re-submit overhead per packet | Few peers, low packet rate |
| Multishot | Near-zero submission overhead on recv path | Requires provided buffers, more complex error handling | Many peers, high packet rate |

**Recommendation:** Start with one-shot receives for simplicity. Switch to multishot
when profiling shows recv SQE resubmission is a bottleneck. The `NetworkIo` abstraction
supports both modes transparently.

### 3.9 SQPOLL Mode (Ultra-Low Latency)

For the lowest possible latency, io_uring supports **Submission Queue Polling**
(`IORING_SETUP_SQPOLL`). In this mode, a dedicated kernel thread continuously polls the
SQ for new entries. User-space never calls `io_uring_enter()` for submissions — it
writes SQEs into the shared ring and the kernel picks them up.

```
Without SQPOLL:                          With SQPOLL:

  User writes SQEs                        User writes SQEs
       │                                       │
       ▼                                       ▼
  io_uring_enter() ← SYSCALL              (no syscall)
       │                                       │
       ▼                                       ▼
  Kernel processes SQEs                   Kernel thread sees SQEs
       │                                       │
       ▼                                       ▼
  Kernel writes CQEs                      Kernel writes CQEs
       │                                       │
       ▼                                       ▼
  User reads CQEs                         User reads CQEs
```

**Configuration:**

```zig
pub const IoUringConfig = struct {
    /// Number of SQE slots (must be power of two).
    queue_depth: u32 = 256,

    /// Enable SQPOLL mode. Requires CAP_SYS_NICE or appropriate
    /// rlimit_memlock settings.
    sqpoll: bool = false,

    /// SQPOLL kernel thread idle timeout in milliseconds. If no SQEs
    /// are submitted for this duration, the kernel thread goes to sleep.
    /// A subsequent submission wakes it up (via io_uring_enter with
    /// IORING_ENTER_SQ_WAKEUP). Set to 0 for infinite polling.
    sqpoll_idle_ms: u32 = 1000,

    /// CPU to pin the SQPOLL kernel thread to (optional).
    sqpoll_cpu: ?u32 = null,
};

pub fn initWithConfig(config: IoUringConfig) !IoUring {
    var flags: u32 = 0;

    if (config.sqpoll) {
        flags |= linux.IORING_SETUP_SQPOLL;
    }

    // CQ size = 2× SQ depth
    flags |= linux.IORING_SETUP_CQSIZE;

    var params = std.mem.zeroes(linux.io_uring_params);
    params.flags = flags;
    params.cq_entries = config.queue_depth * 2;

    if (config.sqpoll) {
        params.sq_thread_idle = config.sqpoll_idle_ms;
        if (config.sqpoll_cpu) |cpu| {
            params.flags |= linux.IORING_SETUP_SQ_AFF;
            params.sq_thread_cpu = cpu;
        }
    }

    const ring = try linux.IoUring.init(config.queue_depth, params);
    return .{ .ring = ring };
}
```

**SQPOLL mode `submit` behavior:**

When SQPOLL is enabled, `submit()` still flushes the SQ tail but may not call
`io_uring_enter()` at all if the kernel thread is actively polling. The Zig standard
library's `IoUring.submit()` handles this correctly — it checks the `SQ_NEED_WAKEUP`
flag and only calls `io_uring_enter(IORING_ENTER_SQ_WAKEUP)` if the kernel thread is
sleeping.

**Requirements and caveats:**

| Requirement | Detail |
|---|---|
| Privileges | `CAP_SYS_NICE` or `CAP_SYS_ADMIN`, or adjusted `rlimit_memlock` |
| CPU cost | Dedicates one CPU core to the kernel polling thread |
| Memory lock | Locked pages count against `RLIMIT_MEMLOCK` |
| Idle fallback | When SQ is idle for `sq_thread_idle` ms, kernel thread sleeps; next submit wakes it |

**Recommendation:** SQPOLL is off by default. Enable it via the broker's configuration
file (`brz.io_uring.sqpoll = true`) for deployments where single-digit-microsecond
latency matters and a dedicated CPU core is acceptable.

### 3.10 Error Handling

io_uring completions report errors as negative `res` values (negated `errno`). The
transport layer must handle these gracefully without crashing or leaking buffers.

**Common send errors:**

| `cqe.res` | errno | Meaning | Action |
|---|---|---|---|
| `-11` | `EAGAIN` | Socket buffer full | Retry on next duty cycle |
| `-101` | `ENETUNREACH` | Network unreachable | Log, mark peer disconnected |
| `-111` | `ECONNREFUSED` | Peer not listening | Log, increment counter |
| `-90` | `EMSGSIZE` | Packet too large | Bug — MTU calculation wrong |

**Common recv errors:**

| `cqe.res` | errno | Meaning | Action |
|---|---|---|---|
| `-11` | `EAGAIN` | No data available | Normal — re-submit recv |
| `-4` | `EINTR` | Interrupted | Re-submit recv |
| `0` | (zero) | Zero-length datagram | Drop, re-submit recv |

**Invariant:** Every buffer acquired from the pool MUST be released back to the pool,
regardless of whether the I/O succeeded or failed. A buffer leak would eventually
exhaust the pool and halt I/O entirely.

```zig
fn onSendComplete(completion: IoUring.Completion) void {
    const buffer_index = CompletionTag.decodeSendBufferIndex(completion.user_data);

    if (completion.result < 0) {
        const err = std.posix.errno(@intCast(-completion.result));
        switch (err) {
            .AGAIN => sender.counters.increment(.send_would_block),
            .NETUNREACH, .CONNREFUSED => {
                sender.counters.increment(.send_peer_unreachable);
                // Optionally mark peer as disconnected
            },
            .MSGSIZE => {
                // Bug: our MTU calculation is wrong
                std.log.err("EMSGSIZE on send — MTU misconfiguration", .{});
            },
            else => sender.counters.increment(.send_errors),
        }
    } else {
        sender.counters.increment(.bytes_sent, @intCast(completion.result));
    }

    // ALWAYS return the buffer, even on error
    sender.send_pool.release(.{
        .index = buffer_index,
        .buffer = sender.send_pool.buffers[buffer_index],
    });
}
```

---

## 4. Platform-Specific I/O Backends

### 4.1 NetworkIo Union

**File: `src/transport/network_io.zig`**

The sender and receiver event loops never reference io_uring, kqueue, or IOCP directly.
They use `NetworkIo`, a tagged union that dispatches to the correct backend at comptime
based on the target OS.

```zig
const std = @import("std");
const builtin = @import("builtin");
const io_uring_backend = @import("io_uring.zig");
const kqueue_backend = @import("kqueue.zig");
const constants = @import("../platform/constants.zig");

/// Platform-specific I/O backend. Selected at comptime based on target OS.
///
/// All methods have the same signatures regardless of backend, so the sender
/// and receiver event loops are platform-independent.
pub const NetworkIo = if (builtin.os.tag == .linux)
    IoUringNetworkIo
else if (builtin.os.tag == .macos)
    KqueueNetworkIo
else
    @compileError("Unsupported OS for NetworkIo: " ++ @tagName(builtin.os.tag));

/// Linux backend: io_uring.
pub const IoUringNetworkIo = struct {
    ring: io_uring_backend.IoUring,
    send_pool: *BufferPool,
    recv_pool: *BufferPool,

    const Self = @This();

    pub fn init(config: io_uring_backend.IoUringConfig, send_pool: *BufferPool, recv_pool: *BufferPool) !Self {
        var ring = try io_uring_backend.IoUring.initWithConfig(config);

        // Register send buffers with the kernel
        const send_iovecs = try send_pool.toIovecs(send_pool.allocator);
        defer send_pool.allocator.free(send_iovecs);
        try ring.registerBuffers(send_iovecs);

        return .{
            .ring = ring,
            .send_pool = send_pool,
            .recv_pool = recv_pool,
        };
    }

    pub fn deinit(self: *Self) void {
        if (self.ring.registered_buffers) {
            self.ring.unregisterBuffers() catch {};
        }
        self.ring.deinit();
    }

    pub fn prepareSend(
        self: *Self,
        socket_fd: i32,
        buf: []const u8,
        dest_addr: *const std.posix.sockaddr,
        dest_addr_len: std.posix.socklen_t,
        msghdr_buf: *std.os.linux.msghdr,
        iov: *std.os.linux.iovec,
        user_data: u64,
    ) !void {
        try self.ring.prepareSend(
            socket_fd, buf, dest_addr, dest_addr_len,
            msghdr_buf, iov, user_data,
        );
    }

    pub fn prepareRecv(
        self: *Self,
        socket_fd: i32,
        buf: []u8,
        src_addr: *std.posix.sockaddr,
        src_addr_len: *std.posix.socklen_t,
        msghdr_buf: *std.os.linux.msghdr,
        iov: *std.os.linux.iovec,
        user_data: u64,
    ) !void {
        try self.ring.prepareRecv(
            socket_fd, buf, src_addr, src_addr_len,
            msghdr_buf, iov, user_data,
        );
    }

    pub fn submit(self: *Self) !u32 {
        return self.ring.submit();
    }

    pub fn pollCompletions(
        self: *Self,
        comptime handler: fn (completion: io_uring_backend.IoUring.Completion) void,
        limit: u32,
    ) u32 {
        return self.ring.pollCompletions(handler, limit);
    }

    pub fn pendingSubmissions(self: *const Self) u32 {
        return self.ring.pending_submissions;
    }
};

/// macOS backend: kqueue + sendmsg/recvmsg.
/// (See Section 4.2 for implementation.)
pub const KqueueNetworkIo = struct {
    // ... (defined in kqueue.zig)
};
```

**Why comptime dispatch instead of runtime vtable?**

The platform I/O backend never changes at runtime — it's determined by the compilation
target. Using `if (builtin.os.tag == .linux)` at the type level means:
- Zero runtime dispatch overhead (no vtable, no branch)
- The compiler eliminates dead code for other platforms
- Type errors are caught at compile time for the target platform
- The generated binary contains only the code for the target OS

### 4.2 kqueue Backend (macOS)

**File: `src/transport/kqueue.zig`**

macOS does not have io_uring. The kqueue backend uses traditional `kqueue` +
`sendmsg`/`recvmsg`, but wraps them in the same `NetworkIo` interface so the event
loops remain unchanged.

```zig
const std = @import("std");
const posix = std.posix;
const constants = @import("../platform/constants.zig");

pub const KqueueNetworkIo = struct {
    kq_fd: i32,
    events: [64]posix.Kevent = undefined,

    const Self = @This();

    pub fn init() !Self {
        const kq_fd = try posix.kqueue();
        return .{ .kq_fd = kq_fd };
    }

    pub fn deinit(self: *Self) void {
        posix.close(self.kq_fd);
    }

    /// Register a socket for read events.
    pub fn registerRead(self: *Self, socket_fd: i32) !void {
        var changelist = [_]posix.Kevent{.{
            .ident = @intCast(socket_fd),
            .filter = posix.system.EVFILT_READ,
            .flags = posix.system.EV_ADD | posix.system.EV_ENABLE,
            .fflags = 0,
            .data = 0,
            .udata = 0,
        }};

        _ = try posix.kevent(self.kq_fd, &changelist, &.{}, null);
    }

    /// Poll for ready events and process them. Returns work count.
    ///
    /// Unlike io_uring, kqueue only tells us "the socket is readable" —
    /// we must then call recvmsg() ourselves. Similarly for sends, we call
    /// sendmsg() directly (no queuing).
    pub fn poll(self: *Self, timeout_ns: ?u64) ![]posix.Kevent {
        var ts: ?posix.timespec = null;
        var ts_val: posix.timespec = undefined;

        if (timeout_ns) |ns| {
            ts_val = .{
                .sec = @intCast(ns / std.time.ns_per_s),
                .nsec = @intCast(ns % std.time.ns_per_s),
            };
            ts = ts_val;
        }

        const n = try posix.kevent(
            self.kq_fd,
            &.{},
            &self.events,
            if (ts) |*t| t else null,
        );

        return self.events[0..n];
    }

    /// Synchronous UDP send — kqueue does not batch sends like io_uring.
    /// Wraps sendmsg() directly.
    pub fn sendTo(
        socket_fd: i32,
        buf: []const u8,
        dest_addr: *const posix.sockaddr,
        dest_addr_len: posix.socklen_t,
    ) !usize {
        var iov = [_]posix.iovec_const{.{
            .base = buf.ptr,
            .len = buf.len,
        }};

        var msg: posix.msghdr_const = .{
            .name = dest_addr,
            .namelen = dest_addr_len,
            .iov = &iov,
            .iovlen = 1,
            .control = null,
            .controllen = 0,
            .flags = 0,
        };

        return try posix.sendmsg(socket_fd, &msg, 0);
    }

    /// Synchronous UDP receive.
    pub fn recvFrom(
        socket_fd: i32,
        buf: []u8,
        src_addr: *posix.sockaddr,
        src_addr_len: *posix.socklen_t,
    ) !usize {
        var iov = [_]posix.iovec{.{
            .base = buf.ptr,
            .len = buf.len,
        }};

        var msg: posix.msghdr = .{
            .name = src_addr,
            .namelen = src_addr_len.*,
            .iov = &iov,
            .iovlen = 1,
            .control = null,
            .controllen = 0,
            .flags = 0,
        };

        const n = try posix.recvmsg(socket_fd, &msg, 0);
        src_addr_len.* = msg.namelen;
        return n;
    }
};
```

**Key differences from the io_uring path:**

| Aspect | io_uring (Linux) | kqueue (macOS) |
|---|---|---|
| Send model | Async: queue SQE, submit batch, process CQE | Sync: `sendmsg()` inline |
| Recv model | Async: queue SQE, process CQE with packet | Sync: `recvmsg()` when kqueue signals readability |
| Batching | N sends → 1 syscall | N sends → N syscalls |
| Buffer management | Registered buffers (kernel-pinned) | Regular user buffers |
| Timer management | `IORING_OP_TIMEOUT` in same ring | Separate `EVFILT_TIMER` in kqueue |

The kqueue backend is inherently less efficient for the broker's workload, but macOS
is primarily a development platform — production deployments target Linux.

### 4.3 IOCP Backend (Windows)

**File: `src/transport/iocp.zig`** (stub — to be implemented)

Windows uses I/O Completion Ports (IOCP) with overlapped I/O for asynchronous socket
operations. The model is structurally similar to io_uring:

1. Associate socket with completion port via `CreateIoCompletionPort`
2. Submit async I/O via `WSASendTo` / `WSARecvFrom` with `OVERLAPPED` structures
3. Poll completions via `GetQueuedCompletionStatusEx` (batched)

```zig
pub const IocpNetworkIo = struct {
    completion_port: std.os.windows.HANDLE,

    const Self = @This();

    pub fn init() !Self {
        const cp = std.os.windows.kernel32.CreateIoCompletionPort(
            std.os.windows.INVALID_HANDLE_VALUE,
            null,
            0,
            0, // concurrency = number of processors
        ) orelse return error.CreateIoCompletionPortFailed;

        return .{ .completion_port = cp };
    }

    pub fn deinit(self: *Self) void {
        std.os.windows.CloseHandle(self.completion_port);
    }

    // TODO: WSASendTo, WSARecvFrom with OVERLAPPED
    // TODO: GetQueuedCompletionStatusEx for batched completion polling
};
```

Windows support is lower priority. The interface is stubbed out to ensure the
`NetworkIo` abstraction is correct, but full implementation is deferred.

---

## 5. Socket Management

**File: `src/transport/udp_socket.zig`**

The `UdpSocket` struct encapsulates platform-specific socket creation, configuration,
and lifecycle. All socket operations are non-blocking.

```zig
const std = @import("std");
const posix = std.posix;
const builtin = @import("builtin");
const constants = @import("../platform/constants.zig");

pub const UdpSocket = struct {
    fd: posix.socket_t,

    const Self = @This();

    /// Create and bind a UDP socket to the given address.
    pub fn bind(address: std.net.Address) !UdpSocket {
        const fd = try posix.socket(
            address.any.family,
            posix.SOCK.DGRAM | posix.SOCK.NONBLOCK | posix.SOCK.CLOEXEC,
            0,
        );
        errdefer posix.close(fd);

        // Allow address reuse (for fast restart)
        try posix.setsockopt(fd, posix.SOL.SOCKET, posix.SO.REUSEADDR, &std.mem.toBytes(@as(c_int, 1)));

        try posix.bind(fd, &address.any, address.getOsSockLen());

        return .{ .fd = fd };
    }

    /// Create an unbound UDP socket (for sending only).
    pub fn create(family: u32) !UdpSocket {
        const fd = try posix.socket(
            family,
            posix.SOCK.DGRAM | posix.SOCK.NONBLOCK | posix.SOCK.CLOEXEC,
            0,
        );

        return .{ .fd = fd };
    }

    /// Set the OS-level send buffer size.
    pub fn setSendBufferSize(self: Self, size: u32) !void {
        try posix.setsockopt(
            self.fd,
            posix.SOL.SOCKET,
            posix.SO.SNDBUF,
            &std.mem.toBytes(@as(c_int, @intCast(size))),
        );
    }

    /// Set the OS-level receive buffer size.
    pub fn setRecvBufferSize(self: Self, size: u32) !void {
        try posix.setsockopt(
            self.fd,
            posix.SOL.SOCKET,
            posix.SO.RCVBUF,
            &std.mem.toBytes(@as(c_int, @intCast(size))),
        );
    }

    /// Get the file descriptor (for io_uring / kqueue / IOCP registration).
    pub fn getFd(self: Self) posix.socket_t {
        return self.fd;
    }

    /// Close the socket.
    pub fn close(self: Self) void {
        posix.close(self.fd);
    }
};
```

**Socket topology design decision:**

The broker uses **one receive socket** bound to its own address and **one send socket**
shared for all outbound traffic. Rationale:

- **One receive socket** — All peers send to the broker's single address. The source
  address in each received packet identifies the sender.
- **One send socket** — The broker sends to different peer addresses using the same
  unbound socket. The OS assigns an ephemeral source port. Since the receiver identifies
  packets by `source_node_id` in the frame header (not by source IP/port), a single
  send socket is sufficient.

```zig
pub const BrokerSockets = struct {
    /// Receive socket — bound to broker's configured address.
    recv_socket: UdpSocket,

    /// Send socket — unbound, used for all outbound traffic.
    send_socket: UdpSocket,

    pub fn init(bind_address: std.net.Address, send_buf_size: u32, recv_buf_size: u32) !BrokerSockets {
        var recv_socket = try UdpSocket.bind(bind_address);
        errdefer recv_socket.close();

        try recv_socket.setRecvBufferSize(recv_buf_size);

        var send_socket = try UdpSocket.create(bind_address.any.family);
        errdefer send_socket.close();

        try send_socket.setSendBufferSize(send_buf_size);

        return .{
            .recv_socket = recv_socket,
            .send_socket = send_socket,
        };
    }

    pub fn deinit(self: *BrokerSockets) void {
        self.recv_socket.close();
        self.send_socket.close();
    }
};
```

---

## 6. Testing

### 6.1 Frame Serialization Tests

Verify that each frame type has the correct byte-level layout, that `packed struct`
sizes match expectations, and that the flyweight parser correctly overlays onto byte
buffers.

```zig
const std = @import("std");
const testing = std.testing;
const frames = @import("../protocol/frames.zig");
const frame_parser = @import("../protocol/frame_parser.zig");

test "FrameHeader is exactly 8 bytes" {
    try testing.expectEqual(@as(usize, 8), @sizeOf(frames.FrameHeader));
}

test "DataFrameHeader is exactly 40 bytes" {
    try testing.expectEqual(@as(usize, 40), @sizeOf(frames.DataFrameHeader));
}

test "SetupFrame is exactly 24 bytes" {
    try testing.expectEqual(@as(usize, 24), @sizeOf(frames.SetupFrame));
}

test "StatusMessage is exactly 28 bytes" {
    try testing.expectEqual(@as(usize, 28), @sizeOf(frames.StatusMessage));
}

test "NakFrame is exactly 24 bytes" {
    try testing.expectEqual(@as(usize, 24), @sizeOf(frames.NakFrame));
}

test "DataFrameHeader roundtrip through byte buffer" {
    // Given: a buffer large enough for a data frame with payload
    var buf: [128]u8 align(8) = [_]u8{0} ** 128;
    const payload = "hello, world";
    const total_len: i32 = @intCast(@sizeOf(frames.DataFrameHeader) + payload.len);

    // When: we write a data frame header + payload
    const header: *frames.DataFrameHeader = @ptrCast(@alignCast(&buf));
    header.* = .{
        .frame_length = total_len,
        .flags = 0xC0, // UNFRAGMENTED
        .source_node_id = 1,
        .target_node_id = 2,
        .source_service_id = 100,
        .target_service_id = 200,
        .template_id = 42,
        .correlation_id = 12345,
        .msg_flags = 0,
        .sequence_number = 99,
    };
    @memcpy(buf[40..][0..payload.len], payload);

    // Then: reading back via flyweight parser yields the same values
    const read_header = frame_parser.readDataFrame(&buf).?;
    try testing.expectEqual(@as(i32, total_len), read_header.frame_length);
    try testing.expectEqual(@as(u8, 1), read_header.source_node_id);
    try testing.expectEqual(@as(u8, 2), read_header.target_node_id);
    try testing.expectEqual(@as(u16, 100), read_header.source_service_id);
    try testing.expectEqual(@as(u16, 200), read_header.target_service_id);
    try testing.expectEqual(@as(u16, 42), read_header.template_id);
    try testing.expectEqual(@as(i32, 12345), read_header.correlation_id);
    try testing.expectEqual(@as(i64, 99), read_header.sequence_number);
    try testing.expect(read_header.isUnfragmented());
    try testing.expect(!read_header.isAdmin());

    // Verify payload is accessible
    const read_payload = frames.DataFrameHeader.payloadSlice(buf[0..@intCast(total_len)]);
    try testing.expectEqualStrings(payload, read_payload);
}

test "parseFrame dispatches DATA correctly" {
    // Given
    var buf: [64]u8 align(8) = [_]u8{0} ** 64;
    const header: *frames.DataFrameHeader = @ptrCast(@alignCast(&buf));
    header.* = .{ .frame_length = 40 };

    // When
    const parsed = frame_parser.parseFrame(&buf);

    // Then
    try testing.expect(parsed != null);
    try testing.expect(parsed.? == .data);
}

test "parseFrame dispatches SETUP correctly" {
    // Given
    var buf: [32]u8 align(8) = [_]u8{0} ** 32;
    const header: *frames.SetupFrame = @ptrCast(@alignCast(&buf));
    header.* = .{
        .frame_length = 24,
        .frame_type = @intFromEnum(frames.FrameType.setup),
        .source_node_id = 5,
        .log_buffer_length = 4 * 1024 * 1024,
        .mtu_length = 1408,
        .initial_sequence = 0,
    };

    // When
    const parsed = frame_parser.parseFrame(&buf);

    // Then
    try testing.expect(parsed != null);
    try testing.expect(parsed.? == .setup);
    try testing.expectEqual(@as(u8, 5), parsed.?.setup.source_node_id);
}

test "makeHeartbeat produces valid data frame" {
    // Given / When
    const hb = frames.makeHeartbeat(1, 2, 42);

    // Then
    try testing.expectEqual(@as(i32, 40), hb.frame_length);
    try testing.expectEqual(@as(u8, 0xC0), hb.flags); // UNFRAGMENTED
    try testing.expectEqual(@as(u8, 1), hb.source_node_id);
    try testing.expectEqual(@as(u8, 2), hb.target_node_id);
    try testing.expectEqual(@as(i64, 42), hb.sequence_number);
}

test "readDataFrame rejects undersized buffer" {
    // Given: buffer too small for a data frame header
    var buf: [20]u8 = [_]u8{0} ** 20;

    // When / Then
    try testing.expect(frame_parser.readDataFrame(&buf) == null);
}

test "DataFrameHeader flags helpers" {
    // Given
    var header: frames.DataFrameHeader = .{ .frame_length = 40 };

    // When: unfragmented
    header.flags = 0xC0;
    try testing.expect(header.isUnfragmented());
    try testing.expect(header.isBegin());
    try testing.expect(header.isEnd());
    try testing.expect(!header.isAdmin());

    // When: begin-only fragment
    header.flags = 0x80;
    try testing.expect(!header.isUnfragmented());
    try testing.expect(header.isBegin());
    try testing.expect(!header.isEnd());

    // When: admin message
    header.flags = 0xE0; // UNFRAGMENTED | ADMIN
    try testing.expect(header.isAdmin());
    try testing.expect(header.isUnfragmented());
}
```

### 6.2 Buffer Pool Tests

```zig
test "BufferPool acquire and release" {
    // Given
    var pool = try BufferPool.init(testing.allocator, 4, 1408);
    defer pool.deinit();

    // When: acquire all buffers
    var slots: [4]BufferSlot = undefined;
    for (0..4) |i| {
        slots[i] = pool.acquire().?;
    }

    // Then: pool is exhausted
    try testing.expect(pool.acquire() == null);
    try testing.expectEqual(@as(u16, 0), pool.available());

    // When: release one buffer
    pool.release(slots[0]);

    // Then: one buffer available
    try testing.expectEqual(@as(u16, 1), pool.available());
    try testing.expect(pool.acquire() != null);
}

test "BufferPool buffers are cache-line aligned" {
    // Given
    var pool = try BufferPool.init(testing.allocator, 8, 1408);
    defer pool.deinit();

    // Then: every buffer is 64-byte aligned
    for (pool.buffers) |buf| {
        try testing.expectEqual(@as(usize, 0), @intFromPtr(buf.ptr) % 64);
    }
}
```

### 6.3 io_uring Smoke Tests (Linux Only)

```zig
const builtin = @import("builtin");

test "io_uring NOP submit and complete" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;

    // Given
    var ring = try IoUring.init(32, 0);
    defer ring.deinit();

    // When: submit a NOP
    try ring.prepareNop(0xDEADBEEF);
    const submitted = try ring.submit();

    // Then: one SQE submitted
    try testing.expectEqual(@as(u32, 1), submitted);

    // When: poll for completion
    var completed: u32 = 0;
    var received_user_data: u64 = 0;

    completed = ring.pollCompletions(struct {
        fn handler(c: IoUring.Completion) void {
            received_user_data = c.user_data;
        }
    }.handler, 10);

    // Then: got one CQE with our user_data
    try testing.expectEqual(@as(u32, 1), completed);
    try testing.expectEqual(@as(u64, 0xDEADBEEF), received_user_data);
}

test "io_uring UDP send and receive" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;

    // Given: two UDP sockets (sender + receiver)
    const recv_addr = try std.net.Address.parseIp4("127.0.0.1", 0);
    var recv_socket = try UdpSocket.bind(recv_addr);
    defer recv_socket.close();

    // Get the actual bound port
    var bound_addr: std.net.Address = undefined;
    var addr_len: posix.socklen_t = @sizeOf(std.net.Address);
    try posix.getsockname(recv_socket.fd, &bound_addr.any, &addr_len);

    var send_socket = try UdpSocket.create(posix.AF.INET);
    defer send_socket.close();

    var ring = try IoUring.init(32, 0);
    defer ring.deinit();

    // When: send a frame via io_uring
    const payload = "test-frame-data";
    var send_msghdr: std.os.linux.msghdr = undefined;
    var send_iov: std.os.linux.iovec = undefined;

    try ring.prepareSend(
        send_socket.fd,
        payload,
        &bound_addr.any,
        addr_len,
        &send_msghdr,
        &send_iov,
        0x1111,
    );
    _ = try ring.submit();

    // Wait for send completion
    std.time.sleep(10 * std.time.ns_per_ms);

    // Then: receive the packet via io_uring
    var recv_buf: [256]u8 = undefined;
    var src_addr: std.posix.sockaddr = undefined;
    var src_addr_len: posix.socklen_t = @sizeOf(std.posix.sockaddr);
    var recv_msghdr: std.os.linux.msghdr = undefined;
    var recv_iov: std.os.linux.iovec = undefined;

    try ring.prepareRecv(
        recv_socket.fd,
        &recv_buf,
        &src_addr,
        &src_addr_len,
        &recv_msghdr,
        &recv_iov,
        0x2222,
    );
    _ = try ring.submitAndWait(1);

    var received_bytes: usize = 0;
    _ = ring.pollCompletions(struct {
        fn handler(c: IoUring.Completion) void {
            received_bytes = @intCast(c.result);
        }
    }.handler, 10);

    // Verify
    try testing.expectEqual(payload.len, received_bytes);
    try testing.expectEqualStrings(payload, recv_buf[0..received_bytes]);
}
```

### 6.4 Benchmark Outline

```zig
// Benchmark: io_uring batched sends vs. sendmsg loop
//
// Setup:
//   - Two loopback UDP sockets
//   - 10,000 packets of MTU size (1408 bytes)
//
// Test A (io_uring):
//   - Queue all 10,000 as SQEs (in batches of 64)
//   - submit() after each batch of 64
//   - Measure total time
//
// Test B (sendmsg):
//   - Call sendmsg() for each of 10,000 packets
//   - Measure total time
//
// Expected result: io_uring is 2-5× faster for batched sends due to
// amortized syscall overhead.
//
// Run: zig build benchmark -- --test-filter "sendmsg vs io_uring"
```

---

## 7. File Structure

```
src/
  protocol/
    frames.zig              — packed structs for all frame types + helpers
    frame_parser.zig        — flyweight parsers, dispatch union, encode helpers
  transport/
    io_uring.zig            — IoUring wrapper (Linux only)
    kqueue.zig              — kqueue wrapper (macOS)
    iocp.zig                — IOCP wrapper (Windows, stub)
    network_io.zig          — NetworkIo comptime union (platform dispatch)
    udp_socket.zig          — UdpSocket struct + BrokerSockets
    buffer_pool.zig         — BufferPool with cache-line-aligned slots
  transport.zig             — public re-exports
```

**Re-exports file: `src/transport.zig`**

```zig
pub const frames = @import("protocol/frames.zig");
pub const frame_parser = @import("protocol/frame_parser.zig");
pub const NetworkIo = @import("transport/network_io.zig").NetworkIo;
pub const UdpSocket = @import("transport/udp_socket.zig").UdpSocket;
pub const BrokerSockets = @import("transport/udp_socket.zig").BrokerSockets;
pub const BufferPool = @import("transport/buffer_pool.zig").BufferPool;
pub const BufferSlot = @import("transport/buffer_pool.zig").BufferSlot;

const builtin = @import("builtin");

pub const IoUring = if (builtin.os.tag == .linux)
    @import("transport/io_uring.zig").IoUring
else
    void;
```

---

## 8. Implementation Steps

Build the transport layer bottom-up. Each step is testable in isolation before
proceeding to the next.

### Step 1: Frame Structs (`protocol/frames.zig`)

1. Define all `packed struct` types: `FrameHeader`, `DataFrameHeader`, `SetupFrame`,
   `StatusMessage`, `NakFrame`.
2. Add `comptime` assertions for struct sizes.
3. Add `FrameType` enum with `fromU16` helper.
4. Add `makeHeartbeat` helper function.
5. Add flag helper methods on `DataFrameHeader`.
6. **Test:** All size assertions pass. Roundtrip encode/decode through byte buffers.

### Step 2: Frame Parser (`protocol/frame_parser.zig`)

1. Implement all `read*` functions (flyweight overlays via `@ptrCast`).
2. Implement `ParsedFrame` union and `parseFrame` dispatch.
3. Implement `encodeDataFrame` for the send path.
4. **Test:** Parse each frame type from raw bytes. Dispatch unknown types to `.unknown`.

### Step 3: Buffer Pool (`transport/buffer_pool.zig`)

1. Implement `BufferPool.init`, `acquire`, `release`, `deinit`.
2. Implement `toIovecs` for io_uring registration.
3. **Test:** Acquire all, verify exhaustion, release one, acquire again.
   Verify cache-line alignment.

### Step 4: UDP Socket (`transport/udp_socket.zig`)

1. Implement `UdpSocket.bind`, `create`, `setSendBufferSize`, `setRecvBufferSize`,
   `close`.
2. Implement `BrokerSockets` struct.
3. **Test:** Bind to loopback, send a packet via `sendto`, receive via `recvfrom`.

### Step 5: IoUring Wrapper (`transport/io_uring.zig`) — Linux only

1. Implement `IoUring.init` with configurable queue depth and flags.
2. Implement `prepareSend`, `prepareRecv`, `prepareTimeout`, `prepareNop`.
3. Implement `submit`, `submitAndWait`.
4. Implement `pollCompletions` (both comptime and dynamic variants).
5. Implement `registerBuffers`, `unregisterBuffers`.
6. **Test:** NOP roundtrip. UDP send+recv through io_uring.

### Step 6: Multishot + SQPOLL (`transport/io_uring.zig`) — Linux only

1. Implement `setupProvidedBufferRing` and `prepareMultishotRecv`.
2. Implement `IoUringConfig` and `initWithConfig` with SQPOLL support.
3. **Test:** Send 10 packets, receive all via one multishot SQE. SQPOLL init
   (may require elevated privileges).

### Step 7: kqueue Backend (`transport/kqueue.zig`) — macOS only

1. Implement `KqueueNetworkIo.init`, `deinit`, `registerRead`, `poll`.
2. Implement `sendTo` and `recvFrom` wrappers.
3. **Test:** Bind, register, send, poll, receive on macOS.

### Step 8: NetworkIo Union (`transport/network_io.zig`)

1. Define `NetworkIo` as comptime-selected type.
2. Implement `IoUringNetworkIo` wrapping `IoUring` + `BufferPool`.
3. Implement `KqueueNetworkIo` wrapping kqueue backend.
4. Stub `IocpNetworkIo` for Windows.
5. **Test:** Write a generic test that uses `NetworkIo` and compiles on all platforms.

### Step 9: Integration

1. Wire `BrokerSockets` + `BufferPool` + `NetworkIo` together.
2. Verify the complete flow: allocate sockets, create buffer pools, initialize
   `NetworkIo`, register buffers, queue a send, submit, poll completion, verify receipt.
3. Run the benchmark comparing io_uring vs. `sendmsg`.

---

## Appendix A: Byte-Level Verification

To verify that `packed struct` layouts match the wire format, dump the struct as raw
bytes and compare against the specification:

```zig
test "DataFrameHeader byte-level layout" {
    const header = frames.DataFrameHeader{
        .frame_length = 0x01020304,
        .version = 0x00,
        .flags = 0xC0,
        .frame_type = 0x0001,
        .term_offset = 0,
        .source_node_id = 0x0A,
        .target_node_id = 0x0B,
        .source_service_id = 0x0064,
        .target_service_id = 0x00C8,
        .template_id = 0x002A,
        .correlation_id = 0x00003039,
        .msg_flags = 0,
        .reserved = [_]u8{0} ** 7,
        .sequence_number = 0x63,
    };

    const bytes: *const [40]u8 = @ptrCast(&header);

    // frame_length at offset 0 (little-endian i32)
    try testing.expectEqual(@as(u8, 0x04), bytes[0]);
    try testing.expectEqual(@as(u8, 0x03), bytes[1]);
    try testing.expectEqual(@as(u8, 0x02), bytes[2]);
    try testing.expectEqual(@as(u8, 0x01), bytes[3]);

    // version at offset 4
    try testing.expectEqual(@as(u8, 0x00), bytes[4]);

    // flags at offset 5
    try testing.expectEqual(@as(u8, 0xC0), bytes[5]);

    // frame_type at offset 6 (little-endian u16)
    try testing.expectEqual(@as(u8, 0x01), bytes[6]);
    try testing.expectEqual(@as(u8, 0x00), bytes[7]);

    // source_node_id at offset 12
    try testing.expectEqual(@as(u8, 0x0A), bytes[12]);

    // target_node_id at offset 13
    try testing.expectEqual(@as(u8, 0x0B), bytes[13]);

    // sequence_number at offset 32 (little-endian i64)
    try testing.expectEqual(@as(u8, 0x63), bytes[32]);
}
```

## Appendix B: io_uring Kernel Version Requirements

| Feature | Minimum kernel | Notes |
|---|---|---|
| Basic io_uring | 5.1 | `IORING_OP_NOP`, `IORING_OP_READV/WRITEV` |
| `IORING_OP_SENDMSG` | 5.3 | Required for UDP sends |
| `IORING_OP_RECVMSG` | 5.3 | Required for UDP receives |
| `IORING_SETUP_SQPOLL` | 5.4 | Kernel-side submission polling |
| `IORING_OP_TIMEOUT` | 5.4 | Timer support |
| Registered buffers | 5.1 | `IORING_REGISTER_BUFFERS` |
| Provided buffers | 5.7 | `IOSQE_BUFFER_SELECT` |
| Buffer rings | 5.19 | `IORING_REGISTER_PBUF_RING` |
| Multishot recv | 5.20 | `IORING_RECV_MULTISHOT` |
| Zero-copy send | 6.0 | `IORING_OP_SEND_ZC` |

**Minimum recommended kernel: 5.4** (covers all basic operations + SQPOLL).
**Optimal kernel: 5.20+** for multishot receives with provided buffer rings.

The `IoUring.init` function should check the kernel version at runtime (via `uname`)
and log a warning if multishot or SQPOLL features are requested but unavailable.

---

## Appendix C: Memory Ordering Notes

Frame headers are written to send buffers by a single thread (the sender event loop)
and read by the kernel. No inter-thread atomic ordering is needed for the encode path.

On the receive path, frame headers are written by the kernel into receive buffers and
read by a single thread (the receiver event loop). Again, no inter-thread atomic
ordering is needed — the io_uring CQE acts as a happens-before barrier.

The only point where atomic ordering matters for the transport layer is when handing
a received frame to the receive log buffer (which may be read by another thread for
routing). That ordering is handled by the receive log buffer layer
([06 — Receive Path](06-receive-path.md)), not by the transport layer itself.

---

*Previous: [03 — Concurrent Data Structures](03-concurrent-data-structures.md)*
*Next: [05 — Send Path](05-send-path.md)*