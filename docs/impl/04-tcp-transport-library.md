# 04 — TCP Transport Library (ringloom\_tcp)

> **Depends on:** [01 — Platform Abstraction](01-platform-abstraction.md) (clocks, error
> utilities), [02 — Memory Layout & Shared Memory](02-memory-layout-and-shared-memory.md)
> (constants, node configuration)
>
> **Depended on by:** [05 — Send Path](05-send-path.md) (frame construction, TCP writes),
> [06 — Receive Path](06-receive-path.md) (TCP reads, frame parsing),
> [11 — Cluster Management](11-cluster-management.md) (peer lifecycle)

This document specifies `ringloom_tcp` — a **standalone Zig library** that provides
high-performance, non-blocking TCP I/O for the RingLoom broker. The library is compiled
as a separate module (`ringloom_tcp`) and imported by the broker and tests without
pulling in broker-internal types.

`ringloom_tcp` owns three responsibilities:

1. **Platform I/O** — abstracting the kernel I/O facility (io\_uring on Linux,
   kqueue on macOS) behind a common interface so that all higher layers are
   platform-agnostic.
2. **Connection lifecycle** — accepting inbound connections, initiating outbound
   connections, maintaining a state machine per peer, and performing an
   application-level handshake to validate identity.
3. **Message framing** — layering a length-prefixed frame protocol on top of the
   TCP byte stream, handling partial reads and writes, and enforcing maximum
   frame sizes.

All code targets **Zig 0.14.x** stable.

---

## Table of Contents

1.  [Overview](#1-overview)
    1.  [Three-Layer Architecture](#11-three-layer-architecture)
    2.  [Compile-Time Backend Selection](#12-compile-time-backend-selection)
2.  [I/O Engine Interface](#2-io-engine-interface)
    1.  [Core Types](#21-core-types)
    2.  [The `IoEngine` Interface](#22-the-ioengine-interface)
    3.  [Completion Type](#23-completion-type)
3.  [io\_uring Backend (Linux)](#3-io_uring-backend-linux)
    1.  [Ring Setup](#31-ring-setup)
    2.  [SQE Patterns](#32-sqe-patterns)
    3.  [Buffer Registration](#33-buffer-registration)
    4.  [Completion Harvesting](#34-completion-harvesting)
    5.  [Error Handling](#35-error-handling)
    6.  [Zig Wrapper](#36-zig-wrapper)
    7.  [Duty-Cycle Integration](#37-duty-cycle-integration)
4.  [kqueue Backend (macOS)](#4-kqueue-backend-macos)
    1.  [Filter Configuration](#41-filter-configuration)
    2.  [Edge-Triggered Semantics](#42-edge-triggered-semantics)
    3.  [Accept and Connect](#43-accept-and-connect)
    4.  [Interface Mapping](#44-interface-mapping)
5.  [Future Backends](#5-future-backends)
    1.  [TCPDirect (Xilinx)](#51-tcpdirect-xilinx)
    2.  [F-Stack (DPDK)](#52-f-stack-dpdk)
    3.  [Build Option Selection](#53-build-option-selection)
6.  [Connection Manager](#6-connection-manager)
    1.  [Connection State Machine](#61-connection-state-machine)
    2.  [State Enum and Connection Slot](#62-state-enum-and-connection-slot)
    3.  [Accept Loop](#63-accept-loop)
    4.  [Outbound Connect with Exponential Backoff](#64-outbound-connect-with-exponential-backoff)
    5.  [Health Monitoring](#65-health-monitoring)
7.  [Connection Handshake Protocol](#7-connection-handshake-protocol)
    1.  [Handshake Frame Layout](#71-handshake-frame-layout)
    2.  [Handshake Packed Struct](#72-handshake-packed-struct)
    3.  [Validation Rules](#73-validation-rules)
    4.  [Stale Connection Detection](#74-stale-connection-detection)
8.  [Message Framing Layer](#8-message-framing-layer)
    1.  [Frame Header Layout](#81-frame-header-layout)
    2.  [Frame Header Packed Struct](#82-frame-header-packed-struct)
    3.  [Partial Read State Machine](#83-partial-read-state-machine)
    4.  [Partial Write State Machine](#84-partial-write-state-machine)
    5.  [Frame Validation](#85-frame-validation)
    6.  [Protocol Errors](#86-protocol-errors)
9.  [TCP Socket Configuration](#9-tcp-socket-configuration)
    1.  [Socket Options Table](#91-socket-options-table)
    2.  [Configuration Code](#92-configuration-code)
10. [Library API Surface](#10-library-api-surface)
    1.  [`TcpTransport` Struct](#101-tcptransport-struct)
    2.  [Lifecycle Methods](#102-lifecycle-methods)
    3.  [I/O Methods](#103-io-methods)
    4.  [Usage Example](#104-usage-example)
11. [Build Integration](#11-build-integration)
    1.  [Module Definition](#111-module-definition)
    2.  [Compile-Time Backend Selection](#112-compile-time-backend-selection)
    3.  [Conditional Compilation](#113-conditional-compilation)
    4.  [Test Targets](#114-test-targets)
12. [Testing Strategy](#12-testing-strategy)
    1.  [Unit Tests — Framing](#121-unit-tests--framing)
    2.  [Unit Tests — Handshake](#122-unit-tests--handshake)
    3.  [Integration Tests](#123-integration-tests)
    4.  [Platform-Specific Tests](#124-platform-specific-tests)

**Appendices**

- [A — Wire Byte-Order Reference](#appendix-a--wire-byte-order-reference)
- [B — Error Code Catalogue](#appendix-b--error-code-catalogue)
- [C — Configuration Defaults](#appendix-c--configuration-defaults)

---

## 1. Overview

`ringloom_tcp` provides RingLoom's broker-to-broker transport layer. It relies on TCP's
reliable, ordered byte-stream delivery and focuses on efficient I/O submission,
connection lifecycle, and message framing.

### 1.1 Three-Layer Architecture

```
┌──────────────────────────────────────────────────────────┐
│                   Broker / Tests                         │
│                                                          │
│   TcpTransport.send()  TcpTransport.poll()               │
│         │                    │                            │
├─────────┼────────────────────┼────────────────────────────┤
│         ▼                    ▼                            │
│  ┌─────────────┐    ┌──────────────────┐                 │
│  │  Framing    │    │  Connection      │     Layer 3     │
│  │  Layer      │    │  Manager         │     (API)       │
│  │             │    │                  │                 │
│  │  encode()   │    │  state machine   │                 │
│  │  decode()   │    │  handshake       │                 │
│  └──────┬──────┘    └────────┬─────────┘                 │
│         │                    │                            │
├─────────┼────────────────────┼────────────────────────────┤
│         ▼                    ▼                            │
│  ┌────────────────────────────────────────┐              │
│  │          I/O Engine Interface          │  Layer 2     │
│  │                                        │  (abstract)  │
│  │  submit_recv()  submit_send()          │              │
│  │  submit_accept()  harvest()            │              │
│  └───────────────────┬────────────────────┘              │
│                      │                                    │
├──────────────────────┼────────────────────────────────────┤
│                      ▼                                    │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐   │
│  │  io_uring    │  │  kqueue      │  │  future      │   │
│  │  (Linux)     │  │  (macOS)     │  │  backends    │   │
│  └──────────────┘  └──────────────┘  └──────────────┘   │
│                                              Layer 1     │
│                                              (platform)  │
└──────────────────────────────────────────────────────────┘
```

| Layer | Responsibility | Key Types |
|-------|---------------|-----------|
| 1 — Platform I/O | Kernel syscall wrappers | `IoUring`, `Kqueue` |
| 2 — I/O Interface | Common submission/completion API | `IoEngine`, `Completion`, `ConnectionHandle` |
| 3 — API | Framing, connection management, public API | `TcpTransport`, `FrameHeader`, `ConnectionState` |

### 1.2 Compile-Time Backend Selection

The I/O backend is chosen at **compile time** via a build option. There is no
runtime dispatch — the backend is a concrete type resolved by `comptime` generics:

```zig
pub fn TcpTransportImpl(comptime Engine: type) type {
    return struct {
        engine: Engine,
        // ... fields parameterized on Engine
    };
}

/// Alias selected by build.zig.
pub const TcpTransport = TcpTransportImpl(selected_engine);
```

This design means:

- Zero virtual-dispatch overhead on the hot path.
- Dead-code elimination removes the unused backend entirely.
- Tests can instantiate `TcpTransportImpl` with a mock engine.

---

## 2. I/O Engine Interface

Every platform backend must satisfy a common compile-time interface. The broker
code and connection manager program against this interface; the concrete backend
is injected at compile time.

### 2.1 Core Types

**File: `src/ringloom_tcp/io_engine.zig`**

```zig
/// Opaque handle to a TCP connection within the I/O engine.
/// Indexes into the engine's internal connection table.
pub const ConnectionHandle = enum(u16) {
    invalid = std.math.maxInt(u16),
    _,

    pub fn toIndex(self: ConnectionHandle) u16 {
        return @intFromEnum(self);
    }
};

/// Result of a single completed I/O operation.
pub const Completion = struct {
    handle: ConnectionHandle,
    op: OpType,
    result: Result,

    pub const OpType = enum(u8) {
        accept,
        connect,
        recv,
        send,
        close,
    };

    pub const Result = union(enum) {
        /// Successful operation — `bytes` is the number of bytes transferred
        /// (for recv/send) or 0 (for accept/connect/close).
        ok: struct { bytes: u32 },
        /// Operation failed with a kernel error code.
        err: std.posix.E,
        /// Peer closed the connection (recv returned 0).
        eof: void,
    };
};
```

### 2.2 The `IoEngine` Interface

A valid `IoEngine` implementation must expose the following declarations. The
interface is checked at compile time via `comptime` assertions in `TcpTransportImpl`.

| Method | Signature | Description |
|--------|-----------|-------------|
| `init` | `fn (allocator: Allocator, max_connections: u16) !Engine` | Create the engine. |
| `deinit` | `fn (self: *Engine) void` | Release kernel resources. |
| `submit_accept` | `fn (self: *Engine, listen_fd: fd_t) !void` | Begin accepting on a listening socket. |
| `submit_connect` | `fn (self: *Engine, handle: ConnectionHandle, addr: Address) !void` | Initiate an outbound TCP connection. |
| `submit_recv` | `fn (self: *Engine, handle: ConnectionHandle, buf: []u8) !void` | Submit a read into `buf`. |
| `submit_send` | `fn (self: *Engine, handle: ConnectionHandle, data: []const u8) !void` | Submit a write of `data`. |
| `submit_close` | `fn (self: *Engine, handle: ConnectionHandle) !void` | Close the connection. |
| `harvest` | `fn (self: *Engine, completions: []Completion) u32` | Drain completed operations (non-blocking). |

**Compile-time interface check:**

```zig
fn assertValidEngine(comptime E: type) void {
    const required = .{
        "init", "deinit", "submit_accept", "submit_connect",
        "submit_recv", "submit_send", "submit_close", "harvest",
    };
    inline for (required) |name| {
        if (!@hasDecl(E, name))
            @compileError("IoEngine missing required method: " ++ name);
    }
}
```

### 2.3 Completion Type

Completions are harvested in batches. The caller provides a slice and the engine
fills it with up to `slice.len` completions, returning the count:

```zig
var completions: [256]Completion = undefined;
const n = engine.harvest(&completions);
for (completions[0..n]) |c| {
    switch (c.op) {
        .recv => handleRecv(c.handle, c.result),
        .send => handleSend(c.handle, c.result),
        .accept => handleAccept(c.handle, c.result),
        .connect => handleConnect(c.handle, c.result),
        .close => handleClose(c.handle),
    }
}
```

The maximum batch size is tunable. The broker defaults to 256 completions per
harvest call. This keeps latency bounded — the event loop processes at most 256
I/O events before yielding to other duties (counter snapshots, heartbeat checks).

---

## 3. io\_uring Backend (Linux)

The Linux backend wraps `io_uring` for zero-copy, batched, asynchronous I/O. It
is the primary production backend. All SQE submission and CQE harvesting is done
through Zig's `std.os.linux.IoUring` wrapper.

### 3.1 Ring Setup

**File: `src/ringloom_tcp/io_uring_engine.zig`**

```zig
const std = @import("std");
const linux = std.os.linux;
const IoUring = linux.IoUring;

pub const IoUringEngine = struct {
    ring: IoUring,
    /// Map from ConnectionHandle → file descriptor.
    fd_table: [max_connections]std.posix.fd_t,
    /// Registered buffer group for recv operations.
    recv_buffers: BufferGroup,
    max_connections: u16,
    listen_fd: std.posix.fd_t,

    const ring_entries = 4096;
    const max_connections = 1024;

    pub fn init(allocator: std.mem.Allocator, max_conns: u16) !IoUringEngine {
        _ = max_conns;
        var ring = try IoUring.init(ring_entries, .{
            .COOP_TASKRUN = true,
            .SINGLE_ISSUER = true,
            .DEFER_TASKRUN = true,
        });
        errdefer ring.deinit();

        const recv_bufs = try BufferGroup.init(allocator, 512, 4096);

        var fd_table: [max_connections]std.posix.fd_t = undefined;
        @memset(&fd_table, -1);

        return .{
            .ring = ring,
            .fd_table = fd_table,
            .recv_buffers = recv_bufs,
            .max_connections = max_connections,
            .listen_fd = -1,
        };
    }

    pub fn deinit(self: *IoUringEngine) void {
        self.recv_buffers.deinit();
        self.ring.deinit();
    }
    // ... methods follow
};
```

**Ring flags explained:**

| Flag | Purpose |
|------|---------|
| `COOP_TASKRUN` | Avoids IPI interrupts; completions are deferred until the next `enter()` call. Reduces context-switch overhead. |
| `SINGLE_ISSUER` | Asserts only one thread submits SQEs. Enables kernel-side optimizations. |
| `DEFER_TASKRUN` | Defers completion processing to `enter()`. Combined with `SINGLE_ISSUER`, this allows the kernel to batch completions efficiently. |

The ring size of **4096 entries** is deliberately large. With up to 1024 connections,
each potentially having one outstanding recv and one outstanding send, the worst case
is 2048 in-flight SQEs plus accept and connect operations.

### 3.2 SQE Patterns

#### RECV

```zig
pub fn submit_recv(self: *IoUringEngine, handle: ConnectionHandle, buf: []u8) !void {
    const fd = self.fd_table[handle.toIndex()];
    var sqe = try self.ring.get_sqe();
    sqe.prep_recv(fd, buf, 0);
    sqe.user_data = encodeUserData(handle, .recv);
}
```

#### SEND

```zig
pub fn submit_send(self: *IoUringEngine, handle: ConnectionHandle, data: []const u8) !void {
    const fd = self.fd_table[handle.toIndex()];
    var sqe = try self.ring.get_sqe();
    sqe.prep_send(fd, data, 0);
    sqe.user_data = encodeUserData(handle, .send);
}
```

#### ACCEPT (multishot)

Multishot accept keeps a single SQE armed and generates a CQE for every incoming
connection. This avoids resubmitting an accept SQE after each new connection.

```zig
pub fn submit_accept(self: *IoUringEngine, listen_fd: std.posix.fd_t) !void {
    self.listen_fd = listen_fd;
    var sqe = try self.ring.get_sqe();
    sqe.prep_multishot_accept(listen_fd, null, null, 0);
    sqe.user_data = encodeUserData(.invalid, .accept);
}
```

When a multishot accept fires, the CQE result contains the new file descriptor.
The `IORING_CQE_F_MORE` flag indicates that the multishot operation is still armed:

```zig
fn processAcceptCompletion(self: *IoUringEngine, cqe: linux.io_uring_cqe) !ConnectionHandle {
    if (cqe.res < 0) {
        const err = std.posix.errno(-cqe.res);
        return error.AcceptFailed;
    }
    const new_fd: std.posix.fd_t = @intCast(cqe.res);

    // Allocate a handle for the new connection.
    const handle = try self.allocateHandle();
    self.fd_table[handle.toIndex()] = new_fd;

    // If IORING_CQE_F_MORE is NOT set, the multishot was cancelled — rearm.
    if (cqe.flags & linux.IORING_CQE_F_MORE == 0) {
        try self.submit_accept(self.listen_fd);
    }

    return handle;
}
```

#### CONNECT

```zig
pub fn submit_connect(self: *IoUringEngine, handle: ConnectionHandle, addr: std.net.Address) !void {
    const fd = self.fd_table[handle.toIndex()];
    var sqe = try self.ring.get_sqe();
    sqe.prep_connect(fd, &addr.any, addr.getOsSockAddr().len);
    sqe.user_data = encodeUserData(handle, .connect);
}
```

#### WRITEV (scatter-gather send)

When the framing layer prepends a header to a payload, it can avoid a copy by
using `writev` with two iovecs — one for the header and one for the body:

```zig
pub fn submit_writev(
    self: *IoUringEngine,
    handle: ConnectionHandle,
    header: []const u8,
    body: []const u8,
) !void {
    const fd = self.fd_table[handle.toIndex()];
    const iovecs = [2]std.posix.iovec_const{
        .{ .base = header.ptr, .len = header.len },
        .{ .base = body.ptr, .len = body.len },
    };
    var sqe = try self.ring.get_sqe();
    sqe.prep_writev(fd, &iovecs, 0);
    sqe.user_data = encodeUserData(handle, .send);
}
```

#### User-Data Encoding

Every SQE carries a 64-bit `user_data` field that is echoed back in the CQE. We
pack the connection handle and operation type into this field:

```zig
fn encodeUserData(handle: ConnectionHandle, op: Completion.OpType) u64 {
    return @as(u64, @intFromEnum(handle)) |
        (@as(u64, @intFromEnum(op)) << 16);
}

fn decodeUserData(user_data: u64) struct { handle: ConnectionHandle, op: Completion.OpType } {
    return .{
        .handle = @enumFromInt(@as(u16, @truncate(user_data))),
        .op = @enumFromInt(@as(u8, @truncate(user_data >> 16))),
    };
}
```

### 3.3 Buffer Registration

For recv operations, we pre-allocate a pool of fixed buffers and register them
with the kernel using `IORING_REGISTER_BUFFERS`. This enables the kernel to
avoid copy-on-receive for registered buffer ranges.

```zig
const BufferGroup = struct {
    buffers: []align(4096) u8,
    buffer_size: u32,
    count: u32,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, count: u32, size: u32) !BufferGroup {
        const total = @as(usize, count) * @as(usize, size);
        const mem = try allocator.alignedAlloc(u8, 4096, total);
        return .{
            .buffers = mem,
            .buffer_size = size,
            .count = count,
            .allocator = allocator,
        };
    }

    pub fn get(self: *const BufferGroup, index: u32) []u8 {
        const start = @as(usize, index) * @as(usize, self.buffer_size);
        return self.buffers[start..][0..self.buffer_size];
    }

    pub fn deinit(self: *BufferGroup) void {
        self.allocator.free(self.buffers);
    }
};
```

### 3.4 Completion Harvesting

The `harvest` method calls `io_uring_enter` with `GETEVENTS` to collect CQEs,
then decodes each CQE into a `Completion`:

```zig
pub fn harvest(self: *IoUringEngine, completions: []Completion) u32 {
    // Submit any pending SQEs and wait for at least 0 completions.
    _ = self.ring.submit() catch return 0;

    var count: u32 = 0;
    while (count < completions.len) {
        const cqe = self.ring.peek_cqe() catch break;
        defer self.ring.advance_cqe(1);

        const decoded = decodeUserData(cqe.user_data);
        completions[count] = .{
            .handle = decoded.handle,
            .op = decoded.op,
            .result = if (cqe.res < 0)
                .{ .err = std.posix.errno(-cqe.res) }
            else if (cqe.res == 0 and decoded.op == .recv)
                .eof
            else
                .{ .ok = .{ .bytes = @intCast(cqe.res) } },
        };
        count += 1;
    }
    return count;
}
```

The harvest loop is deliberately **non-blocking**. It drains whatever CQEs are
available and returns immediately. The caller (the broker event loop) controls
how often `harvest` is invoked within its duty cycle.

### 3.5 Error Handling

io\_uring operations can fail at two points:

1. **SQE submission** — `get_sqe()` returns `error.SubmissionQueueFull`. The caller
   must either flush pending SQEs or increase the ring size.
2. **CQE result** — a negative `res` field indicates a kernel error. Common codes:

| `errno` | Meaning | Action |
|---------|---------|--------|
| `ECONNRESET` | Peer reset the connection | Close handle, notify connection manager |
| `EPIPE` | Write to closed connection | Close handle |
| `ECONNREFUSED` | Connect failed | Schedule reconnect with backoff |
| `ETIMEDOUT` | TCP timeout | Close handle, schedule reconnect |
| `ECANCELED` | SQE was cancelled (e.g., during shutdown) | Ignore |
| `ENOBUFS` | No buffers available | Retry after buffer pool refill |

All errors propagate as `Completion.Result.err` values. The connection manager
decides what to do (reconnect, close, log).

### 3.6 Zig Wrapper

The full io\_uring engine is a single file that imports `std.os.linux` and wraps
the raw ring operations behind the `IoEngine` interface. Key design decisions:

- **No heap allocation on the hot path.** The fd table, buffer group, and
  completion scratch are allocated once at init and reused.
- **Single-threaded.** The engine is owned by one thread (the I/O thread in the
  broker). No synchronization is needed on engine state.
- **Explicit submit/harvest split.** SQEs accumulate until `harvest` is called,
  which flushes them in a single `io_uring_enter` syscall. This amortizes the
  syscall cost over many operations.

```zig
// Conditional compilation — only available on Linux.
const builtin = @import("builtin");
pub const IoUringEngine = if (builtin.os.tag == .linux)
    @import("io_uring_engine.zig").IoUringEngine
else
    @compileError("io_uring is only available on Linux");
```

### 3.7 Duty-Cycle Integration

The broker event loop calls the I/O engine in a duty cycle pattern:

```
┌─────────────────────────────────────────────────┐
│                Broker I/O Thread                 │
│                                                  │
│  ┌──────────┐  ┌───────────┐  ┌──────────────┐  │
│  │ Drain    │  │ Submit    │  │ Harvest      │  │
│  │ send     │──► outbound  │──► completions  │  │
│  │ ring     │  │ SQEs      │  │ from ring    │  │
│  │ buffer   │  │           │  │              │  │
│  └──────────┘  └───────────┘  └──────┬───────┘  │
│       ▲                              │           │
│       │        ┌───────────┐         │           │
│       │        │ Process   │◄────────┘           │
│       └────────│ received  │                     │
│                │ frames    │                     │
│                └───────────┘                     │
└─────────────────────────────────────────────────┘
```

A single duty cycle iteration:

1. **Drain send ring buffer** — read outbound messages from shared memory.
2. **Submit SQEs** — enqueue send operations for drained messages, plus any
   pending recv re-submissions.
3. **Harvest CQEs** — collect completed operations (up to 256 per call).
4. **Process completions** — dispatch recv completions to the framing layer,
   handle connect/accept completions in the connection manager.

The cycle runs in a busy-spin loop with configurable idle behavior (yield after
N empty iterations, or use `io_uring_enter` with a timeout for low-latency idle).

---

## 4. kqueue Backend (macOS)

The macOS backend uses kqueue for event notification. While kqueue does not offer
the same batched-submission model as io\_uring, it provides efficient edge-triggered
readiness notifications for TCP sockets.

### 4.1 Filter Configuration

**File: `src/ringloom_tcp/kqueue_engine.zig`**

```zig
const std = @import("std");
const posix = std.posix;
const system = std.posix.system;

pub const KqueueEngine = struct {
    kq: std.posix.fd_t,
    fd_table: [max_connections]std.posix.fd_t,
    /// Pending read/write state per connection.
    conn_state: [max_connections]ConnState,
    max_connections: u16,
    listen_fd: std.posix.fd_t,

    const max_connections = 1024;

    const ConnState = struct {
        recv_buf: ?[]u8 = null,
        send_buf: ?[]const u8 = null,
        send_offset: u32 = 0,
    };

    pub fn init(allocator: std.mem.Allocator, max_conns: u16) !KqueueEngine {
        _ = allocator;
        _ = max_conns;
        const kq = try posix.kqueue();
        errdefer posix.close(kq);

        var fd_table: [max_connections]std.posix.fd_t = undefined;
        @memset(&fd_table, -1);

        return .{
            .kq = kq,
            .fd_table = fd_table,
            .conn_state = [_]ConnState{.{}} ** max_connections,
            .max_connections = max_connections,
            .listen_fd = -1,
        };
    }

    pub fn deinit(self: *KqueueEngine) void {
        posix.close(self.kq);
    }
    // ... methods follow
};
```

### 4.2 Edge-Triggered Semantics

All filters use `EV_CLEAR` for edge-triggered behavior. This means a notification
fires once when the socket becomes readable/writable. The engine must fully drain
the socket on each notification (read until `EWOULDBLOCK`, write until the buffer
is empty or `EWOULDBLOCK`).

```zig
fn registerSocket(self: *KqueueEngine, handle: ConnectionHandle) !void {
    const fd = self.fd_table[handle.toIndex()];
    const changes = [_]system.kevent{
        .{
            .ident = @intCast(fd),
            .filter = system.EVFILT.READ,
            .flags = system.EV.ADD | system.EV.CLEAR,
            .fflags = 0,
            .data = 0,
            .udata = @intFromEnum(handle),
        },
        .{
            .ident = @intCast(fd),
            .filter = system.EVFILT.WRITE,
            .flags = system.EV.ADD | system.EV.CLEAR,
            .fflags = 0,
            .data = 0,
            .udata = @intFromEnum(handle),
        },
    };
    _ = try posix.kevent(self.kq, &changes, &.{}, null);
}
```

| Flag | Effect |
|------|--------|
| `EV_ADD` | Register the filter for this fd. |
| `EV_CLEAR` | Reset the filter after delivery (edge-triggered). Subsequent events only fire when new data arrives or buffer space opens. |
| `EVFILT_READ` | Fires when data is available to read. |
| `EVFILT_WRITE` | Fires when the socket's send buffer has space. |

### 4.3 Accept and Connect

**Accept** uses `EVFILT_READ` on the listening socket. When the event fires, the
engine calls `posix.accept()` in a loop until `EWOULDBLOCK`:

```zig
pub fn submit_accept(self: *KqueueEngine, listen_fd: std.posix.fd_t) !void {
    self.listen_fd = listen_fd;
    const change = [_]system.kevent{.{
        .ident = @intCast(listen_fd),
        .filter = system.EVFILT.READ,
        .flags = system.EV.ADD | system.EV.CLEAR,
        .fflags = 0,
        .data = 0,
        .udata = @intFromEnum(ConnectionHandle.invalid),
    }};
    _ = try posix.kevent(self.kq, &change, &.{}, null);
}
```

**Connect** registers `EVFILT_WRITE` on the connecting socket. When the event
fires, the engine checks `SO_ERROR` to determine success or failure:

```zig
pub fn submit_connect(self: *KqueueEngine, handle: ConnectionHandle, addr: std.net.Address) !void {
    const fd = self.fd_table[handle.toIndex()];
    posix.connect(fd, &addr.any, addr.getOsSockAddr().len) catch |err| switch (err) {
        error.WouldBlock => {},  // Expected for non-blocking connect.
        else => return err,
    };
    // Write-readiness indicates connect completion.
    try self.registerSocket(handle);
}
```

### 4.4 Interface Mapping

The kqueue engine maps to the `IoEngine` interface by converting between
readiness notifications and the completion model:

| `IoEngine` method | kqueue implementation |
|-------------------|---------------------|
| `submit_recv(handle, buf)` | Store `buf` in `conn_state`. On `EVFILT_READ`, call `posix.recv()` into `buf` and emit a completion. |
| `submit_send(handle, data)` | Store `data` in `conn_state`. On `EVFILT_WRITE`, call `posix.send()` and emit a completion. |
| `submit_accept(listen_fd)` | Register `EVFILT_READ` on listen socket. On event, call `posix.accept()`. |
| `submit_connect(handle, addr)` | Call `posix.connect()` (non-blocking), register `EVFILT_WRITE`. On event, check `SO_ERROR`. |
| `submit_close(handle)` | Call `posix.close(fd)`, emit close completion. |
| `harvest(completions)` | Call `kevent()` with timeout=0 to drain events, translate to completions. |

```zig
pub fn harvest(self: *KqueueEngine, completions: []Completion) u32 {
    var events: [256]system.kevent = undefined;
    const timeout = system.timespec{ .sec = 0, .nsec = 0 };

    const n = posix.kevent(self.kq, &.{}, &events, &timeout) catch return 0;

    var count: u32 = 0;
    for (events[0..n]) |ev| {
        if (count >= completions.len) break;

        const handle: ConnectionHandle = @enumFromInt(@as(u16, @truncate(ev.udata)));

        if (handle == .invalid) {
            // Accept event on the listen socket.
            count = self.processAcceptEvents(completions, count);
            continue;
        }

        if (ev.filter == system.EVFILT.READ) {
            count = self.processReadEvent(handle, completions, count);
        } else if (ev.filter == system.EVFILT.WRITE) {
            count = self.processWriteEvent(handle, completions, count);
        }
    }
    return count;
}
```

---

## 5. Future Backends

The I/O engine interface is designed to accommodate kernel-bypass and
hardware-offload backends. These are not implemented in the initial release but
the interface has been validated against their API patterns.

### 5.1 TCPDirect (Xilinx)

TCPDirect provides a user-space TCP stack for Xilinx (formerly Solarflare)
network adapters. It eliminates kernel involvement entirely for TCP I/O.

**Interface compatibility:**

| `IoEngine` method | TCPDirect equivalent |
|-------------------|---------------------|
| `submit_recv` | `zft_zc_recv()` — zero-copy receive into NIC buffer |
| `submit_send` | `zft_send()` — send from user buffer |
| `submit_accept` | `zftl_accept()` — accept on a listening zocket |
| `submit_connect` | `zft_connect()` — initiate connection |
| `harvest` | `zf_reactor_perform()` — poll the reactor for events |

### 5.2 F-Stack (DPDK)

F-Stack provides a FreeBSD TCP/IP stack running on DPDK. It exposes a POSIX-like
API (`ff_accept`, `ff_recv`, `ff_send`) that maps directly to our interface.

### 5.3 Build Option Selection

**File: `build.zig`** (excerpt)

```zig
const Backend = enum {
    io_uring,
    kqueue,
    tcp_direct,
    f_stack,
};

const backend = b.option(Backend, "tcp-backend", "TCP I/O backend") orelse
    switch (builtin.os.tag) {
        .linux => .io_uring,
        .macos => .kqueue,
        else => @compileError("Unsupported OS for ringloom_tcp"),
    };

const ringloom_tcp = b.addModule("ringloom_tcp", .{
    .root_source_file = b.path("lib/ringloom_tcp/root.zig"),
});
ringloom_tcp.addOptions("build_options", options);
```

The selected backend is passed as a build option and resolved at comptime in the
library's root module.

---

## 6. Connection Manager

The connection manager owns the lifecycle of every TCP peer connection. It tracks
state, drives the handshake, enforces reconnection policy, and monitors health.

### 6.1 Connection State Machine

```
                    ┌──────────┐
                    │  CLOSED  │◄───────────────────────────┐
                    └────┬─────┘                            │
                         │                                  │
              connect()  │  accept()                        │
                    ┌────▼─────┐                            │
                    │CONNECTING│                            │
                    └────┬─────┘                            │
                         │                                  │
              connected  │  timeout/error                   │
                    ┌────▼─────┐       ┌──────┐            │
                    │HANDSHAKE │──────►│CLOSED│            │
                    └────┬─────┘ fail  └──────┘            │
                         │                                  │
          handshake ok   │                                  │
                    ┌────▼─────┐                            │
                    │CONNECTED │                            │
                    └────┬─────┘                            │
                         │                                  │
              close() /  │  error / timeout                 │
              shutdown   │                                  │
                    ┌────▼─────┐                            │
                    │DRAINING  │────────────────────────────┘
                    └──────────┘    drain complete / timeout
```

| State | Description | Entry Condition |
|-------|-------------|-----------------|
| `CLOSED` | No connection. Slot is available. | Initial state, drain complete, or fatal error. |
| `CONNECTING` | TCP three-way handshake in progress. | `connect()` called or accept completed. |
| `HANDSHAKE` | TCP connected, application handshake in progress. | TCP connect completed. |
| `CONNECTED` | Handshake validated. Ready for message I/O. | Handshake frame validated. |
| `DRAINING` | Graceful close in progress. Pending writes flushing. | `close()` called or error detected. |

### 6.2 State Enum and Connection Slot

**File: `src/ringloom_tcp/connection_manager.zig`**

```zig
pub const ConnectionState = enum(u8) {
    closed = 0,
    connecting = 1,
    handshake = 2,
    connected = 3,
    draining = 4,
};

pub const ConnectionSlot = struct {
    handle: ConnectionHandle,
    state: ConnectionState,
    peer_node_id: u8,
    session_epoch: u64,
    last_recv_time_ns: i128,
    last_send_time_ns: i128,
    /// Reconnect attempt counter (reset on successful connect).
    reconnect_attempts: u16,
    /// Pending outbound handshake (null after handshake complete).
    pending_handshake: ?HandshakeFrame,
    /// Read state machine for incoming frames.
    read_state: FrameReader,
    /// Write state machine for outgoing frames.
    write_state: FrameWriter,

    pub fn reset(self: *ConnectionSlot) void {
        self.state = .closed;
        self.peer_node_id = 0;
        self.session_epoch = 0;
        self.last_recv_time_ns = 0;
        self.last_send_time_ns = 0;
        self.reconnect_attempts = 0;
        self.pending_handshake = null;
        self.read_state.reset();
        self.write_state.reset();
    }
};
```

### 6.3 Accept Loop

The accept loop processes `Completion.OpType.accept` events from the I/O engine.
For each accepted connection it:

1. Allocates a `ConnectionSlot`.
2. Configures socket options (see §9).
3. Transitions to `HANDSHAKE` state.
4. Submits an initial `recv` to read the peer's handshake frame.

```zig
fn handleAcceptCompletion(
    self: *ConnectionManager,
    completion: Completion,
) !void {
    switch (completion.result) {
        .ok => {
            const slot_idx = try self.allocateSlot();
            const slot = &self.slots[slot_idx];
            slot.handle = completion.handle;
            slot.state = .handshake;
            slot.last_recv_time_ns = self.clock.nanotime();

            try self.configureSocket(completion.handle);
            try self.engine.submit_recv(
                completion.handle,
                slot.read_state.currentBuffer(),
            );
        },
        .err => |e| {
            self.counters.accept_errors += 1;
            log.err("accept failed: {}", .{e});
        },
        .eof => unreachable,
    }
}
```

### 6.4 Outbound Connect with Exponential Backoff

Outbound connections use exponential backoff on failure. The delay doubles from
an initial 100 ms up to a cap of 1000 ms:

```
Attempt 1: 100 ms
Attempt 2: 200 ms
Attempt 3: 400 ms
Attempt 4: 800 ms
Attempt 5+: 1000 ms (cap)
```

**File: `src/ringloom_tcp/connection_manager.zig`**

```zig
const backoff_initial_ms: u64 = 100;
const backoff_max_ms: u64 = 1000;

fn computeBackoffMs(attempts: u16) u64 {
    if (attempts == 0) return 0;
    const shift: u6 = @intCast(@min(attempts - 1, 4));
    const delay = backoff_initial_ms << shift;
    return @min(delay, backoff_max_ms);
}

fn scheduleReconnect(self: *ConnectionManager, slot: *ConnectionSlot) void {
    slot.state = .closed;
    slot.reconnect_attempts += 1;
    const delay_ms = computeBackoffMs(slot.reconnect_attempts);

    self.timer_queue.schedule(.{
        .callback = reconnectCallback,
        .context = slot,
        .deadline_ns = self.clock.nanotime() + @as(i128, delay_ms) * std.time.ns_per_ms,
    });
}

fn reconnectCallback(ctx: *anyopaque) void {
    const slot: *ConnectionSlot = @ptrCast(@alignCast(ctx));
    // Re-initiate the connection attempt.
    slot.state = .connecting;
    // The outer event loop will pick this up and call engine.submit_connect().
}
```

**Backoff schedule:**

| Attempt | Delay (ms) | Cumulative (ms) |
|---------|-----------|-----------------|
| 1 | 100 | 100 |
| 2 | 200 | 300 |
| 3 | 400 | 700 |
| 4 | 800 | 1500 |
| 5 | 1000 | 2500 |
| 6 | 1000 | 3500 |

### 6.5 Health Monitoring

Each connection slot tracks `last_recv_time_ns`. The health monitor runs
periodically (every 500 ms by default) and checks:

1. **Idle timeout** — if `now - last_recv_time_ns > idle_timeout_ns` (default 5 s),
   the connection is considered dead. Transition to `DRAINING`.
2. **Handshake timeout** — if the slot has been in `HANDSHAKE` state for more than
   2 seconds, abort and close.
3. **Drain timeout** — if the slot has been in `DRAINING` state for more than 1 second,
   force-close.

```zig
pub fn checkHealth(self: *ConnectionManager, now_ns: i128) void {
    for (&self.slots) |*slot| {
        switch (slot.state) {
            .connected => {
                if (now_ns - slot.last_recv_time_ns > self.config.idle_timeout_ns) {
                    log.warn("connection to node {} idle for too long, draining", .{slot.peer_node_id});
                    self.beginDrain(slot);
                }
            },
            .handshake => {
                if (now_ns - slot.last_recv_time_ns > self.config.handshake_timeout_ns) {
                    log.warn("handshake timeout for handle {}", .{slot.handle.toIndex()});
                    self.forceClose(slot);
                }
            },
            .draining => {
                if (now_ns - slot.last_send_time_ns > self.config.drain_timeout_ns) {
                    self.forceClose(slot);
                }
            },
            .closed, .connecting => {},
        }
    }
}
```

---

## 7. Connection Handshake Protocol

Every new TCP connection — whether inbound or outbound — must complete an
application-level handshake before message I/O begins. The handshake validates
node identity, group membership, and detects stale connections from previous
broker instances.

### 7.1 Handshake Frame Layout

The handshake frame is exactly **24 bytes**:

```
 Offset   Size   Type       Field
──────────────────────────────────────────────────
  0       4      u32        magic (0x474E4952)
  4       1      u8         protocol_version
  5       1      u8         source_node_id
  6       1      u8         target_node_id
  7       1      u8         direction
  8       8      u64        session_epoch
 16       4      u32        group_name_hash (FNV-1a)
 20       4      [4]u8      reserved (zero)
──────────────────────────────────────────────────
 Total:  24 bytes
```

**Field descriptions:**

| Field | Description |
|-------|-------------|
| `magic` | `0x474E4952` — "RING" when written little-endian. Used to quickly reject non-RingLoom connections. |
| `protocol_version` | Current version: `1`. Peers must agree on version. |
| `source_node_id` | The `node_id` of the sender of this handshake frame. |
| `target_node_id` | The `node_id` the sender expects to reach. |
| `direction` | `0x01` = outbound (initiator), `0x02` = inbound (acceptor). |
| `session_epoch` | Monotonically increasing epoch timestamp (nanoseconds since UNIX epoch). Used to detect stale connections. |
| `group_name_hash` | FNV-1a hash of the cluster group name. Prevents cross-cluster connections. |
| `reserved` | Must be zero. Future use. |

### 7.2 Handshake Packed Struct

**File: `src/ringloom_tcp/handshake.zig`**

```zig
const std = @import("std");

pub const HandshakeFrame = packed struct {
    magic: u32 = magic_value,
    protocol_version: u8 = protocol_version_current,
    source_node_id: u8,
    target_node_id: u8,
    direction: Direction,
    session_epoch: u64,
    group_name_hash: u32,
    reserved: u32 = 0,

    pub const magic_value: u32 = 0x474E4952;
    pub const protocol_version_current: u8 = 1;
    pub const size = @sizeOf(HandshakeFrame);

    pub const Direction = enum(u8) {
        outbound = 0x01,
        inbound = 0x02,
    };

    comptime {
        std.debug.assert(@sizeOf(HandshakeFrame) == 24);
    }

    pub fn toBytes(self: HandshakeFrame) [size]u8 {
        return @bitCast(self);
    }

    pub fn fromBytes(bytes: *const [size]u8) HandshakeFrame {
        return @bitCast(bytes.*);
    }

    /// Compute FNV-1a hash of the cluster group name.
    pub fn hashGroupName(name: []const u8) u32 {
        var hash: u32 = 0x811c9dc5; // FNV offset basis
        for (name) |byte| {
            hash ^= byte;
            hash *%= 0x01000193; // FNV prime
        }
        return hash;
    }
};
```

### 7.3 Validation Rules

When a handshake frame is received, the following checks are applied in order:

```zig
pub fn validate(
    frame: HandshakeFrame,
    local_node_id: u8,
    expected_group_hash: u32,
) !void {
    // 1. Magic number must match.
    if (frame.magic != HandshakeFrame.magic_value) {
        return error.InvalidMagic;
    }

    // 2. Protocol version must be supported.
    if (frame.protocol_version != HandshakeFrame.protocol_version_current) {
        return error.UnsupportedProtocolVersion;
    }

    // 3. Target node ID must be us.
    if (frame.target_node_id != local_node_id) {
        return error.WrongTargetNode;
    }

    // 4. Source node ID must not be us (no self-connection).
    if (frame.source_node_id == local_node_id) {
        return error.SelfConnection;
    }

    // 5. Cluster group hash must match.
    if (frame.group_name_hash != expected_group_hash) {
        return error.GroupMismatch;
    }

    // 6. Reserved field must be zero.
    if (frame.reserved != 0) {
        return error.InvalidReservedField;
    }
}
```

| Check | Error | Severity |
|-------|-------|----------|
| Bad magic | `InvalidMagic` | Close immediately — not a RingLoom peer. |
| Version mismatch | `UnsupportedProtocolVersion` | Close — incompatible protocol. |
| Wrong target | `WrongTargetNode` | Close — misconfigured peer. |
| Self-connection | `SelfConnection` | Close — configuration error. |
| Group mismatch | `GroupMismatch` | Close — cross-cluster connection. |
| Non-zero reserved | `InvalidReservedField` | Close — malformed frame. |

### 7.4 Stale Connection Detection

The `session_epoch` field detects connections from a previous broker instance.
Each broker generates a new epoch at startup (current time in nanoseconds). When
a handshake is received:

1. If the peer's `session_epoch` is **less than** the stored epoch for that
   `source_node_id`, the connection is stale — close it.
2. If the peer's `session_epoch` is **greater than or equal to** the stored
   epoch, update the stored epoch and accept.

This handles the common case where a broker restarts and the remote peer still
has a TCP connection open to the old instance. The old socket may linger in
`CLOSE_WAIT` or `TIME_WAIT`; the epoch check ensures the new connection
supersedes it.

```zig
fn checkEpoch(
    self: *ConnectionManager,
    peer_node_id: u8,
    peer_epoch: u64,
) !void {
    const stored = self.peer_epochs[peer_node_id];
    if (peer_epoch < stored) {
        return error.StaleConnection;
    }
    self.peer_epochs[peer_node_id] = peer_epoch;
}
```

---

## 8. Message Framing Layer

After the handshake, all data on the TCP stream is framed using a **length-prefixed
protocol**. Every message is preceded by a 24-byte header that specifies the total
frame length, routing metadata, and a correlation ID.

### 8.1 Frame Header Layout

```
 Offset   Size   Type       Field
──────────────────────────────────────────────────
  0       4      u32        frame_length
  4       1      u8         flags
  5       1      u8         source_node_id
  6       1      u8         target_node_id
  7       1      u8         reserved_1 (zero)
  8       2      u16        source_service_id
 10       2      u16        target_service_id
 12       2      u16        template_id
 14       2      u16        reserved_2 (zero)
 16       8      i64        correlation_id
──────────────────────────────────────────────────
 Total:  24 bytes
```

**Field descriptions:**

| Field | Description |
|-------|-------------|
| `frame_length` | Total frame size **including the 24-byte header**. Minimum value: 24 (header-only frame, used for heartbeats). Maximum value: 1,048,576 (1 MB). |
| `flags` | Bit field: bit 0 = heartbeat, bits 1–7 reserved. |
| `source_node_id` | Originating broker's node ID. |
| `target_node_id` | Destination broker's node ID. |
| `reserved_1` | Must be zero. |
| `source_service_id` | Originating service's ID within the broker. |
| `target_service_id` | Destination service's ID. |
| `template_id` | SBE message template ID for the payload. |
| `reserved_2` | Must be zero. |
| `correlation_id` | Caller-assigned ID for request/response correlation. |

All multi-byte fields are **little-endian** (native on x86-64 and ARM64 in LE mode).

### 8.2 Frame Header Packed Struct

**File: `src/ringloom_tcp/frame.zig`**

```zig
const std = @import("std");

pub const FrameHeader = packed struct {
    frame_length: u32,
    flags: u8,
    source_node_id: u8,
    target_node_id: u8,
    reserved_1: u8 = 0,
    source_service_id: u16,
    target_service_id: u16,
    template_id: u16,
    reserved_2: u16 = 0,
    correlation_id: i64,

    pub const size = @sizeOf(FrameHeader);
    pub const max_frame_length: u32 = 1_048_576; // 1 MB
    pub const min_frame_length: u32 = size; // 24 bytes (header-only = heartbeat)

    comptime {
        std.debug.assert(@sizeOf(FrameHeader) == 24);
    }

    pub const Flags = struct {
        pub const heartbeat: u8 = 0x01;
    };

    pub fn isHeartbeat(self: FrameHeader) bool {
        return self.flags & Flags.heartbeat != 0;
    }

    pub fn payloadLength(self: FrameHeader) u32 {
        return self.frame_length - size;
    }

    pub fn toBytes(self: FrameHeader) [size]u8 {
        return @bitCast(self);
    }

    pub fn fromBytes(bytes: *const [size]u8) FrameHeader {
        return @bitCast(bytes.*);
    }
};
```

### 8.3 Partial Read State Machine

TCP is a byte stream — a single `recv()` call may return a partial header, a
complete header with a partial payload, or multiple complete frames. The
`FrameReader` handles all cases:

```
┌──────────────┐   recv bytes    ┌──────────────────┐
│ READ_HEADER  │────────────────►│ got full header? │
│              │                 │                  │
│ need 24 bytes│◄────── no ──────│  accumulate in   │
│ - have N     │                 │  header_buf      │
└──────────────┘                 └────────┬─────────┘
                                          │ yes
                                          ▼
                                 ┌──────────────────┐
                                 │ validate header  │
                                 │ frame_length ok? │
                                 └────────┬─────────┘
                                          │ yes
                                          ▼
                                 ┌──────────────────┐   recv bytes
                                 │ READ_PAYLOAD     │◄──────────┐
                                 │                  │           │
                                 │ need P bytes     │── partial ┘
                                 │ - have M         │
                                 └────────┬─────────┘
                                          │ complete
                                          ▼
                                 ┌──────────────────┐
                                 │ FRAME_READY      │
                                 │ emit to caller   │
                                 └──────────────────┘
```

**File: `src/ringloom_tcp/frame.zig`**

```zig
pub const FrameReader = struct {
    state: State,
    header_buf: [FrameHeader.size]u8,
    header_bytes_read: u8,
    payload_buf: ?[]u8,
    payload_bytes_read: u32,
    current_header: ?FrameHeader,
    allocator: std.mem.Allocator,

    pub const State = enum(u8) {
        read_header,
        read_payload,
        frame_ready,
    };

    pub fn init(allocator: std.mem.Allocator) FrameReader {
        return .{
            .state = .read_header,
            .header_buf = undefined,
            .header_bytes_read = 0,
            .payload_buf = null,
            .payload_bytes_read = 0,
            .current_header = null,
            .allocator = allocator,
        };
    }

    pub fn reset(self: *FrameReader) void {
        if (self.payload_buf) |buf| self.allocator.free(buf);
        self.* = init(self.allocator);
    }

    /// Feed received bytes into the reader. Returns the number of bytes
    /// consumed and whether a complete frame is now available.
    pub fn feed(self: *FrameReader, data: []const u8) !struct { consumed: u32, frame_ready: bool } {
        var offset: u32 = 0;
        var ready = false;

        while (offset < data.len and !ready) {
            switch (self.state) {
                .read_header => {
                    const need = FrameHeader.size - self.header_bytes_read;
                    const avail = @as(u32, @intCast(data.len)) - offset;
                    const take = @min(need, avail);
                    const dst_start = self.header_bytes_read;
                    @memcpy(
                        self.header_buf[dst_start..][0..take],
                        data[offset..][0..take],
                    );
                    self.header_bytes_read += @intCast(take);
                    offset += take;

                    if (self.header_bytes_read == FrameHeader.size) {
                        const header = FrameHeader.fromBytes(&self.header_buf);
                        try validateFrameLength(header.frame_length);
                        self.current_header = header;

                        const payload_len = header.payloadLength();
                        if (payload_len == 0) {
                            self.state = .frame_ready;
                            ready = true;
                        } else {
                            self.payload_buf = try self.allocator.alloc(u8, payload_len);
                            self.payload_bytes_read = 0;
                            self.state = .read_payload;
                        }
                    }
                },
                .read_payload => {
                    const header = self.current_header.?;
                    const payload_len = header.payloadLength();
                    const need = payload_len - self.payload_bytes_read;
                    const avail = @as(u32, @intCast(data.len)) - offset;
                    const take = @min(need, avail);
                    const dst_start = self.payload_bytes_read;
                    @memcpy(
                        self.payload_buf.?[dst_start..][0..take],
                        data[offset..][0..take],
                    );
                    self.payload_bytes_read += take;
                    offset += take;

                    if (self.payload_bytes_read == payload_len) {
                        self.state = .frame_ready;
                        ready = true;
                    }
                },
                .frame_ready => {
                    ready = true;
                },
            }
        }

        return .{ .consumed = offset, .frame_ready = ready };
    }

    /// Retrieve the completed frame. Caller takes ownership of the payload buffer.
    pub fn takeFrame(self: *FrameReader) struct { header: FrameHeader, payload: ?[]u8 } {
        std.debug.assert(self.state == .frame_ready);
        const result = .{
            .header = self.current_header.?,
            .payload = self.payload_buf,
        };
        // Reset for next frame without freeing the payload (caller owns it).
        self.state = .read_header;
        self.header_bytes_read = 0;
        self.payload_buf = null;
        self.payload_bytes_read = 0;
        self.current_header = null;
        return result;
    }
};

fn validateFrameLength(frame_length: u32) !void {
    if (frame_length < FrameHeader.min_frame_length) {
        return error.FrameTooSmall;
    }
    if (frame_length > FrameHeader.max_frame_length) {
        return error.FrameTooLarge;
    }
}
```

### 8.4 Partial Write State Machine

Outbound frames may not be fully written in a single `send()` call, especially
under backpressure. The `FrameWriter` tracks how much of the current frame has
been sent:

```zig
pub const FrameWriter = struct {
    state: State,
    header_buf: [FrameHeader.size]u8,
    header_bytes_sent: u8,
    payload: ?[]const u8,
    payload_bytes_sent: u32,

    pub const State = enum(u8) {
        idle,
        write_header,
        write_payload,
    };

    pub fn init() FrameWriter {
        return .{
            .state = .idle,
            .header_buf = undefined,
            .header_bytes_sent = 0,
            .payload = null,
            .payload_bytes_sent = 0,
        };
    }

    pub fn reset(self: *FrameWriter) void {
        self.* = init();
    }

    /// Begin writing a new frame. The header is serialized immediately.
    pub fn beginFrame(self: *FrameWriter, header: FrameHeader, payload: ?[]const u8) void {
        std.debug.assert(self.state == .idle);
        self.header_buf = header.toBytes();
        self.header_bytes_sent = 0;
        self.payload = payload;
        self.payload_bytes_sent = 0;
        self.state = .write_header;
    }

    /// Get the next slice of bytes to send. Returns null when the frame
    /// is fully written.
    pub fn pendingBytes(self: *FrameWriter) ?[]const u8 {
        return switch (self.state) {
            .idle => null,
            .write_header => self.header_buf[self.header_bytes_sent..],
            .write_payload => if (self.payload) |p|
                p[self.payload_bytes_sent..]
            else
                null,
        };
    }

    /// Record that `n` bytes were successfully sent.
    pub fn advance(self: *FrameWriter, n: u32) void {
        switch (self.state) {
            .write_header => {
                self.header_bytes_sent += @intCast(n);
                if (self.header_bytes_sent == FrameHeader.size) {
                    if (self.payload != null and self.payload.?.len > 0) {
                        self.state = .write_payload;
                    } else {
                        self.state = .idle;
                    }
                }
            },
            .write_payload => {
                self.payload_bytes_sent += n;
                const total = if (self.payload) |p| @as(u32, @intCast(p.len)) else 0;
                if (self.payload_bytes_sent == total) {
                    self.state = .idle;
                }
            },
            .idle => unreachable,
        }
    }

    pub fn isIdle(self: *const FrameWriter) bool {
        return self.state == .idle;
    }
};
```

### 8.5 Frame Validation

Every received frame header is validated before the payload is read:

| Check | Condition | Error |
|-------|-----------|-------|
| Minimum length | `frame_length >= 24` | `FrameTooSmall` |
| Maximum length | `frame_length <= 1,048,576` | `FrameTooLarge` |
| Reserved fields | `reserved_1 == 0 && reserved_2 == 0` | `InvalidReservedField` |
| Source node | `source_node_id != 0` (valid node) | `InvalidSourceNode` |

```zig
pub fn validateHeader(header: FrameHeader) !void {
    try validateFrameLength(header.frame_length);
    if (header.reserved_1 != 0 or header.reserved_2 != 0) {
        return error.InvalidReservedField;
    }
    if (header.source_node_id == 0) {
        return error.InvalidSourceNode;
    }
}
```

### 8.6 Protocol Errors

Any framing error is **fatal to the connection**. The connection manager
immediately transitions the slot to `DRAINING` and closes the TCP socket. There
is no error recovery within a single TCP stream — the peer must reconnect.

This design is intentional. Framing errors indicate either a bug, a network
corruption event, or a non-RingLoom peer. Attempting to resynchronize a corrupted
byte stream is complex, fragile, and unnecessary when reconnection is cheap.

| Error | Source | Action |
|-------|--------|--------|
| `FrameTooSmall` | `frame_length < 24` | Close connection |
| `FrameTooLarge` | `frame_length > 1 MB` | Close connection |
| `InvalidReservedField` | Non-zero reserved bytes | Close connection |
| `InvalidSourceNode` | `source_node_id == 0` | Close connection |
| `InvalidMagic` (handshake) | Bad magic number | Close connection |
| `UnsupportedProtocolVersion` | Version mismatch | Close connection |

---

## 9. TCP Socket Configuration

Every TCP socket — both listening and connected — is configured with a standard
set of options for low-latency, high-throughput messaging.

### 9.1 Socket Options Table

| Option | Value | Purpose |
|--------|-------|---------|
| `TCP_NODELAY` | `1` (enabled) | Disable Nagle's algorithm. Frames are sent immediately without waiting to coalesce small writes. Critical for latency. |
| `SO_SNDBUF` | `262,144` (256 KB) | Set kernel send buffer size. Large enough to absorb bursts without blocking. |
| `SO_RCVBUF` | `262,144` (256 KB) | Set kernel receive buffer size. Matches send buffer for symmetric throughput. |
| `TCP_USER_TIMEOUT` | `5000` (5 s) | Abort connection if transmitted data is unacknowledged for 5 seconds. Faster failure detection than default TCP retransmits. Linux only. |
| `SO_KEEPALIVE` | `1` (enabled) | Enable TCP keepalive probes. Detects dead peers when the connection is idle. |
| Non-blocking | `O_NONBLOCK` | All sockets are non-blocking. I/O operations return immediately with `EWOULDBLOCK` if they cannot complete. |

### 9.2 Configuration Code

**File: `src/ringloom_tcp/socket_config.zig`**

```zig
const std = @import("std");
const posix = std.posix;

pub const SocketConfig = struct {
    send_buffer_size: u32 = 262_144,
    recv_buffer_size: u32 = 262_144,
    tcp_user_timeout_ms: u32 = 5_000,
    enable_keepalive: bool = true,
    enable_nodelay: bool = true,

    pub fn apply(self: SocketConfig, fd: posix.fd_t) !void {
        // TCP_NODELAY — disable Nagle.
        if (self.enable_nodelay) {
            try posix.setsockopt(fd, posix.IPPROTO.TCP, posix.TCP.NODELAY, &std.mem.toBytes(@as(c_int, 1)));
        }

        // SO_SNDBUF
        try posix.setsockopt(fd, posix.SOL.SOCKET, posix.SO.SNDBUF, &std.mem.toBytes(@as(c_int, @intCast(self.send_buffer_size))));

        // SO_RCVBUF
        try posix.setsockopt(fd, posix.SOL.SOCKET, posix.SO.RCVBUF, &std.mem.toBytes(@as(c_int, @intCast(self.recv_buffer_size))));

        // SO_KEEPALIVE
        if (self.enable_keepalive) {
            try posix.setsockopt(fd, posix.SOL.SOCKET, posix.SO.KEEPALIVE, &std.mem.toBytes(@as(c_int, 1)));
        }

        // TCP_USER_TIMEOUT — Linux only.
        if (@import("builtin").os.tag == .linux) {
            const TCP_USER_TIMEOUT = 18;
            try posix.setsockopt(fd, posix.IPPROTO.TCP, TCP_USER_TIMEOUT, &std.mem.toBytes(@as(c_int, @intCast(self.tcp_user_timeout_ms))));
        }

        // Set non-blocking.
        const flags = try posix.fcntl(fd, .F_GETFL);
        _ = try posix.fcntl(fd, .F_SETFL, .{ .flags = @as(u32, @bitCast(flags)) | @as(u32, @bitCast(posix.O{ .NONBLOCK = true })) });
    }
};
```

---

## 10. Library API Surface

The public API is a single `TcpTransport` struct that combines the I/O engine,
connection manager, and framing layer into a cohesive interface.

### 10.1 `TcpTransport` Struct

**File: `src/ringloom_tcp/transport.zig`**

```zig
const std = @import("std");
const ConnectionManager = @import("connection_manager.zig").ConnectionManager;
const FrameHeader = @import("frame.zig").FrameHeader;
const ConnectionHandle = @import("io_engine.zig").ConnectionHandle;
const Completion = @import("io_engine.zig").Completion;
const SocketConfig = @import("socket_config.zig").SocketConfig;

pub fn TcpTransportImpl(comptime Engine: type) type {
    return struct {
        const Self = @This();

        engine: Engine,
        conn_mgr: ConnectionManager,
        allocator: std.mem.Allocator,
        local_node_id: u8,
        group_name_hash: u32,
        session_epoch: u64,
        socket_config: SocketConfig,

        pub fn init(
            allocator: std.mem.Allocator,
            config: Config,
        ) !Self {
            var engine = try Engine.init(allocator, config.max_connections);
            errdefer engine.deinit();

            const conn_mgr = ConnectionManager.init(allocator, config);

            return .{
                .engine = engine,
                .conn_mgr = conn_mgr,
                .allocator = allocator,
                .local_node_id = config.local_node_id,
                .group_name_hash = config.group_name_hash,
                .session_epoch = config.session_epoch,
                .socket_config = config.socket_config,
            };
        }

        pub fn deinit(self: *Self) void {
            self.conn_mgr.deinit();
            self.engine.deinit();
        }

        pub const Config = struct {
            local_node_id: u8,
            group_name_hash: u32,
            session_epoch: u64,
            max_connections: u16 = 1024,
            socket_config: SocketConfig = .{},
            idle_timeout_ns: i128 = 5 * std.time.ns_per_s,
            handshake_timeout_ns: i128 = 2 * std.time.ns_per_s,
            drain_timeout_ns: i128 = 1 * std.time.ns_per_s,
            heartbeat_interval_ns: i128 = 500 * std.time.ns_per_ms,
        };

        // Methods defined below...
    };
}
```

### 10.2 Lifecycle Methods

```zig
        /// Start listening for inbound peer connections.
        pub fn listen(self: *Self, address: std.net.Address) !void {
            const fd = try std.posix.socket(
                address.any.family,
                .{ .STREAM = true, .NONBLOCK = true },
                0,
            );
            errdefer std.posix.close(fd);

            try std.posix.setsockopt(fd, std.posix.SOL.SOCKET, std.posix.SO.REUSEADDR, &std.mem.toBytes(@as(c_int, 1)));
            try std.posix.bind(fd, &address.any, address.getOsSockAddr().len);
            try std.posix.listen(fd, 128);

            try self.socket_config.apply(fd);
            try self.engine.submit_accept(fd);
        }

        /// Initiate an outbound connection to a peer broker.
        pub fn connect(self: *Self, peer_node_id: u8, address: std.net.Address) !ConnectionHandle {
            const handle = try self.conn_mgr.allocateSlot();
            var slot = &self.conn_mgr.slots[handle.toIndex()];
            slot.peer_node_id = peer_node_id;
            slot.state = .connecting;

            const fd = try std.posix.socket(
                address.any.family,
                .{ .STREAM = true, .NONBLOCK = true },
                0,
            );
            try self.socket_config.apply(fd);
            try self.engine.submit_connect(handle, address);

            return handle;
        }

        /// Close a peer connection gracefully.
        pub fn closePeer(self: *Self, handle: ConnectionHandle) void {
            self.conn_mgr.beginDrain(&self.conn_mgr.slots[handle.toIndex()]);
        }
```

### 10.3 I/O Methods

```zig
        /// Send a framed message to a connected peer.
        pub fn send(
            self: *Self,
            handle: ConnectionHandle,
            header: FrameHeader,
            payload: ?[]const u8,
        ) !void {
            const slot = &self.conn_mgr.slots[handle.toIndex()];
            if (slot.state != .connected) return error.NotConnected;

            slot.write_state.beginFrame(header, payload);
            if (slot.write_state.pendingBytes()) |bytes| {
                try self.engine.submit_send(handle, bytes);
            }
            slot.last_send_time_ns = self.conn_mgr.clock.nanotime();
        }

        /// Poll for I/O events and process completions. Returns the number
        /// of completions processed.
        pub fn poll(self: *Self, recv_callback: *const fn (ConnectionHandle, FrameHeader, ?[]u8) void) u32 {
            var completions: [256]Completion = undefined;
            const n = self.engine.harvest(&completions);

            for (completions[0..n]) |c| {
                switch (c.op) {
                    .accept => self.conn_mgr.handleAcceptCompletion(c) catch |err| {
                        std.log.err("accept error: {}", .{err});
                    },
                    .connect => self.handleConnectCompletion(c),
                    .recv => self.handleRecvCompletion(c, recv_callback),
                    .send => self.handleSendCompletion(c),
                    .close => self.conn_mgr.handleCloseCompletion(c),
                }
            }

            return n;
        }

        fn handleConnectCompletion(self: *Self, c: Completion) void {
            var slot = &self.conn_mgr.slots[c.handle.toIndex()];
            switch (c.result) {
                .ok => {
                    slot.state = .handshake;
                    // Send our handshake frame.
                    const hs = HandshakeFrame{
                        .source_node_id = self.local_node_id,
                        .target_node_id = slot.peer_node_id,
                        .direction = .outbound,
                        .session_epoch = self.session_epoch,
                        .group_name_hash = self.group_name_hash,
                    };
                    const bytes = hs.toBytes();
                    self.engine.submit_send(c.handle, &bytes) catch {
                        self.conn_mgr.forceClose(slot);
                    };
                    // Submit recv for the peer's handshake response.
                    self.engine.submit_recv(c.handle, slot.read_state.currentBuffer()) catch {
                        self.conn_mgr.forceClose(slot);
                    };
                },
                .err => {
                    self.conn_mgr.scheduleReconnect(slot);
                },
                .eof => unreachable,
            }
        }

        fn handleRecvCompletion(
            self: *Self,
            c: Completion,
            callback: *const fn (ConnectionHandle, FrameHeader, ?[]u8) void,
        ) void {
            var slot = &self.conn_mgr.slots[c.handle.toIndex()];
            switch (c.result) {
                .ok => |ok| {
                    slot.last_recv_time_ns = self.conn_mgr.clock.nanotime();
                    switch (slot.state) {
                        .handshake => self.processHandshakeRecv(slot, c.handle, ok.bytes),
                        .connected => self.processFrameRecv(slot, c.handle, ok.bytes, callback),
                        else => {},
                    }
                },
                .eof => {
                    self.conn_mgr.beginDrain(slot);
                },
                .err => {
                    self.conn_mgr.forceClose(slot);
                },
            }
        }

        fn handleSendCompletion(self: *Self, c: Completion) void {
            var slot = &self.conn_mgr.slots[c.handle.toIndex()];
            switch (c.result) {
                .ok => |ok| {
                    slot.write_state.advance(ok.bytes);
                    // If more bytes remain, submit another send.
                    if (slot.write_state.pendingBytes()) |remaining| {
                        self.engine.submit_send(c.handle, remaining) catch {
                            self.conn_mgr.forceClose(slot);
                        };
                    }
                },
                .err => {
                    self.conn_mgr.forceClose(slot);
                },
                .eof => {
                    self.conn_mgr.beginDrain(slot);
                },
            }
        }
```

### 10.4 Usage Example

```zig
const std = @import("std");
const ringloom_tcp = @import("ringloom_tcp");
const TcpTransport = ringloom_tcp.TcpTransport;
const FrameHeader = ringloom_tcp.FrameHeader;
const HandshakeFrame = ringloom_tcp.HandshakeFrame;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var transport = try TcpTransport.init(allocator, .{
        .local_node_id = 1,
        .group_name_hash = HandshakeFrame.hashGroupName("my-cluster"),
        .session_epoch = @intCast(std.time.nanoTimestamp()),
    });
    defer transport.deinit();

    // Listen for inbound connections.
    const listen_addr = try std.net.Address.parseIp4("0.0.0.0", 9100);
    try transport.listen(listen_addr);

    // Connect to a peer.
    const peer_addr = try std.net.Address.parseIp4("192.168.1.2", 9100);
    const peer = try transport.connect(2, peer_addr);

    // Main event loop.
    while (true) {
        _ = transport.poll(handleMessage);
        transport.conn_mgr.checkHealth(std.time.nanoTimestamp());
    }
}

fn handleMessage(handle: ringloom_tcp.ConnectionHandle, header: FrameHeader, payload: ?[]u8) void {
    if (header.isHeartbeat()) return;
    // Process application message...
    _ = handle;
    _ = payload;
}
```

---

## 11. Build Integration

`ringloom_tcp` is compiled as a separate Zig module. The broker and tests import it
as a dependency.

### 11.1 Module Definition

**File: `build.zig`** (relevant excerpt)

```zig
const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Build options for backend selection.
    const options = b.addOptions();
    const backend = b.option(
        TcpBackend,
        "tcp-backend",
        "TCP I/O backend (default: auto-detect from target OS)",
    ) orelse detectBackend(target);

    options.addOption(TcpBackend, "tcp_backend", backend);

    // ringloom_tcp module.
    const ringloom_tcp = b.addModule("ringloom_tcp", .{
        .root_source_file = b.path("lib/ringloom_tcp/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    ringloom_tcp.addOptions("build_options", options);

    // Broker executable depends on ringloom_tcp.
    const broker = b.addExecutable(.{
        .name = "ringloom-broker",
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });
    broker.root_module.addImport("ringloom_tcp", ringloom_tcp);
    b.installArtifact(broker);
}

const TcpBackend = enum {
    io_uring,
    kqueue,
};

fn detectBackend(target: std.Build.ResolvedTarget) TcpBackend {
    return switch (target.result.os.tag) {
        .linux => .io_uring,
        .macos, .ios => .kqueue,
        else => .io_uring, // Default to io_uring.
    };
}
```

### 11.2 Compile-Time Backend Selection

**File: `lib/ringloom_tcp/root.zig`**

```zig
const std = @import("std");
const build_options = @import("build_options");

pub const io_engine = @import("io_engine.zig");
pub const frame = @import("frame.zig");
pub const handshake = @import("handshake.zig");
pub const connection_manager = @import("connection_manager.zig");
pub const socket_config = @import("socket_config.zig");
pub const transport = @import("transport.zig");

/// The I/O engine type selected at compile time.
pub const Engine = switch (build_options.tcp_backend) {
    .io_uring => @import("io_uring_engine.zig").IoUringEngine,
    .kqueue => @import("kqueue_engine.zig").KqueueEngine,
};

/// The concrete TcpTransport type for the selected backend.
pub const TcpTransport = transport.TcpTransportImpl(Engine);

// Re-export commonly used types.
pub const FrameHeader = frame.FrameHeader;
pub const FrameReader = frame.FrameReader;
pub const FrameWriter = frame.FrameWriter;
pub const HandshakeFrame = handshake.HandshakeFrame;
pub const ConnectionHandle = io_engine.ConnectionHandle;
pub const Completion = io_engine.Completion;
pub const ConnectionState = connection_manager.ConnectionState;
pub const SocketConfig = socket_config.SocketConfig;
```

### 11.3 Conditional Compilation

Platform-specific code uses `@import("builtin")` to guard sections:

```zig
const builtin = @import("builtin");

pub fn createEngine(allocator: std.mem.Allocator, max_conns: u16) !Engine {
    if (comptime builtin.os.tag == .linux) {
        return IoUringEngine.init(allocator, max_conns);
    } else if (comptime builtin.os.tag == .macos) {
        return KqueueEngine.init(allocator, max_conns);
    } else {
        @compileError("Unsupported OS for ringloom_tcp");
    }
}
```

In tests, a mock engine can be injected without touching the build system:

```zig
const MockEngine = struct {
    completions_to_return: []const Completion = &.{},
    submitted_ops: std.ArrayList(SubmittedOp),

    pub const SubmittedOp = struct {
        handle: ConnectionHandle,
        op: enum { recv, send, accept, connect, close },
    };

    pub fn init(allocator: std.mem.Allocator, max_conns: u16) !MockEngine {
        _ = max_conns;
        return .{
            .submitted_ops = std.ArrayList(SubmittedOp).init(allocator),
        };
    }

    pub fn deinit(self: *MockEngine) void {
        self.submitted_ops.deinit();
    }

    pub fn submit_recv(self: *MockEngine, handle: ConnectionHandle, buf: []u8) !void {
        _ = buf;
        try self.submitted_ops.append(.{ .handle = handle, .op = .recv });
    }

    pub fn submit_send(self: *MockEngine, handle: ConnectionHandle, data: []const u8) !void {
        _ = data;
        try self.submitted_ops.append(.{ .handle = handle, .op = .send });
    }

    pub fn submit_accept(self: *MockEngine, listen_fd: std.posix.fd_t) !void {
        _ = listen_fd;
        try self.submitted_ops.append(.{ .handle = .invalid, .op = .accept });
    }

    pub fn submit_connect(self: *MockEngine, handle: ConnectionHandle, addr: std.net.Address) !void {
        _ = addr;
        try self.submitted_ops.append(.{ .handle = handle, .op = .connect });
    }

    pub fn submit_close(self: *MockEngine, handle: ConnectionHandle) !void {
        try self.submitted_ops.append(.{ .handle = handle, .op = .close });
    }

    pub fn harvest(self: *MockEngine, completions: []Completion) u32 {
        const n = @min(self.completions_to_return.len, completions.len);
        @memcpy(completions[0..n], self.completions_to_return[0..n]);
        return @intCast(n);
    }
};
```

### 11.4 Test Targets

**File: `build.zig`** (test targets excerpt)

```zig
    // Unit tests for ringloom_tcp.
    const ringloom_tcp_tests = b.addTest(.{
        .root_source_file = b.path("lib/ringloom_tcp/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    ringloom_tcp_tests.addOptions("build_options", options);

    const run_ringloom_tcp_tests = b.addRunArtifact(ringloom_tcp_tests);
    const test_step = b.step("test-tcp", "Run ringloom_tcp unit tests");
    test_step.dependOn(&run_ringloom_tcp_tests.step);

    // Integration tests (require two sockets).
    const integration_tests = b.addTest(.{
        .root_source_file = b.path("test/ringloom_tcp_integration.zig"),
        .target = target,
        .optimize = optimize,
    });
    integration_tests.root_module.addImport("ringloom_tcp", ringloom_tcp);

    const run_integration = b.addRunArtifact(integration_tests);
    const integration_step = b.step("test-tcp-integration", "Run ringloom_tcp integration tests");
    integration_step.dependOn(&run_integration.step);
```

---

## 12. Testing Strategy

### 12.1 Unit Tests — Framing

Frame encoding and decoding are tested with known byte patterns. The tests
exercise partial reads, boundary conditions, and error cases.

**File: `lib/ringloom_tcp/frame.zig`** (test section)

```zig
const testing = std.testing;

test "FrameHeader roundtrip" {
    const header = FrameHeader{
        .frame_length = 48,
        .flags = 0,
        .source_node_id = 1,
        .target_node_id = 2,
        .source_service_id = 100,
        .target_service_id = 200,
        .template_id = 42,
        .correlation_id = 0x1234_5678_9ABC_DEF0,
    };
    const bytes = header.toBytes();
    const decoded = FrameHeader.fromBytes(&bytes);

    try testing.expectEqual(header.frame_length, decoded.frame_length);
    try testing.expectEqual(header.source_node_id, decoded.source_node_id);
    try testing.expectEqual(header.target_node_id, decoded.target_node_id);
    try testing.expectEqual(header.source_service_id, decoded.source_service_id);
    try testing.expectEqual(header.target_service_id, decoded.target_service_id);
    try testing.expectEqual(header.template_id, decoded.template_id);
    try testing.expectEqual(header.correlation_id, decoded.correlation_id);
}

test "FrameHeader size is 24 bytes" {
    try testing.expectEqual(@as(usize, 24), @sizeOf(FrameHeader));
}

test "FrameReader partial header" {
    var reader = FrameReader.init(testing.allocator);
    defer reader.reset();

    // Feed first 10 bytes of a 24-byte header.
    const header = FrameHeader{
        .frame_length = 24,
        .flags = FrameHeader.Flags.heartbeat,
        .source_node_id = 1,
        .target_node_id = 2,
        .source_service_id = 0,
        .target_service_id = 0,
        .template_id = 0,
        .correlation_id = 0,
    };
    const bytes = header.toBytes();

    const r1 = try reader.feed(bytes[0..10]);
    try testing.expectEqual(@as(u32, 10), r1.consumed);
    try testing.expect(!r1.frame_ready);

    const r2 = try reader.feed(bytes[10..]);
    try testing.expectEqual(@as(u32, 14), r2.consumed);
    try testing.expect(r2.frame_ready);

    const frame = reader.takeFrame();
    try testing.expectEqual(@as(u32, 24), frame.header.frame_length);
    try testing.expect(frame.header.isHeartbeat());
    try testing.expectEqual(@as(?[]u8, null), frame.payload);
}

test "FrameReader rejects frame too large" {
    var reader = FrameReader.init(testing.allocator);
    defer reader.reset();

    var header_bytes: [24]u8 = undefined;
    // Set frame_length to 2 MB (exceeds 1 MB max).
    std.mem.writeInt(u32, header_bytes[0..4], 2_097_152, .little);
    @memset(header_bytes[4..], 0);
    header_bytes[5] = 1; // source_node_id

    const result = reader.feed(&header_bytes);
    try testing.expectError(error.FrameTooLarge, result);
}

test "FrameWriter partial send" {
    var writer = FrameWriter.init();

    const header = FrameHeader{
        .frame_length = 28,
        .flags = 0,
        .source_node_id = 1,
        .target_node_id = 2,
        .source_service_id = 0,
        .target_service_id = 0,
        .template_id = 0,
        .correlation_id = 0,
    };
    const payload = [_]u8{ 0xDE, 0xAD, 0xBE, 0xEF };
    writer.beginFrame(header, &payload);

    // Simulate partial send of header (10 bytes).
    try testing.expect(!writer.isIdle());
    try testing.expect(writer.pendingBytes() != null);
    writer.advance(10);
    try testing.expect(!writer.isIdle());

    // Send remaining header (14 bytes).
    writer.advance(14);
    // Now should be writing payload.
    try testing.expect(!writer.isIdle());

    // Send payload (4 bytes).
    writer.advance(4);
    try testing.expect(writer.isIdle());
}
```

### 12.2 Unit Tests — Handshake

```zig
test "HandshakeFrame roundtrip" {
    const frame = HandshakeFrame{
        .source_node_id = 1,
        .target_node_id = 2,
        .direction = .outbound,
        .session_epoch = 1_700_000_000_000_000_000,
        .group_name_hash = HandshakeFrame.hashGroupName("test-cluster"),
    };

    const bytes = frame.toBytes();
    const decoded = HandshakeFrame.fromBytes(&bytes);

    try testing.expectEqual(frame.magic, decoded.magic);
    try testing.expectEqual(frame.source_node_id, decoded.source_node_id);
    try testing.expectEqual(frame.target_node_id, decoded.target_node_id);
    try testing.expectEqual(frame.session_epoch, decoded.session_epoch);
    try testing.expectEqual(frame.group_name_hash, decoded.group_name_hash);
}

test "HandshakeFrame size is 24 bytes" {
    try testing.expectEqual(@as(usize, 24), @sizeOf(HandshakeFrame));
}

test "HandshakeFrame validation rejects wrong magic" {
    var frame = HandshakeFrame{
        .source_node_id = 1,
        .target_node_id = 2,
        .direction = .outbound,
        .session_epoch = 1_000,
        .group_name_hash = 0,
    };
    frame.magic = 0xDEADBEEF;

    const bytes = frame.toBytes();
    const decoded = HandshakeFrame.fromBytes(&bytes);
    try testing.expectError(error.InvalidMagic, HandshakeFrame.validate(decoded, 2, 0));
}

test "HandshakeFrame validation rejects self-connection" {
    const frame = HandshakeFrame{
        .source_node_id = 1,
        .target_node_id = 1,
        .direction = .outbound,
        .session_epoch = 1_000,
        .group_name_hash = 0,
    };
    try testing.expectError(error.SelfConnection, HandshakeFrame.validate(frame, 1, 0));
}

test "HandshakeFrame FNV-1a hash" {
    const hash1 = HandshakeFrame.hashGroupName("cluster-a");
    const hash2 = HandshakeFrame.hashGroupName("cluster-b");
    try testing.expect(hash1 != hash2);

    // Same input produces same hash.
    const hash3 = HandshakeFrame.hashGroupName("cluster-a");
    try testing.expectEqual(hash1, hash3);
}

test "Handshake validation rejects group mismatch" {
    const frame = HandshakeFrame{
        .source_node_id = 1,
        .target_node_id = 2,
        .direction = .outbound,
        .session_epoch = 1_000,
        .group_name_hash = HandshakeFrame.hashGroupName("cluster-a"),
    };
    const expected_hash = HandshakeFrame.hashGroupName("cluster-b");
    try testing.expectError(error.GroupMismatch, HandshakeFrame.validate(frame, 2, expected_hash));
}
```

### 12.3 Integration Tests

Integration tests create real TCP connections (loopback) and verify end-to-end
behavior:

**File: `test/ringloom_tcp_integration.zig`**

```zig
const std = @import("std");
const ringloom_tcp = @import("ringloom_tcp");
const TcpTransport = ringloom_tcp.TcpTransport;
const FrameHeader = ringloom_tcp.FrameHeader;
const HandshakeFrame = ringloom_tcp.HandshakeFrame;

test "two transports can connect and exchange messages" {
    const allocator = std.testing.allocator;
    const group_hash = HandshakeFrame.hashGroupName("test-cluster");
    const epoch = @as(u64, @intCast(std.time.nanoTimestamp()));

    // Node 1 — listener.
    var t1 = try TcpTransport.init(allocator, .{
        .local_node_id = 1,
        .group_name_hash = group_hash,
        .session_epoch = epoch,
    });
    defer t1.deinit();

    const listen_addr = try std.net.Address.parseIp4("127.0.0.1", 0);
    try t1.listen(listen_addr);

    // Node 2 — connector.
    var t2 = try TcpTransport.init(allocator, .{
        .local_node_id = 2,
        .group_name_hash = group_hash,
        .session_epoch = epoch,
    });
    defer t2.deinit();

    const peer = try t2.connect(1, listen_addr);

    // Poll until connected.
    var attempts: u32 = 0;
    while (attempts < 1000) : (attempts += 1) {
        _ = t1.poll(noopCallback);
        _ = t2.poll(noopCallback);
        if (t2.conn_mgr.slots[peer.toIndex()].state == .connected) break;
    }
    try std.testing.expect(
        t2.conn_mgr.slots[peer.toIndex()].state == .connected,
    );

    // Send a message from node 2 to node 1.
    const payload = "Hello, node 1!";
    try t2.send(peer, .{
        .frame_length = @intCast(FrameHeader.size + payload.len),
        .flags = 0,
        .source_node_id = 2,
        .target_node_id = 1,
        .source_service_id = 10,
        .target_service_id = 20,
        .template_id = 1,
        .correlation_id = 42,
    }, payload);

    // Poll node 1 until the message arrives.
    var received = false;
    attempts = 0;
    while (attempts < 1000) : (attempts += 1) {
        _ = t1.poll(struct {
            fn cb(_: ringloom_tcp.ConnectionHandle, header: FrameHeader, _payload: ?[]u8) void {
                if (header.correlation_id == 42) {
                    received = true;
                }
            }
        }.cb);
        if (received) break;
    }
    try std.testing.expect(received);
}

fn noopCallback(_: ringloom_tcp.ConnectionHandle, _: FrameHeader, _: ?[]u8) void {}
```

### 12.4 Platform-Specific Tests

Platform-specific tests verify that the I/O engine works correctly on the host OS:

| Test Category | Scope | Run Condition |
|--------------|-------|---------------|
| io\_uring SQE/CQE | Submit and harvest one recv | Linux only |
| io\_uring multishot accept | Accept multiple connections in one SQE | Linux only |
| kqueue edge-triggered | Verify EV\_CLEAR semantics | macOS only |
| kqueue accept loop | Accept until EWOULDBLOCK | macOS only |
| Socket options | Verify TCP\_NODELAY, buffer sizes | All platforms |
| Backoff timing | Verify exponential backoff schedule | All platforms |
| Handshake timeout | Verify 2 s handshake deadline | All platforms |

```zig
test "io_uring: submit and harvest recv" {
    if (@import("builtin").os.tag != .linux) return error.SkipZigTest;

    const allocator = std.testing.allocator;
    var engine = try @import("io_uring_engine.zig").IoUringEngine.init(allocator, 16);
    defer engine.deinit();

    // Create a socketpair for testing.
    const fds = try std.posix.socketpair(.{ .STREAM = true }, 0);
    defer std.posix.close(fds[0]);
    defer std.posix.close(fds[1]);

    // Register fd[0] as handle 0.
    engine.fd_table[0] = fds[0];
    const handle: @import("io_engine.zig").ConnectionHandle = @enumFromInt(0);

    // Write some data on fd[1].
    _ = try std.posix.write(fds[1], "hello");

    // Submit recv on fd[0].
    var buf: [32]u8 = undefined;
    try engine.submit_recv(handle, &buf);

    // Harvest.
    var completions: [16]@import("io_engine.zig").Completion = undefined;
    const n = engine.harvest(&completions);
    try std.testing.expect(n >= 1);
    try std.testing.expectEqual(@import("io_engine.zig").Completion.OpType.recv, completions[0].op);
}

test "exponential backoff schedule" {
    const cm = @import("connection_manager.zig");
    try std.testing.expectEqual(@as(u64, 0), cm.computeBackoffMs(0));
    try std.testing.expectEqual(@as(u64, 100), cm.computeBackoffMs(1));
    try std.testing.expectEqual(@as(u64, 200), cm.computeBackoffMs(2));
    try std.testing.expectEqual(@as(u64, 400), cm.computeBackoffMs(3));
    try std.testing.expectEqual(@as(u64, 800), cm.computeBackoffMs(4));
    try std.testing.expectEqual(@as(u64, 1000), cm.computeBackoffMs(5));
    try std.testing.expectEqual(@as(u64, 1000), cm.computeBackoffMs(10));
}
```

---

## Appendix A — Wire Byte-Order Reference

All wire formats in `ringloom_tcp` use **little-endian** byte order. This matches the
native byte order of x86-64 and ARM64 (in LE mode), which are the only supported
target architectures. No byte-swapping is needed on these platforms.

| Type | Wire Size | Byte Order | Zig Read |
|------|-----------|-----------|----------|
| `u8` | 1 | N/A | Direct read |
| `u16` | 2 | Little-endian | `@bitCast` from packed struct |
| `u32` | 4 | Little-endian | `@bitCast` from packed struct |
| `u64` | 8 | Little-endian | `@bitCast` from packed struct |
| `i64` | 8 | Little-endian | `@bitCast` from packed struct |

For interoperability with big-endian systems (not currently supported), the packed
struct `toBytes`/`fromBytes` would need to be replaced with explicit
`std.mem.writeInt` / `std.mem.readInt` calls specifying `.little`.

---

## Appendix B — Error Code Catalogue

| Code | Source | Meaning |
|------|--------|---------|
| `InvalidMagic` | Handshake | Received frame does not start with `0x474E4952`. |
| `UnsupportedProtocolVersion` | Handshake | Peer uses a different protocol version. |
| `WrongTargetNode` | Handshake | `target_node_id` does not match local node. |
| `SelfConnection` | Handshake | `source_node_id` equals local node (loop). |
| `GroupMismatch` | Handshake | FNV-1a group hash does not match. |
| `InvalidReservedField` | Handshake / Frame | Reserved bytes are non-zero. |
| `StaleConnection` | Epoch check | Peer's `session_epoch` is older than stored. |
| `FrameTooSmall` | Frame validation | `frame_length < 24`. |
| `FrameTooLarge` | Frame validation | `frame_length > 1,048,576`. |
| `InvalidSourceNode` | Frame validation | `source_node_id == 0`. |
| `NotConnected` | Send | Slot is not in `CONNECTED` state. |
| `SlotsFull` | Connection manager | All connection slots are in use. |
| `AcceptFailed` | I/O engine | Kernel `accept()` returned an error. |
| `SubmissionQueueFull` | io\_uring | SQE ring is full; flush or increase size. |

---

## Appendix C — Configuration Defaults

| Parameter | Default | Unit | Override |
|-----------|---------|------|----------|
| Max connections | 1024 | — | `Config.max_connections` |
| io\_uring ring entries | 4096 | — | Compile-time constant |
| Recv buffer pool count | 512 | buffers | Compile-time constant |
| Recv buffer size | 4096 | bytes | Compile-time constant |
| `SO_SNDBUF` | 262,144 | bytes | `SocketConfig.send_buffer_size` |
| `SO_RCVBUF` | 262,144 | bytes | `SocketConfig.recv_buffer_size` |
| `TCP_USER_TIMEOUT` | 5,000 | ms | `SocketConfig.tcp_user_timeout_ms` |
| Idle timeout | 5 | seconds | `Config.idle_timeout_ns` |
| Handshake timeout | 2 | seconds | `Config.handshake_timeout_ns` |
| Drain timeout | 1 | second | `Config.drain_timeout_ns` |
| Heartbeat interval | 500 | ms | `Config.heartbeat_interval_ns` |
| Backoff initial | 100 | ms | `backoff_initial_ms` constant |
| Backoff cap | 1,000 | ms | `backoff_max_ms` constant |
| Max frame size | 1,048,576 | bytes | `FrameHeader.max_frame_length` |
| Completion batch size | 256 | completions | Caller-provided slice |

---

*Previous: [03 — Concurrent Data Structures](03-concurrent-data-structures.md)*
·
*Next: [05 — Send Path](05-send-path.md)*
