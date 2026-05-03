# 02 — Memory Layout & Shared Memory

> **Depends on:** [01 — Platform Abstraction](01-platform-abstraction.md) (mmap, atomics, clocks, process queries)
>
> **Depended on by:** [03 — Concurrent Data Structures](03-concurrent-data-structures.md) (ring buffers overlay the regions defined here)

This document specifies every byte of the memory-mapped files that RingLoom uses for
same-host IPC. It covers the three region types — broker metadata, service metadata,
and receive log buffers — along with the file discovery, singleton management, and
per-service caching layers that sit on top.

All code targets **Zig 0.15.x** stable.

---

## Table of Contents

1. [Overview](#1-overview)
2. [Constants](#2-constants)
3. [Broker Metadata File Layout](#3-broker-metadata-file-layout)
4. [Service Metadata File Layout](#4-service-metadata-file-layout)
5. [Receive Log Buffer Layout](#5-receive-log-buffer-layout)
6. [File Discovery — Service Scanner](#6-file-discovery--service-scanner)
7. [Metadata Descriptor Provider (Singleton)](#7-metadata-descriptor-provider-singleton)
8. [Buffers Provider (Per-Service Cache)](#8-buffers-provider-per-service-cache)
9. [Testing](#9-testing)

---

## 1. Overview

RingLoom uses memory-mapped files on `tmpfs` (`/dev/shm` on Linux) for zero-copy IPC.
Every service on a host maps the broker's file to send control and cross-host messages.
The broker maps every service's file to send control responses and route inbound
cross-host messages. No data is copied between userspace and kernel on the hot path —
producers and consumers read and write the same physical pages.

There are three types of memory-mapped regions:

| Region | Backing | Writer(s) | Reader | Purpose |
|--------|---------|-----------|--------|---------|
| **Broker metadata file** | File on `/dev/shm` | All local services (MPSC) | Broker | Control messages (register, subscribe, heartbeat) and outbound cross-host messages |
| **Service metadata file** | File on `/dev/shm` | Broker + peer services (MPSC) | Owning service | Control responses (registration ack, service instances, leader changed) and application messages |

All files are stored at `<storage_path>/<group>/services/`. The default storage path
is `/dev/shm` on Linux (tmpfs — backed by RAM, survives across process restarts until
reboot). The broker file is always named `broker_0.dat`. Service files are named
`<name>_<id>.dat` (e.g. `pricing_3.dat`).

---

## 2. Constants

Define these in a dedicated `constants.zig` (or a `memory_layout.zig` that the rest of
the subsystem imports). Every value is `comptime`.

```zig
// src/memory/constants.zig

/// Hardware cache line. Matches x86-64 and ARM Cortex-A.
pub const cache_line_length: usize = 64;

/// Two cache lines — the minimum padding between independently-written atomics
/// to prevent false sharing.
pub const cache_line_pad: usize = cache_line_length * 2; // 128

/// Metadata header occupies the first 512 bytes of every metadata file.
/// Fields are packed into the first ~160 bytes; the rest is reserved.
pub const metadata_header_length: usize = 512;

/// Heartbeat timestamp starts at offset 256 within the header (cache-line aligned).
pub const heartbeat_offset_within_header: usize = 256;

/// The nextServiceId counter starts at offset 288 within the header (broker only).
pub const next_service_id_offset_within_header: usize = 288;

/// When blocking mode is enabled, three 128-byte cache-line-padded slots are
/// inserted between the header and the ring buffers.
pub const blocking_trailer_length: usize = 3 * cache_line_pad; // 384

/// Receive log buffer metadata (tail_position + rebuild_position + padding).
pub const recv_log_metadata_length: usize = 256;

/// Ring buffer trailer length (defined in doc 03, but the offset math needs it here).
pub const ring_buffer_trailer_length: usize = 768;

/// Record alignment within the ring buffer.
pub const ring_buffer_alignment: usize = 8;

/// Record header: i32 length + i32 msg_type_id.
pub const ring_buffer_record_header_length: usize = 8;

/// Default buffer sizes.
pub const default_control_buffer_length: usize = 64 * 1024; // 64 KB
pub const default_send_buffer_length: usize = 1024 * 1024; // 1 MB
pub const default_messages_buffer_length: usize = 1024 * 1024; // 1 MB

/// Broker is always service ID 0.
pub const broker_service_id: i32 = 0;

/// Broker service name.
pub const broker_service_name: []const u8 = "broker";

/// Memory page size for file alignment.
pub const page_size: usize = 4096;

/// Default heartbeat timeout (ms) — if a service hasn't written a heartbeat
/// within this window, it is considered dead.
pub const default_heartbeat_timeout_ms: i64 = 10_000;

/// Compile-time power-of-two check.
pub fn isPowerOfTwo(v: usize) bool {
    return v > 0 and (v & (v - 1)) == 0;
}

/// Align `value` up to the next multiple of `alignment`.
/// `alignment` must be a power of two.
pub fn alignUp(value: usize, alignment: usize) usize {
    return (value + (alignment - 1)) & ~(alignment - 1);
}
```

---

## 3. Broker Metadata File Layout

### 3.1 Binary Layout

```
Offset 0                                          512 bytes
┌──────────────────────────────────────────────────────────────┐
│  Metadata Header                                             │
│                                                              │
│  +0:    control_buffer_length     (i32)                      │
│  +4:    messages_buffer_length    (i32)                      │
│  +8:    service_id                (i32)  — always 0          │
│  +12:   node_id                   (i16)                      │
│  +14:   padding                   (i16)                      │
│  +16:   pid                       (i64)                      │
│  +24:   start_timestamp_ms        (i64)                      │
│  +32:   reserved                  (224 bytes, zero-filled)   │
│  +256:  heartbeat_time_ms         (volatile i64)  ← atomic   │
│  +264:  padding                   (24 bytes)                 │
│  +288:  next_service_id           (volatile i32)  ← atomic   │
│  +292:  padding                   (220 bytes)                │
├──────────────────────────────────────────────────────────────┤  ← offset 512
│  Control Ring Buffer  (control_buffer_length bytes)          │
│  MPSC: service → broker control messages                     │
│  (register, subscribe, heartbeat, unregister)                │
├──────────────────────────────────────────────────────────────┤
│  Send Ring Buffer  (messages_buffer_length bytes)            │
│  MPSC: service → broker cross-host outbound messages         │
└──────────────────────────────────────────────────────────────┘

Total file size = 512 + control_buffer_length + messages_buffer_length
                  (aligned up to page_size)
```

The `heartbeat_time_ms` and `next_service_id` fields sit on separate cache-line
boundaries within the 512-byte header. This prevents false sharing between the broker
writing its heartbeat and services doing `fetchAdd` on `next_service_id`.

> **Why 512 bytes?** It is a multiple of both the cache-line pad (128) and the page
> size divisor, and provides ample room for future fields without breaking the layout.

### 3.2 Packed Struct

```zig
// src/memory/broker_metadata.zig

const std = @import("std");
const constants = @import("constants.zig");

/// Overlay for the first 32 bytes of the broker metadata header.
/// Read/written once at creation; immutable after that (except volatile fields).
pub const BrokerMetadataHeader = extern struct {
    control_buffer_length: i32,
    messages_buffer_length: i32,
    service_id: i32,
    node_id: i16,
    _padding0: i16 = 0,
    pid: i64,
    start_timestamp_ms: i64,

    // --- Remainder of the 512-byte header is accessed via raw byte offsets ---
    // +32..+255:  reserved (zero)
    // +256..+263: heartbeat_time_ms  (volatile i64)
    // +264..+287: padding
    // +288..+291: next_service_id    (volatile i32)
    // +292..+511: padding

    comptime {
        // The fixed fields must pack to exactly 32 bytes.
        std.debug.assert(@sizeOf(BrokerMetadataHeader) == 32);
    }
};
```

### 3.3 BrokerMetadataFile Struct

```zig
// src/memory/broker_metadata.zig (continued)

const platform = @import("../platform.zig");

pub const BrokerMetadataFile = struct {
    /// The full mmap'd region.
    mapped_bytes: []align(std.heap.page_size_min) u8,

    /// Pointer to the header overlay (first 32 bytes).
    header: *BrokerMetadataHeader,

    /// Byte slice covering the control ring buffer region (includes trailer).
    control_buffer: []u8,

    /// Byte slice covering the send ring buffer region (includes trailer).
    send_buffer: []u8,

    /// File descriptor (kept open for the lifetime of the mapping).
    fd: std.posix.fd_t,

    // ── Construction ──────────────────────────────────────────────────

    /// Create and initialize a new broker metadata file.
    ///
    /// - `storage_path`: e.g. "/dev/shm"
    /// - `group`: e.g. "default"
    /// - `node_id`: this broker's node identifier
    /// - `control_buffer_length`: must be a power of two
    /// - `messages_buffer_length`: must be a power of two
    pub fn create(
        storage_path: []const u8,
        group: []const u8,
        node_id: i16,
        control_buffer_length: usize,
        messages_buffer_length: usize,
    ) !BrokerMetadataFile {
        if (!constants.isPowerOfTwo(control_buffer_length))
            return error.ControlBufferNotPowerOfTwo;
        if (!constants.isPowerOfTwo(messages_buffer_length))
            return error.MessagesBufferNotPowerOfTwo;

        const total_size = constants.alignUp(
            constants.metadata_header_length + control_buffer_length + messages_buffer_length,
            constants.page_size,
        );

        // Build path: <storage_path>/<group>/services/broker_0.dat
        var path_buf: [std.fs.max_path_bytes]u8 = undefined;
        const path = try buildBrokerPath(&path_buf, storage_path, group);

        // Ensure parent directory exists.
        try ensureDirectoryExists(path);

        // Create file, truncate to total_size, mmap.
        const fd = try platform.createFile(path);
        errdefer std.posix.close(fd);
        try platform.ftruncate(fd, total_size);

        const mapped = try platform.mmap(fd, total_size);
        errdefer platform.munmap(mapped);

        // Zero-fill (mmap of a new file on Linux is already zero, but be explicit).
        @memset(mapped, 0);

        var self = BrokerMetadataFile{
            .mapped_bytes = mapped,
            .header = @ptrCast(@alignCast(mapped.ptr)),
            .control_buffer = mapped[constants.metadata_header_length..][0..control_buffer_length],
            .send_buffer = mapped[constants.metadata_header_length + control_buffer_length ..][0..messages_buffer_length],
            .fd = fd,
        };

        // Write the immutable header fields.
        self.header.control_buffer_length = @intCast(control_buffer_length);
        self.header.messages_buffer_length = @intCast(messages_buffer_length);
        self.header.service_id = constants.broker_service_id;
        self.header.node_id = node_id;
        self.header.pid = @intCast(platform.getPid());
        self.header.start_timestamp_ms = platform.epochMillis();

        // Initialize next_service_id to 1 (0 is reserved for broker).
        self.storeNextServiceId(1);

        // Write initial heartbeat.
        self.storeHeartbeat(platform.epochMillis());

        return self;
    }

    /// Open an existing broker metadata file (read-write).
    /// Validates that the header fields are internally consistent.
    pub fn open(
        storage_path: []const u8,
        group: []const u8,
    ) !BrokerMetadataFile {
        var path_buf: [std.fs.max_path_bytes]u8 = undefined;
        const path = try buildBrokerPath(&path_buf, storage_path, group);

        const fd = try platform.openFile(path);
        errdefer std.posix.close(fd);

        const file_size = try platform.fileSize(fd);
        const mapped = try platform.mmap(fd, file_size);
        errdefer platform.munmap(mapped);

        if (file_size < constants.metadata_header_length)
            return error.FileTooSmall;

        const header: *BrokerMetadataHeader = @ptrCast(@alignCast(mapped.ptr));

        const ctrl_len: usize = @intCast(header.control_buffer_length);
        const msgs_len: usize = @intCast(header.messages_buffer_length);

        if (!constants.isPowerOfTwo(ctrl_len))
            return error.ControlBufferNotPowerOfTwo;
        if (!constants.isPowerOfTwo(msgs_len))
            return error.MessagesBufferNotPowerOfTwo;

        const expected = constants.alignUp(
            constants.metadata_header_length + ctrl_len + msgs_len,
            constants.page_size,
        );
        if (file_size < expected)
            return error.FileSizeMismatch;

        return BrokerMetadataFile{
            .mapped_bytes = mapped,
            .header = header,
            .control_buffer = mapped[constants.metadata_header_length..][0..ctrl_len],
            .send_buffer = mapped[constants.metadata_header_length + ctrl_len ..][0..msgs_len],
            .fd = fd,
        };
    }

    // ── Atomic Accessors ──────────────────────────────────────────────

    /// Atomically load the broker's heartbeat timestamp.
    pub fn loadHeartbeat(self: *const BrokerMetadataFile) i64 {
        const ptr = self.heartbeatPtr();
        return @atomicLoad(i64, ptr, .acquire);
    }

    /// Atomically store the broker's heartbeat timestamp.
    pub fn storeHeartbeat(self: *BrokerMetadataFile, time_ms: i64) void {
        const ptr = self.heartbeatPtr();
        @atomicStore(i64, ptr, time_ms, .release);
    }

    /// Atomically load the current next_service_id value.
    pub fn loadNextServiceId(self: *const BrokerMetadataFile) i32 {
        const ptr = self.nextServiceIdPtr();
        return @atomicLoad(i32, ptr, .acquire);
    }

    /// Atomically store the next_service_id value (used during initialization
    /// or after scanning existing services).
    pub fn storeNextServiceId(self: *BrokerMetadataFile, value: i32) void {
        const ptr = self.nextServiceIdPtr();
        @atomicStore(i32, ptr, value, .release);
    }

    /// Atomically increment next_service_id and return the new value.
    /// This is the primary method used when assigning IDs to new services.
    pub fn incrementAndGetNextServiceId(self: *BrokerMetadataFile) i32 {
        const ptr = self.nextServiceIdPtr();
        const prev = @atomicRmw(i32, ptr, .Add, 1, .acq_rel);
        return prev + 1;
    }

    // ── Buffer Accessors ──────────────────────────────────────────────

    /// Returns the byte slice backing the control ring buffer.
    /// The caller (ring buffer implementation) overlays its trailer at the end.
    pub fn getControlBuffer(self: *const BrokerMetadataFile) []u8 {
        return self.control_buffer;
    }

    /// Returns the byte slice backing the send ring buffer.
    pub fn getSendBuffer(self: *const BrokerMetadataFile) []u8 {
        return self.send_buffer;
    }

    // ── Cleanup ───────────────────────────────────────────────────────

    /// Unmap the file and close the file descriptor.
    pub fn close(self: *BrokerMetadataFile) void {
        platform.munmap(self.mapped_bytes);
        std.posix.close(self.fd);
        self.* = undefined;
    }

    // ── Internal ──────────────────────────────────────────────────────

    fn heartbeatPtr(self: *const BrokerMetadataFile) *volatile i64 {
        const base: [*]u8 = self.mapped_bytes.ptr;
        const offset = constants.heartbeat_offset_within_header;
        return @ptrCast(@alignCast(base + offset));
    }

    fn nextServiceIdPtr(self: *const BrokerMetadataFile) *volatile i32 {
        const base: [*]u8 = self.mapped_bytes.ptr;
        const offset = constants.next_service_id_offset_within_header;
        return @ptrCast(@alignCast(base + offset));
    }

    fn buildBrokerPath(
        buf: *[std.fs.max_path_bytes]u8,
        storage_path: []const u8,
        group: []const u8,
    ) ![]const u8 {
        return std.fmt.bufPrint(buf, "{s}/{s}/services/broker_0.dat", .{
            storage_path,
            group,
        }) catch return error.PathTooLong;
    }

    fn ensureDirectoryExists(file_path: []const u8) !void {
        const dir = std.fs.path.dirname(file_path) orelse return error.InvalidPath;
        std.fs.makeDirAbsolute(dir) catch |err| switch (err) {
            error.PathAlreadyExists => {},
            else => return err,
        };
    }
};
```

### 3.4 Implementation Notes

1. **Buffer sizes must be powers of two.** Both `create()` and `open()` validate this
   and return a typed error if violated. The ring buffer implementation (doc 03)
   depends on `position & (capacity - 1)` for index calculation — a non-power-of-two
   capacity silently corrupts data.

2. **Volatile fields are accessed through raw pointer casts.** The `heartbeatPtr()` and
   `nextServiceIdPtr()` helpers compute a `*volatile i64` / `*volatile i32` from the
   mapped byte slice. Zig's `@atomicLoad` / `@atomicStore` / `@atomicRmw` builtins
   operate on these pointers with explicit memory ordering.

3. **The file is page-aligned.** `total_size` is rounded up to `page_size` so that the
   kernel maps whole pages. This wastes at most 4095 bytes but avoids partial-page
   edge cases on some platforms.

4. **`@memset(mapped, 0)` is required.** While Linux guarantees zero-filled pages for
   new `mmap` regions backed by `ftruncate`'d files, macOS does not always do so for
   `MAP_SHARED` on a newly extended file. Explicit zeroing is defensive.

5. **`incrementAndGetNextServiceId` uses `@atomicRmw(.Add, 1, .acq_rel)`.** The
   `acq_rel` ordering ensures that the incremented value is visible to subsequent
   readers across processes (the mmap'd page is shared memory; cache coherence
   protocols propagate the write).

---

## 4. Service Metadata File Layout

### 4.1 Binary Layout

```
Offset 0                                          512 bytes
┌──────────────────────────────────────────────────────────────┐
│  Metadata Header                                             │
│                                                              │
│  +0:    control_buffer_length     (i32)                      │
│  +4:    messages_buffer_length    (i32)                      │
│  +8:    service_id                (i32)                      │
│  +12:   node_id                   (i16)                      │
│  +14:   blocking_mode             (i16)  0=off, 1=on         │
│  +16:   pid                       (i64)                      │
│  +24:   start_timestamp_ms        (i64)                      │
│  +32:   heartbeat_timeout_ms      (i32)                      │
│  +36:   reserved                  (220 bytes, zero-filled)   │
│  +256:  heartbeat_time_ms         (volatile i64)  ← atomic   │
│  +264:  padding                   (248 bytes)                │
├──────────────────────────────────────────────────────────────┤  ← offset 512

IF blocking_mode == 1:
┌──────────────────────────────────────────────────────────────┐  ← offset 512
│  Blocking Trailer (384 bytes = 3 × 128)                      │
│                                                              │
│  +0:    writer_wait_state   (i32 + 124 bytes padding)        │
│  +128:  reader_wait_state   (i32 + 124 bytes padding)        │
│  +256:  wait_timeout_ns     (i64 + 120 bytes padding)        │
└──────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────┐
│  Control Ring Buffer  (control_buffer_length bytes)          │
│  Broker → service control messages                           │
│  (registration response, service instances, leader changed)  │
├──────────────────────────────────────────────────────────────┤
│  Messages Ring Buffer  (messages_buffer_length bytes)        │
│  Producer → service application messages                     │
│  (from local services via IPC, or from broker for cross-host)│
└──────────────────────────────────────────────────────────────┘

Total file size = 512
               + (384 if blocking_mode == 1, else 0)
               + control_buffer_length
               + messages_buffer_length
               (aligned up to page_size)
```

### 4.2 Blocking Trailer Detail

The blocking trailer enables kernel-assisted producer parking when the ring buffer is
full. Each of the three 128-byte slots is cache-line-padded to prevent false sharing.

| Slot | Offset | Field | Type | Purpose |
|------|--------|-------|------|---------|
| 0 | +0 | `writer_wait_state` | `volatile i32` | Futex/ulock word. 0 = not waiting, 1 = waiting. Producer atomically sets to 1 before parking. Consumer sets to 0 and wakes. |
| 1 | +128 | `reader_wait_state` | `volatile i32` | Futex/ulock word for consumer parking (idle strategy). Producer wakes consumer after committing a record. |
| 2 | +256 | `wait_timeout_ns` | `volatile i64` | Configurable timeout for blocking waits. 0 = infinite. Set at creation time, read by both sides. |

These fields are accessed by the platform abstraction's `ProcessSynchronizer` (doc 01)
which dispatches to `futex(2)` on Linux, `__ulock_wait`/`__ulock_wake` on macOS, or
`WaitOnAddress`/`WakeByAddressSingle` on Windows.

### 4.3 Packed Struct

```zig
// src/memory/service_metadata.zig

const std = @import("std");
const constants = @import("constants.zig");

/// Overlay for the fixed fields of the service metadata header.
pub const ServiceMetadataHeader = extern struct {
    control_buffer_length: i32,
    messages_buffer_length: i32,
    service_id: i32,
    node_id: i16,
    blocking_mode: i16, // 0 = non-blocking, 1 = blocking
    pid: i64,
    start_timestamp_ms: i64,
    heartbeat_timeout_ms: i32,

    comptime {
        std.debug.assert(@sizeOf(ServiceMetadataHeader) == 36);
    }
};

/// Overlay for one 128-byte blocking trailer slot.
pub const BlockingTrailerSlot = extern struct {
    value: i32,
    _pad: [124]u8 = [_]u8{0} ** 124,

    comptime {
        std.debug.assert(@sizeOf(BlockingTrailerSlot) == 128);
    }
};

/// Overlay for the full blocking trailer (384 bytes).
pub const BlockingTrailer = extern struct {
    writer_wait_state: BlockingTrailerSlot,
    reader_wait_state: BlockingTrailerSlot,
    wait_timeout: extern struct {
        value: i64,
        _pad: [120]u8 = [_]u8{0} ** 120,

        comptime {
            std.debug.assert(@sizeOf(@This()) == 128);
        }
    },

    comptime {
        std.debug.assert(@sizeOf(BlockingTrailer) == 384);
    }
};
```

### 4.4 ServiceMetadataFile Struct

```zig
// src/memory/service_metadata.zig (continued)

const platform = @import("../platform.zig");

pub const ServiceMetadataFile = struct {
    mapped_bytes: []align(std.heap.page_size_min) u8,
    header: *ServiceMetadataHeader,

    /// Non-null only when blocking_mode == 1.
    blocking_trailer: ?*BlockingTrailer,

    control_buffer: []u8,
    messages_buffer: []u8,
    fd: std.posix.fd_t,

    // ── Construction ──────────────────────────────────────────────────

    pub const CreateOptions = struct {
        storage_path: []const u8,
        group: []const u8,
        service_name: []const u8,
        service_id: i32,
        node_id: i16,
        blocking_mode: bool = false,
        heartbeat_timeout_ms: i32 = @intCast(constants.default_heartbeat_timeout_ms),
        control_buffer_length: usize = constants.default_control_buffer_length,
        messages_buffer_length: usize = constants.default_messages_buffer_length,
    };

    /// Create and initialize a new service metadata file.
    pub fn create(opts: CreateOptions) !ServiceMetadataFile {
        if (!constants.isPowerOfTwo(opts.control_buffer_length))
            return error.ControlBufferNotPowerOfTwo;
        if (!constants.isPowerOfTwo(opts.messages_buffer_length))
            return error.MessagesBufferNotPowerOfTwo;

        const blocking_extra: usize = if (opts.blocking_mode)
            constants.blocking_trailer_length
        else
            0;

        const total_size = constants.alignUp(
            constants.metadata_header_length +
                blocking_extra +
                opts.control_buffer_length +
                opts.messages_buffer_length,
            constants.page_size,
        );

        var path_buf: [std.fs.max_path_bytes]u8 = undefined;
        const path = try buildServicePath(
            &path_buf,
            opts.storage_path,
            opts.group,
            opts.service_name,
            opts.service_id,
        );

        try ensureDirectoryExists(path);

        const fd = try platform.createFile(path);
        errdefer std.posix.close(fd);
        try platform.ftruncate(fd, total_size);

        const mapped = try platform.mmap(fd, total_size);
        errdefer platform.munmap(mapped);

        @memset(mapped, 0);

        const buffers_offset = constants.metadata_header_length + blocking_extra;

        var self = ServiceMetadataFile{
            .mapped_bytes = mapped,
            .header = @ptrCast(@alignCast(mapped.ptr)),
            .blocking_trailer = if (opts.blocking_mode)
                @ptrCast(@alignCast(mapped.ptr + constants.metadata_header_length))
            else
                null,
            .control_buffer = mapped[buffers_offset..][0..opts.control_buffer_length],
            .messages_buffer = mapped[buffers_offset + opts.control_buffer_length ..][0..opts.messages_buffer_length],
            .fd = fd,
        };

        // Write immutable header fields.
        self.header.control_buffer_length = @intCast(opts.control_buffer_length);
        self.header.messages_buffer_length = @intCast(opts.messages_buffer_length);
        self.header.service_id = opts.service_id;
        self.header.node_id = opts.node_id;
        self.header.blocking_mode = if (opts.blocking_mode) 1 else 0;
        self.header.pid = @intCast(platform.getPid());
        self.header.start_timestamp_ms = platform.epochMillis();
        self.header.heartbeat_timeout_ms = opts.heartbeat_timeout_ms;

        // Write initial heartbeat.
        self.storeHeartbeat(platform.epochMillis());

        // If blocking, initialize the wait timeout.
        if (self.blocking_trailer) |trailer| {
            // Default: 1 second timeout for blocking waits.
            trailer.wait_timeout.value = 1_000_000_000; // 1s in nanoseconds
        }

        return self;
    }

    /// Open an existing service metadata file.
    /// Validates the header and computes buffer slice offsets.
    pub fn open(
        storage_path: []const u8,
        group: []const u8,
        service_name: []const u8,
        service_id: i32,
    ) !ServiceMetadataFile {
        var path_buf: [std.fs.max_path_bytes]u8 = undefined;
        const path = try buildServicePath(
            &path_buf,
            storage_path,
            group,
            service_name,
            service_id,
        );

        const fd = try platform.openFile(path);
        errdefer std.posix.close(fd);

        const file_size = try platform.fileSize(fd);
        const mapped = try platform.mmap(fd, file_size);
        errdefer platform.munmap(mapped);

        if (file_size < constants.metadata_header_length)
            return error.FileTooSmall;

        const header: *ServiceMetadataHeader = @ptrCast(@alignCast(mapped.ptr));

        const ctrl_len: usize = @intCast(header.control_buffer_length);
        const msgs_len: usize = @intCast(header.messages_buffer_length);
        const is_blocking = header.blocking_mode == 1;

        if (!constants.isPowerOfTwo(ctrl_len))
            return error.ControlBufferNotPowerOfTwo;
        if (!constants.isPowerOfTwo(msgs_len))
            return error.MessagesBufferNotPowerOfTwo;

        const blocking_extra: usize = if (is_blocking) constants.blocking_trailer_length else 0;
        const buffers_offset = constants.metadata_header_length + blocking_extra;

        const expected = constants.alignUp(
            buffers_offset + ctrl_len + msgs_len,
            constants.page_size,
        );
        if (file_size < expected)
            return error.FileSizeMismatch;

        return ServiceMetadataFile{
            .mapped_bytes = mapped,
            .header = header,
            .blocking_trailer = if (is_blocking)
                @ptrCast(@alignCast(mapped.ptr + constants.metadata_header_length))
            else
                null,
            .control_buffer = mapped[buffers_offset..][0..ctrl_len],
            .messages_buffer = mapped[buffers_offset + ctrl_len ..][0..msgs_len],
            .fd = fd,
        };
    }

    // ── Atomic Accessors ──────────────────────────────────────────────

    pub fn loadHeartbeat(self: *const ServiceMetadataFile) i64 {
        const ptr = self.heartbeatPtr();
        return @atomicLoad(i64, ptr, .acquire);
    }

    pub fn storeHeartbeat(self: *ServiceMetadataFile, time_ms: i64) void {
        const ptr = self.heartbeatPtr();
        @atomicStore(i64, ptr, time_ms, .release);
    }

    /// Returns true if this service's process is still alive.
    pub fn isProcessAlive(self: *const ServiceMetadataFile) bool {
        const pid: std.posix.pid_t = @intCast(self.header.pid);
        return platform.isProcessAlive(pid);
    }

    /// Returns true if blocking mode is enabled for this service.
    pub fn isBlocking(self: *const ServiceMetadataFile) bool {
        return self.header.blocking_mode == 1;
    }

    // ── Buffer Accessors ──────────────────────────────────────────────

    pub fn getControlBuffer(self: *const ServiceMetadataFile) []u8 {
        return self.control_buffer;
    }

    pub fn getMessagesBuffer(self: *const ServiceMetadataFile) []u8 {
        return self.messages_buffer;
    }

    pub fn getBlockingTrailer(self: *const ServiceMetadataFile) ?*BlockingTrailer {
        return self.blocking_trailer;
    }

    // ── Cleanup ───────────────────────────────────────────────────────

    pub fn close(self: *ServiceMetadataFile) void {
        platform.munmap(self.mapped_bytes);
        std.posix.close(self.fd);
        self.* = undefined;
    }

    // ── Internal ──────────────────────────────────────────────────────

    fn heartbeatPtr(self: *const ServiceMetadataFile) *volatile i64 {
        const base: [*]u8 = self.mapped_bytes.ptr;
        return @ptrCast(@alignCast(base + constants.heartbeat_offset_within_header));
    }

    fn buildServicePath(
        buf: *[std.fs.max_path_bytes]u8,
        storage_path: []const u8,
        group: []const u8,
        service_name: []const u8,
        service_id: i32,
    ) ![]const u8 {
        return std.fmt.bufPrint(buf, "{s}/{s}/services/{s}_{d}.dat", .{
            storage_path,
            group,
            service_name,
            service_id,
        }) catch return error.PathTooLong;
    }

    fn ensureDirectoryExists(file_path: []const u8) !void {
        const dir = std.fs.path.dirname(file_path) orelse return error.InvalidPath;
        std.fs.makeDirAbsolute(dir) catch |err| switch (err) {
            error.PathAlreadyExists => {},
            else => return err,
        };
    }
};
```

### 4.5 PID-Based Reuse

When `open()` is called, the caller should check `isProcessAlive()`. If the process
that created the file is dead, the file can be reused:

```zig
// Example usage in service registration:
fn openOrReuse(opts: ServiceMetadataFile.CreateOptions) !ServiceMetadataFile {
    const existing = ServiceMetadataFile.open(
        opts.storage_path,
        opts.group,
        opts.service_name,
        opts.service_id,
    ) catch |err| switch (err) {
        error.FileNotFound => return ServiceMetadataFile.create(opts),
        else => return err,
    };

    if (!existing.isProcessAlive()) {
        // Process is dead — close the stale mapping and recreate.
        var stale = existing;
        stale.close();
        return ServiceMetadataFile.create(opts);
    }

    return existing;
}
```

The platform abstraction layer (doc 01) provides `isProcessAlive()`:

```zig
// src/platform.zig (excerpt — see doc 01 for full implementation)

/// Returns true if a process with the given PID is currently running.
pub fn isProcessAlive(pid: std.posix.pid_t) bool {
    if (@import("builtin").os.tag == .linux) {
        // kill(pid, 0) — signal 0 checks existence without sending a signal.
        const result = std.posix.kill(pid, 0);
        return result != error.NoSuchProcess and result != error.PermissionDenied;
    } else {
        // macOS / other POSIX: same approach.
        const result = std.posix.kill(pid, 0);
        return result != error.NoSuchProcess;
    }
}
```

On Linux, `/proc/<pid>` can also be checked, but `kill(pid, 0)` is portable and
does not require filesystem access.

---

## 5. Receive Log Buffer Layout

> **Note:** The receive log buffer was used with the previous UDP transport for
> assembling inbound packets and detecting gaps. With the TCP transport, TCP handles
> reliability and ordering natively. The receive log buffer is **no longer part of the
> active architecture**. This section is retained for historical reference only. Inbound
> TCP data is now read through `ringloom_tcp`'s framing layer (doc 04) and routed directly
> to service ring buffers by the receiver event loop (doc 06).

### 5.1 Binary Layout

One receive log buffer existed per connected peer broker. Unlike the metadata files,
these were typically allocated as anonymous memory (not file-backed) since they were
only accessed by the local broker process.

```
Offset 0                                         log_buffer_length bytes
┌──────────────────────────────────────────────────────────────┐
│  Log Data                                                    │
│  (log_buffer_length bytes, MUST be power of 2)               │
│                                                              │
│  Contains received UDP data frames from this peer.           │
│  Frames are 32-byte aligned within the buffer.               │
│  The receiver writes sequentially at tail_position.           │
├──────────────────────────────────────────────────────────────┤  ← offset = log_buffer_length
│  Log Metadata (256 bytes)                                    │
│                                                              │
│  +0:    tail_position       (volatile i64)  ← receiver       │
│  +8:    padding              (120 bytes)                     │
│  +128:  rebuild_position    (volatile i64)  ← control loop   │
│  +136:  padding              (120 bytes)                     │
└──────────────────────────────────────────────────────────────┘

Total allocation = log_buffer_length + 256
```

The two metadata fields are separated by 128 bytes (one cache-line pad) because they
are written by different threads:

- **`tail_position`** — written by the receiver thread after inserting a packet.
  Monotonically increasing byte offset.
- **`rebuild_position`** — written by the control loop / router thread to track how
  far it has processed. Used for gap detection and NAK generation.

Both are accessed via atomic load/store with acquire/release ordering. The actual
buffer index is `position & (log_buffer_length - 1)`.

### 5.2 Packed Struct

```zig
// src/memory/receive_log.zig

const std = @import("std");
const constants = @import("constants.zig");
const platform = @import("../platform.zig");

/// Overlay for the 256-byte metadata region at the end of the log buffer.
pub const ReceiveLogMetadata = extern struct {
    tail_position: i64,
    _tail_pad: [120]u8,
    rebuild_position: i64,
    _rebuild_pad: [120]u8,

    comptime {
        std.debug.assert(@sizeOf(ReceiveLogMetadata) == 256);
    }
};
```

### 5.3 ReceiveLogBuffer Struct

```zig
// src/memory/receive_log.zig (continued)

/// Frame alignment within the log buffer. Frames are padded to 32-byte
/// boundaries so that headers always start at aligned addresses.
pub const frame_alignment: usize = 32;

pub const ReceiveLogBuffer = struct {
    /// The full backing memory: data region + metadata.
    backing: []align(std.heap.page_size_min) u8,

    /// The data region (first `capacity` bytes).
    data: []u8,

    /// Pointer to the metadata overlay at the end.
    metadata: *ReceiveLogMetadata,

    /// Capacity of the data region (power of 2). Used for index masking.
    capacity: usize,

    /// Bitmask: capacity - 1. Used as `position & mask` to get buffer index.
    mask: usize,

    // ── Construction ──────────────────────────────────────────────────

    /// Allocate a new receive log buffer with the given capacity.
    /// `capacity` must be a power of two and >= 4096.
    pub fn allocate(capacity: usize) !ReceiveLogBuffer {
        if (!constants.isPowerOfTwo(capacity))
            return error.CapacityNotPowerOfTwo;
        if (capacity < constants.page_size)
            return error.CapacityTooSmall;

        const total = capacity + constants.recv_log_metadata_length;

        // Use mmap for aligned, zeroed memory.
        const backing = try platform.mmapAnonymous(total);

        return ReceiveLogBuffer{
            .backing = backing,
            .data = backing[0..capacity],
            .metadata = @ptrCast(@alignCast(backing.ptr + capacity)),
            .capacity = capacity,
            .mask = capacity - 1,
        };
    }

    // ── Packet Insertion (single-writer: receiver thread) ─────────────

    /// Insert a received data frame into the log at the current tail position.
    ///
    /// The caller is responsible for ensuring the frame fits within a single
    /// wrap of the buffer (i.e., `frame.len <= capacity`). This is guaranteed
    /// by the MTU and flow control — the sender never sends a frame larger
    /// than the MTU, and the MTU is always smaller than the log buffer.
    ///
    /// The frame is written with a 4-byte length prefix (little-endian i32).
    /// The length field is written LAST with release semantics so that
    /// readers see the full frame before the length becomes non-zero.
    pub fn insertPacket(self: *ReceiveLogBuffer, frame: []const u8) void {
        const frame_length: i32 = @intCast(frame.len);
        const aligned_length = constants.alignUp(
            frame.len + @sizeOf(i32), // 4-byte length prefix + frame data
            frame_alignment,
        );

        const tail = self.loadTailPosition();
        const tail_index = @as(usize, @intCast(tail)) & self.mask;

        // Write frame data first (everything except the length prefix).
        const data_offset = tail_index + @sizeOf(i32);
        @memcpy(self.data[data_offset..][0..frame.len], frame);

        // Write length prefix LAST with release ordering.
        // This acts as the "commit" — readers spin on this field.
        const length_ptr: *volatile i32 = @ptrCast(@alignCast(&self.data[tail_index]));
        @atomicStore(i32, length_ptr, frame_length, .release);

        // Advance tail position.
        self.storeTailPosition(tail + @as(i64, @intCast(aligned_length)));
    }

    // ── Position Accessors ────────────────────────────────────────────

    /// Load the current tail position (acquire). Called by router/control.
    pub fn loadTailPosition(self: *const ReceiveLogBuffer) i64 {
        return @atomicLoad(i64, &self.metadata.tail_position, .acquire);
    }

    /// Store the tail position (release). Called by receiver after insert.
    fn storeTailPosition(self: *ReceiveLogBuffer, pos: i64) void {
        @atomicStore(i64, &self.metadata.tail_position, pos, .release);
    }

    /// Load the rebuild position (acquire). Called by loss detector.
    pub fn loadRebuildPosition(self: *const ReceiveLogBuffer) i64 {
        return @atomicLoad(i64, &self.metadata.rebuild_position, .acquire);
    }

    /// Store the rebuild position (release). Called by control loop.
    pub fn storeRebuildPosition(self: *ReceiveLogBuffer, pos: i64) void {
        @atomicStore(i64, &self.metadata.rebuild_position, pos, .release);
    }

    // ── Data Access ───────────────────────────────────────────────────

    /// Read a frame at the given absolute position.
    /// Returns the frame data slice (excluding the length prefix), or null
    /// if the frame at this position is not yet committed (length <= 0).
    pub fn readFrame(self: *const ReceiveLogBuffer, position: i64) ?[]const u8 {
        const index = @as(usize, @intCast(position)) & self.mask;
        const length_ptr: *const volatile i32 = @ptrCast(@alignCast(&self.data[index]));
        const length = @atomicLoad(i32, length_ptr, .acquire);

        if (length <= 0) return null;

        const data_offset = index + @sizeOf(i32);
        return self.data[data_offset..][0..@as(usize, @intCast(length))];
    }

    // ── Cleanup ───────────────────────────────────────────────────────

    /// Free the backing memory.
    pub fn close(self: *ReceiveLogBuffer) void {
        platform.munmap(self.backing);
        self.* = undefined;
    }
};
```

### 5.4 Circular Overwrite Semantics

The receive log buffer uses a **circular overwrite model**:

1. The receiver writes at `tail_position % capacity`.
2. The router reads at its own tracked position.
3. If the router falls behind by more than `capacity` bytes, stale data is overwritten.
4. Back-pressure via Status Messages (doc 07) prevents this under normal operation:
   the receiver advertises its available window to the sender, which throttles
   accordingly.

Unlike Aeron's 3-partition log buffer with term rotation, RingLoom uses a single partition
because there is exactly one stream per peer link. This simplifies the design at the
cost of requiring careful flow control to avoid overwrite.

---

## 6. File Discovery — Service Scanner

On broker startup, scan for existing service metadata files to recover state after a
broker restart. Live services that were registered before the broker crashed are
re-discovered and re-registered without requiring the services to re-register
themselves.

### 6.1 Data Types

```zig
// src/memory/service_scanner.zig

const std = @import("std");
const constants = @import("constants.zig");
const ServiceMetadataFile = @import("service_metadata.zig").ServiceMetadataFile;
const platform = @import("../platform.zig");

/// Information about a discovered live service.
pub const ServiceInstance = struct {
    service_id: i32,
    service_name: []const u8, // Allocated — caller must free.
    node_id: i16,
    metadata_file: ServiceMetadataFile,
};

/// Result of scanning the services directory.
pub const ScanResult = struct {
    /// All services whose PID is still alive and heartbeat is fresh.
    service_instances: []ServiceInstance,

    /// The next service ID to assign (max found ID + 1).
    next_service_id: i32,

    /// Paths of stale files (dead PID or expired heartbeat) for cleanup.
    stale_file_paths: [][]const u8,

    pub fn deinit(self: *ScanResult, allocator: std.mem.Allocator) void {
        for (self.service_instances) |*inst| {
            allocator.free(inst.service_name);
            inst.metadata_file.close();
        }
        allocator.free(self.service_instances);
        for (self.stale_file_paths) |path| {
            allocator.free(path);
        }
        allocator.free(self.stale_file_paths);
    }
};
```

### 6.2 Scanner Implementation

```zig
pub const ServiceScanner = struct {
    storage_path: []const u8,
    group: []const u8,
    allocator: std.mem.Allocator,

    pub fn init(
        allocator: std.mem.Allocator,
        storage_path: []const u8,
        group: []const u8,
    ) ServiceScanner {
        return .{
            .storage_path = storage_path,
            .group = group,
            .allocator = allocator,
        };
    }

    /// Scan the services directory for `.dat` files.
    /// Returns live services and the next service ID to assign.
    pub fn scan(self: *const ServiceScanner) !ScanResult {
        var path_buf: [std.fs.max_path_bytes]u8 = undefined;
        const dir_path = try std.fmt.bufPrint(&path_buf, "{s}/{s}/services", .{
            self.storage_path,
            self.group,
        });

        var dir = std.fs.openDirAbsolute(dir_path, .{ .iterate = true }) catch |err| switch (err) {
            error.FileNotFound => return ScanResult{
                .service_instances = &.{},
                .next_service_id = 1,
                .stale_file_paths = &.{},
            },
            else => return err,
        };
        defer dir.close();

        var live_list = std.ArrayList(ServiceInstance).init(self.allocator);
        errdefer live_list.deinit();

        var stale_list = std.ArrayList([]const u8).init(self.allocator);
        errdefer stale_list.deinit();

        var max_service_id: i32 = 0;
        const now_ms = platform.epochMillis();

        var iter = dir.iterate();
        while (try iter.next()) |entry| {
            if (!std.mem.endsWith(u8, entry.name, ".dat")) continue;

            // Skip the broker's own file.
            if (std.mem.eql(u8, entry.name, "broker_0.dat")) continue;

            // Parse service name and ID from filename: <name>_<id>.dat
            const parsed = parseFileName(entry.name) orelse continue;

            // Try to open the metadata file.
            const metadata_file = ServiceMetadataFile.open(
                self.storage_path,
                self.group,
                parsed.name,
                parsed.id,
            ) catch continue;

            // Check if the owning process is alive and heartbeat is fresh.
            const heartbeat = metadata_file.loadHeartbeat();
            const heartbeat_timeout: i64 = metadata_file.header.heartbeat_timeout_ms;
            const is_alive = metadata_file.isProcessAlive();
            const is_fresh = (now_ms - heartbeat) < heartbeat_timeout;

            if (is_alive and is_fresh) {
                if (parsed.id > max_service_id) {
                    max_service_id = parsed.id;
                }
                try live_list.append(.{
                    .service_id = parsed.id,
                    .service_name = try self.allocator.dupe(u8, parsed.name),
                    .node_id = metadata_file.header.node_id,
                    .metadata_file = metadata_file,
                });
            } else {
                // Stale — record the path for potential cleanup.
                var full_path_buf: [std.fs.max_path_bytes]u8 = undefined;
                const full_path = try std.fmt.bufPrint(&full_path_buf, "{s}/{s}", .{
                    dir_path,
                    entry.name,
                });
                try stale_list.append(try self.allocator.dupe(u8, full_path));

                // Close the mapping for the stale file.
                var stale_meta = metadata_file;
                stale_meta.close();
            }
        }

        return ScanResult{
            .service_instances = try live_list.toOwnedSlice(),
            .next_service_id = max_service_id + 1,
            .stale_file_paths = try stale_list.toOwnedSlice(),
        };
    }

    const ParsedFileName = struct {
        name: []const u8,
        id: i32,
    };

    /// Parse "<name>_<id>.dat" into name and id.
    fn parseFileName(file_name: []const u8) ?ParsedFileName {
        // Strip ".dat" suffix.
        const without_ext = file_name[0 .. file_name.len - 4];

        // Find the last underscore.
        const last_underscore = std.mem.lastIndexOfScalar(u8, without_ext, '_') orelse return null;
        if (last_underscore == 0) return null;

        const name = without_ext[0..last_underscore];
        const id_str = without_ext[last_underscore + 1 ..];

        const id = std.fmt.parseInt(i32, id_str, 10) catch return null;

        return .{
            .name = name,
            .id = id,
        };
    }
};
```

### 6.3 Scanner Workflow

```
Broker startup
    │
    ▼
ServiceScanner.scan()
    │
    ├── List all .dat files in <storage>/<group>/services/
    │
    ├── For each file (skip broker_0.dat):
    │   ├── Parse name + id from filename
    │   ├── mmap and read header
    │   ├── Check PID alive (kill(pid, 0))
    │   ├── Check heartbeat freshness (now - heartbeat < timeout)
    │   │
    │   ├── If alive AND fresh → add to live list, track max ID
    │   └── If dead OR stale  → add to stale list, close mapping
    │
    ├── next_service_id = max(found IDs) + 1
    │
    └── Return ScanResult { live_instances, next_service_id, stale_paths }
```

After scanning, the broker:
1. Initializes its `nextServiceId` counter to `scan_result.next_service_id`
2. Re-registers each live service in its `ServiceRegistry`
3. Optionally deletes stale files (or leaves them for manual cleanup)

---

## 7. Metadata Descriptor Provider (Singleton)

The `MetadataDescriptorProvider` manages the **broker's own** metadata file. It is a
singleton — exactly one instance exists per broker process.

### 7.1 Implementation

```zig
// src/memory/metadata_descriptor_provider.zig

const std = @import("std");
const BrokerMetadataFile = @import("broker_metadata.zig").BrokerMetadataFile;
const ServiceScanner = @import("service_scanner.zig").ServiceScanner;
const ScanResult = @import("service_scanner.zig").ScanResult;
const constants = @import("constants.zig");
const platform = @import("../platform.zig");

/// Singleton that owns the broker's metadata file.
/// Module-level mutable state — initialized once at startup, reset on close.
var instance: ?MetadataDescriptorProvider = null;

pub const MetadataDescriptorProvider = struct {
    broker_file: BrokerMetadataFile,
    scanner: ServiceScanner,
    allocator: std.mem.Allocator,

    // ── Singleton Access ──────────────────────────────────────────────

    pub const InitOptions = struct {
        allocator: std.mem.Allocator,
        storage_path: []const u8,
        group: []const u8,
        node_id: i16,
        control_buffer_length: usize = constants.default_control_buffer_length,
        messages_buffer_length: usize = constants.default_send_buffer_length,
    };

    /// Initialize the singleton. Must be called exactly once before any
    /// other access. Creates the broker metadata file and the service
    /// scanner.
    ///
    /// If a broker metadata file already exists and the owning PID is dead,
    /// it is overwritten. If the PID is alive, returns an error (another
    /// broker is already running).
    pub fn init(opts: InitOptions) !void {
        if (instance != null) return error.AlreadyInitialized;

        const broker_file = try BrokerMetadataFile.create(
            opts.storage_path,
            opts.group,
            opts.node_id,
            opts.control_buffer_length,
            opts.messages_buffer_length,
        );

        instance = MetadataDescriptorProvider{
            .broker_file = broker_file,
            .scanner = ServiceScanner.init(opts.allocator, opts.storage_path, opts.group),
            .allocator = opts.allocator,
        };
    }

    /// Get the singleton instance. Panics if not initialized.
    pub fn getInstance() *MetadataDescriptorProvider {
        return &(instance orelse @panic("MetadataDescriptorProvider not initialized"));
    }

    // ── Accessors ─────────────────────────────────────────────────────

    /// Returns the control ring buffer region (services → broker).
    pub fn getControlBuffer(self: *MetadataDescriptorProvider) []u8 {
        return self.broker_file.getControlBuffer();
    }

    /// Returns the send ring buffer region (services → broker, cross-host).
    pub fn getSendBuffer(self: *MetadataDescriptorProvider) []u8 {
        return self.broker_file.getSendBuffer();
    }

    /// Update the broker's heartbeat timestamp.
    pub fn updateHeartbeat(self: *MetadataDescriptorProvider) void {
        self.broker_file.storeHeartbeat(platform.epochMillis());
    }

    /// Read the broker's current heartbeat timestamp.
    pub fn readHeartbeat(self: *MetadataDescriptorProvider) i64 {
        return self.broker_file.loadHeartbeat();
    }

    /// Assign the next service ID (atomic increment).
    pub fn assignNextServiceId(self: *MetadataDescriptorProvider) i32 {
        return self.broker_file.incrementAndGetNextServiceId();
    }

    /// Set the next service ID counter (used after scanning).
    pub fn setNextServiceId(self: *MetadataDescriptorProvider, value: i32) void {
        self.broker_file.storeNextServiceId(value);
    }

    /// Scan for existing live services on disk.
    pub fn scanServices(self: *MetadataDescriptorProvider) !ScanResult {
        return self.scanner.scan();
    }

    // ── Cleanup ───────────────────────────────────────────────────────

    /// Unmap the broker's metadata file and reset the singleton.
    pub fn close(self: *MetadataDescriptorProvider) void {
        self.broker_file.close();
        instance = null;
    }
};
```

### 7.2 Usage Pattern

```zig
// In BrokerApplicationFactory or equivalent startup code:

pub fn startBroker(config: BrokerConfig) !void {
    // 1. Initialize the singleton.
    try MetadataDescriptorProvider.init(.{
        .allocator = config.allocator,
        .storage_path = config.storage_path,
        .group = config.group,
        .node_id = config.node_id,
        .control_buffer_length = config.control_buffer_length,
        .messages_buffer_length = config.send_buffer_length,
    });
    const provider = MetadataDescriptorProvider.getInstance();

    // 2. Scan for existing services and set the next ID.
    var scan_result = try provider.scanServices();
    defer scan_result.deinit(config.allocator);
    provider.setNextServiceId(scan_result.next_service_id);

    // 3. Re-register live services found during scan.
    for (scan_result.service_instances) |inst| {
        try service_registry.register(inst);
    }

    // 4. Create ring buffers over the mapped regions.
    var control_rb = RingBuffer.init(provider.getControlBuffer());
    var send_rb = RingBuffer.init(provider.getSendBuffer());

    // ... start event loops ...
}
```

---

## 8. Buffers Provider (Per-Service Cache)

The `BuffersProvider` manages mappings to **other services'** metadata files. The broker
uses it to access each registered service's control and messages ring buffers. Services
use it to access the broker's metadata file and other services' files (for direct IPC).

### 8.1 Implementation

```zig
// src/memory/buffers_provider.zig

const std = @import("std");
const ServiceMetadataFile = @import("service_metadata.zig").ServiceMetadataFile;
const BrokerMetadataFile = @import("broker_metadata.zig").BrokerMetadataFile;
const constants = @import("constants.zig");
const platform = @import("../platform.zig");

pub const BuffersProvider = struct {
    service_file: ServiceMetadataFile,
    service_id: i32,
    service_name: []const u8,

    /// Cache of service_id → BuffersProvider instances.
    /// Module-level state — shared across the process.
    var cache = std.AutoHashMap(i32, *BuffersProvider).init(std.heap.page_allocator);

    // ── Construction ──────────────────────────────────────────────────

    /// Get or create a BuffersProvider for the given service.
    /// If a mapping already exists in the cache, returns it.
    /// Otherwise, opens the service's metadata file and caches the result.
    pub fn getInstance(
        allocator: std.mem.Allocator,
        service_id: i32,
        service_name: []const u8,
        storage_path: []const u8,
        group: []const u8,
    ) !*BuffersProvider {
        if (cache.get(service_id)) |existing| {
            return existing;
        }

        const provider = try allocator.create(BuffersProvider);
        errdefer allocator.destroy(provider);

        provider.* = BuffersProvider{
            .service_file = try ServiceMetadataFile.open(
                storage_path,
                group,
                service_name,
                service_id,
            ),
            .service_id = service_id,
            .service_name = service_name,
        };

        try cache.put(service_id, provider);
        return provider;
    }

    /// Get the cached instance for a service, or null if not cached.
    pub fn getCached(service_id: i32) ?*BuffersProvider {
        return cache.get(service_id);
    }

    // ── Buffer Accessors ──────────────────────────────────────────────

    /// Returns the control ring buffer region for this service.
    /// The broker writes control messages here (registration response,
    /// service instances, leader changed).
    pub fn getControlBuffer(self: *const BuffersProvider) []u8 {
        return self.service_file.getControlBuffer();
    }

    /// Returns the messages ring buffer region for this service.
    /// Producers write application messages here.
    pub fn getMessagesBuffer(self: *const BuffersProvider) []u8 {
        return self.service_file.getMessagesBuffer();
    }

    // ── Health Checks ─────────────────────────────────────────────────

    /// Read the service's last heartbeat timestamp (atomic).
    pub fn readHeartbeat(self: *const BuffersProvider) i64 {
        return self.service_file.loadHeartbeat();
    }

    /// Check if the service is healthy (heartbeat within timeout).
    pub fn isHealthy(self: *const BuffersProvider) bool {
        const now_ms = platform.epochMillis();
        const last_heartbeat = self.readHeartbeat();
        return (now_ms - last_heartbeat) <= constants.default_heartbeat_timeout_ms;
    }

    /// Check if the service's owning process is still alive.
    pub fn isProcessAlive(self: *const BuffersProvider) bool {
        return self.service_file.isProcessAlive();
    }

    /// Returns whether the service's ring buffers use blocking mode.
    pub fn isBlocking(self: *const BuffersProvider) bool {
        return self.service_file.isBlocking();
    }

    // ── Broker nextServiceId Access ───────────────────────────────────

    /// Increment the broker's nextServiceId counter and return the new value.
    /// This requires a separate mapping to the broker's metadata file.
    /// Typically called via `MetadataDescriptorProvider.assignNextServiceId()` instead.
    ///
    /// Provided here for service-side code that maps the broker file directly.
    pub fn incrementAndGetNextServiceId(broker_file: *BrokerMetadataFile) i32 {
        return broker_file.incrementAndGetNextServiceId();
    }

    // ── Cleanup ───────────────────────────────────────────────────────

    /// Close this provider's mapping and remove it from the cache.
    pub fn close(self: *BuffersProvider, allocator: std.mem.Allocator) void {
        _ = cache.remove(self.service_id);
        self.service_file.close();
        allocator.destroy(self);
    }

    /// Close all cached providers. Called at shutdown.
    pub fn closeAll(allocator: std.mem.Allocator) void {
        var iter = cache.valueIterator();
        while (iter.next()) |provider| {
            provider.*.service_file.close();
            allocator.destroy(provider.*);
        }
        cache.clearAndFree();
    }
};
```

### 8.2 Usage Pattern

```zig
// Broker registers a new service:
fn registerService(
    allocator: std.mem.Allocator,
    service_name: []const u8,
    service_id: i32,
    config: *const BrokerConfig,
) !*BuffersProvider {
    const provider = try BuffersProvider.getInstance(
        allocator,
        service_id,
        service_name,
        config.storage_path,
        config.group,
    );

    // Create a ring buffer over the service's control buffer region.
    // The broker will write control responses here.
    var control_rb = RingBuffer.init(provider.getControlBuffer());

    // Write registration response.
    try control_rb.write(
        ControlMessageType.registration_response,
        &registration_response_bytes,
    );

    return provider;
}
```

---

## 9. Testing

### 9.1 Unit Test: Broker Metadata File Layout

```zig
// src/memory/broker_metadata_test.zig

const std = @import("std");
const testing = std.testing;
const BrokerMetadataFile = @import("broker_metadata.zig").BrokerMetadataFile;
const BrokerMetadataHeader = @import("broker_metadata.zig").BrokerMetadataHeader;
const constants = @import("constants.zig");

test "BrokerMetadataHeader has correct size" {
    try testing.expectEqual(@as(usize, 32), @sizeOf(BrokerMetadataHeader));
}

test "create broker metadata file and verify layout" {
    const tmp_dir = testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const storage_path = try tmp_dir.dir.realpathAlloc(testing.allocator, ".");
    defer testing.allocator.free(storage_path);

    // Ensure services subdirectory exists.
    try tmp_dir.dir.makePath("test-group/services");

    var file = try BrokerMetadataFile.create(
        storage_path,
        "test-group",
        42, // node_id
        64 * 1024, // 64 KB control buffer
        1024 * 1024, // 1 MB send buffer
    );
    defer file.close();

    // Verify header fields.
    try testing.expectEqual(@as(i32, 64 * 1024), file.header.control_buffer_length);
    try testing.expectEqual(@as(i32, 1024 * 1024), file.header.messages_buffer_length);
    try testing.expectEqual(@as(i32, 0), file.header.service_id);
    try testing.expectEqual(@as(i16, 42), file.header.node_id);
    try testing.expect(file.header.pid > 0);
    try testing.expect(file.header.start_timestamp_ms > 0);

    // Verify buffer slice sizes.
    try testing.expectEqual(@as(usize, 64 * 1024), file.control_buffer.len);
    try testing.expectEqual(@as(usize, 1024 * 1024), file.send_buffer.len);

    // Verify control buffer starts at offset 512.
    const control_offset = @intFromPtr(file.control_buffer.ptr) - @intFromPtr(file.mapped_bytes.ptr);
    try testing.expectEqual(@as(usize, 512), control_offset);

    // Verify send buffer starts right after control buffer.
    const send_offset = @intFromPtr(file.send_buffer.ptr) - @intFromPtr(file.mapped_bytes.ptr);
    try testing.expectEqual(@as(usize, 512 + 64 * 1024), send_offset);

    // Verify nextServiceId initialized to 1.
    try testing.expectEqual(@as(i32, 1), file.loadNextServiceId());

    // Verify heartbeat was written.
    try testing.expect(file.loadHeartbeat() > 0);
}

test "incrementAndGetNextServiceId is atomic" {
    const tmp_dir = testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const storage_path = try tmp_dir.dir.realpathAlloc(testing.allocator, ".");
    defer testing.allocator.free(storage_path);
    try tmp_dir.dir.makePath("test-group/services");

    var file = try BrokerMetadataFile.create(storage_path, "test-group", 1, 64 * 1024, 64 * 1024);
    defer file.close();

    // Initial value is 1.
    try testing.expectEqual(@as(i32, 1), file.loadNextServiceId());

    // Increment returns the new value.
    try testing.expectEqual(@as(i32, 2), file.incrementAndGetNextServiceId());
    try testing.expectEqual(@as(i32, 3), file.incrementAndGetNextServiceId());
    try testing.expectEqual(@as(i32, 4), file.incrementAndGetNextServiceId());

    // Verify the stored value.
    try testing.expectEqual(@as(i32, 4), file.loadNextServiceId());
}

test "reject non-power-of-two buffer sizes" {
    const tmp_dir = testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const storage_path = try tmp_dir.dir.realpathAlloc(testing.allocator, ".");
    defer testing.allocator.free(storage_path);
    try tmp_dir.dir.makePath("test-group/services");

    // 1000 is not a power of two.
    const result = BrokerMetadataFile.create(storage_path, "test-group", 1, 1000, 64 * 1024);
    try testing.expectError(error.ControlBufferNotPowerOfTwo, result);
}
```

### 9.2 Unit Test: Service Metadata File with Blocking Mode

```zig
// src/memory/service_metadata_test.zig

const std = @import("std");
const testing = std.testing;
const ServiceMetadataFile = @import("service_metadata.zig").ServiceMetadataFile;
const ServiceMetadataHeader = @import("service_metadata.zig").ServiceMetadataHeader;
const BlockingTrailer = @import("service_metadata.zig").BlockingTrailer;
const constants = @import("constants.zig");

test "ServiceMetadataHeader has correct size" {
    try testing.expectEqual(@as(usize, 36), @sizeOf(ServiceMetadataHeader));
}

test "BlockingTrailer has correct size" {
    try testing.expectEqual(@as(usize, 384), @sizeOf(BlockingTrailer));
}

test "create service metadata file without blocking — verify offsets" {
    const tmp_dir = testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const storage_path = try tmp_dir.dir.realpathAlloc(testing.allocator, ".");
    defer testing.allocator.free(storage_path);
    try tmp_dir.dir.makePath("test-group/services");

    var file = try ServiceMetadataFile.create(.{
        .storage_path = storage_path,
        .group = "test-group",
        .service_name = "pricing",
        .service_id = 5,
        .node_id = 1,
        .blocking_mode = false,
        .control_buffer_length = 64 * 1024,
        .messages_buffer_length = 256 * 1024,
    });
    defer file.close();

    // Header fields.
    try testing.expectEqual(@as(i32, 5), file.header.service_id);
    try testing.expectEqual(@as(i16, 1), file.header.node_id);
    try testing.expectEqual(@as(i16, 0), file.header.blocking_mode);
    try testing.expect(!file.isBlocking());
    try testing.expect(file.blocking_trailer == null);

    // Non-blocking: control buffer starts at offset 512 (no blocking trailer).
    const ctrl_offset = @intFromPtr(file.control_buffer.ptr) - @intFromPtr(file.mapped_bytes.ptr);
    try testing.expectEqual(@as(usize, 512), ctrl_offset);

    // Messages buffer starts right after control buffer.
    const msgs_offset = @intFromPtr(file.messages_buffer.ptr) - @intFromPtr(file.mapped_bytes.ptr);
    try testing.expectEqual(@as(usize, 512 + 64 * 1024), msgs_offset);
}

test "create service metadata file with blocking — verify offsets include trailer" {
    const tmp_dir = testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const storage_path = try tmp_dir.dir.realpathAlloc(testing.allocator, ".");
    defer testing.allocator.free(storage_path);
    try tmp_dir.dir.makePath("test-group/services");

    var file = try ServiceMetadataFile.create(.{
        .storage_path = storage_path,
        .group = "test-group",
        .service_name = "orders",
        .service_id = 7,
        .node_id = 2,
        .blocking_mode = true,
        .control_buffer_length = 64 * 1024,
        .messages_buffer_length = 128 * 1024,
    });
    defer file.close();

    try testing.expectEqual(@as(i16, 1), file.header.blocking_mode);
    try testing.expect(file.isBlocking());
    try testing.expect(file.blocking_trailer != null);

    // Blocking: control buffer starts at 512 + 384 = 896.
    const ctrl_offset = @intFromPtr(file.control_buffer.ptr) - @intFromPtr(file.mapped_bytes.ptr);
    try testing.expectEqual(@as(usize, 512 + 384), ctrl_offset);

    // Messages buffer starts at 896 + 64 KB.
    const msgs_offset = @intFromPtr(file.messages_buffer.ptr) - @intFromPtr(file.mapped_bytes.ptr);
    try testing.expectEqual(@as(usize, 512 + 384 + 64 * 1024), msgs_offset);

    // Blocking trailer should have the default timeout.
    const trailer = file.blocking_trailer.?;
    try testing.expectEqual(@as(i64, 1_000_000_000), trailer.wait_timeout.value);
}

test "heartbeat read and write are consistent" {
    const tmp_dir = testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const storage_path = try tmp_dir.dir.realpathAlloc(testing.allocator, ".");
    defer testing.allocator.free(storage_path);
    try tmp_dir.dir.makePath("test-group/services");

    var file = try ServiceMetadataFile.create(.{
        .storage_path = storage_path,
        .group = "test-group",
        .service_name = "test-svc",
        .service_id = 1,
        .node_id = 0,
    });
    defer file.close();

    file.storeHeartbeat(123456789);
    try testing.expectEqual(@as(i64, 123456789), file.loadHeartbeat());

    file.storeHeartbeat(987654321);
    try testing.expectEqual(@as(i64, 987654321), file.loadHeartbeat());
}
```

### 9.3 Unit Test: Receive Log Buffer

```zig
// src/memory/receive_log_test.zig

const std = @import("std");
const testing = std.testing;
const ReceiveLogBuffer = @import("receive_log.zig").ReceiveLogBuffer;
const ReceiveLogMetadata = @import("receive_log.zig").ReceiveLogMetadata;
const constants = @import("constants.zig");

test "ReceiveLogMetadata has correct size" {
    try testing.expectEqual(@as(usize, 256), @sizeOf(ReceiveLogMetadata));
}

test "allocate receive log buffer and verify initial state" {
    var log = try ReceiveLogBuffer.allocate(64 * 1024); // 64 KB
    defer log.close();

    try testing.expectEqual(@as(usize, 64 * 1024), log.capacity);
    try testing.expectEqual(@as(usize, 64 * 1024 - 1), log.mask);
    try testing.expectEqual(@as(i64, 0), log.loadTailPosition());
    try testing.expectEqual(@as(i64, 0), log.loadRebuildPosition());
}

test "reject non-power-of-two capacity" {
    const result = ReceiveLogBuffer.allocate(60_000);
    try testing.expectError(error.CapacityNotPowerOfTwo, result);
}

test "reject capacity smaller than page size" {
    const result = ReceiveLogBuffer.allocate(2048);
    try testing.expectError(error.CapacityTooSmall, result);
}

test "insert packets and verify tail advances" {
    var log = try ReceiveLogBuffer.allocate(64 * 1024);
    defer log.close();

    // Insert a 100-byte frame.
    const frame1 = [_]u8{0xAA} ** 100;
    log.insertPacket(&frame1);

    // Tail should have advanced by aligned(100 + 4, 32) = aligned(104, 32) = 128.
    try testing.expectEqual(@as(i64, 128), log.loadTailPosition());

    // Read the frame back.
    const read1 = log.readFrame(0);
    try testing.expect(read1 != null);
    try testing.expectEqual(@as(usize, 100), read1.?.len);
    try testing.expectEqual(@as(u8, 0xAA), read1.?[0]);

    // Insert a second frame.
    const frame2 = [_]u8{0xBB} ** 50;
    log.insertPacket(&frame2);

    // Tail should advance by aligned(50 + 4, 32) = aligned(54, 32) = 64.
    try testing.expectEqual(@as(i64, 128 + 64), log.loadTailPosition());

    // Read the second frame.
    const read2 = log.readFrame(128);
    try testing.expect(read2 != null);
    try testing.expectEqual(@as(usize, 50), read2.?.len);
    try testing.expectEqual(@as(u8, 0xBB), read2.?[0]);
}

test "rebuild_position tracks independently of tail" {
    var log = try ReceiveLogBuffer.allocate(64 * 1024);
    defer log.close();

    const frame = [_]u8{0xCC} ** 200;
    log.insertPacket(&frame);
    log.insertPacket(&frame);

    // Tail advanced but rebuild stays at 0.
    try testing.expect(log.loadTailPosition() > 0);
    try testing.expectEqual(@as(i64, 0), log.loadRebuildPosition());

    // Control loop advances rebuild.
    log.storeRebuildPosition(128);
    try testing.expectEqual(@as(i64, 128), log.loadRebuildPosition());
}
```

### 9.4 Unit Test: Service Scanner File Name Parsing

```zig
// src/memory/service_scanner_test.zig

const std = @import("std");
const testing = std.testing;
const ServiceScanner = @import("service_scanner.zig").ServiceScanner;

test "parse valid service filename" {
    const parsed = ServiceScanner.parseFileName("pricing_3.dat");
    try testing.expect(parsed != null);
    try testing.expectEqualStrings("pricing", parsed.?.name);
    try testing.expectEqual(@as(i32, 3), parsed.?.id);
}

test "parse filename with underscores in name" {
    const parsed = ServiceScanner.parseFileName("order_service_12.dat");
    try testing.expect(parsed != null);
    try testing.expectEqualStrings("order_service", parsed.?.name);
    try testing.expectEqual(@as(i32, 12), parsed.?.id);
}

test "reject filename without underscore" {
    const parsed = ServiceScanner.parseFileName("invalid.dat");
    try testing.expect(parsed == null);
}

test "reject filename with non-numeric id" {
    const parsed = ServiceScanner.parseFileName("service_abc.dat");
    try testing.expect(parsed == null);
}
```

### 9.5 Integration Test: Cross-Process Shared Memory

This test verifies that two threads (simulating two processes) can share a metadata
file — one writing to the ring buffer region, the other reading.

```zig
// src/memory/integration_test.zig

const std = @import("std");
const testing = std.testing;
const BrokerMetadataFile = @import("broker_metadata.zig").BrokerMetadataFile;

test "two threads share a broker metadata file" {
    const tmp_dir = testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const storage_path = try tmp_dir.dir.realpathAlloc(testing.allocator, ".");
    defer testing.allocator.free(storage_path);
    try tmp_dir.dir.makePath("test-group/services");

    // Thread 1 (broker): create the file.
    var broker_file = try BrokerMetadataFile.create(
        storage_path,
        "test-group",
        1,
        4096, // small control buffer for testing
        4096,
    );
    defer broker_file.close();

    // Write a known pattern into the control buffer region.
    const pattern: u64 = 0xDEADBEEFCAFEBABE;
    @memcpy(broker_file.control_buffer[0..8], std.mem.asBytes(&pattern));

    // Thread 2 (service): open the same file.
    var service_view = try BrokerMetadataFile.open(storage_path, "test-group");
    defer service_view.close();

    // Verify the service sees the same data.
    const read_pattern = std.mem.bytesToValue(u64, service_view.control_buffer[0..8]);
    try testing.expectEqual(pattern, read_pattern);

    // Service writes to the send buffer.
    const msg: u64 = 0x1234567890ABCDEF;
    @memcpy(service_view.send_buffer[0..8], std.mem.asBytes(&msg));

    // Broker reads it back.
    const read_msg = std.mem.bytesToValue(u64, broker_file.send_buffer[0..8]);
    try testing.expectEqual(msg, read_msg);
}

test "atomic nextServiceId visible across mappings" {
    const tmp_dir = testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const storage_path = try tmp_dir.dir.realpathAlloc(testing.allocator, ".");
    defer testing.allocator.free(storage_path);
    try tmp_dir.dir.makePath("test-group/services");

    var file1 = try BrokerMetadataFile.create(storage_path, "test-group", 1, 4096, 4096);
    defer file1.close();

    var file2 = try BrokerMetadataFile.open(storage_path, "test-group");
    defer file2.close();

    // file1 sets nextServiceId.
    file1.storeNextServiceId(10);

    // file2 reads it (both map the same physical page).
    try testing.expectEqual(@as(i32, 10), file2.loadNextServiceId());

    // file2 increments.
    const new_id = file2.incrementAndGetNextServiceId();
    try testing.expectEqual(@as(i32, 11), new_id);

    // file1 sees the increment.
    try testing.expectEqual(@as(i32, 11), file1.loadNextServiceId());
}

test "concurrent incrementAndGetNextServiceId from multiple threads" {
    const tmp_dir = testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const storage_path = try tmp_dir.dir.realpathAlloc(testing.allocator, ".");
    defer testing.allocator.free(storage_path);
    try tmp_dir.dir.makePath("test-group/services");

    var file = try BrokerMetadataFile.create(storage_path, "test-group", 1, 4096, 4096);
    defer file.close();

    const num_threads = 8;
    const increments_per_thread = 1000;

    var threads: [num_threads]std.Thread = undefined;
    for (&threads) |*t| {
        t.* = try std.Thread.spawn(.{}, struct {
            fn run(f: *BrokerMetadataFile) void {
                var i: usize = 0;
                while (i < increments_per_thread) : (i += 1) {
                    _ = f.incrementAndGetNextServiceId();
                }
            }
        }.run, .{&file});
    }

    for (&threads) |*t| {
        t.join();
    }

    // Initial value was 1, plus 8 * 1000 increments.
    const expected: i32 = 1 + num_threads * increments_per_thread;
    try testing.expectEqual(expected, file.loadNextServiceId());
}
```

---

## Appendix: Memory Ordering Quick Reference

All atomic operations in this subsystem use the following ordering:

| Field | Writer | Reader | Ordering |
|-------|--------|--------|----------|
| `heartbeat_time_ms` | Service/broker (periodic) | Broker/service (health check) | `release` / `acquire` |
| `next_service_id` | Service (registration) | Broker (assignment) | `acq_rel` (fetchAdd) |
| `tail_position` (receive log) | Receiver thread | Router thread | `release` / `acquire` |
| `rebuild_position` (receive log) | Control loop | Loss detector | `release` / `acquire` |
| `writer_wait_state` (blocking) | Producer thread | Consumer thread | `release` / `acquire` + futex |
| `reader_wait_state` (blocking) | Consumer thread | Producer thread | `release` / `acquire` + futex |
| frame `length` (receive log) | Receiver thread | Router thread | `release` / `acquire` |

On x86-64, all of these except `fetchAdd` compile to plain loads/stores with compiler
fences (x86 provides TSO — total store ordering). On ARM, they compile to
`ldapr`/`stlr` pairs.

---

*Previous: [01 — Platform Abstraction](01-platform-abstraction.md)*
*Next: [03 — Concurrent Data Structures](03-concurrent-data-structures.md)*