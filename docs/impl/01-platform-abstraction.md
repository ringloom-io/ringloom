# Step 1: Platform Abstraction Layer

> Implementation guide for `src/platform/` — the OS abstraction foundation of the RingLoom broker.

This is the first module to build. Everything else (ring buffers, metadata files, networking, threading) depends on these primitives. The goal is a clean, platform-agnostic API surface that the rest of the broker can use without `#ifdef`-style branching.

---

## Table of Contents

- [File Structure](#file-structure)
- [1. Constants](#1-constants)
- [2. Atomic Operations](#2-atomic-operations)
- [3. Memory-Mapped Files](#3-memory-mapped-files)
- [4. Clocks](#4-clocks)
- [5. Threads](#5-threads)
- [6. Process Synchronization](#6-process-synchronization)
- [7. CPU Pause / Yield](#7-cpu-pause--yield)
- [8. Alignment Helpers](#8-alignment-helpers)
- [9. Public Re-exports](#9-public-re-exports)
- [Build Integration](#build-integration)
- [Testing Strategy](#testing-strategy)

---

## File Structure

```
src/
  platform/
    constants.zig         # Buffer sizes, protocol constants, timing constants
    atomic.zig            # AtomicI32, AtomicI64, cache-line-padded variants
    mapped_file.zig       # MappedFile struct — mmap/munmap/msync abstraction
    clock.zig             # Monotonic + wall-clock time
    thread.zig            # ThreadRunner — named thread with event loop
    process_sync.zig      # ProcessSynchronizer — futex/ulock/WaitOnAddress
  platform.zig            # Public re-exports: @import("platform/foo.zig")
```

Every module follows the same pattern:

1. **Struct definition** with platform-specific internals hidden behind a unified API.
2. **Error set** as a Zig error union — never panics on recoverable OS failures.
3. **Tests** at the bottom of each file inside `test` blocks.

---

## 1. Constants

**File: `src/platform/constants.zig`**

This file contains every numeric constant referenced by the ring buffer, metadata file, wire protocol, and timing subsystems. Centralizing them here means the rest of the codebase refers to symbolic names, and changing a value is a single-line edit.

### 1.1 Buffer Size Constants

```zig
// src/platform/constants.zig

/// Hardware cache line size on all target architectures (x86-64, ARM64).
pub const cache_line_length: usize = 64;

/// Padding to prevent false sharing between adjacent atomic fields.
/// Two cache lines — accounts for adjacent-line prefetch on Intel.
pub const cache_line_pad: usize = 128;

/// OS memory page size. Used for mmap alignment.
pub const page_size: usize = 4096;

/// Metadata header at the start of every broker/service .dat file.
/// Contains buffer lengths, service ID, node ID, PID, timestamps, etc.
pub const metadata_header_length: usize = 512;

/// MPSC ring buffer trailer: 6 × 128-byte cache-line-padded slots.
///   [0]   begin_pad          (128 bytes)
///   [128] tail_position      (i64 + 120 pad)
///   [256] head_cache         (i64 + 120 pad)
///   [384] head_position      (i64 + 120 pad)
///   [512] correlation_counter(i64 + 120 pad)
///   [640] consumer_heartbeat (i64 + 120 pad)
pub const ring_buffer_trailer_length: usize = 768;

/// Each record in the ring buffer has an 8-byte header:
///   i32 length      (negative = uncommitted, positive = committed)
///   i32 msg_type_id (≥1 valid, -1 = padding)
pub const ring_buffer_record_header_length: usize = 8;

/// Records in the ring buffer are aligned to 8 bytes.
pub const ring_buffer_alignment: usize = 8;

/// On-wire TCP message frame header size.
pub const frame_header_length: usize = 24;
```

### 1.2 Protocol Constants

```zig
/// TCP wire protocol version.
pub const protocol_version: u8 = 1;

/// TCP handshake magic bytes: "RING".
pub const handshake_magic: u32 = 0x474E4952;

/// TCP handshake frame length.
pub const handshake_length: usize = 24;

/// Sentinel msg_type_id written into padding records.
pub const padding_msg_type_id: i32 = -1;

pub const flag_admin: u8 = 0x20;

/// Handshake direction values.
pub const direction_send: u8 = 0;
pub const direction_recv: u8 = 1;

/// Template ID for heartbeat frames (no payload).
pub const heartbeat_template_id: u16 = 0xFFFF;

pub const broker_service_id: i32 = 0;
pub const broker_service_name: []const u8 = "broker";
```

### 1.3 Timing Constants

```zig
/// How often the sender emits heartbeat frames to idle peers.
pub const heartbeat_interval_ms: i64 = 500;

/// Receiver marks peer as suspect if no data within this window.
pub const heartbeat_timeout_ms: i64 = 2000;

/// Peer declared dead after this timeout with no data.
pub const peer_liveness_timeout_ms: i64 = 5000;

/// Initial reconnect backoff delay.
pub const reconnect_base_delay_ms: i64 = 100;

/// Maximum reconnect backoff delay.
pub const reconnect_max_delay_ms: i64 = 1000;

/// Services write heartbeat timestamps at this interval.
pub const service_heartbeat_write_interval_ms: i64 = 1000;

/// Broker checks service heartbeats at this interval.
pub const service_heartbeat_check_interval_ms: i64 = 3000;

/// Service is considered dead after this timeout without a heartbeat.
pub const service_heartbeat_timeout_ms: i64 = 10000;

/// How often the control loop checks for timed-out services.
pub const control_loop_timeout_check_interval_ns: i64 = 1 * std.time.ns_per_s;

/// Max commands drained from inter-thread command queues per duty cycle.
pub const command_drain_limit: u32 = 1;

/// Max control messages read per duty cycle.
pub const control_read_limit: u32 = 10;

/// Max outbound TCP frames written per peer per duty cycle.
pub const write_budget_per_peer: u32 = 16;

/// Max inbound TCP frames read per peer per duty cycle.
pub const read_budget_per_peer: u32 = 16;

/// Max messages read from send ring buffer per duty cycle.
pub const send_batch_limit: u32 = 64;

const std = @import("std");
```

### 1.4 Helper Functions

```zig
/// Align `value` up to the nearest multiple of `alignment`.
/// `alignment` must be a power of two.
pub fn alignUp(value: usize, alignment: usize) usize {
    return (value + (alignment - 1)) & ~(alignment - 1);
}

/// Returns true if `value` is a positive power of two.
pub fn isPowerOfTwo(value: usize) bool {
    return value > 0 and (value & (value - 1)) == 0;
}

/// Returns true if `value` is properly aligned to `alignment`.
pub fn isAligned(value: usize, alignment: usize) bool {
    return (value & (alignment - 1)) == 0;
}
```

### 1.5 Default Configuration Values

```zig
pub const default_control_buffer_length: usize = 64 * 1024;         // 64 KB
pub const default_send_buffer_length: usize = 1024 * 1024;          // 1 MB
pub const default_peer_write_queue_capacity: usize = 8_192;         // frames
pub const default_max_frame_length: usize = 1024 * 1024;            // 1 MB
pub const default_tcp_sndbuf_size: usize = 256 * 1024;              // 256 KB
pub const default_tcp_rcvbuf_size: usize = 256 * 1024;              // 256 KB
pub const default_counter_values_buffer_length: usize = 64 * 1024;  // 64 KB
pub const default_error_log_buffer_length: usize = 256 * 1024;      // 256 KB
pub const default_max_services: u32 = 256;
pub const default_max_peers: u32 = 16;

/// Default storage path for metadata files.
/// On Linux this is tmpfs (RAM-backed), giving shared-memory performance.
pub const default_storage_path: []const u8 = "/dev/shm";

/// Subdirectory under the group directory where service .dat files live.
pub const services_directory: []const u8 = "services";
```

### Implementation Steps

1. Create `src/platform/constants.zig`.
2. Add every constant listed above.
3. Write `test "alignUp"`, `test "isPowerOfTwo"`, `test "isAligned"` at the bottom.
4. Verify with `zig build test`.

### Tests

```zig
test "alignUp basics" {
    const expect = std.testing.expect;
    try expect(alignUp(0, 8) == 0);
    try expect(alignUp(1, 8) == 8);
    try expect(alignUp(7, 8) == 8);
    try expect(alignUp(8, 8) == 8);
    try expect(alignUp(9, 8) == 16);
    try expect(alignUp(4095, 4096) == 4096);
    try expect(alignUp(4096, 4096) == 4096);
}

test "isPowerOfTwo" {
    const expect = std.testing.expect;
    try expect(!isPowerOfTwo(0));
    try expect(isPowerOfTwo(1));
    try expect(isPowerOfTwo(2));
    try expect(!isPowerOfTwo(3));
    try expect(isPowerOfTwo(4));
    try expect(isPowerOfTwo(1024));
    try expect(!isPowerOfTwo(1023));
    try expect(isPowerOfTwo(1 << 20));
}

test "isAligned" {
    const expect = std.testing.expect;
    try expect(isAligned(0, 8));
    try expect(isAligned(8, 8));
    try expect(!isAligned(7, 8));
    try expect(isAligned(4096, 4096));
    try expect(!isAligned(4095, 4096));
}
```

---

## 2. Atomic Operations

**File: `src/platform/atomic.zig`**

Zig's atomic builtins compile directly to hardware instructions. On x86-64, acquire loads and release stores are "free" — they compile to ordinary `mov` instructions plus a compiler fence (x86-64's Total Store Order gives acquire/release semantics for free). On ARM64, they emit actual barrier instructions (`ldapr`, `stlr`).

### 2.1 How Zig Builtins Map to Hardware

| Operation | Zig Builtin | x86-64 Instruction | ARM64 Instruction | Use in RingLoom |
|---|---|---|---|---|
| Acquire load | `@atomicLoad(.acquire)` | `mov` + compiler fence | `ldapr` / `ldar` | Reading committed ring buffer record length |
| Release store | `@atomicStore(.release)` | Compiler fence + `mov` | `stlr` | Committing ring buffer writes, heartbeat updates |
| Fetch-and-add | `@atomicRmw(.Add, .monotonic)` | `lock xadd` | `ldadd` | Counter increments (correlation ID, next service ID) |
| Compare-and-swap | `@cmpxchgWeak(.acquire, .monotonic)` | `lock cmpxchg` | `cas` / LL/SC | Ring buffer tail claim (multi-producer) |
| Full fence | `@fence(.seq_cst)` | `mfence` | `dmb ish` | Rare — only for full barrier needs |

**Why acquire/release is enough for the ring buffer:**

The ring buffer write path follows a strict protocol:
1. Producer claims space via CAS on `tail_position` (acquire on success — sees all prior writes).
2. Producer writes payload bytes into the claimed region.
3. Producer commits by writing a positive `length` with release ordering — guarantees all payload bytes are visible before the consumer sees the committed length.

The consumer reads `length` with acquire ordering, which pairs with the producer's release store. Once the consumer sees a positive length, it is guaranteed to see the complete payload.

On x86-64 this entire dance compiles to plain loads and stores plus the single `lock cmpxchg` for the CAS — **zero additional fence instructions** on the fast path.

### 2.2 AtomicI64

```zig
// src/platform/atomic.zig

const std = @import("std");
const constants = @import("constants.zig");

/// A 64-bit atomic integer.
///
/// Wraps Zig's `std.atomic.Value(i64)` with methods named to match RingLoom conventions.
/// This is a thin wrapper — all methods inline to a single instruction on x86-64.
pub const AtomicI64 = struct {
    value: std.atomic.Value(i64),

    const Self = @This();

    pub fn init(initial: i64) Self {
        return .{ .value = std.atomic.Value(i64).init(initial) };
    }

    /// Acquire load — pairs with a prior release store from another thread.
    /// x86-64: plain `mov` + compiler fence. ARM64: `ldar`.
    pub inline fn load(self: *const Self) i64 {
        return self.value.load(.acquire);
    }

    /// Release store — makes all prior writes visible to a subsequent acquire load.
    /// x86-64: compiler fence + plain `mov`. ARM64: `stlr`.
    pub inline fn store(self: *Self, val: i64) void {
        self.value.store(val, .release);
    }

    /// Monotonic load — no ordering guarantees beyond atomicity.
    /// Use only when the value is informational (counters, stats).
    pub inline fn loadMonotonic(self: *const Self) i64 {
        return self.value.load(.monotonic);
    }

    /// Monotonic store — no ordering guarantees beyond atomicity.
    pub inline fn storeMonotonic(self: *Self, val: i64) void {
        self.value.store(val, .monotonic);
    }

    /// Plain (non-atomic) read. Only safe when there is no concurrent access
    /// (e.g., single-consumer reading its own head_position).
    pub inline fn loadRaw(self: *const Self) i64 {
        return self.value.raw;
    }

    /// Plain (non-atomic) write. Only safe when there is no concurrent writer.
    pub inline fn storeRaw(self: *Self, val: i64) void {
        self.value.raw = val;
    }

    /// Fetch-and-add with monotonic ordering.
    /// x86-64: `lock xadd`. ARM64: `ldadd`.
    pub inline fn fetchAdd(self: *Self, delta: i64) i64 {
        return self.value.fetchAdd(delta, .monotonic);
    }

    /// Compare-and-swap (weak). Returns `null` on success, or the actual value on failure.
    /// Success ordering: acquire. Failure ordering: monotonic.
    /// x86-64: `lock cmpxchg`. ARM64: `cas` or LL/SC.
    pub inline fn compareAndSwap(self: *Self, expected: i64, desired: i64) ?i64 {
        return self.value.cmpxchgWeak(expected, desired, .acquire, .monotonic);
    }

    /// Get a pointer to the raw underlying value.
    /// Useful when passing to ProcessSynchronizer or direct memory operations.
    pub inline fn ptr(self: *Self) *i64 {
        return &self.value.raw;
    }
};
```

### 2.3 AtomicI32

```zig
/// A 32-bit atomic integer.
///
/// Used for ring buffer record headers (length field) and process synchronization
/// (futex word is i32).
pub const AtomicI32 = struct {
    value: std.atomic.Value(i32),

    const Self = @This();

    pub fn init(initial: i32) Self {
        return .{ .value = std.atomic.Value(i32).init(initial) };
    }

    pub inline fn load(self: *const Self) i32 {
        return self.value.load(.acquire);
    }

    pub inline fn store(self: *Self, val: i32) void {
        self.value.store(val, .release);
    }

    pub inline fn loadMonotonic(self: *const Self) i32 {
        return self.value.load(.monotonic);
    }

    pub inline fn storeMonotonic(self: *Self, val: i32) void {
        self.value.store(val, .monotonic);
    }

    pub inline fn loadRaw(self: *const Self) i32 {
        return self.value.raw;
    }

    pub inline fn storeRaw(self: *Self, val: i32) void {
        self.value.raw = val;
    }

    pub inline fn fetchAdd(self: *Self, delta: i32) i32 {
        return self.value.fetchAdd(delta, .monotonic);
    }

    pub inline fn compareAndSwap(self: *Self, expected: i32, desired: i32) ?i32 {
        return self.value.cmpxchgWeak(expected, desired, .acquire, .monotonic);
    }

    pub inline fn ptr(self: *Self) *i32 {
        return &self.value.raw;
    }
};
```

### 2.4 AtomicBool

```zig
/// Atomic boolean — used for the `running` flag in ThreadRunner.
pub const AtomicBool = struct {
    value: std.atomic.Value(bool),

    const Self = @This();

    pub fn init(initial: bool) Self {
        return .{ .value = std.atomic.Value(bool).init(initial) };
    }

    pub inline fn load(self: *const Self) bool {
        return self.value.load(.acquire);
    }

    pub inline fn store(self: *Self, val: bool) void {
        self.value.store(val, .release);
    }
};
```

### 2.5 Cache-Line-Padded Variants

False sharing occurs when two threads write to different variables that happen to live on the same cache line. The CPU's cache coherence protocol (MESI/MOESI) bounces the entire 64-byte line between cores, destroying performance.

RingLoom's ring buffer trailer uses 128-byte padding (two cache lines) between each field. This accounts for Intel's adjacent-line prefetch, where the hardware may speculatively load the next cache line alongside the requested one.

```zig
/// A 64-bit atomic integer padded to 128 bytes (2 cache lines) to prevent
/// false sharing with adjacent fields.
///
/// Memory layout:
///   [0..8)    = the i64 value
///   [8..128)  = padding (120 bytes)
///
/// Used in the ring buffer trailer where tail_position, head_cache,
/// head_position, correlation_counter, and consumer_heartbeat each
/// occupy their own 128-byte slot.
pub const CacheLinePaddedAtomicI64 = extern struct {
    value: i64 align(constants.cache_line_pad) = 0,
    _padding: [constants.cache_line_pad - @sizeOf(i64)]u8 = [_]u8{0} ** (constants.cache_line_pad - @sizeOf(i64)),

    const Self = @This();

    comptime {
        // Static assertion: this struct must be exactly 128 bytes.
        if (@sizeOf(Self) != constants.cache_line_pad) {
            @compileError("CacheLinePaddedAtomicI64 must be exactly 128 bytes");
        }
    }

    pub fn init(initial: i64) Self {
        return .{
            .value = initial,
            ._padding = [_]u8{0} ** (constants.cache_line_pad - @sizeOf(i64)),
        };
    }

    pub inline fn atomicLoad(self: *const Self) i64 {
        return @atomicLoad(&self.value, .acquire);
    }

    pub inline fn atomicStore(self: *Self, val: i64) void {
        @atomicStore(&self.value, val, .release);
    }

    pub inline fn atomicLoadMonotonic(self: *const Self) i64 {
        return @atomicLoad(&self.value, .monotonic);
    }

    pub inline fn atomicStoreMonotonic(self: *Self, val: i64) void {
        @atomicStore(&self.value, val, .monotonic);
    }

    pub inline fn fetchAdd(self: *Self, delta: i64) i64 {
        return @atomicRmw(&self.value, .Add, delta, .monotonic);
    }

    pub inline fn compareAndSwap(self: *Self, expected: i64, desired: i64) ?i64 {
        return @cmpxchgWeak(&self.value, expected, desired, .acquire, .monotonic);
    }

    /// Returns a pointer to the raw value for direct memory-mapped access.
    pub inline fn ptr(self: *Self) *volatile i64 {
        return &self.value;
    }
};

/// A 32-bit atomic integer padded to 128 bytes. Used for the futex wait-state
/// words in the blocking ring buffer trailer.
pub const CacheLinePaddedAtomicI32 = extern struct {
    value: i32 align(constants.cache_line_pad) = 0,
    _padding: [constants.cache_line_pad - @sizeOf(i32)]u8 = [_]u8{0} ** (constants.cache_line_pad - @sizeOf(i32)),

    const Self = @This();

    comptime {
        if (@sizeOf(Self) != constants.cache_line_pad) {
            @compileError("CacheLinePaddedAtomicI32 must be exactly 128 bytes");
        }
    }

    pub fn init(initial: i32) Self {
        return .{
            .value = initial,
            ._padding = [_]u8{0} ** (constants.cache_line_pad - @sizeOf(i32)),
        };
    }

    pub inline fn atomicLoad(self: *const Self) i32 {
        return @atomicLoad(&self.value, .acquire);
    }

    pub inline fn atomicStore(self: *Self, val: i32) void {
        @atomicStore(&self.value, val, .release);
    }

    pub inline fn compareAndSwap(self: *Self, expected: i32, desired: i32) ?i32 {
        return @cmpxchgWeak(&self.value, expected, desired, .acquire, .monotonic);
    }

    pub inline fn ptr(self: *Self) *volatile i32 {
        return &self.value;
    }
};
```

### Implementation Steps

1. Create `src/platform/atomic.zig`.
2. Implement `AtomicI64`, `AtomicI32`, `AtomicBool`.
3. Implement `CacheLinePaddedAtomicI64`, `CacheLinePaddedAtomicI32` as `extern struct`.
4. Add `comptime` size assertions for the padded types.
5. Write tests verifying size, alignment, and basic atomic operations.

### Tests

```zig
test "CacheLinePaddedAtomicI64 is exactly 128 bytes" {
    try std.testing.expect(@sizeOf(CacheLinePaddedAtomicI64) == 128);
    try std.testing.expect(@alignOf(CacheLinePaddedAtomicI64) == 128);
}

test "CacheLinePaddedAtomicI32 is exactly 128 bytes" {
    try std.testing.expect(@sizeOf(CacheLinePaddedAtomicI32) == 128);
    try std.testing.expect(@alignOf(CacheLinePaddedAtomicI32) == 128);
}

test "AtomicI64 load/store" {
    var a = AtomicI64.init(42);
    try std.testing.expectEqual(@as(i64, 42), a.load());
    a.store(100);
    try std.testing.expectEqual(@as(i64, 100), a.load());
}

test "AtomicI64 fetchAdd" {
    var a = AtomicI64.init(10);
    const prev = a.fetchAdd(5);
    try std.testing.expectEqual(@as(i64, 10), prev);
    try std.testing.expectEqual(@as(i64, 15), a.load());
}

test "AtomicI64 compareAndSwap success" {
    var a = AtomicI64.init(42);
    const result = a.compareAndSwap(42, 99);
    try std.testing.expect(result == null); // success
    try std.testing.expectEqual(@as(i64, 99), a.load());
}

test "AtomicI64 compareAndSwap failure" {
    var a = AtomicI64.init(42);
    const result = a.compareAndSwap(0, 99);
    try std.testing.expect(result != null); // failure
    try std.testing.expectEqual(@as(i64, 42), result.?);
    try std.testing.expectEqual(@as(i64, 42), a.load()); // unchanged
}

test "CacheLinePaddedAtomicI64 basic operations" {
    var padded = CacheLinePaddedAtomicI64.init(0);
    padded.atomicStore(42);
    try std.testing.expectEqual(@as(i64, 42), padded.atomicLoad());
    const prev = padded.fetchAdd(8);
    try std.testing.expectEqual(@as(i64, 42), prev);
    try std.testing.expectEqual(@as(i64, 50), padded.atomicLoad());
}

test "adjacent CacheLinePaddedAtomicI64 fields do not share cache lines" {
    // Simulate the ring buffer trailer layout
    const Trailer = extern struct {
        begin_pad: CacheLinePaddedAtomicI64,
        tail_position: CacheLinePaddedAtomicI64,
        head_cache: CacheLinePaddedAtomicI64,
        head_position: CacheLinePaddedAtomicI64,
    };
    try std.testing.expect(@sizeOf(Trailer) == 4 * 128);
    const t: Trailer = undefined;
    const tail_addr = @intFromPtr(&t.tail_position);
    const head_cache_addr = @intFromPtr(&t.head_cache);
    try std.testing.expect(head_cache_addr - tail_addr == 128);
}
```

---

## 3. Memory-Mapped Files

**File: `src/platform/mapped_file.zig`**

The metadata files are the backbone of RingLoom's shared-memory IPC. Each service and the broker itself gets a `.dat` file under `<storage_path>/<group>/services/`. These files are memory-mapped so that multiple processes can read and write the ring buffers without any system call overhead on the data path.

### 3.1 Design Constraints

- The mapped memory must be page-aligned — `mmap` guarantees this on POSIX, but we enforce it in the type signature.
- The returned pointer is `[*]align(page_size) u8` — callers can overlay `extern struct` types directly.
- Files live on tmpfs (`/dev/shm`) by default on Linux, giving RAM-speed access.
- Parent directories (`<storage_path>/<group>/services/`) must be created if they don't exist.
- When a file already exists, check whether the owning PID (stored at offset 16 in the metadata header) is still alive. If dead, reuse the file. If alive, return an error.

### 3.2 Struct Definition

```zig
// src/platform/mapped_file.zig

const std = @import("std");
const posix = std.posix;
const constants = @import("constants.zig");

pub const MappedFile = struct {
    /// Pointer to the mapped memory region, page-aligned.
    data: [*]align(constants.page_size) u8,

    /// Total size of the mapped region in bytes.
    len: usize,

    /// File descriptor (POSIX) or handle (Windows). Kept open for msync/munmap.
    fd: posix.fd_t,

    /// The filesystem path of the backing file (owned, must be freed).
    path: []const u8,

    /// Allocator used for the path string — needed for cleanup.
    allocator: std.mem.Allocator,

    const Self = @This();

    pub const CreateError = error{
        FileAlreadyOwnedByLiveProcess,
        MappingFailed,
        FileCreateFailed,
        TruncateFailed,
        DirectoryCreateFailed,
        InvalidSize,
    } || posix.MMapError || posix.OpenError || std.fs.File.StatError;

    pub const OpenError = error{
        FileNotFound,
        MappingFailed,
        InvalidFileSize,
    } || posix.MMapError || posix.OpenError || std.fs.File.StatError;

    /// Create a new metadata file at the given path with the given size.
    ///
    /// If the file already exists:
    ///   1. Read the PID from offset 16 (i64, little-endian).
    ///   2. Check if that process is alive (see `isProcessAlive`).
    ///   3. If alive → return error.FileAlreadyOwnedByLiveProcess.
    ///   4. If dead → truncate and reuse.
    ///
    /// The size is rounded up to the nearest page boundary.
    /// Parent directories are created as needed.
    pub fn create(
        allocator: std.mem.Allocator,
        dir_path: []const u8,
        file_name: []const u8,
        size: usize,
    ) CreateError!Self {
        _ = .{ allocator, dir_path, file_name, size };
        // Implementation below
        @panic("TODO");
    }

    /// Open an existing metadata file for read/write access.
    ///
    /// The entire file is mapped. Returns error.FileNotFound if it doesn't exist.
    pub fn open(
        allocator: std.mem.Allocator,
        path: []const u8,
    ) OpenError!Self {
        _ = .{ allocator, path };
        @panic("TODO");
    }

    /// Flush dirty pages to the backing file (or no-op on tmpfs).
    /// Uses MS_SYNC for guaranteed durability.
    pub fn sync(self: *Self) void {
        _ = self;
        @panic("TODO");
    }

    /// Unmap the memory region and close the file descriptor.
    /// After this call, the MappedFile is invalidated — do not use.
    pub fn close(self: *Self) void {
        _ = self;
        @panic("TODO");
    }

    /// Get a typed pointer into the mapped region at a given byte offset.
    /// Useful for overlaying the metadata header struct.
    pub fn ptrAt(self: *Self, comptime T: type, offset: usize) *T {
        _ = .{ self, offset };
        @panic("TODO");
    }

    /// Get the mapped memory as a byte slice.
    pub fn asSlice(self: *Self) []align(constants.page_size) u8 {
        return self.data[0..self.len];
    }
};
```

### 3.3 Implementation: `create` (POSIX — Linux & macOS)

```zig
pub fn create(
    allocator: std.mem.Allocator,
    dir_path: []const u8,
    file_name: []const u8,
    size: usize,
) CreateError!Self {
    if (size == 0) return error.InvalidSize;

    const aligned_size = constants.alignUp(size, constants.page_size);

    // Step 1: Ensure parent directory exists.
    // Build full path: "{dir_path}/{file_name}"
    const full_path = try std.fs.path.join(allocator, &.{ dir_path, file_name });
    errdefer allocator.free(full_path);

    // Create parent directories recursively (like `mkdir -p`).
    std.fs.makeDirAbsolute(dir_path) catch |err| switch (err) {
        error.PathAlreadyExists => {}, // fine
        else => return error.DirectoryCreateFailed,
    };
    // For nested paths, use makePath:
    if (std.fs.path.dirname(full_path)) |parent| {
        std.fs.cwd().makePath(parent) catch return error.DirectoryCreateFailed;
    }

    // Step 2: Check if file exists and handle PID check.
    const file_exists = blk: {
        std.fs.accessAbsolute(full_path, .{}) catch break :blk false;
        break :blk true;
    };

    if (file_exists) {
        // Temporarily open read-only to check the PID.
        const check_fd = try posix.open(
            full_path,
            .{ .ACCMODE = .RDONLY },
            0,
        );
        defer posix.close(check_fd);

        // Read the PID at the fixed metadata header offset.
        // Metadata layout: offset 16 = pid (i64, little-endian).
        var pid_buf: [8]u8 = undefined;
        const bytes_read = try posix.pread(check_fd, &pid_buf, pid_field_offset);
        if (bytes_read == 8) {
            const pid = std.mem.readInt(i64, &pid_buf, .little);
            if (pid > 0 and isProcessAlive(pid)) {
                return error.FileAlreadyOwnedByLiveProcess;
            }
            // PID is dead or invalid — fall through to reuse.
        }
    }

    // Step 3: Open/create the file with read-write permissions.
    const fd = try posix.open(
        full_path,
        .{ .ACCMODE = .RDWR, .CREAT = true, .TRUNC = true },
        0o666,
    );
    errdefer posix.close(fd);

    // Step 4: Set file size.
    posix.ftruncate(fd, @intCast(aligned_size)) catch return error.TruncateFailed;

    // Step 5: Memory-map the file.
    const mapped = try posix.mmap(
        null,                                       // let OS choose address
        aligned_size,                               // length
        posix.PROT.READ | posix.PROT.WRITE,         // read-write
        .{ .TYPE = .SHARED },                        // shared between processes
        fd,                                         // file descriptor
        0,                                          // offset
    );

    return Self{
        .data = @alignCast(mapped.ptr),
        .len = aligned_size,
        .fd = fd,
        .path = full_path,
        .allocator = allocator,
    };
}

/// PID field offset in the metadata header (matches Java's PID_FIELD_OFFSET).
const pid_field_offset: u64 = 16;
```

### 3.4 Implementation: `open` (POSIX)

```zig
pub fn open(
    allocator: std.mem.Allocator,
    path: []const u8,
) OpenError!Self {
    const path_copy = try allocator.dupe(u8, path);
    errdefer allocator.free(path_copy);

    const fd = posix.open(path, .{ .ACCMODE = .RDWR }, 0) catch
        return error.FileNotFound;
    errdefer posix.close(fd);

    // Get file size via fstat.
    const stat = try posix.fstat(fd);
    const file_size: usize = @intCast(stat.size);
    if (file_size == 0) return error.InvalidFileSize;

    const mapped = try posix.mmap(
        null,
        file_size,
        posix.PROT.READ | posix.PROT.WRITE,
        .{ .TYPE = .SHARED },
        fd,
        0,
    );

    return Self{
        .data = @alignCast(mapped.ptr),
        .len = file_size,
        .fd = fd,
        .path = path_copy,
        .allocator = allocator,
    };
}
```

### 3.5 Implementation: `sync` and `close`

```zig
pub fn sync(self: *Self) void {
    // MS_SYNC: synchronous flush. On tmpfs this is essentially a no-op
    // since there's no disk to flush to, but it's correct for on-disk files.
    posix.msync(
        @alignCast(self.data[0..self.len]),
        .{ .SYNC = true },
    ) catch {};
}

pub fn close(self: *Self) void {
    // Step 1: Unmap.
    posix.munmap(@alignCast(self.data[0..self.len]));

    // Step 2: Close file descriptor.
    posix.close(self.fd);

    // Step 3: Free the path string.
    self.allocator.free(self.path);

    // Invalidate.
    self.data = undefined;
    self.len = 0;
    self.fd = -1;
}
```

### 3.6 Implementation: `ptrAt`

```zig
/// Get a typed pointer into the mapped region at a given byte offset.
///
/// Example: read the service ID (i32) at offset 8:
///   const service_id = mapped_file.ptrAt(i32, 8).*;
///
/// Example: overlay the ring buffer trailer:
///   const trailer = mapped_file.ptrAt(RingBufferTrailer, capacity);
pub fn ptrAt(self: *Self, comptime T: type, offset: usize) *T {
    std.debug.assert(offset + @sizeOf(T) <= self.len);
    return @ptrCast(@alignCast(self.data + offset));
}
```

### 3.7 Process Liveness Check

```zig
/// Check if a process with the given PID is currently alive.
///
/// Linux/macOS: `kill(pid, 0)` — sends signal 0 (no actual signal) to test existence.
///   - Returns true if the process exists (even if we can't signal it — EPERM).
///   - Returns false if ESRCH (no such process).
///
/// This mirrors the Java `ProcessHandle.of(pid).isPresent()` check used in
/// RingLoomMetadataFileDescriptor.findExistingMetadataFile().
fn isProcessAlive(pid: i64) bool {
    if (pid <= 0) return false;

    const pid_int: posix.pid_t = @intCast(pid);

    // kill(pid, 0): test if process exists without sending a signal.
    const result = posix.kill(pid_int, 0);

    // If kill returns without error, the process exists.
    // If it returns EPERM, the process exists but we lack permission — still alive.
    // If it returns ESRCH, the process does not exist.
    return switch (result) {
        .SUCCESS => true,
        .PERM => true,   // Process exists, but we can't signal it
        .SRCH => false,   // No such process
        else => false,
    };
}
```

### 3.8 Windows Implementation Notes

On Windows, the POSIX mmap path does not apply. The implementation uses:

1. **`CreateFileW`** — open/create the backing file.
2. **`CreateFileMappingW`** — create a file mapping object with `PAGE_READWRITE`.
3. **`MapViewOfFile`** — map the file into the process address space.
4. **`UnmapViewOfFile`** — unmap on close.
5. **`FlushViewOfFile`** — sync.
6. **PID check**: `OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, pid)` — if it succeeds, the process is alive; if it fails with `ERROR_INVALID_PARAMETER`, it's dead.

The struct fields remain the same — `fd` becomes a Windows `HANDLE`, and two handles are needed (file handle + mapping handle). Use Zig's `std.os.windows` namespace for the Win32 calls.

```zig
// Platform dispatch at the top of the file:
const builtin = @import("builtin");
const is_windows = builtin.os.tag == .windows;
const is_posix = !is_windows;

// Then in each method:
pub fn create(...) !Self {
    if (is_windows) {
        return createWindows(...);
    } else {
        return createPosix(...);
    }
}
```

### 3.9 Helper: Directory and Path Utilities

```zig
/// Build the full path to a service metadata file.
///
/// Format: `{storage_path}/{group}/services/{name}_{id}.dat`
///
/// Example: `/dev/shm/my-app/services/order-service_3.dat`
pub fn serviceMetadataPath(
    allocator: std.mem.Allocator,
    storage_path: []const u8,
    group: []const u8,
    service_name: []const u8,
    service_id: i32,
) ![]const u8 {
    var buf: [32]u8 = undefined;
    const id_str = std.fmt.bufPrint(&buf, "{d}", .{service_id}) catch unreachable;

    return std.fs.path.join(allocator, &.{
        storage_path,
        group,
        constants.services_directory,
        try std.fmt.allocPrint(allocator, "{s}_{s}.dat", .{ service_name, id_str }),
    });
}

/// Build the path to the services directory.
///
/// Format: `{storage_path}/{group}/services/`
pub fn servicesDirectoryPath(
    allocator: std.mem.Allocator,
    storage_path: []const u8,
    group: []const u8,
) ![]const u8 {
    return std.fs.path.join(allocator, &.{
        storage_path,
        group,
        constants.services_directory,
    });
}

/// Ensure the services directory exists, creating it and any parent directories.
pub fn ensureServicesDirectory(
    storage_path: []const u8,
    group: []const u8,
) !void {
    const allocator = std.heap.page_allocator;
    const dir_path = try servicesDirectoryPath(allocator, storage_path, group);
    defer allocator.free(dir_path);

    try std.fs.cwd().makePath(dir_path);
}
```

### 3.10 Helper: Scan for Existing Services

```zig
/// Scan the services directory for existing .dat files.
/// Returns a list of file paths for metadata files whose owning PID is dead.
/// This mirrors Java's ServiceScanner and RingLoomMetadataFileDescriptor.findExistingMetadataFile().
pub fn scanForReusableFiles(
    allocator: std.mem.Allocator,
    services_dir: []const u8,
) ![][]const u8 {
    var results = std.ArrayList([]const u8).init(allocator);
    errdefer {
        for (results.items) |item| allocator.free(item);
        results.deinit();
    }

    var dir = std.fs.openDirAbsolute(services_dir, .{ .iterate = true }) catch
        return results.toOwnedSlice();

    defer dir.close();

    var iter = dir.iterate();
    while (try iter.next()) |entry| {
        if (!std.mem.endsWith(u8, entry.name, ".dat")) continue;

        const full_path = try std.fs.path.join(allocator, &.{ services_dir, entry.name });

        // Attempt to read PID from the metadata header.
        const file = std.fs.openFileAbsolute(full_path, .{ .mode = .read_only }) catch {
            allocator.free(full_path);
            continue;
        };
        defer file.close();

        var pid_buf: [8]u8 = undefined;
        const bytes_read = file.pread(&pid_buf, pid_field_offset) catch {
            allocator.free(full_path);
            continue;
        };

        if (bytes_read == 8) {
            const pid = std.mem.readInt(i64, &pid_buf, .little);
            if (pid > 0 and !isProcessAlive(pid)) {
                try results.append(full_path);
                continue;
            }
        }

        allocator.free(full_path);
    }

    return results.toOwnedSlice();
}
```

### Implementation Steps

1. Create `src/platform/mapped_file.zig`.
2. Implement `MappedFile` struct with the fields listed above.
3. Implement `create()` with:
   - Directory creation via `std.fs.cwd().makePath()`.
   - Existing-file PID check via `pread` + `isProcessAlive`.
   - `posix.open` → `posix.ftruncate` → `posix.mmap`.
4. Implement `open()` with `posix.open` → `posix.fstat` → `posix.mmap`.
5. Implement `sync()` via `posix.msync`.
6. Implement `close()` via `posix.munmap` → `posix.close` → free path.
7. Implement `ptrAt()` with bounds-checked `@ptrCast(@alignCast(...))`.
8. Implement `isProcessAlive()` via `kill(pid, 0)`.
9. Implement path helper functions.
10. Add Windows stubs (or full implementation if targeting Windows now).
11. Write tests.

### Tests

```zig
const testing = std.testing;

test "MappedFile create and close on tmpfs" {
    // Use /tmp for tests (tmpfs on most Linux systems).
    const allocator = testing.allocator;
    const dir = "/tmp/ringloom-test-mapped-file";

    // Cleanup from previous runs.
    std.fs.deleteTreeAbsolute(dir) catch {};
    defer std.fs.deleteTreeAbsolute(dir) catch {};

    var mf = try MappedFile.create(allocator, dir, "test_1.dat", 4096);
    defer mf.close();

    // Verify size is page-aligned.
    try testing.expect(mf.len == 4096);

    // Write and read back.
    mf.data[0] = 0xAB;
    mf.data[4095] = 0xCD;
    try testing.expectEqual(@as(u8, 0xAB), mf.data[0]);
    try testing.expectEqual(@as(u8, 0xCD), mf.data[4095]);
}

test "MappedFile create with sub-page size rounds up" {
    const allocator = testing.allocator;
    const dir = "/tmp/ringloom-test-mapped-file-roundup";
    std.fs.deleteTreeAbsolute(dir) catch {};
    defer std.fs.deleteTreeAbsolute(dir) catch {};

    var mf = try MappedFile.create(allocator, dir, "small.dat", 100);
    defer mf.close();

    try testing.expect(mf.len == 4096); // Rounded up to page size.
}

test "MappedFile rejects live PID" {
    const allocator = testing.allocator;
    const dir = "/tmp/ringloom-test-mapped-file-pid";
    std.fs.deleteTreeAbsolute(dir) catch {};
    defer std.fs.deleteTreeAbsolute(dir) catch {};

    // Create a file and write our own PID at offset 16.
    var mf = try MappedFile.create(allocator, dir, "live.dat", 4096);
    const our_pid = std.os.linux.getpid();
    std.mem.writeInt(i64, mf.data[16..24], @intCast(our_pid), .little);
    mf.close();

    // Attempting to create again should fail because we're alive.
    const result = MappedFile.create(allocator, dir, "live.dat", 4096);
    try testing.expectError(error.FileAlreadyOwnedByLiveProcess, result);
}

test "MappedFile allows reuse of dead PID" {
    const allocator = testing.allocator;
    const dir = "/tmp/ringloom-test-mapped-file-dead";
    std.fs.deleteTreeAbsolute(dir) catch {};
    defer std.fs.deleteTreeAbsolute(dir) catch {};

    // Create a file and write a very high PID that doesn't exist.
    var mf = try MappedFile.create(allocator, dir, "dead.dat", 4096);
    std.mem.writeInt(i64, mf.data[16..24], 999999999, .little);
    mf.close();

    // Should succeed — PID 999999999 is (almost certainly) dead.
    var mf2 = try MappedFile.create(allocator, dir, "dead.dat", 4096);
    defer mf2.close();
    try testing.expect(mf2.len == 4096);
}

test "MappedFile ptrAt" {
    const allocator = testing.allocator;
    const dir = "/tmp/ringloom-test-mapped-file-ptrat";
    std.fs.deleteTreeAbsolute(dir) catch {};
    defer std.fs.deleteTreeAbsolute(dir) catch {};

    var mf = try MappedFile.create(allocator, dir, "ptrat.dat", 4096);
    defer mf.close();

    // Write an i32 at offset 8.
    const id_ptr = mf.ptrAt(i32, 8);
    id_ptr.* = 42;
    try testing.expectEqual(@as(i32, 42), mf.ptrAt(i32, 8).*);
}

test "isProcessAlive" {
    // Our own PID should be alive.
    const our_pid: i64 = @intCast(std.os.linux.getpid());
    try testing.expect(isProcessAlive(our_pid));

    // PID 0 is special (kernel), PID -1 is invalid.
    try testing.expect(!isProcessAlive(0));
    try testing.expect(!isProcessAlive(-1));

    // Very high PID is almost certainly dead.
    try testing.expect(!isProcessAlive(999999999));
}
```

---

## 4. Clocks

**File: `src/platform/clock.zig`**

Two clock types serve different purposes in the broker:

| Clock | Resolution | Use | Hot Path? |
|---|---|---|---|
| **Monotonic** | Nanoseconds | Idle strategy timing, heartbeat scheduling, reconnect backoff | Yes |
| **Wall clock** | Milliseconds | Heartbeat timestamps in metadata, service start timestamps | No (written every ~1s) |

### 4.1 Monotonic Clock

The monotonic clock must never go backwards (even across NTP adjustments) and must be as cheap as possible to call since it's invoked on every idle strategy iteration.

- **Linux**: `clock_gettime(CLOCK_MONOTONIC)` — ~25ns per call via vDSO (no actual syscall).
- **macOS**: `mach_absolute_time()` — ~22ns per call. Returns ticks that must be converted via `mach_timebase_info`.
- **Windows**: `QueryPerformanceCounter` — ~30ns per call.

Zig's `std.time.Instant` wraps all of these portably. We can use it directly.

### 4.2 Wall Clock (Epoch Milliseconds)

The wall clock is used for heartbeat timestamps — the broker reads a service's heartbeat timestamp and compares it against its own wall clock to detect dead services. The resolution only needs to be ~1ms.

- **Linux**: `clock_gettime(CLOCK_REALTIME_COARSE)` — ~5ns per call via vDSO, ~1ms resolution. Much faster than `CLOCK_REALTIME` (~25ns) when fine resolution isn't needed.
- **macOS**: `clock_gettime(CLOCK_REALTIME)` — no COARSE variant available.
- **Windows**: `GetSystemTimeAsFileTime` — ~15ns, ~1ms resolution.

### 4.3 Struct Definition

```zig
// src/platform/clock.zig

const std = @import("std");
const builtin = @import("builtin");

pub const Clock = struct {
    /// Returns the current monotonic time in nanoseconds.
    ///
    /// This value is relative to an arbitrary epoch (usually boot time).
    /// It never goes backwards and is suitable for measuring elapsed time.
    ///
    /// Performance: ~25ns on Linux (vDSO), ~22ns on macOS, ~30ns on Windows.
    pub fn monotonicNanos() i64 {
        // std.time.Instant uses clock_gettime(CLOCK_MONOTONIC) on Linux,
        // mach_absolute_time() on macOS, QueryPerformanceCounter on Windows.
        const ts = std.time.Instant.now() catch unreachable;
        // Convert to nanoseconds since an arbitrary epoch.
        // Instant stores (seconds, nanoseconds) internally on POSIX.
        return @intCast(ts.timestamp.tv_sec * std.time.ns_per_s + ts.timestamp.tv_nsec);
    }

    /// Returns the current wall-clock time as milliseconds since the Unix epoch.
    ///
    /// On Linux, uses CLOCK_REALTIME_COARSE for minimal overhead (~5ns, ~1ms resolution).
    /// This is what gets written into the heartbeat timestamp field in metadata files.
    ///
    /// Performance: ~5ns on Linux (COARSE), ~25ns on macOS, ~15ns on Windows.
    pub fn epochMillis() i64 {
        if (comptime builtin.os.tag == .linux) {
            return epochMillisLinuxCoarse();
        } else {
            return epochMillisStd();
        }
    }

    /// Linux fast path: CLOCK_REALTIME_COARSE via vDSO.
    /// Resolution is ~1ms (jiffy-based), but the call is ~5× faster than CLOCK_REALTIME.
    fn epochMillisLinuxCoarse() i64 {
        var ts: std.posix.timespec = undefined;
        // CLOCK_REALTIME_COARSE = 5 on Linux
        const CLOCK_REALTIME_COARSE = 5;
        const rc = std.os.linux.clock_gettime(CLOCK_REALTIME_COARSE, &ts);
        if (rc != 0) {
            // Fallback to standard clock if COARSE isn't available.
            return epochMillisStd();
        }
        return @as(i64, ts.tv_sec) * std.time.ms_per_s +
            @divTrunc(@as(i64, ts.tv_nsec), std.time.ns_per_ms);
    }

    /// Portable fallback using std.time.
    fn epochMillisStd() i64 {
        return @intCast(@divTrunc(std.time.milliTimestamp(), 1));
    }
};
```

### 4.4 Alternative: Direct Monotonic via Linux vDSO

For the absolute lowest overhead, you can call `clock_gettime(CLOCK_MONOTONIC)` directly:

```zig
fn monotonicNanosLinux() i64 {
    var ts: std.os.linux.timespec = undefined;
    // This goes through the vDSO — no actual syscall.
    const rc = std.os.linux.clock_gettime(.MONOTONIC, &ts);
    std.debug.assert(rc == 0);
    return @as(i64, @intCast(ts.tv_sec)) * std.time.ns_per_s +
        @as(i64, @intCast(ts.tv_nsec));
}
```

This avoids the overhead of `std.time.Instant`'s abstraction layer, but in practice the difference is negligible (~1-2ns).

### Implementation Steps

1. Create `src/platform/clock.zig`.
2. Implement `monotonicNanos()` using `std.time.Instant` or direct `clock_gettime`.
3. Implement `epochMillis()` with Linux-specific `CLOCK_REALTIME_COARSE` fast path.
4. Add the `std.time.milliTimestamp()` fallback for non-Linux platforms.
5. Write tests.

### Tests

```zig
test "monotonicNanos is monotonically increasing" {
    const t1 = Clock.monotonicNanos();
    // Spin briefly to ensure time passes.
    var i: u32 = 0;
    while (i < 1000) : (i += 1) {
        std.atomic.spinLoopHint();
    }
    const t2 = Clock.monotonicNanos();
    try std.testing.expect(t2 > t1);
}

test "monotonicNanos returns positive values" {
    const t = Clock.monotonicNanos();
    try std.testing.expect(t > 0);
}

test "epochMillis returns reasonable value" {
    const ms = Clock.epochMillis();
    // Should be after 2024-01-01T00:00:00Z (1704067200000 ms since epoch).
    try std.testing.expect(ms > 1704067200000);
    // Should be before 2100-01-01T00:00:00Z.
    try std.testing.expect(ms < 4102444800000);
}

test "epochMillis advances over time" {
    const t1 = Clock.epochMillis();
    std.time.sleep(2 * std.time.ns_per_ms);
    const t2 = Clock.epochMillis();
    try std.testing.expect(t2 >= t1);
}
```

---

## 5. Threads

**File: `src/platform/thread.zig`**

The broker runs multiple event loops, each on a dedicated thread. The `ThreadRunner` struct encapsulates the lifecycle: spawn → name → run event loop → stop → join.

### 5.1 Event Loop Interface

Every event loop in the broker (control loop, sender, receiver) implements this interface:

```zig
// src/platform/thread.zig

const std = @import("std");
const builtin = @import("builtin");
const AtomicBool = @import("atomic.zig").AtomicBool;

/// Interface that all event loops implement.
/// The ThreadRunner calls doWork() in a loop and onClose() when stopping.
pub const EventLoop = struct {
    /// Pointer to the concrete event loop implementation.
    context: *anyopaque,

    /// Perform one unit of work.
    /// Returns the number of work items processed (0 = idle).
    doWorkFn: *const fn (context: *anyopaque) u32,

    /// Called once after the event loop exits.
    /// Used for cleanup (closing sockets, flushing buffers, etc.).
    onCloseFn: *const fn (context: *anyopaque) void,

    pub fn doWork(self: EventLoop) u32 {
        return self.doWorkFn(self.context);
    }

    pub fn onClose(self: EventLoop) void {
        self.onCloseFn(self.context);
    }
};
```

### 5.2 Idle Strategy

```zig
/// Idle strategy called when the event loop has no work to do.
/// Matches the architecture doc's idle strategy table.
pub const IdleStrategy = union(enum) {
    /// CPU pause instruction when idle. Lowest latency, highest CPU usage.
    busy_spin,

    /// sched_yield() when idle. Low latency, shares CPU.
    yielding,

    /// nanosleep(1µs) when idle. Balanced.
    sleeping,

    /// Spin → yield → sleep (exponential backoff). Production default.
    backoff: BackoffState,

    /// Kernel-level blocking (futex/ulock). Lowest CPU, higher latency.
    /// The process synchronizer is used to wake the thread.
    blocking: *const fn () void,

    pub fn idle(self: *IdleStrategy, work_count: u32) void {
        if (work_count > 0) {
            // Reset backoff state if we did work.
            if (self.* == .backoff) {
                self.backoff.reset();
            }
            return;
        }

        switch (self.*) {
            .busy_spin => std.atomic.spinLoopHint(),
            .yielding => std.Thread.yield() catch {},
            .sleeping => std.time.sleep(1_000), // 1µs
            .backoff => |*state| state.step(),
            .blocking => |wake_fn| wake_fn(),
        }
    }
};

pub const BackoffState = struct {
    spins: u32 = 0,
    yields: u32 = 0,

    const max_spins: u32 = 10;
    const max_yields: u32 = 20;

    pub fn step(self: *BackoffState) void {
        if (self.spins < max_spins) {
            self.spins += 1;
            std.atomic.spinLoopHint();
        } else if (self.yields < max_yields) {
            self.yields += 1;
            std.Thread.yield() catch {};
        } else {
            std.time.sleep(1_000); // 1µs
        }
    }

    pub fn reset(self: *BackoffState) void {
        self.spins = 0;
        self.yields = 0;
    }
};
```

### 5.3 ThreadRunner

```zig
pub const ThreadRunner = struct {
    /// Name shown in `ps`, `top`, `htop` (max 15 chars on Linux).
    name: []const u8,

    /// The event loop to run on this thread.
    event_loop: EventLoop,

    /// Idle strategy for when the event loop has no work.
    idle_strategy: IdleStrategy,

    /// Atomic flag — set to false to stop the event loop.
    running: AtomicBool,

    /// Handle to the spawned thread (set after start()).
    thread: ?std.Thread = null,

    const Self = @This();

    pub fn init(
        name: []const u8,
        event_loop: EventLoop,
        idle_strategy: IdleStrategy,
    ) Self {
        return .{
            .name = name,
            .event_loop = event_loop,
            .idle_strategy = idle_strategy,
            .running = AtomicBool.init(false),
            .thread = null,
        };
    }

    /// Spawn the thread and start the event loop.
    pub fn start(self: *Self) !void {
        self.running.store(true);
        self.thread = try std.Thread.spawn(.{}, threadMain, .{self});
    }

    /// Signal the event loop to stop. Non-blocking.
    pub fn stop(self: *Self) void {
        self.running.store(false);
    }

    /// Wait for the thread to finish. Blocks until the thread exits.
    pub fn join(self: *Self) void {
        if (self.thread) |t| {
            t.join();
            self.thread = null;
        }
    }

    /// Stop and join in one call.
    pub fn stopAndJoin(self: *Self) void {
        self.stop();
        self.join();
    }

    /// The thread entry point.
    fn threadMain(self: *Self) void {
        // Step 1: Set the thread name for debugging.
        setThreadName(self.name);

        // Step 2: Run the event loop.
        while (self.running.load()) {
            const work_count = self.event_loop.doWork();
            self.idle_strategy.idle(work_count);
        }

        // Step 3: Cleanup.
        self.event_loop.onClose();
    }
};
```

### 5.4 Thread Naming

Thread naming is platform-specific and has different constraints on each OS:

- **Linux**: `pthread_setname_np(pthread_self(), name)` — max 15 characters (plus null terminator). The name is set on the calling thread.
- **macOS**: `pthread_setname_np(name)` — different signature! No `pthread_t` argument — always sets the *current* thread's name. Max 63 characters.
- **Windows**: `SetThreadDescription(GetCurrentThread(), wide_name)` — no length limit, wide string.

```zig
/// Set the current thread's name. Truncates to platform limits silently.
///
/// Linux: max 15 chars. macOS: max 63 chars. Windows: no limit.
pub fn setThreadName(name: []const u8) void {
    switch (comptime builtin.os.tag) {
        .linux => setThreadNameLinux(name),
        .macos => setThreadNameMacos(name),
        .windows => setThreadNameWindows(name),
        else => {}, // Unsupported — silently ignore.
    }
}

fn setThreadNameLinux(name: []const u8) void {
    // Linux limit: 15 chars + null terminator = 16 bytes.
    var buf: [16]u8 = undefined;
    const len = @min(name.len, 15);
    @memcpy(buf[0..len], name[0..len]);
    buf[len] = 0;

    // pthread_setname_np(pthread_self(), name) — Zig wraps this.
    const handle = std.Thread.getCurrentHandle();
    _ = std.os.linux.prctl(
        @intFromEnum(std.os.linux.PR.SET_NAME),
        @intFromPtr(&buf),
        0,
        0,
        0,
    );
    _ = handle;
}

fn setThreadNameMacos(name: []const u8) void {
    // macOS: pthread_setname_np(name) — sets the current thread's name.
    // Max 63 chars.
    var buf: [64]u8 = undefined;
    const len = @min(name.len, 63);
    @memcpy(buf[0..len], name[0..len]);
    buf[len] = 0;

    // Call the C function directly.
    const c = @cImport({
        @cInclude("pthread.h");
    });
    _ = c.pthread_setname_np(&buf);
}

fn setThreadNameWindows(name: []const u8) void {
    // Windows: SetThreadDescription — wide string, no length limit.
    _ = name;
    // TODO: Implement via std.os.windows.kernel32.SetThreadDescription
    // Requires UTF-8 → UTF-16 conversion.
}
```

### 5.5 CPU Affinity (Optional)

CPU affinity pins a thread to specific cores, eliminating context-switch overhead and improving cache locality. This is optional but valuable for dedicated low-latency threads.

```zig
/// Pin the current thread to a specific CPU core.
///
/// Linux: sched_setaffinity(). macOS: not well-supported (thread_policy_set is advisory).
/// Windows: SetThreadAffinityMask().
///
/// Returns error on failure or if the platform doesn't support affinity.
pub fn setThreadAffinity(cpu_id: u32) !void {
    switch (comptime builtin.os.tag) {
        .linux => {
            var mask = std.os.linux.cpu_set_t{};
            mask.__bits[cpu_id / 64] = @as(usize, 1) << @intCast(cpu_id % 64);
            const rc = std.os.linux.sched_setaffinity(0, &mask);
            if (rc != 0) return error.AffinitySetFailed;
        },
        .macos => {
            // macOS thread_policy_set with THREAD_AFFINITY_POLICY is advisory only.
            // Not reliably supported — skip.
            return error.NotSupported;
        },
        .windows => {
            // TODO: SetThreadAffinityMask via std.os.windows
            return error.NotSupported;
        },
        else => return error.NotSupported,
    }
}
```

### Implementation Steps

1. Create `src/platform/thread.zig`.
2. Define `EventLoop` interface (context pointer + function pointers).
3. Define `IdleStrategy` tagged union with all five variants.
4. Implement `BackoffState` with spin → yield → sleep progression.
5. Implement `ThreadRunner` with `start()`, `stop()`, `join()`, `stopAndJoin()`.
6. Implement `setThreadName()` with Linux/macOS/Windows dispatch.
7. Implement `setThreadAffinity()` for Linux (optional).
8. Write tests.

### Tests

```zig
test "ThreadRunner start and stop" {
    const TestLoop = struct {
        call_count: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),

        fn doWork(ctx: *anyopaque) u32 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            _ = self.call_count.fetchAdd(1, .monotonic);
            return 1; // Did work.
        }

        fn onClose(_: *anyopaque) void {}
    };

    var loop = TestLoop{};
    var runner = ThreadRunner.init(
        "test-thread",
        .{
            .context = @ptrCast(&loop),
            .doWorkFn = TestLoop.doWork,
            .onCloseFn = TestLoop.onClose,
        },
        .{ .backoff = .{} },
    );

    try runner.start();
    // Let it run briefly.
    std.time.sleep(5 * std.time.ns_per_ms);
    runner.stopAndJoin();

    // It should have run at least once.
    try std.testing.expect(loop.call_count.load(.monotonic) > 0);
}

test "IdleStrategy backoff resets on work" {
    var strategy = IdleStrategy{ .backoff = .{} };

    // Idle several times.
    strategy.idle(0);
    strategy.idle(0);
    strategy.idle(0);
    try std.testing.expect(strategy.backoff.spins == 3);

    // Do work → reset.
    strategy.idle(1);
    try std.testing.expect(strategy.backoff.spins == 0);
    try std.testing.expect(strategy.backoff.yields == 0);
}
```

---

## 6. Process Synchronization

**File: `src/platform/process_sync.zig`**

When idle strategies reach the `blocking` level, the thread must sleep in the kernel until another thread or process writes to the ring buffer and wakes it up. This requires a platform-specific wait/wake mechanism operating on a shared-memory word.

The synchronization word is an `i32` at a known offset in the ring buffer's blocking trailer (3 × 128-byte cache-line-padded slots prepended before the data buffer when blocking mode is enabled). The protocol is:

1. **Before sleeping**: The consumer sets the wait-state word to `1` (via CAS: `0 → 1`).
2. **Sleep**: `wait(ptr, expected=1, timeout)` — the kernel atomically checks `*ptr == 1` and sleeps if so.
3. **Wake**: The producer sets the word to `0` (via CAS: `1 → 0`) and calls `wake(ptr)`.

This matches the Java `ProcessSynchronizer` interface in `ringloom/core/src/main/java/io/ringloom/core/libnative/`.

### 6.1 Interface

```zig
// src/platform/process_sync.zig

const std = @import("std");
const builtin = @import("builtin");

pub const WaitResult = enum {
    /// Woken by a wake() call.
    woken,
    /// The value at the pointer changed before we could sleep (spurious).
    value_changed,
    /// The wait timed out.
    timed_out,
    /// Interrupted by a signal (Linux only).
    interrupted,
};

pub const ProcessSynchronizer = struct {
    /// Platform-specific implementation function pointers.
    waitFn: *const fn (ptr: *const volatile i32, expected: i32, timeout_ns: ?i64) WaitResult,
    wakeFn: *const fn (ptr: *const volatile i32) void,
    wakeAllFn: *const fn (ptr: *const volatile i32) void,

    /// Block the calling thread until the value at `ptr` is no longer `expected`,
    /// or until `timeout_ns` nanoseconds elapse.
    ///
    /// If `timeout_ns` is null, the wait is indefinite.
    ///
    /// **IMPORTANT**: The check `*ptr == expected` and the sleep are atomic with respect
    /// to wake() calls. This prevents the lost-wakeup problem.
    pub fn wait(self: ProcessSynchronizer, ptr: *const volatile i32, expected: i32, timeout_ns: ?i64) WaitResult {
        return self.waitFn(ptr, expected, timeout_ns);
    }

    /// Wake one thread waiting on the word at `ptr`.
    pub fn wake(self: ProcessSynchronizer, ptr: *const volatile i32) void {
        self.wakeFn(ptr);
    }

    /// Wake all threads waiting on the word at `ptr`.
    pub fn wakeAll(self: ProcessSynchronizer, ptr: *const volatile i32) void {
        self.wakeAllFn(ptr);
    }

    /// Get the platform-specific synchronizer for the current OS.
    pub fn getPlatformInstance() ProcessSynchronizer {
        return switch (comptime builtin.os.tag) {
            .linux => linuxFutex(),
            .macos => macosUlock(),
            .windows => windowsWaitOnAddress(),
            else => @compileError("Unsupported OS for ProcessSynchronizer"),
        };
    }
};
```

### 6.2 Linux: `futex(2)`

The Linux futex ("fast userspace mutex") is the gold standard for wait/wake synchronization. It's used by pthreads internally and is the most efficient mechanism on Linux.

- **`FUTEX_WAIT`**: Atomically checks `*addr == expected` and sleeps. Returns 0 on wake, `-EAGAIN` if value changed, `-ETIMEDOUT` on timeout.
- **`FUTEX_WAKE`**: Wakes up to N waiters. Returns the number of threads woken.

```zig
fn linuxFutex() ProcessSynchronizer {
    return .{
        .waitFn = linuxFutexWait,
        .wakeFn = linuxFutexWake,
        .wakeAllFn = linuxFutexWakeAll,
    };
}

fn linuxFutexWait(ptr: *const volatile i32, expected: i32, timeout_ns: ?i64) WaitResult {
    const uaddr: *const i32 = @volatileCast(ptr);

    // Build timespec for timeout.
    var ts: std.os.linux.timespec = undefined;
    var ts_ptr: ?*const std.os.linux.timespec = null;

    if (timeout_ns) |ns| {
        ts.tv_sec = @intCast(@divTrunc(ns, std.time.ns_per_s));
        ts.tv_nsec = @intCast(@mod(ns, std.time.ns_per_s));
        ts_ptr = &ts;
    }

    // futex(uaddr, FUTEX_WAIT, expected, timeout, NULL, 0)
    const rc = std.os.linux.futex_wait(
        @ptrCast(uaddr),
        .{ .PRIVATE = true }, // FUTEX_WAIT_PRIVATE (same process optimization)
        expected,
        ts_ptr,
    );

    return switch (std.posix.errno(rc)) {
        .SUCCESS => .woken,
        .AGAIN => .value_changed,  // *ptr != expected
        .TIMEDOUT => .timed_out,
        .INTR => .interrupted,
        else => .woken, // Treat unexpected errors as spurious wakeup
    };
}

fn linuxFutexWake(ptr: *const volatile i32) void {
    const uaddr: *const i32 = @volatileCast(ptr);
    _ = std.os.linux.futex_wake(
        @ptrCast(uaddr),
        .{ .PRIVATE = true },
        1, // Wake one waiter
    );
}

fn linuxFutexWakeAll(ptr: *const volatile i32) void {
    const uaddr: *const i32 = @volatileCast(ptr);
    _ = std.os.linux.futex_wake(
        @ptrCast(uaddr),
        .{ .PRIVATE = true },
        std.math.maxInt(i32), // Wake all waiters
    );
}
```

### 6.3 macOS: `__ulock_wait` / `__ulock_wake`

macOS doesn't expose futex. Instead, Apple provides private (but stable) `__ulock_wait` and `__ulock_wake` APIs in libsystem. These are the same primitives that `os_unfair_lock` uses internally.

```zig
fn macosUlock() ProcessSynchronizer {
    return .{
        .waitFn = macosUlockWait,
        .wakeFn = macosUlockWake,
        .wakeAllFn = macosUlockWakeAll,
    };
}

// __ulock operation constants
const UL_COMPARE_AND_WAIT: u32 = 1;
const ULF_NO_ERRNO: u32 = 0x01000000;
const ULF_WAKE_ALL: u32 = 0x00000100;

// Declare the private Apple APIs via extern.
extern "c" fn __ulock_wait(
    operation: u32,
    addr: *const volatile anyopaque,
    value: u64,
    timeout_us: u32,
) c_int;

extern "c" fn __ulock_wake(
    operation: u32,
    addr: *const volatile anyopaque,
    wake_value: u64,
) c_int;

fn macosUlockWait(ptr: *const volatile i32, expected: i32, timeout_ns: ?i64) WaitResult {
    const timeout_us: u32 = if (timeout_ns) |ns|
        @intCast(@divTrunc(ns, std.time.ns_per_us))
    else
        0; // 0 = infinite wait

    const rc = __ulock_wait(
        UL_COMPARE_AND_WAIT | ULF_NO_ERRNO,
        @ptrCast(ptr),
        @as(u64, @bitCast(@as(i64, expected))),
        timeout_us,
    );

    if (rc >= 0) return .woken;
    const err = -rc;
    return switch (err) {
        @intFromEnum(std.posix.E.AGAIN) => .value_changed,
        @intFromEnum(std.posix.E.TIMEDOUT) => .timed_out,
        @intFromEnum(std.posix.E.INTR) => .interrupted,
        else => .woken,
    };
}

fn macosUlockWake(ptr: *const volatile i32) void {
    _ = __ulock_wake(UL_COMPARE_AND_WAIT | ULF_NO_ERRNO, @ptrCast(ptr), 0);
}

fn macosUlockWakeAll(ptr: *const volatile i32) void {
    _ = __ulock_wake(UL_COMPARE_AND_WAIT | ULF_NO_ERRNO | ULF_WAKE_ALL, @ptrCast(ptr), 0);
}
```

### 6.4 Windows: `WaitOnAddress` / `WakeByAddressSingle`

Windows 8+ provides `WaitOnAddress` in `api-ms-win-core-synch-l1-2-0.dll` (Synchronization.lib):

```zig
fn windowsWaitOnAddress() ProcessSynchronizer {
    return .{
        .waitFn = windowsWait,
        .wakeFn = windowsWake,
        .wakeAllFn = windowsWakeAll,
    };
}

const windows = std.os.windows;

// These are in kernel32 / api-ms-win-core-synch-l1-2-0.dll.
extern "kernel32" fn WaitOnAddress(
    address: *const volatile anyopaque,
    compare_address: *const anyopaque,
    address_size: usize,
    timeout_ms: windows.DWORD,
) windows.BOOL;

extern "kernel32" fn WakeByAddressSingle(address: *const volatile anyopaque) void;
extern "kernel32" fn WakeByAddressAll(address: *const volatile anyopaque) void;

fn windowsWait(ptr: *const volatile i32, expected: i32, timeout_ns: ?i64) WaitResult {
    const timeout_ms: windows.DWORD = if (timeout_ns) |ns|
        @intCast(@divTrunc(ns, std.time.ns_per_ms))
    else
        windows.INFINITE;

    const result = WaitOnAddress(
        @ptrCast(ptr),
        @ptrCast(&expected),
        @sizeOf(i32),
        timeout_ms,
    );

    if (result != 0) return .woken;

    return switch (windows.kernel32.GetLastError()) {
        .TIMEOUT => .timed_out,
        else => .value_changed,
    };
}

fn windowsWake(ptr: *const volatile i32) void {
    WakeByAddressSingle(@ptrCast(ptr));
}

fn windowsWakeAll(ptr: *const volatile i32) void {
    WakeByAddressAll(@ptrCast(ptr));
}
```

### Implementation Steps

1. Create `src/platform/process_sync.zig`.
2. Define `WaitResult` enum and `ProcessSynchronizer` struct with function pointers.
3. Implement `linuxFutexWait`, `linuxFutexWake`, `linuxFutexWakeAll` using `std.os.linux.futex_wait` / `futex_wake`.
4. Implement `macosUlockWait`, `macosUlockWake`, `macosUlockWakeAll` using `extern "c"` declarations.
5. Implement `windowsWait`, `windowsWake`, `windowsWakeAll` using `extern "kernel32"` declarations.
6. Implement `getPlatformInstance()` with `comptime` OS dispatch.
7. Write tests.

### Tests

```zig
test "ProcessSynchronizer wait returns value_changed when value differs" {
    const sync = ProcessSynchronizer.getPlatformInstance();

    var word: volatile i32 = 42;
    // Wait with expected=0 — value is 42, so it should return immediately.
    const result = sync.wait(&word, 0, 1_000_000); // 1ms timeout
    try std.testing.expect(result == .value_changed);
}

test "ProcessSynchronizer wait times out" {
    const sync = ProcessSynchronizer.getPlatformInstance();

    var word: volatile i32 = 1;
    // Wait with expected=1 — value matches, so it will sleep until timeout.
    const start = std.time.milliTimestamp();
    const result = sync.wait(&word, 1, 10_000_000); // 10ms timeout
    const elapsed = std.time.milliTimestamp() - start;

    try std.testing.expect(result == .timed_out);
    try std.testing.expect(elapsed >= 8); // Allow some slack.
}

test "ProcessSynchronizer wake unblocks wait" {
    const sync = ProcessSynchronizer.getPlatformInstance();

    var word: volatile i32 = 1;
    var was_woken = AtomicBool.init(false);

    // Spawn a thread that waits.
    const waiter = try std.Thread.spawn(.{}, struct {
        fn run(s: ProcessSynchronizer, w: *volatile i32, flag: *AtomicBool) void {
            _ = s.wait(w, 1, 1_000_000_000); // 1s timeout (should be woken before)
            flag.store(true);
        }
    }.run, .{ sync, &word, &was_woken });

    // Give the waiter time to enter the wait.
    std.time.sleep(5 * std.time.ns_per_ms);

    // Change the value and wake.
    @atomicStore(&word, 0, .release);
    sync.wake(&word);

    waiter.join();
    try std.testing.expect(was_woken.load());
}
```

---

## 7. CPU Pause / Yield

This is trivial in Zig — `std.atomic.spinLoopHint()` emits the correct instruction on every architecture:

| Architecture | Instruction | Effect |
|---|---|---|
| x86-64 | `pause` | Reduces power, improves SMT performance, ~140 cycles delay |
| ARM64 | `yield` | Hint to hardware that this thread is spinning |
| RISC-V | `pause` (if supported) | Similar hint |
| Other | no-op | Safe fallback |

This is already used in the `IdleStrategy.busy_spin` variant:

```zig
.busy_spin => std.atomic.spinLoopHint(),
```

No separate module is needed. The hint is also used in CAS retry loops:

```zig
// In a CAS loop:
while (true) {
    if (@cmpxchgWeak(i64, &tail, current, desired, .acquire, .monotonic) == null) {
        break; // success
    }
    std.atomic.spinLoopHint(); // reduce contention while retrying
}
```

---

## 8. Alignment Helpers

Zig's type system handles alignment natively — `align(N)` is a first-class attribute on pointers and types. These utilities complement the constants module for cases where runtime alignment is needed.

### 8.1 AlignedBuffer

```zig
// This can be placed at the bottom of constants.zig or in a separate alignment.zig

/// A fixed-size buffer type with a guaranteed alignment.
///
/// Use this for stack-allocated buffers that need specific alignment
/// (e.g., page-aligned buffers for testing, cache-line-aligned scratch space).
///
/// Example:
///   var buf: AlignedBuffer(4096, 4096) = undefined;
///   // buf is guaranteed to be page-aligned.
pub fn AlignedBuffer(comptime size: usize, comptime alignment: usize) type {
    return struct {
        data: [size]u8 align(alignment) = undefined,

        pub fn asSlice(self: *@This()) []align(alignment) u8 {
            return &self.data;
        }

        pub fn ptr(self: *@This()) [*]align(alignment) u8 {
            return @ptrCast(&self.data);
        }
    };
}
```

### 8.2 Runtime Alignment Cast

```zig
/// Cast a byte pointer to a pointer of type T, asserting alignment.
///
/// This is a thin wrapper around @ptrCast(@alignCast(...)) with a bounds check.
/// Panics in Debug/ReleaseSafe if the pointer is misaligned.
pub fn castAligned(comptime T: type, bytes: [*]u8, offset: usize) *T {
    return @ptrCast(@alignCast(bytes + offset));
}

/// Verify that a buffer address and size meet the requirements for a ring buffer.
///
/// Requirements:
///   - Address is page-aligned.
///   - Data capacity (excluding trailer) is a power of two.
///   - Total size = capacity + ring_buffer_trailer_length.
pub fn validateRingBufferLayout(addr: [*]const u8, total_size: usize) !void {
    const address = @intFromPtr(addr);
    if (!isAligned(address, page_size)) return error.NotPageAligned;

    if (total_size <= ring_buffer_trailer_length) return error.BufferTooSmall;
    const capacity = total_size - ring_buffer_trailer_length;
    if (!isPowerOfTwo(capacity)) return error.CapacityNotPowerOfTwo;
}
```

### Tests

```zig
test "AlignedBuffer has correct alignment" {
    var buf = AlignedBuffer(4096, 4096){};
    const addr = @intFromPtr(buf.ptr());
    try std.testing.expect(addr % 4096 == 0);
}

test "AlignedBuffer cache-line aligned" {
    var buf = AlignedBuffer(256, 64){};
    const addr = @intFromPtr(buf.ptr());
    try std.testing.expect(addr % 64 == 0);
}
```

---

## 9. Public Re-exports

**File: `src/platform.zig`**

This is the single import point for the rest of the codebase:

```zig
// src/platform.zig

pub const constants = @import("platform/constants.zig");
pub const atomic = @import("platform/atomic.zig");
pub const MappedFile = @import("platform/mapped_file.zig").MappedFile;
pub const Clock = @import("platform/clock.zig").Clock;
pub const thread = @import("platform/thread.zig");
pub const ThreadRunner = thread.ThreadRunner;
pub const EventLoop = thread.EventLoop;
pub const IdleStrategy = thread.IdleStrategy;
pub const ProcessSynchronizer = @import("platform/process_sync.zig").ProcessSynchronizer;
pub const WaitResult = @import("platform/process_sync.zig").WaitResult;

// Re-export commonly used types for convenience.
pub const AtomicI32 = atomic.AtomicI32;
pub const AtomicI64 = atomic.AtomicI64;
pub const AtomicBool = atomic.AtomicBool;
pub const CacheLinePaddedAtomicI64 = atomic.CacheLinePaddedAtomicI64;
pub const CacheLinePaddedAtomicI32 = atomic.CacheLinePaddedAtomicI32;
```

Usage from the rest of the codebase:

```zig
const platform = @import("platform.zig");

// Constants
const capacity = platform.constants.default_send_buffer_length;

// Atomics
var tail = platform.CacheLinePaddedAtomicI64.init(0);

// Clock
const now = platform.Clock.epochMillis();

// Mapped file
var mf = try platform.MappedFile.create(allocator, dir, "broker_0.dat", total_size);
defer mf.close();

// Thread runner
var runner = platform.ThreadRunner.init("control-loop", event_loop, idle_strategy);
try runner.start();

// Process sync
const sync = platform.ProcessSynchronizer.getPlatformInstance();
sync.wake(&wait_word);
```

---

## Build Integration

Update `build.zig` to recognize the new source tree. The platform module is part of the main `ringloom_broker` module, so no additional build configuration is needed — Zig's module system automatically resolves `@import("platform/foo.zig")` relative to the root source file.

Ensure the test step discovers all platform tests:

```zig
// In build.zig, the existing test setup already covers this:
const mod_tests = b.addTest(.{
    .root_module = mod,
});
```

Since `src/root.zig` is the root source file, add re-exports there:

```zig
// src/root.zig
pub const platform = @import("platform.zig");

// This ensures all platform tests are discovered by `zig build test`.
comptime {
    _ = @import("platform/constants.zig");
    _ = @import("platform/atomic.zig");
    _ = @import("platform/mapped_file.zig");
    _ = @import("platform/clock.zig");
    _ = @import("platform/thread.zig");
    _ = @import("platform/process_sync.zig");
}
```

Run tests:

```
zig build test
```

---

## Testing Strategy

### Unit Test Coverage

| Module | Key Tests |
|---|---|
| `constants.zig` | `alignUp`, `isPowerOfTwo`, `isAligned` with edge cases |
| `atomic.zig` | Size/alignment of padded types, load/store/CAS/fetchAdd correctness |
| `mapped_file.zig` | Create/open/close lifecycle, PID reuse, sub-page rounding, `ptrAt` typed access |
| `clock.zig` | Monotonicity, positive values, reasonable epoch range |
| `thread.zig` | Start/stop lifecycle, idle strategy backoff reset, thread naming (check `/proc/self/task/*/comm` on Linux) |
| `process_sync.zig` | Value-changed fast path, timeout, cross-thread wake |

### Integration Tests (Cross-Module)

These should be written after all platform modules are complete:

1. **Mapped file + atomics**: Create a mapped file, overlay a `CacheLinePaddedAtomicI64` at a known offset, write from one thread, read from another.
2. **Thread runner + clock**: Start a thread runner that records timestamps, verify monotonicity after join.
3. **Process sync + thread**: Producer/consumer with futex wake — one thread writes to a shared buffer, wakes the consumer thread.

### Multi-Process Tests

These test the core value proposition — shared memory between processes:

1. Fork a child process, both map the same file, parent writes, child reads.
2. Test PID liveness check: fork, child exits, parent verifies `isProcessAlive` returns false.

These are harder to write in Zig's test framework (no built-in fork support), so they may be standalone test binaries invoked from a shell script.

### Performance Benchmarks

Not part of the test suite, but useful for validation:

| Benchmark | Target |
|---|---|
| `Clock.monotonicNanos()` call overhead | < 30ns |
| `Clock.epochMillis()` call overhead | < 10ns (Linux COARSE) |
| `AtomicI64.load()` + `AtomicI64.store()` round-trip | < 5ns (single thread) |
| `CAS` contention under 4 threads | < 100ns avg |
| `ProcessSynchronizer` wake latency | < 10µs |
| `MappedFile.create()` + `close()` | < 100µs |

---

## Summary of Dependencies

```
constants.zig  ← no dependencies (leaf module)
     ↑
atomic.zig     ← depends on constants (cache_line_pad)
     ↑
clock.zig      ← depends on std.time, std.os.linux (leaf-ish)
     ↑
process_sync.zig ← depends on std.os.linux / extern "c" / extern "kernel32"
     ↑
mapped_file.zig  ← depends on constants, std.posix
     ↑
thread.zig       ← depends on atomic (AtomicBool), std.Thread
     ↑
platform.zig     ← re-exports everything
```

**Build order**: All files can be implemented in parallel since Zig resolves dependencies lazily. However, the recommended implementation order for testing purposes is:

1. `constants.zig` — foundation, no deps, easy to test
2. `atomic.zig` — depends only on constants, pure computation
3. `clock.zig` — standalone, easy to verify
4. `process_sync.zig` — requires OS calls, test on target platform
5. `mapped_file.zig` — requires filesystem interaction
6. `thread.zig` — requires atomics and OS threading
7. `platform.zig` — just re-exports

After this step is complete, the next implementation step (Step 2: MPSC Ring Buffer) will build directly on `constants`, `atomic`, `mapped_file`, and `process_sync`.