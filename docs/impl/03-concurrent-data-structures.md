# 03 — Concurrent Data Structures

> **Prerequisites:** [01 — Platform Abstraction](01-platform-abstraction.md) (atomics,
> `ProcessSynchronizer`, clocks), [02 — Memory Layout & Shared Memory](02-memory-layout-and-shared-memory.md)
> (metadata files, mmap'd regions, constants).

This document describes every concurrent data structure used by the broker and service
processes. The most critical is the **MPSC ring buffer** — it is the backbone of all
message passing in the system. We also cover **atomic counters** for monitoring and an
**error log** for diagnostics.

All structures operate over pre-existing byte slices obtained from memory-mapped files
(see doc 02). They never allocate. Every field access on the hot path uses explicit
atomic ordering. No mutexes, no locks, no hidden control flow.

---

## Table of Contents

1. [MPSC Ring Buffer](#1-mpsc-ring-buffer)
   1. [Memory Layout](#11-memory-layout)
   2. [Record Header](#12-record-header)
   3. [Constants](#13-constants)
   4. [The `RingBuffer` Struct](#14-the-ringbuffer-struct)
   5. [Trailer Accessor](#15-trailer-accessor)
   6. [Write Algorithm (Multi-Producer)](#16-write-algorithm-multi-producer)
   7. [Read Algorithm (Single Consumer)](#17-read-algorithm-single-consumer)
   8. [Try-Claim API (Zero-Copy Write)](#18-try-claim-api-zero-copy-write)
   9. [Commit and Abort](#19-commit-and-abort)
   10. [Dead Producer Recovery (`unblock`)](#110-dead-producer-recovery-unblock)
   11. [Blocking Ring Buffer Extension](#111-blocking-ring-buffer-extension)
   12. [Utility Accessors](#112-utility-accessors)
2. [Counters](#2-counters)
   1. [Counter Layout](#21-counter-layout)
   2. [Implementation: `CountersManager`](#22-implementation-countersmanager)
3. [Error Log](#3-error-log)
   1. [Error Log Entry Layout](#31-error-log-entry-layout)
   2. [Implementation: `ErrorLog`](#32-implementation-errorlog)
4. [Thread-Local Error State](#4-thread-local-error-state)
5. [Testing](#5-testing)

---

## 1. MPSC Ring Buffer

The ring buffer is the most critical data structure in the entire system. Every message
that flows between processes — control messages, application payloads, cross-host routing
commands — passes through an MPSC ring buffer.

**Usage points:**

| Ring Buffer | Producer(s) | Consumer | Location |
|---|---|---|---|
| Service → broker control | Service control agent | Broker control loop | Service metadata file |
| Broker → service control | Broker control loop | Service control agent | Service metadata file |
| Service → broker send (cross-host) | Service message producer | Broker routing agent | Service metadata file |
| Any → service application messages | Broker routing agent, peer services | Service message consumer | Service metadata file |

The design follows the standard `ManyToOneRingBuffer` pattern: a flat byte buffer with a
trailing metadata region, lock-free multi-producer writes via CAS on a tail counter, and
single-consumer reads that zero consumed records and advance a head counter.

### 1.1 Memory Layout

The ring buffer operates over a contiguous byte slice from mmap'd shared memory. The
slice is divided into two regions:

```
┌──────────────────────────────────────────────┐  ← offset 0
│                                              │
│  Data Buffer                                 │
│  (capacity bytes — MUST be power of 2)       │
│                                              │
│  Contains records: [header][payload][pad]... │
│                                              │
├──────────────────────────────────────────────┤  ← offset = capacity
│                                              │
│  Trailer (768 bytes = 6 × 128-byte slots)    │
│                                              │
│  +0:     begin_pad [128 bytes]               │  ← prevents false sharing
│                                              │     with last data record
│  +128:   tail_position (i64)                 │  ← producers CAS here
│          + pad [120 bytes]                   │
│                                              │
│  +256:   head_cache (i64)                    │  ← producer-local cached
│          + pad [120 bytes]                   │     copy of head (reduces
│                                              │     cross-core traffic)
│                                              │
│  +384:   head_position (i64)                 │  ← consumer writes here
│          + pad [120 bytes]                   │     to free space
│                                              │
│  +512:   correlation_counter (i64)           │  ← atomic counter for
│          + pad [120 bytes]                   │     unique correlation IDs
│                                              │
│  +640:   consumer_heartbeat (i64)            │  ← consumer writes epoch
│          + pad [120 bytes]                   │     millis for liveness
│                                              │
└──────────────────────────────────────────────┘

Total ring buffer size: capacity + 768
```

**Why 128-byte padding?** Each trailer field sits on its own pair of cache lines (2 ×
64 bytes = 128 bytes). This prevents **false sharing**: when one core writes
`tail_position`, the cache-line invalidation does not force a reload of
`head_position` on the consumer's core. Without this padding, producer CAS contention
would bleed into consumer reads, destroying throughput.

**Why is `head_cache` separate from `head_position`?** Producers need to know how much
space is available, which requires reading the consumer's head position. But reading the
*actual* `head_position` on every CAS attempt forces a cross-core cache-line transfer.
Instead, producers first check `head_cache` — a stale-but-close-enough copy that lives
in the producer-contended region. Only when `head_cache` says "full" does the producer
pay the cost of reading the real `head_position` and updating the cache. In the common
case (buffer not near full), the producer never touches the consumer's cache line.

### 1.2 Record Header

Every record in the data buffer starts with an 8-byte header:

```
Offset  Size  Type          Field
──────────────────────────────────────────────────
0       4     volatile i32  length        — record length including header
4       4     i32           msg_type_id   — message type identifier
```

**`length` semantics:**

| Value | Meaning |
|---|---|
| `0` | Empty / unwritten slot |
| `> 0` | Committed record — safe to read. Value = total record length (header + payload) |
| `< 0` | Uncommitted — a producer has claimed this slot but has not yet committed. The absolute value is the record length. Used only by the try-claim API |

**`msg_type_id` semantics:**

| Value | Meaning |
|---|---|
| `>= 1` | Valid application message type |
| `0` | Padding record (used to fill gaps at end-of-buffer wrap) |

Records are aligned to **8 bytes**. If a record's natural length is not a multiple of 8,
it is padded up. The alignment bytes are not included in the `length` field — the
consumer computes the aligned length when advancing.

**Maximum message length** = `capacity / 8`. This limit ensures that even in the worst
case (wrap-around padding consuming nearly the entire buffer), at least one message can
be written.

### 1.3 Constants

These constants should be defined in a shared `constants.zig` module (building on the
constants from doc 02):

```zig
// src/concurrent/ring_buffer.zig (or src/constants.zig)

pub const cache_line_length: usize = 64;

/// Two cache lines — the minimum isolation distance to prevent false sharing.
pub const cache_line_pad: usize = cache_line_length * 2; // 128

/// 8 bytes: i32 length + i32 msg_type_id.
pub const record_header_length: usize = @sizeOf(i32) * 2; // 8

/// Every record is aligned to this boundary.
pub const record_alignment: usize = 8;

/// 6 cache-line-padded slots: begin_pad, tail, head_cache, head, correlation, heartbeat.
pub const trailer_length: usize = cache_line_pad * 6; // 768

/// Padding record type — consumer skips these.
pub const padding_msg_type_id: i32 = 0;

/// Returned by tryClaim when the buffer is full.
pub const insufficient_capacity: i32 = -1;

// ── Trailer field offsets (relative to start of trailer region) ───────

pub const tail_position_offset: usize = cache_line_pad * 1;       // +128
pub const head_cache_position_offset: usize = cache_line_pad * 2; // +256
pub const head_position_offset: usize = cache_line_pad * 3;       // +384
pub const correlation_counter_offset: usize = cache_line_pad * 4; // +512
pub const consumer_heartbeat_offset: usize = cache_line_pad * 5;  // +640

// ── Blocking prefix constants ─────────────────────────────────────────

/// 3 × 128-byte slots for writer_wait_state, reader_wait_state, and wait_timeout.
pub const blocking_prefix_length: usize = cache_line_pad * 3; // 384
```

### 1.4 The `RingBuffer` Struct

The `RingBuffer` struct is a flyweight over an existing byte slice. It stores no data
itself — it is a view with methods.

```zig
// src/concurrent/ring_buffer.zig

const std = @import("std");
const constants = @import("../constants.zig");

pub const RingBuffer = struct {
    /// The full backing memory: data region + trailer.
    buffer: []align(record_alignment) u8,

    /// Usable data capacity in bytes (always a power of 2).
    capacity: usize,

    /// Maximum allowed message payload length.
    max_msg_length: usize,

    /// Offset from buffer start to the trailer region.
    trailer_offset: usize,

    /// Bitmask for converting a logical position to a physical index.
    /// Equal to capacity - 1. Works because capacity is a power of 2.
    capacity_mask: u63,

    // ── Blocking mode fields ──────────────────────────────────────────
    blocking: bool,
    process_synchronizer: ?*ProcessSynchronizer,
    /// Pointer into the blocking prefix: offset 0 of the mmap'd region.
    writer_wait_state: ?*align(constants.cache_line_pad) i32,
    /// Pointer into the blocking prefix: offset cache_line_pad.
    reader_wait_state: ?*align(constants.cache_line_pad) i32,

    pub const InitError = error{
        BufferTooSmall,
        CapacityNotPowerOfTwo,
    };

    /// Initialize a ring buffer over the given byte slice.
    ///
    /// The slice must be at least `trailer_length + 1` bytes. The data capacity
    /// is `buffer.len - trailer_length` and must be a power of two.
    ///
    /// For blocking mode, the caller must provide the *full* mmap'd region
    /// including the 384-byte blocking prefix. The `blocking_prefix` parameter
    /// points to the start of that prefix; `buffer` points to the data+trailer
    /// region that begins *after* the prefix.
    pub fn init(
        buffer: []align(constants.record_alignment) u8,
        blocking: bool,
        process_synchronizer: ?*ProcessSynchronizer,
        blocking_prefix: ?[*]align(constants.cache_line_pad) u8,
    ) InitError!RingBuffer {
        if (buffer.len <= constants.trailer_length) {
            return error.BufferTooSmall;
        }

        const data_capacity = buffer.len - constants.trailer_length;
        if (!std.math.isPowerOfTwo(data_capacity)) {
            return error.CapacityNotPowerOfTwo;
        }

        var rb = RingBuffer{
            .buffer = buffer,
            .capacity = data_capacity,
            .max_msg_length = data_capacity / 8,
            .trailer_offset = data_capacity,
            .capacity_mask = @intCast(data_capacity - 1),
            .blocking = blocking,
            .process_synchronizer = process_synchronizer,
            .writer_wait_state = null,
            .reader_wait_state = null,
        };

        if (blocking) {
            if (blocking_prefix) |prefix| {
                rb.writer_wait_state = @ptrCast(prefix);
                rb.reader_wait_state = @ptrCast(prefix + constants.cache_line_pad);
            }
        }

        return rb;
    }

    /// Compute the total byte count needed for a ring buffer with the given
    /// data capacity and blocking mode.
    pub fn calculateRequiredSize(data_capacity: usize, blocking: bool) usize {
        std.debug.assert(std.math.isPowerOfTwo(data_capacity));
        const prefix = if (blocking) constants.blocking_prefix_length else 0;
        return prefix + data_capacity + constants.trailer_length;
    }
};
```

**Key design decisions:**

- The struct stores derived values (`capacity_mask`, `trailer_offset`) to avoid
  recomputing them on every operation.
- The `buffer` slice includes only the data + trailer region, not the blocking prefix.
  This keeps all index arithmetic relative to the data buffer start.
- The blocking prefix pointers are separate because they live *before* the data buffer
  in the mmap'd file (see doc 02, service metadata file layout).

### 1.5 Trailer Accessor

Rather than casting the trailer to a struct (which would require careful alignment
management across the mmap boundary), we access trailer fields through offset-based
atomic reads and writes. This is both simpler and more explicit about memory ordering.

```zig
// Internal helpers — all offsets are relative to self.buffer.ptr

const RingBuffer = struct {
    // ... fields from above ...

    // ── Trailer atomic accessors ──────────────────────────────────────

    /// Pointer to the i64 at the given trailer field offset.
    inline fn trailerFieldPtr(self: *const RingBuffer, field_offset: usize) *i64 {
        const abs_offset = self.trailer_offset + field_offset;
        return @ptrCast(@alignCast(self.buffer.ptr + abs_offset));
    }

    inline fn loadTailPosition(self: *const RingBuffer, order: std.builtin.AtomicOrder) i64 {
        return @atomicLoad(i64, self.trailerFieldPtr(constants.tail_position_offset), order);
    }

    inline fn loadHeadCache(self: *const RingBuffer, order: std.builtin.AtomicOrder) i64 {
        return @atomicLoad(i64, self.trailerFieldPtr(constants.head_cache_position_offset), order);
    }

    inline fn storeHeadCache(self: *RingBuffer, value: i64, order: std.builtin.AtomicOrder) void {
        @atomicStore(i64, self.trailerFieldPtr(constants.head_cache_position_offset), value, order);
    }

    inline fn loadHeadPosition(self: *const RingBuffer, order: std.builtin.AtomicOrder) i64 {
        return @atomicLoad(i64, self.trailerFieldPtr(constants.head_position_offset), order);
    }

    inline fn storeHeadPosition(self: *RingBuffer, value: i64, order: std.builtin.AtomicOrder) void {
        @atomicStore(i64, self.trailerFieldPtr(constants.head_position_offset), value, order);
    }

    inline fn loadCorrelationCounter(self: *const RingBuffer, order: std.builtin.AtomicOrder) i64 {
        return @atomicLoad(i64, self.trailerFieldPtr(constants.correlation_counter_offset), order);
    }

    inline fn loadConsumerHeartbeat(self: *const RingBuffer, order: std.builtin.AtomicOrder) i64 {
        return @atomicLoad(i64, self.trailerFieldPtr(constants.consumer_heartbeat_offset), order);
    }

    inline fn storeConsumerHeartbeat(self: *RingBuffer, value: i64, order: std.builtin.AtomicOrder) void {
        @atomicStore(i64, self.trailerFieldPtr(constants.consumer_heartbeat_offset), value, order);
    }

    // ── Record header accessors ───────────────────────────────────────

    /// Pointer to the length field (i32) at the given data buffer index.
    inline fn recordLengthPtr(self: *const RingBuffer, index: usize) *i32 {
        return @ptrCast(@alignCast(self.buffer.ptr + index));
    }

    /// Pointer to the msg_type_id field (i32) at the given data buffer index.
    inline fn recordMsgTypeIdPtr(self: *const RingBuffer, index: usize) *i32 {
        return @ptrCast(@alignCast(self.buffer.ptr + index + @sizeOf(i32)));
    }
};
```

### 1.6 Write Algorithm (Multi-Producer)

The write operation is the hot path for producers. It must:
1. Calculate the space required (including possible wrap-around padding).
2. Atomically claim that space via CAS on `tail_position`.
3. Write the record (payload, then header to commit).

```zig
pub const WriteError = error{
    BufferFull,
    InvalidMsgTypeId,
    MessageTooLong,
};

/// Write a message into the ring buffer. Thread-safe for multiple concurrent
/// producers. Returns `error.BufferFull` if there is insufficient space.
pub fn write(self: *RingBuffer, msg_type_id: i32, payload: []const u8) WriteError!void {
    if (msg_type_id < 1) return error.InvalidMsgTypeId;
    if (payload.len > self.max_msg_length) return error.MessageTooLong;

    const record_length: i32 = @intCast(payload.len + constants.record_header_length);
    const aligned_length = std.mem.alignForward(
        usize,
        @intCast(record_length),
        constants.record_alignment,
    );

    const record_index = self.claimCapacity(aligned_length) orelse {
        return error.BufferFull;
    };

    // Write the record: payload first, then header to commit.
    self.writeRecord(record_index, record_length, msg_type_id, payload);
}

/// Internal: CAS loop to claim `required_capacity` bytes in the data region.
/// Returns the physical index where the record header should be written,
/// or null if the buffer is full.
fn claimCapacity(self: *RingBuffer, required_capacity: usize) ?usize {
    const mask = self.capacity_mask;

    // Read the cached head — may be stale, but that's fine for the fast path.
    var head = self.loadHeadCache(.acquire);

    while (true) {
        var tail = self.loadTailPosition(.acquire);
        var available: usize = self.capacity -| @as(usize, @intCast(tail -| head));

        var padding: usize = 0;
        const tail_index: usize = @intCast(tail & mask);
        const to_end: usize = self.capacity - tail_index;

        if (required_capacity > to_end) {
            // ── Wrap case ──────────────────────────────────────────────
            // The message doesn't fit between tail and end of buffer.
            // We need `to_end` bytes of padding at the tail, then
            // `required_capacity` bytes at the start of the buffer.
            //
            // But first: does the head allow it? We need enough free space
            // for padding + message. The message will land at index 0, so
            // the head index must be >= required_capacity.

            var head_index: usize = @intCast(head & mask);

            if (required_capacity > head_index) {
                // Stale head — refresh from the real head position.
                head = self.loadHeadPosition(.acquire);
                head_index = @intCast(head & mask);

                if (required_capacity > head_index) {
                    // Truly full — not enough room at the start.
                    return null;
                }

                self.storeHeadCache(head, .release);
            }

            padding = to_end;
        } else if (available < required_capacity) {
            // ── No-wrap case, but stale head says full ─────────────────
            // Refresh the real head position.
            head = self.loadHeadPosition(.acquire);

            if (required_capacity > self.capacity -| @as(usize, @intCast(tail -| head))) {
                return null;
            }

            self.storeHeadCache(head, .release);
        }

        // ── CAS: try to advance tail by (required_capacity + padding) ──
        const claim_size: i64 = @intCast(required_capacity + padding);
        const result = @cmpxchgWeak(
            i64,
            self.trailerFieldPtr(constants.tail_position_offset),
            tail,
            tail + claim_size,
            .acquire,
            .monotonic,
        );

        if (result == null) {
            // CAS succeeded — we own [tail .. tail + claim_size).

            if (padding > 0) {
                // Write a padding record spanning the gap at end of buffer.
                self.writePaddingRecord(tail_index, @intCast(padding));
                // Actual message goes at index 0 (start of buffer).
                return 0;
            }

            return tail_index;
        }

        // CAS failed — another producer got there first. Retry.
        // The next iteration will reload tail and head.
    }
}
```

**CAS loop explanation:**

The loop is structured so that the *common case* (buffer has plenty of room, no
wrap-around) executes a single CAS attempt and exits. The stale-head-refresh path only
fires when the buffer is nearly full, which is the back-pressure signal.

**Memory ordering rationale:**

| Operation | Ordering | Why |
|---|---|---|
| Load `head_cache` | Acquire | Need to see consumer's latest progress |
| Load `tail_position` | Acquire | Need consistent view of other producers' claims |
| Load `head_position` (refresh) | Acquire | Must see consumer's most recent head advance |
| Store `head_cache` | Release | Other producers must see the updated cache |
| CAS on `tail_position` | Acquire on success, Monotonic on failure | On success: establishes a happens-before with the next reader. On failure: we're about to retry anyway |

### 1.7 Read Algorithm (Single Consumer)

The read side is simpler because there is exactly one consumer. No CAS, no contention
on the head counter.

```zig
/// Callback type for message consumption.
pub const MessageHandler = *const fn (msg_type_id: i32, payload: []const u8) void;

/// Read up to `limit` messages from the buffer, invoking `handler` for each
/// non-padding message. Returns the number of messages read.
///
/// This function MUST be called from a single thread — the ring buffer's
/// designated consumer.
pub fn read(self: *RingBuffer, handler: MessageHandler, limit: u32) u32 {
    // Plain load — no atomic ordering needed. We are the only writer of
    // head_position, and we need to see our own last write (trivially true).
    const head = self.loadHeadPosition(.monotonic);
    const tail = self.loadTailPosition(.acquire);
    const available: usize = @intCast(tail - head);

    if (available == 0) {
        return 0;
    }

    const head_index: usize = @intCast(head & self.capacity_mask);
    var bytes_consumed: usize = 0;
    var messages_read: u32 = 0;

    while (bytes_consumed < available and messages_read < limit) {
        const record_index = (head_index + bytes_consumed) & self.capacity_mask;

        // Acquire load on the length field — the commit fence.
        // If we see a positive length, all preceding writes (payload, msg_type_id)
        // are guaranteed to be visible.
        const record_length = @atomicLoad(
            i32,
            self.recordLengthPtr(record_index),
            .acquire,
        );

        if (record_length <= 0) {
            // Either empty (0) or uncommitted (negative from tryClaim).
            // Stop reading — cannot skip ahead.
            break;
        }

        const aligned_length = std.mem.alignForward(
            usize,
            @intCast(record_length),
            constants.record_alignment,
        );
        bytes_consumed += aligned_length;

        // Read the message type ID. Safe to read with plain load — the
        // acquire on length already established the ordering.
        const msg_type_id = self.recordMsgTypeIdPtr(record_index).*;

        if (msg_type_id != constants.padding_msg_type_id) {
            // Compute the payload slice.
            const payload_offset = record_index + constants.record_header_length;
            const payload_length: usize = @intCast(record_length - constants.record_header_length);
            const payload = self.buffer[payload_offset..][0..payload_length];

            handler(msg_type_id, payload);
            messages_read += 1;
        }
    }

    if (bytes_consumed > 0) {
        // Zero out consumed records. This is critical — it resets the length
        // fields to 0 so that future reads at these positions see "empty"
        // rather than stale committed data.
        @memset(self.buffer[head_index..][0..bytes_consumed], 0);

        // Advance head. Release ordering ensures:
        // 1. The memset (zeroing) is visible before producers reclaim the space.
        // 2. Producers loading head_position with acquire will see the zeroed data.
        self.storeHeadPosition(head + @as(i64, @intCast(bytes_consumed)), .release);
    }

    return messages_read;
}
```

**Why zero the consumed region?** Two reasons:

1. **Correctness.** The write algorithm checks `length == 0` to detect empty slots. If
   we don't zero, a subsequent wrap-around could encounter stale committed lengths from
   a previous generation and misinterpret them.

2. **Security.** In cross-process shared memory, leaving old data around is an
   information leak. Zeroing ensures a producer cannot read a previous consumer's
   message content by inspecting the buffer.

**Why does the consumer stop at `length <= 0`?** In the MPSC model, records are written
in tail-position order but may be *committed* out of order (because CAS winners write at
different speeds). However, the consumer must process records strictly in order. If
record N is uncommitted (`length < 0`) or unwritten (`length == 0`), the consumer cannot
skip to record N+1 — doing so would violate message ordering. The `unblock` mechanism
(§1.10) handles the case where a producer dies and leaves a permanent gap.

### 1.8 Try-Claim API (Zero-Copy Write)

The `write()` method copies the payload into the ring buffer. For performance-critical
paths, the **try-claim** API lets the caller write directly into the ring buffer's
memory, avoiding the copy entirely.

The protocol is:
1. Call `tryClaim()` to reserve space. Returns a `Claim` on success.
2. Write your data into `claim.buffer`.
3. Call `claim.commit()` to make the record visible, **or** `claim.abort()` to discard.

```zig
pub const Claim = struct {
    /// Writable slice for the payload — points directly into the ring buffer.
    buffer: []u8,

    /// Reference back to the ring buffer (needed for commit/abort).
    ring_buffer: *RingBuffer,

    /// Physical index of the record header in the data buffer.
    header_index: usize,

    /// The total record length (header + payload), stored for commit.
    record_length: i32,

    /// Commit the claimed record, making it visible to the consumer.
    /// After calling this, the Claim must not be used again.
    pub fn commit(self: *Claim) void {
        self.ring_buffer.commitAt(self.header_index, self.record_length);
    }

    /// Abort the claimed record, converting it to padding that the consumer
    /// will skip. Use this if you cannot complete the write.
    pub fn abort(self: *Claim) void {
        self.ring_buffer.abortAt(self.header_index, self.record_length);
    }
};

/// Claim space in the ring buffer for zero-copy writing.
///
/// Returns a `Claim` whose `.buffer` field is a writable slice for the payload,
/// or null if the buffer is full.
///
/// The caller MUST call either `claim.commit()` or `claim.abort()` after
/// writing. Failure to do so blocks the consumer at this position forever
/// (until `unblock()` recovers it).
pub fn tryClaim(self: *RingBuffer, msg_type_id: i32, length: usize) ?Claim {
    if (msg_type_id < 1) return null;
    if (length > self.max_msg_length) return null;

    const record_length: i32 = @intCast(length + constants.record_header_length);
    const aligned_length = std.mem.alignForward(
        usize,
        @intCast(record_length),
        constants.record_alignment,
    );

    const record_index = self.claimCapacity(aligned_length) orelse return null;

    // Write the header with NEGATIVE length — this is the "uncommitted" sentinel.
    // The consumer will see this and stop reading (length <= 0 check).
    @atomicStore(i32, self.recordLengthPtr(record_index), -record_length, .release);

    // Write the message type ID. Plain store is safe — the negative length
    // prevents the consumer from reading past the header.
    self.recordMsgTypeIdPtr(record_index).* = msg_type_id;

    // Return a writable slice for the payload region.
    const payload_offset = record_index + constants.record_header_length;
    return Claim{
        .buffer = self.buffer[payload_offset..][0..length],
        .ring_buffer = self,
        .header_index = record_index,
        .record_length = record_length,
    };
}
```

### 1.9 Commit and Abort

These are the two ways to finalize a claimed record:

```zig
/// Commit a record at the given index by flipping the length from negative
/// to positive. This is the linearization point — after this store, the
/// consumer can see and process the record.
fn commitAt(self: *RingBuffer, header_index: usize, record_length: i32) void {
    std.debug.assert(record_length > 0);

    // Release store: all payload writes by the caller happen-before this.
    // The consumer's acquire load on length will see the complete payload.
    @atomicStore(i32, self.recordLengthPtr(header_index), record_length, .release);

    if (self.blocking) {
        self.awakeReader();
    }
}

/// Abort a claimed record by converting it to a padding record. The consumer
/// will skip over it and continue reading subsequent records.
fn abortAt(self: *RingBuffer, header_index: usize, record_length: i32) void {
    std.debug.assert(record_length > 0);

    // Mark as padding so the consumer skips the payload.
    self.recordMsgTypeIdPtr(header_index).* = constants.padding_msg_type_id;

    // Commit with positive length — the consumer needs to see a committed
    // record to advance past this position.
    @atomicStore(i32, self.recordLengthPtr(header_index), record_length, .release);

    if (self.blocking) {
        self.awakeReader();
    }
}
```

**The write path for `write()` (copy-based)** uses a combined helper:

```zig
/// Internal: write a complete record at the given index. This is used by
/// the copy-based `write()` method.
fn writeRecord(
    self: *RingBuffer,
    record_index: usize,
    record_length: i32,
    msg_type_id: i32,
    payload: []const u8,
) void {
    // 1. Write the header with NEGATIVE length (uncommitted sentinel).
    //    This ensures a concurrent consumer stops here.
    @atomicStore(i32, self.recordLengthPtr(record_index), -record_length, .release);

    // 2. Write the message type ID.
    self.recordMsgTypeIdPtr(record_index).* = msg_type_id;

    // 3. Copy payload bytes into the data region.
    const payload_offset = record_index + constants.record_header_length;
    @memcpy(self.buffer[payload_offset..][0..payload.len], payload);

    // 4. Commit: flip length to positive with release semantics.
    //    This is the linearization point. All writes above are now visible
    //    to the consumer (who loads length with acquire).
    @atomicStore(i32, self.recordLengthPtr(record_index), record_length, .release);

    if (self.blocking) {
        self.awakeReader();
    }
}

/// Internal: write a padding record at the given index.
fn writePaddingRecord(self: *RingBuffer, index: usize, length: i32) void {
    // Padding type first, then commit with positive length.
    self.recordMsgTypeIdPtr(index).* = constants.padding_msg_type_id;
    @atomicStore(i32, self.recordLengthPtr(index), length, .release);
}
```

**Commit sequence summary (for both `write` and `tryClaim`):**

```
Producer thread:                     Consumer thread:
─────────────────                    ─────────────────
1. Write payload bytes               
2. Write msg_type_id                 
3. ─── release store (length > 0) ──────→ acquire load (length)
                                     4. Read msg_type_id    ✓ visible
                                     5. Read payload bytes  ✓ visible
```

The release-acquire pair on the `length` field is the sole synchronization mechanism
between producers and the consumer. No fences, no locks.

### 1.10 Dead Producer Recovery (`unblock`)

If a producer crashes (or hangs) between claiming space and committing the record, the
consumer is stuck: it sees `length <= 0` at the head and cannot advance. The `unblock()`
method detects and recovers from this situation.

```zig
/// Attempt to recover from a stuck producer. Call this periodically from
/// a background health-check loop (NOT from the consumer's hot path).
///
/// Returns true if an unblock was performed.
pub fn unblock(self: *RingBuffer) bool {
    const head = self.loadHeadPosition(.acquire);
    const tail = self.loadTailPosition(.acquire);

    if (head == tail) {
        // Buffer is empty — nothing to unblock.
        return false;
    }

    const head_index: usize = @intCast(head & self.capacity_mask);
    const record_length = @atomicLoad(i32, self.recordLengthPtr(head_index), .acquire);

    if (record_length < 0) {
        // ── Case 1: Uncommitted record at head ────────────────────────
        // A producer claimed this slot (via tryClaim) but never committed.
        // Convert it to a committed padding record so the consumer can skip it.
        self.recordMsgTypeIdPtr(head_index).* = constants.padding_msg_type_id;
        @atomicStore(i32, self.recordLengthPtr(head_index), -record_length, .release);

        if (self.blocking) {
            self.awakeReader();
            self.awakeWriter();
        }
        return true;
    }

    if (record_length == 0) {
        // ── Case 2: Gap at head ───────────────────────────────────────
        // The tail has advanced past head (we know head != tail), but the
        // record at head has length 0. This means a producer won the CAS
        // and advanced the tail but died before writing even the negative
        // sentinel.
        //
        // Insert padding spanning from head to either:
        //   a. The end of the buffer (if head_index + gap would wrap), or
        //   b. The distance to tail (if everything fits without wrapping).
        const distance_to_end = self.capacity - head_index;
        const distance_to_tail: usize = @intCast(tail - head);
        const padding_length = @min(distance_to_end, distance_to_tail);

        if (padding_length > 0) {
            self.recordMsgTypeIdPtr(head_index).* = constants.padding_msg_type_id;
            @atomicStore(
                i32,
                self.recordLengthPtr(head_index),
                @intCast(padding_length),
                .release,
            );

            if (self.blocking) {
                self.awakeReader();
                self.awakeWriter();
            }
            return true;
        }
    }

    return false;
}
```

**When to call `unblock()`:** This should be called from a periodic health-check task
(e.g., the broker's control loop, every few seconds). It is NOT safe to call from
multiple threads concurrently — it assumes single-writer semantics on the head region.
Typically the consumer thread calls it when it detects that it has been stuck (no
progress for N milliseconds despite non-empty tail).

### 1.11 Blocking Ring Buffer Extension

By default, the ring buffer is **non-blocking**: `write()` returns `error.BufferFull`
immediately and `read()` returns 0 immediately when empty. For cross-process IPC where
busy-spinning wastes CPU, blocking mode adds kernel-level park/wake using the platform's
`ProcessSynchronizer` (futex on Linux, ulock on macOS, WaitOnAddress on Windows — see
doc 01).

**Blocking prefix layout** (384 bytes, inserted between the metadata header and the
ring buffer data region in the mmap'd file):

```
┌────────────────────────────────────────────┐  ← blocking prefix start
│  Slot 0: writer_wait_state                 │
│    +0:  state (i32)     ← futex word       │
│    +4:  padding [124 bytes]                │
├────────────────────────────────────────────┤  ← +128
│  Slot 1: reader_wait_state                 │
│    +0:  state (i32)     ← futex word       │
│    +4:  padding [124 bytes]                │
├────────────────────────────────────────────┤  ← +256
│  Slot 2: wait_timeout                      │
│    +0:  timeout (i64)   ← nanoseconds      │
│    +8:  padding [120 bytes]                │
└────────────────────────────────────────────┘  ← +384 = ring buffer data start
```

**Blocking write** — when `write()` gets `BufferFull`:

```zig
/// Write a message, blocking if the buffer is full.
/// `timeout_ns`: maximum time to wait in nanoseconds, or null for indefinite.
pub fn writeBlocking(
    self: *RingBuffer,
    msg_type_id: i32,
    payload: []const u8,
    timeout_ns: ?i64,
) WriteError!void {
    // Fast path: try non-blocking write first.
    self.write(msg_type_id, payload) catch |err| switch (err) {
        error.BufferFull => {
            if (!self.blocking) return error.BufferFull;

            // Park on the writer wait state. The consumer will wake us
            // after advancing head_position.
            self.process_synchronizer.?.wait(self.writer_wait_state.?, timeout_ns);

            // Retry once after waking.
            return self.write(msg_type_id, payload);
        },
        else => return err,
    };
}
```

**Blocking read** — when `read()` returns 0:

```zig
/// Read messages, blocking if the buffer is empty.
pub fn readBlocking(
    self: *RingBuffer,
    handler: MessageHandler,
    limit: u32,
    timeout_ns: ?i64,
) u32 {
    // Fast path: try non-blocking read first.
    const count = self.read(handler, limit);
    if (count > 0) return count;

    if (!self.blocking) return 0;

    // Park on the reader wait state. A producer will wake us
    // after committing a record.
    self.process_synchronizer.?.wait(self.reader_wait_state.?, timeout_ns);

    // Retry once after waking.
    return self.read(handler, limit);
}
```

**Wake helpers:**

```zig
fn awakeReader(self: *RingBuffer) void {
    if (self.process_synchronizer) |sync| {
        if (self.reader_wait_state) |state| {
            sync.wake(state, true); // wake_all = true
        }
    }
}

fn awakeWriter(self: *RingBuffer) void {
    if (self.process_synchronizer) |sync| {
        if (self.writer_wait_state) |state| {
            sync.wake(state, true);
        }
    }
}
```

**Integration with the consumer's `read()` method:** After advancing `head_position` and
zeroing consumed data, the non-blocking `read()` must also wake blocked writers:

```zig
// At the end of read(), after storeHeadPosition:
if (bytes_consumed > 0) {
    @memset(self.buffer[head_index..][0..bytes_consumed], 0);
    self.storeHeadPosition(head + @as(i64, @intCast(bytes_consumed)), .release);

    if (self.blocking) {
        self.awakeWriter(); // <── wake any parked producers
    }
}
```

And after `writeRecord()` / `commitAt()`, the producer wakes the reader:

```zig
// Already shown in writeRecord and commitAt above:
if (self.blocking) {
    self.awakeReader();
}
```

**Spurious wakeups are expected and safe.** Both the write and read retry paths simply
attempt the operation again. If the buffer is still full/empty after waking, the caller
can choose to re-park or return a failure.

### 1.12 Utility Accessors

```zig
/// Return the usable data capacity in bytes.
pub fn capacity(self: *const RingBuffer) usize {
    return self.capacity;
}

/// Return the maximum allowed message payload length.
pub fn maxMessageLength(self: *const RingBuffer) usize {
    return self.max_msg_length;
}

/// Return the current producer position (logical, monotonically increasing).
pub fn producerPosition(self: *const RingBuffer) i64 {
    return self.loadTailPosition(.acquire);
}

/// Return the current consumer position (logical, monotonically increasing).
pub fn consumerPosition(self: *const RingBuffer) i64 {
    return self.loadHeadPosition(.acquire);
}

/// Return the approximate number of bytes currently used in the buffer.
pub fn size(self: *const RingBuffer) usize {
    var head = self.loadHeadPosition(.acquire);
    var tail = self.loadTailPosition(.acquire);
    // Guard against reading head before tail in a racy scenario.
    while (tail < head) {
        head = self.loadHeadPosition(.acquire);
        tail = self.loadTailPosition(.acquire);
    }
    return @intCast(tail - head);
}

/// Atomically increment and return the next correlation ID.
/// Used to generate unique IDs for request-response correlation.
pub fn nextCorrelationId(self: *RingBuffer) i64 {
    return @atomicRmw(
        i64,
        self.trailerFieldPtr(constants.correlation_counter_offset),
        .Add,
        1,
        .monotonic,
    );
}

/// Write the consumer heartbeat timestamp (epoch milliseconds).
/// Called periodically by the consumer to signal liveness.
pub fn setConsumerHeartbeatTime(self: *RingBuffer, epoch_ms: i64) void {
    self.storeConsumerHeartbeat(epoch_ms, .release);
}

/// Read the consumer heartbeat timestamp.
/// Used by health checkers to detect dead consumers.
pub fn consumerHeartbeatTime(self: *const RingBuffer) i64 {
    return self.loadConsumerHeartbeat(.acquire);
}

/// Returns true if this ring buffer was created in blocking mode.
pub fn isBlocking(self: *const RingBuffer) bool {
    return self.blocking;
}
```

---

## 2. Counters

Counters are shared atomic `i64` values used for monitoring and diagnostics. They are
accessed by the broker, services, and external monitoring tools (which mmap the same
file). Each counter has a **value** (in one buffer) and **metadata** (in another buffer),
separated so that high-frequency value updates don't invalidate cache lines holding
rarely-changing metadata.

### 2.1 Counter Layout

**Counter Values Buffer** — one 128-byte slot per counter:

```
┌──────────────────────────────────────────┐
│  Counter 0                               │
│    +0:   value (volatile i64)            │
│    +8:   padding [120 bytes]             │
├──────────────────────────────────────────┤  ← +128
│  Counter 1                               │
│    +0:   value (volatile i64)            │
│    +8:   padding [120 bytes]             │
├──────────────────────────────────────────┤  ← +256
│  ...                                     │
└──────────────────────────────────────────┘
```

**Counter Metadata Buffer** — one 256-byte slot per counter:

```
┌──────────────────────────────────────────┐
│  Counter 0 Metadata                      │
│    +0:   state (volatile i32)            │  0 = UNUSED, 1 = ALLOCATED, -1 = RECLAIMED
│    +4:   type_id (i32)                   │  application-defined counter type
│    +8:   label_length (i32)              │
│    +12:  label [244 bytes]               │  human-readable name (UTF-8)
├──────────────────────────────────────────┤  ← +256
│  Counter 1 Metadata                      │
│  ...                                     │
└──────────────────────────────────────────┘
```

**Why 128-byte value slots?** Same reason as the ring buffer trailer: prevent false
sharing. Counter 0 and Counter 1 must never share a cache line, because they may be
incremented by different threads on different cores.

**Why 256-byte metadata slots?** Metadata is larger (label can be up to 244 bytes) and
is written once at allocation time. The extra space is not a concern — the metadata
buffer is sized at startup and never grows.

**Counter states:**

| Value | Name | Meaning |
|---|---|---|
| `0` | `UNUSED` | Slot is free for allocation |
| `1` | `ALLOCATED` | Slot is in use — value is meaningful |
| `-1` | `RECLAIMED` | Slot was freed — value should be ignored. Can be re-allocated |

### 2.2 Implementation: `CountersManager`

```zig
// src/concurrent/counters.zig

const std = @import("std");
const constants = @import("../constants.zig");

pub const counter_value_length: usize = constants.cache_line_pad; // 128
pub const counter_metadata_length: usize = 256;

pub const CounterState = enum(i32) {
    unused = 0,
    allocated = 1,
    reclaimed = -1,
};

pub const CountersManager = struct {
    values_buffer: []align(constants.cache_line_pad) u8,
    metadata_buffer: []u8,
    max_counter_id: usize,

    pub fn init(
        values_buffer: []align(constants.cache_line_pad) u8,
        metadata_buffer: []u8,
    ) CountersManager {
        const max_by_values = values_buffer.len / counter_value_length;
        const max_by_metadata = metadata_buffer.len / counter_metadata_length;
        const max_id = @min(max_by_values, max_by_metadata);

        return .{
            .values_buffer = values_buffer,
            .metadata_buffer = metadata_buffer,
            .max_counter_id = if (max_id > 0) max_id - 1 else 0,
        };
    }

    /// Allocate a new counter. Scans for the first UNUSED slot and atomically
    /// transitions it to ALLOCATED. Returns the counter ID, or null if all
    /// slots are full.
    pub fn allocate(self: *CountersManager, type_id: i32, label: []const u8) ?usize {
        var id: usize = 0;
        while (id <= self.max_counter_id) : (id += 1) {
            const state_ptr = self.metadataStatePtr(id);
            const current = @atomicLoad(i32, state_ptr, .acquire);

            if (current == @intFromEnum(CounterState.unused) or
                current == @intFromEnum(CounterState.reclaimed))
            {
                // Try to claim this slot.
                const result = @cmpxchgStrong(
                    i32,
                    state_ptr,
                    current,
                    @intFromEnum(CounterState.allocated),
                    .acq_rel,
                    .monotonic,
                );

                if (result == null) {
                    // Won the slot — write metadata.
                    self.writeMetadata(id, type_id, label);
                    // Zero the value.
                    @atomicStore(i64, self.valuePtr(id), 0, .release);
                    return id;
                }
            }
        }

        return null; // All slots full.
    }

    /// Free a counter, setting its state to RECLAIMED and zeroing the value.
    pub fn free(self: *CountersManager, counter_id: usize) void {
        std.debug.assert(counter_id <= self.max_counter_id);

        @atomicStore(i64, self.valuePtr(counter_id), 0, .release);
        @atomicStore(i32, self.metadataStatePtr(counter_id), @intFromEnum(CounterState.reclaimed), .release);
    }

    /// Get the current value of a counter (atomic load).
    pub fn get(self: *const CountersManager, counter_id: usize) i64 {
        std.debug.assert(counter_id <= self.max_counter_id);
        return @atomicLoad(i64, self.valuePtr(counter_id), .acquire);
    }

    /// Atomically increment a counter by 1.
    pub fn increment(self: *CountersManager, counter_id: usize) void {
        self.add(counter_id, 1);
    }

    /// Atomically add `delta` to a counter.
    pub fn add(self: *CountersManager, counter_id: usize, delta: i64) void {
        std.debug.assert(counter_id <= self.max_counter_id);
        _ = @atomicRmw(i64, self.valuePtr(counter_id), .Add, delta, .monotonic);
    }

    /// Set a counter to an absolute value (atomic store).
    pub fn set(self: *CountersManager, counter_id: usize, value: i64) void {
        std.debug.assert(counter_id <= self.max_counter_id);
        @atomicStore(i64, self.valuePtr(counter_id), value, .release);
    }

    /// Return the maximum valid counter ID.
    pub fn maxCounterId(self: *const CountersManager) usize {
        return self.max_counter_id;
    }

    /// Iterate all allocated counters, calling `callback` for each.
    pub fn forEach(
        self: *const CountersManager,
        callback: *const fn (id: usize, type_id: i32, label: []const u8, value: i64) void,
    ) void {
        var id: usize = 0;
        while (id <= self.max_counter_id) : (id += 1) {
            const state = @atomicLoad(i32, self.metadataStatePtr(id), .acquire);
            if (state == @intFromEnum(CounterState.allocated)) {
                const meta = self.readMetadata(id);
                const value = @atomicLoad(i64, self.valuePtr(id), .acquire);
                callback(id, meta.type_id, meta.label, value);
            }
        }
    }

    // ── Internal helpers ──────────────────────────────────────────────

    fn valuePtr(self: *const CountersManager, id: usize) *i64 {
        const offset = id * counter_value_length;
        return @ptrCast(@alignCast(self.values_buffer.ptr + offset));
    }

    fn metadataStatePtr(self: *const CountersManager, id: usize) *i32 {
        const offset = id * counter_metadata_length;
        return @ptrCast(@alignCast(self.metadata_buffer.ptr + offset));
    }

    const MetadataView = struct {
        type_id: i32,
        label: []const u8,
    };

    fn writeMetadata(self: *CountersManager, id: usize, type_id: i32, label: []const u8) void {
        const base = id * counter_metadata_length;

        // type_id at offset +4
        const type_id_ptr: *i32 = @ptrCast(@alignCast(self.metadata_buffer.ptr + base + 4));
        type_id_ptr.* = type_id;

        // label_length at offset +8
        const label_len = @min(label.len, 244);
        const label_len_ptr: *i32 = @ptrCast(@alignCast(self.metadata_buffer.ptr + base + 8));
        label_len_ptr.* = @intCast(label_len);

        // label bytes at offset +12
        @memcpy(self.metadata_buffer[base + 12 ..][0..label_len], label[0..label_len]);
    }

    fn readMetadata(self: *const CountersManager, id: usize) MetadataView {
        const base = id * counter_metadata_length;

        const type_id_ptr: *const i32 = @ptrCast(@alignCast(self.metadata_buffer.ptr + base + 4));
        const label_len_ptr: *const i32 = @ptrCast(@alignCast(self.metadata_buffer.ptr + base + 8));
        const label_len: usize = @intCast(label_len_ptr.*);

        return .{
            .type_id = type_id_ptr.*,
            .label = self.metadata_buffer[base + 12 ..][0..label_len],
        };
    }
};
```

**Usage example:**

```zig
// At startup:
var counters = CountersManager.init(counter_values_slice, counter_metadata_slice);

// Allocate well-known counters:
const bytes_sent_id = counters.allocate(0, "bytes_sent") orelse @panic("out of counters");
const msgs_routed_id = counters.allocate(0, "messages_routed_local") orelse @panic("out of counters");

// On the hot path — zero allocation, single atomic add:
counters.add(bytes_sent_id, @intCast(frame_length));
counters.increment(msgs_routed_id);

// From a monitoring tool:
counters.forEach(printCounter);
```

---

## 3. Error Log

The error log is a flat buffer for recording unique error observations. Unlike a
traditional log that appends every error message, the error log **deduplicates**: if the
same error is observed multiple times, the existing entry's `observation_count` is
incremented and its `last_observation_timestamp` is updated. This keeps the log bounded
and avoids the "10 million 'buffer full' messages" problem.

### 3.1 Error Log Entry Layout

```
┌────────────────────────────────────────────────────────┐  ← entry start
│  length                    (volatile i32)  — 4 bytes   │  0 = empty, >0 = entry size
│  observation_count         (volatile i32)  — 4 bytes   │  incremented for repeats
│  last_observation_timestamp (volatile i64) — 8 bytes   │  epoch ms of most recent
│  first_observation_timestamp (i64)         — 8 bytes   │  epoch ms of first occurrence
│  description               (variable)     — N bytes    │  UTF-8 error description
└────────────────────────────────────────────────────────┘

Entry total size: 24 + description.len, aligned to 4 bytes.
```

The error log is a **linear array** of entries packed sequentially. New entries are
appended at `next_offset`. The log does NOT wrap — once full, new unique errors are
dropped (but existing entries continue to have their counts incremented).

### 3.2 Implementation: `ErrorLog`

```zig
// src/concurrent/error_log.zig

const std = @import("std");

pub const entry_header_length: usize = 24; // 4 + 4 + 8 + 8
pub const entry_alignment: usize = @sizeOf(i32); // 4 bytes

pub const ErrorLog = struct {
    buffer: []u8,

    /// Offset of the next free slot in the buffer. Only modified by `record()`.
    next_offset: usize,

    pub fn init(buffer: []u8) ErrorLog {
        return .{
            .buffer = buffer,
            .next_offset = 0,
        };
    }

    /// Record an error observation. If an entry with the same description
    /// already exists, increments its observation count. Otherwise, appends
    /// a new entry.
    ///
    /// Returns true if the observation was recorded, false if the log is
    /// full and this is a new (non-duplicate) error.
    pub fn record(self: *ErrorLog, description: []const u8, timestamp_ms: i64) bool {
        // ── Phase 1: scan for an existing entry with the same description ──
        var offset: usize = 0;
        while (offset < self.next_offset) {
            const entry_length = self.readEntryLength(offset);
            if (entry_length <= 0) break;

            const desc_len: usize = @intCast(entry_length - entry_header_length);
            const desc_offset = offset + entry_header_length;
            const existing_desc = self.buffer[desc_offset..][0..desc_len];

            if (std.mem.eql(u8, existing_desc, description)) {
                // Found a match — increment observation count and update timestamp.
                self.incrementObservationCount(offset);
                self.updateLastTimestamp(offset, timestamp_ms);
                return true;
            }

            offset += std.mem.alignForward(usize, @intCast(entry_length), entry_alignment);
        }

        // ── Phase 2: append a new entry ────────────────────────────────
        const total_length: usize = entry_header_length + description.len;
        const aligned_length = std.mem.alignForward(usize, total_length, entry_alignment);

        if (self.next_offset + aligned_length > self.buffer.len) {
            return false; // Log is full.
        }

        const base = self.next_offset;

        // Write fields in order. The length is written LAST with release
        // semantics so that concurrent readers see a consistent entry.

        // observation_count = 1 (at base + 4)
        const obs_ptr: *i32 = @ptrCast(@alignCast(self.buffer.ptr + base + 4));
        @atomicStore(i32, obs_ptr, 1, .monotonic);

        // last_observation_timestamp = timestamp (at base + 8)
        const last_ts_ptr: *i64 = @ptrCast(@alignCast(self.buffer.ptr + base + 8));
        @atomicStore(i64, last_ts_ptr, timestamp_ms, .monotonic);

        // first_observation_timestamp = timestamp (at base + 16)
        const first_ts_ptr: *i64 = @ptrCast(@alignCast(self.buffer.ptr + base + 16));
        first_ts_ptr.* = timestamp_ms;

        // description bytes (at base + 24)
        @memcpy(self.buffer[base + entry_header_length ..][0..description.len], description);

        // Commit: write length with release store.
        const length_ptr: *volatile i32 = @ptrCast(@alignCast(self.buffer.ptr + base));
        @atomicStore(i32, length_ptr, @intCast(total_length), .release);

        self.next_offset = base + aligned_length;
        return true;
    }

    /// Entry view returned by the iterator.
    pub const Entry = struct {
        observation_count: i32,
        last_observation_timestamp: i64,
        first_observation_timestamp: i64,
        description: []const u8,
    };

    /// Iterate all entries in the error log, calling `callback` for each.
    pub fn forEach(
        self: *const ErrorLog,
        callback: *const fn (entry: Entry) void,
    ) void {
        var offset: usize = 0;
        while (offset < self.buffer.len) {
            const entry_length = self.readEntryLength(offset);
            if (entry_length <= 0) break;

            const base = offset;
            const obs_ptr: *const i32 = @ptrCast(@alignCast(self.buffer.ptr + base + 4));
            const last_ts_ptr: *const i64 = @ptrCast(@alignCast(self.buffer.ptr + base + 8));
            const first_ts_ptr: *const i64 = @ptrCast(@alignCast(self.buffer.ptr + base + 16));

            const desc_len: usize = @intCast(entry_length - entry_header_length);

            callback(.{
                .observation_count = @atomicLoad(i32, obs_ptr, .acquire),
                .last_observation_timestamp = @atomicLoad(i64, last_ts_ptr, .acquire),
                .first_observation_timestamp = first_ts_ptr.*,
                .description = self.buffer[base + entry_header_length ..][0..desc_len],
            });

            offset += std.mem.alignForward(usize, @intCast(entry_length), entry_alignment);
        }
    }

    // ── Internal helpers ──────────────────────────────────────────────

    fn readEntryLength(self: *const ErrorLog, offset: usize) i32 {
        const ptr: *const i32 = @ptrCast(@alignCast(self.buffer.ptr + offset));
        return @atomicLoad(i32, ptr, .acquire);
    }

    fn incrementObservationCount(self: *ErrorLog, offset: usize) void {
        const ptr: *i32 = @ptrCast(@alignCast(self.buffer.ptr + offset + 4));
        _ = @atomicRmw(i32, ptr, .Add, 1, .monotonic);
    }

    fn updateLastTimestamp(self: *ErrorLog, offset: usize, timestamp_ms: i64) void {
        const ptr: *i64 = @ptrCast(@alignCast(self.buffer.ptr + offset + 8));
        @atomicStore(i64, ptr, timestamp_ms, .release);
    }
};
```

**Usage example:**

```zig
var error_log = ErrorLog.init(error_log_buffer);

// First occurrence:
_ = error_log.record("send ring buffer full for service 7", getEpochMs());

// Same error again — observation_count becomes 2:
_ = error_log.record("send ring buffer full for service 7", getEpochMs());

// Different error — new entry:
_ = error_log.record("unknown target node 42", getEpochMs());

// Iterate all:
error_log.forEach(struct {
    fn print(entry: ErrorLog.Entry) void {
        std.log.warn(
            "[{d}x] {s} (first: {d}, last: {d})",
            .{ entry.observation_count, entry.description,
               entry.first_observation_timestamp, entry.last_observation_timestamp },
        );
    }
}.print);
```

---

## 4. Thread-Local Error State

For detailed error reporting in contexts where returning an error code through the call
stack is not practical (e.g., deep inside a callback chain), each thread maintains a
thread-local error state.

```zig
// src/concurrent/error_state.zig

pub const max_error_message_length: usize = 8192;

pub const ErrorState = struct {
    errcode: i32 = 0,
    errmsg: [max_error_message_length]u8 = undefined,
    msg_len: usize = 0,

    /// Set the error code and message.
    pub fn set(self: *ErrorState, code: i32, msg: []const u8) void {
        self.errcode = code;
        const len = @min(msg.len, max_error_message_length);
        @memcpy(self.errmsg[0..len], msg[0..len]);
        self.msg_len = len;
    }

    /// Set the error code and a formatted message.
    pub fn setFmt(self: *ErrorState, code: i32, comptime fmt: []const u8, args: anytype) void {
        self.errcode = code;
        const result = std.fmt.bufPrint(&self.errmsg, fmt, args) catch |err| switch (err) {
            error.NoSpaceLeft => {
                // Truncated — that's fine, we keep what we have.
                self.msg_len = max_error_message_length;
                return;
            },
        };
        self.msg_len = result.len;
    }

    /// Clear the error state.
    pub fn clear(self: *ErrorState) void {
        self.errcode = 0;
        self.msg_len = 0;
    }

    /// Get the current error message, or null if no error is set.
    pub fn message(self: *const ErrorState) ?[]const u8 {
        if (self.errcode == 0) return null;
        return self.errmsg[0..self.msg_len];
    }

    /// Returns true if an error is currently set.
    pub fn isSet(self: *const ErrorState) bool {
        return self.errcode != 0;
    }
};

/// Thread-local error state — each thread gets its own instance.
pub threadlocal var err_state: ErrorState = .{};
```

**Usage pattern:**

```zig
const error_state = @import("concurrent/error_state.zig");

fn routeMessage(target_node_id: i32, payload: []const u8) bool {
    const producer = registry.getProducer(target_node_id) orelse {
        error_state.err_state.setFmt(
            -1,
            "no producer for node {d}",
            .{target_node_id},
        );
        return false;
    };

    producer.write(payload) catch |err| {
        error_state.err_state.setFmt(
            -2,
            "write to node {d} failed: {s}",
            .{ target_node_id, @errorName(err) },
        );
        return false;
    };

    return true;
}
```

This is not a replacement for Zig's error return mechanism — it is a supplementary
channel for rich diagnostic context that would be awkward to pass through error return
values (especially through function pointer callbacks like `MessageHandler`).

---

## 5. Testing

Thorough testing of lock-free data structures is essential. Bugs in CAS loops or memory
ordering can be silent on x86 (which has a strong memory model) and only manifest on ARM
or under extreme contention.

### 5.1 Ring Buffer Tests

**Single-threaded correctness:**

```zig
test "write and read single message" {
    var buf = allocateAlignedBuffer(1024 + constants.trailer_length);
    defer freeBuffer(buf);
    @memset(buf, 0);

    var rb = try RingBuffer.init(buf, false, null, null);

    const payload = "hello, ring buffer!";
    try rb.write(1, payload);

    var received_type: i32 = 0;
    var received_payload: []const u8 = &.{};

    const count = rb.read(struct {
        fn handler(msg_type_id: i32, data: []const u8) void {
            received_type = msg_type_id;
            received_payload = data;
        }
    }.handler, 10);

    try std.testing.expectEqual(@as(u32, 1), count);
    try std.testing.expectEqual(@as(i32, 1), received_type);
    try std.testing.expectEqualSlices(u8, payload, received_payload);
}
```

**Write N, read N:**

```zig
test "write N messages then read all" {
    var buf = allocateAlignedBuffer(4096 + constants.trailer_length);
    defer freeBuffer(buf);
    @memset(buf, 0);

    var rb = try RingBuffer.init(buf, false, null, null);
    const n: usize = 50;

    // Write N messages with distinct payloads.
    for (0..n) |i| {
        var payload: [32]u8 = undefined;
        const len = std.fmt.bufPrint(&payload, "message-{d}", .{i}) catch unreachable;
        try rb.write(@intCast(i + 1), len);
    }

    // Read all N.
    var count: u32 = 0;
    count = rb.read(struct {
        fn handler(_: i32, _: []const u8) void {}
    }.handler, 1000);

    try std.testing.expectEqual(@as(u32, @intCast(n)), count);
}
```

**Wrap-around:**

```zig
test "wrap-around: write fills to end, next message wraps to start" {
    // Small capacity so wrapping is easy to trigger.
    const capacity = 256;
    var buf = allocateAlignedBuffer(capacity + constants.trailer_length);
    defer freeBuffer(buf);
    @memset(buf, 0);

    var rb = try RingBuffer.init(buf, false, null, null);

    // Write messages until buffer is mostly full.
    var written: usize = 0;
    while (written < capacity - 40) {
        const payload = "wrap-test-payload!";
        rb.write(1, payload) catch break;
        written += std.mem.alignForward(
            usize,
            payload.len + constants.record_header_length,
            constants.record_alignment,
        );
    }

    // Read all to free space.
    _ = rb.read(struct {
        fn handler(_: i32, _: []const u8) void {}
    }.handler, 1000);

    // Now write a message that triggers wrapping.
    // The tail is near the end; the message should wrap to the start.
    try rb.write(42, "this-wraps-around");

    var got_type: i32 = 0;
    _ = rb.read(struct {
        fn handler(msg_type_id: i32, _: []const u8) void {
            got_type = msg_type_id;
        }
    }.handler, 1000);

    try std.testing.expectEqual(@as(i32, 42), got_type);
}
```

**Buffer full:**

```zig
test "write returns error.BufferFull when buffer is exhausted" {
    const capacity = 128; // Minimal
    var buf = allocateAlignedBuffer(capacity + constants.trailer_length);
    defer freeBuffer(buf);
    @memset(buf, 0);

    var rb = try RingBuffer.init(buf, false, null, null);

    // Fill the buffer. Max message = 128 / 8 = 16 bytes.
    var full = false;
    for (0..100) |_| {
        rb.write(1, "fill") catch |err| {
            try std.testing.expectEqual(RingBuffer.WriteError.BufferFull, err);
            full = true;
            break;
        };
    }
    try std.testing.expect(full);
}
```

**Try-claim + commit:**

```zig
test "tryClaim and commit" {
    var buf = allocateAlignedBuffer(1024 + constants.trailer_length);
    defer freeBuffer(buf);
    @memset(buf, 0);

    var rb = try RingBuffer.init(buf, false, null, null);

    // Claim space for a 16-byte payload.
    var claim = rb.tryClaim(7, 16) orelse return error.Unexpected;

    // Write directly into the ring buffer.
    @memcpy(claim.buffer[0..5], "hello");
    claim.commit();

    var got_type: i32 = 0;
    const count = rb.read(struct {
        fn handler(msg_type_id: i32, _: []const u8) void {
            got_type = msg_type_id;
        }
    }.handler, 10);

    try std.testing.expectEqual(@as(u32, 1), count);
    try std.testing.expectEqual(@as(i32, 7), got_type);
}
```

**Try-claim + abort:**

```zig
test "tryClaim and abort is skipped by consumer" {
    var buf = allocateAlignedBuffer(1024 + constants.trailer_length);
    defer freeBuffer(buf);
    @memset(buf, 0);

    var rb = try RingBuffer.init(buf, false, null, null);

    // Claim and abort — should become padding.
    var claim = rb.tryClaim(7, 16) orelse return error.Unexpected;
    claim.abort();

    // Write a real message after the aborted one.
    try rb.write(99, "after-abort");

    // Read — should only get the real message, not the aborted one.
    var got_type: i32 = 0;
    const count = rb.read(struct {
        fn handler(msg_type_id: i32, _: []const u8) void {
            got_type = msg_type_id;
        }
    }.handler, 10);

    try std.testing.expectEqual(@as(u32, 1), count);
    try std.testing.expectEqual(@as(i32, 99), got_type);
}
```

**Multi-threaded write stress test:**

```zig
test "multi-producer stress: M threads × K messages" {
    const capacity = 64 * 1024; // 64 KB
    var buf = allocateAlignedBuffer(capacity + constants.trailer_length);
    defer freeBuffer(buf);
    @memset(buf, 0);

    var rb = try RingBuffer.init(buf, false, null, null);

    const M = 4;  // producer threads
    const K = 1000; // messages per thread
    var total_read = std.atomic.Value(u64).init(0);

    // Spawn M producer threads.
    var threads: [M]std.Thread = undefined;
    for (0..M) |t| {
        threads[t] = try std.Thread.spawn(.{}, struct {
            fn run(ring: *RingBuffer, thread_id: usize) void {
                for (0..K) |i| {
                    var payload: [48]u8 = undefined;
                    const len = std.fmt.bufPrint(&payload, "t{d}-msg{d}", .{ thread_id, i }) catch unreachable;
                    // Retry on BufferFull.
                    while (true) {
                        ring.write(1, len) catch {
                            std.Thread.yield() catch {};
                            continue;
                        };
                        break;
                    }
                }
            }
        }.run, .{ &rb, t });
    }

    // Consumer thread: read until we have M × K messages.
    var messages_read: u64 = 0;
    while (messages_read < M * K) {
        const n = rb.read(struct {
            fn handler(_: i32, _: []const u8) void {}
        }.handler, 256);
        messages_read += n;
        if (n == 0) {
            std.Thread.yield() catch {};
        }
    }

    // Join producers.
    for (&threads) |*t| t.join();

    try std.testing.expectEqual(@as(u64, M * K), messages_read);
}
```

**Dead producer recovery:**

```zig
test "unblock recovers from crashed producer (negative length)" {
    var buf = allocateAlignedBuffer(1024 + constants.trailer_length);
    defer freeBuffer(buf);
    @memset(buf, 0);

    var rb = try RingBuffer.init(buf, false, null, null);

    // Simulate a producer that claims but never commits.
    var claim = rb.tryClaim(1, 32) orelse return error.Unexpected;
    // "crash" — don't call claim.commit() or claim.abort()
    _ = claim; // suppress unused warning

    // Write a second message (this goes after the uncommitted one).
    try rb.write(2, "after-crash");

    // Consumer is stuck — length at head is negative.
    const count_before = rb.read(struct {
        fn handler(_: i32, _: []const u8) void {}
    }.handler, 10);
    try std.testing.expectEqual(@as(u32, 0), count_before);

    // Unblock recovers the stuck slot.
    const unblocked = rb.unblock();
    try std.testing.expect(unblocked);

    // Now both the padding (former uncommitted) and the real message are readable.
    var got_type: i32 = 0;
    const count_after = rb.read(struct {
        fn handler(msg_type_id: i32, _: []const u8) void {
            got_type = msg_type_id;
        }
    }.handler, 10);

    // Should read at least the message after the crashed one.
    try std.testing.expect(count_after >= 1);
    try std.testing.expectEqual(@as(i32, 2), got_type);
}
```

### 5.2 Counter Tests

```zig
test "allocate and increment counter" {
    var values_buf: [128 * 4]u8 align(128) = [_]u8{0} ** (128 * 4);
    var meta_buf: [256 * 4]u8 = [_]u8{0} ** (256 * 4);

    var cm = CountersManager.init(&values_buf, &meta_buf);

    const id = cm.allocate(1, "test_counter") orelse return error.Unexpected;
    try std.testing.expectEqual(@as(i64, 0), cm.get(id));

    cm.increment(id);
    cm.increment(id);
    cm.increment(id);
    try std.testing.expectEqual(@as(i64, 3), cm.get(id));

    cm.add(id, 10);
    try std.testing.expectEqual(@as(i64, 13), cm.get(id));
}

test "free and reallocate counter" {
    var values_buf: [128 * 2]u8 align(128) = [_]u8{0} ** (128 * 2);
    var meta_buf: [256 * 2]u8 = [_]u8{0} ** (256 * 2);

    var cm = CountersManager.init(&values_buf, &meta_buf);

    const id1 = cm.allocate(1, "counter_a") orelse return error.Unexpected;
    const id2 = cm.allocate(2, "counter_b") orelse return error.Unexpected;

    // Buffer is full (only 2 slots).
    try std.testing.expectEqual(@as(?usize, null), cm.allocate(3, "counter_c"));

    // Free one.
    cm.free(id1);

    // Now we can allocate again, reusing the freed slot.
    const id3 = cm.allocate(3, "counter_c") orelse return error.Unexpected;
    try std.testing.expectEqual(id1, id3); // Same slot reused.

    _ = id2;
}

test "forEach iterates only allocated counters" {
    var values_buf: [128 * 4]u8 align(128) = [_]u8{0} ** (128 * 4);
    var meta_buf: [256 * 4]u8 = [_]u8{0} ** (256 * 4);

    var cm = CountersManager.init(&values_buf, &meta_buf);

    _ = cm.allocate(1, "active_1");
    const id2 = cm.allocate(2, "freed_one") orelse return error.Unexpected;
    _ = cm.allocate(3, "active_2");
    cm.free(id2);

    var count: usize = 0;
    cm.forEach(struct {
        fn cb(_: usize, _: i32, _: []const u8, _: i64) void {
            count += 1;
        }
    }.cb);

    try std.testing.expectEqual(@as(usize, 2), count);
}
```

### 5.3 Error Log Tests

```zig
test "record new error" {
    var buf: [4096]u8 = [_]u8{0} ** 4096;
    var log = ErrorLog.init(&buf);

    const ok = log.record("something went wrong", 1000);
    try std.testing.expect(ok);

    var entry_count: usize = 0;
    log.forEach(struct {
        fn cb(entry: ErrorLog.Entry) void {
            entry_count += 1;
            std.testing.expectEqual(@as(i32, 1), entry.observation_count) catch unreachable;
            std.testing.expectEqual(@as(i64, 1000), entry.first_observation_timestamp) catch unreachable;
            std.testing.expectEqual(@as(i64, 1000), entry.last_observation_timestamp) catch unreachable;
            std.testing.expectEqualSlices(u8, "something went wrong", entry.description) catch unreachable;
        }
    }.cb);

    try std.testing.expectEqual(@as(usize, 1), entry_count);
}

test "record same error twice increments observation_count" {
    var buf: [4096]u8 = [_]u8{0} ** 4096;
    var log = ErrorLog.init(&buf);

    _ = log.record("buffer full", 1000);
    _ = log.record("buffer full", 2000);

    log.forEach(struct {
        fn cb(entry: ErrorLog.Entry) void {
            std.testing.expectEqual(@as(i32, 2), entry.observation_count) catch unreachable;
            std.testing.expectEqual(@as(i64, 1000), entry.first_observation_timestamp) catch unreachable;
            std.testing.expectEqual(@as(i64, 2000), entry.last_observation_timestamp) catch unreachable;
        }
    }.cb);
}

test "record returns false when log is full" {
    // Tiny buffer — only room for one or two entries.
    var buf: [64]u8 = [_]u8{0} ** 64;
    var log = ErrorLog.init(&buf);

    const ok1 = log.record("first", 1000);
    try std.testing.expect(ok1);

    // This should fail — not enough room for a second entry.
    const ok2 = log.record("second error that is quite long and won't fit", 2000);
    try std.testing.expect(!ok2);
}

test "different errors get separate entries" {
    var buf: [4096]u8 = [_]u8{0} ** 4096;
    var log = ErrorLog.init(&buf);

    _ = log.record("error A", 1000);
    _ = log.record("error B", 2000);
    _ = log.record("error A", 3000); // Increment first entry.

    var entry_count: usize = 0;
    log.forEach(struct {
        fn cb(_: ErrorLog.Entry) void {
            entry_count += 1;
        }
    }.cb);

    try std.testing.expectEqual(@as(usize, 2), entry_count);
}
```

### 5.4 Testing Tips

1. **Always `@memset(buf, 0)` before initializing a ring buffer.** The read algorithm
   relies on `length == 0` meaning "empty." Uninitialized memory will cause phantom
   reads.

2. **Test on ARM.** x86's strong memory model hides acquire/release bugs. If you only
   have x86 hardware, use ThreadSanitizer (`-fsanitize=thread` via Zig's
   `-DReleaseSafe` or by passing flags to the linker) to catch data races.

3. **Use small capacities in tests.** A 128-byte or 256-byte ring buffer forces
   wrap-around, padding, and buffer-full conditions with just a few writes. This
   exercises edge cases that a 1 MB buffer would never hit in a unit test.

4. **Multi-threaded tests need patience.** Lock-free bugs are often timing-dependent.
   Run stress tests in a loop (1000+ iterations) with varying thread counts to increase
   the chance of hitting race windows.

5. **Verify zeroing.** After reading, inspect the consumed region to confirm it was
   zeroed. A missing `@memset` is a correctness bug that may only manifest after a
   buffer wraps.

6. **Test correlation ID uniqueness.** Spawn multiple threads calling
   `nextCorrelationId()` concurrently and verify that all returned values are unique.

---

*Previous: [02 — Memory Layout & Shared Memory](02-memory-layout-and-shared-memory.md)*
·
*Next: [04 — TCP Transport Library](04-tcp-transport-library.md)*
