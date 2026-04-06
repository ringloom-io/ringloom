//! Buffer pool — fixed-size pool of pre-allocated, cache-line-aligned buffers.
//!
//! Used by both the sender (to stage outbound frames) and the receiver (to
//! provide buffers for incoming packets).
//!
//! Design constraints:
//! - Single-threaded access. Each pool belongs to one event loop thread.
//! - Fixed capacity. All buffers allocated at init, never grown.
//! - Cache-line aligned. Each buffer starts at a 64-byte boundary.

const std = @import("std");
const builtin = @import("builtin");
const constants = @import("brz_common").platform.constants;
const Allocator = std.mem.Allocator;

/// A buffer slot returned by `acquire`. Contains the buffer index (for
/// io_uring registration correlation) and the buffer memory itself.
pub const BufferSlot = struct {
    /// Index of this buffer in the pool. Used as the registered buffer index
    /// for io_uring operations.
    index: u16,

    /// The buffer memory itself.
    buffer: []align(constants.cache_line_length) u8,
};

/// Fixed-size pool of pre-allocated, cache-line-aligned buffers.
///
/// Acquire/release are O(1) operations using a simple free stack.
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
        var allocated: usize = 0;
        errdefer {
            for (0..allocated) |i| {
                allocator.free(buffers[i]);
            }
        }

        for (0..count) |i| {
            buffers[i] = try allocator.alignedAlloc(
                u8,
                @enumFromInt(std.math.log2(constants.cache_line_length)),
                buf_size,
            );
            allocated += 1;
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
    /// Only available on Linux (io_uring requires iovec for buffer registration).
    pub fn toIovecs(self: *const Self, alloc: Allocator) ![]std.os.linux.iovec {
        if (comptime builtin.os.tag != .linux) {
            @compileError("toIovecs is only available on Linux");
        }
        const iovecs = try alloc.alloc(std.os.linux.iovec, self.count);
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

// ── Tests ─────────────────────────────────────────────────────────────

test "BufferPool acquire and release" {
    // Given
    var pool = try BufferPool.init(std.testing.allocator, 4, 1408);
    defer pool.deinit();

    // When: acquire all buffers
    var slots: [4]BufferSlot = undefined;
    for (0..4) |i| {
        slots[i] = pool.acquire().?;
    }

    // Then: pool is exhausted
    try std.testing.expect(pool.acquire() == null);
    try std.testing.expectEqual(@as(u16, 0), pool.available());

    // When: release one buffer
    pool.release(slots[0]);

    // Then: one buffer available
    try std.testing.expectEqual(@as(u16, 1), pool.available());
    try std.testing.expect(pool.acquire() != null);
}

test "BufferPool buffers are cache-line aligned" {
    // Given
    var pool = try BufferPool.init(std.testing.allocator, 8, 1408);
    defer pool.deinit();

    // Then: every buffer is 64-byte aligned
    for (pool.buffers) |buf| {
        try std.testing.expectEqual(@as(usize, 0), @intFromPtr(buf.ptr) % 64);
    }
}

test "BufferPool capacity and available" {
    // Given
    var pool = try BufferPool.init(std.testing.allocator, 16, 256);
    defer pool.deinit();

    // Then
    try std.testing.expectEqual(@as(u16, 16), pool.capacity());
    try std.testing.expectEqual(@as(u16, 16), pool.available());

    // When: acquire one
    _ = pool.acquire();

    // Then
    try std.testing.expectEqual(@as(u16, 16), pool.capacity());
    try std.testing.expectEqual(@as(u16, 15), pool.available());
}

test "BufferPool acquired buffers have correct size" {
    // Given
    var pool = try BufferPool.init(std.testing.allocator, 4, 1408);
    defer pool.deinit();

    // When
    const slot = pool.acquire().?;
    defer pool.release(slot);

    // Then
    try std.testing.expectEqual(@as(usize, 1408), slot.buffer.len);
}

test "BufferPool release and re-acquire returns valid buffer" {
    // Given
    var pool = try BufferPool.init(std.testing.allocator, 2, 64);
    defer pool.deinit();

    // When: acquire, write, release, re-acquire
    const slot1 = pool.acquire().?;
    @memset(slot1.buffer, 0xAA);
    pool.release(slot1);

    const slot2 = pool.acquire().?;
    defer pool.release(slot2);

    // Then: buffer is valid (can be written to)
    try std.testing.expectEqual(@as(usize, 64), slot2.buffer.len);
    @memset(slot2.buffer, 0xBB);
}
