const std = @import("std");
const constants = @import("brz_common").platform.constants;

/// Pre-allocated pool of frame-sized, 64-byte-aligned buffers for zero-allocation
/// TCP frame building. Each buffer is acquired before constructing a frame and
/// released after the data has been written to the TCP connection.
///
/// Single-threaded — only accessed by the sender event loop.
pub const SendBufferPool = struct {
    /// All buffer slices, indexed by pool index.
    buffers: [][]align(64) u8,
    /// Stack of free buffer indices. Pop to acquire, push to release.
    free_stack: []u32,
    /// Number of free buffers (also the stack top pointer).
    free_count: u32,
    /// Total number of buffers.
    capacity: u32,
    /// Backing allocator for cleanup.
    allocator: std.mem.Allocator,

    const Self = @This();

    pub fn init(count: u32, buf_size: usize, allocator: std.mem.Allocator) !Self {
        const buffers = try allocator.alloc([]align(64) u8, count);
        errdefer allocator.free(buffers);

        const free_stack = try allocator.alloc(u32, count);
        errdefer allocator.free(free_stack);

        var allocated: u32 = 0;
        errdefer {
            for (0..allocated) |i| {
                allocator.free(buffers[i]);
            }
        }

        for (0..count) |i| {
            buffers[i] = try allocator.alignedAlloc(u8, @enumFromInt(std.math.log2(@as(usize, 64))), buf_size);
            free_stack[i] = @intCast(i);
            allocated += 1;
        }

        return .{
            .buffers = buffers,
            .free_stack = free_stack,
            .free_count = count,
            .capacity = count,
            .allocator = allocator,
        };
    }

    /// Acquire a buffer from the pool. Returns null if all buffers are in flight.
    pub fn acquire(self: *Self) ?[]align(64) u8 {
        if (self.free_count == 0) return null;
        self.free_count -= 1;
        const idx = self.free_stack[self.free_count];
        return self.buffers[idx];
    }

    /// Release a buffer back to the pool after the send completes.
    pub fn release(self: *Self, buf_ptr: [*]align(64) u8) void {
        // Find the index by pointer identity
        for (self.buffers, 0..) |buf, i| {
            if (buf.ptr == buf_ptr) {
                self.free_stack[self.free_count] = @intCast(i);
                self.free_count += 1;
                return;
            }
        }
        // Should never happen — indicates a bug in buffer tracking
        unreachable;
    }

    /// Return the number of currently available (free) buffers.
    pub fn available(self: *const Self) u32 {
        return self.free_count;
    }

    /// Free all buffers and pool metadata.
    pub fn deinit(self: *Self) void {
        for (0..self.capacity) |i| {
            self.allocator.free(self.buffers[i]);
        }
        self.allocator.free(self.buffers);
        self.allocator.free(self.free_stack);
    }
};

// ── Tests ─────────────────────────────────────────────────────────────

test "acquire and release roundtrip" {
    const allocator = std.testing.allocator;

    var pool = try SendBufferPool.init(4, constants.default_max_frame_length, allocator);
    defer pool.deinit();

    // Acquire all 4 buffers.
    var acquired: [4][]align(64) u8 = undefined;
    for (0..4) |i| {
        acquired[i] = pool.acquire() orelse return error.UnexpectedNull;
    }

    // 5th acquire should return null.
    try std.testing.expect(pool.acquire() == null);

    // Release one buffer and verify we can acquire again.
    pool.release(acquired[0].ptr);
    const reacquired = pool.acquire();
    try std.testing.expect(reacquired != null);
}

test "buffers are 64-byte aligned" {
    const allocator = std.testing.allocator;

    var pool = try SendBufferPool.init(4, constants.default_max_frame_length, allocator);
    defer pool.deinit();

    for (0..4) |_| {
        const buf = pool.acquire() orelse return error.UnexpectedNull;
        try std.testing.expect(@intFromPtr(buf.ptr) % 64 == 0);
    }
}

test "acquired buffers have correct size" {
    const allocator = std.testing.allocator;

    var pool = try SendBufferPool.init(2, constants.default_max_frame_length, allocator);
    defer pool.deinit();

    const buf = pool.acquire() orelse return error.UnexpectedNull;
    try std.testing.expectEqual(constants.default_max_frame_length, buf.len);
}

test "available tracks free count" {
    const allocator = std.testing.allocator;

    var pool = try SendBufferPool.init(4, constants.default_max_frame_length, allocator);
    defer pool.deinit();

    try std.testing.expectEqual(@as(u32, 4), pool.available());

    const buf = pool.acquire() orelse return error.UnexpectedNull;
    try std.testing.expectEqual(@as(u32, 3), pool.available());

    pool.release(buf.ptr);
    try std.testing.expectEqual(@as(u32, 4), pool.available());
}

test "release and re-acquire returns valid buffer" {
    const allocator = std.testing.allocator;

    var pool = try SendBufferPool.init(2, constants.default_max_frame_length, allocator);
    defer pool.deinit();

    // Acquire and fill with 0xAA.
    const buf1 = pool.acquire() orelse return error.UnexpectedNull;
    @memset(buf1, 0xAA);

    // Release and re-acquire.
    pool.release(buf1.ptr);
    const buf2 = pool.acquire() orelse return error.UnexpectedNull;

    // Fill with 0xBB — should not crash.
    @memset(buf2, 0xBB);

    try std.testing.expectEqual(constants.default_max_frame_length, buf2.len);
}
