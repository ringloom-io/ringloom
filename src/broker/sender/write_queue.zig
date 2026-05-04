//! Per-peer bounded write queue for TCP outbound frames.
//!
//! Each peer has its own write queue that decouples message routing from TCP writes.
//! This prevents one slow peer from blocking writes to other peers. The queue is a
//! fixed-size ring buffer of serialized frame data (header + payload bytes).
//!
//! Overflow strategy: drop the oldest enqueued message to make room for new ones.
//! This prioritizes recency — newer messages are more likely to be relevant.

const std = @import("std");

pub const WriteQueue = struct {
    /// Backing storage for frame data. Each slot holds a serialized frame
    /// (24-byte header + payload). Slot size is fixed at max_frame_size.
    slots: []u8,

    /// Array of frame lengths for each slot. 0 = empty.
    lengths: []u32,

    /// Ring buffer indices.
    head: u32,
    tail: u32,
    count: u32,
    capacity: u32,
    mask: u32,

    /// Maximum frame size (header + payload) per slot.
    max_frame_size: u32,

    const Self = @This();

    pub fn init(capacity: u32, max_frame_size: u32, allocator: std.mem.Allocator) !Self {
        std.debug.assert(std.math.isPowerOfTwo(capacity));

        const total_size = @as(usize, capacity) * @as(usize, max_frame_size);
        const slots = try allocator.alloc(u8, total_size);
        @memset(slots, 0);

        const lengths = try allocator.alloc(u32, capacity);
        @memset(lengths, 0);

        return .{
            .slots = slots,
            .lengths = lengths,
            .head = 0,
            .tail = 0,
            .count = 0,
            .capacity = capacity,
            .mask = capacity - 1,
            .max_frame_size = max_frame_size,
        };
    }

    /// Enqueue a complete frame (header + payload) into the write queue.
    pub fn enqueue(self: *Self, frame_data: []const u8) error{Overflow}!void {
        if (self.count >= self.capacity) return error.Overflow;
        if (frame_data.len > self.max_frame_size) return error.Overflow;

        const slot_index = self.tail & self.mask;
        const offset = @as(usize, slot_index) * @as(usize, self.max_frame_size);

        @memcpy(self.slots[offset..][0..frame_data.len], frame_data);
        self.lengths[slot_index] = @intCast(frame_data.len);

        self.tail +%= 1;
        self.count += 1;
    }

    /// Drop the oldest enqueued message to make room.
    pub fn dropOldest(self: *Self) u32 {
        if (self.count == 0) return 0;

        const slot_index = self.head & self.mask;
        const dropped_len = self.lengths[slot_index];
        self.lengths[slot_index] = 0;

        self.head +%= 1;
        self.count -= 1;

        return dropped_len;
    }

    /// Peek at the next frame to send without removing it.
    pub fn peek(self: *const Self) ?[]const u8 {
        if (self.count == 0) return null;

        const slot_index = self.head & self.mask;
        const len = self.lengths[slot_index];
        const offset = @as(usize, slot_index) * @as(usize, self.max_frame_size);

        return self.slots[offset..][0..len];
    }

    /// Dequeue the oldest frame after it has been successfully written.
    pub fn dequeue(self: *Self) void {
        if (self.count == 0) return;

        const slot_index = self.head & self.mask;
        self.lengths[slot_index] = 0;

        self.head +%= 1;
        self.count -= 1;
    }

    /// Peek at the size of the frame at `offset` slots from the head.
    /// Returns 0 if offset is beyond the queue.
    pub fn peekFrameSize(self: *const Self, offset: u32) u32 {
        if (offset >= self.count) return 0;
        const slot_index = (self.head +% offset) & self.mask;
        return self.lengths[slot_index];
    }

    /// Return the number of contiguous frames available for vectored write.
    pub fn contiguousCount(self: *const Self, limit: u32) u32 {
        return self.contiguousCountFrom(0, limit);
    }

    /// Return the number of contiguous frames starting `skip` frames from head.
    /// Used by io_uring path to skip in-flight frames.
    pub fn contiguousCountFrom(self: *const Self, skip: u32, limit: u32) u32 {
        if (self.count <= skip) return 0;
        const available = self.count - skip;

        const start = (self.head +% skip) & self.mask;
        const until_wrap = self.capacity - start;
        return @min(@min(available, until_wrap), limit);
    }

    /// Fill an iovec array with pointers to contiguous queued frames.
    /// The first iovec is adjusted by `first_offset` to support partial-write resumption.
    /// Returns the number of iovecs filled.
    pub fn fillIovecs(
        self: *const Self,
        iovecs: []std.posix.iovec_const,
        first_offset: usize,
        limit: u32,
    ) u32 {
        return self.fillIovecsFrom(iovecs, 0, first_offset, limit);
    }

    /// Fill iovecs starting `skip` frames from head (to skip in-flight frames).
    pub fn fillIovecsFrom(
        self: *const Self,
        iovecs: []std.posix.iovec_const,
        skip: u32,
        first_offset: usize,
        limit: u32,
    ) u32 {
        const n = self.contiguousCountFrom(skip, @min(limit, @as(u32, @intCast(iovecs.len))));
        if (n == 0) return 0;

        for (0..n) |i| {
            const slot_index = (self.head +% skip +% @as(u32, @intCast(i))) & self.mask;
            const len = self.lengths[slot_index];
            const offset = @as(usize, slot_index) * @as(usize, self.max_frame_size);

            if (i == 0 and first_offset > 0) {
                iovecs[i] = .{
                    .base = self.slots[offset + first_offset ..].ptr,
                    .len = len - first_offset,
                };
            } else {
                iovecs[i] = .{
                    .base = self.slots[offset..].ptr,
                    .len = len,
                };
            }
        }

        return n;
    }

    pub fn isEmpty(self: *const Self) bool {
        return self.count == 0;
    }

    pub fn isFull(self: *const Self) bool {
        return self.count >= self.capacity;
    }

    pub fn byteSize(self: *const Self) u64 {
        var total: u64 = 0;
        var i: u32 = 0;
        while (i < self.count) : (i += 1) {
            const slot_index = (self.head +% i) & self.mask;
            total += self.lengths[slot_index];
        }
        return total;
    }

    /// Clear all entries without freeing memory.
    pub fn clear(self: *Self) void {
        self.head = 0;
        self.tail = 0;
        self.count = 0;
        @memset(self.lengths, 0);
    }

    pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
        allocator.free(self.slots);
        allocator.free(self.lengths);
    }
};

// ── Tests ─────────────────────────────────────────────────────────────

const testing = std.testing;

test "WriteQueue enqueue and peek" {
    const allocator = testing.allocator;
    var q = try WriteQueue.init(4, 64, allocator);
    defer q.deinit(allocator);

    const frame = [_]u8{1} ** 24;
    try q.enqueue(&frame);

    try testing.expectEqual(@as(u32, 1), q.count);
    const peeked = q.peek().?;
    try testing.expectEqual(@as(usize, 24), peeked.len);
    try testing.expectEqual(@as(u8, 1), peeked[0]);
}

test "WriteQueue dequeue advances head" {
    const allocator = testing.allocator;
    var q = try WriteQueue.init(4, 64, allocator);
    defer q.deinit(allocator);

    const frame1 = [_]u8{1} ** 24;
    const frame2 = [_]u8{2} ** 24;
    try q.enqueue(&frame1);
    try q.enqueue(&frame2);

    q.dequeue();
    try testing.expectEqual(@as(u32, 1), q.count);
    const peeked = q.peek().?;
    try testing.expectEqual(@as(u8, 2), peeked[0]);
}

test "WriteQueue overflow returns error" {
    const allocator = testing.allocator;
    var q = try WriteQueue.init(2, 64, allocator);
    defer q.deinit(allocator);

    const frame = [_]u8{0} ** 24;
    try q.enqueue(&frame);
    try q.enqueue(&frame);

    try testing.expectError(error.Overflow, q.enqueue(&frame));
}

test "WriteQueue dropOldest frees a slot" {
    const allocator = testing.allocator;
    var q = try WriteQueue.init(2, 64, allocator);
    defer q.deinit(allocator);

    const frame1 = [_]u8{1} ** 24;
    const frame2 = [_]u8{2} ** 24;
    const frame3 = [_]u8{3} ** 24;
    try q.enqueue(&frame1);
    try q.enqueue(&frame2);

    _ = q.dropOldest();
    try q.enqueue(&frame3);

    const peeked = q.peek().?;
    try testing.expectEqual(@as(u8, 2), peeked[0]);
}

test "WriteQueue contiguousCount" {
    const allocator = testing.allocator;
    var q = try WriteQueue.init(4, 64, allocator);
    defer q.deinit(allocator);

    const frame = [_]u8{0} ** 24;
    try q.enqueue(&frame);
    try q.enqueue(&frame);
    try q.enqueue(&frame);

    try testing.expectEqual(@as(u32, 3), q.contiguousCount(16));
    try testing.expectEqual(@as(u32, 2), q.contiguousCount(2));
}

test "WriteQueue fillIovecs returns correct iov entries" {
    const allocator = testing.allocator;
    var q = try WriteQueue.init(4, 64, allocator);
    defer q.deinit(allocator);

    const frame1 = [_]u8{0xAA} ** 24;
    const frame2 = [_]u8{0xBB} ** 32;
    try q.enqueue(&frame1);
    try q.enqueue(&frame2);

    var iovecs: [4]std.posix.iovec_const = undefined;
    const n = q.fillIovecs(&iovecs, 0, 4);
    try testing.expectEqual(@as(u32, 2), n);
    try testing.expectEqual(@as(usize, 24), iovecs[0].len);
    try testing.expectEqual(@as(usize, 32), iovecs[1].len);
    try testing.expectEqual(@as(u8, 0xAA), iovecs[0].base[0]);
    try testing.expectEqual(@as(u8, 0xBB), iovecs[1].base[0]);
}

test "WriteQueue fillIovecs respects first_offset" {
    const allocator = testing.allocator;
    var q = try WriteQueue.init(4, 64, allocator);
    defer q.deinit(allocator);

    const frame = [_]u8{0xCC} ** 48;
    try q.enqueue(&frame);

    var iovecs: [4]std.posix.iovec_const = undefined;
    const n = q.fillIovecs(&iovecs, 10, 4);
    try testing.expectEqual(@as(u32, 1), n);
    try testing.expectEqual(@as(usize, 38), iovecs[0].len);
}

test "WriteQueue isEmpty and isFull" {
    const allocator = testing.allocator;
    var q = try WriteQueue.init(2, 64, allocator);
    defer q.deinit(allocator);

    try testing.expect(q.isEmpty());
    try testing.expect(!q.isFull());

    const frame = [_]u8{0} ** 24;
    try q.enqueue(&frame);
    try q.enqueue(&frame);

    try testing.expect(!q.isEmpty());
    try testing.expect(q.isFull());
}

test "WriteQueue clear resets state" {
    const allocator = testing.allocator;
    var q = try WriteQueue.init(4, 64, allocator);
    defer q.deinit(allocator);

    const frame = [_]u8{0} ** 24;
    try q.enqueue(&frame);
    try q.enqueue(&frame);

    q.clear();

    try testing.expect(q.isEmpty());
    try testing.expectEqual(@as(u32, 0), q.count);
}
