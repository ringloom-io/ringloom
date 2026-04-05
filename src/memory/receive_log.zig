//! Receive log buffer for the BRZ broker.
//!
//! One receive log buffer exists per connected peer broker. Unlike the
//! metadata files, these are typically allocated as anonymous memory
//! (not file-backed) since they are only accessed by the local broker process.

const std = @import("std");
const constants = @import("constants.zig");
const platform = @import("../platform.zig");

/// Overlay for the 256-byte metadata region at the end of the log buffer.
pub const ReceiveLogMetadata = extern struct {
    tail_position: i64 = 0,
    _tail_pad: [120]u8 = [_]u8{0} ** 120,
    rebuild_position: i64 = 0,
    _rebuild_pad: [120]u8 = [_]u8{0} ** 120,

    comptime {
        std.debug.assert(@sizeOf(ReceiveLogMetadata) == 256);
    }
};

/// Frame alignment within the log buffer. Frames are padded to 32-byte
/// boundaries so that headers always start at aligned addresses.
pub const frame_alignment: usize = 32;

pub const ReceiveLogBuffer = struct {
    /// The full backing memory: data region + metadata.
    backing: []align(constants.page_size) u8,

    /// The data region (first `capacity` bytes).
    data: []u8,

    /// Pointer to the metadata overlay at the end.
    metadata: *ReceiveLogMetadata,

    /// Capacity of the data region (power of 2). Used for index masking.
    capacity: usize,

    /// Bitmask: capacity - 1. Used as `position & mask` to get buffer index.
    mask: usize,

    const Self = @This();

    // ── Construction ──────────────────────────────────────────────────

    /// Allocate a new receive log buffer with the given capacity.
    /// `capacity` must be a power of two and >= page_size (4096).
    pub fn allocate(capacity: usize) !ReceiveLogBuffer {
        if (!constants.isPowerOfTwo(capacity))
            return error.CapacityNotPowerOfTwo;
        if (capacity < constants.page_size)
            return error.CapacityTooSmall;

        const total = capacity + constants.recv_log_metadata_length;

        // Use anonymous mmap for aligned, zeroed memory.
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

// ── Tests ─────────────────────────────────────────────────────────────

const testing = std.testing;

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
