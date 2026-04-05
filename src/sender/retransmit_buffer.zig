const std = @import("std");
const constants = @import("../platform/constants.zig");

/// Per-peer circular buffer that holds copies of recently sent frames for
/// retransmission on NAK. Single-threaded — only accessed by the sender
/// event loop.
pub const RetransmitBuffer = struct {
    /// Backing memory, page-aligned, power-of-two capacity.
    buffer: []align(4096) u8,

    /// Total capacity in bytes. Must be a power of two.
    capacity: usize,

    /// Size of each slot in bytes: slot_header_length + max_frame_size.
    slot_size: usize,

    /// Number of slots: capacity / slot_size.
    slot_count: usize,

    /// Mask for fast modulo: slot_count - 1. Only valid if slot_count is power of two.
    slot_mask: usize,

    const slot_header_length: usize = 16;

    const SlotHeader = extern struct {
        stored_sequence_number: i64,
        stored_frame_length: i32,
        _padding: i32 = 0,
    };

    const Self = @This();

    pub fn init(capacity: usize, max_frame_size: usize, allocator: std.mem.Allocator) !Self {
        // Validate capacity is a power of two
        if (!constants.isPowerOfTwo(capacity)) return error.CapacityNotPowerOfTwo;

        const slot_size = constants.alignUp(slot_header_length + max_frame_size, 64);
        const slot_count = capacity / slot_size;

        // Round slot_count down to the nearest power of two for fast masking.
        const effective_slot_count = blk: {
            var n = slot_count;
            n |= n >> 1;
            n |= n >> 2;
            n |= n >> 4;
            n |= n >> 8;
            n |= n >> 16;
            n |= n >> 32;
            break :blk (n >> 1) + 1;
        };

        const effective_capacity = effective_slot_count * slot_size;
        const buf = try allocator.alignedAlloc(u8, @enumFromInt(std.math.log2(@as(usize, 4096))), effective_capacity);
        @memset(buf, 0);

        return .{
            .buffer = buf,
            .capacity = effective_capacity,
            .slot_size = slot_size,
            .slot_count = effective_slot_count,
            .slot_mask = effective_slot_count - 1,
        };
    }

    /// Store a frame in the retransmit buffer at the slot determined by the
    /// sequence number. Silently overwrites any existing frame in that slot.
    pub fn store(self: *Self, seq: i64, frame: []const u8) void {
        const slot_index = @as(usize, @intCast(seq)) & self.slot_mask;
        const offset = slot_index * self.slot_size;

        // Write slot header
        const slot_header: *SlotHeader = @ptrCast(@alignCast(self.buffer[offset..].ptr));
        slot_header.* = .{
            .stored_sequence_number = seq,
            .stored_frame_length = @intCast(frame.len),
        };

        // Copy frame data after header
        const data_offset = offset + slot_header_length;
        @memcpy(self.buffer[data_offset..][0..frame.len], frame);
    }

    /// Look up a frame by sequence number. Returns the frame bytes if the slot
    /// still holds the requested sequence, or null if it has been overwritten.
    pub fn lookup(self: *const Self, seq: i64) ?[]const u8 {
        const slot_index = @as(usize, @intCast(seq)) & self.slot_mask;
        const offset = slot_index * self.slot_size;

        const slot_header: *const SlotHeader = @ptrCast(@alignCast(self.buffer[offset..].ptr));

        // Validate: has this slot been overwritten by a newer frame?
        if (slot_header.stored_sequence_number != seq) return null;

        const frame_len = @as(usize, @intCast(slot_header.stored_frame_length));
        if (frame_len == 0) return null;

        const data_offset = offset + slot_header_length;
        return self.buffer[data_offset..][0..frame_len];
    }

    /// Returns true if the given sequence number is still available for retransmit.
    pub fn isAvailable(self: *const Self, seq: i64) bool {
        return self.lookup(seq) != null;
    }

    pub fn close(self: *Self, allocator: std.mem.Allocator) void {
        allocator.free(self.buffer);
        self.buffer = &.{};
    }
};

// ── Tests ─────────────────────────────────────────────────────────────

test "store and lookup frame in retransmit buffer" {
    const allocator = std.testing.allocator;

    var rb = try RetransmitBuffer.init(64 * 1024, constants.default_mtu_length, allocator);
    defer rb.close(allocator);

    const frame = "hello retransmit";
    rb.store(42, frame);

    const result = rb.lookup(42);
    try std.testing.expect(result != null);
    try std.testing.expectEqualSlices(u8, frame, result.?);
}

test "lookup returns null for overwritten slot" {
    const allocator = std.testing.allocator;

    var rb = try RetransmitBuffer.init(64 * 1024, constants.default_mtu_length, allocator);
    defer rb.close(allocator);

    const frame_a = "original frame";
    const frame_b = "replacement frame";

    // Store at sequence 0.
    rb.store(0, frame_a);
    try std.testing.expect(rb.lookup(0) != null);

    // Store at sequence = slot_count, which wraps to the same slot.
    const wrap_seq: i64 = @intCast(rb.slot_count);
    rb.store(wrap_seq, frame_b);

    // The original sequence should now be gone.
    try std.testing.expect(rb.lookup(0) == null);

    // The new sequence should be present.
    const result = rb.lookup(wrap_seq);
    try std.testing.expect(result != null);
    try std.testing.expectEqualSlices(u8, frame_b, result.?);
}

test "lookup returns null for never-stored sequence" {
    const allocator = std.testing.allocator;

    var rb = try RetransmitBuffer.init(64 * 1024, constants.default_mtu_length, allocator);
    defer rb.close(allocator);

    try std.testing.expect(rb.lookup(999) == null);
}

test "init rejects non-power-of-two capacity" {
    const allocator = std.testing.allocator;

    const result = RetransmitBuffer.init(1000, constants.default_mtu_length, allocator);
    try std.testing.expectError(error.CapacityNotPowerOfTwo, result);
}

test "slot header is exactly 16 bytes" {
    comptime {
        std.debug.assert(@sizeOf(RetransmitBuffer.SlotHeader) == 16);
    }
}
