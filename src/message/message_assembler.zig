//! Message Assembler — reassembles fragmented messages on the consumer side.
//!
//! Uses a pre-allocated growable buffer. When the first fragment (FLAG_BEGIN)
//! arrives, the assembler starts accumulating. When the last fragment (FLAG_END)
//! arrives, the complete message is dispatched to the application handler.

const std = @import("std");
const constants = @import("../memory/constants.zig");

pub const MessageAssembler = struct {
    buffer: []u8,
    buffer_len: usize,
    capacity: usize,
    in_progress: bool,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, max_message_size: usize) !Self {
        const buf = try allocator.alloc(u8, max_message_size);
        return .{
            .buffer = buf,
            .buffer_len = 0,
            .capacity = max_message_size,
            .in_progress = false,
        };
    }

    /// Process a fragment. Returns the complete message when all fragments
    /// have been received, or null if more fragments are expected.
    pub fn onFragment(self: *Self, flags: u8, data: []const u8) ?[]const u8 {
        const is_begin = (flags & constants.flag_begin) != 0;
        const is_end = (flags & constants.flag_end) != 0;

        if (is_begin and is_end) {
            // Unfragmented message — return directly.
            return data;
        }

        if (is_begin) {
            // Start of a new fragmented message.
            self.buffer_len = 0;
            self.in_progress = true;
        }

        if (!self.in_progress) return null;

        // Append this fragment's data to the assembly buffer.
        if (self.buffer_len + data.len > self.capacity) {
            // Message exceeds maximum size — discard.
            self.in_progress = false;
            self.buffer_len = 0;
            return null;
        }

        @memcpy(self.buffer[self.buffer_len..][0..data.len], data);
        self.buffer_len += data.len;

        if (is_end) {
            // All fragments received — return the complete message.
            self.in_progress = false;
            return self.buffer[0..self.buffer_len];
        }

        return null; // More fragments expected.
    }

    pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
        allocator.free(self.buffer);
    }
};

// ── Tests ─────────────────────────────────────────────────────────────

const testing = std.testing;

test "unfragmented message passes through directly" {
    // Given: an assembler.
    var assembler = try MessageAssembler.init(testing.allocator, 4096);
    defer assembler.deinit(testing.allocator);

    // When: an unfragmented message arrives (BEGIN | END).
    const flags = constants.flag_begin | constants.flag_end;
    const result = assembler.onFragment(flags, "complete message");

    // Then: it's returned immediately without copying.
    try testing.expect(result != null);
    try testing.expectEqualStrings("complete message", result.?);
}

test "fragmented message is reassembled" {
    // Given: an assembler.
    var assembler = try MessageAssembler.init(testing.allocator, 4096);
    defer assembler.deinit(testing.allocator);

    // When: three fragments arrive.
    const result1 = assembler.onFragment(constants.flag_begin, "hello ");
    try testing.expect(result1 == null); // not complete yet

    const result2 = assembler.onFragment(0, "beautiful ");
    try testing.expect(result2 == null); // not complete yet

    const result3 = assembler.onFragment(constants.flag_end, "world");
    try testing.expect(result3 != null); // complete!

    // Then: the reassembled message is correct.
    try testing.expectEqualStrings("hello beautiful world", result3.?);
}

test "oversized fragment is discarded" {
    // Given: an assembler with a tiny capacity.
    var assembler = try MessageAssembler.init(testing.allocator, 16);
    defer assembler.deinit(testing.allocator);

    // When: fragments exceed capacity.
    _ = assembler.onFragment(constants.flag_begin, "12345678901234567");

    // Then: the fragment is discarded (exceeds 16-byte capacity).
    const result = assembler.onFragment(constants.flag_end, "overflow");
    try testing.expect(result == null);
}
