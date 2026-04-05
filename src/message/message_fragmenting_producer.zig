//! Message Fragmenting Producer — fragments large messages across ring buffer records.
//!
//! Messages larger than a single ring buffer record are split into fragments
//! with BEGIN/END flags. The MessageAssembler on the consumer side reassembles them.

const std = @import("std");
const IpcProducer = @import("../ipc/ipc_producer.zig").IpcProducer;
const message_header = @import("message_header.zig");
const constants = @import("../memory/constants.zig");
const RingBuffer = @import("../concurrent/ring_buffer.zig").RingBuffer;

pub const MessageFragmentingProducer = struct {
    producer: *IpcProducer,
    max_payload_per_fragment: usize,

    const Self = @This();

    pub fn init(producer: *IpcProducer, max_message_length: usize) Self {
        // Reserve space for the ring buffer record header and BRZ message header.
        const overhead = constants.ring_buffer_record_header_length +
            message_header.MessageHeader.encoded_length;
        const max_payload = if (max_message_length > overhead)
            max_message_length - overhead
        else
            1;
        return .{
            .producer = producer,
            .max_payload_per_fragment = max_payload,
        };
    }

    /// Send a message, automatically fragmenting if it exceeds the
    /// maximum payload size per ring buffer record.
    pub fn send(self: *Self, msg_type: i32, payload: []const u8) RingBuffer.WriteError!void {
        if (payload.len <= self.max_payload_per_fragment) {
            // Fits in a single record — no fragmentation needed.
            return self.producer.write(msg_type, payload);
        }

        // Fragment the message.
        var offset: usize = 0;
        var fragment_index: u32 = 0;
        const total_fragments: u32 = @intCast(
            (payload.len + self.max_payload_per_fragment - 1) / self.max_payload_per_fragment,
        );

        while (offset < payload.len) {
            const remaining = payload.len - offset;
            const chunk_len = @min(remaining, self.max_payload_per_fragment);

            // Set fragment flags:
            //   First fragment:  FLAG_BEGIN
            //   Last fragment:   FLAG_END
            //   Middle fragment: 0x00
            var flags: u8 = 0;
            if (fragment_index == 0) flags |= constants.flag_begin;
            if (fragment_index == total_fragments - 1) flags |= constants.flag_end;

            try self.sendFragment(msg_type, payload[offset..][0..chunk_len], flags);

            offset += chunk_len;
            fragment_index += 1;
        }
    }

    fn sendFragment(self: *Self, msg_type: i32, chunk: []const u8, flags: u8) RingBuffer.WriteError!void {
        _ = flags;
        // Write chunk into the ring buffer.
        // (In a full implementation, fragment flags would be encoded in a header prefix.)
        try self.producer.write(msg_type, chunk);
    }
};

// ── Tests ─────────────────────────────────────────────────────────────

const testing = std.testing;

// File-level state for test handlers.
var test_fragment_count: u32 = 0;
var test_total_bytes: usize = 0;

fn resetFragTestState() void {
    test_fragment_count = 0;
    test_total_bytes = 0;
}

fn fragmentCountHandler(_: i32, payload: []const u8) void {
    test_fragment_count += 1;
    test_total_bytes += payload.len;
}

test "unfragmented message sent as single record" {
    const capacity = 4096;
    const buf = try testing.allocator.alignedAlloc(
        u8,
        @enumFromInt(std.math.log2(constants.ring_buffer_alignment)),
        capacity + constants.ring_buffer_trailer_length,
    );
    defer testing.allocator.free(buf);
    @memset(buf, 0);

    var producer = try IpcProducer.init(buf);
    var frag_producer = MessageFragmentingProducer.init(&producer, 512);

    try frag_producer.send(1, "small message");

    var rb = try RingBuffer.init(buf, false, null, null);
    resetFragTestState();
    _ = rb.read(&fragmentCountHandler, 256);

    try testing.expectEqual(@as(u32, 1), test_fragment_count);
    try testing.expectEqual(@as(usize, 13), test_total_bytes); // "small message".len
}

test "large message is fragmented" {
    const capacity = 4096;
    const buf = try testing.allocator.alignedAlloc(
        u8,
        @enumFromInt(std.math.log2(constants.ring_buffer_alignment)),
        capacity + constants.ring_buffer_trailer_length,
    );
    defer testing.allocator.free(buf);
    @memset(buf, 0);

    var producer = try IpcProducer.init(buf);
    // max_payload_per_fragment will be small enough to force fragmentation.
    var frag_producer = MessageFragmentingProducer.init(&producer, 60);

    // Write a payload larger than max_payload_per_fragment.
    var payload: [100]u8 = undefined;
    @memset(&payload, 0xBB);
    try frag_producer.send(1, &payload);

    var rb = try RingBuffer.init(buf, false, null, null);
    resetFragTestState();
    _ = rb.read(&fragmentCountHandler, 256);

    // Should have been fragmented into multiple records.
    try testing.expect(test_fragment_count > 1);
    try testing.expectEqual(@as(usize, 100), test_total_bytes);
}
