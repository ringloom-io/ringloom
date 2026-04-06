//! Unit tests for IpcProducer and IpcConsumer.

const std = @import("std");
const testing = std.testing;
const brz_common = @import("brz_common");
const IpcProducer = @import("ipc_producer.zig").IpcProducer;
const IpcConsumer = @import("ipc_consumer.zig").IpcConsumer;
const RingBuffer = brz_common.concurrent.ring_buffer.RingBuffer;
const constants = brz_common.memory.constants;

const rb_alignment: ?std.mem.Alignment = @enumFromInt(std.math.log2(constants.ring_buffer_alignment));

// File-level state for handler callbacks (Zig fn ptrs can't capture mutable locals).
var test_received_type: i32 = 0;
var test_received_payload_buf: [256]u8 = undefined;
var test_received_payload_len: usize = 0;

fn resetTestState() void {
    test_received_type = 0;
    test_received_payload_len = 0;
    test_received_payload_buf = undefined;
}

fn testCaptureHandler(msg_type: i32, buffer: []const u8) void {
    test_received_type = msg_type;
    test_received_payload_len = buffer.len;
    if (buffer.len <= test_received_payload_buf.len) {
        @memcpy(test_received_payload_buf[0..buffer.len], buffer);
    }
}

test "IpcProducer write and IpcConsumer poll roundtrip" {
    // Given: a shared buffer simulating a service's messages ring buffer.
    const capacity = 4096;
    const buf = try testing.allocator.alignedAlloc(
        u8,
        rb_alignment,
        capacity + constants.ring_buffer_trailer_length,
    );
    defer testing.allocator.free(buf);
    @memset(buf, 0);

    var producer = try IpcProducer.init(buf);
    var consumer = try IpcConsumer.init(buf);

    // When: a message is written via the producer.
    const payload = "hello from service-a";
    try producer.write(42, payload);

    // Then: the consumer reads it back with the correct type and content.
    resetTestState();
    const count = consumer.poll(&testCaptureHandler, 10);

    try testing.expectEqual(@as(u32, 1), count);
    try testing.expectEqual(@as(i32, 42), test_received_type);
    try testing.expectEqualStrings(payload, test_received_payload_buf[0..test_received_payload_len]);
}

test "IpcProducer write returns error when buffer is full" {
    // Given: a minimal ring buffer.
    const capacity = 256;
    const buf = try testing.allocator.alignedAlloc(
        u8,
        rb_alignment,
        capacity + constants.ring_buffer_trailer_length,
    );
    defer testing.allocator.free(buf);
    @memset(buf, 0);

    var producer = try IpcProducer.init(buf);

    // When: we write a large payload.
    // max_msg_length = 256 / 8 = 32 bytes, so 200 bytes is way too large.
    var large_payload: [200]u8 = undefined;
    @memset(&large_payload, 0xAA);

    // Then: write should fail — message too long.
    const result = producer.write(1, &large_payload);
    try testing.expectError(RingBuffer.WriteError.MessageTooLong, result);
}

test "IpcProducer tryClaim and commit roundtrip" {
    // Given: a shared buffer.
    const capacity = 4096;
    const buf = try testing.allocator.alignedAlloc(
        u8,
        rb_alignment,
        capacity + constants.ring_buffer_trailer_length,
    );
    defer testing.allocator.free(buf);
    @memset(buf, 0);

    var producer = try IpcProducer.init(buf);
    var consumer = try IpcConsumer.init(buf);

    // When: claim, write, and commit.
    const payload = "zero-copy message";
    var claim = producer.tryClaim(7, payload.len) orelse return error.TestUnexpectedResult;
    @memcpy(claim.buffer[0..payload.len], payload);
    claim.commit();

    // Then: the consumer reads it.
    resetTestState();
    const count = consumer.poll(&testCaptureHandler, 10);

    try testing.expectEqual(@as(u32, 1), count);
    try testing.expectEqual(@as(i32, 7), test_received_type);
    try testing.expectEqualStrings(payload, test_received_payload_buf[0..test_received_payload_len]);
}

test "IpcProducer remainingCapacity decreases after write" {
    // Given: a ring buffer.
    const capacity = 4096;
    const buf = try testing.allocator.alignedAlloc(
        u8,
        rb_alignment,
        capacity + constants.ring_buffer_trailer_length,
    );
    defer testing.allocator.free(buf);
    @memset(buf, 0);

    var producer = try IpcProducer.init(buf);

    const initial = producer.remainingCapacity();
    try testing.expect(initial > 0);

    // When: write a message.
    try producer.write(1, "test message");

    // Then: remaining capacity decreases.
    const after = producer.remainingCapacity();
    try testing.expect(after < initial);
}
