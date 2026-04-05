//! Receive log buffer packet insertion for the BRZ broker receive path.
//!
//! Extends the ReceiveLogBuffer (defined in memory/receive_log.zig) with
//! position-based packet insertion where the frame's sequence_number
//! determines the buffer position. This decouples insertion from the
//! sequential tail-append model used by the base struct.

const std = @import("std");
const constants = @import("../platform/constants.zig");
const frames = @import("../protocol/frames.zig");
const ReceiveLogBuffer = @import("../memory/receive_log.zig").ReceiveLogBuffer;

pub const frame_alignment: usize = 32;

/// Insert a received data frame into the log buffer at the position
/// determined by its sequence_number.
///
/// The frame is stored as: [i32 length prefix][frame bytes][padding to 32B].
///
/// The length prefix is written LAST with release semantics — this is the
/// commit pattern. Readers (loss detector, router) spin on the length field
/// and only see the frame once the length becomes positive.
///
/// Insertion is idempotent: if the slot already contains a frame (length > 0),
/// the write is skipped. This handles retransmitted packets gracefully.
pub fn insertPacket(log: *ReceiveLogBuffer, frame: []const u8) void {
    if (frame.len < @sizeOf(frames.DataFrameHeader)) return;

    const header: *const frames.DataFrameHeader = @ptrCast(@alignCast(frame.ptr));
    const sequence = header.sequence_number;
    if (sequence < 0) return; // invalid sequence

    // Calculate the buffer position for this sequence number.
    // In BRZ's model, sequence_number IS the byte position in the logical
    // stream — the sender assigns it as a monotonic byte offset.
    const position: i64 = sequence;
    const index: usize = @intCast(@as(u64, @bitCast(position)) & log.mask);

    // ── Idempotency check ─────────────────────────────────────────────
    // If the slot already has a committed frame (length > 0) or has been
    // consumed (length == -1), skip the write. This handles retransmits
    // and duplicate packets.
    const length_ptr: *volatile i32 = @ptrCast(@alignCast(&log.data[index]));
    const existing = @atomicLoad(i32, length_ptr, .acquire);
    if (existing != 0) return;

    // ── Write frame data FIRST ────────────────────────────────────────
    // Copy the entire frame (header + payload) into the slot, starting
    // after the 4-byte length prefix.
    const frame_length: i32 = @intCast(frame.len);
    const data_start = index + @sizeOf(i32);
    @memcpy(log.data[data_start..][0..frame.len], frame);

    // ── Write aligned-length field ────────────────────────────────────
    // Store the 32-byte-aligned total slot size in a secondary field
    // at a fixed offset within the slot. This is needed by the
    // consumption position scanner to advance past consumed frames even
    // after the primary length is overwritten with the consumed marker (-1).
    const total_slot_size: i32 = @intCast(constants.alignUp(
        frame.len + @sizeOf(i32),
        frame_alignment,
    ));
    const aligned_len_offset = index + @sizeOf(i32) + @sizeOf(i32); // after length prefix + frame_length field of DataFrameHeader
    const aligned_len_ptr: *volatile i32 = @ptrCast(@alignCast(&log.data[aligned_len_offset]));
    @atomicStore(i32, aligned_len_ptr, total_slot_size, .monotonic);

    // ── Commit: write length prefix LAST with release ─────────────────
    // This is the publication barrier. Once the length becomes positive,
    // readers can see the complete frame data.
    @atomicStore(i32, length_ptr, frame_length, .release);

    // ── Advance high-water mark (tail_position) ───────────────────────
    // Single writer (receiver thread), so a simple max is sufficient.
    const new_tail: i64 = position + @as(i64, @intCast(total_slot_size));
    const current_tail = log.loadTailPosition();
    if (new_tail > current_tail) {
        log.storeTailPosition(new_tail);
    }
}

/// Read a frame at the given absolute position.
/// Returns the frame data slice (excluding the 4-byte length prefix),
/// or null if the frame is not yet committed (length <= 0).
pub fn readFrame(log: *const ReceiveLogBuffer, position: i64) ?[]const u8 {
    const index: usize = @intCast(@as(u64, @bitCast(position)) & log.mask);
    const length_ptr: *const volatile i32 = @ptrCast(@alignCast(&log.data[index]));
    const length = @atomicLoad(i32, length_ptr, .acquire);

    if (length <= 0) return null;

    const data_start = index + @sizeOf(i32);
    return log.data[data_start..][0..@as(usize, @intCast(length))];
}

// ── Tests ─────────────────────────────────────────────────────────────

const testing = std.testing;

test "insertPacket writes frame and advances tail" {
    // Given
    var log = try ReceiveLogBuffer.allocate(4096);
    defer log.close();

    var frame_buf: [128]u8 align(8) = [_]u8{0} ** 128;
    const header: *frames.DataFrameHeader = @ptrCast(@alignCast(&frame_buf));
    header.* = .{
        .frame_length = 80, // 40-byte header + 40-byte payload
        .flags = constants.flag_unfragmented,
        .sequence_number = 0,
        .source_node_id = 1,
        .target_service_id = 5,
    };

    // When
    insertPacket(&log, frame_buf[0..80]);

    // Then
    const tail = log.loadTailPosition();
    try testing.expect(tail > 0);
    // Tail should be aligned to 32 bytes: 4 (length prefix) + 80 (frame) = 84 → aligned to 96
    try testing.expectEqual(@as(i64, 96), tail);

    // Verify the frame is readable
    const read_frame = readFrame(&log, 0);
    try testing.expect(read_frame != null);
    try testing.expectEqual(@as(usize, 80), read_frame.?.len);
}

test "insertPacket is idempotent — retransmit is skipped" {
    // Given
    var log = try ReceiveLogBuffer.allocate(4096);
    defer log.close();

    var frame_buf: [128]u8 align(8) = [_]u8{0} ** 128;
    const header: *frames.DataFrameHeader = @ptrCast(@alignCast(&frame_buf));
    header.* = .{
        .frame_length = 80,
        .flags = constants.flag_unfragmented,
        .sequence_number = 0,
    };

    // When — insert twice at the same position
    insertPacket(&log, frame_buf[0..80]);
    const tail_after_first = log.loadTailPosition();

    insertPacket(&log, frame_buf[0..80]);
    const tail_after_second = log.loadTailPosition();

    // Then — tail should not advance on retransmit
    try testing.expectEqual(tail_after_first, tail_after_second);
}

test "insertPacket handles out-of-order packets" {
    // Given
    var log = try ReceiveLogBuffer.allocate(4096);
    defer log.close();

    // Insert packet at sequence 96 first (out of order)
    var frame2_buf: [128]u8 align(8) = [_]u8{0} ** 128;
    const header2: *frames.DataFrameHeader = @ptrCast(@alignCast(&frame2_buf));
    header2.* = .{
        .frame_length = 80,
        .flags = constants.flag_unfragmented,
        .sequence_number = 96,
    };
    insertPacket(&log, frame2_buf[0..80]);

    // Then — tail should jump to position after packet 2
    const tail_after_second = log.loadTailPosition();
    try testing.expectEqual(@as(i64, 192), tail_after_second);

    // Insert packet at sequence 0 (filling the gap)
    var frame1_buf: [128]u8 align(8) = [_]u8{0} ** 128;
    const header1: *frames.DataFrameHeader = @ptrCast(@alignCast(&frame1_buf));
    header1.* = .{
        .frame_length = 80,
        .flags = constants.flag_unfragmented,
        .sequence_number = 0,
    };
    insertPacket(&log, frame1_buf[0..80]);

    // Then — tail should NOT decrease
    const tail_after_first = log.loadTailPosition();
    try testing.expectEqual(@as(i64, 192), tail_after_first);

    // Both frames should be readable
    try testing.expect(readFrame(&log, 0) != null);
    try testing.expect(readFrame(&log, 96) != null);
}

test "insertPacket rejects frames smaller than DataFrameHeader" {
    // Given
    var log = try ReceiveLogBuffer.allocate(4096);
    defer log.close();

    var small_buf: [20]u8 = [_]u8{0} ** 20;

    // When — insert a too-small frame
    insertPacket(&log, &small_buf);

    // Then — tail should remain at 0
    try testing.expectEqual(@as(i64, 0), log.loadTailPosition());
}

test "insertPacket rejects negative sequence numbers" {
    // Given
    var log = try ReceiveLogBuffer.allocate(4096);
    defer log.close();

    var frame_buf: [128]u8 align(8) = [_]u8{0} ** 128;
    const header: *frames.DataFrameHeader = @ptrCast(@alignCast(&frame_buf));
    header.* = .{
        .frame_length = 80,
        .flags = constants.flag_unfragmented,
        .sequence_number = -1,
    };

    // When
    insertPacket(&log, frame_buf[0..80]);

    // Then — tail should remain at 0
    try testing.expectEqual(@as(i64, 0), log.loadTailPosition());
}
