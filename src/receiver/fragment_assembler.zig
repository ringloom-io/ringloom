//! Fragment assembler for the BRZ broker receive path.
//!
//! Reassembles fragmented messages from multiple DATA frames. One assembler
//! exists per unique source (keyed by source_node_id << 16 | source_service_id).
//! A single source can only have one fragmented message in flight at a time.

const std = @import("std");
const constants = @import("../platform/constants.zig");
const frames = @import("../protocol/frames.zig");

/// Maximum reassembled message size. Messages larger than this are dropped.
/// This is a safety limit — in practice, messages rarely exceed a few hundred KB.
const MAX_REASSEMBLED_MESSAGE_SIZE: usize = 16 * 1024 * 1024; // 16 MiB

/// Assembles a fragmented message from multiple DATA frames.
///
/// The assembler is stateful: it accumulates fragment payloads in a pre-allocated
/// buffer and emits the complete message when the END fragment arrives. If
/// fragments arrive out of order or a gap is detected, the in-progress message
/// is discarded and reassembly restarts on the next BEGIN fragment.
pub const FragmentAssembler = struct {
    /// Growable assembly buffer. Allocated once at startup, grows as needed.
    /// This is the one place where the hot path MAY allocate (if a message
    /// exceeds the initial buffer capacity). In practice, the buffer is sized
    /// to handle the expected maximum message size, so allocations are rare.
    buffer: []u8,

    /// Current write position within the buffer.
    buffer_len: usize,

    /// Total buffer capacity.
    buffer_capacity: usize,

    /// The expected sequence_number of the next fragment.
    expected_sequence: i64,

    /// The correlation_id of the message currently being assembled.
    /// Used to detect interleaved messages from the same source (which
    /// would indicate a protocol error).
    active_correlation_id: i32,

    /// True if we are actively assembling a message (received BEGIN,
    /// waiting for more fragments or END).
    assembling: bool,

    /// Captured routing fields from the BEGIN fragment's header.
    /// These are used when routing the reassembled message.
    source_node_id: u8,
    source_service_id: u16,
    target_service_id: u16,
    template_id: u16,

    /// Key for this assembler: (source_node_id << 16) | source_service_id.
    key: u32,

    const Self = @This();

    /// Initialize a fragment assembler with pre-allocated buffer.
    pub fn init(initial_capacity: usize, key: u32) Self {
        // Pre-allocate using page_allocator — this is startup, not hot path.
        const buf = std.heap.page_allocator.alloc(u8, initial_capacity) catch
            @panic("failed to allocate fragment assembler buffer");

        return .{
            .buffer = buf,
            .buffer_len = 0,
            .buffer_capacity = initial_capacity,
            .expected_sequence = 0,
            .active_correlation_id = 0,
            .assembling = false,
            .source_node_id = 0,
            .source_service_id = 0,
            .target_service_id = 0,
            .template_id = 0,
            .key = key,
        };
    }

    /// Process a fragment. Returns the complete reassembled message payload
    /// if this was the final fragment, or null if more fragments are expected.
    ///
    /// If the fragment is out of order or belongs to a different message than
    /// the one being assembled, the in-progress assembly is discarded.
    pub fn onFragment(
        self: *Self,
        header: *const frames.DataFrameHeader,
        frame: []const u8,
    ) ?[]const u8 {
        const payload = frames.DataFrameHeader.payloadSlice(frame);

        // ── BEGIN fragment ────────────────────────────────────────────
        if (header.isBegin()) {
            // Start a new assembly, discarding any in-progress message.
            self.buffer_len = 0;
            self.assembling = true;
            self.active_correlation_id = header.correlation_id;
            self.expected_sequence = header.sequence_number;
            self.source_node_id = header.source_node_id;
            self.source_service_id = header.source_service_id;
            self.target_service_id = header.target_service_id;
            self.template_id = header.template_id;

            if (!self.appendPayload(payload)) {
                self.reset();
                return null;
            }

            // Advance expected sequence by the aligned frame size.
            self.advanceExpectedSequence(header.frame_length);

            // BEGIN+END = unfragmented message that shouldn't reach here,
            // but handle it gracefully.
            if (header.isEnd()) {
                self.assembling = false;
                return self.buffer[0..self.buffer_len];
            }

            return null;
        }

        // ── Not a BEGIN fragment — must be in active assembly ─────────
        if (!self.assembling) {
            // Received a middle or end fragment without a preceding BEGIN.
            // This can happen after packet loss. Discard.
            return null;
        }

        // ── Sequence check ────────────────────────────────────────────
        if (header.sequence_number != self.expected_sequence) {
            // Out-of-order or gap. The loss detector will send a NAK and
            // the sender will retransmit. Meanwhile, discard the partial
            // assembly — we'll start fresh from the next BEGIN.
            self.reset();
            return null;
        }

        // ── Correlation ID check ──────────────────────────────────────
        if (header.correlation_id != self.active_correlation_id) {
            // Interleaved message from the same source. Protocol violation.
            self.reset();
            return null;
        }

        // ── Append payload ────────────────────────────────────────────
        if (!self.appendPayload(payload)) {
            self.reset();
            return null;
        }

        self.advanceExpectedSequence(header.frame_length);

        // ── END fragment ──────────────────────────────────────────────
        if (header.isEnd()) {
            self.assembling = false;
            return self.buffer[0..self.buffer_len];
        }

        // More fragments expected.
        return null;
    }

    /// Append payload bytes to the assembly buffer. Returns false if the
    /// message exceeds the maximum size limit.
    fn appendPayload(self: *Self, payload: []const u8) bool {
        const new_len = self.buffer_len + payload.len;

        if (new_len > MAX_REASSEMBLED_MESSAGE_SIZE) return false;

        // Grow buffer if necessary (rare — only on first large message).
        if (new_len > self.buffer_capacity) {
            const new_capacity = std.math.ceilPowerOfTwo(usize, new_len) catch
                return false;
            const new_buf = std.heap.page_allocator.realloc(
                self.buffer,
                new_capacity,
            ) catch return false;
            self.buffer = new_buf;
            self.buffer_capacity = new_capacity;
        }

        @memcpy(self.buffer[self.buffer_len..][0..payload.len], payload);
        self.buffer_len = new_len;
        return true;
    }

    /// Calculate the next expected sequence number by advancing past the
    /// current frame's aligned slot in the send buffer.
    fn advanceExpectedSequence(self: *Self, frame_length: i32) void {
        const aligned = constants.alignUp(
            @as(usize, @intCast(frame_length)) + @sizeOf(i32), // frame + length prefix
            32, // frame alignment in the receive log
        );
        self.expected_sequence += @as(i64, @intCast(aligned));
    }

    /// Discard in-progress assembly.
    pub fn reset(self: *Self) void {
        self.buffer_len = 0;
        self.assembling = false;
        self.active_correlation_id = 0;
        self.expected_sequence = 0;
    }

    /// Release the assembly buffer.
    pub fn deinit(self: *Self) void {
        std.heap.page_allocator.free(self.buffer);
        self.* = undefined;
    }
};

// ── Tests ─────────────────────────────────────────────────────────────

const testing = std.testing;

test "reassemble 3-fragment message" {
    // Given
    var assembler = FragmentAssembler.init(4096, 0x00010002);
    defer assembler.deinit();

    // Fragment 1: BEGIN
    var frag1_buf: [80]u8 align(@alignOf(frames.DataFrameHeader)) = [_]u8{0} ** 80;
    const h1: *frames.DataFrameHeader = @ptrCast(&frag1_buf);
    h1.* = .{
        .frame_length = 80,
        .flags = constants.flag_begin,
        .sequence_number = 0,
        .correlation_id = 42,
        .source_node_id = 1,
        .source_service_id = 2,
        .target_service_id = 5,
    };
    // Fill payload with 'A'
    @memset(frag1_buf[40..80], 'A');

    const result1 = assembler.onFragment(h1, &frag1_buf);
    try testing.expect(result1 == null); // not complete yet

    // Fragment 2: middle
    // Expected sequence: alignUp(80 + 4, 32) = alignUp(84, 32) = 96
    var frag2_buf: [80]u8 align(@alignOf(frames.DataFrameHeader)) = [_]u8{0} ** 80;
    const h2: *frames.DataFrameHeader = @ptrCast(&frag2_buf);
    h2.* = .{
        .frame_length = 80,
        .flags = 0, // neither BEGIN nor END
        .sequence_number = 96,
        .correlation_id = 42,
        .source_node_id = 1,
        .source_service_id = 2,
        .target_service_id = 5,
    };
    @memset(frag2_buf[40..80], 'B');

    const result2 = assembler.onFragment(h2, &frag2_buf);
    try testing.expect(result2 == null); // not complete yet

    // Fragment 3: END
    var frag3_buf: [80]u8 align(@alignOf(frames.DataFrameHeader)) = [_]u8{0} ** 80;
    const h3: *frames.DataFrameHeader = @ptrCast(&frag3_buf);
    h3.* = .{
        .frame_length = 80,
        .flags = constants.flag_end,
        .sequence_number = 192, // 96 + 96
        .correlation_id = 42,
        .source_node_id = 1,
        .source_service_id = 2,
        .target_service_id = 5,
    };
    @memset(frag3_buf[40..80], 'C');

    // When
    const result3 = assembler.onFragment(h3, &frag3_buf);

    // Then — should return the reassembled message (120 bytes: 3 × 40-byte payloads)
    try testing.expect(result3 != null);
    const reassembled = result3.?;
    try testing.expectEqual(@as(usize, 120), reassembled.len);

    // Verify payload contents
    for (reassembled[0..40]) |b| try testing.expectEqual(@as(u8, 'A'), b);
    for (reassembled[40..80]) |b| try testing.expectEqual(@as(u8, 'B'), b);
    for (reassembled[80..120]) |b| try testing.expectEqual(@as(u8, 'C'), b);
}

test "out-of-order fragment discards in-progress assembly" {
    // Given
    var assembler = FragmentAssembler.init(4096, 0x00010002);
    defer assembler.deinit();

    // BEGIN fragment
    var frag1_buf: [80]u8 align(@alignOf(frames.DataFrameHeader)) = [_]u8{0} ** 80;
    const h1: *frames.DataFrameHeader = @ptrCast(&frag1_buf);
    h1.* = .{
        .frame_length = 80,
        .flags = constants.flag_begin,
        .sequence_number = 0,
        .correlation_id = 42,
    };
    _ = assembler.onFragment(h1, &frag1_buf);
    try testing.expect(assembler.assembling);

    // Out-of-order fragment (wrong sequence — expected 96, got 192)
    var frag_bad_buf: [80]u8 align(@alignOf(frames.DataFrameHeader)) = [_]u8{0} ** 80;
    const h_bad: *frames.DataFrameHeader = @ptrCast(&frag_bad_buf);
    h_bad.* = .{
        .frame_length = 80,
        .flags = 0,
        .sequence_number = 192, // wrong — should be 96
        .correlation_id = 42,
    };

    // When
    const result = assembler.onFragment(h_bad, &frag_bad_buf);

    // Then
    try testing.expect(result == null);
    try testing.expect(!assembler.assembling); // assembly discarded
}

test "fragment with wrong correlation_id discards assembly" {
    // Given
    var assembler = FragmentAssembler.init(4096, 0x00010002);
    defer assembler.deinit();

    var frag1_buf: [80]u8 align(@alignOf(frames.DataFrameHeader)) = [_]u8{0} ** 80;
    const h1: *frames.DataFrameHeader = @ptrCast(&frag1_buf);
    h1.* = .{
        .frame_length = 80,
        .flags = constants.flag_begin,
        .sequence_number = 0,
        .correlation_id = 42,
    };
    _ = assembler.onFragment(h1, &frag1_buf);

    // Middle fragment with different correlation_id
    var frag2_buf: [80]u8 align(@alignOf(frames.DataFrameHeader)) = [_]u8{0} ** 80;
    const h2: *frames.DataFrameHeader = @ptrCast(&frag2_buf);
    h2.* = .{
        .frame_length = 80,
        .flags = 0,
        .sequence_number = 96,
        .correlation_id = 99, // wrong!
    };

    // When
    const result = assembler.onFragment(h2, &frag2_buf);

    // Then
    try testing.expect(result == null);
    try testing.expect(!assembler.assembling);
}

test "middle fragment without active assembly is discarded" {
    // Given
    var assembler = FragmentAssembler.init(4096, 0x00010002);
    defer assembler.deinit();

    // Directly send a middle fragment with no preceding BEGIN
    var frag_buf: [80]u8 align(@alignOf(frames.DataFrameHeader)) = [_]u8{0} ** 80;
    const h: *frames.DataFrameHeader = @ptrCast(&frag_buf);
    h.* = .{
        .frame_length = 80,
        .flags = 0,
        .sequence_number = 0,
        .correlation_id = 42,
    };

    // When
    const result = assembler.onFragment(h, &frag_buf);

    // Then
    try testing.expect(result == null);
    try testing.expect(!assembler.assembling);
}

test "reset clears assembly state" {
    // Given
    var assembler = FragmentAssembler.init(4096, 0x00010002);
    defer assembler.deinit();

    var frag_buf: [80]u8 align(@alignOf(frames.DataFrameHeader)) = [_]u8{0} ** 80;
    const h: *frames.DataFrameHeader = @ptrCast(&frag_buf);
    h.* = .{
        .frame_length = 80,
        .flags = constants.flag_begin,
        .sequence_number = 0,
        .correlation_id = 42,
    };
    _ = assembler.onFragment(h, &frag_buf);
    try testing.expect(assembler.assembling);

    // When — new BEGIN starts fresh
    h.* = .{
        .frame_length = 80,
        .flags = constants.flag_begin,
        .sequence_number = 0,
        .correlation_id = 99,
    };
    _ = assembler.onFragment(h, &frag_buf);

    // Then — assembling with new correlation_id
    try testing.expect(assembler.assembling);
    try testing.expectEqual(@as(i32, 99), assembler.active_correlation_id);
}
