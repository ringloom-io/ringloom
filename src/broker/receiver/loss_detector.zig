//! Loss detector for the BRZ broker receive path.
//!
//! Scans a peer's receive log buffer for gaps (positions where the frame
//! length is zero between rebuild_position and tail_position). When a gap
//! persists beyond the NAK delay, signals that a NAK should be sent.
//!
//! The detector tracks one active gap at a time (the earliest gap from
//! rebuild_position). This simplification ensures retransmitted frames
//! fill in from the bottom, allowing consumption_position to advance.

const std = @import("std");
const brz_common = @import("brz_common");
const constants = brz_common.platform.constants;
const ReceiveLogBuffer = brz_common.memory.receive_log.ReceiveLogBuffer;

pub const LossDetector = struct {
    /// Position of the currently tracked gap (if any).
    active_gap_position: i64,

    /// Length of the currently tracked gap in bytes.
    active_gap_length: i32,

    /// Timestamp (monotonic ns) at which we will send the NAK for the
    /// active gap. Set to `now + NAK_INITIAL_DELAY_NS` when a new gap
    /// is first detected. Reset to `now + NAK_RETRY_DELAY_NS` after
    /// each NAK is sent.
    nak_expiry_ns: i64,

    /// True if there is an active gap being tracked.
    has_active_gap: bool,

    /// The highest position that has been verified as contiguous.
    /// This is the loss detector's own cursor — it only advances when
    /// all frames up to this point are present.
    rebuild_position: i64,

    const Self = @This();

    pub fn init(initial_position: i64) Self {
        return .{
            .active_gap_position = 0,
            .active_gap_length = 0,
            .nak_expiry_ns = 0,
            .has_active_gap = false,
            .rebuild_position = initial_position,
        };
    }

    /// Scan the receive log buffer for gaps between rebuild_position and
    /// tail_position. If a gap is found and the NAK timer has expired,
    /// returns 1 (indicating a NAK should be sent). Otherwise returns 0.
    ///
    /// This function also advances rebuild_position past contiguous
    /// committed frames, which is an important secondary effect: the
    /// rebuild_position is used by the receiver window calculation to
    /// determine how much data is "in flight" vs. "fully received".
    pub fn scan(self: *Self, log: *ReceiveLogBuffer, now_ns: i64) u32 {
        const hwm = log.loadTailPosition();

        // Nothing to scan — tail hasn't advanced beyond our cursor.
        if (self.rebuild_position >= hwm) return 0;

        const mask = log.mask;
        var pos = self.rebuild_position;

        // ── Advance through contiguous committed frames ───────────────
        while (pos < hwm) {
            const index: usize = @intCast(@as(u64, @bitCast(pos)) & mask);
            const length_ptr: *const volatile i32 = @ptrCast(@alignCast(&log.data[index]));
            const frame_length = @atomicLoad(i32, length_ptr, .acquire);

            if (frame_length <= 0 and frame_length != frame_consumed_marker) {
                // ── Gap found (length == 0) ───────────────────────────
                // The frame at this position has not been received.
                break;
            }

            // Frame is present (length > 0) or consumed (length == -1).
            // Read the aligned slot size to advance past it.
            const aligned_len = readAlignedFrameLength(log, index);
            if (aligned_len <= 0) break; // safety: corrupted slot

            pos += @as(i64, aligned_len);
        }

        // Update rebuild_position — everything up to `pos` is contiguous.
        if (pos > self.rebuild_position) {
            self.rebuild_position = pos;
            log.storeRebuildPosition(pos);

            // If we advanced past the active gap, clear it.
            if (self.has_active_gap and pos > self.active_gap_position) {
                self.has_active_gap = false;
            }
        }

        // If we've caught up to the high-water mark, no gap.
        if (pos >= hwm) return 0;

        // ── A gap exists at `pos` ─────────────────────────────────────
        // Find the end of the gap (scan forward until a committed frame).
        const gap_start = pos;
        var gap_end = pos;
        while (gap_end < hwm) {
            const idx: usize = @intCast(@as(u64, @bitCast(gap_end)) & mask);
            const lp: *const volatile i32 = @ptrCast(@alignCast(&log.data[idx]));
            const fl = @atomicLoad(i32, lp, .acquire);

            if (fl > 0 or fl == frame_consumed_marker) break; // end of gap

            // Advance by one frame alignment — we don't know the actual
            // frame size for the missing slot, so we advance by the
            // minimum alignment.
            gap_end += @as(i64, @intCast(frame_alignment));
        }

        const gap_length: i32 = @intCast(gap_end - gap_start);

        // ── Track the gap ─────────────────────────────────────────────
        if (!self.has_active_gap or self.active_gap_position != gap_start) {
            // New gap (or the gap moved). Set the initial NAK delay.
            self.active_gap_position = gap_start;
            self.active_gap_length = gap_length;
            self.has_active_gap = true;
            self.nak_expiry_ns = now_ns + constants.nak_initial_delay_ns;
            return 0; // don't NAK yet — wait for the delay
        }

        // ── Check NAK timer ───────────────────────────────────────────
        if (now_ns < self.nak_expiry_ns) return 0; // not yet

        // Timer expired — update the gap length (it may have grown or
        // partially filled since we started tracking it) and signal
        // that a NAK should be sent.
        self.active_gap_length = gap_length;
        self.nak_expiry_ns = now_ns + constants.nak_retry_delay_ns;

        return 1; // caller should send a NAK
    }

    /// Returns the position and length of the current active gap.
    /// Only valid when scan() returned > 0.
    pub fn activeGap(self: *const Self) struct { position: i64, length: i32 } {
        return .{
            .position = self.active_gap_position,
            .length = self.active_gap_length,
        };
    }

    /// Sentinel value for consumed frames (must match the router's marker).
    const frame_consumed_marker: i32 = -1;

    /// Minimum frame alignment in the receive log.
    const frame_alignment: usize = 32;
};

/// Read the aligned frame length from the receive log at the given slot index.
///
/// The receive log stores each frame as:
///   [slot_index + 0]: commit marker — i32 frame data length (written last with release)
///   [slot_index + 4]: frame data (DataFrameHeader + payload)
///
/// The commit marker holds the raw frame data length. We add the 4-byte
/// length prefix and align up to `frame_alignment` (32 bytes) to get the
/// total slot size — matching the calculation in `ReceiveLogBuffer.insertPacket`.
fn readAlignedFrameLength(log: *const ReceiveLogBuffer, slot_index: usize) i32 {
    // Read the commit marker (frame data length) at the slot start.
    // For consumed frames (marker == -1) we need the original length,
    // so we read the frame_length field from the DataFrameHeader that
    // was written into the frame data region at slot_index + 4.
    const marker_ptr: *const volatile i32 = @ptrCast(@alignCast(&log.data[slot_index]));
    const marker = @atomicLoad(i32, marker_ptr, .acquire);

    const frame_data_length: usize = if (marker > 0)
        @intCast(marker)
    else if (marker == -1) blk: {
        // Frame was consumed — the commit marker was overwritten with -1.
        // Read the original frame_length from the DataFrameHeader which
        // is at slot_index + 4 (the first i32 of the frame data).
        const frame_len_ptr: *const volatile i32 = @ptrCast(@alignCast(&log.data[slot_index + @sizeOf(i32)]));
        const fl = @atomicLoad(i32, frame_len_ptr, .acquire);
        if (fl <= 0) break :blk 0;
        break :blk @intCast(fl);
    } else 0;

    if (frame_data_length == 0) return 0;

    // Total slot = length prefix (4 bytes) + frame data, aligned up to 32.
    return @intCast(constants.alignUp(frame_data_length + @sizeOf(i32), 32));
}

// ── Tests ─────────────────────────────────────────────────────────────

const testing = std.testing;

test "scan returns 0 when rebuild_position equals tail" {
    // Given
    var log = try ReceiveLogBuffer.allocate(4096);
    defer log.close();

    var detector = LossDetector.init(0);

    // When — no packets inserted, tail is still 0
    const result = detector.scan(&log, 0);

    // Then — no work
    try testing.expectEqual(@as(u32, 0), result);
    try testing.expect(!detector.has_active_gap);
}

test "scan advances rebuild_position through contiguous frames" {
    // Given — two contiguous frames inserted sequentially
    var log = try ReceiveLogBuffer.allocate(4096);
    defer log.close();

    var detector = LossDetector.init(0);

    // Insert two 80-byte frames (frame data). Each slot = alignUp(80 + 4, 32) = alignUp(84, 32) = 96 bytes.
    const frame1 = [_]u8{0xAA} ** 80;
    log.insertPacket(&frame1);

    const frame2 = [_]u8{0xBB} ** 80;
    log.insertPacket(&frame2);

    // Tail should be at 192 (2 × 96).
    try testing.expectEqual(@as(i64, 192), log.loadTailPosition());

    // When
    const result = detector.scan(&log, 0);

    // Then — no gap, rebuild_position advances past both frames
    try testing.expectEqual(@as(u32, 0), result);
    try testing.expectEqual(@as(i64, 192), detector.rebuild_position);
    try testing.expect(!detector.has_active_gap);
}

test "scan detects gap and returns 1 after initial delay" {
    // Given — a gap at position 0, with a frame present further ahead.
    // We simulate this by writing a frame, then zeroing out the commit
    // marker at position 0 to create a gap, while the tail remains advanced.
    var log = try ReceiveLogBuffer.allocate(4096);
    defer log.close();

    // Insert a frame at position 0 (so tail advances to 96).
    const frame = [_]u8{0xAA} ** 80;
    log.insertPacket(&frame);

    // Insert a second frame at position 96 (tail advances to 192).
    log.insertPacket(&frame);

    // Now zero out the commit marker at position 0 to simulate a gap.
    const marker_ptr: *volatile i32 = @ptrCast(@alignCast(&log.data[0]));
    @atomicStore(i32, marker_ptr, 0, .release);

    var detector = LossDetector.init(0);

    // When — first scan at t=1s
    const now_ns: i64 = 1_000_000_000;
    const result1 = detector.scan(&log, now_ns);

    // Then — gap detected, but NAK not yet due (initial delay)
    try testing.expectEqual(@as(u32, 0), result1);
    try testing.expect(detector.has_active_gap);
    try testing.expectEqual(@as(i64, 0), detector.active_gap_position);

    // When — scan again after initial delay (60ms)
    const result2 = detector.scan(&log, now_ns + 61 * std.time.ns_per_ms);

    // Then — NAK should fire
    try testing.expectEqual(@as(u32, 1), result2);
}

test "gap is cleared when missing frame arrives" {
    // Given — a gap at position 0, frame at position 96
    var log = try ReceiveLogBuffer.allocate(4096);
    defer log.close();

    const frame = [_]u8{0xAA} ** 80;
    log.insertPacket(&frame); // pos 0, tail -> 96
    log.insertPacket(&frame); // pos 96, tail -> 192

    // Zero out commit marker at position 0 to simulate gap.
    const marker_ptr: *volatile i32 = @ptrCast(@alignCast(&log.data[0]));
    @atomicStore(i32, marker_ptr, 0, .release);

    var detector = LossDetector.init(0);

    _ = detector.scan(&log, 0); // detect gap
    try testing.expect(detector.has_active_gap);
    try testing.expectEqual(@as(i64, 0), detector.active_gap_position);

    // Fill the gap — restore the commit marker.
    @atomicStore(i32, marker_ptr, 80, .release);

    // When — scan again
    _ = detector.scan(&log, 0);

    // Then — gap should be cleared, rebuild_position advanced past both frames
    try testing.expect(!detector.has_active_gap);
    try testing.expectEqual(@as(i64, 192), detector.rebuild_position);
}

test "NAK retry fires after retry delay" {
    // Given — a persistent gap at position 0
    var log = try ReceiveLogBuffer.allocate(4096);
    defer log.close();

    const frame = [_]u8{0xAA} ** 80;
    log.insertPacket(&frame); // pos 0, tail -> 96
    log.insertPacket(&frame); // pos 96, tail -> 192

    // Zero out commit marker at position 0 to simulate gap.
    const marker_ptr: *volatile i32 = @ptrCast(@alignCast(&log.data[0]));
    @atomicStore(i32, marker_ptr, 0, .release);

    var detector = LossDetector.init(0);
    var now_ns: i64 = 0;

    // Scan 1: detect gap, start NAK timer
    _ = detector.scan(&log, now_ns);
    try testing.expect(detector.has_active_gap);

    // Scan 2: 61ms later — first NAK fires
    now_ns += 61 * std.time.ns_per_ms;
    const r1 = detector.scan(&log, now_ns);
    try testing.expectEqual(@as(u32, 1), r1);

    // Scan 3: 30ms after first NAK — retry not yet due
    now_ns += 30 * std.time.ns_per_ms;
    const r2 = detector.scan(&log, now_ns);
    try testing.expectEqual(@as(u32, 0), r2);

    // Scan 4: 61ms after first NAK — retry fires
    now_ns += 31 * std.time.ns_per_ms;
    const r3 = detector.scan(&log, now_ns);
    try testing.expectEqual(@as(u32, 1), r3);
}
