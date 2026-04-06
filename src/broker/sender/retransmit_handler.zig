//! RetransmitHandler — NAK state machine with linger suppression.
//!
//! After retransmitting a range, the handler enters a "lingering" state for
//! `constants.retransmit_linger_ns` (10 µs). Any NAK that overlaps the
//! lingering range is suppressed. After the linger period expires, the
//! handler returns to inactive and will honor new NAKs.

const std = @import("std");
const constants = @import("brz_common").platform.constants;
const RetransmitBuffer = @import("retransmit_buffer.zig").RetransmitBuffer;

pub const RetransmitHandler = struct {
    pub const State = enum {
        /// No active retransmit. Ready to process any NAK.
        inactive,
        /// Recently retransmitted. Suppressing overlapping NAKs until expiry.
        lingering,
    };

    state: State,
    position: i64,
    length: i32,
    expiry_ns: i64,

    const Self = @This();

    pub fn init() Self {
        return .{
            .state = .inactive,
            .position = 0,
            .length = 0,
            .expiry_ns = 0,
        };
    }

    /// Process a NAK. If not suppressed, retransmit immediately and enter linger state.
    /// `send_fn` is called for each frame that needs retransmission.
    pub fn onNak(
        self: *Self,
        position: i64,
        length: i32,
        now_ns: i64,
        retransmit_buffer: *const RetransmitBuffer,
        send_fn: *const fn (frame: []const u8) void,
    ) void {
        // Suppress overlapping NAKs during linger period
        if (self.state == .lingering and self.overlaps(position, length)) {
            return;
        }

        // Retransmit immediately
        self.resend(position, length, retransmit_buffer, send_fn);

        // Enter linger state
        self.state = .lingering;
        self.position = position;
        self.length = length;
        self.expiry_ns = now_ns + constants.retransmit_linger_ns;
    }

    /// Transition back to inactive when the linger period expires.
    pub fn processTimeouts(self: *Self, now_ns: i64) void {
        if (self.state == .lingering and now_ns >= self.expiry_ns) {
            self.state = .inactive;
        }
    }

    /// Check if the given range overlaps the currently lingering range.
    pub fn overlaps(self: *const Self, position: i64, length: i32) bool {
        const self_end = self.position + @as(i64, self.length);
        const other_end = position + @as(i64, length);
        return position < self_end and self.position < other_end;
    }

    /// Look up frames in the retransmit buffer and resend them.
    fn resend(
        self: *Self,
        position: i64,
        length: i32,
        retransmit_buffer: *const RetransmitBuffer,
        send_fn: *const fn (frame: []const u8) void,
    ) void {
        _ = self;
        const frame_alignment: usize = 32;
        var offset: i64 = position;
        const end: i64 = position + @as(i64, length);

        while (offset < end) {
            const seq_estimate = @divTrunc(offset, @as(i64, @intCast(frame_alignment)));
            if (retransmit_buffer.lookup(seq_estimate)) |frame| {
                send_fn(frame);
            }
            offset += @as(i64, @intCast(frame_alignment));
        }
    }

    // ── Test helpers ──────────────────────────────────────────────────

    /// Test-only: trigger state transition without requiring network I/O.
    pub fn onNakForTest(self: *Self, position: i64, length: i32, now_ns: i64) void {
        if (self.state == .lingering and self.overlaps(position, length)) {
            return;
        }
        self.state = .lingering;
        self.position = position;
        self.length = length;
        self.expiry_ns = now_ns + constants.retransmit_linger_ns;
    }

    /// Test-only: check if a NAK would be suppressed without doing anything.
    pub fn wouldSuppress(self: *const Self, position: i64, length: i32) bool {
        return self.state == .lingering and self.overlaps(position, length);
    }
};

// ── Tests ─────────────────────────────────────────────────────────────

test "retransmit handler transitions inactive → lingering → inactive" {
    // Given
    var handler = RetransmitHandler.init();
    try std.testing.expectEqual(RetransmitHandler.State.inactive, handler.state);

    const now_ns: i64 = 1_000_000;

    // When — process a NAK
    handler.onNakForTest(100, 40, now_ns);

    // Then — handler should be lingering with the correct range
    try std.testing.expectEqual(RetransmitHandler.State.lingering, handler.state);
    try std.testing.expectEqual(@as(i64, 100), handler.position);
    try std.testing.expectEqual(@as(i32, 40), handler.length);

    // When — process timeouts after linger period expires
    handler.processTimeouts(now_ns + constants.retransmit_linger_ns + 1);

    // Then — handler should be back to inactive
    try std.testing.expectEqual(RetransmitHandler.State.inactive, handler.state);
}

test "retransmit handler suppresses overlapping NAK during linger" {
    // Given — handler is lingering for range [100, 140)
    var handler = RetransmitHandler.init();
    const now_ns: i64 = 1_000_000;
    handler.onNakForTest(100, 40, now_ns);

    // When / Then — an identical NAK should be suppressed
    try std.testing.expect(handler.wouldSuppress(100, 40));
}

test "retransmit handler allows non-overlapping NAK during linger" {
    // Given — handler is lingering for range [100, 140)
    var handler = RetransmitHandler.init();
    const now_ns: i64 = 1_000_000;
    handler.onNakForTest(100, 40, now_ns);

    // When / Then — a non-overlapping NAK should not be suppressed
    try std.testing.expect(!handler.wouldSuppress(200, 40));
}

test "processTimeouts does nothing when not lingering" {
    // Given — handler is inactive
    var handler = RetransmitHandler.init();
    try std.testing.expectEqual(RetransmitHandler.State.inactive, handler.state);

    // When — process timeouts
    handler.processTimeouts(999_999_999);

    // Then — handler should still be inactive
    try std.testing.expectEqual(RetransmitHandler.State.inactive, handler.state);
}

test "overlaps detects partial overlap" {
    // Given — handler is lingering for range [100, 200)
    var handler = RetransmitHandler.init();
    const now_ns: i64 = 1_000_000;
    handler.onNakForTest(100, 100, now_ns);

    // When / Then — partial overlap: [150, 250) overlaps [100, 200)
    try std.testing.expect(handler.overlaps(150, 100));

    // When / Then — adjacent range: [200, 240) does NOT overlap [100, 200)
    try std.testing.expect(!handler.overlaps(200, 40));
}
