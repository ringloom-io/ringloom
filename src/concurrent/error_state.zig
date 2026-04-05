//! Thread-local error state for rich diagnostic context.
//!
//! Provides a per-thread `ErrorState` that captures an error code and a
//! human-readable message without heap allocation. The message buffer is
//! a fixed 8 KiB array stored in thread-local storage so that each thread
//! can independently record its most recent error.

const std = @import("std");

/// Maximum length (in bytes) of an error message that can be stored.
pub const max_error_message_length: usize = 8192;

/// Per-thread error state carrying an error code and a formatted message.
///
/// Usage:
///   - Call `set` or `setFmt` to record an error.
///   - Call `message` to retrieve the stored message (returns `null` when no error is set).
///   - Call `clear` to reset to the no-error state.
pub const ErrorState = struct {
    errcode: i32 = 0,
    errmsg: [max_error_message_length]u8 = undefined,
    msg_len: usize = 0,

    /// Record an error with a pre-built message slice.
    /// The message is truncated to `max_error_message_length` if necessary.
    pub fn set(self: *ErrorState, code: i32, msg: []const u8) void {
        self.errcode = code;
        const len = @min(msg.len, max_error_message_length);
        @memcpy(self.errmsg[0..len], msg[0..len]);
        self.msg_len = len;
    }

    /// Record an error with `std.fmt`-style formatting.
    /// If the formatted output exceeds the buffer the message is silently truncated.
    pub fn setFmt(self: *ErrorState, code: i32, comptime fmt: []const u8, args: anytype) void {
        self.errcode = code;
        const result = std.fmt.bufPrint(&self.errmsg, fmt, args) catch |err| switch (err) {
            error.NoSpaceLeft => {
                self.msg_len = max_error_message_length;
                return;
            },
        };
        self.msg_len = result.len;
    }

    /// Reset to the no-error state.
    pub fn clear(self: *ErrorState) void {
        self.errcode = 0;
        self.msg_len = 0;
    }

    /// Return the stored message, or `null` if no error is set.
    pub fn message(self: *const ErrorState) ?[]const u8 {
        if (self.errcode == 0) return null;
        return self.errmsg[0..self.msg_len];
    }

    /// Return `true` when an error has been recorded (code != 0).
    pub fn isSet(self: *const ErrorState) bool {
        return self.errcode != 0;
    }
};

/// Thread-local error state instance. Each thread gets its own copy.
pub threadlocal var err_state: ErrorState = .{};

// ── Tests ─────────────────────────────────────────────────────────────

test "set and read error state" {
    var state = ErrorState{};

    state.set(-1, "something went wrong");

    try std.testing.expect(state.isSet());
    try std.testing.expectEqual(@as(i32, -1), state.errcode);

    const msg = state.message() orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("something went wrong", msg);
}

test "clear resets error state" {
    var state = ErrorState{};

    state.set(42, "an error");
    try std.testing.expect(state.isSet());

    state.clear();

    try std.testing.expect(!state.isSet());
    try std.testing.expectEqual(@as(?[]const u8, null), state.message());
}

test "setFmt formats message" {
    var state = ErrorState{};

    state.setFmt(7, "failed after {d} retries: {s}", .{ 3, "timeout" });

    try std.testing.expect(state.isSet());
    try std.testing.expectEqual(@as(i32, 7), state.errcode);

    const msg = state.message() orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("failed after 3 retries: timeout", msg);
}

test "setFmt truncates when message exceeds buffer" {
    var state = ErrorState{};

    // Build a format string argument that will exceed 8192 bytes.
    const long_payload = "X" ** (max_error_message_length + 100);
    state.setFmt(1, "{s}", .{long_payload});

    try std.testing.expect(state.isSet());
    try std.testing.expectEqual(max_error_message_length, state.msg_len);
}

test "threadlocal err_state is independently accessible" {
    // Verify the threadlocal variable exists and is usable from the test thread.
    err_state.set(99, "thread error");
    try std.testing.expect(err_state.isSet());
    try std.testing.expectEqual(@as(i32, 99), err_state.errcode);

    const msg = err_state.message() orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("thread error", msg);

    err_state.clear();
    try std.testing.expect(!err_state.isSet());
}
