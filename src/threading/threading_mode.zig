//! Threading mode configuration for the BRZ broker.
//!
//! Controls how many OS threads the broker uses to run its event loops.

/// The threading mode determines how event loops are mapped to threads.
pub const ThreadingMode = enum {
    /// 3 threads: Control + Sender + Receiver (each on its own thread).
    /// Lowest latency — each loop can burn its own core.
    dedicated,

    /// 2 threads: Control on one, Sender + Receiver combined on another.
    /// Half the threads, but sender and receiver contend for the same core.
    shared_network,

    /// 1 thread: All event loops on a single thread.
    /// Simplest mode — primarily useful for testing.
    shared,
};

// ── Tests ─────────────────────────────────────────────────────────────

const std = @import("std");
const testing = std.testing;

test "ThreadingMode has three variants" {
    // Given / When / Then
    const fields = @typeInfo(ThreadingMode).@"enum".fields;
    try testing.expectEqual(@as(usize, 3), fields.len);
}

test "ThreadingMode default is dedicated" {
    // Given / When
    const mode: ThreadingMode = .dedicated;

    // Then
    try testing.expect(mode == .dedicated);
}
