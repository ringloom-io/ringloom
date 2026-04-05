//! Deduplicating error log over a flat byte buffer.
//!
//! When the same error description is recorded more than once, the existing
//! entry's observation count is atomically incremented and its last-observation
//! timestamp is updated, rather than appending a duplicate entry.
//!
//! Entry layout (24-byte header + variable-length description):
//!
//!   Offset  Size  Field
//!   ──────  ────  ─────────────────────────────
//!   0       4     entry_length (i32, atomic release/acquire — commit flag)
//!   4       4     observation_count (i32, atomic)
//!   8       8     last_observation_timestamp (i64, atomic)
//!   16      8     first_observation_timestamp (i64, plain)
//!   24      N     description bytes (UTF-8)
//!
//! Entries are aligned to 8-byte boundaries so that atomic operations on the
//! i64 timestamp fields are naturally aligned.

const std = @import("std");

/// Size of the fixed header preceding every entry's description.
pub const entry_header_length: usize = 24; // 4 + 4 + 8 + 8

/// Alignment boundary for each entry's total (header + description) length.
/// Must be 8 so that i64 fields at offsets +8 and +16 are naturally aligned.
pub const entry_alignment: usize = @sizeOf(i64); // 8 bytes

/// A deduplicating, append-only error log backed by a caller-provided buffer.
pub const ErrorLog = struct {
    buffer: []u8,
    next_offset: usize,

    /// A snapshot of a single error-log entry, returned by `forEach`.
    pub const Entry = struct {
        observation_count: i32,
        last_observation_timestamp: i64,
        first_observation_timestamp: i64,
        description: []const u8,
    };

    /// Initialise an `ErrorLog` over `buffer`. The buffer is *not* zeroed —
    /// the caller must ensure it starts as all-zeroes if a clean log is desired.
    pub fn init(buffer: []u8) ErrorLog {
        return .{
            .buffer = buffer,
            .next_offset = 0,
        };
    }

    /// Record an error.
    ///
    /// If an entry with an identical `description` already exists its
    /// observation count is incremented and `timestamp_ms` is stored as
    /// the last-observation timestamp.  Otherwise a new entry is appended.
    ///
    /// Returns `true` on success, `false` if the log is full and the
    /// description has not been seen before (i.e. dedup still works when
    /// the log is full).
    pub fn record(self: *ErrorLog, description: []const u8, timestamp_ms: i64) bool {
        // ── Phase 1: scan for a duplicate ────────────────────────────
        var offset: usize = 0;
        while (offset < self.next_offset) {
            const len = self.readEntryLength(offset);
            if (len <= 0) break;
            const entry_len: usize = @intCast(len);

            const desc_len = entry_len - entry_header_length;
            const desc_start = offset + entry_header_length;
            const desc_end = desc_start + desc_len;

            if (desc_end <= self.buffer.len and
                desc_len == description.len and
                std.mem.eql(u8, self.buffer[desc_start..desc_end], description))
            {
                self.incrementObservationCount(offset);
                self.updateLastTimestamp(offset, timestamp_ms);
                return true;
            }

            offset += alignForward(entry_len, entry_alignment);
        }

        // ── Phase 2: append a new entry ──────────────────────────────
        const total_length = entry_header_length + description.len;
        const aligned_length = alignForward(total_length, entry_alignment);

        if (self.next_offset + aligned_length > self.buffer.len) return false;

        const base = self.next_offset;

        // observation_count = 1  (base + 4, atomic monotonic)
        const count_ptr: *i32 = @ptrCast(@alignCast(self.buffer.ptr + base + 4));
        @atomicStore(i32, count_ptr, 1, .monotonic);

        // last_observation_timestamp  (base + 8, atomic monotonic)
        const last_ts_ptr: *i64 = @ptrCast(@alignCast(self.buffer.ptr + base + 8));
        @atomicStore(i64, last_ts_ptr, timestamp_ms, .monotonic);

        // first_observation_timestamp  (base + 16, plain store)
        const first_ts_ptr: *i64 = @ptrCast(@alignCast(self.buffer.ptr + base + 16));
        first_ts_ptr.* = timestamp_ms;

        // description bytes  (base + 24)
        @memcpy(self.buffer[base + entry_header_length .. base + entry_header_length + description.len], description);

        // Commit: write length at base + 0 (atomic release) — makes all
        // preceding writes visible to readers that do an acquire load.
        const len_ptr: *i32 = @ptrCast(@alignCast(self.buffer.ptr + base));
        @atomicStore(i32, len_ptr, @intCast(total_length), .release);

        self.next_offset = base + aligned_length;
        return true;
    }

    /// Iterate over every committed entry in the log.
    pub fn forEach(self: *const ErrorLog, callback: *const fn (entry: Entry) void) void {
        var offset: usize = 0;
        while (offset < self.buffer.len) {
            const len = self.readEntryLength(offset);
            if (len <= 0) break;
            const entry_len: usize = @intCast(len);
            const desc_len = entry_len - entry_header_length;

            // observation_count (atomic acquire)
            const count_ptr: *const i32 = @ptrCast(@alignCast(self.buffer.ptr + offset + 4));
            const observation_count = @atomicLoad(i32, count_ptr, .acquire);

            // last_observation_timestamp (atomic acquire)
            const last_ts_ptr: *const i64 = @ptrCast(@alignCast(self.buffer.ptr + offset + 8));
            const last_ts = @atomicLoad(i64, last_ts_ptr, .acquire);

            // first_observation_timestamp (plain load)
            const first_ts_ptr: *const i64 = @ptrCast(@alignCast(self.buffer.ptr + offset + 16));
            const first_ts = first_ts_ptr.*;

            const desc_start = offset + entry_header_length;

            callback(.{
                .observation_count = observation_count,
                .last_observation_timestamp = last_ts,
                .first_observation_timestamp = first_ts,
                .description = self.buffer[desc_start .. desc_start + desc_len],
            });

            offset += alignForward(entry_len, entry_alignment);
        }
    }

    // ── Internal Helpers ──────────────────────────────────────────────

    /// Read the entry length at `offset` with acquire ordering.
    fn readEntryLength(self: *const ErrorLog, offset: usize) i32 {
        const ptr: *const i32 = @ptrCast(@alignCast(self.buffer.ptr + offset));
        return @atomicLoad(i32, ptr, .acquire);
    }

    /// Atomically increment the observation count at `offset + 4`.
    fn incrementObservationCount(self: *ErrorLog, offset: usize) void {
        const ptr: *i32 = @ptrCast(@alignCast(self.buffer.ptr + offset + 4));
        _ = @atomicRmw(i32, ptr, .Add, 1, .monotonic);
    }

    /// Atomically update the last-observation timestamp at `offset + 8`.
    fn updateLastTimestamp(self: *ErrorLog, offset: usize, timestamp_ms: i64) void {
        const ptr: *i64 = @ptrCast(@alignCast(self.buffer.ptr + offset + 8));
        @atomicStore(i64, ptr, timestamp_ms, .release);
    }
};

/// Round `value` up to the next multiple of `alignment`.
fn alignForward(value: usize, alignment: usize) usize {
    return (value + alignment - 1) & ~(alignment - 1);
}

// ── Tests ─────────────────────────────────────────────────────────────

// File-level mutable state used by forEach test callbacks.
var test_entry_count: usize = 0;
var test_last_entry: ?ErrorLog.Entry = null;

fn testCountCallback(entry: ErrorLog.Entry) void {
    test_entry_count += 1;
    test_last_entry = .{
        .observation_count = entry.observation_count,
        .last_observation_timestamp = entry.last_observation_timestamp,
        .first_observation_timestamp = entry.first_observation_timestamp,
        .description = entry.description,
    };
}

// Additional callback state for the multi-entry test.
var test_entries_seen: [8]ErrorLog.Entry = undefined;
var test_entries_seen_count: usize = 0;

fn testCollectCallback(entry: ErrorLog.Entry) void {
    if (test_entries_seen_count < test_entries_seen.len) {
        test_entries_seen[test_entries_seen_count] = entry;
    }
    test_entries_seen_count += 1;
}

test "record new error" {
    // Given
    var buf: [256]u8 align(8) = @splat(0);
    var log = ErrorLog.init(&buf);

    // When
    const ok = log.record("something went wrong", 1000);

    // Then
    try std.testing.expect(ok);

    test_entry_count = 0;
    test_last_entry = null;
    log.forEach(&testCountCallback);

    try std.testing.expectEqual(@as(usize, 1), test_entry_count);
    try std.testing.expect(test_last_entry != null);

    const entry = test_last_entry.?;
    try std.testing.expectEqual(@as(i32, 1), entry.observation_count);
    try std.testing.expectEqual(@as(i64, 1000), entry.first_observation_timestamp);
    try std.testing.expectEqual(@as(i64, 1000), entry.last_observation_timestamp);
    try std.testing.expectEqualStrings("something went wrong", entry.description);
}

test "record same error twice increments observation_count" {
    // Given
    var buf: [256]u8 align(8) = @splat(0);
    var log = ErrorLog.init(&buf);

    // When
    _ = log.record("duplicate error", 1000);
    _ = log.record("duplicate error", 2000);

    // Then
    test_entry_count = 0;
    test_last_entry = null;
    log.forEach(&testCountCallback);

    try std.testing.expectEqual(@as(usize, 1), test_entry_count);

    const entry = test_last_entry.?;
    try std.testing.expectEqual(@as(i32, 2), entry.observation_count);
    try std.testing.expectEqual(@as(i64, 1000), entry.first_observation_timestamp);
    try std.testing.expectEqual(@as(i64, 2000), entry.last_observation_timestamp);
}

test "record returns false when log is full" {
    // Given — a tiny buffer that can hold one short entry but not a second long one.
    // entry_alignment is 8, so the buffer must be a multiple of 8 and aligned to 8.
    var buf: [64]u8 align(8) = @splat(0);
    var log = ErrorLog.init(&buf);

    // When
    const first = log.record("ok", 100);
    const second = log.record("this description is way too long to fit in the remaining space", 200);

    // Then
    try std.testing.expect(first);
    try std.testing.expect(!second);
}

test "different errors get separate entries" {
    // Given
    var buf: [512]u8 align(8) = @splat(0);
    var log = ErrorLog.init(&buf);

    // When
    _ = log.record("error A", 100);
    _ = log.record("error B", 200);
    _ = log.record("error A", 300); // duplicate of first

    // Then — should see exactly 2 distinct entries
    test_entries_seen_count = 0;
    log.forEach(&testCollectCallback);

    try std.testing.expectEqual(@as(usize, 2), test_entries_seen_count);

    // First entry: "error A" observed twice
    try std.testing.expectEqualStrings("error A", test_entries_seen[0].description);
    try std.testing.expectEqual(@as(i32, 2), test_entries_seen[0].observation_count);
    try std.testing.expectEqual(@as(i64, 100), test_entries_seen[0].first_observation_timestamp);
    try std.testing.expectEqual(@as(i64, 300), test_entries_seen[0].last_observation_timestamp);

    // Second entry: "error B" observed once
    try std.testing.expectEqualStrings("error B", test_entries_seen[1].description);
    try std.testing.expectEqual(@as(i32, 1), test_entries_seen[1].observation_count);
    try std.testing.expectEqual(@as(i64, 200), test_entries_seen[1].first_observation_timestamp);
    try std.testing.expectEqual(@as(i64, 200), test_entries_seen[1].last_observation_timestamp);
}
