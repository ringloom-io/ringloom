//! Atomic counters manager for the BRZ broker.
//!
//! Manages shared atomic counters where each counter has a value (in one buffer)
//! and metadata (in another buffer). The two buffers are separated so that
//! frequent value updates do not invalidate metadata cache lines.
//!
//! Counter values are cache-line-padded (128 bytes each) to prevent false sharing.
//! Metadata entries are 256 bytes each, holding state, type_id, and a label string.
//!
//! Metadata layout per slot (256 bytes):
//!   [0..4)    state      (i32: unused=0, allocated=1, reclaimed=-1)
//!   [4..8)    type_id    (i32)
//!   [8..12)   label_len  (i32)
//!   [12..256) label      (up to 244 bytes of UTF-8 text)

const std = @import("std");
const constants = @import("../platform/constants.zig");

// ── Public Constants ──────────────────────────────────────────────────

/// Each counter value occupies one full cache-line-pad (128 bytes) to
/// prevent false sharing between adjacent counters.
pub const counter_value_length: usize = constants.cache_line_pad; // 128

/// Each metadata slot is 256 bytes: state (4) + type_id (4) + label_len (4)
/// + label (up to 244).
pub const counter_metadata_length: usize = 256;

/// Maximum number of label bytes that fit in a metadata slot.
const max_label_length: usize = counter_metadata_length - 12; // 244

/// State of a counter slot.
pub const CounterState = enum(i32) {
    unused = 0,
    allocated = 1,
    reclaimed = -1,
};

// ── CountersManager ───────────────────────────────────────────────────

/// Manages a set of shared atomic counters backed by two separate buffers:
/// one for values and one for metadata.
pub const CountersManager = struct {
    values_buffer: []align(constants.cache_line_pad) u8,
    metadata_buffer: []u8,
    max_counter_id: usize,

    const Self = @This();

    /// Metadata view returned by `readMetadata`.
    pub const MetadataView = struct {
        type_id: i32,
        label: []const u8,
    };

    // ── Construction ──────────────────────────────────────────────────

    /// Create a new `CountersManager` over the provided buffers.
    ///
    /// `max_counter_id` is the minimum of the two buffer capacities minus one,
    /// or zero if either buffer is too small to hold even a single counter.
    pub fn init(
        values_buffer: []align(constants.cache_line_pad) u8,
        metadata_buffer: []u8,
    ) Self {
        const max_by_values = values_buffer.len / counter_value_length;
        const max_by_metadata = metadata_buffer.len / counter_metadata_length;
        const capacity = @min(max_by_values, max_by_metadata);
        return .{
            .values_buffer = values_buffer,
            .metadata_buffer = metadata_buffer,
            .max_counter_id = if (capacity == 0) 0 else capacity - 1,
        };
    }

    // ── Allocation / Free ─────────────────────────────────────────────

    /// Scan for an unused or reclaimed slot, atomically claim it, write
    /// metadata, zero the value, and return the counter id.
    /// Returns `null` if every slot is already allocated.
    pub fn allocate(self: *Self, type_id: i32, label: []const u8) ?usize {
        var id: usize = 0;
        while (id <= self.max_counter_id) : (id += 1) {
            const state_ptr = self.metadataStatePtr(id);
            const current = @atomicLoad(i32, state_ptr, .acquire);
            const state: CounterState = @enumFromInt(current);

            switch (state) {
                .unused, .reclaimed => {
                    // Try to claim: CAS current → allocated.
                    const desired = @intFromEnum(CounterState.allocated);
                    if (@cmpxchgWeak(
                        i32,
                        state_ptr,
                        current,
                        desired,
                        .acq_rel,
                        .monotonic,
                    ) == null) {
                        // CAS succeeded — we own this slot.
                        self.writeMetadata(id, type_id, label);
                        // Zero the value with release so readers see consistent state.
                        @atomicStore(i64, self.valuePtr(id), 0, .release);
                        return id;
                    }
                    // CAS failed — another thread grabbed it; continue scanning.
                },
                .allocated => {},
            }
        }
        return null;
    }

    /// Release a counter slot. Zeroes the value and marks the slot as reclaimed.
    pub fn free(self: *Self, counter_id: usize) void {
        std.debug.assert(counter_id <= self.max_counter_id);
        @atomicStore(i64, self.valuePtr(counter_id), 0, .release);
        @atomicStore(i32, self.metadataStatePtr(counter_id), @intFromEnum(CounterState.reclaimed), .release);
    }

    // ── Value Operations ──────────────────────────────────────────────

    /// Read the current value of a counter (acquire load).
    pub fn get(self: *Self, counter_id: usize) i64 {
        std.debug.assert(counter_id <= self.max_counter_id);
        return @atomicLoad(i64, self.valuePtr(counter_id), .acquire);
    }

    /// Increment a counter by 1.
    pub fn increment(self: *Self, counter_id: usize) void {
        self.add(counter_id, 1);
    }

    /// Add `delta` to a counter (monotonic fetch-add).
    pub fn add(self: *Self, counter_id: usize, delta: i64) void {
        std.debug.assert(counter_id <= self.max_counter_id);
        _ = @atomicRmw(i64, self.valuePtr(counter_id), .Add, delta, .monotonic);
    }

    /// Overwrite a counter value (release store).
    pub fn set(self: *Self, counter_id: usize, value: i64) void {
        std.debug.assert(counter_id <= self.max_counter_id);
        @atomicStore(i64, self.valuePtr(counter_id), value, .release);
    }

    // ── Queries ───────────────────────────────────────────────────────

    /// The maximum counter id that can be allocated (inclusive).
    pub fn maxCounterId(self: *const Self) usize {
        return self.max_counter_id;
    }

    /// Iterate all **allocated** counters, calling `callback` for each.
    pub fn forEach(
        self: *Self,
        callback: *const fn (id: usize, type_id: i32, label: []const u8, value: i64) void,
    ) void {
        var id: usize = 0;
        while (id <= self.max_counter_id) : (id += 1) {
            const state_raw = @atomicLoad(i32, self.metadataStatePtr(id), .acquire);
            const state: CounterState = @enumFromInt(state_raw);
            if (state == .allocated) {
                const meta = self.readMetadata(id);
                const value = @atomicLoad(i64, self.valuePtr(id), .acquire);
                callback(id, meta.type_id, meta.label, value);
            }
        }
    }

    // ── Internal Helpers ──────────────────────────────────────────────

    /// Return a pointer to the i64 value for counter `id`.
    fn valuePtr(self: *Self, id: usize) *i64 {
        const offset = id * counter_value_length;
        const byte_ptr: [*]u8 = @ptrCast(self.values_buffer.ptr + offset);
        return @ptrCast(@alignCast(byte_ptr));
    }

    /// Return a pointer to the state word (i32) at the start of the
    /// metadata slot for counter `id`.
    fn metadataStatePtr(self: *Self, id: usize) *i32 {
        const offset = id * counter_metadata_length;
        const byte_ptr: [*]u8 = @ptrCast(self.metadata_buffer.ptr + offset);
        return @ptrCast(@alignCast(byte_ptr));
    }

    /// Write type_id and label into the metadata slot for counter `id`.
    ///
    /// Layout within the 256-byte slot:
    ///   +0  state      (i32)  — already written by the CAS in `allocate`
    ///   +4  type_id    (i32)
    ///   +8  label_len  (i32)
    ///   +12 label      (up to 244 bytes)
    fn writeMetadata(self: *Self, id: usize, type_id: i32, label: []const u8) void {
        const base = id * counter_metadata_length;

        // type_id at +4
        const type_id_ptr: *i32 = @ptrCast(@alignCast(self.metadata_buffer.ptr + base + 4));
        type_id_ptr.* = type_id;

        // label_length at +8
        const clamped_len: usize = @min(label.len, max_label_length);
        const label_len_ptr: *i32 = @ptrCast(@alignCast(self.metadata_buffer.ptr + base + 8));
        label_len_ptr.* = @intCast(clamped_len);

        // label bytes at +12
        const label_dst = self.metadata_buffer[base + 12 .. base + 12 + clamped_len];
        @memcpy(label_dst, label[0..clamped_len]);
    }

    /// Read the type_id and label from the metadata slot for counter `id`.
    fn readMetadata(self: *Self, id: usize) MetadataView {
        const base = id * counter_metadata_length;

        const type_id_ptr: *const i32 = @ptrCast(@alignCast(self.metadata_buffer.ptr + base + 4));
        const label_len_ptr: *const i32 = @ptrCast(@alignCast(self.metadata_buffer.ptr + base + 8));
        const label_len: usize = @intCast(label_len_ptr.*);

        return .{
            .type_id = type_id_ptr.*,
            .label = self.metadata_buffer[base + 12 .. base + 12 + label_len],
        };
    }
};

// ── Tests ─────────────────────────────────────────────────────────────

// File-level mutable state for forEach callback (Zig fn ptrs can't capture).
var test_foreach_count: usize = 0;
fn testForEachCallback(_: usize, _: i32, _: []const u8, _: i64) void {
    test_foreach_count += 1;
}

test "allocate and increment counter" {
    var values_buf: [128 * 4]u8 align(128) = [_]u8{0} ** (128 * 4);
    var meta_buf: [256 * 4]u8 align(4) = [_]u8{0} ** (256 * 4);

    var mgr = CountersManager.init(&values_buf, &meta_buf);

    // Allocate a counter and verify the initial value is zero.
    const id = mgr.allocate(1, "test-counter") orelse unreachable;
    try std.testing.expectEqual(@as(i64, 0), mgr.get(id));

    // Increment three times.
    mgr.increment(id);
    mgr.increment(id);
    mgr.increment(id);
    try std.testing.expectEqual(@as(i64, 3), mgr.get(id));

    // Add 10.
    mgr.add(id, 10);
    try std.testing.expectEqual(@as(i64, 13), mgr.get(id));
}

test "free and reallocate counter" {
    // Buffer sized for exactly 2 counters.
    var values_buf: [128 * 2]u8 align(128) = [_]u8{0} ** (128 * 2);
    var meta_buf: [256 * 2]u8 align(4) = [_]u8{0} ** (256 * 2);

    var mgr = CountersManager.init(&values_buf, &meta_buf);

    const id0 = mgr.allocate(1, "counter-0") orelse unreachable;
    const id1 = mgr.allocate(2, "counter-1") orelse unreachable;

    // Third allocation must fail — all slots taken.
    try std.testing.expect(mgr.allocate(3, "counter-2") == null);

    // Free the first counter.
    mgr.free(id0);

    // Reallocate — should reuse the same slot.
    const id_reused = mgr.allocate(4, "counter-reused") orelse unreachable;
    try std.testing.expectEqual(id0, id_reused);

    // The other counter is still intact.
    _ = id1;
}

test "forEach iterates only allocated counters" {
    var values_buf: [128 * 4]u8 align(128) = [_]u8{0} ** (128 * 4);
    var meta_buf: [256 * 4]u8 align(4) = [_]u8{0} ** (256 * 4);

    var mgr = CountersManager.init(&values_buf, &meta_buf);

    const id0 = mgr.allocate(1, "a") orelse unreachable;
    const id1 = mgr.allocate(2, "b") orelse unreachable;
    _ = mgr.allocate(3, "c") orelse unreachable;

    // Free the middle one.
    _ = id0;
    mgr.free(id1);

    // forEach should visit only the 2 remaining allocated counters.
    test_foreach_count = 0;
    mgr.forEach(&testForEachCallback);
    try std.testing.expectEqual(@as(usize, 2), test_foreach_count);
}

test "set overwrites counter value" {
    var values_buf: [128 * 4]u8 align(128) = [_]u8{0} ** (128 * 4);
    var meta_buf: [256 * 4]u8 align(4) = [_]u8{0} ** (256 * 4);

    var mgr = CountersManager.init(&values_buf, &meta_buf);

    const id = mgr.allocate(1, "overwrite-me") orelse unreachable;
    mgr.set(id, 42);
    try std.testing.expectEqual(@as(i64, 42), mgr.get(id));
}
