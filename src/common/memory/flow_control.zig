//! Flow Control Counters Region — shared-memory layout for remote service
//! remaining-bytes counters.
//!
//! This region is appended to the broker metadata file (after the send ring
//! buffer) and provides per-remote-service flow control entries. Each entry
//! is 64 bytes (one cache line) and contains the advisory remaining capacity
//! for a remote service's messages ring buffer.
//!
//! Writer: broker control loop (single writer — no CAS needed).
//! Reader: ServiceClient threads (acquire loads).

const std = @import("std");
const constants = @import("constants.zig");

// ── Entry State ──────────────────────────────────────────────────────

pub const SlotState = enum(u8) {
    free = 0,
    allocated = 1,
    reclaimed = 2,
};

pub const PressureState = enum(u8) {
    unknown = 0,
    normal = 1,
    pressured = 2,
};

// ── Flow Control Entry (64 bytes, cache-line aligned) ────────────────

pub const FlowControlEntry = extern struct {
    /// Slot lifecycle: 0=FREE, 1=ALLOCATED, 2=RECLAIMED.
    state: u8,
    /// Coarse-grained pressure signal: 0=UNKNOWN, 1=NORMAL, 2=PRESSURED.
    pressure_state: u8,
    _reserved0: u16 = 0,
    /// Target service's ID.
    service_id: i32,
    /// Target service's host node.
    node_id: i16,
    /// Slot reuse generation counter.
    generation: u16,
    /// Target ring buffer capacity (bytes).
    capacity: u32,
    /// Last known remaining bytes (advisory, written by control loop).
    remaining_bytes: u32,
    _reserved1: u32 = 0,
    /// Monotonic timestamp of last update (nanoseconds).
    last_update_ns: i64,
    _padding: [32]u8 = [_]u8{0} ** 32,

    comptime {
        std.debug.assert(@sizeOf(FlowControlEntry) == 64);
    }

    pub fn loadState(self: *const volatile FlowControlEntry) SlotState {
        const raw = @atomicLoad(u8, &self.state, .acquire);
        return std.enums.fromInt(SlotState, raw) orelse .free;
    }

    pub fn storeState(self: *volatile FlowControlEntry, s: SlotState) void {
        @atomicStore(u8, &self.state, @intFromEnum(s), .release);
    }

    pub fn loadPressureState(self: *const volatile FlowControlEntry) PressureState {
        const raw = @atomicLoad(u8, &self.pressure_state, .acquire);
        return std.enums.fromInt(PressureState, raw) orelse .unknown;
    }

    pub fn storePressureState(self: *volatile FlowControlEntry, ps: PressureState) void {
        @atomicStore(u8, &self.pressure_state, @intFromEnum(ps), .release);
    }

    pub fn loadRemainingBytes(self: *const volatile FlowControlEntry) u32 {
        return @atomicLoad(u32, &self.remaining_bytes, .acquire);
    }

    pub fn storeRemainingBytes(self: *volatile FlowControlEntry, value: u32) void {
        @atomicStore(u32, &self.remaining_bytes, value, .release);
    }

    pub fn loadLastUpdateNs(self: *const volatile FlowControlEntry) i64 {
        return @atomicLoad(i64, &self.last_update_ns, .acquire);
    }

    pub fn storeLastUpdateNs(self: *volatile FlowControlEntry, value: i64) void {
        @atomicStore(i64, &self.last_update_ns, value, .release);
    }

    pub fn loadGeneration(self: *const volatile FlowControlEntry) u16 {
        return @atomicLoad(u16, &self.generation, .acquire);
    }

    pub fn storeGeneration(self: *volatile FlowControlEntry, value: u16) void {
        @atomicStore(u16, &self.generation, value, .release);
    }
};

// ── Flow Control Region Header (64 bytes) ────────────────────────────

pub const FlowControlHeader = extern struct {
    version: u32,
    max_entries: u32,
    _padding: [56]u8 = [_]u8{0} ** 56,

    comptime {
        std.debug.assert(@sizeOf(FlowControlHeader) == 64);
    }
};

// ── Flow Control Region ──────────────────────────────────────────────

pub const FlowControlRegion = struct {
    header: *volatile FlowControlHeader,
    entries: [*]volatile FlowControlEntry,
    max_entries: u32,

    pub const fc_version: u32 = 1;
    pub const header_size: usize = @sizeOf(FlowControlHeader);
    pub const entry_size: usize = @sizeOf(FlowControlEntry);

    pub const InitError = error{
        BufferTooSmall,
        InvalidVersion,
    };

    /// Calculate the total region size for a given number of entries.
    pub fn regionSize(max_entries: u32) usize {
        return header_size + @as(usize, max_entries) * entry_size;
    }

    /// Initialize a new flow control region over a byte slice (e.g. from mmap).
    /// Writes the header and zeroes all entries.
    pub fn initNew(buf: []u8, max_entries: u32) InitError!FlowControlRegion {
        const required = regionSize(max_entries);
        if (buf.len < required) return error.BufferTooSmall;

        @memset(buf[0..required], 0);

        const header: *volatile FlowControlHeader = @ptrCast(@alignCast(buf.ptr));
        header.version = fc_version;
        header.max_entries = max_entries;

        const entries_base = buf.ptr + header_size;
        const entries: [*]volatile FlowControlEntry = @ptrCast(@alignCast(entries_base));

        return .{
            .header = header,
            .entries = entries,
            .max_entries = max_entries,
        };
    }

    /// Open an existing flow control region from a byte slice.
    pub fn initExisting(buf: []u8) InitError!FlowControlRegion {
        if (buf.len < header_size) return error.BufferTooSmall;

        const header: *volatile FlowControlHeader = @ptrCast(@alignCast(buf.ptr));

        if (header.version != fc_version) return error.InvalidVersion;

        const max_entries = header.max_entries;
        const required = regionSize(max_entries);
        if (buf.len < required) return error.BufferTooSmall;

        const entries_base = buf.ptr + header_size;
        const entries: [*]volatile FlowControlEntry = @ptrCast(@alignCast(entries_base));

        return .{
            .header = header,
            .entries = entries,
            .max_entries = max_entries,
        };
    }

    /// Get an entry by index. Returns null if out of bounds.
    pub fn getEntry(self: *const FlowControlRegion, index: u32) ?*volatile FlowControlEntry {
        if (index >= self.max_entries) return null;
        return &self.entries[index];
    }

    pub const FindResult = struct {
        entry: *volatile FlowControlEntry,
        generation: u16,
    };

    /// Find an entry by service_id and node_id. Linear scan (typical count < 256).
    /// Returns the entry and its generation at time of lookup. The caller should
    /// validate generation to detect stale slot references after recycling.
    pub fn findByService(self: *const FlowControlRegion, service_id: i32, node_id: i16) ?FindResult {
        for (0..self.max_entries) |i| {
            const entry = &self.entries[i];
            if (entry.loadState() == .allocated and
                entry.service_id == service_id and
                entry.node_id == node_id)
            {
                const gen = entry.loadGeneration();
                // Re-validate state to detect concurrent reclaim between
                // our initial state check and the generation read.
                if (entry.loadState() == .allocated) {
                    return .{ .entry = entry, .generation = gen };
                }
            }
        }
        return null;
    }

    /// Find a FREE slot and allocate it for the given service.
    /// Sets all fields and transitions state to ALLOCATED (release store).
    /// Returns the slot index, or null if no free slots.
    pub fn allocateSlot(
        self: *const FlowControlRegion,
        service_id: i32,
        node_id: i16,
        capacity: u32,
    ) ?u32 {
        for (0..self.max_entries) |i| {
            const entry = &self.entries[i];
            if (entry.loadState() == .free) {
                // Set identifying fields before making visible.
                entry.service_id = service_id;
                entry.node_id = node_id;
                const prev_gen = entry.loadGeneration();
                entry.storeGeneration(prev_gen +% 1);
                entry.capacity = capacity;
                entry.storeRemainingBytes(capacity); // optimistic default
                entry.storePressureState(.unknown);
                entry.storeLastUpdateNs(0);
                // Release store makes all fields visible.
                entry.storeState(.allocated);
                return @intCast(i);
            }
        }
        return null;
    }

    /// Mark a slot as RECLAIMED (release store). Caller must wait a grace
    /// period before calling freeSlot().
    pub fn reclaimSlot(self: *const FlowControlRegion, index: u32) void {
        if (index >= self.max_entries) return;
        self.entries[index].storeState(.reclaimed);
    }

    /// Transition a RECLAIMED slot back to FREE for reuse.
    pub fn freeSlot(self: *const FlowControlRegion, index: u32) void {
        if (index >= self.max_entries) return;
        self.entries[index].storeState(.free);
    }
};

// ── Tests ─────────────────────────────────────────────────────────────

const testing = std.testing;

test "FlowControlEntry has correct size" {
    try testing.expectEqual(@as(usize, 64), @sizeOf(FlowControlEntry));
}

test "FlowControlHeader has correct size" {
    try testing.expectEqual(@as(usize, 64), @sizeOf(FlowControlHeader));
}

test "regionSize calculation" {
    try testing.expectEqual(@as(usize, 64 + 256 * 64), FlowControlRegion.regionSize(256));
    try testing.expectEqual(@as(usize, 64 + 0), FlowControlRegion.regionSize(0));
    try testing.expectEqual(@as(usize, 64 + 64), FlowControlRegion.regionSize(1));
}

test "initNew and initExisting round-trip" {
    const max_entries: u32 = 8;
    const size = FlowControlRegion.regionSize(max_entries);
    var buf: [FlowControlRegion.regionSize(8)]u8 align(64) = undefined;
    _ = &buf;

    var region = try FlowControlRegion.initNew(&buf, max_entries);
    try testing.expectEqual(@as(u32, 1), region.header.version);
    try testing.expectEqual(max_entries, region.max_entries);

    // All entries should be FREE initially.
    for (0..max_entries) |i| {
        const entry = region.getEntry(@intCast(i)).?;
        try testing.expectEqual(SlotState.free, entry.loadState());
    }

    // Re-open as existing.
    const region2 = try FlowControlRegion.initExisting(&buf);
    try testing.expectEqual(max_entries, region2.max_entries);
    _ = size;
}

test "allocateSlot and findByService" {
    var buf: [FlowControlRegion.regionSize(4)]u8 align(64) = undefined;
    _ = &buf;
    var region = try FlowControlRegion.initNew(&buf, 4);

    // Allocate a slot.
    const idx = region.allocateSlot(42, 2, 1024 * 1024).?;
    try testing.expectEqual(@as(u32, 0), idx);

    // Entry should be ALLOCATED with correct fields.
    const entry = region.getEntry(idx).?;
    try testing.expectEqual(SlotState.allocated, entry.loadState());
    try testing.expectEqual(@as(i32, 42), entry.service_id);
    try testing.expectEqual(@as(i16, 2), entry.node_id);
    try testing.expectEqual(@as(u32, 1024 * 1024), entry.capacity);
    try testing.expectEqual(@as(u32, 1024 * 1024), entry.loadRemainingBytes());
    try testing.expectEqual(PressureState.unknown, entry.loadPressureState());

    // findByService should return the entry with its generation.
    const result = region.findByService(42, 2).?;
    try testing.expectEqual(@as(i32, 42), result.entry.service_id);
    try testing.expectEqual(@as(u16, 1), result.generation);

    // findByService with wrong service_id returns null.
    try testing.expect(region.findByService(99, 2) == null);
}

test "reclaimSlot and freeSlot lifecycle" {
    var buf: [FlowControlRegion.regionSize(4)]u8 align(64) = undefined;
    _ = &buf;
    var region = try FlowControlRegion.initNew(&buf, 4);

    const idx = region.allocateSlot(10, 1, 4096).?;
    try testing.expectEqual(SlotState.allocated, region.getEntry(idx).?.loadState());

    region.reclaimSlot(idx);
    try testing.expectEqual(SlotState.reclaimed, region.getEntry(idx).?.loadState());

    // findByService should NOT find reclaimed entries.
    try testing.expect(region.findByService(10, 1) == null);

    region.freeSlot(idx);
    try testing.expectEqual(SlotState.free, region.getEntry(idx).?.loadState());

    // Slot should be reusable — allocating again bumps generation.
    const idx2 = region.allocateSlot(20, 3, 8192).?;
    try testing.expectEqual(idx, idx2);
    try testing.expectEqual(@as(u16, 2), region.getEntry(idx2).?.loadGeneration());
}

test "allocateSlot returns null when full" {
    var buf: [FlowControlRegion.regionSize(2)]u8 align(64) = undefined;
    _ = &buf;
    var region = try FlowControlRegion.initNew(&buf, 2);

    _ = region.allocateSlot(1, 1, 4096).?;
    _ = region.allocateSlot(2, 1, 4096).?;

    // Third allocation should fail.
    try testing.expect(region.allocateSlot(3, 1, 4096) == null);
}

test "atomic remaining_bytes updates are visible" {
    var buf: [FlowControlRegion.regionSize(1)]u8 align(64) = undefined;
    _ = &buf;
    var region = try FlowControlRegion.initNew(&buf, 1);

    const idx = region.allocateSlot(1, 1, 1024 * 1024).?;
    const entry = region.getEntry(idx).?;

    // Initial remaining = capacity.
    try testing.expectEqual(@as(u32, 1024 * 1024), entry.loadRemainingBytes());

    // Simulate control loop writing a reduced value.
    entry.storeRemainingBytes(256 * 1024);
    try testing.expectEqual(@as(u32, 256 * 1024), entry.loadRemainingBytes());

    // Simulate pressure state change.
    entry.storePressureState(.pressured);
    try testing.expectEqual(PressureState.pressured, entry.loadPressureState());
}

test "initExisting rejects wrong version" {
    var buf: [FlowControlRegion.regionSize(4)]u8 align(64) = undefined;
    _ = &buf;
    _ = try FlowControlRegion.initNew(&buf, 4);

    // Corrupt the version.
    const header: *FlowControlHeader = @ptrCast(@alignCast(&buf));
    header.version = 99;

    try testing.expectError(error.InvalidVersion, FlowControlRegion.initExisting(&buf));
}

test "initNew rejects too-small buffer" {
    var buf: [32]u8 align(64) = undefined;
    _ = &buf;
    try testing.expectError(error.BufferTooSmall, FlowControlRegion.initNew(&buf, 4));
}
