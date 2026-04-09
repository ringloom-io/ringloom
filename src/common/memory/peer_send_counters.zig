//! Per-Peer Send Counters Region — shared-memory layout for per-peer
//! outbound pipeline visibility.
//!
//! This region is appended to the broker metadata file (after the flow
//! control counters region) and provides per-peer send state. Each entry
//! is 128 bytes (2 cache lines) and contains connection state, pending
//! byte counts, and diagnostic counters.
//!
//! Writer: sender thread only (single writer — release stores).
//! Reader: ServiceClient threads (acquire loads).

const std = @import("std");
const constants = @import("constants.zig");

// ── Peer Entry (128 bytes, 2 cache lines) ────────────────────────────

pub const PeerEntry = extern struct {
    /// Peer's node_id. 0 = slot unused.
    node_id: i16,
    /// Slot lifecycle: 0=free, 1=active.
    state: u8,
    /// 0=disconnected, 1=connected (volatile, written by sender).
    connection_state: u8,
    _reserved0: u32 = 0,
    /// Bytes in the send ring buffer destined for this peer.
    ring_bytes_pending: u64,
    /// Bytes currently in this peer's write queue.
    queue_bytes_pending: u64,
    /// Write queue capacity (static, set on allocation).
    queue_capacity: u64,
    /// Lifetime bytes successfully sent to this peer (monotonic).
    total_bytes_sent: u64,
    /// Lifetime bytes dropped for this peer (monotonic).
    total_bytes_dropped: u64,
    /// Monotonic timestamp of last counter update (nanoseconds).
    last_update_ns: u64,
    _reserved1: [72]u8 = [_]u8{0} ** 72,

    comptime {
        std.debug.assert(@sizeOf(PeerEntry) == 128);
    }

    pub fn loadConnectionState(self: *const volatile PeerEntry) bool {
        return @atomicLoad(u8, &self.connection_state, .acquire) == 1;
    }

    pub fn storeConnectionState(self: *volatile PeerEntry, connected: bool) void {
        @atomicStore(u8, &self.connection_state, if (connected) @as(u8, 1) else @as(u8, 0), .release);
    }

    pub fn loadRingBytesPending(self: *const volatile PeerEntry) u64 {
        return @atomicLoad(u64, &self.ring_bytes_pending, .acquire);
    }

    pub fn storeRingBytesPending(self: *volatile PeerEntry, value: u64) void {
        @atomicStore(u64, &self.ring_bytes_pending, value, .release);
    }

    pub fn loadQueueBytesPending(self: *const volatile PeerEntry) u64 {
        return @atomicLoad(u64, &self.queue_bytes_pending, .acquire);
    }

    pub fn storeQueueBytesPending(self: *volatile PeerEntry, value: u64) void {
        @atomicStore(u64, &self.queue_bytes_pending, value, .release);
    }

    pub fn loadTotalBytesSent(self: *const volatile PeerEntry) u64 {
        return @atomicLoad(u64, &self.total_bytes_sent, .acquire);
    }

    pub fn storeTotalBytesSent(self: *volatile PeerEntry, value: u64) void {
        @atomicStore(u64, &self.total_bytes_sent, value, .release);
    }

    pub fn loadTotalBytesDropped(self: *const volatile PeerEntry) u64 {
        return @atomicLoad(u64, &self.total_bytes_dropped, .acquire);
    }

    pub fn storeTotalBytesDropped(self: *volatile PeerEntry, value: u64) void {
        @atomicStore(u64, &self.total_bytes_dropped, value, .release);
    }

    pub fn loadLastUpdateNs(self: *const volatile PeerEntry) u64 {
        return @atomicLoad(u64, &self.last_update_ns, .acquire);
    }

    pub fn storeLastUpdateNs(self: *volatile PeerEntry, value: u64) void {
        @atomicStore(u64, &self.last_update_ns, value, .release);
    }
};

// ── Per-Peer Send Counters Region Header (128 bytes) ─────────────────

pub const PeerSendCountersHeader = extern struct {
    version: u32,
    entry_count: u32,
    entry_size: u32 = 128,
    _reserved: [116]u8 = [_]u8{0} ** 116,

    comptime {
        std.debug.assert(@sizeOf(PeerSendCountersHeader) == 128);
    }
};

// ── Per-Peer Send Counters Region ────────────────────────────────────

pub const PeerSendCountersRegion = struct {
    header: *volatile PeerSendCountersHeader,
    entries: [*]volatile PeerEntry,
    entry_count: u32,

    pub const peer_counters_version: u32 = 1;
    pub const header_size: usize = @sizeOf(PeerSendCountersHeader);
    pub const entry_size: usize = @sizeOf(PeerEntry);

    pub const InitError = error{
        BufferTooSmall,
        InvalidVersion,
    };

    /// Calculate the total region size for a given number of peer entries.
    pub fn regionSize(max_peers: u32) usize {
        return header_size + @as(usize, max_peers) * entry_size;
    }

    /// Initialize a new region over a byte slice (e.g. from mmap).
    pub fn initNew(buf: []u8, max_peers: u32) InitError!PeerSendCountersRegion {
        const required = regionSize(max_peers);
        if (buf.len < required) return error.BufferTooSmall;

        @memset(buf[0..required], 0);

        const header: *volatile PeerSendCountersHeader = @ptrCast(@alignCast(buf.ptr));
        header.version = peer_counters_version;
        header.entry_count = max_peers;
        header.entry_size = entry_size;

        const entries_base = buf.ptr + header_size;
        const entries: [*]volatile PeerEntry = @ptrCast(@alignCast(entries_base));

        return .{
            .header = header,
            .entries = entries,
            .entry_count = max_peers,
        };
    }

    /// Open an existing region from a byte slice.
    pub fn initExisting(buf: []u8) InitError!PeerSendCountersRegion {
        if (buf.len < header_size) return error.BufferTooSmall;

        const header: *volatile PeerSendCountersHeader = @ptrCast(@alignCast(buf.ptr));

        if (header.version != peer_counters_version) return error.InvalidVersion;

        const max_peers = header.entry_count;
        const required = regionSize(max_peers);
        if (buf.len < required) return error.BufferTooSmall;

        const entries_base = buf.ptr + header_size;
        const entries: [*]volatile PeerEntry = @ptrCast(@alignCast(entries_base));

        return .{
            .header = header,
            .entries = entries,
            .entry_count = max_peers,
        };
    }

    /// Find a peer entry by node_id. Linear scan (typical count < 16).
    pub fn findPeer(self: *const PeerSendCountersRegion, node_id: i16) ?*volatile PeerEntry {
        for (0..self.entry_count) |i| {
            const entry = &self.entries[i];
            if (entry.state == 1 and entry.node_id == node_id) {
                return entry;
            }
        }
        return null;
    }

    /// Find a peer entry by node_id, or allocate a free slot if not found.
    /// Returns null if not found and no free slots available.
    pub fn findOrAllocPeer(self: *const PeerSendCountersRegion, node_id: i16, queue_capacity: u64) ?*volatile PeerEntry {
        // First pass: look for existing.
        for (0..self.entry_count) |i| {
            const entry = &self.entries[i];
            if (entry.state == 1 and entry.node_id == node_id) {
                return entry;
            }
        }

        // Second pass: allocate a free slot.
        for (0..self.entry_count) |i| {
            const entry = &self.entries[i];
            if (entry.state == 0) {
                entry.node_id = node_id;
                entry.queue_capacity = queue_capacity;
                entry.state = 1; // active
                return entry;
            }
        }

        return null;
    }

    /// Free a peer slot (set state=0). Caller ensures no more writes in flight.
    pub fn freePeer(self: *const PeerSendCountersRegion, node_id: i16) void {
        for (0..self.entry_count) |i| {
            const entry = &self.entries[i];
            if (entry.state == 1 and entry.node_id == node_id) {
                entry.state = 0;
                entry.node_id = 0;
                return;
            }
        }
    }
};

// ── Tests ─────────────────────────────────────────────────────────────

const testing = std.testing;

test "PeerEntry has correct size" {
    try testing.expectEqual(@as(usize, 128), @sizeOf(PeerEntry));
}

test "PeerSendCountersHeader has correct size" {
    try testing.expectEqual(@as(usize, 128), @sizeOf(PeerSendCountersHeader));
}

test "regionSize calculation" {
    try testing.expectEqual(@as(usize, 128 + 8 * 128), PeerSendCountersRegion.regionSize(8));
    try testing.expectEqual(@as(usize, 128 + 32 * 128), PeerSendCountersRegion.regionSize(32));
}

test "initNew and initExisting round-trip" {
    const max_peers: u32 = 4;
    var buf: [PeerSendCountersRegion.regionSize(4)]u8 align(128) = undefined;
    _ = &buf;

    const region = try PeerSendCountersRegion.initNew(&buf, max_peers);
    try testing.expectEqual(@as(u32, 1), region.header.version);
    try testing.expectEqual(max_peers, region.entry_count);

    // All entries should be free initially.
    for (0..max_peers) |i| {
        try testing.expectEqual(@as(u8, 0), region.entries[i].state);
    }

    // Re-open as existing.
    const region2 = try PeerSendCountersRegion.initExisting(&buf);
    try testing.expectEqual(max_peers, region2.entry_count);
}

test "findOrAllocPeer allocates and finds" {
    var buf: [PeerSendCountersRegion.regionSize(4)]u8 align(128) = undefined;
    _ = &buf;
    var region = try PeerSendCountersRegion.initNew(&buf, 4);

    // Allocate peer with node_id=5.
    const entry = region.findOrAllocPeer(5, 4096).?;
    try testing.expectEqual(@as(i16, 5), entry.node_id);
    try testing.expectEqual(@as(u8, 1), entry.state);
    try testing.expectEqual(@as(u64, 4096), entry.queue_capacity);

    // Finding same peer returns same entry.
    const entry2 = region.findOrAllocPeer(5, 4096).?;
    try testing.expectEqual(@intFromPtr(entry), @intFromPtr(entry2));

    // Find different peer allocates new slot.
    const entry3 = region.findOrAllocPeer(6, 8192).?;
    try testing.expectEqual(@as(i16, 6), entry3.node_id);
    try testing.expect(@intFromPtr(entry) != @intFromPtr(entry3));
}

test "findPeer returns null for unknown peer" {
    var buf: [PeerSendCountersRegion.regionSize(4)]u8 align(128) = undefined;
    _ = &buf;
    var region = try PeerSendCountersRegion.initNew(&buf, 4);

    try testing.expect(region.findPeer(99) == null);
}

test "freePeer makes slot reusable" {
    var buf: [PeerSendCountersRegion.regionSize(2)]u8 align(128) = undefined;
    _ = &buf;
    var region = try PeerSendCountersRegion.initNew(&buf, 2);

    _ = region.findOrAllocPeer(5, 4096).?;
    _ = region.findOrAllocPeer(6, 4096).?;

    // Both slots occupied — new peer fails.
    try testing.expect(region.findOrAllocPeer(7, 4096) == null);

    // Free one.
    region.freePeer(5);
    try testing.expect(region.findPeer(5) == null);

    // Now can allocate.
    const entry = region.findOrAllocPeer(7, 4096).?;
    try testing.expectEqual(@as(i16, 7), entry.node_id);
}

test "atomic connection state operations" {
    var buf: [PeerSendCountersRegion.regionSize(1)]u8 align(128) = undefined;
    _ = &buf;
    var region = try PeerSendCountersRegion.initNew(&buf, 1);

    const entry = region.findOrAllocPeer(1, 4096).?;

    // Initially disconnected.
    try testing.expect(!entry.loadConnectionState());

    // Set connected.
    entry.storeConnectionState(true);
    try testing.expect(entry.loadConnectionState());

    // Set disconnected.
    entry.storeConnectionState(false);
    try testing.expect(!entry.loadConnectionState());
}

test "atomic pending bytes operations" {
    var buf: [PeerSendCountersRegion.regionSize(1)]u8 align(128) = undefined;
    _ = &buf;
    var region = try PeerSendCountersRegion.initNew(&buf, 1);

    const entry = region.findOrAllocPeer(1, 4096).?;

    entry.storeRingBytesPending(1024);
    try testing.expectEqual(@as(u64, 1024), entry.loadRingBytesPending());

    entry.storeQueueBytesPending(2048);
    try testing.expectEqual(@as(u64, 2048), entry.loadQueueBytesPending());

    entry.storeTotalBytesSent(100_000);
    try testing.expectEqual(@as(u64, 100_000), entry.loadTotalBytesSent());

    entry.storeTotalBytesDropped(500);
    try testing.expectEqual(@as(u64, 500), entry.loadTotalBytesDropped());
}

test "initExisting rejects wrong version" {
    var buf: [PeerSendCountersRegion.regionSize(4)]u8 align(128) = undefined;
    _ = &buf;
    _ = try PeerSendCountersRegion.initNew(&buf, 4);

    // Corrupt version.
    const header: *PeerSendCountersHeader = @ptrCast(@alignCast(&buf));
    header.version = 99;

    try testing.expectError(error.InvalidVersion, PeerSendCountersRegion.initExisting(&buf));
}
