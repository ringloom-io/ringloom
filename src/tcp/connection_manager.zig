//! TCP connection state management.
//!
//! Tracks the lifecycle of every peer TCP connection: closed → connecting →
//! handshake → connected → draining. Manages session epochs for stale
//! detection and exponential backoff for reconnection.

const std = @import("std");
const Clock = @import("brz_common").platform.Clock;
const io_engine = @import("io_engine.zig");
const frame_mod = @import("frame.zig");
const handshake_mod = @import("handshake.zig");

pub const ConnectionHandle = io_engine.ConnectionHandle;

pub const ConnectionState = enum(u8) {
    closed,
    connecting,
    handshake,
    connected,
    draining,
};

/// Per-peer connection tracking.
pub const PeerConnection = struct {
    state: ConnectionState = .closed,
    handle: ConnectionHandle = .invalid,
    node_id: u8 = 0,
    session_epoch: u64 = 0,
    last_recv_time_ns: i128 = 0,
    last_send_time_ns: i128 = 0,
    reconnect_delay_ms: u64 = reconnect_initial_delay_ms,
    reconnect_after_ns: i128 = 0,

    pub const reconnect_initial_delay_ms: u64 = 100;
    pub const reconnect_max_delay_ms: u64 = 1000;

    pub fn isConnected(self: PeerConnection) bool {
        return self.state == .connected;
    }

    pub fn resetBackoff(self: *PeerConnection) void {
        self.reconnect_delay_ms = reconnect_initial_delay_ms;
    }

    pub fn advanceBackoff(self: *PeerConnection) void {
        self.reconnect_delay_ms = @min(
            self.reconnect_delay_ms * 2,
            reconnect_max_delay_ms,
        );
    }
};

/// Manages the connection state table for all peers.
pub const ConnectionManager = struct {
    peers: []PeerConnection,
    local_node_id: u8,
    group_name_hash: u32,
    session_epoch: u64,
    allocator: std.mem.Allocator,

    pub const max_peers: u8 = 255;

    pub fn init(
        allocator: std.mem.Allocator,
        local_node_id: u8,
        group_name: []const u8,
        peer_count: u8,
    ) !ConnectionManager {
        const peers = try allocator.alloc(PeerConnection, peer_count);
        for (peers) |*p| {
            p.* = .{};
        }
        return .{
            .peers = peers,
            .local_node_id = local_node_id,
            .group_name_hash = handshake_mod.HandshakeFrame.hashGroupName(group_name),
            .session_epoch = @intCast(Clock.monotonicNanosStable()),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *ConnectionManager) void {
        self.allocator.free(self.peers);
    }

    pub fn getPeer(self: *ConnectionManager, node_id: u8) ?*PeerConnection {
        for (self.peers) |*p| {
            if (p.node_id == node_id) return p;
        }
        return null;
    }

    /// Find or create a peer entry, returning null if the table is full.
    pub fn getOrCreatePeer(self: *ConnectionManager, node_id: u8) ?*PeerConnection {
        // Try existing
        if (self.getPeer(node_id)) |p| return p;
        // Allocate from a free slot
        for (self.peers) |*p| {
            if (p.node_id == 0 and p.state == .closed) {
                p.node_id = node_id;
                return p;
            }
        }
        return null;
    }

    /// Transition a peer to the connecting state.
    pub fn startConnecting(self: *ConnectionManager, peer: *PeerConnection, handle: ConnectionHandle) void {
        _ = self;
        peer.state = .connecting;
        peer.handle = handle;
    }

    /// Transition a peer to the handshake state (connection established).
    pub fn startHandshake(self: *ConnectionManager, peer: *PeerConnection) void {
        _ = self;
        peer.state = .handshake;
    }

    /// Transition a peer to connected state (handshake completed).
    pub fn markConnected(self: *ConnectionManager, peer: *PeerConnection, now_ns: i128) void {
        _ = self;
        peer.state = .connected;
        peer.last_recv_time_ns = now_ns;
        peer.last_send_time_ns = now_ns;
        peer.resetBackoff();
    }

    /// Mark a peer as disconnected.
    pub fn markDisconnected(self: *ConnectionManager, peer: *PeerConnection) void {
        _ = self;
        peer.state = .closed;
        peer.handle = .invalid;
        peer.advanceBackoff();
    }

    /// Check if a heartbeat timeout has occurred for a connected peer.
    pub fn isHeartbeatTimedOut(self: *ConnectionManager, peer: *PeerConnection, now_ns: i128, timeout_ms: u64) bool {
        _ = self;
        if (peer.state != .connected) return false;
        const timeout_ns: i128 = @as(i128, timeout_ms) * 1_000_000;
        return (now_ns - peer.last_recv_time_ns) > timeout_ns;
    }

    /// Check if a peer is ready for reconnection.
    pub fn isReconnectReady(self: *ConnectionManager, peer: *PeerConnection, now_ns: i128) bool {
        _ = self;
        if (peer.state != .closed) return false;
        if (peer.node_id == 0) return false;
        return now_ns >= peer.reconnect_after_ns;
    }
};

// ── Tests ─────────────────────────────────────────────────────────────

const testing = std.testing;

test "PeerConnection default state is closed" {
    const pc = PeerConnection{};
    try testing.expectEqual(ConnectionState.closed, pc.state);
    try testing.expect(!pc.isConnected());
}

test "PeerConnection backoff escalation" {
    var pc = PeerConnection{};
    try testing.expectEqual(@as(u64, 100), pc.reconnect_delay_ms);

    pc.advanceBackoff();
    try testing.expectEqual(@as(u64, 200), pc.reconnect_delay_ms);

    pc.advanceBackoff();
    try testing.expectEqual(@as(u64, 400), pc.reconnect_delay_ms);

    pc.advanceBackoff();
    try testing.expectEqual(@as(u64, 800), pc.reconnect_delay_ms);

    pc.advanceBackoff();
    try testing.expectEqual(@as(u64, 1000), pc.reconnect_delay_ms); // capped

    pc.advanceBackoff();
    try testing.expectEqual(@as(u64, 1000), pc.reconnect_delay_ms); // stays capped

    pc.resetBackoff();
    try testing.expectEqual(@as(u64, 100), pc.reconnect_delay_ms);
}

test "ConnectionManager init/deinit" {
    var cm = try ConnectionManager.init(testing.allocator, 1, "test-cluster", 4);
    defer cm.deinit();

    try testing.expectEqual(@as(u8, 1), cm.local_node_id);
    try testing.expectEqual(@as(usize, 4), cm.peers.len);
}

test "ConnectionManager getOrCreatePeer" {
    var cm = try ConnectionManager.init(testing.allocator, 1, "test-cluster", 4);
    defer cm.deinit();

    const p = cm.getOrCreatePeer(2).?;
    try testing.expectEqual(@as(u8, 2), p.node_id);

    // Should return same peer for same node_id.
    const p2 = cm.getOrCreatePeer(2).?;
    try testing.expectEqual(p, p2);
}

test "ConnectionManager lifecycle" {
    var cm = try ConnectionManager.init(testing.allocator, 1, "test-cluster", 4);
    defer cm.deinit();

    const peer = cm.getOrCreatePeer(2).?;
    const handle = ConnectionHandle.fromIndex(0);

    cm.startConnecting(peer, handle);
    try testing.expectEqual(ConnectionState.connecting, peer.state);

    cm.startHandshake(peer);
    try testing.expectEqual(ConnectionState.handshake, peer.state);

    cm.markConnected(peer, 1_000_000);
    try testing.expect(peer.isConnected());
    try testing.expectEqual(@as(u64, 100), peer.reconnect_delay_ms);

    cm.markDisconnected(peer);
    try testing.expectEqual(ConnectionState.closed, peer.state);
    try testing.expectEqual(@as(u64, 200), peer.reconnect_delay_ms);
}

test "ConnectionManager heartbeat timeout" {
    var cm = try ConnectionManager.init(testing.allocator, 1, "test-cluster", 4);
    defer cm.deinit();

    const peer = cm.getOrCreatePeer(2).?;
    const now_ns: i128 = 5_000_000_000; // 5 seconds
    cm.startConnecting(peer, ConnectionHandle.fromIndex(0));
    cm.startHandshake(peer);
    cm.markConnected(peer, now_ns);

    // No timeout yet (within 2000ms window).
    try testing.expect(!cm.isHeartbeatTimedOut(peer, now_ns + 1_000_000_000, 2000));

    // Timeout after 3 seconds (> 2000ms).
    try testing.expect(cm.isHeartbeatTimedOut(peer, now_ns + 3_000_000_000, 2000));
}
