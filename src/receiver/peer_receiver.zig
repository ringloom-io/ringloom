//! Per-peer receiver state for the BRZ broker receive path.
//!
//! Each connected peer broker is represented by a PeerReceiver. This struct
//! holds the per-peer receive log buffer, loss detection state, and the
//! peer's network address (for sending NAKs and SMs back).

const std = @import("std");
const constants = @import("../platform/constants.zig");
const platform = @import("../platform.zig");
const ReceiveLogBuffer = @import("../memory/receive_log.zig").ReceiveLogBuffer;
const LossDetector = @import("loss_detector.zig").LossDetector;

/// Per-peer receiver state. One instance per connected peer broker.
pub const PeerReceiver = struct {
    /// The peer's node ID. Matches the source_node_id in data frame headers.
    node_id: u8,

    /// Per-peer receive log buffer. Frames from this peer are inserted here.
    recv_log: *ReceiveLogBuffer,

    /// Loss detector state — scans recv_log for gaps and generates NAKs.
    loss_detector: LossDetector,

    /// The peer's source address. Captured from the SETUP frame or the
    /// first data frame received. Used as the destination for NAKs and SMs.
    address: std.net.Address,

    /// Timestamp (monotonic ns) of the last packet received from this peer.
    /// Used for liveness detection — if no packets arrive for a configurable
    /// timeout, the peer is considered dead.
    last_packet_received_ns: i64,

    /// The initial sequence number from the peer's SETUP frame. Used to
    /// initialize the loss detector's scan start position.
    initial_sequence: i64,

    /// Pre-allocated buffer for encoding outbound Status Messages to this peer.
    sm_buffer: [28]u8,

    /// Pre-allocated buffer for encoding outbound NAK frames to this peer.
    nak_buffer: [24]u8,

    /// Whether this peer is actively connected.
    connected: bool,

    const Self = @This();

    pub fn init(
        node_id: u8,
        recv_log: *ReceiveLogBuffer,
        address: std.net.Address,
        initial_sequence: i64,
    ) Self {
        return .{
            .node_id = node_id,
            .recv_log = recv_log,
            .loss_detector = LossDetector.init(initial_sequence),
            .address = address,
            .last_packet_received_ns = platform.Clock.monotonicNanos(),
            .initial_sequence = initial_sequence,
            .sm_buffer = [_]u8{0} ** 28,
            .nak_buffer = [_]u8{0} ** 24,
            .connected = true,
        };
    }

    /// Reset peer state for reconnection (new SETUP from same peer).
    pub fn resetForReconnect(
        self: *Self,
        address: std.net.Address,
        initial_sequence: i64,
    ) void {
        self.loss_detector = LossDetector.init(initial_sequence);
        self.address = address;
        self.last_packet_received_ns = platform.Clock.monotonicNanos();
        self.initial_sequence = initial_sequence;
        self.connected = true;
    }
};

// ── Tests ─────────────────────────────────────────────────────────────

const testing = std.testing;

test "PeerReceiver init sets correct defaults" {
    // Given
    var log = try ReceiveLogBuffer.allocate(4096);
    defer log.close();

    const address = std.net.Address.initIp4(.{ 127, 0, 0, 1 }, 9001);

    // When
    const peer = PeerReceiver.init(1, &log, address, 0);

    // Then
    try testing.expectEqual(@as(u8, 1), peer.node_id);
    try testing.expectEqual(@as(i64, 0), peer.initial_sequence);
    try testing.expect(peer.connected);
    try testing.expect(peer.last_packet_received_ns > 0);
    try testing.expect(!peer.loss_detector.has_active_gap);
}

test "PeerReceiver resetForReconnect updates state" {
    // Given
    var log = try ReceiveLogBuffer.allocate(4096);
    defer log.close();

    const address1 = std.net.Address.initIp4(.{ 127, 0, 0, 1 }, 9001);
    var peer = PeerReceiver.init(1, &log, address1, 0);

    // Simulate some activity
    peer.loss_detector.has_active_gap = true;
    peer.connected = false;

    // When — reconnect from a different address with new initial sequence
    const address2 = std.net.Address.initIp4(.{ 10, 0, 0, 1 }, 9002);
    peer.resetForReconnect(address2, 100);

    // Then
    try testing.expectEqual(@as(i64, 100), peer.initial_sequence);
    try testing.expect(peer.connected);
    try testing.expect(!peer.loss_detector.has_active_gap);
    try testing.expectEqual(@as(i64, 100), peer.loss_detector.rebuild_position);
}
