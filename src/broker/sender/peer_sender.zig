const std = @import("std");
const RetransmitBuffer = @import("retransmit_buffer.zig").RetransmitBuffer;
const RetransmitHandler = @import("retransmit_handler.zig").RetransmitHandler;

/// Per-peer sender state: network address, flow-control state, per-peer
/// sequence numbering, and retransmit buffer.
pub const PeerSender = struct {
    /// The peer broker's node ID.
    node_id: u8,

    /// UDP address of the peer broker.
    address: std.net.Address,

    /// The maximum send position allowed by the receiver's flow control.
    /// Updated when a Status Message arrives.
    /// Invariant: sender must not advance send_position beyond send_limit.
    send_limit: i64,

    /// Current send position — monotonically increasing byte offset of data
    /// sent to this peer. Advances by the aligned frame size after each send.
    send_position: i64,

    /// Monotonic sequence number for frames sent to this peer.
    /// Incremented once per data frame (not per heartbeat).
    sequence_number: i64,

    /// Circular buffer of recently sent frames, for retransmission on NAK.
    retransmit_buffer: *RetransmitBuffer,

    /// Per-peer retransmit state machine (linger suppression).
    retransmit_handler: RetransmitHandler,

    /// Whether the peer has completed the SETUP / initial-SM handshake.
    connected: bool,

    /// Monotonic timestamp (ns) of the last Status Message received from this peer.
    /// Used to detect peer timeout.
    last_sm_received_ns: i64,

    /// The UDP socket file descriptor used to communicate with this peer.
    socket_fd: std.posix.fd_t,

    const Self = @This();

    pub fn init(
        node_id: u8,
        address: std.net.Address,
        socket_fd: std.posix.fd_t,
        retransmit_buffer: *RetransmitBuffer,
    ) Self {
        return .{
            .node_id = node_id,
            .address = address,
            .send_limit = 0,
            .send_position = 0,
            .sequence_number = 0,
            .retransmit_buffer = retransmit_buffer,
            .retransmit_handler = RetransmitHandler.init(),
            .connected = false,
            .last_sm_received_ns = 0,
            .socket_fd = socket_fd,
        };
    }

    /// Advance the sequence number and return the previous value (post-increment returns old).
    pub inline fn nextSequence(self: *Self) i64 {
        const seq = self.sequence_number;
        self.sequence_number += 1;
        return seq;
    }

    /// Return the current sequence number without advancing.
    /// Used for heartbeats (which do not consume a sequence number).
    pub inline fn currentSequence(self: *const Self) i64 {
        return self.sequence_number;
    }

    /// Returns true if the sender is flow-controlled (cannot send more data).
    pub inline fn isFlowControlled(self: *const Self) bool {
        return self.send_position >= self.send_limit;
    }
};

// ── Tests ─────────────────────────────────────────────────────────────

const constants = @import("brz_common").platform.constants;

test "nextSequence advances and returns old value" {
    // Given
    const allocator = std.testing.allocator;
    var rb = try RetransmitBuffer.init(64 * 1024, constants.default_mtu_length, allocator);
    defer rb.close(allocator);

    const address = std.net.Address.initIp4(.{ 127, 0, 0, 1 }, 9001);
    var peer = PeerSender.init(1, address, -1, &rb);

    // When / Then — each call returns the old value and advances
    try std.testing.expectEqual(@as(i64, 0), peer.nextSequence());
    try std.testing.expectEqual(@as(i64, 1), peer.nextSequence());
    try std.testing.expectEqual(@as(i64, 2), peer.nextSequence());

    // Then — currentSequence reflects the new value
    try std.testing.expectEqual(@as(i64, 3), peer.currentSequence());
}

test "currentSequence does not advance" {
    // Given
    const allocator = std.testing.allocator;
    var rb = try RetransmitBuffer.init(64 * 1024, constants.default_mtu_length, allocator);
    defer rb.close(allocator);

    const address = std.net.Address.initIp4(.{ 127, 0, 0, 1 }, 9001);
    var peer = PeerSender.init(1, address, -1, &rb);

    // When / Then — calling currentSequence multiple times returns the same value
    try std.testing.expectEqual(@as(i64, 0), peer.currentSequence());
    try std.testing.expectEqual(@as(i64, 0), peer.currentSequence());
}

test "isFlowControlled returns true when position >= limit" {
    // Given
    const allocator = std.testing.allocator;
    var rb = try RetransmitBuffer.init(64 * 1024, constants.default_mtu_length, allocator);
    defer rb.close(allocator);

    const address = std.net.Address.initIp4(.{ 127, 0, 0, 1 }, 9001);
    var peer = PeerSender.init(1, address, -1, &rb);

    // When — send_position == send_limit
    peer.send_position = 100;
    peer.send_limit = 100;

    // Then — flow controlled
    try std.testing.expect(peer.isFlowControlled());

    // When — send_position < send_limit
    peer.send_position = 99;
    peer.send_limit = 100;

    // Then — not flow controlled
    try std.testing.expect(!peer.isFlowControlled());

    // When — send_position > send_limit
    peer.send_position = 101;
    peer.send_limit = 100;

    // Then — flow controlled
    try std.testing.expect(peer.isFlowControlled());
}

test "init sets default values correctly" {
    // Given
    const allocator = std.testing.allocator;
    var rb = try RetransmitBuffer.init(64 * 1024, constants.default_mtu_length, allocator);
    defer rb.close(allocator);

    const address = std.net.Address.initIp4(.{ 127, 0, 0, 1 }, 9001);

    // When
    const peer = PeerSender.init(1, address, -1, &rb);

    // Then
    try std.testing.expectEqual(false, peer.connected);
    try std.testing.expectEqual(@as(i64, 0), peer.send_limit);
    try std.testing.expectEqual(@as(i64, 0), peer.send_position);
    try std.testing.expectEqual(@as(i64, 0), peer.sequence_number);
    try std.testing.expectEqual(@as(i64, 0), peer.last_sm_received_ns);
}
