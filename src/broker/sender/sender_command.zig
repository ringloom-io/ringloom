const std = @import("std");
const udp = @import("ringloom_udp");

/// Commands sent from the control loop to the sender event loop via the command queue.
pub const SenderCommand = union(enum) {
    /// Add a new peer. The sender creates a PeerSender, opens a socket,
    /// and sends a SETUP frame.
    add_peer: struct {
        node_id: u8,
        address: udp.Address,
    },

    /// Remove a peer. The sender closes the socket, drains the write queue,
    /// and removes the PeerSender from the map.
    remove_peer: struct {
        node_id: u8,
    },

    /// Reconnect a peer. The sender resets connection state and starts
    /// the reconnect backoff timer.
    reconnect_peer: struct {
        node_id: u8,
    },
};

// ── Tests ─────────────────────────────────────────────────────────────

test "SenderCommand can represent add_peer" {
    const addr = udp.Address.initIp4(.{ 127, 0, 0, 1 }, 8080);

    const cmd = SenderCommand{ .add_peer = .{
        .node_id = 42,
        .address = addr,
    } };

    try std.testing.expectEqual(@as(u8, 42), cmd.add_peer.node_id);
    try std.testing.expectEqual(@as(u16, 8080), cmd.add_peer.address.port);
}

test "SenderCommand can represent remove_peer" {
    const cmd = SenderCommand{ .remove_peer = .{
        .node_id = 7,
    } };

    try std.testing.expectEqual(@as(u8, 7), cmd.remove_peer.node_id);
}
