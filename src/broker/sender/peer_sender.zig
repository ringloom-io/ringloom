// SPDX-License-Identifier: Apache-2.0
//! Per-peer sender state for the RingLoom broker reliable UDP send path.

const std = @import("std");
const udp = @import("ringloom_udp");

pub const ConnectionState = enum {
    disconnected,
    connected,
};

pub const PeerSender = struct {
    node_id: u8,
    address: udp.Address,
    state: ConnectionState,
    last_send_ns: i64,
    last_recv_ns: i64,
    total_bytes_sent: u64,
    total_bytes_dropped: u64,

    const Self = @This();

    pub fn init(node_id: u8, address: udp.Address, allocator: std.mem.Allocator) !Self {
        _ = allocator;
        return .{
            .node_id = node_id,
            .address = address,
            .state = .connected,
            .last_send_ns = 0,
            .last_recv_ns = 0,
            .total_bytes_sent = 0,
            .total_bytes_dropped = 0,
        };
    }

    pub fn resetForReconnect(self: *Self) void {
        self.state = .connected;
        self.last_send_ns = 0;
        self.last_recv_ns = 0;
    }

    pub fn advanceBackoff(self: *Self, now_ns: i64) void {
        _ = self;
        _ = now_ns;
    }

    pub fn resetBackoff(self: *Self) void {
        _ = self;
    }

    pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
        _ = self;
        _ = allocator;
    }
};

test "PeerSender init sets UDP defaults" {
    const address = udp.Address.initIp4(.{ 127, 0, 0, 1 }, 9001);

    var peer = try PeerSender.init(1, address, std.testing.allocator);
    defer peer.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(u8, 1), peer.node_id);
    try std.testing.expectEqual(ConnectionState.connected, peer.state);
    try std.testing.expectEqual(@as(u16, 9001), peer.address.port);
    try std.testing.expectEqual(@as(i64, 0), peer.last_send_ns);
}

test "PeerSender resetForReconnect keeps peer usable" {
    const address = udp.Address.initIp4(.{ 127, 0, 0, 1 }, 9001);

    var peer = try PeerSender.init(1, address, std.testing.allocator);
    peer.state = .disconnected;
    peer.last_send_ns = 100;

    peer.resetForReconnect();

    try std.testing.expectEqual(ConnectionState.connected, peer.state);
    try std.testing.expectEqual(@as(i64, 0), peer.last_send_ns);
}
