// SPDX-License-Identifier: Apache-2.0
//! Per-peer receiver state for the RingLoom broker reliable UDP receive path.

const std = @import("std");
const udp = @import("ringloom_udp");
const Clock = @import("ringloom_common").platform.clock.Clock;

pub const LivenessState = enum {
    alive,
    suspect,
    dead,
};

pub const ReadState = struct {};

pub const PeerReceiver = struct {
    node_id: u8,
    address: udp.Address,
    session_epoch: u32,
    last_recv_ns: i64,
    liveness: LivenessState,
    connected: bool,

    const Self = @This();

    pub fn init(node_id: u8, address: udp.Address, session_epoch: u32) Self {
        return .{
            .node_id = node_id,
            .address = address,
            .session_epoch = session_epoch,
            .last_recv_ns = Clock.monotonicNanos(),
            .liveness = .alive,
            .connected = true,
        };
    }

    pub fn resetForReconnect(self: *Self, address: udp.Address, session_epoch: u32) void {
        self.address = address;
        self.session_epoch = session_epoch;
        self.last_recv_ns = Clock.monotonicNanos();
        self.liveness = .alive;
        self.connected = true;
    }

    pub fn updateLiveness(self: *Self, now_ns: i64, timeout_ns: i64) LivenessState {
        const elapsed_ns = now_ns - self.last_recv_ns;
        const suspect_threshold_ns = @divTrunc(timeout_ns * 3, 4);
        if (elapsed_ns >= timeout_ns) {
            self.liveness = .dead;
        } else if (elapsed_ns >= suspect_threshold_ns) {
            self.liveness = .suspect;
        } else {
            self.liveness = .alive;
        }
        return self.liveness;
    }

    pub fn close(self: *Self) void {
        self.connected = false;
        self.liveness = .dead;
    }
};

test "PeerReceiver init sets UDP defaults" {
    const address = udp.Address.initIp4(.{ 127, 0, 0, 1 }, 9001);
    const peer = PeerReceiver.init(1, address, 42);

    try std.testing.expectEqual(@as(u8, 1), peer.node_id);
    try std.testing.expectEqual(@as(u16, 9001), peer.address.port);
    try std.testing.expectEqual(@as(u32, 42), peer.session_epoch);
    try std.testing.expect(peer.connected);
    try std.testing.expect(peer.last_recv_ns > 0);
    try std.testing.expectEqual(LivenessState.alive, peer.liveness);
}

test "PeerReceiver updateLiveness transitions to dead" {
    const address = udp.Address.initIp4(.{ 127, 0, 0, 1 }, 9001);
    var peer = PeerReceiver.init(1, address, 42);
    peer.last_recv_ns = 0;
    try std.testing.expectEqual(LivenessState.dead, peer.updateLiveness(2_000, 1_000));
}
