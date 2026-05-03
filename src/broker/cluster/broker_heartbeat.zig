//! Broker-to-broker heartbeat sender.
//!
//! Each broker broadcasts an admin BrokerHeartbeat (templateId = 1) to all
//! peers at a regular interval. This serves a dual purpose:
//!
//! 1. Liveness signal — the broker-agent thread monitors Node.last_heartbeat_ns
//!    to detect dead peers.
//! 2. Leadership assertion — the heartbeat carries the sender's nodeId, which is
//!    its priority for VRRP-style leader election (lowest nodeId wins).

const std = @import("std");
const Clock = @import("ringloom_common").platform.clock.Clock;
const admin = @import("admin_messages.zig");

pub const BrokerHeartbeatSender = struct {
    local_node_id: u8,
    local_host_and_port: [22]u8,
    next_heartbeat_ns: i64 = 0,
    send_buf: [64]u8 = undefined,

    /// Function pointer to broadcast a message to all peers.
    broadcast_fn: *const fn (buf: []const u8) void,

    /// Interval between broker heartbeats.
    pub const HEARTBEAT_INTERVAL_NS: i64 = 1 * std.time.ns_per_s;

    pub fn init(
        local_node_id: u8,
        local_host_and_port: [22]u8,
        broadcast_fn: *const fn (buf: []const u8) void,
    ) BrokerHeartbeatSender {
        return .{
            .local_node_id = local_node_id,
            .local_host_and_port = local_host_and_port,
            .broadcast_fn = broadcast_fn,
        };
    }

    /// Duty-cycle function: send heartbeat if interval has elapsed.
    /// Returns 1 if a heartbeat was sent, 0 otherwise.
    pub fn sendIfDue(self: *BrokerHeartbeatSender, now_ns: i64) u32 {
        if (now_ns < self.next_heartbeat_ns) return 0;

        const len = admin.encodeAdminMessage(
            &self.send_buf,
            admin.BrokerHeartbeatBody,
            admin.TEMPLATE_BROKER_HEARTBEAT,
            .{
                .node_id = self.local_node_id,
                .host_and_port = self.local_host_and_port,
            },
        );
        self.broadcast_fn(self.send_buf[0..len]);
        self.next_heartbeat_ns = now_ns + HEARTBEAT_INTERVAL_NS;
        return 1;
    }
};

// ── Tests ─────────────────────────────────────────────────────────────

const testing = std.testing;

// Test state
var test_broadcast_count: u32 = 0;
var test_last_broadcast: [64]u8 = undefined;
var test_last_broadcast_len: usize = 0;

fn testBroadcast(buf: []const u8) void {
    test_broadcast_count += 1;
    test_last_broadcast_len = @min(buf.len, test_last_broadcast.len);
    @memcpy(test_last_broadcast[0..test_last_broadcast_len], buf[0..test_last_broadcast_len]);
}

test "sendIfDue sends heartbeat when due" {
    // Given
    test_broadcast_count = 0;
    var sender = BrokerHeartbeatSender.init(
        1,
        admin.padHostPort("localhost:40456"),
        &testBroadcast,
    );

    // When — first call (next_heartbeat_ns = 0, so always due)
    const work = sender.sendIfDue(1_000_000_000);

    // Then
    try testing.expectEqual(@as(u32, 1), work);
    try testing.expectEqual(@as(u32, 1), test_broadcast_count);
    try testing.expect(test_last_broadcast_len > 0);
}

test "sendIfDue does not send before interval" {
    // Given
    test_broadcast_count = 0;
    var sender = BrokerHeartbeatSender.init(
        1,
        admin.padHostPort("localhost:40456"),
        &testBroadcast,
    );
    _ = sender.sendIfDue(1_000_000_000); // first send
    test_broadcast_count = 0;

    // When — call before interval elapsed
    const work = sender.sendIfDue(1_500_000_000); // only 0.5s later

    // Then
    try testing.expectEqual(@as(u32, 0), work);
    try testing.expectEqual(@as(u32, 0), test_broadcast_count);
}

test "sendIfDue sends again after interval" {
    // Given
    test_broadcast_count = 0;
    var sender = BrokerHeartbeatSender.init(
        1,
        admin.padHostPort("localhost:40456"),
        &testBroadcast,
    );
    _ = sender.sendIfDue(1_000_000_000); // first send
    test_broadcast_count = 0;

    // When — call after interval elapsed
    const work = sender.sendIfDue(2_000_000_001); // 1s + 1ns later

    // Then
    try testing.expectEqual(@as(u32, 1), work);
    try testing.expectEqual(@as(u32, 1), test_broadcast_count);
}

test "heartbeat encodes correct header" {
    // Given
    test_broadcast_count = 0;
    var sender = BrokerHeartbeatSender.init(
        42,
        admin.padHostPort("192.168.1.1:9090"),
        &testBroadcast,
    );

    // When
    _ = sender.sendIfDue(1_000_000_000);

    // Then — verify the encoded header
    const header = admin.decodeHeader(&test_last_broadcast).?;
    try testing.expectEqual(admin.TEMPLATE_BROKER_HEARTBEAT, header.template_id);
    try testing.expectEqual(admin.SCHEMA_ID, header.schema_id);
    try testing.expectEqual(admin.SCHEMA_VERSION, header.version);
}
