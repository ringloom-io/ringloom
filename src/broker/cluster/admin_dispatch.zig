//! Admin message dispatch — receiver-side decode → command queue → broker-agent.
//!
//! Admin messages arrive as DATA frames with the ADMIN flag (0x20). The
//! receiver event loop decodes the AdminMessageHeader, copies the payload
//! into a typed AdminCommand, and enqueues it for the broker-agent thread.

const std = @import("std");
const admin = @import("admin_messages.zig");

// ── Admin Command ─────────────────────────────────────────────────────

/// Admin command variants posted from receiver → broker-agent.
pub const AdminCommand = union(enum) {
    broker_heartbeat: struct {
        node_id: u8,
        host_and_port: [22]u8,
        received_ns: i64,
    },
    cluster_state_snapshot: struct {
        /// Snapshot payload data (after AdminMessageHeader).
        /// Fixed buffer — snapshots are bounded by max_entries × entry_size.
        data: [max_snapshot_data_len]u8,
        len: u16,
    },
    service_added: struct {
        data: [@sizeOf(admin.ServiceAddedBody)]u8,
    },
    service_removed: struct {
        data: [@sizeOf(admin.ServiceRemovedBody)]u8,
    },
    service_leader_designated: struct {
        data: [@sizeOf(admin.ServiceLeaderDesignatedBody)]u8,
    },
    peer_connected: struct {
        node_id: u8,
    },

    /// Maximum snapshot payload (body after header): 1 byte node_id +
    /// 4 bytes group header + 256 entries × 35 bytes = 8965 bytes.
    const max_snapshot_data_len: usize = 1 + @sizeOf(admin.GroupHeader) + 256 * @sizeOf(admin.SnapshotEntry);
};

// ── Admin Command Queue ───────────────────────────────────────────────

/// Bounded SPSC queue for AdminCommand.
/// The receiver event loop is the single producer; the broker-agent is the
/// single consumer. No synchronization needed beyond atomic positions.
pub fn AdminCommandQueue(comptime capacity: u32) type {
    return struct {
        buffer: [capacity]AdminCommand = undefined,
        write_pos: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
        read_pos: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),

        const Self = @This();
        const mask: u32 = capacity - 1;

        comptime {
            std.debug.assert(capacity > 0 and (capacity & (capacity - 1)) == 0); // power of two
        }

        /// Try to enqueue a command. Returns true on success.
        pub fn enqueue(self: *Self, cmd: AdminCommand) bool {
            const wp = self.write_pos.load(.acquire);
            const rp = self.read_pos.load(.acquire);
            if (wp - rp >= capacity) return false; // full

            self.buffer[wp & mask] = cmd;
            self.write_pos.store(wp + 1, .release);
            return true;
        }

        /// Try to dequeue a command. Returns null if empty.
        pub fn dequeue(self: *Self) ?AdminCommand {
            const rp = self.read_pos.load(.acquire);
            const wp = self.write_pos.load(.acquire);
            if (rp == wp) return null; // empty

            const cmd = self.buffer[rp & mask];
            self.read_pos.store(rp + 1, .release);
            return cmd;
        }

        /// Returns the number of pending commands.
        pub fn size(self: *const Self) u32 {
            const wp = self.write_pos.load(.acquire);
            const rp = self.read_pos.load(.acquire);
            return wp - rp;
        }
    };
}

// ── Dispatch Function ─────────────────────────────────────────────────

/// Called by the receiver event loop when a DATA frame has the ADMIN flag.
/// Decodes the header, copies the payload, and enqueues a typed command.
pub fn dispatchAdminMessage(
    payload: []const u8,
    cmd_queue: anytype,
    now_ns: i64,
) void {
    if (payload.len < @sizeOf(admin.AdminMessageHeader)) return;

    const header = admin.decodeHeader(payload) orelse return;
    const body = admin.bodySlice(payload);

    switch (header.template_id) {
        admin.TEMPLATE_BROKER_HEARTBEAT => {
            if (body.len < @sizeOf(admin.BrokerHeartbeatBody)) return;
            var msg_body: admin.BrokerHeartbeatBody = undefined;
            const body_bytes: *[@sizeOf(admin.BrokerHeartbeatBody)]u8 = @ptrCast(&msg_body);
            @memcpy(body_bytes, body[0..@sizeOf(admin.BrokerHeartbeatBody)]);

            _ = cmd_queue.enqueue(.{
                .broker_heartbeat = .{
                    .node_id = msg_body.node_id,
                    .host_and_port = msg_body.host_and_port,
                    .received_ns = now_ns,
                },
            });
        },
        admin.TEMPLATE_CLUSTER_STATE_SNAPSHOT => {
            var cmd = AdminCommand{
                .cluster_state_snapshot = .{
                    .data = undefined,
                    .len = @intCast(@min(body.len, AdminCommand.max_snapshot_data_len)),
                },
            };
            const copy_len = @min(body.len, AdminCommand.max_snapshot_data_len);
            @memcpy(cmd.cluster_state_snapshot.data[0..copy_len], body[0..copy_len]);
            _ = cmd_queue.enqueue(cmd);
        },
        admin.TEMPLATE_SERVICE_ADDED => {
            if (body.len < @sizeOf(admin.ServiceAddedBody)) return;
            var cmd = AdminCommand{
                .service_added = .{ .data = undefined },
            };
            @memcpy(&cmd.service_added.data, body[0..@sizeOf(admin.ServiceAddedBody)]);
            _ = cmd_queue.enqueue(cmd);
        },
        admin.TEMPLATE_SERVICE_REMOVED => {
            if (body.len < @sizeOf(admin.ServiceRemovedBody)) return;
            var cmd = AdminCommand{
                .service_removed = .{ .data = undefined },
            };
            @memcpy(&cmd.service_removed.data, body[0..@sizeOf(admin.ServiceRemovedBody)]);
            _ = cmd_queue.enqueue(cmd);
        },
        admin.TEMPLATE_SERVICE_LEADER_DESIGNATED => {
            if (body.len < @sizeOf(admin.ServiceLeaderDesignatedBody)) return;
            var cmd = AdminCommand{
                .service_leader_designated = .{ .data = undefined },
            };
            @memcpy(&cmd.service_leader_designated.data, body[0..@sizeOf(admin.ServiceLeaderDesignatedBody)]);
            _ = cmd_queue.enqueue(cmd);
        },
        else => {}, // unknown templateId — silently drop
    }
}

// ── Tests ─────────────────────────────────────────────────────────────

const testing = std.testing;

test "AdminCommandQueue enqueue and dequeue" {
    // Given
    var queue: AdminCommandQueue(4) = .{};

    // When
    const ok = queue.enqueue(.{
        .broker_heartbeat = .{
            .node_id = 1,
            .host_and_port = admin.padHostPort("localhost:40456"),
            .received_ns = 1_000_000_000,
        },
    });

    // Then
    try testing.expect(ok);
    try testing.expectEqual(@as(u32, 1), queue.size());

    const cmd = queue.dequeue().?;
    try testing.expectEqual(@as(u8, 1), cmd.broker_heartbeat.node_id);
    try testing.expectEqual(@as(u32, 0), queue.size());
}

test "AdminCommandQueue dequeue returns null when empty" {
    // Given
    var queue: AdminCommandQueue(4) = .{};

    // When / Then
    try testing.expect(queue.dequeue() == null);
}

test "AdminCommandQueue full returns false" {
    // Given
    var queue: AdminCommandQueue(2) = .{};
    _ = queue.enqueue(.{ .broker_heartbeat = .{ .node_id = 1, .host_and_port = undefined, .received_ns = 0 } });
    _ = queue.enqueue(.{ .broker_heartbeat = .{ .node_id = 2, .host_and_port = undefined, .received_ns = 0 } });

    // When
    const ok = queue.enqueue(.{ .broker_heartbeat = .{ .node_id = 3, .host_and_port = undefined, .received_ns = 0 } });

    // Then
    try testing.expect(!ok);
}

test "dispatchAdminMessage dispatches BrokerHeartbeat" {
    // Given
    var queue: AdminCommandQueue(4) = .{};
    var buf: [64]u8 = undefined;
    const len = admin.encodeAdminMessage(
        &buf,
        admin.BrokerHeartbeatBody,
        admin.TEMPLATE_BROKER_HEARTBEAT,
        admin.BrokerHeartbeatBody{
            .node_id = 5,
            .host_and_port = admin.padHostPort("host:1234"),
        },
    );

    // When
    dispatchAdminMessage(buf[0..len], &queue, 999);

    // Then
    const cmd = queue.dequeue().?;
    try testing.expectEqual(@as(u8, 5), cmd.broker_heartbeat.node_id);
    try testing.expectEqual(@as(i64, 999), cmd.broker_heartbeat.received_ns);
}

test "dispatchAdminMessage dispatches ServiceAdded" {
    // Given
    var queue: AdminCommandQueue(4) = .{};
    var buf: [64]u8 = undefined;
    const len = admin.encodeAdminMessage(
        &buf,
        admin.ServiceAddedBody,
        admin.TEMPLATE_SERVICE_ADDED,
        admin.ServiceAddedBody{
            .node_id = 2,
            .service_id = 42,
            .service_name = admin.padServiceName("pricing"),
            .leader_election_enabled = 1,
        },
    );

    // When
    dispatchAdminMessage(buf[0..len], &queue, 0);

    // Then
    const cmd = queue.dequeue().?;
    switch (cmd) {
        .service_added => {},
        else => return error.TestUnexpectedResult,
    }
}

test "dispatchAdminMessage ignores unknown templateId" {
    // Given
    var queue: AdminCommandQueue(4) = .{};
    var buf: [16]u8 = undefined;
    const header = admin.AdminMessageHeader{
        .block_length = 0,
        .template_id = 99,
        .schema_id = admin.SCHEMA_ID,
        .version = admin.SCHEMA_VERSION,
    };
    const header_bytes: *const [8]u8 = @ptrCast(&header);
    @memcpy(buf[0..8], header_bytes);

    // When
    dispatchAdminMessage(buf[0..8], &queue, 0);

    // Then
    try testing.expect(queue.dequeue() == null);
}

test "dispatchAdminMessage ignores short payload" {
    // Given
    var queue: AdminCommandQueue(4) = .{};
    const buf = [_]u8{ 0, 1, 2 };

    // When
    dispatchAdminMessage(&buf, &queue, 0);

    // Then
    try testing.expect(queue.dequeue() == null);
}
