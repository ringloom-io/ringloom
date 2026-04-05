//! Cluster State Synchronization — snapshot + incremental updates.
//!
//! The cluster maintains an eventually-consistent replica of the service
//! registry on every broker. The source of truth for a service instance
//! is the broker where that service is locally registered.
//!
//! Snapshot: sent when a new peer connects (SETUP handshake completes).
//! Incremental: ServiceAdded/ServiceRemoved broadcast on the fly.

const std = @import("std");
const admin = @import("admin_messages.zig");

const log = std.log.scoped(.cluster_state);

// ── Remote Service Instance ───────────────────────────────────────────

pub const RemoteServiceInstance = struct {
    service_id: u16,
    node_id: u8,
    service_name: [32]u8,
    leader_election_enabled: bool,
};

// ── ClusterState ──────────────────────────────────────────────────────

pub const ClusterState = struct {
    /// Pre-allocated buffer for snapshot encoding (up to 4096 entries).
    snapshot_buf: [max_snapshot_buf_size]u8 = undefined,
    /// Pre-allocated buffer for incremental message encoding.
    send_buf: [128]u8 = undefined,

    const max_snapshot_entries: usize = 256;
    const max_snapshot_buf_size: usize = @sizeOf(admin.AdminMessageHeader) + 1 + @sizeOf(admin.GroupHeader) + max_snapshot_entries * @sizeOf(admin.SnapshotEntry);

    pub fn init() ClusterState {
        return .{};
    }

    // ── Snapshot encoding ────────────────────────────────────────────

    /// Encode a ClusterStateSnapshot into the provided buffer.
    /// Returns the total encoded length.
    pub fn encodeClusterStateSnapshot(
        buf: []u8,
        local_node_id: u8,
        instances: []const RemoteServiceInstance,
    ) usize {
        var offset: usize = 0;

        // Admin message header
        const header = admin.AdminMessageHeader{
            .block_length = 1, // node_id only (fixed part before group)
            .template_id = admin.TEMPLATE_CLUSTER_STATE_SNAPSHOT,
            .schema_id = admin.SCHEMA_ID,
            .version = admin.SCHEMA_VERSION,
        };
        const header_bytes: *const [@sizeOf(admin.AdminMessageHeader)]u8 = @ptrCast(&header);
        @memcpy(buf[offset..][0..@sizeOf(admin.AdminMessageHeader)], header_bytes);
        offset += @sizeOf(admin.AdminMessageHeader);

        // Fixed field: node_id
        buf[offset] = local_node_id;
        offset += 1;

        // Group header
        const group_hdr = admin.GroupHeader{
            .block_length = @intCast(@sizeOf(admin.SnapshotEntry)),
            .num_in_group = @intCast(instances.len),
        };
        const group_bytes: *const [@sizeOf(admin.GroupHeader)]u8 = @ptrCast(&group_hdr);
        @memcpy(buf[offset..][0..@sizeOf(admin.GroupHeader)], group_bytes);
        offset += @sizeOf(admin.GroupHeader);

        // Group entries
        for (instances) |inst| {
            const entry = admin.SnapshotEntry{
                .service_id = inst.service_id,
                .service_name = inst.service_name,
                .leader_election_enabled = if (inst.leader_election_enabled) 1 else 0,
            };
            const entry_bytes: *const [@sizeOf(admin.SnapshotEntry)]u8 = @ptrCast(&entry);
            @memcpy(buf[offset..][0..@sizeOf(admin.SnapshotEntry)], entry_bytes);
            offset += @sizeOf(admin.SnapshotEntry);
        }

        return offset;
    }

    /// Send our local service instances as a snapshot to all peers.
    /// Called on the broker-agent thread when a new peer connection is established.
    pub fn sendClusterStateSnapshot(
        self: *ClusterState,
        local_instances: []const RemoteServiceInstance,
        local_node_id: u8,
        send_fn: *const fn (buf: []const u8) void,
    ) void {
        if (local_instances.len == 0) return;

        const len = encodeClusterStateSnapshot(
            &self.snapshot_buf,
            local_node_id,
            local_instances,
        );
        send_fn(self.snapshot_buf[0..len]);
    }

    // ── Snapshot decoding ────────────────────────────────────────────

    /// Decode a ClusterStateSnapshot payload (after AdminMessageHeader).
    /// Returns the sender's node_id and a slice iterator over the entries.
    pub fn decodeSnapshotPayload(payload: []const u8) ?SnapshotIterator {
        if (payload.len < 1 + @sizeOf(admin.GroupHeader)) return null;

        const node_id = payload[0];
        var group_hdr: admin.GroupHeader = undefined;
        const group_bytes: *[@sizeOf(admin.GroupHeader)]u8 = @ptrCast(&group_hdr);
        @memcpy(group_bytes, payload[1..][0..@sizeOf(admin.GroupHeader)]);

        return .{
            .node_id = node_id,
            .num_entries = group_hdr.num_in_group,
            .entry_size = group_hdr.block_length,
            .data = payload[1 + @sizeOf(admin.GroupHeader) ..],
            .index = 0,
        };
    }

    pub const SnapshotIterator = struct {
        node_id: u8,
        num_entries: u16,
        entry_size: u16,
        data: []const u8,
        index: u16,

        pub fn next(self: *SnapshotIterator) ?admin.SnapshotEntry {
            if (self.index >= self.num_entries) return null;
            const offset = @as(usize, self.index) * @as(usize, self.entry_size);
            if (offset + @sizeOf(admin.SnapshotEntry) > self.data.len) return null;

            var entry: admin.SnapshotEntry = undefined;
            const entry_bytes: *[@sizeOf(admin.SnapshotEntry)]u8 = @ptrCast(&entry);
            @memcpy(entry_bytes, self.data[offset..][0..@sizeOf(admin.SnapshotEntry)]);
            self.index += 1;
            return entry;
        }
    };

    // ── Incremental: ServiceAdded ────────────────────────────────────

    /// Broadcast a ServiceAdded event to all peer brokers.
    pub fn broadcastServiceAdded(
        self: *ClusterState,
        service_id: u16,
        service_name: [32]u8,
        leader_election_enabled: bool,
        local_node_id: u8,
        broadcast_fn: *const fn (buf: []const u8) void,
    ) void {
        const len = admin.encodeAdminMessage(
            &self.send_buf,
            admin.ServiceAddedBody,
            admin.TEMPLATE_SERVICE_ADDED,
            .{
                .node_id = local_node_id,
                .service_id = service_id,
                .service_name = service_name,
                .leader_election_enabled = if (leader_election_enabled) 1 else 0,
            },
        );
        broadcast_fn(self.send_buf[0..len]);
    }

    /// Broadcast a ServiceRemoved event to all peer brokers.
    pub fn broadcastServiceRemoved(
        self: *ClusterState,
        service_id: u16,
        service_name: [32]u8,
        local_node_id: u8,
        broadcast_fn: *const fn (buf: []const u8) void,
    ) void {
        const len = admin.encodeAdminMessage(
            &self.send_buf,
            admin.ServiceRemovedBody,
            admin.TEMPLATE_SERVICE_REMOVED,
            .{
                .node_id = local_node_id,
                .service_id = service_id,
                .service_name = service_name,
            },
        );
        broadcast_fn(self.send_buf[0..len]);
    }

    /// Broadcast ServiceLeaderDesignated to all peer brokers.
    /// Called only when this broker is the cluster leader.
    pub fn broadcastServiceLeaderDesignated(
        self: *ClusterState,
        service_id: u16,
        service_name: [32]u8,
        node_id: u8,
        broadcast_fn: *const fn (buf: []const u8) void,
    ) void {
        const len = admin.encodeAdminMessage(
            &self.send_buf,
            admin.ServiceLeaderDesignatedBody,
            admin.TEMPLATE_SERVICE_LEADER_DESIGNATED,
            .{
                .node_id = node_id,
                .service_id = service_id,
                .service_name = service_name,
            },
        );
        broadcast_fn(self.send_buf[0..len]);
    }
};

// ── Tests ─────────────────────────────────────────────────────────────

const testing = std.testing;

// Test broadcast state
var test_broadcast_count: u32 = 0;
var test_last_broadcast: [4096]u8 = undefined;
var test_last_broadcast_len: usize = 0;

fn testBroadcast(buf: []const u8) void {
    test_broadcast_count += 1;
    test_last_broadcast_len = @min(buf.len, test_last_broadcast.len);
    @memcpy(test_last_broadcast[0..test_last_broadcast_len], buf[0..test_last_broadcast_len]);
}

test "cluster state snapshot encode and decode roundtrip" {
    // Given: two local service instances
    const instances = [_]RemoteServiceInstance{
        .{ .service_id = 1, .node_id = 1, .service_name = admin.padServiceName("pricing"), .leader_election_enabled = true },
        .{ .service_id = 2, .node_id = 1, .service_name = admin.padServiceName("orders"), .leader_election_enabled = false },
    };

    var buf: [4096]u8 = undefined;
    const len = ClusterState.encodeClusterStateSnapshot(&buf, 1, &instances);

    // When: decode (skip AdminMessageHeader)
    const payload = buf[@sizeOf(admin.AdminMessageHeader)..len];
    var iter = ClusterState.decodeSnapshotPayload(payload).?;

    // Then: matches input
    try testing.expectEqual(@as(u8, 1), iter.node_id);
    try testing.expectEqual(@as(u16, 2), iter.num_entries);

    const entry1 = iter.next().?;
    try testing.expectEqual(@as(u16, 1), entry1.service_id);
    try testing.expectEqualStrings("pricing", admin.trimServiceName(&entry1.service_name));
    try testing.expectEqual(@as(u8, 1), entry1.leader_election_enabled);

    const entry2 = iter.next().?;
    try testing.expectEqual(@as(u16, 2), entry2.service_id);
    try testing.expectEqualStrings("orders", admin.trimServiceName(&entry2.service_name));
    try testing.expectEqual(@as(u8, 0), entry2.leader_election_enabled);

    try testing.expect(iter.next() == null);
}

test "empty snapshot is not sent" {
    // Given
    test_broadcast_count = 0;
    var state = ClusterState.init();
    const instances = [_]RemoteServiceInstance{};

    // When
    state.sendClusterStateSnapshot(&instances, 1, &testBroadcast);

    // Then
    try testing.expectEqual(@as(u32, 0), test_broadcast_count);
}

test "broadcastServiceAdded encodes correctly" {
    // Given
    test_broadcast_count = 0;
    var state = ClusterState.init();

    // When
    state.broadcastServiceAdded(
        42,
        admin.padServiceName("my-service"),
        true,
        1,
        &testBroadcast,
    );

    // Then
    try testing.expectEqual(@as(u32, 1), test_broadcast_count);
    const header = admin.decodeHeader(&test_last_broadcast).?;
    try testing.expectEqual(admin.TEMPLATE_SERVICE_ADDED, header.template_id);
}

test "broadcastServiceRemoved encodes correctly" {
    // Given
    test_broadcast_count = 0;
    var state = ClusterState.init();

    // When
    state.broadcastServiceRemoved(
        42,
        admin.padServiceName("my-service"),
        1,
        &testBroadcast,
    );

    // Then
    try testing.expectEqual(@as(u32, 1), test_broadcast_count);
    const header = admin.decodeHeader(&test_last_broadcast).?;
    try testing.expectEqual(admin.TEMPLATE_SERVICE_REMOVED, header.template_id);
}

test "broadcastServiceLeaderDesignated encodes correctly" {
    // Given
    test_broadcast_count = 0;
    var state = ClusterState.init();

    // When
    state.broadcastServiceLeaderDesignated(
        42,
        admin.padServiceName("pricing"),
        2,
        &testBroadcast,
    );

    // Then
    try testing.expectEqual(@as(u32, 1), test_broadcast_count);
    const header = admin.decodeHeader(&test_last_broadcast).?;
    try testing.expectEqual(admin.TEMPLATE_SERVICE_LEADER_DESIGNATED, header.template_id);
}

test "sendClusterStateSnapshot sends encoded data" {
    // Given
    test_broadcast_count = 0;
    var state = ClusterState.init();
    const instances = [_]RemoteServiceInstance{
        .{ .service_id = 1, .node_id = 1, .service_name = admin.padServiceName("svc1"), .leader_election_enabled = false },
    };

    // When
    state.sendClusterStateSnapshot(&instances, 1, &testBroadcast);

    // Then
    try testing.expectEqual(@as(u32, 1), test_broadcast_count);
    try testing.expect(test_last_broadcast_len > @sizeOf(admin.AdminMessageHeader));
}
