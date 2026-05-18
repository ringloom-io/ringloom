// SPDX-License-Identifier: Apache-2.0
//! Service-side Aeron client runtime for v2 remote outbound direct UDP.

const std = @import("std");
const ringloom_aeron = @import("ringloom_aeron");
const ringloom_common = @import("ringloom_common");

const memory = ringloom_common.memory;

pub const ServiceAeronRuntimeError = error{
    MissingAeronDiscovery,
} || ringloom_aeron.Error || std.mem.Allocator.Error;

pub const ServiceAeronRuntime = struct {
    const DirectPeerPublication = struct {
        node_id: u8,
        data_stream_id: i32,
        data_channel: [:0]u8,
        publication: ?ringloom_aeron.ExclusivePublication = null,
    };

    allocator: std.mem.Allocator,
    directory: [:0]u8,
    client: ringloom_aeron.Client,
    direct_peers: std.ArrayList(DirectPeerPublication) = .empty,

    const Self = @This();

    pub fn connect(
        allocator: std.mem.Allocator,
        discovery: *const memory.BrokerAeronDiscovery,
    ) ServiceAeronRuntimeError!Self {
        const directory = discovery.directory();
        if (directory.len == 0) {
            return error.MissingAeronDiscovery;
        }

        const owned_directory = try allocator.dupeZ(u8, directory);
        errdefer allocator.free(owned_directory);

        var client = try ringloom_aeron.Client.connect(.{
            .directory = owned_directory,
            .use_conductor_agent_invoker = true,
            .driver_timeout_ms = 5000,
        });
        errdefer client.deinit();

        var direct_peers: std.ArrayList(DirectPeerPublication) = .empty;
        errdefer {
            for (direct_peers.items) |*peer| {
                if (peer.publication) |*direct_publication| {
                    direct_publication.close() catch {};
                }
                allocator.free(peer.data_channel);
            }
            direct_peers.deinit(allocator);
        }
        for (discovery.peers()) |peer| {
            const owned_peer_channel = try allocator.dupeZ(u8, peer.dataChannel());
            direct_peers.append(allocator, .{
                .node_id = peer.node_id,
                .data_stream_id = peer.data_stream_id,
                .data_channel = owned_peer_channel,
            }) catch |err| {
                allocator.free(owned_peer_channel);
                return err;
            };
        }

        return .{
            .allocator = allocator,
            .directory = owned_directory,
            .client = client,
            .direct_peers = direct_peers,
        };
    }

    pub fn deinit(self: *Self) void {
        for (self.direct_peers.items) |*peer| {
            if (peer.publication) |*publication| {
                publication.close() catch {};
            }
            self.allocator.free(peer.data_channel);
        }
        self.direct_peers.deinit(self.allocator);
        self.client.deinit();
        self.allocator.free(self.directory);
    }

    pub fn offerToNode(self: *Self, target_node_id: i16, bytes: []const u8) ringloom_aeron.OfferResult {
        const publication = (self.directPublicationForNode(target_node_id) catch return .{ .failed = ringloom_aeron.lastError() }) orelse
            return .not_connected;
        return publication.offer(bytes);
    }

    pub fn tryClaimToNode(self: *Self, target_node_id: i16, length: usize) ringloom_aeron.ClaimResult {
        const publication = (self.directPublicationForNode(target_node_id) catch return .{ .failed = ringloom_aeron.lastError() }) orelse
            return .not_connected;
        return publication.tryClaim(length);
    }

    pub fn maxPayloadLengthForNode(self: *Self, target_node_id: i16) usize {
        const publication = (self.directPublicationForNode(target_node_id) catch return 0) orelse return 0;
        return publication.maxPayloadLength();
    }

    pub fn doWork(self: *Self) ringloom_aeron.Error!u32 {
        return @intCast(try self.client.invokeConductor());
    }

    pub fn publicationConnected(self: *Self) bool {
        for (self.direct_peers.items) |*peer| {
            if (peer.publication) |*publication| {
                if (publication.isConnected()) return true;
            }
        }
        return false;
    }

    pub fn directPeerCount(self: *const Self) usize {
        return self.direct_peers.items.len;
    }

    fn directPublicationForNode(self: *Self, target_node_id: i16) ringloom_aeron.Error!?*ringloom_aeron.ExclusivePublication {
        if (target_node_id < 0 or target_node_id > std.math.maxInt(u8)) return null;
        const node_id: u8 = @intCast(target_node_id);
        for (self.direct_peers.items) |*peer| {
            if (peer.node_id != node_id) continue;
            if (peer.publication) |*publication| return publication;
            peer.publication = try self.client.addExclusivePublication(
                peer.data_channel,
                peer.data_stream_id,
                null,
            );
            if (peer.publication) |*publication| return publication;
        }
        return null;
    }
};

const testing = std.testing;

test "ServiceAeronRuntime rejects missing discovery" {
    const discovery: memory.BrokerAeronDiscovery = .{};
    try testing.expectError(
        error.MissingAeronDiscovery,
        ServiceAeronRuntime.connect(testing.allocator, &discovery),
    );
}
