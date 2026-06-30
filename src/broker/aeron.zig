// SPDX-License-Identifier: Apache-2.0
//! Broker-side Aeron driver integration helpers.

const std = @import("std");
const ringloom_aeron = @import("ringloom_aeron");
const ringloom_common = @import("ringloom_common");
const repl_publisher = @import("topics/repl_channel.zig");

const BrokerConfig = ringloom_common.config.broker_config.BrokerConfig;
const PeerEndpoint = ringloom_common.config.broker_config.PeerEndpoint;
const memory = ringloom_common.memory;

pub const AgentInvoker = struct {
    context: *anyopaque,
    invokeFn: *const fn (context: *anyopaque) anyerror!u32,

    pub fn fromDriverInvoker(invoker: *ringloom_aeron.AgentInvoker) AgentInvoker {
        return .{
            .context = @ptrCast(invoker),
            .invokeFn = invokeDriverInvoker,
        };
    }

    pub fn invoke(self: *AgentInvoker) anyerror!u32 {
        return self.invokeFn(self.context);
    }

    fn invokeDriverInvoker(context: *anyopaque) anyerror!u32 {
        const invoker: *ringloom_aeron.AgentInvoker = @ptrCast(@alignCast(context));
        const work_count = try invoker.invoke();
        return @intCast(work_count);
    }
};

pub const AgentAssignment = struct {
    control: ?AgentInvoker = null,
    sender: ?AgentInvoker = null,
    receiver: ?AgentInvoker = null,
};

pub const BrokerAeronClientError = ringloom_aeron.Error;

/// Broker-owned Aeron client used by broker UDP publications/subscriptions.
pub const BrokerAeronClient = struct {
    client: ringloom_aeron.Client,

    pub fn open(
        directory: [:0]const u8,
    ) BrokerAeronClientError!BrokerAeronClient {
        var client = try ringloom_aeron.Client.connect(.{
            .directory = directory,
            .use_conductor_agent_invoker = false,
            .driver_timeout_ms = 5000,
        });
        errdefer client.deinit();

        return .{ .client = client };
    }

    pub fn deinit(self: *BrokerAeronClient) void {
        self.client.deinit();
        self.* = undefined;
    }
};

pub const BrokerUdpTransportError = ringloom_aeron.Error || std.mem.Allocator.Error || std.fmt.BufPrintError;
pub const PeerDiscoveryBuildError = error{TooManyPeers} || std.fmt.BufPrintError;

pub const BrokerUdpForwardResult = enum {
    forwarded,
    unknown_peer,
    not_connected,
    back_pressured,
    admin_action,
    closed,
    max_position_exceeded,
    failed,
};

const PeerPublication = struct {
    node_id: u8,
    data_channel: [:0]u8,
    admin_channel: [:0]u8,
    data_stream_id: i32,
    admin_stream_id: i32,
    data_publication: ringloom_aeron.ExclusivePublication,
    admin_publication: ringloom_aeron.ExclusivePublication,
    /// Topic replication publication (only created when topics are enabled).
    repl_channel: ?[:0]u8 = null,
    repl_stream_id: i32 = 0,
    repl_publication: ?ringloom_aeron.ExclusivePublication = null,
};

/// Broker-owned Aeron UDP data publications/subscription.
///
/// This helper borrows the broker Aeron client owned by `BrokerAeronClient` so the
/// process has one Aeron client lifecycle. It owns the pub/sub registrations and
/// closes them before the shared client is closed.
pub const BrokerUdpTransport = struct {
    allocator: std.mem.Allocator,
    data_inbound_channel: [:0]u8,
    admin_inbound_channel: [:0]u8,
    data_inbound_stream_id: i32,
    admin_inbound_stream_id: i32,
    data_inbound_subscription: ringloom_aeron.Subscription,
    admin_inbound_subscription: ringloom_aeron.Subscription,
    /// Topic replication inbound subscription (only when topics enabled).
    repl_inbound_channel: ?[:0]u8 = null,
    repl_inbound_stream_id: i32 = 0,
    repl_inbound_subscription: ?ringloom_aeron.Subscription = null,
    /// Topic IPC inbound subscription (co-located publishers, only when topics enabled).
    topic_ipc_stream_id: i32 = 0,
    topic_ipc_subscription: ?ringloom_aeron.Subscription = null,
    peers: []PeerPublication,

    pub fn open(
        allocator: std.mem.Allocator,
        config: *const BrokerConfig,
        client: *ringloom_aeron.Client,
        driver_agents: *ringloom_aeron.DriverAgents,
    ) BrokerUdpTransportError!BrokerUdpTransport {
        var channel_buf: [256]u8 = undefined;
        const inbound_uri = try ringloom_aeron.ChannelUri.udpEndpoint(
            &channel_buf,
            config.local_host,
            config.local_port,
            config.aeron_udp_term_length,
        );
        const inbound_channel = try allocator.dupeZ(u8, inbound_uri);
        errdefer allocator.free(inbound_channel);
        const admin_inbound_channel = try allocator.dupeZ(u8, inbound_uri);
        errdefer allocator.free(admin_inbound_channel);

        const inbound_stream_id = dataStreamId(config, config.node_id);
        const admin_inbound_stream_id = adminStreamId(config, config.node_id);
        var inbound_subscription = try client.addSubscription(inbound_channel, inbound_stream_id, driver_agents);
        errdefer inbound_subscription.close() catch {};
        var admin_inbound_subscription = try client.addSubscription(admin_inbound_channel, admin_inbound_stream_id, driver_agents);
        errdefer admin_inbound_subscription.close() catch {};

        // Topic replication inbound subscription (full-mesh replication path).
        const topics_enabled = config.topics.enabled;
        var repl_inbound_channel: ?[:0]u8 = null;
        errdefer if (repl_inbound_channel) |c| allocator.free(c);
        var repl_inbound_subscription: ?ringloom_aeron.Subscription = null;
        errdefer if (repl_inbound_subscription) |*s| s.close() catch {};
        const repl_inbound_stream_id = replStreamId(config, config.node_id);
        if (topics_enabled) {
            repl_inbound_channel = try allocator.dupeZ(u8, inbound_uri);
            repl_inbound_subscription = try client.addSubscription(repl_inbound_channel.?, repl_inbound_stream_id, driver_agents);
        }

        // Topic IPC inbound subscription (co-located publisher → broker).
        const topic_ipc_stream_id: i32 = if (topics_enabled)
            @as(i32, @intCast(config.topics.pub_stream_base)) + @as(i32, @intCast(config.node_id))
        else
            0;
        var topic_ipc_subscription: ?ringloom_aeron.Subscription = null;
        errdefer if (topic_ipc_subscription) |*s| s.close() catch {};
        if (topics_enabled) {
            var ipc_uri_buf: [64]u8 = undefined;
            const ipc_channel_z = try std.fmt.bufPrintZ(&ipc_uri_buf, "aeron:ipc", .{});
            topic_ipc_subscription = try client.addSubscription(ipc_channel_z, topic_ipc_stream_id, driver_agents);
        }

        const peers = try allocator.alloc(PeerPublication, config.peer_endpoints.len);
        errdefer allocator.free(peers);
        var initialized: usize = 0;
        errdefer {
            for (peers[0..initialized]) |*peer| {
                peer.data_publication.close() catch {};
                peer.admin_publication.close() catch {};
                if (peer.repl_publication) |*p| p.close() catch {};
                allocator.free(peer.data_channel);
                allocator.free(peer.admin_channel);
                if (peer.repl_channel) |c| allocator.free(c);
            }
        }

        for (config.peer_endpoints, 0..) |endpoint, i| {
            const peer_uri = try peerChannelUri(&channel_buf, endpoint, config.aeron_udp_term_length);
            const peer_channel = try allocator.dupeZ(u8, peer_uri);
            errdefer if (initialized == i) allocator.free(peer_channel);
            const peer_admin_channel = try allocator.dupeZ(u8, peer_uri);
            errdefer if (initialized == i) allocator.free(peer_admin_channel);

            const stream_id = dataStreamId(config, endpoint.node_id);
            const admin_stream_id = adminStreamId(config, endpoint.node_id);
            var publication = try client.addExclusivePublication(peer_channel, stream_id, driver_agents);
            errdefer if (initialized == i) publication.close() catch {};
            var admin_publication = try client.addExclusivePublication(peer_admin_channel, admin_stream_id, driver_agents);
            errdefer if (initialized == i) admin_publication.close() catch {};

            var peer_repl_channel: ?[:0]u8 = null;
            errdefer if (initialized == i and peer_repl_channel != null) allocator.free(peer_repl_channel.?);
            var peer_repl_publication: ?ringloom_aeron.ExclusivePublication = null;
            errdefer if (initialized == i and peer_repl_publication != null) peer_repl_publication.?.close() catch {};
            const peer_repl_stream_id = replStreamId(config, endpoint.node_id);
            if (topics_enabled) {
                peer_repl_channel = try allocator.dupeZ(u8, peer_uri);
                peer_repl_publication = try client.addExclusivePublication(peer_repl_channel.?, peer_repl_stream_id, driver_agents);
            }

            peers[i] = .{
                .node_id = endpoint.node_id,
                .data_channel = peer_channel,
                .admin_channel = peer_admin_channel,
                .data_stream_id = stream_id,
                .admin_stream_id = admin_stream_id,
                .data_publication = publication,
                .admin_publication = admin_publication,
                .repl_channel = peer_repl_channel,
                .repl_stream_id = peer_repl_stream_id,
                .repl_publication = peer_repl_publication,
            };
            initialized += 1;
        }

        return .{
            .allocator = allocator,
            .data_inbound_channel = inbound_channel,
            .admin_inbound_channel = admin_inbound_channel,
            .data_inbound_stream_id = inbound_stream_id,
            .admin_inbound_stream_id = admin_inbound_stream_id,
            .data_inbound_subscription = inbound_subscription,
            .admin_inbound_subscription = admin_inbound_subscription,
            .repl_inbound_channel = repl_inbound_channel,
            .repl_inbound_stream_id = repl_inbound_stream_id,
            .repl_inbound_subscription = repl_inbound_subscription,
            .topic_ipc_stream_id = topic_ipc_stream_id,
            .topic_ipc_subscription = topic_ipc_subscription,
            .peers = peers,
        };
    }

    pub fn deinit(self: *BrokerUdpTransport) void {
        self.data_inbound_subscription.close() catch {};
        self.admin_inbound_subscription.close() catch {};
        if (self.repl_inbound_subscription) |*s| s.close() catch {};
        if (self.topic_ipc_subscription) |*s| s.close() catch {};
        self.allocator.free(self.data_inbound_channel);
        self.allocator.free(self.admin_inbound_channel);
        if (self.repl_inbound_channel) |c| self.allocator.free(c);
        for (self.peers) |*peer| {
            peer.data_publication.close() catch {};
            peer.admin_publication.close() catch {};
            if (peer.repl_publication) |*p| p.close() catch {};
            self.allocator.free(peer.data_channel);
            self.allocator.free(peer.admin_channel);
            if (peer.repl_channel) |c| self.allocator.free(c);
        }
        self.allocator.free(self.peers);
        self.* = undefined;
    }

    pub fn subscriptionPtr(self: *BrokerUdpTransport) *ringloom_aeron.Subscription {
        return &self.data_inbound_subscription;
    }

    pub fn adminSubscriptionPtr(self: *BrokerUdpTransport) *ringloom_aeron.Subscription {
        return &self.admin_inbound_subscription;
    }

    /// Inbound topic-replication subscription, or null when topics are disabled.
    pub fn replSubscriptionPtr(self: *BrokerUdpTransport) ?*ringloom_aeron.Subscription {
        if (self.repl_inbound_subscription) |*s| return s;
        return null;
    }

    /// Inbound topic IPC subscription (co-located publishers), or null when topics are disabled.
    pub fn topicIpcSubscriptionPtr(self: *BrokerUdpTransport) ?*ringloom_aeron.Subscription {
        if (self.topic_ipc_subscription) |*s| return s;
        return null;
    }

    /// Forwards a wrapped topic-replication frame to a peer over the repl stream.
    pub fn forwardReplFrame(self: *BrokerUdpTransport, target_node_id: u8, frame: []const u8) BrokerUdpForwardResult {
        const peer = self.findPeer(target_node_id) orelse return .unknown_peer;
        if (peer.repl_publication) |*pub_ref| return forwardFrame(pub_ref, frame);
        return .not_connected;
    }

    /// True if the repl publication to `target_node_id` is connected.
    pub fn replConnected(self: *BrokerUdpTransport, target_node_id: u8) bool {
        const peer = self.findPeer(target_node_id) orelse return false;
        if (peer.repl_publication) |*pub_ref| return pub_ref.isConnected();
        return false;
    }

    pub fn forwardDataFrame(self: *BrokerUdpTransport, target_node_id: u16, frame: []const u8) BrokerUdpForwardResult {
        if (target_node_id > std.math.maxInt(u8)) return .unknown_peer;
        const peer = self.findPeer(@intCast(target_node_id)) orelse return .unknown_peer;
        return forwardFrame(&peer.data_publication, frame);
    }

    pub fn forwardAdminFrame(self: *BrokerUdpTransport, target_node_id: u8, frame: []const u8) BrokerUdpForwardResult {
        const peer = self.findPeer(target_node_id) orelse return .unknown_peer;
        return forwardFrame(&peer.admin_publication, frame);
    }

    fn forwardFrame(publication: *ringloom_aeron.ExclusivePublication, frame: []const u8) BrokerUdpForwardResult {
        if (frame.len <= publication.maxPayloadLength()) {
            switch (publication.tryClaim(frame.len)) {
                .claim => |claim_value| {
                    var claim = claim_value;
                    @memcpy(claim.bytes(), frame);
                    claim.commit() catch return .failed;
                    return .forwarded;
                },
                .not_connected => return .not_connected,
                .back_pressured => return .back_pressured,
                .admin_action => return .admin_action,
                .closed => return .closed,
                .max_position_exceeded => return .max_position_exceeded,
                .failed => return .failed,
            }
        }
        return mapOfferResult(publication.offer(frame));
    }

    fn mapOfferResult(result: ringloom_aeron.OfferResult) BrokerUdpForwardResult {
        return switch (result) {
            .position => .forwarded,
            .not_connected => .not_connected,
            .back_pressured => .back_pressured,
            .admin_action => .admin_action,
            .closed => .closed,
            .max_position_exceeded => .max_position_exceeded,
            .failed => .failed,
        };
    }

    fn findPeer(self: *BrokerUdpTransport, node_id: u8) ?*PeerPublication {
        for (self.peers) |*peer| {
            if (peer.node_id == node_id) return peer;
        }
        return null;
    }
};

pub fn dataStreamId(config: *const BrokerConfig, node_id: u8) i32 {
    return config.aeron_data_stream_base + @as(i32, @intCast(node_id));
}

pub fn adminStreamId(config: *const BrokerConfig, node_id: u8) i32 {
    return config.aeron_admin_stream_base + @as(i32, @intCast(node_id));
}

pub fn replStreamId(config: *const BrokerConfig, node_id: u8) i32 {
    return @as(i32, @intCast(config.topics.repl_stream_base)) + @as(i32, @intCast(node_id));
}

/// Adapter exposing a `BrokerUdpTransport` as a topic-replication `Publisher`
/// (the SPI the `ReplHub` uses to send wrapped ringloom-queue frames to peers).
/// The transport pointer is bound after the transport is opened.
pub const ReplPublisherAdapter = struct {
    transport: ?*BrokerUdpTransport = null,

    pub fn bind(self: *ReplPublisherAdapter, transport: *BrokerUdpTransport) void {
        self.transport = transport;
    }

    fn offerImpl(ctx: *anyopaque, target_node: u8, frame: []const u8) i64 {
        const self: *ReplPublisherAdapter = @ptrCast(@alignCast(ctx));
        const transport = self.transport orelse return -2; // not_connected
        return switch (transport.forwardReplFrame(target_node, frame)) {
            .forwarded => @as(i64, @intCast(frame.len)),
            .back_pressured => -1,
            .not_connected, .unknown_peer => -2,
            .admin_action => -3,
            .closed => -4,
            .max_position_exceeded => -5,
            .failed => -2,
        };
    }

    fn isConnectedImpl(ctx: *anyopaque, target_node: u8) bool {
        const self: *ReplPublisherAdapter = @ptrCast(@alignCast(ctx));
        const transport = self.transport orelse return false;
        return transport.replConnected(target_node);
    }

    fn isBackPressuredImpl(ctx: *anyopaque, target_node: u8) bool {
        _ = ctx;
        _ = target_node;
        return false;
    }

    pub fn publisher(self: *ReplPublisherAdapter) repl_publisher.Publisher {
        return .{
            .ctx = @ptrCast(self),
            .offerFn = offerImpl,
            .isConnectedFn = isConnectedImpl,
            .isBackPressuredFn = isBackPressuredImpl,
        };
    }
};

pub fn peerChannelUri(
    buffer: []u8,
    endpoint: PeerEndpoint,
    term_length: ?usize,
) std.fmt.BufPrintError![:0]u8 {
    return ringloom_aeron.ChannelUri.udpEndpoint(buffer, endpoint.host, endpoint.port, term_length);
}

pub fn buildPeerDiscovery(
    out: []memory.BrokerAeronPeerConfig,
    channel_buffers: [][memory.constants.max_aeron_channel_length]u8,
    config: *const BrokerConfig,
) PeerDiscoveryBuildError![]const memory.BrokerAeronPeerConfig {
    if (config.peer_endpoints.len > out.len or config.peer_endpoints.len > channel_buffers.len) {
        return error.TooManyPeers;
    }

    for (config.peer_endpoints, 0..) |endpoint, i| {
        const channel = try peerChannelUri(
            &channel_buffers[i],
            endpoint,
            config.aeron_udp_term_length,
        );
        out[i] = .{
            .node_id = endpoint.node_id,
            .data_stream_id = dataStreamId(config, endpoint.node_id),
            .data_channel = channel,
        };
    }

    return out[0..config.peer_endpoints.len];
}

/// Assigns each Aeron agent to exactly one RingLoom logical event loop.
///
/// Broker threading mode is applied later by composing logical loops into one,
/// two, or three OS threads. This keeps the Aeron topology independent from
/// RingLoom's OS-thread topology while preventing concurrent invocation of the
/// same Aeron agent.
pub fn assignAgents(agents: *ringloom_aeron.DriverAgents) AgentAssignment {
    return switch (agents.*) {
        .dedicated => |*dedicated| .{
            .control = AgentInvoker.fromDriverInvoker(&dedicated.conductor),
            .sender = AgentInvoker.fromDriverInvoker(&dedicated.sender),
            .receiver = AgentInvoker.fromDriverInvoker(&dedicated.receiver),
        },
        .shared_network => |*shared_network| .{
            .control = AgentInvoker.fromDriverInvoker(&shared_network.conductor),
            .sender = AgentInvoker.fromDriverInvoker(&shared_network.network),
        },
        .shared => |*shared| .{
            .control = AgentInvoker.fromDriverInvoker(shared),
        },
    };
}

test "fake AgentInvoker can be invoked through broker wrapper" {
    const Counter = struct {
        value: u32 = 0,

        fn invoke(context: *anyopaque) anyerror!u32 {
            const self: *@This() = @ptrCast(@alignCast(context));
            self.value += 1;
            return 1;
        }
    };

    var counter = Counter{};
    var invoker = AgentInvoker{
        .context = &counter,
        .invokeFn = Counter.invoke,
    };

    try std.testing.expectEqual(@as(u32, 1), try invoker.invoke());
    try std.testing.expectEqual(@as(u32, 1), counter.value);
}

test "buildPeerDiscovery publishes direct UDP metadata" {
    const peers = [_]PeerEndpoint{
        .{ .node_id = 2, .host = "127.0.0.2", .port = 40123 },
    };
    const config = BrokerConfig{
        .node_id = 1,
        .local_host = "127.0.0.1",
        .local_port = 40122,
        .peer_endpoints = peers[0..],
    };

    var out: [1]memory.BrokerAeronPeerConfig = undefined;
    var channel_bufs: [1][memory.constants.max_aeron_channel_length]u8 = undefined;
    const discovery = try buildPeerDiscovery(&out, &channel_bufs, &config);

    try std.testing.expectEqual(@as(usize, 1), discovery.len);
    try std.testing.expectEqual(@as(u8, 2), discovery[0].node_id);
    try std.testing.expectEqual(@as(i32, 30_002), discovery[0].data_stream_id);
    try std.testing.expectEqualStrings(
        "aeron:udp?endpoint=127.0.0.2:40123|term-length=16777216",
        discovery[0].data_channel,
    );
}

test "broker UDP stream and peer channel mapping" {
    const peers = [_]PeerEndpoint{
        .{ .node_id = 7, .host = "127.0.0.1", .port = 40456 },
    };
    const config = BrokerConfig{
        .node_id = 3,
        .local_host = "127.0.0.1",
        .local_port = 40455,
        .peer_endpoints = peers[0..],
    };

    try std.testing.expectEqual(@as(i32, 30_003), dataStreamId(&config, config.node_id));
    try std.testing.expectEqual(@as(i32, 30_007), dataStreamId(&config, peers[0].node_id));
    try std.testing.expectEqual(@as(i32, 20_003), adminStreamId(&config, config.node_id));
    try std.testing.expectEqual(@as(i32, 20_007), adminStreamId(&config, peers[0].node_id));

    var channel_buf: [128]u8 = undefined;
    try std.testing.expectEqualStrings(
        "aeron:udp?endpoint=127.0.0.1:40456|term-length=16777216",
        try peerChannelUri(&channel_buf, peers[0], config.aeron_udp_term_length),
    );
}
