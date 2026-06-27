//! Receiver Event Loop — Aeron UDP receive and final delivery.
//!
//! V2 has no TCP listener, TCP frame parser, or broker-owned socket network path.
//! Aeron owns network I/O; this loop polls inbound Aeron UDP fragments and routes
//! complete RingLoom data/admin frames.

const std = @import("std");
const ringloom_common = @import("ringloom_common");
const ringloom_aeron = @import("ringloom_aeron");

const constants = ringloom_common.platform.constants;
const Clock = ringloom_common.platform.clock.Clock;
const AtomicBool = ringloom_common.platform.atomic.AtomicBool;
const CountersManager = ringloom_common.concurrent.counters.CountersManager;
const data_header = ringloom_common.message.data_header;
const latency_trace = ringloom_common.message.latency_trace;

const broker_aeron = @import("../aeron.zig");
const message_router = @import("message_router.zig");
const ServiceRegistry = message_router.ServiceRegistry;
const admin_dispatch = @import("../cluster/admin_dispatch.zig");
const admin_messages = @import("../cluster/admin_messages.zig");
const AdminCommandQueue = admin_dispatch.AdminCommandQueue;
const topics = @import("../topics.zig");
const topic_data_header = ringloom_common.message.topic_data_header;

const log = std.log.scoped(.receiver);

pub const LivenessState = enum {
    alive,
    suspect,
    dead,
};

pub const PeerLiveness = struct {
    node_id: u8,
    last_recv_ns: i64,
    liveness: LivenessState,

    pub fn init(node_id: u8, now_ns: i64) PeerLiveness {
        return .{
            .node_id = node_id,
            .last_recv_ns = now_ns,
            .liveness = .alive,
        };
    }

    pub fn markAlive(self: *PeerLiveness, now_ns: i64) void {
        self.last_recv_ns = now_ns;
        self.liveness = .alive;
    }

    pub fn update(self: *PeerLiveness, now_ns: i64) LivenessState {
        const elapsed_ns = now_ns - self.last_recv_ns;
        const suspect_threshold_ns: i64 = 1500 * std.time.ns_per_ms;
        const dead_threshold_ns: i64 = 3 * std.time.ns_per_s;

        if (elapsed_ns >= dead_threshold_ns) {
            self.liveness = .dead;
        } else if (elapsed_ns >= suspect_threshold_ns) {
            self.liveness = .suspect;
        } else {
            self.liveness = .alive;
        }
        return self.liveness;
    }
};

pub const ReceiverCounters = struct {
    bytes_received: usize = 0,
    frames_routed: usize = 0,
    unknown_service_drops: usize = 0,
    service_full_drops: usize = 0,
    invalid_frame_drops: usize = 0,
    unknown_peer_drops: usize = 0,
    connection_errors: usize = 0,
    heartbeat_timeouts: usize = 0,
    admin_messages_received: usize = 0,
    admin_message_errors: usize = 0,
    udp_misdirected_drops: usize = 0,
    admin_udp_invalid_frames: usize = 0,
    admin_udp_misdirected_drops: usize = 0,

    pub fn allocate(counters: *CountersManager) ReceiverCounters {
        return .{
            .bytes_received = counters.allocate(1, "recv_bytes_received") orelse 0,
            .frames_routed = counters.allocate(1, "recv_frames_routed") orelse 0,
            .unknown_service_drops = counters.allocate(1, "recv_unknown_service_drops") orelse 0,
            .service_full_drops = counters.allocate(1, "recv_service_full_drops") orelse 0,
            .invalid_frame_drops = counters.allocate(1, "recv_invalid_frame_drops") orelse 0,
            .unknown_peer_drops = counters.allocate(1, "recv_unknown_peer_drops") orelse 0,
            .connection_errors = counters.allocate(1, "recv_connection_errors") orelse 0,
            .heartbeat_timeouts = counters.allocate(1, "recv_heartbeat_timeouts") orelse 0,
            .admin_messages_received = counters.allocate(1, "recv_admin_messages_received") orelse 0,
            .admin_message_errors = counters.allocate(1, "recv_admin_message_errors") orelse 0,
            .udp_misdirected_drops = counters.allocate(1, "recv_udp_misdirected_drops") orelse 0,
            .admin_udp_invalid_frames = counters.allocate(1, "recv_admin_udp_invalid_frames") orelse 0,
            .admin_udp_misdirected_drops = counters.allocate(1, "recv_admin_udp_misdirected_drops") orelse 0,
        };
    }
};

pub const ReceiverAeronOptions = struct {
    broker_udp_transport: ?*broker_aeron.BrokerUdpTransport = null,
    max_data_payload_length: usize = constants.default_max_frame_length,
    peer_node_ids: []const u8 = &.{},
};

pub const ReceiverEventLoop = struct {
    peers: [256]?PeerLiveness,
    service_registry: *ServiceRegistry,
    counters: *CountersManager,
    counter_ids: ReceiverCounters,
    local_node_id: u8,
    running: AtomicBool,
    admin_cmd_queue: ?*AdminCommandQueue(64),
    benchmark_latency_tracing_enabled: bool,
    aeron_agent: ?broker_aeron.AgentInvoker,
    broker_udp_transport: ?*broker_aeron.BrokerUdpTransport,
    broker_udp_assembler: ringloom_aeron.FragmentAssembler,
    broker_admin_udp_assembler: ringloom_aeron.FragmentAssembler,
    broker_repl_udp_assembler: ringloom_aeron.FragmentAssembler,
    broker_topic_ipc_assembler: ringloom_aeron.FragmentAssembler,
    /// Persistent topics subsystem (null when topics are disabled).
    topic_subsystem: ?*topics.TopicSubsystem = null,
    max_data_payload_length: usize,

    const Self = @This();

    pub fn init(
        service_registry: *ServiceRegistry,
        counters: *CountersManager,
        local_node_id: u8,
        allocator: std.mem.Allocator,
    ) Self {
        return initWithGroup(service_registry, counters, local_node_id, allocator, "ringloom", null, false);
    }

    pub fn initWithGroup(
        service_registry: *ServiceRegistry,
        counters: *CountersManager,
        local_node_id: u8,
        allocator: std.mem.Allocator,
        group_name: []const u8,
        admin_queue: ?*AdminCommandQueue(64),
        benchmark_latency_tracing_enabled: bool,
    ) Self {
        _ = allocator;
        _ = group_name;
        return initWithAeron(
            service_registry,
            counters,
            local_node_id,
            admin_queue,
            benchmark_latency_tracing_enabled,
            .{},
        );
    }

    pub fn initWithAeron(
        service_registry: *ServiceRegistry,
        counters: *CountersManager,
        local_node_id: u8,
        admin_queue: ?*AdminCommandQueue(64),
        benchmark_latency_tracing_enabled: bool,
        aeron_options: ReceiverAeronOptions,
    ) Self {
        var self = Self{
            .peers = [_]?PeerLiveness{null} ** 256,
            .service_registry = service_registry,
            .counters = counters,
            .counter_ids = ReceiverCounters.allocate(counters),
            .local_node_id = local_node_id,
            .running = AtomicBool.init(true),
            .admin_cmd_queue = admin_queue,
            .benchmark_latency_tracing_enabled = benchmark_latency_tracing_enabled,
            .aeron_agent = null,
            .broker_udp_transport = aeron_options.broker_udp_transport,
            .broker_udp_assembler = ringloom_aeron.FragmentAssembler.init(.{
                .context = null,
                .callback = onBrokerUdpFragment,
            }),
            .broker_admin_udp_assembler = ringloom_aeron.FragmentAssembler.init(.{
                .context = null,
                .callback = onBrokerAdminUdpFragment,
            }),
            .broker_repl_udp_assembler = ringloom_aeron.FragmentAssembler.init(.{
                .context = null,
                .callback = onBrokerReplUdpFragment,
            }),
            .broker_topic_ipc_assembler = ringloom_aeron.FragmentAssembler.init(.{
                .context = null,
                .callback = onBrokerTopicIpcFragment,
            }),
            .max_data_payload_length = aeron_options.max_data_payload_length,
        };
        const now_ns = Clock.monotonicNanos();
        for (aeron_options.peer_node_ids) |node_id| {
            if (node_id != local_node_id) {
                self.peers[node_id] = PeerLiveness.init(node_id, now_ns);
            }
        }
        return self;
    }

    pub fn setAeronAgent(self: *Self, agent: ?broker_aeron.AgentInvoker) void {
        self.aeron_agent = agent;
    }

    pub fn setTopicSubsystem(self: *Self, subsystem: ?*topics.TopicSubsystem) void {
        self.topic_subsystem = subsystem;
    }

    pub fn deinit(self: *Self) void {
        self.broker_udp_assembler.deinit();
        self.broker_admin_udp_assembler.deinit();
        self.broker_repl_udp_assembler.deinit();
        self.broker_topic_ipc_assembler.deinit();
    }

    pub fn doWork(self: *Self) u32 {
        var work_count: u32 = 0;
        const now_ns = Clock.monotonicNanos();

        work_count += self.invokeAeronAgent();
        work_count += self.pollBrokerUdp();
        work_count += self.pollBrokerAdminUdp();
        work_count += self.pollBrokerReplUdp();
        work_count += self.pollBrokerTopicIpc();
        if (self.topic_subsystem) |ts| work_count += ts.receiverStep();
        work_count += self.checkHeartbeatTimeouts(now_ns);

        return work_count;
    }

    fn invokeAeronAgent(self: *Self) u32 {
        if (self.aeron_agent) |*agent| {
            return agent.invoke() catch |err| {
                log.err("aeron receiver invocation failed: {}", .{err});
                return 0;
            };
        }
        return 0;
    }

    fn pollBrokerUdp(self: *Self) u32 {
        const transport_ref = self.broker_udp_transport orelse return 0;
        self.broker_udp_assembler.handler.context = self;
        const fragments = self.broker_udp_assembler.poll(transport_ref.subscriptionPtr(), constants.aeron_fragment_read_limit) catch |err| {
            log.err("broker Aeron UDP poll failed: {}", .{err});
            self.counters.increment(self.counter_ids.connection_errors);
            return 0;
        };
        return @intCast(fragments);
    }

    fn pollBrokerAdminUdp(self: *Self) u32 {
        const transport_ref = self.broker_udp_transport orelse return 0;
        self.broker_admin_udp_assembler.handler.context = self;
        const fragments = self.broker_admin_udp_assembler.poll(transport_ref.adminSubscriptionPtr(), constants.aeron_fragment_read_limit) catch |err| {
            log.err("broker Aeron admin UDP poll failed: {}", .{err});
            self.counters.increment(self.counter_ids.connection_errors);
            return 0;
        };
        return @intCast(fragments);
    }

    fn pollBrokerReplUdp(self: *Self) u32 {
        if (self.topic_subsystem == null) return 0;
        const transport_ref = self.broker_udp_transport orelse return 0;
        const repl_sub = transport_ref.replSubscriptionPtr() orelse return 0;
        self.broker_repl_udp_assembler.handler.context = self;
        const fragments = self.broker_repl_udp_assembler.poll(repl_sub, constants.aeron_fragment_read_limit) catch |err| {
            log.err("broker Aeron repl UDP poll failed: {}", .{err});
            self.counters.increment(self.counter_ids.connection_errors);
            return 0;
        };
        return @intCast(fragments);
    }

    fn pollBrokerTopicIpc(self: *Self) u32 {
        if (self.topic_subsystem == null) return 0;
        const transport_ref = self.broker_udp_transport orelse return 0;
        const ipc_sub = transport_ref.topicIpcSubscriptionPtr() orelse return 0;
        self.broker_topic_ipc_assembler.handler.context = self;
        const fragments = self.broker_topic_ipc_assembler.poll(ipc_sub, constants.aeron_fragment_read_limit) catch |err| {
            log.err("broker Aeron topic IPC poll failed: {}", .{err});
            self.counters.increment(self.counter_ids.connection_errors);
            return 0;
        };
        return @intCast(fragments);
    }

    fn onBrokerUdpFragment(context: ?*anyopaque, bytes: []const u8) void {
        const self: *Self = @ptrCast(@alignCast(context.?));
        self.processBrokerUdpFragment(bytes);
    }

    fn onBrokerAdminUdpFragment(context: ?*anyopaque, bytes: []const u8) void {
        const self: *Self = @ptrCast(@alignCast(context.?));
        self.processBrokerAdminUdpFragment(bytes);
    }

    fn onBrokerReplUdpFragment(context: ?*anyopaque, bytes: []const u8) void {
        const self: *Self = @ptrCast(@alignCast(context.?));
        self.processBrokerReplUdpFragment(bytes);
    }

    fn onBrokerTopicIpcFragment(context: ?*anyopaque, bytes: []const u8) void {
        const self: *Self = @ptrCast(@alignCast(context.?));
        self.processBrokerTopicIpcFragment(bytes);
    }

    fn processBrokerReplUdpFragment(self: *Self, bytes: []const u8) void {
        self.counters.add(self.counter_ids.bytes_received, @intCast(bytes.len));
        const ts = self.topic_subsystem orelse return;
        const env = topic_data_header.TopicReplEnvelope.decode(bytes) catch {
            self.counters.increment(self.counter_ids.invalid_frame_drops);
            return;
        };
        self.refreshPeerLiveness(@intCast(env.source_node_id));
        ts.onReplFrame(env, env.frameSlice(bytes));
    }

    fn processBrokerTopicIpcFragment(self: *Self, bytes: []const u8) void {
        self.counters.add(self.counter_ids.bytes_received, @intCast(bytes.len));

        const frame = data_header.decodeFrame(bytes, self.max_data_payload_length) catch |err| {
            log.warn("dropping invalid broker topic IPC frame: {}", .{err});
            self.counters.increment(self.counter_ids.invalid_frame_drops);
            return;
        };

        // IPC frames from co-located publishers always target this broker.
        // Skip the target_node_id check and peer liveness refresh (local IPC).
        if (frame.header.flags & constants.flag_topic != 0) {
            self.routeTopicPublish(frame);
            return;
        }
    }

    fn processBrokerUdpFragment(self: *Self, bytes: []const u8) void {
        self.counters.add(self.counter_ids.bytes_received, @intCast(bytes.len));

        const frame = data_header.decodeFrame(bytes, self.max_data_payload_length) catch |err| {
            log.warn("dropping invalid broker UDP data frame: {}", .{err});
            self.counters.increment(self.counter_ids.invalid_frame_drops);
            return;
        };

        if (frame.header.target_node_id != self.local_node_id) {
            self.counters.increment(self.counter_ids.udp_misdirected_drops);
            return;
        }

        self.refreshPeerLiveness(@intCast(frame.header.source_node_id));

        // Topic publish frames (flag_topic, target_service_id == 0) are demuxed
        // to the topics engine instead of service routing (spec 04).
        if (frame.header.flags & constants.flag_topic != 0) {
            self.routeTopicPublish(frame);
            return;
        }

        self.routeDataFrameToService(frame);
    }

    fn routeTopicPublish(self: *Self, frame: data_header.DecodedFrame) void {
        const ts = self.topic_subsystem orelse return;
        const pub_hdr = topic_data_header.TopicPublishHeader.decode(frame.payload) catch {
            self.counters.increment(self.counter_ids.invalid_frame_drops);
            return;
        };
        const payload = frame.payload[topic_data_header.TopicPublishHeader.encoded_length..];
        // Benchmark-only stage tracing (zero-copy, gated): stamp the broker-A
        // receiver-ingress time into the app payload. This rides byte-exact
        // through replication into the replica queue, so the subscriber can
        // split end-to-end latency into pub→broker-A vs. broker-A→sub.
        if (self.benchmark_latency_tracing_enabled) {
            latency_trace.stampReceiverIngress(@constCast(payload), @intCast(Clock.monotonicNanosStable()));
        }
        ts.onPublish(.{
            .topic_id = pub_hdr.topic_id,
            .leader_epoch = pub_hdr.leader_epoch,
            .correlation_id = pub_hdr.correlation_id,
            .source_node = @intCast(frame.header.source_node_id),
            .source_service = @intCast(frame.header.source_service_id),
            .ack_mode = topics.AckMode.fromU8(pub_hdr.ack_mode) orelse .fire_and_forget,
            .payload = payload,
        });
    }

    fn routeDataFrameToService(self: *Self, frame: data_header.DecodedFrame) void {
        if (self.benchmark_latency_tracing_enabled) {
            latency_trace.stampReceiverIngress(@constCast(frame.payload), @intCast(Clock.monotonicNanosStable()));
        }
        const result = message_router.routeDataToService(
            self.service_registry,
            frame.header,
            frame.payload,
        );
        switch (result) {
            .success => self.counters.increment(self.counter_ids.frames_routed),
            .unknown_service => self.counters.increment(self.counter_ids.unknown_service_drops),
            .service_full => self.counters.increment(self.counter_ids.service_full_drops),
        }
    }

    pub fn processBrokerAdminUdpFragment(self: *Self, bytes: []const u8) void {
        self.counters.add(self.counter_ids.bytes_received, @intCast(bytes.len));

        const frame = admin_messages.decodeAeronAdminFrame(bytes, self.local_node_id) catch |err| {
            switch (err) {
                error.TargetNodeMismatch => self.counters.increment(self.counter_ids.admin_udp_misdirected_drops),
                else => self.counters.increment(self.counter_ids.admin_udp_invalid_frames),
            }
            return;
        };

        self.refreshPeerLiveness(frame.header.source_node_id);

        const queue = self.admin_cmd_queue orelse {
            self.counters.increment(self.counter_ids.admin_message_errors);
            return;
        };

        admin_dispatch.dispatchAdminMessage(
            frame.payload,
            queue,
            Clock.monotonicNanos(),
            frame.header.source_node_id,
        );
        self.counters.increment(self.counter_ids.admin_messages_received);
    }

    fn refreshPeerLiveness(self: *Self, source_node_id: u8) void {
        if (source_node_id == self.local_node_id) return;
        const now_ns = Clock.monotonicNanos();
        if (self.peers[source_node_id]) |*peer| {
            peer.markAlive(now_ns);
        } else {
            self.peers[source_node_id] = PeerLiveness.init(source_node_id, now_ns);
        }
    }

    fn checkHeartbeatTimeouts(self: *Self, now_ns: i64) u32 {
        var work_count: u32 = 0;
        for (&self.peers) |*slot| {
            if (slot.*) |*peer| {
                const prev_liveness = peer.liveness;
                const new_liveness = peer.update(now_ns);
                if (new_liveness == .dead and prev_liveness != .dead) {
                    self.counters.increment(self.counter_ids.heartbeat_timeouts);
                    work_count += 1;
                }
            }
        }
        return work_count;
    }

    pub fn addPeer(self: *Self, node_id: u8) void {
        if (node_id == self.local_node_id) return;
        self.peers[node_id] = PeerLiveness.init(node_id, Clock.monotonicNanos());
    }

    pub fn removePeer(self: *Self, node_id: u8) void {
        self.peers[node_id] = null;
    }

    pub fn lookupPeer(self: *Self, node_id: u8) ?*PeerLiveness {
        if (self.peers[node_id]) |*peer| return peer;
        return null;
    }
};

const testing = std.testing;

fn createTestCounters(
    values_buf: *align(128) [128 * 64]u8,
    meta_buf: *[256 * 64]u8,
) CountersManager {
    return CountersManager.init(values_buf, meta_buf);
}

test "ReceiverEventLoop init and peer liveness" {
    var registry = ServiceRegistry.init();
    var values_buf: [128 * 64]u8 align(128) = [_]u8{0} ** (128 * 64);
    var meta_buf: [256 * 64]u8 align(4) = [_]u8{0} ** (256 * 64);
    var counters = createTestCounters(&values_buf, &meta_buf);

    var recv_loop = ReceiverEventLoop.initWithAeron(
        &registry,
        &counters,
        1,
        null,
        false,
        .{ .peer_node_ids = &.{2} },
    );
    defer recv_loop.deinit();

    try testing.expectEqual(@as(u8, 1), recv_loop.local_node_id);
    try testing.expect(recv_loop.lookupPeer(2) != null);

    const now = Clock.monotonicNanos();
    var peer = recv_loop.lookupPeer(2).?;
    peer.last_recv_ns = now - 1600 * std.time.ns_per_ms;
    try testing.expectEqual(LivenessState.suspect, peer.update(now));
    peer.last_recv_ns = now - 4 * std.time.ns_per_s;
    try testing.expectEqual(@as(u32, 1), recv_loop.checkHeartbeatTimeouts(now));
    try testing.expectEqual(@as(i64, 1), counters.get(recv_loop.counter_ids.heartbeat_timeouts));
}

test "processBrokerUdpFragment drops frames for another node" {
    var registry = ServiceRegistry.init();
    var values_buf: [128 * 64]u8 align(128) = [_]u8{0} ** (128 * 64);
    var meta_buf: [256 * 64]u8 align(4) = [_]u8{0} ** (256 * 64);
    var counters = createTestCounters(&values_buf, &meta_buf);

    var recv_loop = ReceiverEventLoop.init(&registry, &counters, 1, testing.allocator);
    defer recv_loop.deinit();

    var frame_buf: [128]u8 = [_]u8{0} ** 128;
    const payload = "misdirected";
    const frame_len = try data_header.encodeFrame(&frame_buf, payload, .{
        .source_node_id = 2,
        .source_service_id = 10,
        .target_node_id = 3,
        .target_service_id = 20,
        .template_id = 7,
        .payload_length = payload.len,
    }, constants.default_max_frame_length);

    recv_loop.processBrokerUdpFragment(frame_buf[0..frame_len]);
    try testing.expectEqual(@as(i64, 1), counters.get(recv_loop.counter_ids.udp_misdirected_drops));
}

test "processBrokerAdminUdpFragment dispatches admin envelope" {
    var registry = ServiceRegistry.init();
    var values_buf: [128 * 64]u8 align(128) = [_]u8{0} ** (128 * 64);
    var meta_buf: [256 * 64]u8 align(4) = [_]u8{0} ** (256 * 64);
    var counters = createTestCounters(&values_buf, &meta_buf);
    var queue: AdminCommandQueue(64) = .{};

    var recv_loop = ReceiverEventLoop.initWithGroup(
        &registry,
        &counters,
        2,
        testing.allocator,
        "ringloom",
        &queue,
        false,
    );
    defer recv_loop.deinit();

    var payload_buf: [128]u8 = undefined;
    const payload_len = admin_messages.encodeAdminMessage(
        &payload_buf,
        admin_messages.ServiceAddedBody,
        admin_messages.TEMPLATE_SERVICE_ADDED,
        .{
            .node_id = 1,
            .service_id = 10,
            .service_name = admin_messages.padServiceName("echo"),
            .leader_election_enabled = 1,
        },
    );
    var frame_buf: [256]u8 = undefined;
    const frame_len = try admin_messages.encodeAeronAdminFrame(
        &frame_buf,
        1,
        2,
        99,
        payload_buf[0..payload_len],
    );

    recv_loop.processBrokerAdminUdpFragment(frame_buf[0..frame_len]);

    try testing.expectEqual(@as(i64, 1), counters.get(recv_loop.counter_ids.admin_messages_received));
    try testing.expectEqual(@as(u32, 1), queue.size());
    try testing.expect(recv_loop.lookupPeer(1) != null);
}
