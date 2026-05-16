// SPDX-License-Identifier: Apache-2.0
//! Receiver event loop for the v2 reliable UDP broker-to-broker path.

const std = @import("std");
const ringloom_common = @import("ringloom_common");
const udp = @import("ringloom_udp");

const constants = ringloom_common.platform.constants;
const memory_constants = ringloom_common.memory.constants;
const Clock = ringloom_common.platform.clock.Clock;
const AtomicBool = ringloom_common.platform.atomic.AtomicBool;
const CountersManager = ringloom_common.concurrent.counters.CountersManager;
const latency_trace = ringloom_common.message.latency_trace;

const PeerReceiver = @import("peer_receiver.zig").PeerReceiver;
const LivenessState = @import("peer_receiver.zig").LivenessState;
const ReadState = @import("peer_receiver.zig").ReadState;
const message_router = @import("message_router.zig");
const ServiceRegistry = message_router.ServiceRegistry;
const admin_dispatch = @import("../cluster/admin_dispatch.zig");
const AdminCommandQueue = admin_dispatch.AdminCommandQueue;
const StreamReceiver = @import("udp_reassembly.zig").StreamReceiver;

const log = std.log.scoped(.receiver);

pub const ReceiverCounters = struct {
    bytes_received: usize = 0,
    frames_routed: usize = 0,
    heartbeats_received: usize = 0,
    unknown_service_drops: usize = 0,
    service_full_drops: usize = 0,
    invalid_frame_drops: usize = 0,
    unknown_peer_drops: usize = 0,
    connections_accepted: usize = 0,
    handshake_failures: usize = 0,
    connection_errors: usize = 0,
    heartbeat_timeouts: usize = 0,
    peer_reconnects: usize = 0,
    admin_messages_received: usize = 0,
    admin_message_errors: usize = 0,
    status_sent: usize = 0,
    naks_sent: usize = 0,
    duplicate_packets: usize = 0,
    stale_session_packets: usize = 0,
    af_xdp_enabled: usize = 0,
    af_xdp_fallbacks: usize = 0,

    pub fn allocate(counters: *CountersManager) ReceiverCounters {
        return .{
            .bytes_received = counters.allocate(1, "udp_recv_bytes_received") orelse 0,
            .frames_routed = counters.allocate(1, "udp_recv_frames_routed") orelse 0,
            .heartbeats_received = counters.allocate(1, "udp_recv_heartbeats_received") orelse 0,
            .unknown_service_drops = counters.allocate(1, "udp_recv_unknown_service_drops") orelse 0,
            .service_full_drops = counters.allocate(1, "udp_recv_service_full_drops") orelse 0,
            .invalid_frame_drops = counters.allocate(1, "udp_recv_invalid_frame_drops") orelse 0,
            .unknown_peer_drops = counters.allocate(1, "udp_recv_unknown_peer_drops") orelse 0,
            .connections_accepted = counters.allocate(1, "udp_recv_peers_configured") orelse 0,
            .handshake_failures = counters.allocate(1, "udp_setup_failures") orelse 0,
            .connection_errors = counters.allocate(1, "udp_recv_endpoint_errors") orelse 0,
            .heartbeat_timeouts = counters.allocate(1, "udp_recv_heartbeat_timeouts") orelse 0,
            .peer_reconnects = counters.allocate(1, "udp_recv_peer_reconnects") orelse 0,
            .admin_messages_received = counters.allocate(1, "udp_recv_admin_messages_received") orelse 0,
            .admin_message_errors = counters.allocate(1, "udp_recv_admin_message_errors") orelse 0,
            .status_sent = counters.allocate(1, "udp_status_sent") orelse 0,
            .naks_sent = counters.allocate(1, "udp_naks_sent") orelse 0,
            .duplicate_packets = counters.allocate(1, "udp_duplicate_packets") orelse 0,
            .stale_session_packets = counters.allocate(1, "udp_stale_session_packets") orelse 0,
            .af_xdp_enabled = counters.allocate(1, "af_xdp_enabled") orelse 0,
            .af_xdp_fallbacks = counters.allocate(1, "af_xdp_fallbacks") orelse 0,
        };
    }
};

pub const ReceiverIoUringOptions = struct {
    mtu: u16 = udp.protocol.default_mtu,
    term_length: u32 = 64 * 1024,
    receiver_window_length: u32 = 32 * 1024,
    max_message_length: u32 = 64 * 1024,
    heartbeat_timeout_ns: i64 = 2_000 * std.time.ns_per_ms,
    nak_initial_delay_ns: i64 = 50 * std.time.ns_per_us,
    nak_retry_delay_ns: i64 = 250 * std.time.ns_per_us,
    status_interval_ns: i64 = 500 * std.time.ns_per_ms,
};

const PeerMap = struct {
    entries: [256]?*PeerReceiver,
    count: u32,

    fn init() PeerMap {
        return .{ .entries = [_]?*PeerReceiver{null} ** 256, .count = 0 };
    }

    fn get(self: *const PeerMap, node_id: u8) ?*PeerReceiver {
        return self.entries[node_id];
    }

    fn put(self: *PeerMap, node_id: u8, peer: *PeerReceiver) void {
        if (self.entries[node_id] == null) self.count += 1;
        self.entries[node_id] = peer;
    }

    fn remove(self: *PeerMap, node_id: u8) ?*PeerReceiver {
        const peer = self.entries[node_id];
        if (peer != null) {
            self.entries[node_id] = null;
            self.count -= 1;
        }
        return peer;
    }

    const Iterator = struct {
        map: *const PeerMap,
        index: usize,

        fn next(self: *Iterator) ?*PeerReceiver {
            while (self.index < self.map.entries.len) {
                const i = self.index;
                self.index += 1;
                if (self.map.entries[i]) |peer| return peer;
            }
            return null;
        }
    };

    fn iterator(self: *const PeerMap) Iterator {
        return .{ .map = self, .index = 0 };
    }
};

pub const ReceiverEventLoop = struct {
    peers: PeerMap,
    service_registry: *ServiceRegistry,
    counters: *CountersManager,
    counter_ids: ReceiverCounters,
    local_node_id: u8,
    running: AtomicBool,
    allocator: std.mem.Allocator,
    group_name_hash: u32,
    admin_cmd_queue: ?*AdminCommandQueue(64),
    benchmark_latency_tracing_enabled: bool,
    iouring_options: ReceiverIoUringOptions,
    endpoint: ?udp.UdpEndpoint,
    endpoint_scratch: []u8,
    packet_buf: []u8,
    streams: [memory_constants.default_send_buffer_entry_count]?StreamReceiver,

    const Self = @This();

    pub fn init(
        service_registry: *ServiceRegistry,
        counters: *CountersManager,
        local_node_id: u8,
        allocator: std.mem.Allocator,
    ) Self {
        return Self.initWithGroup(service_registry, counters, local_node_id, allocator, "ringloom", null, false);
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
        return initWithGroupAndIoUring(
            service_registry,
            counters,
            local_node_id,
            allocator,
            group_name,
            admin_queue,
            benchmark_latency_tracing_enabled,
            .{},
        );
    }

    pub fn initWithGroupAndIoUring(
        service_registry: *ServiceRegistry,
        counters: *CountersManager,
        local_node_id: u8,
        allocator: std.mem.Allocator,
        group_name: []const u8,
        admin_queue: ?*AdminCommandQueue(64),
        benchmark_latency_tracing_enabled: bool,
        iouring_options: ReceiverIoUringOptions,
    ) Self {
        const scratch_len = @as(usize, iouring_options.mtu) * 8;
        const scratch = allocator.alloc(u8, scratch_len) catch @panic("receiver scratch allocation failed");
        const packet_buf = allocator.alloc(u8, iouring_options.mtu) catch @panic("receiver packet allocation failed");
        return .{
            .peers = PeerMap.init(),
            .service_registry = service_registry,
            .counters = counters,
            .counter_ids = ReceiverCounters.allocate(counters),
            .local_node_id = local_node_id,
            .running = AtomicBool.init(true),
            .allocator = allocator,
            .group_name_hash = hashGroupName(group_name),
            .admin_cmd_queue = admin_queue,
            .benchmark_latency_tracing_enabled = benchmark_latency_tracing_enabled,
            .iouring_options = iouring_options,
            .endpoint = null,
            .endpoint_scratch = scratch,
            .packet_buf = packet_buf,
            .streams = [_]?StreamReceiver{null} ** memory_constants.default_send_buffer_entry_count,
        };
    }

    pub fn deinit(self: *Self) void {
        if (self.endpoint) |*endpoint| endpoint.deinit();
        self.endpoint = null;
        for (&self.streams) |*maybe_stream| {
            if (maybe_stream.*) |*stream| stream.deinit();
            maybe_stream.* = null;
        }
        var iter = self.peers.iterator();
        while (iter.next()) |peer| {
            peer.close();
            self.allocator.destroy(peer);
        }
        self.allocator.free(self.endpoint_scratch);
        self.allocator.free(self.packet_buf);
    }

    pub fn initListener(self: *Self, host: []const u8, port: u16) !void {
        try self.configureEndpoint(host, port);
    }

    pub fn configureEndpoint(self: *Self, host: []const u8, port: u16) !void {
        const local_address = try udp.Address.parseIp4(host, port);
        try self.configureEndpointWithConfig(.{
            .local_address = local_address,
            .mtu = self.iouring_options.mtu,
        });
    }

    pub fn configureEndpointWithConfig(self: *Self, config: udp.EndpointConfig) !void {
        if (self.endpoint) |*endpoint| endpoint.deinit();
        self.endpoint = null;

        self.endpoint = try udp.UdpEndpoint.init(config);
        const selection = self.endpoint.?.selection;
        if (selection.engine == .af_xdp) {
            self.counters.set(self.counter_ids.af_xdp_enabled, 1);
        } else {
            self.counters.set(self.counter_ids.af_xdp_enabled, 0);
        }
        if (selection.fell_back) {
            self.counters.add(self.counter_ids.af_xdp_fallbacks, @intCast(selection.fallback_count));
        }
    }

    pub fn doWork(self: *Self) u32 {
        var work_count: u32 = 0;
        const now_ns = Clock.monotonicNanos();
        work_count += self.pollUdpPackets(now_ns);
        work_count += self.emitScheduledControl(now_ns);
        work_count += self.checkHeartbeatTimeouts(now_ns);
        return work_count;
    }

    fn pollUdpPackets(self: *Self, now_ns: i64) u32 {
        var endpoint = if (self.endpoint) |*ep| ep else return 0;
        var packets: [8]udp.PacketView = undefined;
        const count = endpoint.poll(&packets, self.endpoint_scratch) catch |err| {
            self.counters.increment(self.counter_ids.connection_errors);
            log.warn("udp receiver poll failed: {}", .{err});
            return 0;
        };

        var work_count: u32 = 0;
        for (packets[0..count]) |packet| {
            self.handlePacket(packet, now_ns);
            work_count += 1;
        }
        return work_count;
    }

    fn handlePacket(self: *Self, packet: udp.PacketView, now_ns: i64) void {
        const common = udp.protocol.decodeCommon(packet.bytes, self.iouring_options.mtu) catch {
            self.counters.increment(self.counter_ids.invalid_frame_drops);
            return;
        };
        self.counters.add(self.counter_ids.bytes_received, @intCast(packet.length));

        switch (common.kind() catch {
            self.counters.increment(self.counter_ids.invalid_frame_drops);
            return;
        }) {
            .setup => self.handleSetup(packet, now_ns),
            .data => self.handleData(packet, now_ns),
            .heartbeat => self.handleHeartbeat(packet, now_ns),
            .rttm => {},
            .protocol_error => self.counters.increment(self.counter_ids.invalid_frame_drops),
            else => {},
        }
    }

    fn handleSetup(self: *Self, packet: udp.PacketView, now_ns: i64) void {
        const setup = udp.protocol.decodeSetup(
            packet.bytes,
            self.iouring_options.mtu,
            self.local_node_id,
            self.group_name_hash,
        ) catch {
            self.counters.increment(self.counter_ids.handshake_failures);
            return;
        };

        self.addPeer(setup.source_node_id, packet.source, setup.common.session_id) catch {
            self.counters.increment(self.counter_ids.connection_errors);
            return;
        };

        const stream = self.ensureStream(setup.*, packet.source) catch {
            self.counters.increment(self.counter_ids.connection_errors);
            return;
        };
        stream.next_status_ns = now_ns;

        const response = udp.SetupResponseHeader{
            .common = udp.CommonHeader.init(.setup_response, @sizeOf(udp.SetupResponseHeader), @sizeOf(udp.SetupResponseHeader), setup.common.session_id, 0),
            .stream_id = setup.stream_id,
            .receiver_id = self.local_node_id,
            .initial_term_id = setup.initial_term_id,
            .active_term_id = setup.active_term_id,
            .term_length = setup.term_length,
            .mtu = setup.mtu,
        };
        self.sendHeader(packet.source, response) catch self.counters.increment(self.counter_ids.connection_errors);
        const status = stream.makeStatus(now_ns, self.iouring_options.receiver_window_length);
        self.sendHeader(packet.source, status) catch self.counters.increment(self.counter_ids.connection_errors);
        self.counters.increment(self.counter_ids.status_sent);
    }

    fn handleData(self: *Self, packet: udp.PacketView, now_ns: i64) void {
        const header = udp.protocol.decodeData(packet.bytes, self.iouring_options.mtu) catch {
            self.counters.increment(self.counter_ids.invalid_frame_drops);
            return;
        };
        if (header.target_node_id != self.local_node_id) {
            self.counters.increment(self.counter_ids.invalid_frame_drops);
            return;
        }
        const stream = self.streamForId(header.stream_id) orelse {
            self.counters.increment(self.counter_ids.unknown_peer_drops);
            return;
        };
        const payload = packet.bytes[udp.DataHeader.encoded_length..header.common.frame_length];
        const result = stream.insertData(header.*, payload, now_ns) catch {
            self.counters.increment(self.counter_ids.invalid_frame_drops);
            return;
        };
        switch (result) {
            .inserted => {},
            .duplicate => self.counters.increment(self.counter_ids.duplicate_packets),
            .stale, .wrong_session => self.counters.increment(self.counter_ids.stale_session_packets),
            .wrong_source, .wrong_stream => self.counters.increment(self.counter_ids.invalid_frame_drops),
        }

        self.updatePeerLiveness(header.source_node_id, packet.source, header.common.session_id, now_ns);
        self.routeReadyMessages(stream, header.source_node_id);
        if (stream.statusDue(now_ns)) {
            const status = stream.makeStatus(now_ns, self.iouring_options.receiver_window_length);
            self.sendHeader(packet.source, status) catch self.counters.increment(self.counter_ids.connection_errors);
            self.counters.increment(self.counter_ids.status_sent);
        }
        if (stream.nakDue(now_ns)) {
            const nak = stream.makeNak(now_ns);
            self.sendHeader(packet.source, nak) catch self.counters.increment(self.counter_ids.connection_errors);
            self.counters.increment(self.counter_ids.naks_sent);
        }
    }

    fn routeReadyMessages(self: *Self, stream: *StreamReceiver, source_node_id: u8) void {
        while (stream.rebuildNext() catch null) |delivery| {
            if ((delivery.header.route_flags & constants.flag_admin) != 0) {
                self.handleAdminMessage(delivery.header, delivery.payload, source_node_id);
                continue;
            }
            if (self.benchmark_latency_tracing_enabled) {
                latency_trace.stampReceiverIngress(
                    @constCast(delivery.payload),
                    @intCast(Clock.monotonicNanosStable()),
                );
            }
            const result = message_router.routeUdpDataToService(self.service_registry, delivery.header, delivery.payload);
            switch (result) {
                .success => self.counters.increment(self.counter_ids.frames_routed),
                .unknown_service => self.counters.increment(self.counter_ids.unknown_service_drops),
                .service_full => self.counters.increment(self.counter_ids.service_full_drops),
            }
        }
    }

    fn handleHeartbeat(self: *Self, packet: udp.PacketView, now_ns: i64) void {
        if (packet.bytes.len < @sizeOf(udp.HeartbeatHeader)) {
            self.counters.increment(self.counter_ids.invalid_frame_drops);
            return;
        }
        const heartbeat: *const udp.HeartbeatHeader = @ptrCast(@alignCast(packet.bytes.ptr));
        const stream = self.streamForId(heartbeat.stream_id);
        if (stream) |s| {
            self.updatePeerLiveness(s.source_node_id, packet.source, heartbeat.common.session_id, now_ns);
        }
        self.counters.increment(self.counter_ids.heartbeats_received);
    }

    fn handleAdminMessage(self: *Self, header: udp.DataHeader, payload: []const u8, source_node_id: u8) void {
        const queue = self.admin_cmd_queue orelse {
            self.counters.increment(self.counter_ids.admin_message_errors);
            return;
        };
        _ = header;
        admin_dispatch.dispatchAdminMessage(payload, queue, Clock.monotonicNanos(), source_node_id);
        self.counters.increment(self.counter_ids.admin_messages_received);
    }

    fn emitScheduledControl(self: *Self, now_ns: i64) u32 {
        var count: u32 = 0;
        for (&self.streams) |*maybe_stream| {
            if (maybe_stream.*) |*stream| {
                const peer = self.peers.get(stream.source_node_id) orelse continue;
                if (stream.nakDue(now_ns)) {
                    const nak = stream.makeNak(now_ns);
                    self.sendHeader(peer.address, nak) catch continue;
                    self.counters.increment(self.counter_ids.naks_sent);
                    count += 1;
                }
                if (stream.statusDue(now_ns)) {
                    const status = stream.makeStatus(now_ns, self.iouring_options.receiver_window_length);
                    self.sendHeader(peer.address, status) catch continue;
                    self.counters.increment(self.counter_ids.status_sent);
                    count += 1;
                }
            }
        }
        return count;
    }

    fn ensureStream(self: *Self, setup: udp.SetupHeader, source: udp.Address) !*StreamReceiver {
        if (self.streamForId(setup.stream_id)) |stream| return stream;
        for (&self.streams) |*maybe_stream| {
            if (maybe_stream.* == null) {
                maybe_stream.* = try StreamReceiver.init(self.allocator, setup.stream_id, setup.source_node_id, setup.common.session_id, setup.initial_term_id, .{
                    .term_length = setup.term_length,
                    .window_length = self.iouring_options.receiver_window_length,
                    .max_message_length = self.iouring_options.max_message_length,
                    .initial_nak_delay_ns = self.iouring_options.nak_initial_delay_ns,
                    .nak_retry_delay_ns = self.iouring_options.nak_retry_delay_ns,
                    .status_interval_ns = self.iouring_options.status_interval_ns,
                });
                self.updatePeerLiveness(setup.source_node_id, source, setup.common.session_id, Clock.monotonicNanos());
                return &maybe_stream.*.?;
            }
        }
        return error.StreamTableFull;
    }

    fn streamForId(self: *Self, stream_id: u32) ?*StreamReceiver {
        for (&self.streams) |*maybe_stream| {
            if (maybe_stream.*) |*stream| {
                if (stream.stream_id == stream_id) return stream;
            }
        }
        return null;
    }

    fn sendHeader(self: *Self, destination: udp.Address, header: anytype) !void {
        var endpoint = if (self.endpoint) |*ep| ep else return error.EndpointNotConfigured;
        const bytes = std.mem.asBytes(&header);
        _ = try endpoint.send(bytes, destination);
    }

    fn checkHeartbeatTimeouts(self: *Self, now_ns: i64) u32 {
        var work_count: u32 = 0;
        var peer_iter = self.peers.iterator();
        while (peer_iter.next()) |peer| {
            const prev_liveness = peer.liveness;
            const new_liveness = peer.updateLiveness(now_ns, self.iouring_options.heartbeat_timeout_ns);
            if (new_liveness == .dead and prev_liveness != .dead) {
                self.counters.increment(self.counter_ids.heartbeat_timeouts);
                work_count += 1;
            }
        }
        return work_count;
    }

    pub fn addPeer(self: *Self, node_id: u8, address: udp.Address, session_epoch: u32) !void {
        if (self.peers.get(node_id)) |existing| {
            existing.resetForReconnect(address, session_epoch);
            self.counters.increment(self.counter_ids.peer_reconnects);
            return;
        }

        const peer = try self.allocator.create(PeerReceiver);
        errdefer self.allocator.destroy(peer);
        peer.* = PeerReceiver.init(node_id, address, session_epoch);
        self.peers.put(node_id, peer);
        self.counters.increment(self.counter_ids.connections_accepted);
        if (self.admin_cmd_queue) |q| {
            _ = q.enqueue(.{ .peer_connected = .{ .node_id = node_id } });
        }
    }

    pub fn removePeer(self: *Self, node_id: u8) void {
        if (self.peers.remove(node_id)) |peer| {
            peer.close();
            self.allocator.destroy(peer);
        }
    }

    pub fn lookupPeer(self: *Self, node_id: u8) ?*PeerReceiver {
        return self.peers.get(node_id);
    }

    fn updatePeerLiveness(self: *Self, node_id: u8, address: udp.Address, session_id: u32, now_ns: i64) void {
        if (self.peers.get(node_id)) |peer| {
            peer.address = address;
            peer.session_epoch = session_id;
            peer.last_recv_ns = now_ns;
            peer.liveness = .alive;
            peer.connected = true;
        } else {
            self.addPeer(node_id, address, session_id) catch {};
        }
    }
};

fn hashGroupName(group_name: []const u8) u32 {
    var hash: u32 = 2166136261;
    for (group_name) |byte| hash = (hash ^ byte) *% 16777619;
    return hash;
}

const testing = std.testing;

fn createTestCounters(
    values_buf: *align(128) [128 * 64]u8,
    meta_buf: *[256 * 64]u8,
) CountersManager {
    return CountersManager.init(values_buf, meta_buf);
}

test "ReceiverEventLoop init and deinit" {
    const allocator = testing.allocator;
    var registry = ServiceRegistry.init();
    var values_buf: [128 * 64]u8 align(128) = [_]u8{0} ** (128 * 64);
    var meta_buf: [256 * 64]u8 align(4) = [_]u8{0} ** (256 * 64);
    var counters = createTestCounters(&values_buf, &meta_buf);

    var recv_loop = ReceiverEventLoop.init(&registry, &counters, 1, allocator);
    defer recv_loop.deinit();

    try testing.expectEqual(@as(u8, 1), recv_loop.local_node_id);
    try testing.expectEqual(@as(u32, 0), recv_loop.peers.count);
}

test "ReceiverEventLoop records AF_XDP fallback selection" {
    if (@import("builtin").os.tag != .linux) return error.SkipZigTest;

    const allocator = testing.allocator;
    var registry = ServiceRegistry.init();
    var values_buf: [128 * 64]u8 align(128) = [_]u8{0} ** (128 * 64);
    var meta_buf: [256 * 64]u8 align(4) = [_]u8{0} ** (256 * 64);
    var counters = createTestCounters(&values_buf, &meta_buf);

    var recv_loop = ReceiverEventLoop.init(&registry, &counters, 1, allocator);
    defer recv_loop.deinit();

    try recv_loop.configureEndpointWithConfig(.{
        .local_address = .initIp4(.{ 127, 0, 0, 1 }, 0),
        .engine_mode = .prefer_af_xdp,
        .af_xdp = .{ .interfaces = &.{"lo"}, .ports = &.{9000} },
    });

    try testing.expectEqual(@as(i64, 0), counters.get(recv_loop.counter_ids.af_xdp_enabled));
    try testing.expectEqual(@as(i64, 1), counters.get(recv_loop.counter_ids.af_xdp_fallbacks));
}

test "addPeer and removePeer use UDP addresses" {
    const allocator = testing.allocator;
    var registry = ServiceRegistry.init();
    var values_buf: [128 * 64]u8 align(128) = [_]u8{0} ** (128 * 64);
    var meta_buf: [256 * 64]u8 align(4) = [_]u8{0} ** (256 * 64);
    var counters = createTestCounters(&values_buf, &meta_buf);

    var recv_loop = ReceiverEventLoop.init(&registry, &counters, 1, allocator);
    defer recv_loop.deinit();

    try recv_loop.addPeer(2, .initIp4(.{ 127, 0, 0, 1 }, 9001), 1);
    try testing.expectEqual(@as(u32, 1), recv_loop.peers.count);
    try testing.expect(recv_loop.lookupPeer(2) != null);

    recv_loop.removePeer(2);
    try testing.expectEqual(@as(u32, 0), recv_loop.peers.count);
}
