// SPDX-License-Identifier: Apache-2.0
//! Sender event loop for the v2 reliable UDP broker-to-broker path.

const std = @import("std");
const ringloom_common = @import("ringloom_common");
const udp = @import("ringloom_udp");

const constants = ringloom_common.platform.constants;
const memory_constants = ringloom_common.memory.constants;
const Clock = ringloom_common.platform.clock.Clock;
const AtomicBool = ringloom_common.platform.atomic.AtomicBool;
const RingBuffer = ringloom_common.concurrent.ring_buffer.RingBuffer;
const CountersManager = ringloom_common.concurrent.counters.CountersManager;
const PeerSendCountersRegion = ringloom_common.memory.PeerSendCountersRegion;
const SendBufferDirectory = ringloom_common.memory.SendBufferDirectory;
const SendBufferEntry = ringloom_common.memory.SendBufferEntry;
const SendBufferPressureState = ringloom_common.memory.SendBufferPressureState;
const MessageHeader = ringloom_common.message.message_header.MessageHeader;

const PeerSender = @import("peer_sender.zig").PeerSender;
const ConnectionState = @import("peer_sender.zig").ConnectionState;
const SenderCommand = @import("sender_command.zig").SenderCommand;
const StreamSender = @import("udp_scheduler.zig").StreamSender;

const log = std.log.scoped(.sender);

const route_flag_admin: u16 = constants.flag_admin;
const route_flag_begin: u16 = 0x80;
const route_flag_end: u16 = 0x40;
const route_flag_unfragmented: u16 = route_flag_begin | route_flag_end;

pub const SenderCounters = struct {
    frames_sent: usize = 0,
    bytes_sent: usize = 0,
    heartbeats_sent: usize = 0,
    send_back_pressure: usize = 0,
    malformed_messages_dropped: usize = 0,
    unknown_peer_messages_dropped: usize = 0,
    peer_not_connected_drops: usize = 0,
    peer_queue_overflow_drops: usize = 0,
    send_errors: usize = 0,
    peers_connected: usize = 0,
    peers_disconnected: usize = 0,
    peers_timed_out: usize = 0,
    reconnect_attempts: usize = 0,
    endpoint_send_pressure: usize = 0,
    endpoint_send_errors: usize = 0,
    nak_received: usize = 0,
    retransmits_sent: usize = 0,
    status_received: usize = 0,
    setup_sent: usize = 0,

    pub fn allocate(counters: *CountersManager) SenderCounters {
        return .{
            .frames_sent = counters.allocate(1, "udp_frames_sent") orelse 0,
            .bytes_sent = counters.allocate(1, "udp_bytes_sent") orelse 0,
            .heartbeats_sent = counters.allocate(1, "udp_heartbeats_sent") orelse 0,
            .send_back_pressure = counters.allocate(1, "udp_send_back_pressure") orelse 0,
            .malformed_messages_dropped = counters.allocate(1, "sender_malformed_messages_dropped") orelse 0,
            .unknown_peer_messages_dropped = counters.allocate(1, "sender_unknown_peer_messages_dropped") orelse 0,
            .peer_not_connected_drops = counters.allocate(1, "sender_peer_not_connected_drops") orelse 0,
            .peer_queue_overflow_drops = counters.allocate(1, "sender_destination_overflow_drops") orelse 0,
            .send_errors = counters.allocate(1, "udp_send_errors") orelse 0,
            .peers_connected = counters.allocate(1, "udp_peers_configured") orelse 0,
            .peers_disconnected = counters.allocate(1, "udp_peers_removed") orelse 0,
            .peers_timed_out = counters.allocate(1, "udp_peers_timed_out") orelse 0,
            .reconnect_attempts = counters.allocate(1, "udp_setup_retries") orelse 0,
            .endpoint_send_pressure = counters.allocate(1, "udp_endpoint_send_pressure") orelse 0,
            .endpoint_send_errors = counters.allocate(1, "udp_endpoint_send_errors") orelse 0,
            .nak_received = counters.allocate(1, "udp_naks_received") orelse 0,
            .retransmits_sent = counters.allocate(1, "udp_retransmits_sent") orelse 0,
            .status_received = counters.allocate(1, "udp_status_received") orelse 0,
            .setup_sent = counters.allocate(1, "udp_setup_sent") orelse 0,
        };
    }
};

pub const SenderOptions = struct {
    mtu: u16 = udp.protocol.default_mtu,
    term_length: u32 = 64 * 1024,
    receiver_window_length: u32 = 32 * 1024,
    heartbeat_interval_ns: i64 = 500 * std.time.ns_per_ms,
    setup_interval_ns: i64 = 500 * std.time.ns_per_ms,
    setup_retry_limit: u8 = 5,
    retransmit_linger_ns: i64 = 10 * std.time.ns_per_ms,
};

const PeerMap = struct {
    entries: [256]?*PeerSender,
    count: u32,

    fn init() PeerMap {
        return .{ .entries = [_]?*PeerSender{null} ** 256, .count = 0 };
    }

    fn get(self: *const PeerMap, node_id: u8) ?*PeerSender {
        return self.entries[node_id];
    }

    fn put(self: *PeerMap, node_id: u8, peer: *PeerSender) void {
        if (self.entries[node_id] == null) self.count += 1;
        self.entries[node_id] = peer;
    }

    fn remove(self: *PeerMap, node_id: u8) ?*PeerSender {
        const peer = self.entries[node_id];
        if (peer != null) {
            self.entries[node_id] = null;
            self.count -= 1;
        }
        return peer;
    }

    fn contains(self: *const PeerMap, node_id: u8) bool {
        return self.entries[node_id] != null;
    }

    const Iterator = struct {
        map: *const PeerMap,
        index: usize,

        fn next(self: *Iterator) ?*PeerSender {
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

pub const SenderEventLoop = struct {
    send_ring_buffer: ?*RingBuffer,
    send_buffer_directory: ?*SendBufferDirectory,
    broker_mapped_bytes: ?[]u8,
    round_robin_index: u32,
    peers: PeerMap,
    counters: *CountersManager,
    counter_ids: SenderCounters,
    last_heartbeat_ns: i64,
    local_node_id: u8,
    running: AtomicBool,
    allocator: std.mem.Allocator,
    pending_send_count: u32,
    group_name_hash: u32,
    benchmark_latency_tracing_enabled: bool,
    options: SenderOptions,
    endpoint: ?udp.PosixEndpoint,
    endpoint_scratch: []u8,
    packet_buf: []u8,
    streams: [memory_constants.default_send_buffer_entry_count]?StreamSender,
    peer_send_counters: ?PeerSendCountersRegion,

    const Self = @This();

    pub fn init(
        send_ring_buffer: *RingBuffer,
        counters: *CountersManager,
        local_node_id: u8,
        allocator: std.mem.Allocator,
    ) !Self {
        return Self.initWithGroup(send_ring_buffer, counters, local_node_id, allocator, "ringloom", false);
    }

    pub fn initWithGroup(
        send_ring_buffer: *RingBuffer,
        counters: *CountersManager,
        local_node_id: u8,
        allocator: std.mem.Allocator,
        group_name: []const u8,
        benchmark_latency_tracing_enabled: bool,
    ) !Self {
        return initWithGroupAndOptions(
            send_ring_buffer,
            counters,
            local_node_id,
            allocator,
            group_name,
            benchmark_latency_tracing_enabled,
            .{},
        );
    }

    pub fn initWithGroupAndOptions(
        send_ring_buffer: *RingBuffer,
        counters: *CountersManager,
        local_node_id: u8,
        allocator: std.mem.Allocator,
        group_name: []const u8,
        benchmark_latency_tracing_enabled: bool,
        options: SenderOptions,
    ) !Self {
        var self = try initBase(counters, local_node_id, allocator, group_name, benchmark_latency_tracing_enabled, options);
        self.send_ring_buffer = send_ring_buffer;
        return self;
    }

    pub fn initWithDirectoryAndOptions(
        send_buffer_directory: *SendBufferDirectory,
        broker_mapped_bytes: []u8,
        counters: *CountersManager,
        local_node_id: u8,
        allocator: std.mem.Allocator,
        group_name: []const u8,
        benchmark_latency_tracing_enabled: bool,
        options: SenderOptions,
    ) !Self {
        var self = try initBase(counters, local_node_id, allocator, group_name, benchmark_latency_tracing_enabled, options);
        self.send_buffer_directory = send_buffer_directory;
        self.broker_mapped_bytes = broker_mapped_bytes;
        return self;
    }

    fn initBase(
        counters: *CountersManager,
        local_node_id: u8,
        allocator: std.mem.Allocator,
        group_name: []const u8,
        benchmark_latency_tracing_enabled: bool,
        options: SenderOptions,
    ) !Self {
        const scratch_len = @as(usize, options.mtu) * 8;
        const scratch = try allocator.alloc(u8, scratch_len);
        errdefer allocator.free(scratch);
        const packet_buf = try allocator.alloc(u8, options.mtu);
        errdefer allocator.free(packet_buf);

        return .{
            .send_ring_buffer = null,
            .send_buffer_directory = null,
            .broker_mapped_bytes = null,
            .round_robin_index = 0,
            .peers = PeerMap.init(),
            .counters = counters,
            .counter_ids = SenderCounters.allocate(counters),
            .last_heartbeat_ns = 0,
            .local_node_id = local_node_id,
            .running = AtomicBool.init(true),
            .allocator = allocator,
            .pending_send_count = 0,
            .group_name_hash = hashGroupName(group_name),
            .benchmark_latency_tracing_enabled = benchmark_latency_tracing_enabled,
            .options = options,
            .endpoint = null,
            .endpoint_scratch = scratch,
            .packet_buf = packet_buf,
            .streams = [_]?StreamSender{null} ** memory_constants.default_send_buffer_entry_count,
            .peer_send_counters = null,
        };
    }

    pub fn configureEndpoint(self: *Self, host: []const u8, port: u16) !void {
        if (self.endpoint) |*endpoint| endpoint.deinit();
        const local_address = try udp.Address.parseIp4(host, port);
        self.endpoint = try udp.PosixEndpoint.init(.{
            .local_address = local_address,
            .mtu = self.options.mtu,
        });
    }

    pub fn setPeerSendCountersRegion(self: *Self, region: ?PeerSendCountersRegion) void {
        self.peer_send_counters = region;
    }

    pub fn deinit(self: *Self) void {
        if (self.endpoint) |*endpoint| endpoint.deinit();
        self.endpoint = null;
        for (&self.streams) |*maybe_stream| {
            if (maybe_stream.*) |*stream| stream.deinit(self.allocator);
            maybe_stream.* = null;
        }
        var iter = self.peers.iterator();
        while (iter.next()) |peer| {
            peer.deinit(self.allocator);
            self.allocator.destroy(peer);
        }
        self.allocator.free(self.endpoint_scratch);
        self.allocator.free(self.packet_buf);
    }

    pub fn doWork(self: *Self) u32 {
        var work_count: u32 = 0;
        const now_ns = Clock.monotonicNanos();
        self.pending_send_count = 0;

        work_count += self.pollControlFrames(now_ns);
        work_count += self.processRetransmits(now_ns);
        work_count += self.drainDestinationBuffers(constants.send_batch_limit);
        work_count += self.readLegacyRing(constants.send_batch_limit);
        work_count += self.sendDueHeartbeats(now_ns);
        self.publishPeerSendCounters(now_ns);

        return work_count + self.pending_send_count;
    }

    threadlocal var tls_self: ?*Self = null;
    threadlocal var tls_destination_entry: ?*SendBufferEntry = null;

    fn onOutboundMessageThunk(msg_type_id: i32, payload: []const u8) void {
        if (tls_self) |self| self.onOutboundMessage(msg_type_id, payload);
    }

    pub fn onOutboundMessage(self: *Self, msg_type_id: i32, payload: []const u8) void {
        if (msg_type_id != constants.message_envelope_msg_type_id) {
            self.counters.increment(self.counter_ids.malformed_messages_dropped);
            return;
        }
        self.sendEnvelope(payload, tls_destination_entry) catch |err| {
            self.recordSendFailure(err, payload.len, tls_destination_entry);
        };
    }

    fn drainDestinationBuffers(self: *Self, limit: u32) u32 {
        const directory = self.send_buffer_directory orelse return 0;
        const mapped = self.broker_mapped_bytes orelse return 0;
        if (directory.entries.len == 0 or limit == 0) return 0;

        var work_count: u32 = 0;
        var visited: u32 = 0;
        const entry_count: u32 = @intCast(directory.entries.len);
        var index = self.round_robin_index % entry_count;

        while (visited < entry_count and work_count < limit) : (visited += 1) {
            const entry = &directory.entries[index];
            defer index = (index + 1) % entry_count;

            const state = entry.loadState();
            if (state != .active and state != .draining) continue;
            const stream = self.ensureStreamForEntry(entry) catch {
                entry.storePressureState(.peer_down);
                continue;
            };
            entry.storePressureState(stream.pressureState(state));
            self.sendSetupIfDue(stream, entry, Clock.monotonicNanos());
            if (stream.setup_state != .confirmed) continue;
            if (!self.canDrainDestination(entry)) continue;

            const ring_slice = directory.ringSliceForEntry(mapped, entry) catch continue;
            var ring = RingBuffer.init(ring_slice, false, null, null) catch continue;

            tls_self = self;
            tls_destination_entry = entry;
            const read_count = ring.read(onOutboundMessageThunk, 1);
            tls_destination_entry = null;
            tls_self = null;

            if (read_count > 0) {
                work_count += read_count;
                self.round_robin_index = (index + 1) % entry_count;
            }
        }
        return work_count;
    }

    fn readLegacyRing(self: *Self, limit: u32) u32 {
        if (self.send_buffer_directory != null) return 0;
        const send_ring_buffer = self.send_ring_buffer orelse return 0;
        tls_self = self;
        defer tls_self = null;
        return send_ring_buffer.read(onOutboundMessageThunk, limit);
    }

    fn canDrainDestination(self: *Self, entry: *const SendBufferEntry) bool {
        _ = self;
        return switch (entry.loadPressureState()) {
            .unknown, .normal => true,
            .flow_blocked,
            .congested,
            .term_blocked,
            .peer_down,
            .draining,
            .closed,
            => false,
        };
    }

    fn ensureStreamForEntry(self: *Self, entry: *const SendBufferEntry) !*StreamSender {
        if (entry.target_node_id <= 0 or entry.target_node_id > std.math.maxInt(u8)) return error.InvalidPeer;
        if (entry.target_service_id < 0 or entry.target_service_id > std.math.maxInt(u16)) return error.InvalidPeer;
        if (self.peers.get(@intCast(entry.target_node_id)) == null) return error.UnknownPeer;
        const directory = self.send_buffer_directory orelse return error.InvalidPeer;
        var index: usize = directory.entries.len;
        for (directory.entries, 0..) |*candidate, i| {
            if (candidate == entry) {
                index = i;
                break;
            }
        }
        if (index >= self.streams.len) return error.InvalidPeer;
        if (self.streams[index] == null) {
            self.streams[index] = try StreamSender.init(
                self.allocator,
                entry.stream_id,
                @intCast(entry.target_node_id),
                @intCast(entry.target_service_id),
                sessionId(self.local_node_id, @intCast(entry.target_node_id)),
                1,
                .{
                    .term_length = self.options.term_length,
                    .mtu = self.options.mtu,
                    .initial_sender_limit = self.options.receiver_window_length,
                    .congestion_window = self.options.receiver_window_length,
                    .heartbeat_interval_ns = self.options.heartbeat_interval_ns,
                    .setup_interval_ns = self.options.setup_interval_ns,
                    .setup_retry_limit = self.options.setup_retry_limit,
                    .retransmit_linger_ns = self.options.retransmit_linger_ns,
                },
            );
        }
        return &self.streams[index].?;
    }

    fn streamForId(self: *Self, stream_id: u32) ?*StreamSender {
        for (&self.streams) |*maybe_stream| {
            if (maybe_stream.*) |*stream| {
                if (stream.stream_id == stream_id) return stream;
            }
        }
        return null;
    }

    fn sendEnvelope(self: *Self, payload: []const u8, entry: ?*SendBufferEntry) !void {
        if (payload.len < MessageHeader.encoded_length) return error.MalformedEnvelope;
        const envelope = MessageHeader.decode(payload[0..MessageHeader.encoded_length]);
        if (envelope.payload_length < 0) return error.MalformedEnvelope;
        const body_len: usize = @intCast(envelope.payload_length);
        if (body_len != payload.len - MessageHeader.encoded_length) return error.MalformedEnvelope;
        if (envelope.target_node_id <= 0 or envelope.target_node_id > std.math.maxInt(u8)) return error.MalformedEnvelope;
        if (envelope.target_service_id < 0 or envelope.target_service_id > std.math.maxInt(u16)) return error.MalformedEnvelope;
        if (envelope.source_service_id < 0) return error.MalformedEnvelope;

        const target_node_id: u8 = @intCast(envelope.target_node_id);
        const peer = self.peers.get(target_node_id) orelse return error.UnknownPeer;
        if (peer.state != .connected) return error.PeerDisconnected;
        const destination_entry = entry orelse return error.NoDestinationEntry;
        const stream = try self.ensureStreamForEntry(destination_entry);
        if (stream.setup_state != .confirmed) return error.PeerDisconnected;

        if (entry) |dest| dest.subtractPending(@intCast(ringCost(payload.len)));
        const body = payload[MessageHeader.encoded_length..];
        try self.sendBodyFragments(stream, peer, envelope.*, body);
    }

    fn sendBodyFragments(
        self: *Self,
        stream: *StreamSender,
        peer: *PeerSender,
        envelope: MessageHeader,
        body: []const u8,
    ) !void {
        const max_payload = self.options.mtu - udp.DataHeader.encoded_length;
        var offset: usize = 0;
        const message_id = stream.term_log.sender_position;
        while (offset < body.len or (body.len == 0 and offset == 0)) {
            const remaining = body.len - offset;
            const term_offset = stream.term_log.sender_position % stream.options.term_length;
            const term_remaining = stream.options.term_length - term_offset;
            const term_payload_capacity = if (term_remaining >= udp.DataHeader.encoded_length)
                term_remaining - udp.DataHeader.encoded_length
            else
                max_payload;
            const chunk_len = if (body.len == 0)
                0
            else
                @min(remaining, @min(max_payload, term_payload_capacity));
            const chunk = body[offset..][0..chunk_len];
            var route_flags: u16 = @as(u16, envelope.flags) & route_flag_admin;
            if (offset == 0) route_flags |= route_flag_begin;
            if (offset + chunk_len >= body.len) route_flags |= route_flag_end;
            if (offset == 0 and offset + chunk_len >= body.len) route_flags |= route_flag_unfragmented;

            const header = udp.DataHeader.init(.{
                .session_id = stream.session_id,
                .stream_id = stream.stream_id,
                .term_id = stream.term_log.active_term_id,
                .term_offset = @intCast(stream.term_log.sender_position % stream.options.term_length),
                .message_id = message_id,
                .fragment_offset = @intCast(offset),
                .message_length = @intCast(body.len),
                .payload_length = chunk.len,
                .source_node_id = self.local_node_id,
                .target_node_id = peer.node_id,
                .route_flags = route_flags,
                .source_service_id = @intCast(envelope.source_service_id),
                .target_service_id = @intCast(envelope.target_service_id),
                .template_id = envelope.template_id,
                .correlation_id = envelope.correlation_id,
            });
            const start_position = try stream.term_log.appendData(header, chunk);
            try self.sendFromTerm(stream, peer, start_position, self.options.mtu);
            stream.last_frame_sent_ns = Clock.monotonicNanos();
            self.pending_send_count += 1;
            self.counters.increment(self.counter_ids.frames_sent);
            if (body.len == 0) break;
            offset += chunk_len;
        }
    }

    fn sendFromTerm(self: *Self, stream: *StreamSender, peer: *PeerSender, position: u64, max_length: u32) !void {
        var out: [1]udp.term_log.ScannedFrame = undefined;
        const term_id = stream.initial_term_id + @as(i32, @intCast(position / stream.options.term_length));
        const term_offset: u32 = @intCast(position % stream.options.term_length);
        const count = stream.term_log.scan(term_id, term_offset, max_length, &out);
        if (count == 0) return error.TermFrameMissing;
        try self.sendScannedFrame(peer, out[0]);
    }

    fn sendScannedFrame(self: *Self, peer: *PeerSender, frame: udp.term_log.ScannedFrame) !void {
        const frame_len = frame.header.common.frame_length;
        if (frame_len > self.packet_buf.len) return error.FrameTooLarge;
        @memcpy(self.packet_buf[0..udp.DataHeader.encoded_length], std.mem.asBytes(frame.header));
        if (frame.payload.len > 0) {
            @memcpy(self.packet_buf[udp.DataHeader.encoded_length..][0..frame.payload.len], frame.payload);
        }
        try self.sendPacket(peer, self.packet_buf[0..frame_len]);
    }

    fn sendPacket(self: *Self, peer: *PeerSender, bytes: []const u8) !void {
        var endpoint = if (self.endpoint) |*ep| ep else return error.EndpointNotConfigured;
        _ = endpoint.send(bytes, peer.address) catch |err| switch (err) {
            error.WouldBlock => {
                self.counters.increment(self.counter_ids.endpoint_send_pressure);
                return error.WouldBlock;
            },
            else => {
                self.counters.increment(self.counter_ids.endpoint_send_errors);
                return err;
            },
        };
        peer.last_send_ns = Clock.monotonicNanos();
        peer.total_bytes_sent += bytes.len;
        self.counters.add(self.counter_ids.bytes_sent, @intCast(bytes.len));
    }

    fn sendSetupIfDue(self: *Self, stream: *StreamSender, entry: *const SendBufferEntry, now_ns: i64) void {
        if (!stream.setupDue(now_ns)) return;
        const peer = self.peers.get(stream.target_node_id) orelse return;
        const setup = udp.SetupHeader{
            .common = udp.CommonHeader.init(.setup, udp.SetupHeader.encoded_length, udp.SetupHeader.encoded_length, stream.session_id, 0),
            .source_node_id = self.local_node_id,
            .target_node_id = stream.target_node_id,
            .stream_id = stream.stream_id,
            .initial_term_id = stream.initial_term_id,
            .active_term_id = stream.term_log.active_term_id,
            .term_length = stream.options.term_length,
            .mtu = stream.options.mtu,
            .sender_epoch = @intCast(@max(Clock.monotonicNanos(), 0)),
            .group_name_hash = self.group_name_hash,
        };
        self.sendHeader(peer, setup) catch {
            self.counters.increment(self.counter_ids.send_errors);
            return;
        };
        _ = entry;
        self.counters.increment(self.counter_ids.setup_sent);
        self.counters.increment(self.counter_ids.reconnect_attempts);
    }

    fn sendDueHeartbeats(self: *Self, now_ns: i64) u32 {
        var count: u32 = 0;
        for (&self.streams) |*maybe_stream| {
            if (maybe_stream.*) |*stream| {
                if (!stream.heartbeatDue(now_ns)) continue;
                const peer = self.peers.get(stream.target_node_id) orelse continue;
                const position = stream.term_log.sender_position;
                const heartbeat = udp.HeartbeatHeader{
                    .common = udp.CommonHeader.init(.heartbeat, @sizeOf(udp.HeartbeatHeader), @sizeOf(udp.HeartbeatHeader), stream.session_id, 0),
                    .stream_id = stream.stream_id,
                    .term_id = stream.initial_term_id + @as(i32, @intCast(position / stream.options.term_length)),
                    .term_offset = @intCast(position % stream.options.term_length),
                };
                self.sendHeader(peer, heartbeat) catch continue;
                self.counters.increment(self.counter_ids.heartbeats_sent);
                count += 1;
            }
        }
        return count;
    }

    fn sendHeader(self: *Self, peer: *PeerSender, header: anytype) !void {
        const bytes = std.mem.asBytes(&header);
        try self.sendPacket(peer, bytes);
    }

    fn pollControlFrames(self: *Self, now_ns: i64) u32 {
        var endpoint = if (self.endpoint) |*ep| ep else return 0;
        var packets: [8]udp.PacketView = undefined;
        const count = endpoint.poll(&packets, self.endpoint_scratch) catch return 0;
        var work_count: u32 = 0;
        for (packets[0..count]) |packet| {
            const common = udp.protocol.decodeCommon(packet.bytes, self.options.mtu) catch {
                self.counters.increment(self.counter_ids.malformed_messages_dropped);
                continue;
            };
            switch (common.kind() catch continue) {
                .setup_response => self.handleSetupResponse(packet.bytes, now_ns),
                .status => self.handleStatus(packet.bytes, now_ns),
                .nak => self.handleNak(packet.bytes, now_ns),
                .rttm => {},
                .heartbeat => {},
                .protocol_error => self.counters.increment(self.counter_ids.send_errors),
                else => {},
            }
            work_count += 1;
        }
        return work_count;
    }

    fn handleSetupResponse(self: *Self, bytes: []const u8, now_ns: i64) void {
        _ = now_ns;
        if (bytes.len < @sizeOf(udp.SetupResponseHeader)) return;
        const response: *const udp.SetupResponseHeader = @ptrCast(@alignCast(bytes.ptr));
        const stream = self.streamForId(response.stream_id) orelse return;
        stream.setup_state = .confirmed;
        stream.term_log.sender_limit = @max(stream.term_log.sender_limit, self.options.receiver_window_length);
    }

    fn handleStatus(self: *Self, bytes: []const u8, now_ns: i64) void {
        _ = now_ns;
        if (bytes.len < @sizeOf(udp.StatusHeader)) return;
        const status: *const udp.StatusHeader = @ptrCast(@alignCast(bytes.ptr));
        const stream = self.streamForId(status.stream_id) orelse return;
        stream.applyStatus(status.*);
        self.counters.increment(self.counter_ids.status_received);
    }

    fn handleNak(self: *Self, bytes: []const u8, now_ns: i64) void {
        if (bytes.len < @sizeOf(udp.NakHeader)) return;
        const nak: *const udp.NakHeader = @ptrCast(@alignCast(bytes.ptr));
        const stream = self.streamForId(nak.stream_id) orelse return;
        if (stream.scheduleNak(nak.*, now_ns)) {
            stream.congestion.onLoss();
            self.counters.increment(self.counter_ids.nak_received);
        }
    }

    fn processRetransmits(self: *Self, now_ns: i64) u32 {
        var work_count: u32 = 0;
        var frames: [4]udp.term_log.ScannedFrame = undefined;
        for (&self.streams) |*maybe_stream| {
            if (maybe_stream.*) |*stream| {
                const peer = self.peers.get(stream.target_node_id) orelse continue;
                const count = stream.retransmitDue(now_ns, self.options.mtu, &frames);
                for (frames[0..count]) |frame| {
                    self.sendScannedFrame(peer, frame) catch continue;
                    self.counters.increment(self.counter_ids.retransmits_sent);
                    work_count += 1;
                }
            }
        }
        return work_count;
    }

    fn recordSendFailure(self: *Self, err: anyerror, payload_len: usize, entry: ?*SendBufferEntry) void {
        switch (err) {
            error.MalformedEnvelope => self.counters.increment(self.counter_ids.malformed_messages_dropped),
            error.UnknownPeer => self.counters.increment(self.counter_ids.unknown_peer_messages_dropped),
            error.PeerDisconnected, error.EndpointNotConfigured, error.NoDestinationEntry => self.counters.increment(self.counter_ids.peer_not_connected_drops),
            error.SenderLimitReached, error.WouldBlock => self.counters.increment(self.counter_ids.send_back_pressure),
            else => self.counters.increment(self.counter_ids.send_errors),
        }
        if (entry) |dest| {
            dest.addDropped();
            dest.storePressureState(pressureForError(err));
        }
        _ = payload_len;
    }

    fn pressureForError(err: anyerror) SendBufferPressureState {
        return switch (err) {
            error.SenderLimitReached => .flow_blocked,
            error.WouldBlock => .congested,
            error.PeerDisconnected, error.UnknownPeer, error.EndpointNotConfigured, error.NoDestinationEntry => .peer_down,
            else => .normal,
        };
    }

    pub fn addPeer(self: *Self, node_id: u8, address: udp.Address) !void {
        if (self.peers.contains(node_id)) return;
        const peer = try self.allocator.create(PeerSender);
        errdefer self.allocator.destroy(peer);
        peer.* = try PeerSender.init(node_id, address, self.allocator);
        self.peers.put(node_id, peer);
        self.counters.increment(self.counter_ids.peers_connected);
        self.publishPeerCounter(peer, Clock.monotonicNanos());
    }

    pub fn removePeer(self: *Self, node_id: u8) void {
        if (self.peers.remove(node_id)) |peer| {
            if (self.peer_send_counters) |region| region.freePeer(node_id);
            peer.deinit(self.allocator);
            self.allocator.destroy(peer);
            self.counters.increment(self.counter_ids.peers_disconnected);
        }
    }

    pub fn dispatchCommand(self: *Self, cmd: SenderCommand) void {
        switch (cmd) {
            .add_peer => |add| self.addPeer(add.node_id, add.address) catch self.counters.increment(self.counter_ids.send_errors),
            .remove_peer => |remove| self.removePeer(remove.node_id),
            .reconnect_peer => |reconnect| if (self.peers.get(reconnect.node_id)) |peer| peer.resetForReconnect(),
        }
    }

    pub fn readFromRingBuffer(self: *Self, limit: u32) u32 {
        return if (self.send_buffer_directory != null)
            self.drainDestinationBuffers(limit)
        else
            self.readLegacyRing(limit);
    }

    fn publishPeerSendCounters(self: *Self, now_ns: i64) void {
        if (self.peer_send_counters == null) return;
        var peer_iter = self.peers.iterator();
        while (peer_iter.next()) |peer| self.publishPeerCounter(peer, now_ns);
    }

    fn publishPeerCounter(self: *Self, peer: *PeerSender, now_ns: i64) void {
        const region = self.peer_send_counters orelse return;
        const entry = region.findOrAllocPeer(peer.node_id, self.options.receiver_window_length) orelse return;
        entry.storeConnectionState(peer.state == .connected);
        entry.storeQueueBytesPending(0);
        entry.storeTotalBytesSent(peer.total_bytes_sent);
        entry.storeTotalBytesDropped(peer.total_bytes_dropped);
        entry.storeLastUpdateNs(@intCast(@max(now_ns, 0)));
    }

    fn ringCost(payload_len: usize) usize {
        return alignUp(8 + payload_len, 8);
    }

    fn alignUp(value: usize, alignment: usize) usize {
        return (value + alignment - 1) & ~(alignment - 1);
    }
};

fn hashGroupName(group_name: []const u8) u32 {
    var hash: u32 = 2166136261;
    for (group_name) |byte| hash = (hash ^ byte) *% 16777619;
    return hash;
}

fn sessionId(source_node_id: u8, target_node_id: u8) u32 {
    return (@as(u32, source_node_id) << 24) | (@as(u32, target_node_id) << 16) | 2;
}

const testing = std.testing;

fn createTestCounters(
    values_buf: *align(128) [128 * 64]u8,
    meta_buf: *[256 * 64]u8,
) CountersManager {
    return CountersManager.init(values_buf, meta_buf);
}

test "SenderEventLoop init and deinit" {
    const allocator = testing.allocator;
    const rb_capacity: usize = 1024;
    const rb_buf_size = rb_capacity + constants.ring_buffer_trailer_length;
    const rb_buf = try allocator.alignedAlloc(u8, .@"8", rb_buf_size);
    defer allocator.free(rb_buf);
    @memset(rb_buf, 0);
    var rb = try RingBuffer.init(rb_buf, false, null, null);

    var values_buf: [128 * 64]u8 align(128) = [_]u8{0} ** (128 * 64);
    var meta_buf: [256 * 64]u8 align(4) = [_]u8{0} ** (256 * 64);
    var counters = createTestCounters(&values_buf, &meta_buf);

    var sender = try SenderEventLoop.init(&rb, &counters, 1, allocator);
    defer sender.deinit();

    try testing.expectEqual(@as(u8, 1), sender.local_node_id);
    try testing.expectEqual(@as(u32, 0), sender.peers.count);
}

test "addPeer and removePeer use UDP addresses" {
    const allocator = testing.allocator;
    const rb_capacity: usize = 1024;
    const rb_buf_size = rb_capacity + constants.ring_buffer_trailer_length;
    const rb_buf = try allocator.alignedAlloc(u8, .@"8", rb_buf_size);
    defer allocator.free(rb_buf);
    @memset(rb_buf, 0);
    var rb = try RingBuffer.init(rb_buf, false, null, null);

    var values_buf: [128 * 64]u8 align(128) = [_]u8{0} ** (128 * 64);
    var meta_buf: [256 * 64]u8 align(4) = [_]u8{0} ** (256 * 64);
    var counters = createTestCounters(&values_buf, &meta_buf);

    var sender = try SenderEventLoop.init(&rb, &counters, 1, allocator);
    defer sender.deinit();

    try sender.addPeer(2, .initIp4(.{ 127, 0, 0, 1 }, 9002));
    try testing.expectEqual(@as(u32, 1), sender.peers.count);
    try testing.expect(sender.peers.get(2) != null);
    sender.removePeer(2);
    try testing.expectEqual(@as(u32, 0), sender.peers.count);
}

test "onOutboundMessage drops malformed envelope" {
    const allocator = testing.allocator;
    const rb_capacity: usize = 1024;
    const rb_buf_size = rb_capacity + constants.ring_buffer_trailer_length;
    const rb_buf = try allocator.alignedAlloc(u8, .@"8", rb_buf_size);
    defer allocator.free(rb_buf);
    @memset(rb_buf, 0);
    var rb = try RingBuffer.init(rb_buf, false, null, null);

    var values_buf: [128 * 64]u8 align(128) = [_]u8{0} ** (128 * 64);
    var meta_buf: [256 * 64]u8 align(4) = [_]u8{0} ** (256 * 64);
    var counters = createTestCounters(&values_buf, &meta_buf);

    var sender = try SenderEventLoop.init(&rb, &counters, 1, allocator);
    defer sender.deinit();

    sender.onOutboundMessage(constants.message_envelope_msg_type_id, "short");
    try testing.expectEqual(@as(i64, 1), counters.get(sender.counter_ids.malformed_messages_dropped));
}
