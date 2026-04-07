//! Receiver Event Loop — the main duty-cycle loop for the inbound TCP message path.
//!
//! Single-threaded event loop that owns all incoming TCP connections from peer
//! brokers and the TCP listener socket. It runs as a duty-cycle event loop
//! following the standard BRZ pattern.
//!
//! TCP provides reliable ordered delivery, so the receiver is free from several
//! complexities that would exist in a UDP design: no receive log buffer, no gap
//! detection, no NAK generation, no fragment reassembly.

const std = @import("std");
const brz_common = @import("brz_common");
const constants = brz_common.platform.constants;
const Clock = brz_common.platform.clock.Clock;
const AtomicBool = brz_common.platform.atomic.AtomicBool;

const RingBuffer = brz_common.concurrent.ring_buffer.RingBuffer;
const CountersManager = brz_common.concurrent.counters.CountersManager;

const frame_parser = brz_common.protocol.frame_parser;
const TcpFrameHeader = frame_parser.TcpFrameHeader;

const PeerReceiver = @import("peer_receiver.zig").PeerReceiver;
const LivenessState = @import("peer_receiver.zig").LivenessState;
const ReadState = @import("peer_receiver.zig").ReadState;
const message_router = @import("message_router.zig");
const ServiceRegistry = message_router.ServiceRegistry;

// ── Counter IDs ───────────────────────────────────────────────────────

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

    pub fn allocate(counters: *CountersManager) ReceiverCounters {
        return .{
            .bytes_received = counters.allocate(1, "recv_bytes_received") orelse 0,
            .frames_routed = counters.allocate(1, "recv_frames_routed") orelse 0,
            .heartbeats_received = counters.allocate(1, "recv_heartbeats_received") orelse 0,
            .unknown_service_drops = counters.allocate(1, "recv_unknown_service_drops") orelse 0,
            .service_full_drops = counters.allocate(1, "recv_service_full_drops") orelse 0,
            .invalid_frame_drops = counters.allocate(1, "recv_invalid_frame_drops") orelse 0,
            .unknown_peer_drops = counters.allocate(1, "recv_unknown_peer_drops") orelse 0,
            .connections_accepted = counters.allocate(1, "recv_connections_accepted") orelse 0,
            .handshake_failures = counters.allocate(1, "recv_handshake_failures") orelse 0,
            .connection_errors = counters.allocate(1, "recv_connection_errors") orelse 0,
            .heartbeat_timeouts = counters.allocate(1, "recv_heartbeat_timeouts") orelse 0,
            .peer_reconnects = counters.allocate(1, "recv_peer_reconnects") orelse 0,
            .admin_messages_received = counters.allocate(1, "recv_admin_messages_received") orelse 0,
            .admin_message_errors = counters.allocate(1, "recv_admin_message_errors") orelse 0,
        };
    }
};

// ── Peer Map ──────────────────────────────────────────────────────────

const PeerMap = struct {
    entries: [256]?*PeerReceiver,
    count: u32,

    fn init() PeerMap {
        return .{
            .entries = [_]?*PeerReceiver{null} ** 256,
            .count = 0,
        };
    }

    fn get(self: *const PeerMap, node_id: u8) ?*PeerReceiver {
        return self.entries[node_id];
    }

    fn put(self: *PeerMap, node_id: u8, peer: *PeerReceiver) void {
        if (self.entries[node_id] == null) {
            self.count += 1;
        }
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
            while (self.index < 256) {
                const i = self.index;
                self.index += 1;
                if (self.map.entries[i]) |peer| {
                    return peer;
                }
            }
            return null;
        }
    };

    fn iterator(self: *const PeerMap) Iterator {
        return .{ .map = self, .index = 0 };
    }
};

// ── Receiver Event Loop ───────────────────────────────────────────────

pub const ReceiverEventLoop = struct {
    /// Connected peers, keyed by node ID.
    peers: PeerMap,

    /// Service registry: serviceId → service ring buffer.
    service_registry: *ServiceRegistry,

    /// Shared counters manager for observability.
    counters: *CountersManager,

    /// Well-known counter slot IDs.
    counter_ids: ReceiverCounters,

    /// This broker's node ID.
    local_node_id: u8,

    /// Whether this event loop is running.
    running: AtomicBool,

    /// Allocator for dynamic peer allocation.
    allocator: std.mem.Allocator,

    const Self = @This();

    pub fn init(
        service_registry: *ServiceRegistry,
        counters: *CountersManager,
        local_node_id: u8,
        allocator: std.mem.Allocator,
    ) Self {
        return .{
            .peers = PeerMap.init(),
            .service_registry = service_registry,
            .counters = counters,
            .counter_ids = ReceiverCounters.allocate(counters),
            .local_node_id = local_node_id,
            .running = AtomicBool.init(true),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        var iter = self.peers.iterator();
        while (iter.next()) |peer| {
            self.allocator.free(peer.read_state.payload_buf);
            peer.close();
            self.allocator.destroy(peer);
        }
    }

    // ── Duty Cycle ────────────────────────────────────────────────────

    /// Called by the ThreadRunner on every iteration.
    pub fn doWork(self: *Self) u32 {
        var work_count: u32 = 0;
        const now_ns = Clock.monotonicNanos();

        // ── Phase 1: Check heartbeat timeouts ─────────────────────────
        work_count += self.checkHeartbeatTimeouts(now_ns);

        return work_count;
    }

    // ── Frame Processing ──────────────────────────────────────────────

    /// Process a complete frame received from a peer.
    pub fn processCompleteFrame(
        self: *Self,
        source_node_id: u8,
        header_buf: *const [TcpFrameHeader.size]u8,
        payload: []const u8,
    ) void {
        const header: *const TcpFrameHeader = @ptrCast(@alignCast(header_buf));
        const now_ns = Clock.monotonicNanos();

        // Update peer's last received timestamp
        if (self.peers.get(source_node_id)) |peer| {
            peer.last_recv_ns = now_ns;
            peer.liveness = .alive;
        } else {
            self.counters.increment(self.counter_ids.unknown_peer_drops);
            return;
        }

        self.counters.add(self.counter_ids.bytes_received, @intCast(TcpFrameHeader.size + payload.len));

        // Validate source_node_id matches the peer
        if (header.source_node_id != source_node_id) {
            self.counters.increment(self.counter_ids.invalid_frame_drops);
            return;
        }

        // Validate target_node_id matches this broker
        if (header.target_node_id != self.local_node_id) {
            self.counters.increment(self.counter_ids.invalid_frame_drops);
            return;
        }

        // Classify and route
        if (header.isHeartbeat()) {
            self.counters.increment(self.counter_ids.heartbeats_received);
            return;
        }

        if (header.isAdmin()) {
            self.handleAdminMessage(header, payload);
            return;
        }

        // Route to target service
        const frame_length = header.frame_length;
        // Build the complete frame: header + payload
        var frame_buf: [constants.default_max_frame_length]u8 align(8) = undefined;
        if (frame_length > constants.default_max_frame_length) {
            self.counters.increment(self.counter_ids.invalid_frame_drops);
            return;
        }
        const fl: usize = @intCast(frame_length);
        @memcpy(frame_buf[0..TcpFrameHeader.size], header_buf);
        if (payload.len > 0) {
            @memcpy(frame_buf[TcpFrameHeader.size..][0..payload.len], payload);
        }

        const result = message_router.routeToService(
            self.service_registry,
            header.target_service_id,
            frame_buf[0..fl],
            frame_length,
        );
        switch (result) {
            .success => self.counters.increment(self.counter_ids.frames_routed),
            .unknown_service => self.counters.increment(self.counter_ids.unknown_service_drops),
            .service_full => self.counters.increment(self.counter_ids.service_full_drops),
        }
    }

    // ── Admin Messages ────────────────────────────────────────────────

    fn handleAdminMessage(self: *Self, header: *const TcpFrameHeader, payload: []const u8) void {
        _ = payload;
        const broker_service = self.service_registry.lookup(@intCast(constants.broker_service_id)) orelse {
            self.counters.increment(self.counter_ids.admin_message_errors);
            return;
        };

        const frame_len: usize = @intCast(header.frame_length);
        const header_bytes: *const [TcpFrameHeader.size]u8 = @ptrCast(header);
        broker_service.messages_ring_buffer.write(2, header_bytes[0..@min(frame_len, TcpFrameHeader.size)]) catch {
            self.counters.increment(self.counter_ids.admin_message_errors);
            return;
        };

        self.counters.increment(self.counter_ids.admin_messages_received);
    }

    // ── Heartbeat Timeout Detection ───────────────────────────────────

    fn checkHeartbeatTimeouts(self: *Self, now_ns: i64) u32 {
        var work_count: u32 = 0;
        var peer_iter = self.peers.iterator();

        while (peer_iter.next()) |peer| {
            const prev_liveness = peer.liveness;
            const new_liveness = peer.updateLiveness(now_ns);

            if (new_liveness == .dead and prev_liveness != .dead) {
                self.counters.increment(self.counter_ids.heartbeat_timeouts);
                work_count += 1;
            }
        }

        return work_count;
    }

    // ── Peer Lifecycle ────────────────────────────────────────────────

    pub fn addPeer(
        self: *Self,
        node_id: u8,
        socket_fd: std.posix.fd_t,
        address: std.net.Address,
        session_epoch: u32,
    ) !void {
        // Handle reconnection — replace existing peer
        if (self.peers.get(node_id)) |existing| {
            existing.resetForReconnect(socket_fd, address, session_epoch);
            self.counters.increment(self.counter_ids.peer_reconnects);
            return;
        }

        const payload_buf = try self.allocator.alloc(u8, constants.default_max_frame_length);
        errdefer self.allocator.free(payload_buf);

        const peer = try self.allocator.create(PeerReceiver);
        errdefer self.allocator.destroy(peer);

        peer.* = PeerReceiver.init(node_id, socket_fd, address, session_epoch, payload_buf);
        self.peers.put(node_id, peer);
        self.counters.increment(self.counter_ids.connections_accepted);
    }

    pub fn removePeer(self: *Self, node_id: u8) void {
        if (self.peers.remove(node_id)) |peer| {
            peer.close();
            self.allocator.free(peer.read_state.payload_buf);
            self.allocator.destroy(peer);
        }
    }

    pub fn lookupPeer(self: *Self, node_id: u8) ?*PeerReceiver {
        return self.peers.get(node_id);
    }
};

// ── Tests ─────────────────────────────────────────────────────────────

const testing = std.testing;

fn createTestCounters(
    values_buf: *align(128) [128 * 64]u8,
    meta_buf: *[256 * 64]u8,
) CountersManager {
    return CountersManager.init(
        values_buf,
        meta_buf,
    );
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

test "addPeer and removePeer" {
    const allocator = testing.allocator;
    var registry = ServiceRegistry.init();

    var values_buf: [128 * 64]u8 align(128) = [_]u8{0} ** (128 * 64);
    var meta_buf: [256 * 64]u8 align(4) = [_]u8{0} ** (256 * 64);
    var counters = createTestCounters(&values_buf, &meta_buf);

    var recv_loop = ReceiverEventLoop.init(&registry, &counters, 1, allocator);
    defer recv_loop.deinit();

    const addr = std.net.Address.initIp4(.{ 127, 0, 0, 1 }, 9001);
    try recv_loop.addPeer(2, -1, addr, 1);

    try testing.expectEqual(@as(u32, 1), recv_loop.peers.count);
    try testing.expect(recv_loop.lookupPeer(2) != null);

    recv_loop.removePeer(2);
    try testing.expectEqual(@as(u32, 0), recv_loop.peers.count);
    try testing.expect(recv_loop.lookupPeer(2) == null);
}

test "processCompleteFrame drops frame from unknown peer" {
    const allocator = testing.allocator;
    var registry = ServiceRegistry.init();

    var values_buf: [128 * 64]u8 align(128) = [_]u8{0} ** (128 * 64);
    var meta_buf: [256 * 64]u8 align(4) = [_]u8{0} ** (256 * 64);
    var counters = createTestCounters(&values_buf, &meta_buf);

    var recv_loop = ReceiverEventLoop.init(&registry, &counters, 1, allocator);
    defer recv_loop.deinit();

    var header_buf: [TcpFrameHeader.size]u8 align(@alignOf(TcpFrameHeader)) = [_]u8{0} ** TcpFrameHeader.size;
    const header: *TcpFrameHeader = @ptrCast(@alignCast(&header_buf));
    header.* = .{
        .frame_length = 24,
        .source_node_id = 5,
        .target_node_id = 1,
    };

    recv_loop.processCompleteFrame(5, &header_buf, &.{});

    try testing.expectEqual(@as(i64, 1), counters.get(recv_loop.counter_ids.unknown_peer_drops));
}

test "processCompleteFrame handles heartbeat" {
    const allocator = testing.allocator;
    var registry = ServiceRegistry.init();

    var values_buf: [128 * 64]u8 align(128) = [_]u8{0} ** (128 * 64);
    var meta_buf: [256 * 64]u8 align(4) = [_]u8{0} ** (256 * 64);
    var counters = createTestCounters(&values_buf, &meta_buf);

    var recv_loop = ReceiverEventLoop.init(&registry, &counters, 1, allocator);
    defer recv_loop.deinit();

    const addr = std.net.Address.initIp4(.{ 127, 0, 0, 1 }, 9001);
    try recv_loop.addPeer(2, -1, addr, 1);

    var header_buf: [TcpFrameHeader.size]u8 align(@alignOf(TcpFrameHeader)) = [_]u8{0} ** TcpFrameHeader.size;
    const header: *TcpFrameHeader = @ptrCast(@alignCast(&header_buf));
    header.* = .{
        .frame_length = 24,
        .flags = 0x01, // heartbeat flag
        .source_node_id = 2,
        .target_node_id = 1,
        .template_id = 0xFFFF,
    };

    recv_loop.processCompleteFrame(2, &header_buf, &.{});

    try testing.expectEqual(@as(i64, 1), counters.get(recv_loop.counter_ids.heartbeats_received));
}

test "processCompleteFrame routes data to service" {
    const allocator = testing.allocator;

    var rb_buf: [4096 + 768]u8 align(8) = undefined;
    @memset(&rb_buf, 0);
    var rb = RingBuffer.init(&rb_buf, false, null, null) catch unreachable;

    var registry = ServiceRegistry.init();
    registry.register(.{
        .service_id = 10,
        .service_name = "test-service",
        .node_id = 1,
        .messages_ring_buffer = &rb,
    });

    var values_buf: [128 * 64]u8 align(128) = [_]u8{0} ** (128 * 64);
    var meta_buf: [256 * 64]u8 align(4) = [_]u8{0} ** (256 * 64);
    var counters = createTestCounters(&values_buf, &meta_buf);

    var recv_loop = ReceiverEventLoop.init(&registry, &counters, 1, allocator);
    defer recv_loop.deinit();

    const addr = std.net.Address.initIp4(.{ 127, 0, 0, 1 }, 9001);
    try recv_loop.addPeer(2, -1, addr, 1);

    var header_buf: [TcpFrameHeader.size]u8 align(@alignOf(TcpFrameHeader)) = [_]u8{0} ** TcpFrameHeader.size;
    const header: *TcpFrameHeader = @ptrCast(@alignCast(&header_buf));
    header.* = .{
        .frame_length = 32,
        .source_node_id = 2,
        .target_node_id = 1,
        .target_service_id = 10,
        .template_id = 42,
    };

    const payload = [_]u8{ 1, 2, 3, 4, 5, 6, 7, 8 };
    recv_loop.processCompleteFrame(2, &header_buf, &payload);

    try testing.expectEqual(@as(i64, 1), counters.get(recv_loop.counter_ids.frames_routed));
}

test "doWork returns 0 when no peers connected" {
    const allocator = testing.allocator;
    var registry = ServiceRegistry.init();

    var values_buf: [128 * 64]u8 align(128) = [_]u8{0} ** (128 * 64);
    var meta_buf: [256 * 64]u8 align(4) = [_]u8{0} ** (256 * 64);
    var counters = createTestCounters(&values_buf, &meta_buf);

    var recv_loop = ReceiverEventLoop.init(&registry, &counters, 1, allocator);
    defer recv_loop.deinit();

    const work = recv_loop.doWork();
    try testing.expectEqual(@as(u32, 0), work);
}
