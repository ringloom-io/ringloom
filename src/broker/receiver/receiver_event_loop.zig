//! Receiver Event Loop — the main duty-cycle loop for the inbound TCP message path.
//!
//! Single-threaded event loop that owns all incoming TCP connections from peer
//! brokers and the TCP listener socket. It runs as a duty-cycle event loop
//! following the standard RingLoom pattern.
//!

const std = @import("std");
const ringloom_common = @import("ringloom_common");
const net = @import("ringloom_tcp").socket;
const constants = ringloom_common.platform.constants;
const Clock = ringloom_common.platform.clock.Clock;
const AtomicBool = ringloom_common.platform.atomic.AtomicBool;
const platform = ringloom_common.platform;

const RingBuffer = ringloom_common.concurrent.ring_buffer.RingBuffer;
const CountersManager = ringloom_common.concurrent.counters.CountersManager;

const frame_parser = ringloom_common.protocol.frame_parser;
const TcpFrameHeader = frame_parser.TcpFrameHeader;

const tcp = @import("ringloom_tcp");
const HandshakeFrame = tcp.HandshakeFrame;
const SocketConfig = tcp.SocketConfig;

const PeerReceiver = @import("peer_receiver.zig").PeerReceiver;
const LivenessState = @import("peer_receiver.zig").LivenessState;
const ReadState = @import("peer_receiver.zig").ReadState;
const message_router = @import("message_router.zig");
const ServiceRegistry = message_router.ServiceRegistry;
const latency_trace = ringloom_common.message.latency_trace;

const admin_dispatch = @import("../cluster/admin_dispatch.zig");
const AdminCommandQueue = admin_dispatch.AdminCommandQueue;

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

    /// TCP listener socket file descriptor (-1 if not listening).
    listener_fd: std.posix.fd_t,

    /// FNV-1a hash of the cluster group name (for handshake validation).
    group_name_hash: u32,

    /// Pending connections awaiting handshake completion.
    pending_connections: [max_pending]PendingConnection,
    pending_count: u32,

    /// Admin command queue — forward admin messages to control loop.
    admin_cmd_queue: ?*AdminCommandQueue(64),

    /// Enable benchmark-only payload timestamp tracing. Disabled in production.
    benchmark_latency_tracing_enabled: bool,

    const Self = @This();
    const max_pending = 16;

    const PendingConnection = struct {
        fd: std.posix.fd_t,
        address: net.Address,
        handshake_buf: [HandshakeFrame.size]u8,
        bytes_read: u8,
    };

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
        return .{
            .peers = PeerMap.init(),
            .service_registry = service_registry,
            .counters = counters,
            .counter_ids = ReceiverCounters.allocate(counters),
            .local_node_id = local_node_id,
            .running = AtomicBool.init(true),
            .allocator = allocator,
            .listener_fd = -1,
            .group_name_hash = HandshakeFrame.hashGroupName(group_name),
            .pending_connections = undefined,
            .pending_count = 0,
            .admin_cmd_queue = admin_queue,
            .benchmark_latency_tracing_enabled = benchmark_latency_tracing_enabled,
        };
    }

    pub fn deinit(self: *Self) void {
        // Close listener.
        if (self.listener_fd >= 0) {
            platform.closeFd(self.listener_fd);
            self.listener_fd = -1;
        }
        // Close pending connections.
        for (self.pending_connections[0..self.pending_count]) |*pc| {
            if (pc.fd >= 0) platform.closeFd(pc.fd);
        }
        self.pending_count = 0;
        // Close and free peers.
        var iter = self.peers.iterator();
        while (iter.next()) |peer| {
            self.allocator.free(peer.read_state.payload_buf);
            self.allocator.free(peer.recv_buf);
            peer.close();
            self.allocator.destroy(peer);
        }
    }

    // ── Listener Setup ───────────────────────────────────────────────

    pub fn initListener(self: *Self, host: []const u8, port: u16) !void {
        const addr = try net.Address.parseIp4(host, port);
        const fd = try net.socket(
            addr.any.family,
            std.posix.SOCK.STREAM | std.posix.SOCK.NONBLOCK | std.posix.SOCK.CLOEXEC,
            0,
        );
        errdefer platform.closeFd(fd);

        const cfg = SocketConfig{ .reuse_addr = true, .tcp_nodelay = false, .keepalive = false };
        try cfg.apply(fd);

        try net.bind(fd, &addr.any, addr.getOsSockLen());
        try net.listen(fd, 128);

        self.listener_fd = fd;
    }

    // ── Duty Cycle ────────────────────────────────────────────────────

    /// Called by the ThreadRunner on every iteration.
    pub fn doWork(self: *Self) u32 {
        var work_count: u32 = 0;
        const now_ns = Clock.monotonicNanos();

        // ── Phase 1: Accept new connections ───────────────────────────
        work_count += self.acceptNewConnections();

        // ── Phase 2: Complete pending handshakes ──────────────────────
        work_count += self.processPendingHandshakes();

        // ── Phase 3: Read from connected peers ───────────────────────
        work_count += self.readFromPeers();

        // ── Phase 4: Read again to catch data that arrived during
        //             frame parsing (mirrors sender double-flush) ──────
        work_count += self.readFromPeers();

        // ── Phase 5: Check heartbeat timeouts ─────────────────────────
        work_count += self.checkHeartbeatTimeouts(now_ns);

        return work_count;
    }

    // ── TCP Accept ────────────────────────────────────────────────────

    fn acceptNewConnections(self: *Self) u32 {
        if (self.listener_fd < 0) return 0;
        var accepted: u32 = 0;

        while (self.pending_count < max_pending) {
            var addr: net.Address = undefined;
            var addr_len: std.posix.socklen_t = @sizeOf(std.posix.sockaddr);
            const fd = net.accept(
                self.listener_fd,
                &addr.any,
                &addr_len,
                std.posix.SOCK.NONBLOCK | std.posix.SOCK.CLOEXEC,
            ) catch |err| {
                switch (err) {
                    error.WouldBlock => break,
                    else => break,
                }
            };

            const sock_cfg = SocketConfig{};
            sock_cfg.apply(fd) catch {
                platform.closeFd(fd);
                continue;
            };

            self.pending_connections[self.pending_count] = .{
                .fd = fd,
                .address = addr,
                .handshake_buf = std.mem.zeroes([HandshakeFrame.size]u8),
                .bytes_read = 0,
            };
            self.pending_count += 1;
            accepted += 1;
        }

        return accepted;
    }

    // ── Pending Handshake Processing ─────────────────────────────────

    fn processPendingHandshakes(self: *Self) u32 {
        var completed: u32 = 0;
        var i: u32 = 0;

        while (i < self.pending_count) {
            var pc = &self.pending_connections[i];
            const remaining = pc.handshake_buf[pc.bytes_read..];
            const n = std.posix.read(pc.fd, remaining) catch |err| {
                switch (err) {
                    error.WouldBlock => {
                        i += 1;
                        continue;
                    },
                    else => {
                        platform.closeFd(pc.fd);
                        self.removePendingAt(i);
                        self.counters.increment(self.counter_ids.handshake_failures);
                        continue;
                    },
                }
            };

            if (n == 0) {
                // EOF before handshake complete.
                platform.closeFd(pc.fd);
                self.removePendingAt(i);
                self.counters.increment(self.counter_ids.handshake_failures);
                continue;
            }

            pc.bytes_read += @intCast(n);

            if (pc.bytes_read >= HandshakeFrame.size) {
                // Handshake complete — validate.
                const frame = HandshakeFrame.fromBytes(&pc.handshake_buf).*;
                HandshakeFrame.validate(frame, self.local_node_id, self.group_name_hash) catch {
                    platform.closeFd(pc.fd);
                    self.removePendingAt(i);
                    self.counters.increment(self.counter_ids.handshake_failures);
                    continue;
                };

                // Valid — add as peer.
                const epoch: u32 = @truncate(frame.session_epoch);
                self.addPeer(frame.source_node_id, pc.fd, pc.address, epoch) catch {
                    platform.closeFd(pc.fd);
                    self.removePendingAt(i);
                    self.counters.increment(self.counter_ids.connection_errors);
                    continue;
                };

                self.removePendingAt(i);
                completed += 1;
                continue;
            }

            i += 1;
        }

        return completed;
    }

    fn removePendingAt(self: *Self, index: u32) void {
        if (self.pending_count > 0) {
            self.pending_connections[index] = self.pending_connections[self.pending_count - 1];
            self.pending_count -= 1;
        }
    }

    // ── TCP Read ──────────────────────────────────────────────────────

    fn readFromPeers(self: *Self) u32 {
        var work_count: u32 = 0;

        var peer_iter = self.peers.iterator();
        while (peer_iter.next()) |peer| {
            if (!peer.connected or peer.socket_fd < 0) continue;

            // Fill recv buffer with one big read from the socket.
            // This amortizes syscall overhead across many frames.
            var got_new_data = false;
            if (peer.fillRecvBuffer()) |n| {
                if (n == 0 and peer.recvBufAvailable() == 0) {
                    // EOF with no buffered data.
                    self.disconnectPeer(peer);
                    continue;
                }
                if (n > 0) got_new_data = true;
            } else |err| {
                switch (err) {
                    error.WouldBlock => {},
                    else => {
                        self.disconnectPeer(peer);
                        continue;
                    },
                }
            }

            // Parse frames from the recv buffer (no more syscalls).
            if (got_new_data or peer.recvBufAvailable() > 0) {
                var budget: u32 = constants.read_budget_per_peer;
                while (budget > 0) : (budget -= 1) {
                    const read_result = self.readOnePeerStep(peer);
                    if (!read_result.progress) break;
                    work_count += read_result.frames_completed;
                }
            }
        }

        return work_count;
    }

    const ReadResult = struct { progress: bool, frames_completed: u32 };

    fn readOnePeerStep(self: *Self, peer: *PeerReceiver) ReadResult {
        var rs = &peer.read_state;

        switch (rs.phase) {
            .reading_header => {
                const remaining = rs.header_buf[rs.header_bytes_read..];
                const n = peer.readFromBuffer(remaining);
                if (n == 0) {
                    return .{ .progress = false, .frames_completed = 0 };
                }

                rs.header_bytes_read += @intCast(n);
                if (rs.header_bytes_read < TcpFrameHeader.size) {
                    return .{ .progress = true, .frames_completed = 0 };
                }

                // Header complete — extract frame_length.
                const header: *const TcpFrameHeader = @ptrCast(@alignCast(&rs.header_buf));
                rs.frame_length = header.frame_length;

                if (rs.frame_length < TcpFrameHeader.size or rs.frame_length > constants.tcp_max_frame_length) {
                    self.disconnectPeer(peer);
                    self.counters.increment(self.counter_ids.invalid_frame_drops);
                    return .{ .progress = false, .frames_completed = 0 };
                }

                const payload_len = rs.payloadLength();
                if (payload_len == 0) {
                    // Header-only frame (e.g. heartbeat).
                    self.processCompleteFrame(peer.node_id, &rs.header_buf, &.{});
                    rs.reset();
                    return .{ .progress = true, .frames_completed = 1 };
                }

                rs.phase = .reading_payload;
                rs.payload_bytes_read = 0;
                return .{ .progress = true, .frames_completed = 0 };
            },
            .reading_payload => {
                const payload_len = rs.payloadLength();
                const remaining = rs.payload_buf[rs.payload_bytes_read..payload_len];
                const n = peer.readFromBuffer(remaining);
                if (n == 0) {
                    return .{ .progress = false, .frames_completed = 0 };
                }

                rs.payload_bytes_read += @intCast(n);
                if (rs.payload_bytes_read < payload_len) {
                    return .{ .progress = true, .frames_completed = 0 };
                }

                // Frame complete.
                self.processCompleteFrame(peer.node_id, &rs.header_buf, rs.payload_buf[0..payload_len]);
                rs.reset();
                return .{ .progress = true, .frames_completed = 1 };
            },
        }
    }

    fn disconnectPeer(self: *Self, peer: *PeerReceiver) void {
        peer.close();
        self.counters.increment(self.counter_ids.connection_errors);
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

        if (self.benchmark_latency_tracing_enabled) {
            latency_trace.stampReceiverIngress(@constCast(payload), @intCast(Clock.monotonicNanosStable()));
        }

        // Route application payload to target service. The TcpFrameHeader is
        // stripped so the service sees the same payload format as local IPC.
        const result = message_router.routeToService(
            self.service_registry,
            header.target_service_id,
            payload,
        );
        switch (result) {
            .success => self.counters.increment(self.counter_ids.frames_routed),
            .unknown_service => self.counters.increment(self.counter_ids.unknown_service_drops),
            .service_full => self.counters.increment(self.counter_ids.service_full_drops),
        }
    }

    // ── Admin Messages ────────────────────────────────────────────────

    fn handleAdminMessage(self: *Self, header: *const TcpFrameHeader, payload: []const u8) void {
        const queue = self.admin_cmd_queue orelse {
            self.counters.increment(self.counter_ids.admin_message_errors);
            return;
        };

        // Validate source_node_id from TCP header matches admin payload
        admin_dispatch.dispatchAdminMessage(
            payload,
            queue,
            Clock.monotonicNanos(),
            header.source_node_id,
        );

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
        address: net.Address,
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

        const recv_buf = try self.allocator.alloc(u8, PeerReceiver.recv_buf_size);
        errdefer self.allocator.free(recv_buf);

        const peer = try self.allocator.create(PeerReceiver);
        errdefer self.allocator.destroy(peer);

        peer.* = PeerReceiver.init(node_id, socket_fd, address, session_epoch, payload_buf, recv_buf);
        self.peers.put(node_id, peer);
        self.counters.increment(self.counter_ids.connections_accepted);

        // Notify control loop of new peer so it re-broadcasts local services.
        if (self.admin_cmd_queue) |q| {
            _ = q.enqueue(.{ .peer_connected = .{ .node_id = node_id } });
        }
    }

    pub fn removePeer(self: *Self, node_id: u8) void {
        if (self.peers.remove(node_id)) |peer| {
            peer.close();
            self.allocator.free(peer.read_state.payload_buf);
            self.allocator.free(peer.recv_buf);
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

    const addr = net.Address.initIp4(.{ 127, 0, 0, 1 }, 9001);
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

    const addr = net.Address.initIp4(.{ 127, 0, 0, 1 }, 9001);
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

    const addr = net.Address.initIp4(.{ 127, 0, 0, 1 }, 9001);
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
