//! Receiver Event Loop — the main duty-cycle loop for the inbound TCP message path.
//!
//! Single-threaded event loop that owns all incoming TCP connections from peer
//! brokers and the TCP listener socket. It runs as a duty-cycle event loop
//! following the standard RingLoom pattern.
//!

const std = @import("std");
const builtin = @import("builtin");
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
const transport = @import("../transport.zig");

const PeerReceiver = @import("peer_receiver.zig").PeerReceiver;
const LivenessState = @import("peer_receiver.zig").LivenessState;
const ReadState = @import("peer_receiver.zig").ReadState;
const message_router = @import("message_router.zig");
const ServiceRegistry = message_router.ServiceRegistry;
const latency_trace = ringloom_common.message.latency_trace;

const admin_dispatch = @import("../cluster/admin_dispatch.zig");
const AdminCommandQueue = admin_dispatch.AdminCommandQueue;
const linux = std.os.linux;
const log = std.log.scoped(.receiver);

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
    sync_read_calls: usize = 0,
    sync_read_bytes: usize = 0,
    iouring_enabled: usize = 0,
    iouring_fallbacks: usize = 0,
    iouring_errors: usize = 0,
    iouring_cqes: usize = 0,
    iouring_accept_cqes: usize = 0,
    iouring_recv_cqes: usize = 0,
    iouring_recv_bytes: usize = 0,
    iouring_accept_rearms: usize = 0,
    iouring_recv_rearms: usize = 0,
    iouring_buffer_starvations: usize = 0,
    iouring_fallback_unsupported: usize = 0,
    iouring_fallback_init_errors: usize = 0,
    iouring_fallback_runtime_errors: usize = 0,
    iouring_recv_copied_bytes: usize = 0,

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
            .sync_read_calls = counters.allocate(1, "recv_sync_read_calls") orelse 0,
            .sync_read_bytes = counters.allocate(1, "recv_sync_read_bytes") orelse 0,
            .iouring_enabled = counters.allocate(1, "recv_iouring_enabled") orelse 0,
            .iouring_fallbacks = counters.allocate(1, "recv_iouring_fallbacks") orelse 0,
            .iouring_errors = counters.allocate(1, "recv_iouring_errors") orelse 0,
            .iouring_cqes = counters.allocate(1, "recv_iouring_cqes") orelse 0,
            .iouring_accept_cqes = counters.allocate(1, "recv_iouring_accept_cqes") orelse 0,
            .iouring_recv_cqes = counters.allocate(1, "recv_iouring_recv_cqes") orelse 0,
            .iouring_recv_bytes = counters.allocate(1, "recv_iouring_recv_bytes") orelse 0,
            .iouring_accept_rearms = counters.allocate(1, "recv_iouring_accept_rearms") orelse 0,
            .iouring_recv_rearms = counters.allocate(1, "recv_iouring_recv_rearms") orelse 0,
            .iouring_buffer_starvations = counters.allocate(1, "recv_iouring_buffer_starvations") orelse 0,
            .iouring_fallback_unsupported = counters.allocate(1, "recv_iouring_fallback_unsupported") orelse 0,
            .iouring_fallback_init_errors = counters.allocate(1, "recv_iouring_fallback_init_errors") orelse 0,
            .iouring_fallback_runtime_errors = counters.allocate(1, "recv_iouring_fallback_runtime_errors") orelse 0,
            .iouring_recv_copied_bytes = counters.allocate(1, "recv_iouring_recv_copied_bytes") orelse 0,
        };
    }
};

pub const ReceiverIoUringOptions = struct {
    enabled: bool = false,
    queue_depth: u16 = 256,
    cq_depth: u32 = 1024,
    sqpoll: bool = false,
    single_issuer: bool = true,
    coop_taskrun: bool = true,
    cqe_batch_size: u32 = 256,
    recv_buffer_size: u32 = 16 * 1024,
    recv_buffer_count: u16 = 256,
};

const ReceiverIoUringState = struct {
    ring: transport.IoUring,
    recv_group: linux.IoUring.BufferGroup,
    accept_armed: bool = false,
    cqe_buf: [256]linux.io_uring_cqe = undefined,
};

const ReceiverIoUringFallbackPhase = enum {
    init,
    runtime,
};

const ReceiverIoOp = enum(u8) {
    accept = 1,
    recv = 2,
};

fn encodeIoUserData(op: ReceiverIoOp, node_id: u8, generation: u8) u64 {
    return @as(u64, @intFromEnum(op)) |
        (@as(u64, node_id) << 8) |
        (@as(u64, generation) << 16);
}

fn decodeIoUserData(user_data: u64) struct { op: ReceiverIoOp, node_id: u8, generation: u8 } {
    return .{
        .op = @enumFromInt(@as(u8, @truncate(user_data))),
        .node_id = @truncate(user_data >> 8),
        .generation = @truncate(user_data >> 16),
    };
}

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

    /// Requested io_uring receiver settings. Disabled by default.
    iouring_options: ReceiverIoUringOptions,

    /// Active io_uring receiver state, if initialization and feature probing succeeded.
    iouring_state: ?ReceiverIoUringState,

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
            .iouring_options = iouring_options,
            .iouring_state = null,
        };
    }

    pub fn deinit(self: *Self) void {
        // Close listener.
        if (self.listener_fd >= 0) {
            platform.closeFd(self.listener_fd);
            self.listener_fd = -1;
        }
        if (self.iouring_state) |*state| {
            state.recv_group.deinit(self.allocator);
            state.ring.deinit();
            self.iouring_state = null;
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

        if (self.iouring_options.enabled) {
            self.enableIoUringReceiver() catch |err| {
                self.recordIoUringFallback(err, .init);
            };
        }
    }

    fn enableIoUringReceiver(self: *Self) !void {
        if (comptime builtin.os.tag != .linux) return error.UnsupportedPlatform;
        if (self.listener_fd < 0) return error.InvalidListener;

        var ring = try transport.IoUring.initWithConfig(.{
            .queue_depth = self.iouring_options.queue_depth,
            .cq_depth = self.iouring_options.cq_depth,
            .sqpoll = self.iouring_options.sqpoll,
            .single_issuer = self.iouring_options.single_issuer,
            .coop_taskrun = self.iouring_options.coop_taskrun,
        });
        var ring_moved_to_state = false;
        errdefer if (!ring_moved_to_state) ring.deinit();

        if (!ring.capabilities.accept_supported or !ring.capabilities.recv_supported) {
            return error.IoUringTcpOpsUnsupported;
        }

        self.iouring_state = .{
            .ring = ring,
            .recv_group = undefined,
            .accept_armed = false,
        };
        ring_moved_to_state = true;
        errdefer {
            if (self.iouring_state) |*state| {
                state.ring.deinit();
            }
            self.iouring_state = null;
        }

        var state = &self.iouring_state.?;
        state.recv_group = try linux.IoUring.BufferGroup.init(
            &state.ring.ring,
            self.allocator,
            0,
            self.iouring_options.recv_buffer_size,
            self.iouring_options.recv_buffer_count,
        );
        errdefer state.recv_group.deinit(self.allocator);

        try self.armIoUringAccept();
        self.counters.increment(self.counter_ids.iouring_enabled);
    }

    fn disableIoUringReceiver(self: *Self, err: anyerror) void {
        self.recordIoUringFallback(err, .runtime);
        if (self.iouring_state) |*state| {
            state.recv_group.deinit(self.allocator);
            state.ring.deinit();
            self.iouring_state = null;
        }
        var iter = self.peers.iterator();
        while (iter.next()) |peer| {
            peer.io_recv_armed = false;
        }
    }

    fn logIoUringFallback(err: anyerror) void {
        log.warn("receiver io_uring disabled; falling back to synchronous TCP: {}", .{err});
    }

    fn recordIoUringFallback(self: *Self, err: anyerror, phase: ReceiverIoUringFallbackPhase) void {
        self.counters.increment(self.counter_ids.iouring_fallbacks);
        switch (err) {
            error.UnsupportedPlatform,
            error.IoUringTcpOpsUnsupported,
            error.MultishotAcceptUnsupported,
            => self.counters.increment(self.counter_ids.iouring_fallback_unsupported),
            else => switch (phase) {
                .init => self.counters.increment(self.counter_ids.iouring_fallback_init_errors),
                .runtime => self.counters.increment(self.counter_ids.iouring_fallback_runtime_errors),
            },
        }
        logIoUringFallback(err);
    }

    fn armIoUringAccept(self: *Self) !void {
        var state = if (self.iouring_state) |*s| s else return error.IoUringNotActive;
        try state.ring.prepareAcceptMultishot(
            self.listener_fd,
            encodeIoUserData(.accept, 0, 0),
            std.posix.SOCK.NONBLOCK | std.posix.SOCK.CLOEXEC,
        );
        _ = try state.ring.submit();
        state.accept_armed = true;
        self.counters.increment(self.counter_ids.iouring_accept_rearms);
    }

    fn armIoUringRecv(self: *Self, peer: *PeerReceiver) !void {
        var state = if (self.iouring_state) |*s| s else return error.IoUringNotActive;
        if (peer.io_recv_armed or !peer.connected or peer.socket_fd < 0) return;
        try state.ring.prepareRecvMultishot(
            &state.recv_group,
            peer.socket_fd,
            encodeIoUserData(.recv, peer.node_id, peer.io_generation),
            0,
        );
        _ = try state.ring.submit();
        peer.io_recv_armed = true;
        self.counters.increment(self.counter_ids.iouring_recv_rearms);
    }

    // ── Duty Cycle ────────────────────────────────────────────────────

    /// Called by the ThreadRunner on every iteration.
    pub fn doWork(self: *Self) u32 {
        var work_count: u32 = 0;
        const now_ns = Clock.monotonicNanos();

        if (self.iouring_state != null) {
            // ── Phase 1: Harvest io_uring accept/recv completions ─────
            work_count += self.pollIoUringCompletions();

            // ── Phase 2: Complete pending handshakes ──────────────────
            work_count += self.processPendingHandshakes();

            // ── Phase 3: Harvest again to catch data that arrived while
            //             parsing handshakes/routing prior completions.
            work_count += self.pollIoUringCompletions();
        } else {
            // ── Phase 1: Accept new connections ───────────────────────
            work_count += self.acceptNewConnections();

            // ── Phase 2: Complete pending handshakes ──────────────────
            work_count += self.processPendingHandshakes();

            // ── Phase 3: Read from connected peers ───────────────────
            work_count += self.readFromPeers();

            // ── Phase 4: Read again to catch data that arrived during
            //             frame parsing (mirrors sender double-flush) ──
            work_count += self.readFromPeers();
        }

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

    fn pollIoUringCompletions(self: *Self) u32 {
        var state = if (self.iouring_state) |*s| s else return 0;
        const limit = @min(self.iouring_options.cqe_batch_size, @as(u32, @intCast(state.cqe_buf.len)));
        const count = state.ring.ring.copy_cqes(state.cqe_buf[0..limit], 0) catch |err| {
            self.counters.increment(self.counter_ids.iouring_errors);
            self.disableIoUringReceiver(err);
            return 0;
        };
        if (count == 0) return 0;

        self.counters.add(self.counter_ids.iouring_cqes, count);

        var work_count: u32 = 0;
        for (state.cqe_buf[0..count]) |cqe| {
            const decoded = decodeIoUserData(cqe.user_data);
            switch (decoded.op) {
                .accept => {
                    self.counters.increment(self.counter_ids.iouring_accept_cqes);
                    work_count += self.handleIoUringAcceptCqe(cqe);
                },
                .recv => {
                    self.counters.increment(self.counter_ids.iouring_recv_cqes);
                    work_count += self.handleIoUringRecvCqe(cqe, decoded.node_id, decoded.generation);
                },
            }
        }

        return work_count;
    }

    fn handleIoUringAcceptCqe(self: *Self, cqe: linux.io_uring_cqe) u32 {
        if (self.iouring_state) |*state| {
            if ((cqe.flags & linux.IORING_CQE_F_MORE) == 0) {
                state.accept_armed = false;
            }
        }

        if (cqe.res < 0) {
            self.counters.increment(self.counter_ids.iouring_errors);
            if (cqe.err() == .INVAL or cqe.err() == .OPNOTSUPP) {
                self.disableIoUringReceiver(error.MultishotAcceptUnsupported);
            } else if (self.iouring_state != null) {
                self.armIoUringAccept() catch |err| self.disableIoUringReceiver(err);
            }
            return 0;
        }

        const fd: std.posix.fd_t = @intCast(cqe.res);
        const sock_cfg = SocketConfig{};
        sock_cfg.apply(fd) catch {
            platform.closeFd(fd);
            self.counters.increment(self.counter_ids.connection_errors);
            return 0;
        };

        if (self.pending_count >= max_pending) {
            platform.closeFd(fd);
            self.counters.increment(self.counter_ids.connection_errors);
            return 0;
        }

        self.pending_connections[self.pending_count] = .{
            .fd = fd,
            .address = std.mem.zeroes(net.Address),
            .handshake_buf = std.mem.zeroes([HandshakeFrame.size]u8),
            .bytes_read = 0,
        };
        self.pending_count += 1;

        if (self.iouring_state) |state| {
            if (!state.accept_armed) {
                self.armIoUringAccept() catch |err| self.disableIoUringReceiver(err);
            }
        }

        return 1;
    }

    fn handleIoUringRecvCqe(self: *Self, cqe: linux.io_uring_cqe, node_id: u8, generation: u8) u32 {
        const peer = self.peers.get(node_id) orelse return 0;
        if (generation != peer.io_generation) return 0;

        if ((cqe.flags & linux.IORING_CQE_F_MORE) == 0) {
            peer.io_recv_armed = false;
        }

        if (cqe.res <= 0) {
            if (cqe.res < 0) {
                self.counters.increment(self.counter_ids.iouring_errors);
                if (cqe.err() == .NOBUFS) {
                    self.counters.increment(self.counter_ids.iouring_buffer_starvations);
                    if (self.iouring_state != null and peer.connected and !peer.io_recv_armed) {
                        self.armIoUringRecv(peer) catch |err| self.disableIoUringReceiver(err);
                    }
                    return 0;
                }
            }
            self.disconnectPeer(peer);
            return 0;
        }

        var state = if (self.iouring_state) |*s| s else return 0;
        const data = state.recv_group.get(cqe) catch |err| {
            self.counters.increment(self.counter_ids.iouring_errors);
            self.disableIoUringReceiver(err);
            return 0;
        };
        defer state.recv_group.put(cqe) catch {
            self.counters.increment(self.counter_ids.iouring_errors);
        };

        self.counters.add(self.counter_ids.iouring_recv_bytes, @intCast(data.len));
        const completed = self.processPeerBytes(peer, data);

        if (self.iouring_state != null and peer.connected and !peer.io_recv_armed) {
            self.armIoUringRecv(peer) catch |err| self.disableIoUringReceiver(err);
        }

        return completed;
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
                if (self.iouring_state != null) {
                    if (self.peers.get(frame.source_node_id)) |peer| {
                        self.armIoUringRecv(peer) catch |err| {
                            self.disableIoUringReceiver(err);
                        };
                    }
                }

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
                if (n > 0) {
                    self.counters.increment(self.counter_ids.sync_read_calls);
                    self.counters.add(self.counter_ids.sync_read_bytes, @intCast(n));
                }
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

    fn processPeerBytes(self: *Self, peer: *PeerReceiver, data: []const u8) u32 {
        var offset: usize = 0;
        var work_count: u32 = 0;

        while (offset < data.len) {
            const copied = peer.appendRecvData(data[offset..]);
            self.counters.add(self.counter_ids.iouring_recv_copied_bytes, @intCast(copied));
            if (copied == 0) {
                self.disconnectPeer(peer);
                self.counters.increment(self.counter_ids.connection_errors);
                break;
            }
            offset += copied;

            var budget: u32 = constants.read_budget_per_peer;
            while (budget > 0) : (budget -= 1) {
                const read_result = self.readOnePeerStep(peer);
                if (!read_result.progress) break;
                work_count += read_result.frames_completed;
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

        // Route application payload to target service. The transport frame
        // stays broker-internal; the logical template ID is preserved as the
        // service-visible ring-buffer message type.
        const result = message_router.routeToService(
            self.service_registry,
            header.target_service_id,
            header.template_id,
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

var test_received_msg_type: i32 = 0;
var test_received_payload_len: usize = 0;
var test_received_payload_buf: [256]u8 = undefined;

fn testCaptureHandler(msg_type_id: i32, payload: []const u8) void {
    test_received_msg_type = msg_type_id;
    test_received_payload_len = payload.len;
    if (payload.len <= test_received_payload_buf.len) {
        @memcpy(test_received_payload_buf[0..payload.len], payload);
    }
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

    test_received_msg_type = 0;
    test_received_payload_len = 0;
    const messages_read = rb.read(&testCaptureHandler, 10);
    try testing.expectEqual(@as(u32, 1), messages_read);
    try testing.expectEqual(@as(i32, 42), test_received_msg_type);
    try testing.expectEqual(payload.len, test_received_payload_len);
    try testing.expectEqualSlices(u8, payload[0..], test_received_payload_buf[0..test_received_payload_len]);
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
