//! Sender Event Loop — the main duty-cycle loop for the outbound TCP message path.
//!
//! The sender event loop is the sole consumer of the send ring buffer (MPSC).
//! It reads outbound messages, routes them to the correct peer's write queue,
//! and manages outgoing TCP connections (heartbeats, reconnection with backoff).
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
const PeerSendCountersRegion = ringloom_common.memory.PeerSendCountersRegion;

const frame_parser = ringloom_common.protocol.frame_parser;
const TcpFrameHeader = frame_parser.TcpFrameHeader;
const latency_trace = ringloom_common.message.latency_trace;

const tcp = @import("ringloom_tcp");
const HandshakeFrame = tcp.HandshakeFrame;
const SocketConfig = tcp.SocketConfig;

const PeerSender = @import("peer_sender.zig").PeerSender;
const ConnectionState = @import("peer_sender.zig").ConnectionState;
const SendBufferPool = @import("send_buffer_pool.zig").SendBufferPool;
const SenderCommand = @import("sender_command.zig").SenderCommand;
const transport = @import("../transport.zig");

const linux = std.os.linux;
const posix = std.posix;
const log = std.log.scoped(.sender);

// ── Counter IDs ───────────────────────────────────────────────────────

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
    sync_writev_calls: usize = 0,
    sync_writev_frames: usize = 0,
    sync_writev_bytes: usize = 0,
    iouring_sender_enabled: usize = 0,
    iouring_sender_fallbacks: usize = 0,
    iouring_sender_errors: usize = 0,
    iouring_sender_sqes: usize = 0,
    iouring_sender_cqes: usize = 0,
    iouring_sender_writev_frames: usize = 0,
    iouring_sender_writev_bytes: usize = 0,

    pub fn allocate(counters: *CountersManager) SenderCounters {
        return .{
            .frames_sent = counters.allocate(1, "frames_sent") orelse 0,
            .bytes_sent = counters.allocate(1, "bytes_sent") orelse 0,
            .heartbeats_sent = counters.allocate(1, "heartbeats_sent") orelse 0,
            .send_back_pressure = counters.allocate(1, "send_back_pressure") orelse 0,
            .malformed_messages_dropped = counters.allocate(1, "malformed_messages_dropped") orelse 0,
            .unknown_peer_messages_dropped = counters.allocate(1, "unknown_peer_messages_dropped") orelse 0,
            .peer_not_connected_drops = counters.allocate(1, "peer_not_connected_drops") orelse 0,
            .peer_queue_overflow_drops = counters.allocate(1, "peer_queue_overflow_drops") orelse 0,
            .send_errors = counters.allocate(1, "send_errors") orelse 0,
            .peers_connected = counters.allocate(1, "peers_connected") orelse 0,
            .peers_disconnected = counters.allocate(1, "peers_disconnected") orelse 0,
            .peers_timed_out = counters.allocate(1, "peers_timed_out") orelse 0,
            .reconnect_attempts = counters.allocate(1, "reconnect_attempts") orelse 0,
            .sync_writev_calls = counters.allocate(1, "sender_sync_writev_calls") orelse 0,
            .sync_writev_frames = counters.allocate(1, "sender_sync_writev_frames") orelse 0,
            .sync_writev_bytes = counters.allocate(1, "sender_sync_writev_bytes") orelse 0,
            .iouring_sender_enabled = counters.allocate(1, "sender_iouring_enabled") orelse 0,
            .iouring_sender_fallbacks = counters.allocate(1, "sender_iouring_fallbacks") orelse 0,
            .iouring_sender_errors = counters.allocate(1, "sender_iouring_errors") orelse 0,
            .iouring_sender_sqes = counters.allocate(1, "sender_iouring_sqes") orelse 0,
            .iouring_sender_cqes = counters.allocate(1, "sender_iouring_cqes") orelse 0,
            .iouring_sender_writev_frames = counters.allocate(1, "sender_iouring_writev_frames") orelse 0,
            .iouring_sender_writev_bytes = counters.allocate(1, "sender_iouring_writev_bytes") orelse 0,
        };
    }
};

pub const SenderOptions = struct {
    io_uring_enabled: bool = false,
    io_uring_queue_depth: u16 = 256,
    io_uring_cq_depth: u32 = 1024,
    io_uring_sqpoll: bool = false,
    io_uring_single_issuer: bool = true,
    io_uring_coop_taskrun: bool = true,
    io_uring_cqe_batch_size: u32 = 64,
    writev_batch_size: u32 = 64,
    write_budget_per_peer: u32 = constants.write_budget_per_peer,
};

// ── Peer Map ──────────────────────────────────────────────────────────

const PeerMap = struct {
    entries: [256]?*PeerSender,
    count: u32,

    fn init() PeerMap {
        return .{
            .entries = [_]?*PeerSender{null} ** 256,
            .count = 0,
        };
    }

    fn get(self: *const PeerMap, node_id: u8) ?*PeerSender {
        return self.entries[node_id];
    }

    fn put(self: *PeerMap, node_id: u8, peer: *PeerSender) void {
        if (self.entries[node_id] == null) {
            self.count += 1;
        }
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

// ── Sender Event Loop ─────────────────────────────────────────────────

pub const SenderEventLoop = struct {
    /// The MPSC ring buffer that local services write cross-host messages into.
    send_ring_buffer: *RingBuffer,

    /// Per-peer sender state, keyed by node ID.
    peers: PeerMap,

    /// Shared counters manager for observability.
    counters: *CountersManager,

    /// Well-known counter slot IDs.
    counter_ids: SenderCounters,

    /// Monotonic timestamp (ns) of the last heartbeat round.
    last_heartbeat_ns: i64,

    /// This broker's node ID.
    local_node_id: u8,

    /// Whether this event loop is running.
    running: AtomicBool,

    /// Allocator for dynamic peer allocation.
    allocator: std.mem.Allocator,

    /// Accumulated work count for the current duty cycle.
    pending_send_count: u32,

    /// FNV-1a hash of the cluster group name (for handshake validation).
    group_name_hash: u32,

    /// Enable benchmark-only payload timestamp tracing. Disabled in production.
    benchmark_latency_tracing_enabled: bool,
    options: SenderOptions,

    /// Pre-allocated iovec array for writev batching (sync fallback path).
    writev_iovecs: [max_writev_batch]posix.iovec_const = undefined,

    /// io_uring ring for batched async TCP sends (Linux only).
    io_ring: ?transport.IoUring,

    /// Optional shared-memory per-peer counters published for ServiceClients.
    peer_send_counters: ?PeerSendCountersRegion,

    /// CQE harvest buffer.
    cqe_buf: [max_cqe_batch]linux.io_uring_cqe = undefined,

    const max_writev_batch = 1024;
    const max_cqe_batch = 256;

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
        var self = Self{
            .send_ring_buffer = send_ring_buffer,
            .peers = PeerMap.init(),
            .counters = counters,
            .counter_ids = SenderCounters.allocate(counters),
            .last_heartbeat_ns = 0,
            .local_node_id = local_node_id,
            .running = AtomicBool.init(true),
            .allocator = allocator,
            .pending_send_count = 0,
            .group_name_hash = HandshakeFrame.hashGroupName(group_name),
            .benchmark_latency_tracing_enabled = benchmark_latency_tracing_enabled,
            .options = options,
            .io_ring = null,
            .peer_send_counters = null,
        };

        if (options.io_uring_enabled) {
            self.enableIoUringSender() catch |err| {
                self.counters.increment(self.counter_ids.iouring_sender_fallbacks);
                logIoUringSenderFallback(err);
            };
        }

        return self;
    }

    pub fn setPeerSendCountersRegion(self: *Self, region: ?PeerSendCountersRegion) void {
        self.peer_send_counters = region;
    }

    pub fn deinit(self: *Self) void {
        if (self.io_ring) |*ring| {
            ring.deinit();
        }
        self.io_ring = null;
        var iter = self.peers.iterator();
        while (iter.next()) |peer| {
            peer.deinit(self.allocator);
            self.allocator.destroy(peer);
        }
    }

    fn enableIoUringSender(self: *Self) !void {
        if (comptime builtin.os.tag != .linux) return error.UnsupportedPlatform;

        var ring = try transport.IoUring.initWithConfig(.{
            .queue_depth = self.options.io_uring_queue_depth,
            .cq_depth = self.options.io_uring_cq_depth,
            .sqpoll = self.options.io_uring_sqpoll,
            .single_issuer = self.options.io_uring_single_issuer,
            .coop_taskrun = self.options.io_uring_coop_taskrun,
        });
        errdefer ring.deinit();

        if (!ring.capabilities.writev_supported) {
            return error.IoUringWritevUnsupported;
        }

        self.io_ring = ring;
        self.counters.increment(self.counter_ids.iouring_sender_enabled);
    }

    fn disableIoUringSender(self: *Self, err: anyerror) void {
        self.counters.increment(self.counter_ids.iouring_sender_fallbacks);
        logIoUringSenderFallback(err);
        if (self.io_ring) |*ring| {
            ring.deinit();
        }
        self.io_ring = null;
        self.clearIoUringPendingState();
    }

    fn logIoUringSenderFallback(err: anyerror) void {
        log.warn("sender io_uring disabled; falling back to synchronous writev: {}", .{err});
    }

    // ── Duty Cycle ────────────────────────────────────────────────────

    /// Called by the ThreadRunner on every iteration.
    pub fn doWork(self: *Self) u32 {
        var work_count: u32 = 0;
        const now_ns = Clock.monotonicNanos();
        self.pending_send_count = 0;

        // ── Phase 0: Harvest io_uring completions ────────────────────
        // Process completed sends before flushing new data to free queue
        // space and clear io_pending flags as early as possible.
        if (self.io_ring != null) {
            work_count += self.harvestSendCompletions();
        }

        // ── Phase 1: Flush write queues to TCP ───────────────────────
        // Flush before draining to keep write queues shallow and reduce
        // overflow-induced drops under high message rates.
        work_count += self.flushWriteQueues();

        // ── Phase 1b: Immediate harvest after io_uring submit ────────
        // For io_uring, the kernel often completes socket writes immediately
        // (just copying to TCP send buffer). Harvesting here eliminates the
        // 1-cycle latency penalty for these fast completions.
        if (self.io_ring != null) {
            work_count += self.harvestSendCompletions();
        }

        // ── Phase 2: Drain send ring buffer ──────────────────────────
        // Limit drain to available write queue space to apply backpressure
        // when TCP can't keep up, preventing write queue overflow.
        {
            const drain_limit = self.availableWriteQueueSpace();
            if (drain_limit > 0) {
                tls_self = self;
                defer {
                    tls_self = null;
                }
                work_count += self.send_ring_buffer.read(
                    onOutboundMessageThunk,
                    @min(constants.send_batch_limit, drain_limit),
                );
            }
        }

        // ── Phase 3: Flush newly drained messages ────────────────────
        // Second flush to immediately send messages just drained from
        // the ring buffer, minimizing latency for the common case.
        work_count += self.flushWriteQueues();

        // ── Phase 3b: Immediate harvest after second flush ───────────
        if (self.io_ring != null) {
            work_count += self.harvestSendCompletions();
        }

        // ── Phase 4: Send heartbeats ─────────────────────────────────
        if (now_ns - self.last_heartbeat_ns >= constants.default_heartbeat_interval_ns) {
            self.sendHeartbeats(now_ns);
            self.last_heartbeat_ns = now_ns;
        }

        // ── Phase 5: Process reconnections ───────────────────────────
        self.processReconnections(now_ns);

        work_count += self.pending_send_count;

        self.publishPeerSendCounters(now_ns);

        return work_count;
    }

    // ── Ring Buffer Read Callback ─────────────────────────────────────

    threadlocal var tls_self: ?*Self = null;

    fn onOutboundMessageThunk(msg_type_id: i32, payload: []const u8) void {
        if (tls_self) |self| {
            self.onOutboundMessage(msg_type_id, payload);
        }
    }

    /// Process a single outbound message from the send ring buffer.
    pub fn onOutboundMessage(self: *Self, msg_type_id: i32, payload: []const u8) void {
        _ = msg_type_id;

        // Payload must contain at least a complete TCP frame header (24 bytes).
        if (payload.len < constants.tcp_header_length) {
            self.counters.increment(self.counter_ids.malformed_messages_dropped);
            return;
        }

        const header: *const TcpFrameHeader = @ptrCast(@alignCast(payload.ptr));
        const target_node_id = header.target_node_id;
        self.subtractPeerRingPending(target_node_id, ringCost(payload.len));

        // Look up peer
        const peer = self.peers.get(target_node_id) orelse {
            self.counters.increment(self.counter_ids.unknown_peer_messages_dropped);
            return;
        };

        // Must be connected
        if (peer.state != .connected) {
            self.counters.increment(self.counter_ids.peer_not_connected_drops);
            peer.total_bytes_dropped += payload.len;
            return;
        }

        if (self.benchmark_latency_tracing_enabled) {
            latency_trace.stampSenderDequeue(
                @constCast(payload[constants.tcp_header_length..]),
                @intCast(Clock.monotonicNanosStable()),
            );
        }

        // Enqueue into peer's write queue (drop-oldest on overflow)
        peer.write_queue.enqueue(payload) catch {
            const dropped_len = peer.write_queue.dropOldest();
            peer.total_bytes_dropped += dropped_len;
            self.counters.increment(self.counter_ids.peer_queue_overflow_drops);
            peer.write_queue.enqueue(payload) catch unreachable;
        };

        self.pending_send_count += 1;
        self.counters.increment(self.counter_ids.frames_sent);
        self.counters.add(self.counter_ids.bytes_sent, @intCast(payload.len));
    }

    // ── Heartbeats ────────────────────────────────────────────────────

    fn sendHeartbeats(self: *Self, now_ns: i64) void {
        var peer_iter = self.peers.iterator();
        while (peer_iter.next()) |peer| {
            if (peer.state != .connected) continue;

            // Only send heartbeat if no data was sent within the interval.
            if (now_ns - peer.last_send_ns < constants.default_heartbeat_interval_ns) continue;

            // Build a heartbeat frame.
            var heartbeat: TcpFrameHeader = .{
                .frame_length = constants.tcp_header_length,
                .flags = 0x01, // heartbeat flag
                .source_node_id = self.local_node_id,
                .target_node_id = peer.node_id,
                .source_service_id = 0,
                .target_service_id = 0,
                .template_id = constants.heartbeat_template_id,
                .correlation_id = 0,
            };

            if (self.io_ring != null) {
                // With io_uring: route heartbeat through write queue to avoid
                // mixing sync writes with async I/O on the same fd.
                const heartbeat_bytes = std.mem.asBytes(&heartbeat);
                peer.write_queue.enqueue(heartbeat_bytes) catch {
                    continue;
                };
                peer.last_send_ns = now_ns;
                self.counters.increment(self.counter_ids.heartbeats_sent);
                self.pending_send_count += 1;
            } else {
                // Sync path: direct write.
                const heartbeat_bytes = std.mem.asBytes(&heartbeat);
                const written = net.write(peer.socket_fd, heartbeat_bytes) catch |err| {
                    switch (err) {
                        error.WouldBlock => continue,
                        else => {
                            self.disconnectPeer(peer);
                            continue;
                        },
                    }
                };
                if (written < heartbeat_bytes.len) {
                    self.disconnectPeer(peer);
                    continue;
                }

                peer.last_send_ns = now_ns;
                self.counters.increment(self.counter_ids.heartbeats_sent);
                self.pending_send_count += 1;
            }
        }
    }

    // ── Backpressure ────────────────────────────────────────────────

    /// Return the minimum available write queue space across all connected peers.
    /// This limits ring buffer drain to prevent write queue overflow.
    /// With io_uring, in-flight frames still occupy queue slots until CQE arrives.
    fn availableWriteQueueSpace(self: *Self) u32 {
        var min_space: u32 = constants.send_batch_limit;
        var has_connected = false;

        var peer_iter = self.peers.iterator();
        while (peer_iter.next()) |peer| {
            if (peer.state != .connected) continue;
            has_connected = true;
            const space = peer.write_queue.capacity - peer.write_queue.count;
            min_space = @min(min_space, space);
        }

        // If no peers are connected, drain freely (messages will be dropped by routing).
        return if (has_connected) min_space else constants.send_batch_limit;
    }

    // ── TCP Write Queue Flush ────────────────────────────────────────

    fn flushWriteQueues(self: *Self) u32 {
        if (self.io_ring != null) {
            return self.flushWriteQueuesIoUring();
        }
        return self.flushWriteQueuesSync();
    }

    /// io_uring path: prepare writev SQEs for all peers, submit in one batch.
    /// Only one outstanding writev per socket to prevent TCP stream corruption
    /// on partial writes (second writev would interleave with incomplete first).
    fn flushWriteQueuesIoUring(self: *Self) u32 {
        var ring = &(self.io_ring.?);
        var sqe_count: u32 = 0;

        var peer_iter = self.peers.iterator();
        while (peer_iter.next()) |peer| {
            if (peer.state != .connected) continue;
            if (peer.io_pending) continue;
            if (peer.write_queue.isEmpty()) continue;

            // Fill iovecs from queued frames (no in-flight since io_pending guards).
            const batch_limit = @min(@min(
                self.options.writev_batch_size,
                @as(u32, PeerSender.max_send_batch),
            ), self.options.write_budget_per_peer);
            const n = peer.write_queue.fillIovecsFrom(
                &peer.send_iovecs,
                0,
                peer.partial_write_offset,
                batch_limit,
            );
            if (n == 0) continue;

            // Compute total bytes for this batch.
            var total_bytes: usize = 0;
            for (peer.send_iovecs[0..n]) |iov| total_bytes += iov.len;

            // Encode user_data with node_id, generation, and frame count.
            const user_data = peer.encodeUserData(@intCast(n));

            // Submit writev SQE.
            _ = ring.ring.writev(user_data, peer.socket_fd, peer.send_iovecs[0..n], 0) catch {
                // SQ full — skip this peer, will retry next cycle.
                self.counters.increment(self.counter_ids.iouring_sender_errors);
                continue;
            };

            peer.io_pending = true;
            peer.in_flight_frames = n;
            peer.in_flight_bytes = total_bytes;
            sqe_count += 1;
            self.counters.increment(self.counter_ids.iouring_sender_sqes);
            self.counters.add(self.counter_ids.iouring_sender_writev_frames, @intCast(n));
            self.counters.add(self.counter_ids.iouring_sender_writev_bytes, @intCast(total_bytes));
        }

        // Single syscall to submit all prepared SQEs.
        if (sqe_count > 0) {
            _ = ring.ring.submit() catch {
                self.counters.increment(self.counter_ids.iouring_sender_errors);
                self.disableIoUringSender(error.IoUringSubmitFailed);
            };
        }

        return 0; // Actual completion counting happens in harvestSendCompletions.
    }

    /// Harvest io_uring CQEs for completed sends.
    /// With io_pending, each peer has at most one outstanding batch.
    fn harvestSendCompletions(self: *Self) u32 {
        var ring = &(self.io_ring.?);
        var work_count: u32 = 0;

        const limit = @min(self.options.io_uring_cqe_batch_size, @as(u32, @intCast(self.cqe_buf.len)));
        const count = ring.ring.copy_cqes(self.cqe_buf[0..limit], 0) catch {
            self.counters.increment(self.counter_ids.iouring_sender_errors);
            return 0;
        };
        self.counters.add(self.counter_ids.iouring_sender_cqes, @intCast(count));

        for (self.cqe_buf[0..count]) |cqe| {
            const decoded = PeerSender.decodeUserData(cqe.user_data);
            const peer = self.peers.get(decoded.node_id) orelse continue;

            // Stale CQE from a previous connection generation — ignore.
            if (decoded.generation != peer.io_generation) continue;

            peer.io_pending = false;

            if (cqe.res < 0) {
                peer.in_flight_frames = 0;
                peer.in_flight_bytes = 0;
                self.counters.increment(self.counter_ids.iouring_sender_errors);
                self.disableIoUringSender(error.IoUringWritevFailed);
                return work_count;
            }

            if (cqe.res == 0) {
                peer.in_flight_frames = 0;
                peer.in_flight_bytes = 0;
                self.disconnectPeer(peer);
                continue;
            }

            const written: usize = @intCast(cqe.res);

            // Walk the WriteQueue from head to determine which frames
            // were fully written.
            var bytes_remaining = written;
            var frames_completed: u32 = 0;
            while (frames_completed < decoded.frame_count) {
                const frame_size = peer.write_queue.peekFrameSize(frames_completed);
                if (frame_size == 0) break;
                const effective_size: usize = if (frames_completed == 0)
                    frame_size - peer.partial_write_offset
                else
                    frame_size;
                if (bytes_remaining >= effective_size) {
                    bytes_remaining -= effective_size;
                    frames_completed += 1;
                } else {
                    break;
                }
            }

            // Dequeue fully-written frames.
            for (0..frames_completed) |_| {
                peer.write_queue.dequeue();
            }

            peer.in_flight_frames = 0;
            peer.in_flight_bytes = 0;
            peer.total_bytes_sent += written;

            if (frames_completed > 0) {
                peer.last_send_ns = Clock.monotonicNanos();
                work_count += frames_completed;
            }

            if (bytes_remaining > 0) {
                // Partial write — update offset into the partially-written frame.
                if (frames_completed == 0) {
                    peer.partial_write_offset += bytes_remaining;
                } else {
                    peer.partial_write_offset = bytes_remaining;
                }
            } else {
                peer.partial_write_offset = 0;
            }
        }

        return work_count;
    }

    /// Synchronous writev path (fallback when io_uring is not available).
    fn flushWriteQueuesSync(self: *Self) u32 {
        var work_count: u32 = 0;

        var peer_iter = self.peers.iterator();
        while (peer_iter.next()) |peer| {
            if (peer.state != .connected) continue;
            if (peer.write_queue.isEmpty()) continue;

            var budget: u32 = self.options.write_budget_per_peer;

            while (budget > 0) {
                const batch_limit = @min(@min(budget, max_writev_batch), self.options.writev_batch_size);
                const n = peer.write_queue.fillIovecs(&self.writev_iovecs, peer.partial_write_offset, batch_limit);
                if (n == 0) break;

                const iovs = self.writev_iovecs[0..n];
                var total_bytes: usize = 0;
                for (iovs) |iov| total_bytes += iov.len;

                const written = net.writev(peer.socket_fd, iovs) catch |err| {
                    switch (err) {
                        error.WouldBlock => {
                            peer.write_blocked = true;
                            break;
                        },
                        else => {
                            self.disconnectPeer(peer);
                            break;
                        },
                    }
                };
                self.counters.increment(self.counter_ids.sync_writev_calls);
                self.counters.add(self.counter_ids.sync_writev_frames, @intCast(n));
                self.counters.add(self.counter_ids.sync_writev_bytes, @intCast(written));
                peer.total_bytes_sent += written;

                // Walk iovecs to determine which frames were fully written.
                var remaining = written;
                var frames_completed: u32 = 0;
                for (iovs) |iov| {
                    if (remaining >= iov.len) {
                        remaining -= iov.len;
                        frames_completed += 1;
                    } else {
                        break;
                    }
                }

                // Dequeue fully-written frames.
                for (0..frames_completed) |_| {
                    peer.write_queue.dequeue();
                }

                if (frames_completed > 0) {
                    peer.last_send_ns = Clock.monotonicNanos();
                    work_count += frames_completed;
                    budget -|= frames_completed;
                }

                if (written < total_bytes) {
                    // Partial write: update offset into the partially-written frame.
                    if (frames_completed == 0) {
                        peer.partial_write_offset += remaining;
                    } else {
                        peer.partial_write_offset = remaining;
                    }
                    peer.write_blocked = true;
                    break;
                } else {
                    peer.partial_write_offset = 0;
                    peer.write_blocked = false;
                }
            }
        }

        return work_count;
    }

    fn clearIoUringPendingState(self: *Self) void {
        var peer_iter = self.peers.iterator();
        while (peer_iter.next()) |peer| {
            peer.io_pending = false;
            peer.in_flight_frames = 0;
            peer.in_flight_bytes = 0;
        }
    }

    // ── Reconnections ─────────────────────────────────────────────────

    fn processReconnections(self: *Self, now_ns: i64) void {
        var peer_iter = self.peers.iterator();
        while (peer_iter.next()) |peer| {
            switch (peer.state) {
                .disconnected => {
                    if (now_ns < peer.next_reconnect_ns) continue;
                    self.initiateConnect(peer, now_ns);
                },
                .connecting => {
                    self.checkConnectCompletion(peer);
                },
                .connected => {},
            }
        }
    }

    fn initiateConnect(self: *Self, peer: *PeerSender, now_ns: i64) void {
        self.counters.increment(self.counter_ids.reconnect_attempts);

        const fd = net.socket(
            peer.address.any.family,
            std.posix.SOCK.STREAM | std.posix.SOCK.NONBLOCK | std.posix.SOCK.CLOEXEC,
            0,
        ) catch {
            peer.advanceBackoff(now_ns);
            return;
        };

        // Apply socket tuning (TCP_NODELAY, buffer sizes, etc.)
        const sock_cfg = SocketConfig{};
        sock_cfg.apply(fd) catch {
            platform.closeFd(fd);
            peer.advanceBackoff(now_ns);
            return;
        };

        // Non-blocking connect — expect EINPROGRESS.
        net.connect(fd, &peer.address.any, peer.address.getOsSockLen()) catch |err| {
            switch (err) {
                error.WouldBlock => {
                    // EINPROGRESS — connect in progress, will complete async.
                    peer.socket_fd = fd;
                    peer.state = .connecting;
                    return;
                },
                else => {
                    platform.closeFd(fd);
                    peer.advanceBackoff(now_ns);
                    return;
                },
            }
        };

        // Immediate connect success (rare, e.g. loopback).
        peer.socket_fd = fd;
        self.completeConnect(peer);
    }

    fn checkConnectCompletion(self: *Self, peer: *PeerSender) void {
        // Poll for connect completion using SO_ERROR.
        const so_error = net.getSocketError(peer.socket_fd) catch {
            self.disconnectPeer(peer);
            return;
        };
        if (so_error != 0) {
            // Connect failed.
            self.disconnectPeer(peer);
            return;
        }

        // SO_ERROR == 0 means connect completed successfully.
        self.completeConnect(peer);
    }

    fn completeConnect(self: *Self, peer: *PeerSender) void {
        // Send the handshake frame.
        const handshake = HandshakeFrame{
            .source_node_id = self.local_node_id,
            .target_node_id = peer.node_id,
            .direction = .outbound,
            .session_epoch = @intCast(Clock.monotonicNanos()),
            .group_name_hash = self.group_name_hash,
        };
        const handshake_bytes = handshake.toBytes();
        _ = net.write(peer.socket_fd, handshake_bytes) catch {
            self.disconnectPeer(peer);
            return;
        };

        peer.state = .connected;
        peer.resetBackoff();
        peer.write_blocked = false;
        peer.partial_write_offset = 0;
        peer.io_pending = false;
        peer.in_flight_frames = 0;
        peer.in_flight_bytes = 0;
        self.counters.increment(self.counter_ids.peers_connected);
        self.publishPeerCounter(peer, Clock.monotonicNanos());
    }

    fn disconnectPeer(self: *Self, peer: *PeerSender) void {
        if (peer.socket_fd >= 0) {
            platform.closeFd(peer.socket_fd);
            peer.socket_fd = -1;
        }
        peer.state = .disconnected;
        peer.write_blocked = false;
        peer.partial_write_offset = 0;
        // Bump generation so stale CQEs from this connection are ignored.
        peer.io_generation +%= 1;
        peer.io_pending = false;
        peer.in_flight_frames = 0;
        peer.in_flight_bytes = 0;
        peer.advanceBackoff(Clock.monotonicNanos());
        self.counters.increment(self.counter_ids.peers_disconnected);
        self.publishPeerCounter(peer, Clock.monotonicNanos());
    }

    // ── Peer Lifecycle Management ─────────────────────────────────────

    pub fn addPeer(self: *Self, node_id: u8, address: net.Address) !void {
        if (self.peers.contains(node_id)) return;

        const peer = try self.allocator.create(PeerSender);
        errdefer self.allocator.destroy(peer);

        peer.* = try PeerSender.init(node_id, address, self.allocator);
        errdefer peer.deinit(self.allocator);

        self.peers.put(node_id, peer);
        self.publishPeerCounter(peer, Clock.monotonicNanos());
    }

    pub fn removePeer(self: *Self, node_id: u8) void {
        if (self.peers.remove(node_id)) |peer| {
            if (self.peer_send_counters) |region| {
                region.freePeer(node_id);
            }
            peer.deinit(self.allocator);
            self.allocator.destroy(peer);
            self.counters.increment(self.counter_ids.peers_disconnected);
        }
    }

    pub fn dispatchCommand(self: *Self, cmd: SenderCommand) void {
        switch (cmd) {
            .add_peer => |add| {
                self.addPeer(add.node_id, add.address) catch {
                    self.counters.increment(self.counter_ids.send_errors);
                };
            },
            .remove_peer => |remove| {
                self.removePeer(remove.node_id);
            },
            .reconnect_peer => |reconnect| {
                if (self.peers.get(reconnect.node_id)) |peer| {
                    peer.resetForReconnect();
                    peer.advanceBackoff(Clock.monotonicNanos());
                }
            },
        }
    }

    // ── Read callback wiring ──────────────────────────────────────────

    pub fn readFromRingBuffer(self: *Self, limit: u32) u32 {
        tls_self = self;
        defer tls_self = null;
        return self.send_ring_buffer.read(onOutboundMessageThunk, limit);
    }

    fn publishPeerSendCounters(self: *Self, now_ns: i64) void {
        if (self.peer_send_counters == null) return;
        var peer_iter = self.peers.iterator();
        while (peer_iter.next()) |peer| {
            self.publishPeerCounter(peer, now_ns);
        }
    }

    fn publishPeerCounter(self: *Self, peer: *PeerSender, now_ns: i64) void {
        const region = self.peer_send_counters orelse return;
        const queue_capacity_bytes = @as(u64, peer.write_queue.capacity) *
            @as(u64, peer.write_queue.max_frame_size);
        const entry = region.findOrAllocPeer(peer.node_id, queue_capacity_bytes) orelse return;
        entry.storeConnectionState(peer.state == .connected);
        entry.storeQueueBytesPending(peer.write_queue.byteSize());
        entry.storeTotalBytesSent(peer.total_bytes_sent);
        entry.storeTotalBytesDropped(peer.total_bytes_dropped);
        entry.storeLastUpdateNs(@intCast(@max(now_ns, 0)));
    }

    fn subtractPeerRingPending(self: *Self, node_id: u8, bytes: usize) void {
        const region = self.peer_send_counters orelse return;
        const entry = region.findPeer(node_id) orelse return;
        entry.subtractRingBytesPending(@intCast(bytes));
    }

    fn ringCost(payload_len: usize) usize {
        return alignUp(8 + payload_len, 8);
    }

    fn alignUp(value: usize, alignment: usize) usize {
        return (value + alignment - 1) & ~(alignment - 1);
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

test "SenderEventLoop init and deinit" {
    const allocator = testing.allocator;

    const rb_capacity: usize = 1024;
    const rb_buf_size = rb_capacity + constants.ring_buffer_trailer_length;
    const rb_buf = try allocator.alignedAlloc(u8, @enumFromInt(std.math.log2(@as(usize, constants.ring_buffer_alignment))), rb_buf_size);
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

test "addPeer and removePeer" {
    const allocator = testing.allocator;

    const rb_capacity: usize = 1024;
    const rb_buf_size = rb_capacity + constants.ring_buffer_trailer_length;
    const rb_buf = try allocator.alignedAlloc(u8, @enumFromInt(std.math.log2(@as(usize, constants.ring_buffer_alignment))), rb_buf_size);
    defer allocator.free(rb_buf);
    @memset(rb_buf, 0);

    var rb = try RingBuffer.init(rb_buf, false, null, null);

    var values_buf: [128 * 64]u8 align(128) = [_]u8{0} ** (128 * 64);
    var meta_buf: [256 * 64]u8 align(4) = [_]u8{0} ** (256 * 64);
    var counters = createTestCounters(&values_buf, &meta_buf);

    var sender = try SenderEventLoop.init(&rb, &counters, 0, allocator);
    defer sender.deinit();

    const addr1 = net.Address.initIp4(.{ 127, 0, 0, 1 }, 9001);
    const addr2 = net.Address.initIp4(.{ 127, 0, 0, 1 }, 9002);

    try sender.addPeer(1, addr1);
    try sender.addPeer(2, addr2);

    try testing.expectEqual(@as(u32, 2), sender.peers.count);
    try testing.expect(sender.peers.get(1) != null);
    try testing.expect(sender.peers.get(2) != null);

    sender.removePeer(1);

    try testing.expectEqual(@as(u32, 1), sender.peers.count);
    try testing.expect(sender.peers.get(1) == null);
    try testing.expect(sender.peers.get(2) != null);
}

test "onOutboundMessage drops malformed message" {
    const allocator = testing.allocator;

    const rb_capacity: usize = 1024;
    const rb_buf_size = rb_capacity + constants.ring_buffer_trailer_length;
    const rb_buf = try allocator.alignedAlloc(u8, @enumFromInt(std.math.log2(@as(usize, constants.ring_buffer_alignment))), rb_buf_size);
    defer allocator.free(rb_buf);
    @memset(rb_buf, 0);

    var rb = try RingBuffer.init(rb_buf, false, null, null);

    var values_buf: [128 * 64]u8 align(128) = [_]u8{0} ** (128 * 64);
    var meta_buf: [256 * 64]u8 align(4) = [_]u8{0} ** (256 * 64);
    var counters = createTestCounters(&values_buf, &meta_buf);

    var sender = try SenderEventLoop.init(&rb, &counters, 0, allocator);
    defer sender.deinit();

    // Payload too small for a TCP frame header
    const small_payload = [_]u8{ 0x01, 0x02, 0x03 };
    sender.onOutboundMessage(0, &small_payload);

    try testing.expectEqual(@as(i64, 1), counters.get(sender.counter_ids.malformed_messages_dropped));
}

test "onOutboundMessage drops message to unknown peer" {
    const allocator = testing.allocator;

    const rb_capacity: usize = 1024;
    const rb_buf_size = rb_capacity + constants.ring_buffer_trailer_length;
    const rb_buf = try allocator.alignedAlloc(u8, @enumFromInt(std.math.log2(@as(usize, constants.ring_buffer_alignment))), rb_buf_size);
    defer allocator.free(rb_buf);
    @memset(rb_buf, 0);

    var rb = try RingBuffer.init(rb_buf, false, null, null);

    var values_buf: [128 * 64]u8 align(128) = [_]u8{0} ** (128 * 64);
    var meta_buf: [256 * 64]u8 align(4) = [_]u8{0} ** (256 * 64);
    var counters = createTestCounters(&values_buf, &meta_buf);

    var sender = try SenderEventLoop.init(&rb, &counters, 0, allocator);
    defer sender.deinit();

    // Valid-sized payload targeting node 5 (which doesn't exist)
    var payload_buf: [64]u8 align(8) = [_]u8{0} ** 64;
    const header: *TcpFrameHeader = @ptrCast(@alignCast(&payload_buf));
    header.* = .{
        .frame_length = 64,
        .target_node_id = 5,
    };

    sender.onOutboundMessage(0, &payload_buf);

    try testing.expectEqual(@as(i64, 1), counters.get(sender.counter_ids.unknown_peer_messages_dropped));
}

test "onOutboundMessage drops message to disconnected peer" {
    const allocator = testing.allocator;

    const rb_capacity: usize = 1024;
    const rb_buf_size = rb_capacity + constants.ring_buffer_trailer_length;
    const rb_buf = try allocator.alignedAlloc(u8, @enumFromInt(std.math.log2(@as(usize, constants.ring_buffer_alignment))), rb_buf_size);
    defer allocator.free(rb_buf);
    @memset(rb_buf, 0);

    var rb = try RingBuffer.init(rb_buf, false, null, null);

    var values_buf: [128 * 64]u8 align(128) = [_]u8{0} ** (128 * 64);
    var meta_buf: [256 * 64]u8 align(4) = [_]u8{0} ** (256 * 64);
    var counters = createTestCounters(&values_buf, &meta_buf);

    var sender = try SenderEventLoop.init(&rb, &counters, 0, allocator);
    defer sender.deinit();

    const addr = net.Address.initIp4(.{ 127, 0, 0, 1 }, 9001);
    try sender.addPeer(1, addr);
    // peer.state defaults to .disconnected

    var payload_buf: [64]u8 align(8) = [_]u8{0} ** 64;
    const header: *TcpFrameHeader = @ptrCast(@alignCast(&payload_buf));
    header.* = .{
        .frame_length = 64,
        .target_node_id = 1,
    };

    sender.onOutboundMessage(0, &payload_buf);

    try testing.expectEqual(@as(i64, 1), counters.get(sender.counter_ids.peer_not_connected_drops));
}

test "onOutboundMessage enqueues frame to connected peer" {
    const allocator = testing.allocator;

    const rb_capacity: usize = 1024;
    const rb_buf_size = rb_capacity + constants.ring_buffer_trailer_length;
    const rb_buf = try allocator.alignedAlloc(u8, @enumFromInt(std.math.log2(@as(usize, constants.ring_buffer_alignment))), rb_buf_size);
    defer allocator.free(rb_buf);
    @memset(rb_buf, 0);

    var rb = try RingBuffer.init(rb_buf, false, null, null);

    var values_buf: [128 * 64]u8 align(128) = [_]u8{0} ** (128 * 64);
    var meta_buf: [256 * 64]u8 align(4) = [_]u8{0} ** (256 * 64);
    var counters = createTestCounters(&values_buf, &meta_buf);

    var sender = try SenderEventLoop.init(&rb, &counters, 0, allocator);
    defer sender.deinit();

    const addr = net.Address.initIp4(.{ 127, 0, 0, 1 }, 9001);
    try sender.addPeer(1, addr);
    const peer = sender.peers.get(1).?;
    peer.state = .connected;

    var payload_buf: [64]u8 align(8) = [_]u8{0} ** 64;
    const header: *TcpFrameHeader = @ptrCast(@alignCast(&payload_buf));
    header.* = .{
        .frame_length = 64,
        .target_node_id = 1,
        .source_service_id = 5,
        .target_service_id = 10,
        .template_id = 42,
        .correlation_id = 12345,
    };

    sender.onOutboundMessage(0, &payload_buf);

    try testing.expectEqual(@as(i64, 1), counters.get(sender.counter_ids.frames_sent));
    try testing.expect(counters.get(sender.counter_ids.bytes_sent) > 0);
    try testing.expect(!peer.write_queue.isEmpty());
}

test "dispatchCommand handles add_peer" {
    const allocator = testing.allocator;

    const rb_capacity: usize = 1024;
    const rb_buf_size = rb_capacity + constants.ring_buffer_trailer_length;
    const rb_buf = try allocator.alignedAlloc(u8, @enumFromInt(std.math.log2(@as(usize, constants.ring_buffer_alignment))), rb_buf_size);
    defer allocator.free(rb_buf);
    @memset(rb_buf, 0);

    var rb = try RingBuffer.init(rb_buf, false, null, null);

    var values_buf: [128 * 64]u8 align(128) = [_]u8{0} ** (128 * 64);
    var meta_buf: [256 * 64]u8 align(4) = [_]u8{0} ** (256 * 64);
    var counters = createTestCounters(&values_buf, &meta_buf);

    var sender = try SenderEventLoop.init(&rb, &counters, 0, allocator);
    defer sender.deinit();

    const addr = net.Address.initIp4(.{ 127, 0, 0, 1 }, 9001);
    sender.dispatchCommand(.{ .add_peer = .{ .node_id = 3, .address = addr } });

    try testing.expectEqual(@as(u32, 1), sender.peers.count);
    try testing.expect(sender.peers.get(3) != null);
}
