//! Sender Event Loop — the main duty-cycle loop for the outbound TCP message path.
//!
//! The sender event loop is the sole consumer of the send ring buffer (MPSC).
//! It reads outbound messages, routes them to the correct peer's write queue,
//! and manages outgoing TCP connections (heartbeats, reconnection with backoff).
//!
//! TCP replaces UDP: no retransmit buffer, no message fragmentation, no NAK
//! handling, no Status Messages. The sender writes complete frames to the TCP
//! stream and relies on the kernel for segmentation, retransmission, and flow
//! control.

const std = @import("std");
const brz_common = @import("brz_common");
const constants = brz_common.platform.constants;
const Clock = brz_common.platform.clock.Clock;
const AtomicBool = brz_common.platform.atomic.AtomicBool;

const RingBuffer = brz_common.concurrent.ring_buffer.RingBuffer;
const CountersManager = brz_common.concurrent.counters.CountersManager;

const frame_parser = brz_common.protocol.frame_parser;
const TcpFrameHeader = frame_parser.TcpFrameHeader;

const tcp = @import("brz_tcp");
const HandshakeFrame = tcp.HandshakeFrame;
const SocketConfig = tcp.SocketConfig;

const PeerSender = @import("peer_sender.zig").PeerSender;
const ConnectionState = @import("peer_sender.zig").ConnectionState;
const SendBufferPool = @import("send_buffer_pool.zig").SendBufferPool;
const SenderCommand = @import("sender_command.zig").SenderCommand;

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
        };
    }
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

    /// Pre-allocated iovec array for writev batching.
    writev_iovecs: [max_writev_batch]std.posix.iovec_const = undefined,

    const max_writev_batch = 64;

    const Self = @This();

    pub fn init(
        send_ring_buffer: *RingBuffer,
        counters: *CountersManager,
        local_node_id: u8,
        allocator: std.mem.Allocator,
    ) !Self {
        return Self.initWithGroup(send_ring_buffer, counters, local_node_id, allocator, "brz");
    }

    pub fn initWithGroup(
        send_ring_buffer: *RingBuffer,
        counters: *CountersManager,
        local_node_id: u8,
        allocator: std.mem.Allocator,
        group_name: []const u8,
    ) !Self {
        return .{
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
        };
    }

    pub fn deinit(self: *Self) void {
        var iter = self.peers.iterator();
        while (iter.next()) |peer| {
            peer.deinit(self.allocator);
            self.allocator.destroy(peer);
        }
    }

    // ── Duty Cycle ────────────────────────────────────────────────────

    /// Called by the ThreadRunner on every iteration.
    pub fn doWork(self: *Self) u32 {
        var work_count: u32 = 0;
        const now_ns = Clock.monotonicNanos();
        self.pending_send_count = 0;

        // ── Phase 1: Flush write queues to TCP first ─────────────────
        // Flush before draining to keep write queues shallow and reduce
        // overflow-induced drops under high message rates.
        work_count += self.flushWriteQueues();

        // ── Phase 2: Drain send ring buffer ──────────────────────────
        // Limit drain to available write queue space to apply backpressure
        // when TCP can't keep up, preventing write queue overflow.
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

        // ── Phase 3: Send heartbeats ─────────────────────────────────
        if (now_ns - self.last_heartbeat_ns >= constants.default_heartbeat_interval_ns) {
            self.sendHeartbeats(now_ns);
            self.last_heartbeat_ns = now_ns;
        }

        // ── Phase 4: Process reconnections ───────────────────────────
        self.processReconnections(now_ns);

        work_count += self.pending_send_count;

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

        // Look up peer
        const peer = self.peers.get(target_node_id) orelse {
            self.counters.increment(self.counter_ids.unknown_peer_messages_dropped);
            return;
        };

        // Must be connected
        if (peer.state != .connected) {
            self.counters.increment(self.counter_ids.peer_not_connected_drops);
            return;
        }

        // Enqueue into peer's write queue (drop-oldest on overflow)
        peer.write_queue.enqueue(payload) catch {
            _ = peer.write_queue.dropOldest();
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

            // Build and send a heartbeat frame directly to TCP.
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
            const heartbeat_bytes = std.mem.asBytes(&heartbeat);
            const written = std.posix.write(peer.socket_fd, heartbeat_bytes) catch |err| {
                switch (err) {
                    error.WouldBlock => continue,
                    else => {
                        self.disconnectPeer(peer);
                        continue;
                    },
                }
            };
            if (written < heartbeat_bytes.len) {
                // Partial heartbeat write — treat as error, disconnect.
                self.disconnectPeer(peer);
                continue;
            }

            peer.last_send_ns = now_ns;
            self.counters.increment(self.counter_ids.heartbeats_sent);
            self.pending_send_count += 1;
        }
    }

    // ── Backpressure ────────────────────────────────────────────────

    /// Return the minimum available write queue space across all connected peers.
    /// This limits ring buffer drain to prevent write queue overflow.
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
        var work_count: u32 = 0;

        var peer_iter = self.peers.iterator();
        while (peer_iter.next()) |peer| {
            if (peer.state != .connected) continue;
            if (peer.write_queue.isEmpty()) continue;

            var budget: u32 = constants.write_budget_per_peer;

            while (budget > 0) {
                const batch_limit = @min(budget, max_writev_batch);
                const n = peer.write_queue.fillIovecs(&self.writev_iovecs, peer.partial_write_offset, batch_limit);
                if (n == 0) break;

                const iovs = self.writev_iovecs[0..n];
                var total_bytes: usize = 0;
                for (iovs) |iov| total_bytes += iov.len;

                const written = std.posix.writev(peer.socket_fd, iovs) catch |err| {
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

        const fd = std.posix.socket(
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
            std.posix.close(fd);
            peer.advanceBackoff(now_ns);
            return;
        };

        // Non-blocking connect — expect EINPROGRESS.
        std.posix.connect(fd, &peer.address.any, peer.address.getOsSockLen()) catch |err| {
            switch (err) {
                error.WouldBlock => {
                    // EINPROGRESS — connect in progress, will complete async.
                    peer.socket_fd = fd;
                    peer.state = .connecting;
                    return;
                },
                else => {
                    std.posix.close(fd);
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
        var err_val: [4]u8 = undefined;
        std.posix.getsockopt(
            peer.socket_fd,
            std.posix.SOL.SOCKET,
            std.posix.SO.ERROR,
            &err_val,
        ) catch {
            self.disconnectPeer(peer);
            return;
        };
        const so_error = std.mem.bytesAsValue(c_int, &err_val);
        if (so_error.* != 0) {
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
        _ = std.posix.write(peer.socket_fd, handshake_bytes) catch {
            self.disconnectPeer(peer);
            return;
        };

        peer.state = .connected;
        peer.resetBackoff();
        peer.write_blocked = false;
        peer.partial_write_offset = 0;
        self.counters.increment(self.counter_ids.peers_connected);
    }

    fn disconnectPeer(self: *Self, peer: *PeerSender) void {
        if (peer.socket_fd >= 0) {
            std.posix.close(peer.socket_fd);
            peer.socket_fd = -1;
        }
        peer.state = .disconnected;
        peer.write_blocked = false;
        peer.partial_write_offset = 0;
        peer.advanceBackoff(Clock.monotonicNanos());
        self.counters.increment(self.counter_ids.peers_disconnected);
    }

    // ── Peer Lifecycle Management ─────────────────────────────────────

    pub fn addPeer(self: *Self, node_id: u8, address: std.net.Address) !void {
        if (self.peers.contains(node_id)) return;

        const peer = try self.allocator.create(PeerSender);
        errdefer self.allocator.destroy(peer);

        peer.* = try PeerSender.init(node_id, address, self.allocator);
        errdefer peer.deinit(self.allocator);

        self.peers.put(node_id, peer);
    }

    pub fn removePeer(self: *Self, node_id: u8) void {
        if (self.peers.remove(node_id)) |peer| {
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

    const addr1 = std.net.Address.initIp4(.{ 127, 0, 0, 1 }, 9001);
    const addr2 = std.net.Address.initIp4(.{ 127, 0, 0, 1 }, 9002);

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

    const addr = std.net.Address.initIp4(.{ 127, 0, 0, 1 }, 9001);
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

    const addr = std.net.Address.initIp4(.{ 127, 0, 0, 1 }, 9001);
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

    const addr = std.net.Address.initIp4(.{ 127, 0, 0, 1 }, 9001);
    sender.dispatchCommand(.{ .add_peer = .{ .node_id = 3, .address = addr } });

    try testing.expectEqual(@as(u32, 1), sender.peers.count);
    try testing.expect(sender.peers.get(3) != null);
}
