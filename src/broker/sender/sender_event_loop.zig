//! Sender Event Loop — the main duty-cycle loop for the outbound message path.
//!
//! The sender event loop is the sole consumer of the send ring buffer (MPSC).
//! It reads outbound messages, routes them to the correct peer, fragments
//! oversized messages, builds data frame headers, stores frames in the
//! retransmit buffer, and enqueues sends via the platform I/O backend.
//!
//! It also processes inbound control traffic from receivers:
//! - Status Messages (SM): update flow-control send limits.
//! - NAK frames: trigger retransmission from the retransmit buffer.
//!
//! Finally, it emits heartbeat frames (zero-length DATA frames) every 100ms
//! to all connected peers.

const std = @import("std");
const brz_common = @import("brz_common");
const constants = brz_common.platform.constants;
const Clock = brz_common.platform.clock.Clock;
const AtomicBool = brz_common.platform.atomic.AtomicBool;

const RingBuffer = brz_common.concurrent.ring_buffer.RingBuffer;
const CountersManager = brz_common.concurrent.counters.CountersManager;

const frames = brz_common.protocol.frames;
const DataFrameHeader = frames.DataFrameHeader;
const SetupFrame = frames.SetupFrame;
const StatusMessage = frames.StatusMessage;
const NakFrame = frames.NakFrame;
const FrameType = frames.FrameType;

const PeerSender = @import("peer_sender.zig").PeerSender;
const RetransmitBuffer = @import("retransmit_buffer.zig").RetransmitBuffer;
const RetransmitHandler = @import("retransmit_handler.zig").RetransmitHandler;
const SendBufferPool = @import("send_buffer_pool.zig").SendBufferPool;
const SenderCommand = @import("sender_command.zig").SenderCommand;

// ── Counter IDs ───────────────────────────────────────────────────────

/// Well-known counter IDs for the sender subsystem. These are logical
/// indices into the counters manager; the actual slot is allocated at
/// init time and stored in `SenderCounters`.
pub const SenderCounters = struct {
    frames_sent: usize = 0,
    bytes_sent: usize = 0,
    heartbeats_sent: usize = 0,
    setup_frames_sent: usize = 0,
    status_messages_received: usize = 0,
    naks_received: usize = 0,
    retransmits_sent: usize = 0,
    send_back_pressure: usize = 0,
    send_buffer_pool_exhausted: usize = 0,
    fragmented_messages_sent: usize = 0,
    fragmented_messages_incomplete: usize = 0,
    malformed_messages_dropped: usize = 0,
    unknown_peer_messages_dropped: usize = 0,
    disconnected_peer_messages_dropped: usize = 0,
    unknown_peer_sm_received: usize = 0,
    unknown_peer_nak_received: usize = 0,
    malformed_frames_received: usize = 0,
    send_errors: usize = 0,
    peer_socket_errors: usize = 0,
    peers_connected: usize = 0,
    peers_disconnected: usize = 0,
    peers_timed_out: usize = 0,

    pub fn allocate(counters: *CountersManager) SenderCounters {
        return .{
            .frames_sent = counters.allocate(1, "frames_sent") orelse 0,
            .bytes_sent = counters.allocate(1, "bytes_sent") orelse 0,
            .heartbeats_sent = counters.allocate(1, "heartbeats_sent") orelse 0,
            .setup_frames_sent = counters.allocate(1, "setup_frames_sent") orelse 0,
            .status_messages_received = counters.allocate(1, "status_messages_received") orelse 0,
            .naks_received = counters.allocate(1, "naks_received") orelse 0,
            .retransmits_sent = counters.allocate(1, "retransmits_sent") orelse 0,
            .send_back_pressure = counters.allocate(1, "send_back_pressure") orelse 0,
            .send_buffer_pool_exhausted = counters.allocate(1, "send_buffer_pool_exhausted") orelse 0,
            .fragmented_messages_sent = counters.allocate(1, "fragmented_messages_sent") orelse 0,
            .fragmented_messages_incomplete = counters.allocate(1, "fragmented_messages_incomplete") orelse 0,
            .malformed_messages_dropped = counters.allocate(1, "malformed_messages_dropped") orelse 0,
            .unknown_peer_messages_dropped = counters.allocate(1, "unknown_peer_messages_dropped") orelse 0,
            .disconnected_peer_messages_dropped = counters.allocate(1, "disconnected_peer_messages_dropped") orelse 0,
            .unknown_peer_sm_received = counters.allocate(1, "unknown_peer_sm_received") orelse 0,
            .unknown_peer_nak_received = counters.allocate(1, "unknown_peer_nak_received") orelse 0,
            .malformed_frames_received = counters.allocate(1, "malformed_frames_received") orelse 0,
            .send_errors = counters.allocate(1, "send_errors") orelse 0,
            .peer_socket_errors = counters.allocate(1, "peer_socket_errors") orelse 0,
            .peers_connected = counters.allocate(1, "peers_connected") orelse 0,
            .peers_disconnected = counters.allocate(1, "peers_disconnected") orelse 0,
            .peers_timed_out = counters.allocate(1, "peers_timed_out") orelse 0,
        };
    }
};

// ── Peer Map ──────────────────────────────────────────────────────────

/// Simple u8-keyed hash map for PeerSender pointers. Max `default_max_peers` entries.
/// Uses a fixed-size array for O(1) lookup by node ID (u8 → max 256 entries).
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

    /// Iterator over non-null peer entries.
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

    /// Monotonic timestamp (ns) of the next scheduled heartbeat round.
    next_heartbeat_ns: i64,

    /// This broker's node ID.
    local_node_id: u8,

    /// Pre-allocated send buffer pool for staging outbound frames.
    send_buffer_pool: SendBufferPool,

    /// Whether this event loop is running. Set to false by the shutdown path.
    running: AtomicBool,

    /// Allocator for dynamic peer allocation.
    allocator: std.mem.Allocator,

    /// Accumulated send count for the current duty cycle (for tests).
    pending_send_count: u32,

    const Self = @This();

    pub fn init(
        send_ring_buffer: *RingBuffer,
        counters: *CountersManager,
        local_node_id: u8,
        allocator: std.mem.Allocator,
    ) !Self {
        return .{
            .send_ring_buffer = send_ring_buffer,
            .peers = PeerMap.init(),
            .counters = counters,
            .counter_ids = SenderCounters.allocate(counters),
            .next_heartbeat_ns = 0,
            .local_node_id = local_node_id,
            .send_buffer_pool = try SendBufferPool.init(
                constants.send_batch_limit * 2,
                constants.default_mtu_length,
                allocator,
            ),
            .running = AtomicBool.init(true),
            .allocator = allocator,
            .pending_send_count = 0,
        };
    }

    pub fn deinit(self: *Self) void {
        // Clean up all peers
        var iter = self.peers.iterator();
        while (iter.next()) |peer| {
            peer.retransmit_buffer.close(self.allocator);
            self.allocator.destroy(peer.retransmit_buffer);
            self.allocator.destroy(peer);
        }
        self.send_buffer_pool.deinit();
    }

    // ── Duty Cycle ────────────────────────────────────────────────────

    /// Called by the ThreadRunner on every iteration.
    /// Returns the total number of items processed (work count).
    pub fn doWork(self: *Self) u32 {
        var work_count: u32 = 0;
        const now_ns = Clock.monotonicNanos();
        self.pending_send_count = 0;

        // ── Phase 1: (Placeholder for io_uring completions) ──────────
        // In the full implementation, this reclaims send buffers from
        // completed sends. Stubbed here as the NetworkIo interface is
        // wired in the broker application layer.

        // ── Phase 2: Drain send ring buffer ──────────────────────────
        // Read outbound messages written by local services.
        work_count += self.send_ring_buffer.read(
            onOutboundMessageThunk,
            constants.send_batch_limit,
        );

        // ── Phase 3: Send heartbeats ─────────────────────────────────
        if (now_ns >= self.next_heartbeat_ns) {
            self.sendHeartbeats(now_ns);
            self.next_heartbeat_ns = now_ns + constants.udp_heartbeat_interval_ns;
        }

        // ── Phase 4: Process retransmit timeouts ─────────────────────
        var peer_iter = self.peers.iterator();
        while (peer_iter.next()) |peer| {
            peer.retransmit_handler.processTimeouts(now_ns);
        }

        // ── Phase 5: Check peer health ───────────────────────────────
        self.checkPeerHealth(now_ns);

        work_count += self.pending_send_count;

        return work_count;
    }

    // ── Ring Buffer Read Callback ─────────────────────────────────────

    /// Ring buffer message handler thunk. The ring buffer calls this with
    /// msg_type_id and payload. We use a file-level function because
    /// RingBuffer.MessageHandler is `*const fn(i32, []const u8) void` and
    /// cannot capture `self`. Instead, we store self in a thread-local.
    ///
    /// Note: This is safe because the sender event loop is single-threaded.
    threadlocal var tls_self: ?*Self = null;

    fn onOutboundMessageThunk(msg_type_id: i32, payload: []const u8) void {
        if (tls_self) |self| {
            self.onOutboundMessage(msg_type_id, payload);
        }
    }

    /// Process a single outbound message from the send ring buffer.
    pub fn onOutboundMessage(self: *Self, msg_type_id: i32, payload: []const u8) void {
        _ = msg_type_id; // Routing is in the payload header

        // The payload starts with BRZ routing fields. Parse target_node_id.
        if (payload.len < constants.data_frame_header_length) {
            self.counters.increment(self.counter_ids.malformed_messages_dropped);
            return;
        }

        const header: *const DataFrameHeader = @ptrCast(@alignCast(payload.ptr));
        const target_node_id = header.target_node_id;

        // Look up peer
        const peer = self.peers.get(target_node_id) orelse {
            self.counters.increment(self.counter_ids.unknown_peer_messages_dropped);
            return;
        };

        // Must be connected (SETUP + initial SM exchange complete)
        if (!peer.connected) {
            self.counters.increment(self.counter_ids.disconnected_peer_messages_dropped);
            return;
        }

        // Flow control gate
        if (peer.isFlowControlled()) {
            self.counters.increment(self.counter_ids.send_back_pressure);
            return;
        }

        // Determine whether fragmentation is needed
        const max_payload = constants.default_mtu_length - constants.data_frame_header_length;

        if (payload.len > max_payload) {
            self.fragmentAndSend(peer, payload);
        } else {
            self.sendSingleFrame(peer, payload, constants.flag_unfragmented);
        }
    }

    // ── Message Fragmentation ─────────────────────────────────────────

    /// Fragment a message that exceeds the MTU and send each fragment as a
    /// separate data frame.
    fn fragmentAndSend(self: *Self, peer: *PeerSender, payload: []const u8) void {
        const max_payload = constants.default_mtu_length - constants.data_frame_header_length;
        var offset: usize = 0;
        var is_first: bool = true;

        while (offset < payload.len) {
            const remaining = payload.len - offset;
            const chunk_len = @min(remaining, max_payload);
            const is_last = (offset + chunk_len == payload.len);

            var flag: u8 = 0;
            if (is_first) flag |= constants.flag_begin;
            if (is_last) flag |= constants.flag_end;

            self.sendSingleFrame(peer, payload[offset..][0..chunk_len], flag);

            offset += chunk_len;
            is_first = false;

            // Re-check flow control between fragments
            if (peer.isFlowControlled()) {
                self.counters.increment(self.counter_ids.send_back_pressure);
                self.counters.increment(self.counter_ids.fragmented_messages_incomplete);
                return;
            }
        }

        self.counters.increment(self.counter_ids.fragmented_messages_sent);
    }

    // ── Sending a Frame ───────────────────────────────────────────────

    /// Build a data frame from the payload, store it in the retransmit buffer,
    /// and record it for transmission.
    fn sendSingleFrame(
        self: *Self,
        peer: *PeerSender,
        payload: []const u8,
        flag: u8,
    ) void {
        const seq = peer.nextSequence();
        const frame_length: usize = constants.data_frame_header_length + payload.len;

        // ── Acquire a pre-allocated send buffer from the pool ────────
        const send_buf = self.send_buffer_pool.acquire() orelse {
            self.counters.increment(self.counter_ids.send_buffer_pool_exhausted);
            return;
        };

        // ── Build the data frame header (40 bytes) ───────────────────
        const header: *DataFrameHeader = @ptrCast(@alignCast(send_buf.ptr));
        header.* = .{
            .frame_length = @intCast(frame_length),
            .flags = flag,
            .frame_type = @intFromEnum(FrameType.data),
            .term_offset = @intCast(peer.send_position),
            .source_node_id = self.local_node_id,
            .target_node_id = peer.node_id,
            .source_service_id = 0,
            .target_service_id = 0,
            .template_id = 0,
            .correlation_id = 0,
            .msg_flags = 0,
            .sequence_number = seq,
        };

        // Copy routing fields from the original BRZ message header in the payload.
        if (payload.len >= constants.data_frame_header_length) {
            const src_header: *const DataFrameHeader = @ptrCast(@alignCast(payload.ptr));
            header.source_service_id = src_header.source_service_id;
            header.target_service_id = src_header.target_service_id;
            header.template_id = src_header.template_id;
            header.correlation_id = src_header.correlation_id;
            header.msg_flags = src_header.msg_flags;
        }

        // ── Copy payload after the header ────────────────────────────
        if (payload.len > 0) {
            @memcpy(
                send_buf[constants.data_frame_header_length..][0..payload.len],
                payload,
            );
        }

        // ── Store in retransmit buffer (before send) ─────────────────
        peer.retransmit_buffer.store(seq, send_buf[0..frame_length]);

        // ── Record the send (actual I/O submission is done by the broker
        //    application layer which owns the NetworkIo instance) ──────
        self.pending_send_count += 1;

        // ── Advance send position ────────────────────────────────────
        peer.send_position += @as(i64, @intCast(constants.alignUp(frame_length, 32)));

        // ── Release the send buffer back to the pool ─────────────────
        // In the full implementation with io_uring, the buffer would be
        // released in the CQE completion callback. For now, we release
        // immediately since we've already copied into the retransmit buffer.
        self.send_buffer_pool.release(send_buf.ptr);

        // ── Update counters ──────────────────────────────────────────
        self.counters.increment(self.counter_ids.frames_sent);
        self.counters.add(self.counter_ids.bytes_sent, @intCast(frame_length));
    }

    // ── Handling Status Messages ──────────────────────────────────────

    /// Called when the sender receives a Status Message from a peer.
    /// Updates the flow-control send limit.
    pub fn handleStatusMessage(self: *Self, sm_bytes: []const u8) void {
        if (sm_bytes.len < @sizeOf(StatusMessage)) {
            self.counters.increment(self.counter_ids.malformed_frames_received);
            return;
        }

        const sm: *const StatusMessage = @ptrCast(@alignCast(sm_bytes.ptr));
        const peer = self.peers.get(sm.node_id) orelse {
            self.counters.increment(self.counter_ids.unknown_peer_sm_received);
            return;
        };

        // Update flow-control send limit. Use @max to ensure it never decreases.
        const new_limit = sm.consumption_position + @as(i64, sm.receiver_window);
        peer.send_limit = @max(peer.send_limit, new_limit);

        // Update liveness timestamp
        peer.last_sm_received_ns = Clock.monotonicNanos();

        // Mark connected on first SM
        if (!peer.connected) {
            peer.connected = true;
            self.counters.increment(self.counter_ids.peers_connected);
        }

        self.counters.increment(self.counter_ids.status_messages_received);
    }

    // ── Handling NAKs ─────────────────────────────────────────────────

    /// Called when the sender receives a NAK from a peer requesting retransmission.
    pub fn handleNak(self: *Self, nak_bytes: []const u8) void {
        if (nak_bytes.len < @sizeOf(NakFrame)) {
            self.counters.increment(self.counter_ids.malformed_frames_received);
            return;
        }

        const nak: *const NakFrame = @ptrCast(@alignCast(nak_bytes.ptr));
        const peer = self.peers.get(nak.node_id) orelse {
            self.counters.increment(self.counter_ids.unknown_peer_nak_received);
            return;
        };

        // Delegate to the per-peer retransmit handler
        peer.retransmit_handler.onNak(
            nak.position,
            nak.length,
            Clock.monotonicNanos(),
            peer.retransmit_buffer,
            &noopSendFn, // In the full implementation, this would enqueue via NetworkIo
        );

        self.counters.increment(self.counter_ids.naks_received);
    }

    fn noopSendFn(_: []const u8) void {
        // Placeholder — actual send is wired via NetworkIo in the broker application.
    }

    // ── Heartbeats ────────────────────────────────────────────────────

    /// Send heartbeat frames to all connected peers.
    fn sendHeartbeats(self: *Self, now_ns: i64) void {
        _ = now_ns;

        var peer_iter = self.peers.iterator();
        while (peer_iter.next()) |peer| {
            if (!peer.connected) continue;

            // Heartbeats are zero-length data frames. They do NOT
            // increment the sequence number.
            self.counters.increment(self.counter_ids.heartbeats_sent);
            self.pending_send_count += 1;

            // In the full implementation, we'd build a DataFrameHeader on
            // the stack and enqueue it via network_io.prepareSend(). The
            // heartbeat carries peer.currentSequence() to tell the receiver
            // "I have nothing new since this sequence."
            _ = peer.currentSequence();
        }
    }

    // ── Connection Setup ──────────────────────────────────────────────

    /// Send a SETUP frame to a newly added peer to initiate the connection.
    fn sendSetup(self: *Self, peer: *PeerSender) void {
        // Build a SETUP frame (24 bytes) on the stack
        _ = SetupFrame{
            .frame_length = @intCast(@sizeOf(SetupFrame)),
            .flags = 0,
            .frame_type = @intFromEnum(FrameType.setup),
            .source_node_id = self.local_node_id,
            .log_buffer_length = @intCast(constants.default_recv_log_buffer_length),
            .mtu_length = @intCast(constants.default_mtu_length),
            .initial_sequence = 0,
        };

        // In the full implementation, enqueue via network_io.prepareSend()
        _ = peer;
        self.counters.increment(self.counter_ids.setup_frames_sent);
    }

    // ── Peer Lifecycle Management ─────────────────────────────────────

    /// Add a new peer to the sender. Creates a PeerSender, allocates a
    /// retransmit buffer, and sends a SETUP frame.
    pub fn addPeer(self: *Self, node_id: u8, address: std.net.Address) !void {
        if (self.peers.contains(node_id)) return; // Already tracked

        // Allocate retransmit buffer on the heap
        const retransmit_buf = try self.allocator.create(RetransmitBuffer);
        errdefer self.allocator.destroy(retransmit_buf);

        retransmit_buf.* = try RetransmitBuffer.init(
            constants.default_retransmit_buffer_length,
            constants.default_mtu_length,
            self.allocator,
        );
        errdefer retransmit_buf.close(self.allocator);

        // Allocate PeerSender on the heap
        const peer = try self.allocator.create(PeerSender);
        errdefer self.allocator.destroy(peer);

        peer.* = PeerSender.init(
            node_id,
            address,
            -1, // Socket fd — opened by the broker application layer
            retransmit_buf,
        );

        self.peers.put(node_id, peer);

        // Initiate connection
        self.sendSetup(peer);
    }

    /// Remove a peer from the sender. Frees resources.
    pub fn removePeer(self: *Self, node_id: u8) void {
        if (self.peers.remove(node_id)) |peer| {
            peer.retransmit_buffer.close(self.allocator);
            self.allocator.destroy(peer.retransmit_buffer);
            self.allocator.destroy(peer);
            self.counters.increment(self.counter_ids.peers_disconnected);
        }
    }

    /// Dispatch a sender command from the control loop.
    pub fn dispatchCommand(self: *Self, cmd: SenderCommand) void {
        switch (cmd) {
            .add_peer => |add| {
                self.addPeer(add.node_id, add.address) catch {
                    self.counters.increment(self.counter_ids.peer_socket_errors);
                };
            },
            .remove_peer => |remove| {
                self.removePeer(remove.node_id);
            },
        }
    }

    // ── Peer Health Monitoring ────────────────────────────────────────

    /// Check peer health based on the last SM received timestamp.
    fn checkPeerHealth(self: *Self, now_ns: i64) void {
        const peer_timeout_ns = constants.sm_timeout_ns * 2;

        var peer_iter = self.peers.iterator();
        while (peer_iter.next()) |peer| {
            if (!peer.connected) continue;

            if (peer.last_sm_received_ns > 0 and
                now_ns - peer.last_sm_received_ns > peer_timeout_ns)
            {
                peer.connected = false;
                self.counters.increment(self.counter_ids.peers_timed_out);
            }
        }
    }

    // ── Read callback wiring ──────────────────────────────────────────

    /// Wrapper to set up the TLS self pointer and read from the ring buffer.
    /// This is necessary because RingBuffer.read expects a bare function pointer.
    pub fn readFromRingBuffer(self: *Self, limit: u32) u32 {
        tls_self = self;
        defer tls_self = null;
        return self.send_ring_buffer.read(onOutboundMessageThunk, limit);
    }
};

// ── Tests ─────────────────────────────────────────────────────────────

const testing = std.testing;

/// Helper to create a counters manager backed by test buffers.
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

    // Given: buffers for ring buffer and counters
    const rb_capacity: usize = 1024;
    const rb_buf_size = rb_capacity + constants.ring_buffer_trailer_length;
    const rb_buf = try allocator.alignedAlloc(u8, @enumFromInt(std.math.log2(@as(usize, constants.ring_buffer_alignment))), rb_buf_size);
    defer allocator.free(rb_buf);
    @memset(rb_buf, 0);

    var rb = try RingBuffer.init(rb_buf, false, null, null);

    var values_buf: [128 * 64]u8 align(128) = [_]u8{0} ** (128 * 64);
    var meta_buf: [256 * 64]u8 align(4) = [_]u8{0} ** (256 * 64);
    var counters = createTestCounters(&values_buf, &meta_buf);

    // When: create sender event loop
    var sender = try SenderEventLoop.init(&rb, &counters, 1, allocator);
    defer sender.deinit();

    // Then: basic state is correct
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

    // When: add two peers
    const addr1 = std.net.Address.initIp4(.{ 127, 0, 0, 1 }, 9001);
    const addr2 = std.net.Address.initIp4(.{ 127, 0, 0, 1 }, 9002);

    try sender.addPeer(1, addr1);
    try sender.addPeer(2, addr2);

    // Then: both peers exist
    try testing.expectEqual(@as(u32, 2), sender.peers.count);
    try testing.expect(sender.peers.get(1) != null);
    try testing.expect(sender.peers.get(2) != null);

    // When: remove peer 1
    sender.removePeer(1);

    // Then: only peer 2 remains
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

    // When: receive a payload too small for a data frame header
    const small_payload = [_]u8{ 0x01, 0x02, 0x03 };
    sender.onOutboundMessage(0, &small_payload);

    // Then: malformed counter incremented
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

    // Given: a valid-sized payload targeting node 5 (which doesn't exist)
    var payload_buf: [64]u8 align(8) = [_]u8{0} ** 64;
    const header: *DataFrameHeader = @ptrCast(@alignCast(&payload_buf));
    header.* = .{
        .frame_length = 64,
        .target_node_id = 5,
    };

    // When
    sender.onOutboundMessage(0, &payload_buf);

    // Then
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

    // Given: a peer that is not yet connected
    const addr = std.net.Address.initIp4(.{ 127, 0, 0, 1 }, 9001);
    try sender.addPeer(1, addr);
    // peer.connected defaults to false

    var payload_buf: [64]u8 align(8) = [_]u8{0} ** 64;
    const header: *DataFrameHeader = @ptrCast(@alignCast(&payload_buf));
    header.* = .{
        .frame_length = 64,
        .target_node_id = 1,
    };

    // When
    sender.onOutboundMessage(0, &payload_buf);

    // Then
    try testing.expectEqual(@as(i64, 1), counters.get(sender.counter_ids.disconnected_peer_messages_dropped));
}

test "onOutboundMessage drops message when peer is flow controlled" {
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

    // Given: a connected peer with send_limit = 0 (flow controlled)
    const addr = std.net.Address.initIp4(.{ 127, 0, 0, 1 }, 9001);
    try sender.addPeer(1, addr);
    const peer = sender.peers.get(1).?;
    peer.connected = true;
    peer.send_limit = 0; // Flow controlled

    var payload_buf: [64]u8 align(8) = [_]u8{0} ** 64;
    const header: *DataFrameHeader = @ptrCast(@alignCast(&payload_buf));
    header.* = .{
        .frame_length = 64,
        .target_node_id = 1,
    };

    // When
    sender.onOutboundMessage(0, &payload_buf);

    // Then
    try testing.expectEqual(@as(i64, 1), counters.get(sender.counter_ids.send_back_pressure));
}

test "onOutboundMessage sends single frame to connected peer" {
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

    // Given: a connected peer with a large send limit
    const addr = std.net.Address.initIp4(.{ 127, 0, 0, 1 }, 9001);
    try sender.addPeer(1, addr);
    const peer = sender.peers.get(1).?;
    peer.connected = true;
    peer.send_limit = 1_000_000;

    // Given: a valid message targeting node 1
    var payload_buf: [64]u8 align(8) = [_]u8{0} ** 64;
    const header: *DataFrameHeader = @ptrCast(@alignCast(&payload_buf));
    header.* = .{
        .frame_length = 64,
        .target_node_id = 1,
        .source_service_id = 5,
        .target_service_id = 10,
        .template_id = 42,
        .correlation_id = 12345,
    };

    // When
    sender.onOutboundMessage(0, &payload_buf);

    // Then: frame was sent (counter incremented)
    try testing.expectEqual(@as(i64, 1), counters.get(sender.counter_ids.frames_sent));
    try testing.expect(counters.get(sender.counter_ids.bytes_sent) > 0);

    // And: peer's sequence and position advanced
    try testing.expectEqual(@as(i64, 1), peer.sequence_number);
    try testing.expect(peer.send_position > 0);

    // And: frame is stored in the retransmit buffer
    try testing.expect(peer.retransmit_buffer.isAvailable(0));
}

test "handleStatusMessage updates peer send limit and marks connected" {
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

    // Given: a peer that is not yet connected
    const addr = std.net.Address.initIp4(.{ 127, 0, 0, 1 }, 9001);
    try sender.addPeer(1, addr);
    const peer = sender.peers.get(1).?;
    try testing.expectEqual(@as(i64, 0), peer.send_limit);
    try testing.expect(!peer.connected);

    // When: SM received with consumption_position=1000, receiver_window=4096
    var sm_buf: [@sizeOf(StatusMessage)]u8 align(8) = [_]u8{0} ** @sizeOf(StatusMessage);
    const sm: *StatusMessage = @ptrCast(@alignCast(&sm_buf));
    sm.* = .{
        .frame_type = @intFromEnum(FrameType.sm),
        .node_id = 1,
        .consumption_position = 1000,
        .receiver_window = 4096,
    };

    sender.handleStatusMessage(&sm_buf);

    // Then: send_limit = 1000 + 4096 = 5096
    try testing.expectEqual(@as(i64, 5096), peer.send_limit);
    try testing.expect(peer.connected);
    try testing.expectEqual(@as(i64, 1), counters.get(sender.counter_ids.status_messages_received));
    try testing.expectEqual(@as(i64, 1), counters.get(sender.counter_ids.peers_connected));
}

test "handleStatusMessage send_limit is monotonically non-decreasing" {
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

    // Given: first SM sets limit to 5096
    var sm_buf: [@sizeOf(StatusMessage)]u8 align(8) = [_]u8{0} ** @sizeOf(StatusMessage);
    const sm: *StatusMessage = @ptrCast(@alignCast(&sm_buf));
    sm.* = .{
        .frame_type = @intFromEnum(FrameType.sm),
        .node_id = 1,
        .consumption_position = 1000,
        .receiver_window = 4096,
    };
    sender.handleStatusMessage(&sm_buf);
    try testing.expectEqual(@as(i64, 5096), peer.send_limit);

    // When: stale SM with lower limit arrives
    sm.consumption_position = 500;
    sm.receiver_window = 2000;
    sender.handleStatusMessage(&sm_buf);

    // Then: send_limit should NOT decrease
    try testing.expectEqual(@as(i64, 5096), peer.send_limit);
}

test "handleNak increments naks_received counter" {
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

    // When: NAK received
    var nak_buf: [@sizeOf(NakFrame)]u8 align(8) = [_]u8{0} ** @sizeOf(NakFrame);
    const nak: *NakFrame = @ptrCast(@alignCast(&nak_buf));
    nak.* = .{
        .frame_type = @intFromEnum(FrameType.nak),
        .node_id = 1,
        .position = 0,
        .length = 32,
    };

    sender.handleNak(&nak_buf);

    // Then
    try testing.expectEqual(@as(i64, 1), counters.get(sender.counter_ids.naks_received));
}

test "handleNak for unknown peer increments counter" {
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

    // When: NAK for unknown peer 99
    var nak_buf: [@sizeOf(NakFrame)]u8 align(8) = [_]u8{0} ** @sizeOf(NakFrame);
    const nak: *NakFrame = @ptrCast(@alignCast(&nak_buf));
    nak.* = .{
        .frame_type = @intFromEnum(FrameType.nak),
        .node_id = 99,
        .position = 0,
        .length = 32,
    };

    sender.handleNak(&nak_buf);

    // Then
    try testing.expectEqual(@as(i64, 1), counters.get(sender.counter_ids.unknown_peer_nak_received));
}

test "dispatchCommand add_peer and remove_peer" {
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

    // When: dispatch add_peer command
    const addr = std.net.Address.initIp4(.{ 127, 0, 0, 1 }, 9001);
    sender.dispatchCommand(.{ .add_peer = .{ .node_id = 3, .address = addr } });

    // Then: peer exists
    try testing.expect(sender.peers.get(3) != null);

    // When: dispatch remove_peer command
    sender.dispatchCommand(.{ .remove_peer = .{ .node_id = 3 } });

    // Then: peer is gone
    try testing.expect(sender.peers.get(3) == null);
}

test "PeerMap iterator visits all peers" {
    var map = PeerMap.init();

    // We need stable peer allocations for this test
    var peer1 = PeerSender{
        .node_id = 1,
        .address = std.net.Address.initIp4(.{ 127, 0, 0, 1 }, 9001),
        .send_limit = 0,
        .send_position = 0,
        .sequence_number = 0,
        .retransmit_buffer = undefined,
        .retransmit_handler = RetransmitHandler.init(),
        .connected = false,
        .last_sm_received_ns = 0,
        .socket_fd = -1,
    };
    var peer2 = PeerSender{
        .node_id = 5,
        .address = std.net.Address.initIp4(.{ 127, 0, 0, 1 }, 9002),
        .send_limit = 0,
        .send_position = 0,
        .sequence_number = 0,
        .retransmit_buffer = undefined,
        .retransmit_handler = RetransmitHandler.init(),
        .connected = false,
        .last_sm_received_ns = 0,
        .socket_fd = -1,
    };

    map.put(1, &peer1);
    map.put(5, &peer2);

    // When: iterate
    var iter = map.iterator();
    var count: u32 = 0;
    while (iter.next()) |_| {
        count += 1;
    }

    // Then: visited both peers
    try testing.expectEqual(@as(u32, 2), count);
}

test "checkPeerHealth marks timed-out peer as disconnected" {
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
    peer.connected = true;
    peer.last_sm_received_ns = 1_000_000; // Very old timestamp

    // When: check health at a much later time (past sm_timeout_ns * 2)
    const far_future_ns: i64 = 1_000_000 + constants.sm_timeout_ns * 2 + 1;
    sender.checkPeerHealth(far_future_ns);

    // Then: peer should be disconnected
    try testing.expect(!peer.connected);
    try testing.expectEqual(@as(i64, 1), counters.get(sender.counter_ids.peers_timed_out));
}
