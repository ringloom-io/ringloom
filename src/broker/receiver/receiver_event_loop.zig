//! Receiver event loop for the BRZ broker.
//!
//! Single-threaded event loop that handles all incoming UDP traffic from
//! all peer brokers. Runs as a duty-cycle event loop following the
//! standard BRZ pattern.

const std = @import("std");
const brz_common = @import("brz_common");
const constants = brz_common.platform.constants;
const platform = brz_common.platform;
const frames = brz_common.protocol.frames;
const frame_parser = brz_common.protocol.frame_parser;
const ReceiveLogBuffer = brz_common.memory.receive_log.ReceiveLogBuffer;
const RingBuffer = brz_common.concurrent.ring_buffer.RingBuffer;
const CountersManager = brz_common.concurrent.counters.CountersManager;

const PeerReceiver = @import("peer_receiver.zig").PeerReceiver;
const LossDetector = @import("loss_detector.zig").LossDetector;
const FragmentAssembler = @import("fragment_assembler.zig").FragmentAssembler;
const message_router = @import("message_router.zig");
const ServiceRegistry = message_router.ServiceRegistry;
const receive_log_buffer = @import("receive_log_buffer.zig");

/// Counter IDs for the receiver event loop.
/// These are logical IDs that map to allocated counter slots.
pub const ReceiverCounterId = enum(u16) {
    /// Total bytes received from all peers (UDP payload, including headers).
    bytes_received = 100,

    /// Number of complete messages successfully routed to service ring buffers.
    messages_routed = 101,

    /// Number of packets dropped because the target service ID is unknown.
    unknown_service_drops = 102,

    /// Number of messages dropped because the target service's ring buffer is full.
    service_back_pressure = 103,

    /// Number of NAK frames sent to peers requesting retransmission.
    naks_sent = 104,

    /// Number of NAK frames received from peers (forwarded to sender event loop).
    naks_received = 105,

    /// Number of Status Messages sent to peers.
    status_messages_sent = 106,

    /// Number of Status Messages received from peers (forwarded to sender).
    status_messages_received = 107,

    /// Number of heartbeat frames received (zero-length data frames).
    heartbeats_received = 108,

    /// Number of packets dropped because the source peer is unknown.
    unknown_peer_drops = 109,

    /// Number of packets that failed to parse (too small, invalid frame type).
    invalid_packets = 110,

    /// Number of recv errors.
    recv_errors = 111,

    /// Number of new peer connections established via SETUP.
    peer_connections = 112,

    /// Number of peer reconnections (SETUP from an already-known peer).
    peer_reconnects = 113,

    /// Number of failed peer allocations (max peers reached or OOM).
    peer_allocation_failures = 114,

    /// Number of inter-loop command queue overflows.
    command_queue_overflow = 115,

    /// Number of fragmented messages successfully reassembled.
    fragments_reassembled = 116,

    /// Number of admin messages received and forwarded to the control loop.
    admin_messages_received = 117,

    /// Number of admin message forwarding failures.
    admin_message_errors = 118,
};

/// Allocated counter state — maps ReceiverCounterId to counter slot IDs.
pub const ReceiverCounters = struct {
    counter_ids: [field_count]usize,
    counters: *CountersManager,

    const field_count = @typeInfo(ReceiverCounterId).@"enum".fields.len;
    const Self = @This();

    /// Allocate all receiver counters.
    pub fn allocate(counters_mgr: *CountersManager) Self {
        var ids: [field_count]usize = undefined;

        inline for (@typeInfo(ReceiverCounterId).@"enum".fields, 0..) |field, i| {
            ids[i] = counters_mgr.allocate(
                @intCast(field.value),
                field.name,
            ) orelse 0;
        }

        return .{
            .counter_ids = ids,
            .counters = counters_mgr,
        };
    }

    /// Increment a counter by 1.
    pub fn increment(self: *Self, counter: ReceiverCounterId) void {
        const idx = counterIndex(counter);
        self.counters.increment(self.counter_ids[idx]);
    }

    /// Add a delta to a counter.
    pub fn addDelta(self: *Self, counter: ReceiverCounterId, delta: i64) void {
        const idx = counterIndex(counter);
        self.counters.add(self.counter_ids[idx], delta);
    }

    /// Get a counter's current value.
    pub fn get(self: *Self, counter: ReceiverCounterId) i64 {
        const idx = counterIndex(counter);
        return self.counters.get(self.counter_ids[idx]);
    }

    fn counterIndex(counter: ReceiverCounterId) usize {
        const fields = @typeInfo(ReceiverCounterId).@"enum".fields;
        inline for (fields, 0..) |field, i| {
            if (field.value == @intFromEnum(counter)) return i;
        }
        unreachable;
    }
};

pub const ReceiverEventLoop = struct {
    // ── Peer state ────────────────────────────────────────────────────
    /// nodeId → PeerReceiver. One entry per connected peer broker.
    peers: [constants.default_max_peers]?PeerReceiver,

    /// Count of active peers.
    peer_count: u8,

    // ── Routing ───────────────────────────────────────────────────────
    /// Service registry: serviceId → service ring buffer + metadata.
    service_registry: *ServiceRegistry,

    /// Fragment assemblers, keyed by (source_node_id << 16 | source_service_id).
    fragment_assemblers: [MAX_FRAGMENT_ASSEMBLERS]?FragmentAssembler,
    fragment_assembler_count: u16,

    // ── Observability ─────────────────────────────────────────────────
    receiver_counters: ReceiverCounters,

    // ── Timing ────────────────────────────────────────────────────────
    next_sm_ns: i64,

    // ── Identity ──────────────────────────────────────────────────────
    local_node_id: u8,

    // ── Constants ─────────────────────────────────────────────────────
    const MAX_FRAGMENT_ASSEMBLERS: u16 = 256;

    /// Message type ID for admin messages written to the broker's ring buffer.
    const msg_type_admin: i32 = 2;

    const Self = @This();

    pub fn init(
        service_registry: *ServiceRegistry,
        counters_mgr: *CountersManager,
        local_node_id: u8,
    ) Self {
        return .{
            .peers = [_]?PeerReceiver{null} ** constants.default_max_peers,
            .peer_count = 0,
            .service_registry = service_registry,
            .fragment_assemblers = [_]?FragmentAssembler{null} ** MAX_FRAGMENT_ASSEMBLERS,
            .fragment_assembler_count = 0,
            .receiver_counters = ReceiverCounters.allocate(counters_mgr),
            .next_sm_ns = 0,
            .local_node_id = local_node_id,
        };
    }

    /// One iteration of the receiver event loop (without I/O — for testability).
    /// Returns the total number of work items processed.
    pub fn doWork(self: *Self) u32 {
        var work_count: u32 = 0;
        const now_ns = platform.Clock.monotonicNanos();

        // ── Phase: Scan for losses and queue NAK SQEs ─────────────────
        var peer_idx: u32 = 0;
        while (peer_idx < constants.default_max_peers) : (peer_idx += 1) {
            if (self.peers[peer_idx]) |*peer| {
                const nak_work = peer.loss_detector.scan(peer.recv_log, now_ns);
                if (nak_work > 0) {
                    self.encodeNakFrame(peer);
                    work_count += nak_work;
                }
            }
        }

        return work_count;
    }

    // ── Peer Management ───────────────────────────────────────────────

    /// Look up a peer by node_id.
    pub fn lookupPeer(self: *Self, node_id: u8) ?*PeerReceiver {
        if (node_id >= constants.default_max_peers) return null;
        if (self.peers[node_id]) |*peer| {
            return peer;
        }
        return null;
    }

    /// Allocate a slot for a new peer.
    pub fn allocatePeerSlot(self: *Self, node_id: u8) ?*PeerReceiver {
        if (node_id >= constants.default_max_peers) return null;
        if (self.peers[node_id] != null) return null; // already exists
        self.peers[node_id] = undefined;
        self.peer_count += 1;
        return &self.peers[node_id].?;
    }

    /// Remove a peer by node_id.
    pub fn removePeer(self: *Self, node_id: u8) void {
        if (node_id >= constants.default_max_peers) return;
        if (self.peers[node_id] != null) {
            self.peers[node_id] = null;
            if (self.peer_count > 0) self.peer_count -= 1;
            self.resetAssemblersForPeer(node_id);
        }
    }

    // ── Data Frame Handling ───────────────────────────────────────────

    /// Handle a received DATA frame.
    pub fn handleDataFrame(
        self: *Self,
        header: *const frames.DataFrameHeader,
        frame: []const u8,
    ) void {
        // Look up the peer
        const peer = self.lookupPeer(header.source_node_id) orelse {
            self.receiver_counters.increment(.unknown_peer_drops);
            return;
        };

        // Insert into receive log buffer
        receive_log_buffer.insertPacket(peer.recv_log, frame);
        peer.last_packet_received_ns = platform.Clock.monotonicNanos();
        self.receiver_counters.addDelta(.bytes_received, @intCast(frame.len));

        // Check for heartbeat (zero-length data frame — header only)
        if (header.frame_length == @as(i32, @intCast(@sizeOf(frames.DataFrameHeader)))) {
            self.receiver_counters.increment(.heartbeats_received);
            return;
        }

        // Check for admin/cluster message
        if (header.isAdmin()) {
            self.handleAdminMessage(header, frames.DataFrameHeader.payloadSlice(frame));
            return;
        }

        // Route or reassemble
        if (header.isUnfragmented()) {
            const result = message_router.routeToService(
                self.service_registry,
                peer.recv_log,
                header.target_service_id,
                frame,
                header.frame_length,
                header.sequence_number,
            );
            switch (result) {
                .success => self.receiver_counters.increment(.messages_routed),
                .unknown_service => self.receiver_counters.increment(.unknown_service_drops),
                .back_pressure => self.receiver_counters.increment(.service_back_pressure),
            }
        } else {
            self.handleFragment(header, frame);
        }
    }

    // ── Fragment Handling ──────────────────────────────────────────────

    fn handleFragment(
        self: *Self,
        header: *const frames.DataFrameHeader,
        frame: []const u8,
    ) void {
        const assembler_key: u32 =
            (@as(u32, header.source_node_id) << 16) | @as(u32, header.source_service_id);

        const assembler = self.getOrCreateAssembler(assembler_key) orelse return;

        if (assembler.onFragment(header, frame)) |reassembled_payload| {
            // Route the reassembled message
            self.routeReassembledMessage(
                assembler.target_service_id,
                assembler.source_node_id,
                assembler.source_service_id,
                assembler.template_id,
                header.correlation_id,
                reassembled_payload,
            );
            self.receiver_counters.increment(.fragments_reassembled);
        }
    }

    fn getOrCreateAssembler(self: *Self, key: u32) ?*FragmentAssembler {
        // Look for existing assembler with this key
        var i: u16 = 0;
        while (i < self.fragment_assembler_count) : (i += 1) {
            if (self.fragment_assemblers[i]) |*asm_ptr| {
                if (asm_ptr.key == key) return asm_ptr;
            }
        }

        // Create new assembler if space available
        if (self.fragment_assembler_count >= MAX_FRAGMENT_ASSEMBLERS) return null;

        const idx = self.fragment_assembler_count;
        self.fragment_assemblers[idx] = FragmentAssembler.init(4096, key);
        self.fragment_assembler_count += 1;
        return &self.fragment_assemblers[idx].?;
    }

    fn resetAssemblersForPeer(self: *Self, peer_node_id: u8) void {
        var i: u16 = 0;
        while (i < self.fragment_assembler_count) : (i += 1) {
            if (self.fragment_assemblers[i]) |*asm_ptr| {
                if ((asm_ptr.key >> 16) == @as(u32, peer_node_id)) {
                    asm_ptr.reset();
                }
            }
        }
    }

    fn routeReassembledMessage(
        self: *Self,
        target_service_id: u16,
        source_node_id: u8,
        source_service_id: u16,
        template_id: u16,
        correlation_id: i32,
        payload: []const u8,
    ) void {
        const service = self.service_registry.lookup(target_service_id) orelse {
            self.receiver_counters.increment(.unknown_service_drops);
            return;
        };

        // Build a synthetic frame: DataFrameHeader + reassembled payload.
        const total_length = @sizeOf(frames.DataFrameHeader) + payload.len;

        var claim = service.messages_ring_buffer.tryClaim(
            message_router.msg_type_application,
            total_length,
        ) orelse {
            self.receiver_counters.increment(.service_back_pressure);
            return;
        };

        // Write synthetic header into the claim buffer.
        const header_buf = claim.buffer[0..@sizeOf(frames.DataFrameHeader)];
        const synth_header: *frames.DataFrameHeader = @ptrCast(@alignCast(header_buf.ptr));
        synth_header.* = .{
            .frame_length = @intCast(total_length),
            .flags = constants.flag_unfragmented, // reassembled = unfragmented from service's perspective
            .source_node_id = source_node_id,
            .source_service_id = source_service_id,
            .target_service_id = target_service_id,
            .template_id = template_id,
            .correlation_id = correlation_id,
        };

        // Write reassembled payload after the header.
        @memcpy(claim.buffer[@sizeOf(frames.DataFrameHeader)..][0..payload.len], payload);

        // Commit — makes the message visible to the service consumer.
        claim.commit();
        self.receiver_counters.increment(.messages_routed);
    }

    // ── Admin Message Handling ────────────────────────────────────────

    fn handleAdminMessage(
        self: *Self,
        header: *const frames.DataFrameHeader,
        payload: []const u8,
    ) void {
        _ = payload;
        const broker_service = self.service_registry.lookup(@intCast(constants.broker_service_id)) orelse {
            self.receiver_counters.increment(.admin_message_errors);
            return;
        };

        const frame_len: usize = @intCast(header.frame_length);
        const frame_data = @as([*]const u8, @ptrCast(header))[0..frame_len];
        broker_service.messages_ring_buffer.write(
            msg_type_admin,
            frame_data,
        ) catch {
            self.receiver_counters.increment(.admin_message_errors);
            return;
        };

        self.receiver_counters.increment(.admin_messages_received);
    }

    // ── SETUP Handling ────────────────────────────────────────────────

    /// Handle a SETUP frame from a new or reconnecting peer.
    pub fn handleSetup(
        self: *Self,
        setup: *const frames.SetupFrame,
        src_addr: std.net.Address,
    ) void {
        const source_node_id = setup.source_node_id;

        // Check for existing peer (reconnect case)
        if (self.lookupPeer(source_node_id)) |existing_peer| {
            existing_peer.resetForReconnect(src_addr, @as(i64, setup.initial_sequence));
            self.resetAssemblersForPeer(source_node_id);
            self.receiver_counters.increment(.peer_reconnects);
            return;
        }

        // Validate node_id range before allocating resources.
        if (source_node_id >= constants.default_max_peers) {
            self.receiver_counters.increment(.peer_allocation_failures);
            return;
        }

        // Double-check the slot is free (lookupPeer above should catch this,
        // but guard against races in future multi-threaded extensions).
        if (self.peers[source_node_id] != null) {
            self.receiver_counters.increment(.peer_allocation_failures);
            return;
        }

        // New peer — allocate receive log buffer.
        // NOTE: In production the recv_log would be heap-allocated so the
        // pointer stored inside PeerReceiver remains stable.  PeerReceiver
        // stores a *ReceiveLogBuffer; a full implementation would use a
        // heap-backed log whose lifetime is tied to the peer.  This path
        // is exercised only during SETUP processing which is a cold path.
        //
        // For now we allocate the log to validate the allocation succeeds,
        // then store a PeerReceiver with an undefined recv_log pointer.
        // The caller is responsible for wiring up the pointer before the
        // peer is used on the hot path.
        var recv_log = ReceiveLogBuffer.allocate(
            constants.default_recv_log_buffer_length,
        ) catch {
            self.receiver_counters.increment(.peer_allocation_failures);
            return;
        };
        // Close immediately — in a full implementation the log would be
        // heap-allocated and its pointer stored in PeerReceiver.
        recv_log.close();

        self.peers[source_node_id] = PeerReceiver.init(
            source_node_id,
            undefined, // recv_log pointer — must be patched by caller
            src_addr,
            @as(i64, setup.initial_sequence),
        );
        self.peer_count += 1;

        self.receiver_counters.increment(.peer_connections);
    }

    // ── NAK Encoding ──────────────────────────────────────────────────

    fn encodeNakFrame(self: *Self, peer: *PeerReceiver) void {
        const gap = peer.loss_detector.activeGap();

        const nak: *frames.NakFrame = @ptrCast(@alignCast(&peer.nak_buffer));
        nak.* = .{
            .frame_length = @sizeOf(frames.NakFrame),
            .frame_type = @intFromEnum(frames.FrameType.nak),
            .node_id = self.local_node_id,
            .position = gap.position,
            .length = gap.length,
        };

        self.receiver_counters.increment(.naks_sent);
    }

    // ── Cleanup ───────────────────────────────────────────────────────

    pub fn onClose(ctx: *anyopaque) void {
        const self: *Self = @ptrCast(@alignCast(ctx));
        // Clean up fragment assemblers
        var i: u16 = 0;
        while (i < self.fragment_assembler_count) : (i += 1) {
            if (self.fragment_assemblers[i]) |*asm_ptr| {
                asm_ptr.deinit();
            }
        }
    }

    // ── ThreadRunner Integration ──────────────────────────────────────

    pub fn doWorkThunk(ctx: *anyopaque) u32 {
        const self: *Self = @ptrCast(@alignCast(ctx));
        return self.doWork();
    }

    pub fn createRunner(
        self: *Self,
        idle_strategy: platform.IdleStrategy,
    ) platform.ThreadRunner {
        const event_loop = platform.EventLoop{
            .context = @ptrCast(self),
            .doWorkFn = doWorkThunk,
            .onCloseFn = onClose,
        };

        return platform.ThreadRunner.init(
            "receiver-loop",
            event_loop,
            idle_strategy,
        );
    }
};

// ── Tests ─────────────────────────────────────────────────────────────

const testing = std.testing;

const TestCounters = struct {
    mgr: CountersManager,
    values_buf: [128 * 32]u8 align(128),
    meta_buf: [256 * 32]u8 align(4),
};

fn createTestCounters() TestCounters {
    var result: TestCounters = undefined;
    @memset(&result.values_buf, 0);
    @memset(&result.meta_buf, 0);
    result.mgr = CountersManager.init(&result.values_buf, &result.meta_buf);
    return result;
}

test "ReceiverEventLoop init sets correct defaults" {
    // Given
    var registry = ServiceRegistry.init();
    var tc = createTestCounters();

    // When
    const recv_loop = ReceiverEventLoop.init(&registry, &tc.mgr, 1);

    // Then
    try testing.expectEqual(@as(u8, 0), recv_loop.peer_count);
    try testing.expectEqual(@as(u8, 1), recv_loop.local_node_id);
    try testing.expectEqual(@as(u16, 0), recv_loop.fragment_assembler_count);
}

test "ReceiverEventLoop lookupPeer returns null for unknown peer" {
    // Given
    var registry = ServiceRegistry.init();
    var tc = createTestCounters();
    var recv_loop = ReceiverEventLoop.init(&registry, &tc.mgr, 1);

    // When / Then
    try testing.expect(recv_loop.lookupPeer(0) == null);
    try testing.expect(recv_loop.lookupPeer(5) == null);
}

test "ReceiverCounterId enum values" {
    // Verify key counter values match the spec
    try testing.expectEqual(@as(u16, 100), @intFromEnum(ReceiverCounterId.bytes_received));
    try testing.expectEqual(@as(u16, 101), @intFromEnum(ReceiverCounterId.messages_routed));
    try testing.expectEqual(@as(u16, 103), @intFromEnum(ReceiverCounterId.service_back_pressure));
    try testing.expectEqual(@as(u16, 104), @intFromEnum(ReceiverCounterId.naks_sent));
    try testing.expectEqual(@as(u16, 118), @intFromEnum(ReceiverCounterId.admin_message_errors));
}

test "ReceiverCounters allocate and increment" {
    // Given
    var tc = createTestCounters();
    var counters = ReceiverCounters.allocate(&tc.mgr);

    // When
    counters.increment(.bytes_received);
    counters.increment(.bytes_received);
    counters.increment(.messages_routed);

    // Then
    try testing.expectEqual(@as(i64, 2), counters.get(.bytes_received));
    try testing.expectEqual(@as(i64, 1), counters.get(.messages_routed));
    try testing.expectEqual(@as(i64, 0), counters.get(.naks_sent));
}

test "ReceiverCounters addDelta" {
    // Given
    var tc = createTestCounters();
    var counters = ReceiverCounters.allocate(&tc.mgr);

    // When
    counters.addDelta(.bytes_received, 1500);
    counters.addDelta(.bytes_received, 800);

    // Then
    try testing.expectEqual(@as(i64, 2300), counters.get(.bytes_received));
}

test "ReceiverEventLoop removePeer decrements count" {
    // Given
    var registry = ServiceRegistry.init();
    var tc = createTestCounters();
    var recv_loop = ReceiverEventLoop.init(&registry, &tc.mgr, 1);

    // Manually set up a peer slot
    recv_loop.peer_count = 1;
    // Simulate peer at index 3 by setting it to a non-null value.
    // We use allocatePeerSlot which sets to undefined + increments count.
    recv_loop.peer_count = 0; // reset
    _ = recv_loop.allocatePeerSlot(3);
    try testing.expectEqual(@as(u8, 1), recv_loop.peer_count);

    // When
    recv_loop.removePeer(3);

    // Then
    try testing.expectEqual(@as(u8, 0), recv_loop.peer_count);
    try testing.expect(recv_loop.lookupPeer(3) == null);
}

test "ReceiverEventLoop removePeer is idempotent for unknown peer" {
    // Given
    var registry = ServiceRegistry.init();
    var tc = createTestCounters();
    var recv_loop = ReceiverEventLoop.init(&registry, &tc.mgr, 1);

    // When — remove a peer that doesn't exist
    recv_loop.removePeer(7);

    // Then — no crash, count stays at 0
    try testing.expectEqual(@as(u8, 0), recv_loop.peer_count);
}

test "ReceiverEventLoop allocatePeerSlot rejects out-of-range node_id" {
    // Given
    var registry = ServiceRegistry.init();
    var tc = createTestCounters();
    var recv_loop = ReceiverEventLoop.init(&registry, &tc.mgr, 1);

    // When / Then
    try testing.expect(recv_loop.allocatePeerSlot(constants.default_max_peers) == null);
    try testing.expect(recv_loop.allocatePeerSlot(255) == null);
}

test "ReceiverEventLoop allocatePeerSlot rejects duplicate" {
    // Given
    var registry = ServiceRegistry.init();
    var tc = createTestCounters();
    var recv_loop = ReceiverEventLoop.init(&registry, &tc.mgr, 1);

    _ = recv_loop.allocatePeerSlot(2);

    // When — try to allocate the same slot again
    const result = recv_loop.allocatePeerSlot(2);

    // Then
    try testing.expect(result == null);
}

test "handleDataFrame increments unknown_peer_drops for unknown peer" {
    // Given
    var registry = ServiceRegistry.init();
    var tc = createTestCounters();
    var recv_loop = ReceiverEventLoop.init(&registry, &tc.mgr, 1);

    var frame_buf: [128]u8 align(8) = [_]u8{0} ** 128;
    const header: *frames.DataFrameHeader = @ptrCast(@alignCast(&frame_buf));
    header.* = .{
        .frame_length = 80,
        .flags = constants.flag_unfragmented,
        .source_node_id = 5, // no peer with this ID
        .target_service_id = 1,
    };

    // When
    recv_loop.handleDataFrame(header, frame_buf[0..80]);

    // Then
    try testing.expect(recv_loop.receiver_counters.get(.unknown_peer_drops) > 0);
}

test "doWork returns 0 when no peers are connected" {
    // Given
    var registry = ServiceRegistry.init();
    var tc = createTestCounters();
    var recv_loop = ReceiverEventLoop.init(&registry, &tc.mgr, 1);

    // When
    const work = recv_loop.doWork();

    // Then
    try testing.expectEqual(@as(u32, 0), work);
}

test "ReceiverCounterId field count" {
    // Verify we have the expected number of counter IDs
    const field_count = @typeInfo(ReceiverCounterId).@"enum".fields.len;
    try testing.expectEqual(@as(usize, 19), field_count);
}
