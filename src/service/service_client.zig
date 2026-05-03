//! ServiceClient — client-side proxy for sending messages to a named service.
//!
//! Encapsulates service discovery, instance tracking, and load balancing.
//! Automatically routes via same-host direct path or cross-host broker-routed
//! path depending on the target instance's node_id.

const std = @import("std");
const ringloom_common = @import("ringloom_common");
const IpcProducer = @import("ipc/ipc_producer.zig").IpcProducer;
const ServiceInstance = @import("service_instance.zig").ServiceInstance;
const load_balancer = @import("load_balancer.zig");
const message_header = ringloom_common.message.message_header;
const memory = ringloom_common.memory;
const constants = ringloom_common.memory.constants;
const RingBuffer = ringloom_common.concurrent.ring_buffer.RingBuffer;
const FlowControlRegion = memory.FlowControlRegion;
const FlowControlEntry = memory.FlowControlEntry;
const PeerSendCountersRegion = memory.PeerSendCountersRegion;
const fc_config_mod = @import("flow_control_config.zig");
const FlowControlConfig = fc_config_mod.FlowControlConfig;
const BackpressureStrategy = fc_config_mod.BackpressureStrategy;
const Clock = ringloom_common.platform.Clock;

const frame_parser = ringloom_common.protocol.frame_parser;
const TcpFrameHeader = frame_parser.TcpFrameHeader;
const BrokerMetadataFile = memory.BrokerMetadataFile;
const MessageHeader = message_header.MessageHeader;

pub const ServiceClient = struct {
    service_name: []const u8,
    instances: std.ArrayList(ServiceInstance),
    allocator: std.mem.Allocator,
    balancer: load_balancer.ClientLoadBalancer,

    /// IPC context — set during RingLoomEngine initialization.
    broker_meta: ?*BrokerMetadataFile,
    local_node_id: i16,
    local_service_id: i32,

    /// Flow control configuration.
    fc_config: FlowControlConfig = .{},

    /// Cached pointer to the flow control counters region (null if FC disabled).
    fc_region: ?FlowControlRegion = null,

    /// Cached pointer to the per-peer send counters region (null if disabled).
    peer_send_counters: ?PeerSendCountersRegion = null,

    const Self = @This();

    pub const SendError = error{
        NoAvailableInstance,
        ProducerNotInitialized,
        SendBufferFull,
        NoLeaderAvailable,
        BackPressure,
        BackPressureTimeout,
        PeerCongested,
        PeerDisconnected,
    } || RingBuffer.WriteError;

    pub fn init(
        allocator: std.mem.Allocator,
        service_name: []const u8,
        broker_meta: ?*BrokerMetadataFile,
        local_node_id: i16,
        local_service_id: i32,
    ) Self {
        return .{
            .service_name = service_name,
            .instances = .empty,
            .allocator = allocator,
            .balancer = .{ .round_robin = .{} },
            .broker_meta = broker_meta,
            .local_node_id = local_node_id,
            .local_service_id = local_service_id,
        };
    }

    /// Initialize with flow control configuration.
    pub fn initWithFlowControl(
        allocator: std.mem.Allocator,
        service_name: []const u8,
        broker_meta: ?*BrokerMetadataFile,
        local_node_id: i16,
        local_service_id: i32,
        fc_config: FlowControlConfig,
        fc_region: ?FlowControlRegion,
        peer_send_counters_region: ?PeerSendCountersRegion,
    ) Self {
        return .{
            .service_name = service_name,
            .instances = .empty,
            .allocator = allocator,
            .balancer = .{ .round_robin = .{} },
            .broker_meta = broker_meta,
            .local_node_id = local_node_id,
            .local_service_id = local_service_id,
            .fc_config = fc_config,
            .fc_region = fc_region,
            .peer_send_counters = peer_send_counters_region,
        };
    }

    /// Send a message to one instance of this service, selected by the
    /// load balancer.
    pub fn send(self: *Self, payload: []const u8) SendError!void {
        const instance = self.balancer.next(self.instances.items) orelse
            return error.NoAvailableInstance;

        if (self.fc_config.enabled) {
            try self.applyFlowControl(instance, payload.len);
        }

        if (instance.node_id == self.local_node_id) {
            // Same-host direct path.
            const producer = instance.ipc_producer orelse
                return error.ProducerNotInitialized;
            try producer.write(constants.application_msg_type_id, payload);
        } else {
            // Cross-host routed path.
            try self.sendToRemoteService(
                instance.node_id,
                instance.service_id,
                payload,
            );
        }
    }

    /// Send a message to a specific instance (bypasses load balancer).
    pub fn sendTo(self: *Self, target_service_id: i32, payload: []const u8) SendError!void {
        const instance = self.findInstance(target_service_id) orelse
            return error.NoAvailableInstance;

        if (self.fc_config.enabled) {
            try self.applyFlowControl(instance, payload.len);
        }

        if (instance.node_id == self.local_node_id) {
            const producer = instance.ipc_producer orelse
                return error.ProducerNotInitialized;
            try producer.write(constants.application_msg_type_id, payload);
        } else {
            try self.sendToRemoteService(
                instance.node_id,
                instance.service_id,
                payload,
            );
        }
    }

    /// Send to the leader instance only.
    pub fn sendToLeader(self: *Self, payload: []const u8) SendError!void {
        for (self.instances.items) |*inst| {
            if (inst.is_leader) {
                if (self.fc_config.enabled) {
                    try self.applyFlowControl(inst, payload.len);
                }

                if (inst.node_id == self.local_node_id) {
                    const producer = inst.ipc_producer orelse
                        return error.ProducerNotInitialized;
                    try producer.write(constants.application_msg_type_id, payload);
                } else {
                    try self.sendToRemoteService(
                        inst.node_id,
                        inst.service_id,
                        payload,
                    );
                }
                return;
            }
        }
        return error.NoLeaderAvailable;
    }

    // ── Instance Management ───────────────────────────────────────────

    pub fn addInstance(self: *Self, instance: ServiceInstance) !void {
        try self.instances.append(self.allocator, instance);
    }

    pub fn removeInstance(self: *Self, service_id: i32) void {
        var i: usize = 0;
        while (i < self.instances.items.len) {
            if (self.instances.items[i].service_id == service_id) {
                const removed = self.instances.swapRemove(i);
                if (removed.ipc_producer) |producer| {
                    self.allocator.destroy(producer);
                }
                return;
            }
            i += 1;
        }
    }

    pub fn updateLeader(self: *Self, leader_service_id: i32) void {
        for (self.instances.items) |*inst| {
            inst.is_leader = (inst.service_id == leader_service_id);
        }
    }

    pub fn instanceCount(self: *const Self) usize {
        return self.instances.items.len;
    }

    pub fn findInstance(self: *const Self, service_id: i32) ?*ServiceInstance {
        for (self.instances.items) |*inst| {
            if (inst.service_id == service_id) return inst;
        }
        return null;
    }

    pub fn deinit(self: *Self) void {
        for (self.instances.items) |inst| {
            if (inst.ipc_producer) |producer| {
                self.allocator.destroy(producer);
            }
        }
        self.instances.deinit(self.allocator);
    }

    // ── Flow Control API ─────────────────────────────────────────────

    /// Returns the estimated remaining bytes in the target's buffer.
    /// For local instances: exact (from ring buffer via IpcProducer).
    /// For remote instances: advisory (from propagated counter in FC region).
    pub fn remainingBytes(self: *const Self, instance: *const ServiceInstance) usize {
        if (instance.fc_slot_id < 0) {
            // Local instance — read ring buffer directly.
            const producer = instance.ipc_producer orelse return 0;
            return producer.remainingCapacity();
        } else {
            // Remote instance — read from flow control counters region.
            return self.readFcCounter(instance);
        }
    }

    /// Returns the estimated remaining bytes in the broker's send ring buffer.
    pub fn sendBufferRemaining(self: *const Self) usize {
        const broker = self.broker_meta orelse return 0;
        const send_buf = broker.getSendBuffer();
        var send_rb = RingBuffer.init(
            @alignCast(send_buf),
            false,
            null,
            null,
        ) catch return 0;
        return send_rb.getCapacity() - send_rb.size();
    }

    /// Returns the total bytes pending in the outbound pipeline for a specific peer.
    /// Returns null if per-peer counters are disabled or the peer is not found.
    pub fn peerSendPending(self: *const Self, node_id: i16) ?u64 {
        const region = self.peer_send_counters orelse return null;
        const entry = region.findPeer(node_id) orelse return null;
        const ring_pending = @atomicLoad(u64, &entry.ring_bytes_pending, .acquire);
        const queue_pending = @atomicLoad(u64, &entry.queue_bytes_pending, .acquire);
        return ring_pending + queue_pending;
    }

    /// Returns true if the peer broker is currently connected.
    /// Returns null if per-peer counters are disabled or the peer is not found.
    pub fn isPeerConnected(self: *const Self, node_id: i16) ?bool {
        const region = self.peer_send_counters orelse return null;
        const entry = region.findPeer(node_id) orelse return null;
        return @atomicLoad(u8, &entry.connection_state, .acquire) == 1;
    }

    // ── Internal: Flow Control ───────────────────────────────────────

    /// Pre-send flow control check for all five paths.
    fn applyFlowControl(self: *Self, instance: *const ServiceInstance, payload_len: usize) SendError!void {
        // Path 1/3: Target service remaining capacity.
        const remaining = self.remainingBytes(instance);
        const required = ringCost(payload_len);
        const threshold = @max(required, self.fc_config.min_remaining_bytes);

        if (remaining < threshold) {
            try self.applyStrategy(remaining, required);
        }

        // Paths 2/4/5: Only apply to remote instances.
        if (instance.node_id != self.local_node_id) {
            const send_cost = ringCost(TcpFrameHeader.size + payload_len);

            // Path 5: Peer connectivity (cheapest — single byte read).
            if (self.fc_config.check_peer_connectivity) {
                if (self.isPeerConnected(instance.node_id)) |connected| {
                    if (!connected) return error.PeerDisconnected;
                }
            }

            // Path 4: Per-peer send congestion (two u64 reads).
            if (self.fc_config.per_peer_pending_threshold > 0) {
                if (self.peerSendPending(instance.node_id)) |pending| {
                    if (pending + send_cost > self.fc_config.per_peer_pending_threshold) {
                        return error.PeerCongested;
                    }
                }
            }

            // Path 2: Global send ring buffer remaining.
            const send_remaining = self.sendBufferRemaining();
            if (send_remaining < send_cost) {
                try self.applyStrategy(send_remaining, send_cost);
            }
        }
    }

    /// Apply the configured backpressure strategy.
    fn applyStrategy(self: *Self, remaining: usize, required: usize) SendError!void {
        switch (self.fc_config.strategy) {
            .drop => return error.BackPressure,
            .spin => try self.spinUntilCapacity(remaining, required),
        }
    }

    /// Spin-wait until capacity is available or timeout expires.
    fn spinUntilCapacity(self: *Self, initial_remaining: usize, required: usize) SendError!void {
        _ = initial_remaining;
        const timeout_ns: u64 = @as(u64, self.fc_config.spin_timeout_ms) * 1_000_000;
        const start = Clock.monotonicNanos();
        const deadline_ns: i64 = @intCast(timeout_ns);

        while (true) {
            // Re-read remaining capacity (may have changed).
            const send_remaining = self.sendBufferRemaining();
            if (send_remaining >= required) return;

            const elapsed = Clock.monotonicNanos() - start;
            if (elapsed >= deadline_ns) {
                return error.BackPressureTimeout;
            }

            std.atomic.spinLoopHint();
        }
    }

    /// Read the FC counter for a remote instance, validating generation.
    fn readFcCounter(self: *const Self, instance: *const ServiceInstance) usize {
        const region = self.fc_region orelse return 0;
        const slot_id: u32 = if (instance.fc_slot_id >= 0) @intCast(instance.fc_slot_id) else return 0;
        const entry = region.getEntry(slot_id) orelse return 0;

        // Validate generation atomically to detect stale slot references.
        if (entry.loadGeneration() != instance.fc_slot_generation) return 0;
        if (entry.loadState() != .allocated) return 0;

        return entry.loadRemainingBytes();
    }

    /// Calculate the ring buffer cost of a message (record header + alignment).
    fn ringCost(payload_len: usize) usize {
        // Ring buffer record: 8-byte header + payload, aligned to 8 bytes.
        return alignUp(8 + payload_len, 8);
    }

    fn alignUp(value: usize, alignment: usize) usize {
        return (value + alignment - 1) & ~(alignment - 1);
    }

    // ── Internal: Cross-Host Send ─────────────────────────────────────

    fn sendToRemoteService(
        self: *Self,
        target_node_id: i16,
        target_service_id: i32,
        payload: []const u8,
    ) SendError!void {
        const broker = self.broker_meta orelse return error.SendBufferFull;
        const send_buf = broker.getSendBuffer();

        // We need at least ring_buffer_alignment alignment for RingBuffer.init.
        // The send buffer from BrokerMetadataFile is part of an aligned mmap region.
        var send_rb = RingBuffer.init(
            @alignCast(send_buf),
            false,
            null,
            null,
        ) catch return error.SendBufferFull;

        // Write TcpFrameHeader + app payload — matching the wire format that
        // the sender event loop expects (it casts the ring buffer payload as
        // TcpFrameHeader to read target_node_id for routing).
        const total_len = TcpFrameHeader.size + payload.len;
        var claim = send_rb.tryClaim(constants.application_msg_type_id, total_len) orelse
            return error.SendBufferFull;

        const header: *TcpFrameHeader = @ptrCast(@alignCast(claim.buffer.ptr));
        header.* = .{
            .frame_length = @intCast(total_len),
            .flags = 0,
            .source_node_id = @intCast(self.local_node_id),
            .target_node_id = @intCast(target_node_id),
            .source_service_id = @intCast(self.local_service_id),
            .target_service_id = @intCast(target_service_id),
            .template_id = 0,
            .correlation_id = 0,
        };

        // Copy the application payload immediately after the TCP frame header.
        if (payload.len > 0) {
            @memcpy(claim.buffer[TcpFrameHeader.size..][0..payload.len], payload);
        }

        // Commit — makes the message visible to the broker's sender event loop.
        claim.commit();
    }
};
