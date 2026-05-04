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
const PeerEntry = memory.PeerEntry;
const fc_config_mod = @import("flow_control_config.zig");
const FlowControlConfig = fc_config_mod.FlowControlConfig;
const BackpressureStrategy = fc_config_mod.BackpressureStrategy;
const Clock = ringloom_common.platform.Clock;
const ServiceCounters = ringloom_common.monitoring.ServiceCounters;
const ServiceCounter = ringloom_common.monitoring.ServiceCounter;

const frame_parser = ringloom_common.protocol.frame_parser;
const TcpFrameHeader = frame_parser.TcpFrameHeader;
const BrokerMetadataFile = memory.BrokerMetadataFile;
const MessageHeader = message_header.MessageHeader;

pub const ServiceClient = struct {
    pub const TargetInstanceInfo = extern struct {
        target_service_id: i32,
        target_node_id: i16,
        is_leader: bool,
    };

    pub const LifecycleEventType = enum {
        available,
        unavailable,
    };

    pub const LifecycleEvent = struct {
        event_type: LifecycleEventType,
        service_name: []const u8,
        service_id: i32,
        node_id: i16,
        is_leader: bool,
    };

    pub const LifecycleHandler = *const fn (
        context: ?*anyopaque,
        event: LifecycleEvent,
    ) void;

    service_name: []const u8,
    instances: std.ArrayList(ServiceInstance),
    instances_lock: std.atomic.Mutex = .unlocked,
    allocator: std.mem.Allocator,
    balancer: load_balancer.ClientLoadBalancer,
    owns_service_name: bool = false,
    lifecycle_handler: ?LifecycleHandler = null,
    lifecycle_context: ?*anyopaque = null,

    /// IPC context — set during RingLoomEngine initialization.
    broker_meta: ?*BrokerMetadataFile,
    broker_send_ring_buffer: ?RingBuffer = null,
    local_node_id: i16,
    local_service_id: i32,
    service_counters: ?*ServiceCounters = null,

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

    pub const SendClaim = struct {
        claim: RingBuffer.Claim,
        payload: []u8,
        peer_counter_entry: ?*volatile PeerEntry = null,
        peer_ring_cost: u64 = 0,
        service_counters: ?*ServiceCounters = null,
        logical_payload_len: usize = 0,

        pub fn commit(self: *SendClaim) void {
            self.claim.commit();
            if (self.peer_counter_entry) |entry| {
                entry.addRingBytesPending(self.peer_ring_cost);
            }
            if (self.service_counters) |counters| {
                counters.increment(.messages_sent);
                counters.add(.bytes_sent, @intCast(self.logical_payload_len));
            }
        }

        pub fn abort(self: *SendClaim) void {
            self.claim.abort();
        }
    };

    pub fn init(
        allocator: std.mem.Allocator,
        service_name: []const u8,
        broker_meta: ?*BrokerMetadataFile,
        local_node_id: i16,
        local_service_id: i32,
        service_counters: ?*ServiceCounters,
    ) Self {
        return .{
            .service_name = service_name,
            .instances = .empty,
            .allocator = allocator,
            .balancer = .{ .round_robin = .{} },
            .broker_meta = broker_meta,
            .broker_send_ring_buffer = initBrokerSendRingBuffer(broker_meta),
            .local_node_id = local_node_id,
            .local_service_id = local_service_id,
            .service_counters = service_counters,
            .fc_region = initFlowControlRegion(broker_meta),
            .peer_send_counters = initPeerSendCountersRegion(broker_meta),
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
        service_counters: ?*ServiceCounters,
    ) Self {
        return .{
            .service_name = service_name,
            .instances = .empty,
            .allocator = allocator,
            .balancer = .{ .round_robin = .{} },
            .broker_meta = broker_meta,
            .broker_send_ring_buffer = initBrokerSendRingBuffer(broker_meta),
            .local_node_id = local_node_id,
            .local_service_id = local_service_id,
            .service_counters = service_counters,
            .fc_config = fc_config,
            .fc_region = fc_region,
            .peer_send_counters = peer_send_counters_region,
        };
    }

    /// Send a message to one instance of this service, selected by the
    /// load balancer.
    pub fn send(self: *Self, payload: []const u8) SendError!void {
        self.lockInstances();
        defer self.unlockInstances();

        const instance = self.balancer.next(self.instances.items) orelse
            {
                self.recordSendError(error.NoAvailableInstance);
                return error.NoAvailableInstance;
            };

        if (self.fc_config.enabled) {
            self.applyFlowControl(instance, payload.len) catch |err| {
                self.recordSendError(err);
                return err;
            };
        }

        if (instance.node_id == self.local_node_id) {
            // Same-host direct path.
            const producer = instance.ipc_producer orelse
                return error.ProducerNotInitialized;
            producer.write(constants.application_msg_type_id, payload) catch |err| {
                self.recordSendError(err);
                return err;
            };
            self.recordSend(payload.len);
        } else {
            // Cross-host routed path.
            self.sendToRemoteService(
                instance.node_id,
                instance.service_id,
                payload,
            ) catch |err| {
                self.recordSendError(err);
                return err;
            };
            self.recordSend(payload.len);
        }
    }

    /// Send a message to a specific instance (bypasses load balancer).
    pub fn sendTo(
        self: *Self,
        target_node_id: i16,
        target_service_id: i32,
        payload: []const u8,
    ) SendError!void {
        self.lockInstances();
        defer self.unlockInstances();

        const instance = self.findInstance(target_node_id, target_service_id) orelse
            {
                self.recordSendError(error.NoAvailableInstance);
                return error.NoAvailableInstance;
            };

        if (self.fc_config.enabled) {
            self.applyFlowControl(instance, payload.len) catch |err| {
                self.recordSendError(err);
                return err;
            };
        }

        if (instance.node_id == self.local_node_id) {
            const producer = instance.ipc_producer orelse
                return error.ProducerNotInitialized;
            producer.write(constants.application_msg_type_id, payload) catch |err| {
                self.recordSendError(err);
                return err;
            };
            self.recordSend(payload.len);
        } else {
            self.sendToRemoteService(
                instance.node_id,
                instance.service_id,
                payload,
            ) catch |err| {
                self.recordSendError(err);
                return err;
            };
            self.recordSend(payload.len);
        }
    }

    /// Send to the leader instance only.
    pub fn sendToLeader(self: *Self, payload: []const u8) SendError!void {
        self.lockInstances();
        defer self.unlockInstances();

        for (self.instances.items) |*inst| {
            if (inst.is_leader) {
                if (self.fc_config.enabled) {
                    self.applyFlowControl(inst, payload.len) catch |err| {
                        self.recordSendError(err);
                        return err;
                    };
                }

                if (inst.node_id == self.local_node_id) {
                    const producer = inst.ipc_producer orelse
                        return error.ProducerNotInitialized;
                    producer.write(constants.application_msg_type_id, payload) catch |err| {
                        self.recordSendError(err);
                        return err;
                    };
                    self.recordSend(payload.len);
                } else {
                    self.sendToRemoteService(
                        inst.node_id,
                        inst.service_id,
                        payload,
                    ) catch |err| {
                        self.recordSendError(err);
                        return err;
                    };
                    self.recordSend(payload.len);
                }
                return;
            }
        }
        self.recordSendError(error.NoAvailableInstance);
        return error.NoLeaderAvailable;
    }

    /// Claim writable ring-buffer memory for a zero-copy send on the
    /// load-balanced path.
    pub fn tryClaim(self: *Self, template_id: u16, payload_len: usize) SendError!SendClaim {
        self.lockInstances();
        defer self.unlockInstances();

        const instance = self.balancer.next(self.instances.items) orelse
            {
                self.recordSendError(error.NoAvailableInstance);
                return error.NoAvailableInstance;
            };

        if (self.fc_config.enabled) {
            self.applyFlowControl(instance, payload_len) catch |err| {
                self.recordSendError(err);
                return err;
            };
        }

        return self.tryClaimToInstance(instance, template_id, payload_len);
    }

    // ── Instance Management ───────────────────────────────────────────

    pub fn addInstance(self: *Self, instance: ServiceInstance) !void {
        self.lockInstances();
        defer self.unlockInstances();
        try self.instances.append(self.allocator, instance);
        self.emitInstanceAvailable(instance);
    }

    pub fn emitInstanceAvailable(self: *Self, instance: ServiceInstance) void {
        self.emitLifecycle(.{
            .event_type = .available,
            .service_name = instance.service_name,
            .service_id = instance.service_id,
            .node_id = instance.node_id,
            .is_leader = instance.is_leader,
        });
    }

    pub fn removeInstance(self: *Self, node_id: i16, service_id: i32) void {
        self.lockInstances();
        defer self.unlockInstances();
        var i: usize = 0;
        while (i < self.instances.items.len) {
            if (self.instances.items[i].service_id == service_id and
                self.instances.items[i].node_id == node_id)
            {
                const removed = self.instances.swapRemove(i);
                self.emitLifecycle(.{
                    .event_type = .unavailable,
                    .service_name = removed.service_name,
                    .service_id = removed.service_id,
                    .node_id = removed.node_id,
                    .is_leader = removed.is_leader,
                });
                if (removed.ipc_producer) |producer| {
                    self.allocator.destroy(producer);
                }
                return;
            }
            i += 1;
        }
    }

    pub fn updateLeader(self: *Self, leader_service_id: i32) void {
        self.lockInstances();
        defer self.unlockInstances();
        for (self.instances.items) |*inst| {
            const was_leader = inst.is_leader;
            inst.is_leader = (inst.service_id == leader_service_id);
            if (was_leader != inst.is_leader) {
                self.emitLifecycle(.{
                    .event_type = .available,
                    .service_name = inst.service_name,
                    .service_id = inst.service_id,
                    .node_id = inst.node_id,
                    .is_leader = inst.is_leader,
                });
            }
        }
    }

    pub fn instanceCount(self: *const Self) usize {
        self.lockInstances();
        defer self.unlockInstances();
        return self.instances.items.len;
    }

    pub fn copyTargetInstances(self: *const Self, out: []TargetInstanceInfo) usize {
        self.lockInstances();
        defer self.unlockInstances();
        const copy_len = @min(out.len, self.instances.items.len);
        for (out[0..copy_len], self.instances.items[0..copy_len]) |*slot, inst| {
            slot.* = .{
                .target_service_id = inst.service_id,
                .target_node_id = inst.node_id,
                .is_leader = inst.is_leader,
            };
        }
        return self.instances.items.len;
    }

    pub fn findInstance(self: *const Self, node_id: i16, service_id: i32) ?*ServiceInstance {
        for (self.instances.items) |*inst| {
            if (inst.service_id == service_id and inst.node_id == node_id) return inst;
        }
        return null;
    }

    pub fn setLifecycleHandler(
        self: *Self,
        handler: ?LifecycleHandler,
        context: ?*anyopaque,
    ) void {
        self.lifecycle_handler = handler;
        self.lifecycle_context = if (handler == null) null else context;
    }

    pub fn deinit(self: *Self) void {
        for (self.instances.items) |inst| {
            if (inst.ipc_producer) |producer| {
                self.allocator.destroy(producer);
            }
        }
        self.instances.deinit(self.allocator);
        if (self.owns_service_name) {
            self.allocator.free(self.service_name);
        }
    }

    pub fn emitLifecycle(self: *Self, event: LifecycleEvent) void {
        if (self.lifecycle_handler) |handler| {
            handler(self.lifecycle_context, event);
        }
    }

    pub fn lockInstances(self: *const Self) void {
        const lock = @constCast(&self.instances_lock);
        while (!lock.tryLock()) {
            std.atomic.spinLoopHint();
        }
    }

    pub fn unlockInstances(self: *const Self) void {
        @constCast(&self.instances_lock).unlock();
    }

    // ── Flow Control API ─────────────────────────────────────────────

    /// Returns the estimated remaining bytes in the target's buffer.
    /// For local instances: exact (from ring buffer via IpcProducer).
    /// For remote instances: advisory (from propagated counter in FC region).
    pub fn remainingBytes(self: *const Self, instance: *const ServiceInstance) ?usize {
        if (instance.node_id == self.local_node_id) {
            // Local instance — read ring buffer directly.
            const producer = instance.ipc_producer orelse return 0;
            return producer.remainingCapacity();
        }

        // Remote instance — read from flow control counters region if assigned.
        return self.readFcCounter(instance);
    }

    /// Returns the estimated remaining bytes in the broker's send ring buffer.
    pub fn sendBufferRemaining(self: *const Self) usize {
        if (self.broker_send_ring_buffer) |send_rb| {
            var ring = send_rb;
            return ring.getCapacity() - ring.size();
        }
        return 0;
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
        const required = conservativeRingCost(payload_len);
        if (self.remainingBytes(instance)) |remaining| {
            const threshold = @max(required, self.fc_config.min_remaining_bytes);

            if (remaining < threshold) {
                try self.applyStrategy(.target_buffer, remaining, required, instance);
            }
        }

        // Paths 2/4/5: Only apply to remote instances.
        if (instance.node_id != self.local_node_id) {
            const send_cost = conservativeRingCost(TcpFrameHeader.size + payload_len);

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
                        try self.applyStrategy(.peer_congestion, 0, send_cost, instance);
                    }
                }
            }

            // Path 2: Global send ring buffer remaining.
            const send_remaining = self.sendBufferRemaining();
            if (send_remaining < send_cost) {
                try self.applyStrategy(.send_buffer, send_remaining, send_cost, instance);
            }
        }
    }

    const BackpressurePath = enum {
        target_buffer,
        send_buffer,
        peer_congestion,
    };

    /// Apply the configured backpressure strategy.
    fn applyStrategy(
        self: *Self,
        path: BackpressurePath,
        remaining: usize,
        required: usize,
        instance: *const ServiceInstance,
    ) SendError!void {
        switch (self.fc_config.strategy) {
            .drop => switch (path) {
                .peer_congestion => return error.PeerCongested,
                else => return error.BackPressure,
            },
            .spin => try self.spinUntilCapacity(path, remaining, required, instance),
        }
    }

    /// Spin-wait until capacity is available or timeout expires.
    fn spinUntilCapacity(
        self: *Self,
        path: BackpressurePath,
        initial_remaining: usize,
        required: usize,
        instance: *const ServiceInstance,
    ) SendError!void {
        _ = initial_remaining;
        const timeout_ns: u64 = @as(u64, self.fc_config.spin_timeout_ms) * 1_000_000;
        const start = Clock.monotonicNanos();
        const deadline_ns: i64 = @intCast(timeout_ns);

        while (true) {
            if (self.pathHasCapacity(path, instance, required)) return;

            const elapsed = Clock.monotonicNanos() - start;
            if (elapsed >= deadline_ns) {
                return error.BackPressureTimeout;
            }

            std.atomic.spinLoopHint();
        }
    }

    fn pathHasCapacity(
        self: *Self,
        path: BackpressurePath,
        instance: *const ServiceInstance,
        required: usize,
    ) bool {
        return switch (path) {
            .target_buffer => if (self.remainingBytes(instance)) |remaining|
                remaining >= @max(required, self.fc_config.min_remaining_bytes)
            else
                true,
            .send_buffer => self.sendBufferRemaining() >= required,
            .peer_congestion => blk: {
                const threshold = self.fc_config.per_peer_pending_threshold;
                if (threshold == 0) break :blk true;
                const pending = self.peerSendPending(instance.node_id) orelse break :blk true;
                break :blk pending + required <= threshold;
            },
        };
    }

    /// Read the FC counter for a remote instance, validating generation.
    fn readFcCounter(self: *const Self, instance: *const ServiceInstance) ?usize {
        const region = self.fc_region orelse return null;
        const slot_id: u32 = if (instance.fc_slot_id >= 0) @intCast(instance.fc_slot_id) else return null;
        const entry = region.getEntry(slot_id) orelse return null;

        // Validate generation atomically to detect stale slot references.
        if (entry.loadState() != .allocated) return null;
        if (entry.loadGeneration() != instance.fc_slot_generation) return null;
        if (entry.capacity == 0) return null;

        return entry.loadRemainingBytes();
    }

    /// Calculate the ring buffer cost of a message (record header + alignment).
    fn ringCost(payload_len: usize) usize {
        // Ring buffer record: 8-byte header + payload, aligned to 8 bytes.
        return alignUp(8 + payload_len, 8);
    }

    fn conservativeRingCost(payload_len: usize) usize {
        return ringCost(payload_len) * 2;
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
        var send_claim = try self.tryClaimRemoteService(
            target_node_id,
            target_service_id,
            0,
            payload.len,
        );

        // Copy the application payload immediately after the TCP frame header.
        if (payload.len > 0) {
            @memcpy(send_claim.payload, payload);
        }

        // Commit — makes the message visible to the broker's sender event loop.
        send_claim.commit();
    }

    fn tryClaimToInstance(
        self: *Self,
        instance: *ServiceInstance,
        template_id: u16,
        payload_len: usize,
    ) SendError!SendClaim {
        if (instance.node_id == self.local_node_id) {
            const producer = instance.ipc_producer orelse
                return error.ProducerNotInitialized;
            const msg_type_id: i32 = if (template_id == 0)
                constants.application_msg_type_id
            else
                @intCast(template_id);

            if (payload_len > producer.ring_buffer.maxMessageLength()) {
                return error.MessageTooLong;
            }

            const claim = producer.tryClaim(msg_type_id, payload_len) orelse
                {
                    self.recordSendError(error.SendBufferFull);
                    return error.SendBufferFull;
                };
            return .{
                .claim = claim,
                .payload = claim.buffer,
                .service_counters = self.service_counters,
                .logical_payload_len = payload_len,
            };
        }

        return self.tryClaimRemoteService(
            instance.node_id,
            instance.service_id,
            template_id,
            payload_len,
        );
    }

    fn tryClaimRemoteService(
        self: *Self,
        target_node_id: i16,
        target_service_id: i32,
        template_id: u16,
        payload_len: usize,
    ) SendError!SendClaim {
        const send_rb = self.brokerSendRingBuffer() orelse return error.SendBufferFull;
        const total_len = TcpFrameHeader.size + payload_len;

        if (total_len > send_rb.maxMessageLength()) {
            return error.MessageTooLong;
        }

        var claim = send_rb.tryClaim(constants.application_msg_type_id, total_len) orelse
            {
                self.recordSendError(error.SendBufferFull);
                return error.SendBufferFull;
            };

        const header: *TcpFrameHeader = @ptrCast(@alignCast(claim.buffer.ptr));
        header.* = .{
            .frame_length = @intCast(total_len),
            .flags = 0,
            .source_node_id = @intCast(self.local_node_id),
            .target_node_id = @intCast(target_node_id),
            .source_service_id = @intCast(self.local_service_id),
            .target_service_id = @intCast(target_service_id),
            .template_id = template_id,
            .correlation_id = 0,
        };

        return .{
            .claim = claim,
            .payload = claim.buffer[TcpFrameHeader.size..][0..payload_len],
            .peer_counter_entry = self.peerCounterEntry(target_node_id),
            .peer_ring_cost = @intCast(ringCost(total_len)),
            .service_counters = self.service_counters,
            .logical_payload_len = payload_len,
        };
    }

    fn recordSend(self: *const Self, payload_len: usize) void {
        const counters = self.service_counters orelse return;
        counters.increment(.messages_sent);
        counters.add(.bytes_sent, @intCast(payload_len));
    }

    fn recordSendError(self: *const Self, err: anyerror) void {
        const counters = self.service_counters orelse return;
        switch (err) {
            error.SendBufferFull, error.BufferFull => counters.increment(.send_buffer_full),
            error.BackPressure => counters.increment(.backpressure),
            error.BackPressureTimeout => counters.increment(.backpressure_timeouts),
            error.PeerCongested => counters.increment(.peer_congestion),
            error.PeerDisconnected => counters.increment(.peer_disconnected),
            error.NoAvailableInstance => counters.increment(.no_available_instance),
            else => {},
        }
    }

    fn brokerSendRingBuffer(self: *Self) ?*RingBuffer {
        if (self.broker_send_ring_buffer == null) return null;
        return &self.broker_send_ring_buffer.?;
    }

    fn initBrokerSendRingBuffer(broker_meta: ?*BrokerMetadataFile) ?RingBuffer {
        const broker = broker_meta orelse return null;
        return RingBuffer.init(
            @alignCast(broker.getSendBuffer()),
            false,
            null,
            null,
        ) catch null;
    }

    fn initFlowControlRegion(broker_meta: ?*BrokerMetadataFile) ?FlowControlRegion {
        const broker = broker_meta orelse return null;
        return broker.getFlowControlRegion();
    }

    fn initPeerSendCountersRegion(broker_meta: ?*BrokerMetadataFile) ?PeerSendCountersRegion {
        const broker = broker_meta orelse return null;
        return broker.getPeerSendCountersRegion();
    }

    fn peerCounterEntry(self: *const Self, node_id: i16) ?*volatile PeerEntry {
        const region = self.peer_send_counters orelse return null;
        return region.findPeer(node_id);
    }
};
