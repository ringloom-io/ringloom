//! ServiceClient — client-side proxy for sending messages to a named service.
//!
//! Encapsulates service discovery, instance tracking, and load balancing.
//! Automatically routes via same-host direct path or cross-host broker-routed
//! path depending on the target instance's node_id.

const std = @import("std");
const ringloom_common = @import("ringloom_common");
const IpcProducer = @import("ipc/ipc_producer.zig").IpcProducer;
const ServiceInstance = @import("service_instance.zig").ServiceInstance;
const ServiceAeronRuntime = @import("aeron_runtime.zig").ServiceAeronRuntime;
const load_balancer = @import("load_balancer.zig");
const message_header = ringloom_common.message.message_header;
const data_header = ringloom_common.message.data_header;
const ringloom_aeron = @import("ringloom_aeron");
const memory = ringloom_common.memory;
const constants = ringloom_common.memory.constants;
const RingBuffer = ringloom_common.concurrent.ring_buffer.RingBuffer;
const FlowControlRegion = memory.FlowControlRegion;
const FlowControlEntry = memory.FlowControlEntry;
const fc_config_mod = @import("flow_control_config.zig");
const FlowControlConfig = fc_config_mod.FlowControlConfig;
const BackpressureStrategy = fc_config_mod.BackpressureStrategy;
const Clock = ringloom_common.platform.Clock;
const ServiceCounters = ringloom_common.monitoring.ServiceCounters;
const ServiceCounter = ringloom_common.monitoring.ServiceCounter;

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

    pub const RemotePublicationStatus = enum(u8) {
        unknown,
        claimed,
        not_connected,
        back_pressured,
        admin_action,
        closed,
        max_position_exceeded,
        failed,
    };

    pub const RemotePublicationHealth = struct {
        configured: bool,
        direct_peer_count: usize = 0,
        last_status: RemotePublicationStatus,
        last_status_ns: i64,
    };

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
    aeron_runtime: ?*ServiceAeronRuntime = null,
    local_node_id: i16,
    local_service_id: i32,
    service_counters: ?*ServiceCounters = null,

    /// Flow control configuration.
    fc_config: FlowControlConfig = .{},

    /// Cached pointer to the flow control counters region (null if FC disabled).
    fc_region: ?FlowControlRegion = null,

    /// Last observed direct Aeron remote publication state.
    remote_publication_status: RemotePublicationStatus = .unknown,
    remote_publication_status_ns: i64 = 0,
    remote_frame_scratch: ?[]u8 = null,

    const Self = @This();

    pub const SendError = error{
        NoAvailableInstance,
        ProducerNotInitialized,
        SendBufferFull,
        RemoteTransportUnavailable,
        NoLeaderAvailable,
        BackPressure,
        BackPressureTimeout,
        OutOfMemory,
    } || RingBuffer.WriteError;

    const ClaimStorage = union(enum) {
        local: RingBuffer.Claim,
        remote: ringloom_aeron.BufferClaim,
    };

    pub const SendClaim = struct {
        claim: ClaimStorage,
        payload: []u8,
        service_counters: ?*ServiceCounters = null,
        logical_payload_len: usize = 0,

        pub fn commit(self: *SendClaim) void {
            switch (self.claim) {
                .local => |*claim| claim.commit(),
                .remote => |*claim| claim.commit() catch {
                    if (self.service_counters) |counters| {
                        counters.increment(.remote_transport_unavailable);
                    }
                    return;
                },
            }
            if (self.service_counters) |counters| {
                counters.increment(.messages_sent);
                counters.add(.bytes_sent, @intCast(self.logical_payload_len));
            }
        }

        pub fn abort(self: *SendClaim) void {
            switch (self.claim) {
                .local => |*claim| claim.abort(),
                .remote => |*claim| claim.abort() catch {},
            }
        }
    };

    pub fn init(
        allocator: std.mem.Allocator,
        service_name: []const u8,
        broker_meta: ?*BrokerMetadataFile,
        aeron_runtime: ?*ServiceAeronRuntime,
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
            .aeron_runtime = aeron_runtime,
            .local_node_id = local_node_id,
            .local_service_id = local_service_id,
            .service_counters = service_counters,
            .fc_region = initFlowControlRegion(broker_meta),
        };
    }

    /// Initialize with flow control configuration.
    pub fn initWithFlowControl(
        allocator: std.mem.Allocator,
        service_name: []const u8,
        broker_meta: ?*BrokerMetadataFile,
        aeron_runtime: ?*ServiceAeronRuntime,
        local_node_id: i16,
        local_service_id: i32,
        fc_config: FlowControlConfig,
        fc_region: ?FlowControlRegion,
        service_counters: ?*ServiceCounters,
    ) Self {
        return .{
            .service_name = service_name,
            .instances = .empty,
            .allocator = allocator,
            .balancer = .{ .round_robin = .{} },
            .broker_meta = broker_meta,
            .aeron_runtime = aeron_runtime,
            .local_node_id = local_node_id,
            .local_service_id = local_service_id,
            .service_counters = service_counters,
            .fc_config = fc_config,
            .fc_region = fc_region,
        };
    }

    /// Send a message to one instance of this service, selected by the
    /// load balancer.
    pub fn send(self: *Self, payload: []const u8) SendError!void {
        try self.sendMessage(0, payload);
    }

    pub fn sendMessage(self: *Self, template_id: u16, payload: []const u8) SendError!void {
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

        try self.sendToInstance(instance, template_id, 0, payload, false);
    }

    pub fn sendMessageRequest(
        self: *Self,
        template_id: u16,
        correlation_id: i64,
        payload: []const u8,
    ) SendError!void {
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

        try self.sendToInstance(instance, template_id, correlation_id, payload, true);
    }

    /// Send a message to a specific instance (bypasses load balancer).
    pub fn sendTo(
        self: *Self,
        target_node_id: i16,
        target_service_id: i32,
        payload: []const u8,
    ) SendError!void {
        try self.sendToMessage(target_node_id, target_service_id, 0, payload);
    }

    pub fn sendToMessage(
        self: *Self,
        target_node_id: i16,
        target_service_id: i32,
        template_id: u16,
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

        try self.sendToInstance(instance, template_id, 0, payload, false);
    }

    pub fn sendToMessageRequest(
        self: *Self,
        target_node_id: i16,
        target_service_id: i32,
        template_id: u16,
        correlation_id: i64,
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

        try self.sendToInstance(instance, template_id, correlation_id, payload, true);
    }

    /// Send to the leader instance only.
    pub fn sendToLeader(self: *Self, payload: []const u8) SendError!void {
        try self.sendToLeaderMessage(0, payload);
    }

    pub fn sendToLeaderMessage(self: *Self, template_id: u16, payload: []const u8) SendError!void {
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

                try self.sendToInstance(inst, template_id, 0, payload, false);
                return;
            }
        }
        self.recordSendError(error.NoAvailableInstance);
        return error.NoLeaderAvailable;
    }

    pub fn sendToLeaderMessageRequest(
        self: *Self,
        template_id: u16,
        correlation_id: i64,
        payload: []const u8,
    ) SendError!void {
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

                try self.sendToInstance(inst, template_id, correlation_id, payload, true);
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

        return self.tryClaimToInstance(instance, template_id, 0, payload_len, false);
    }

    pub fn tryClaimRequest(
        self: *Self,
        template_id: u16,
        correlation_id: i64,
        payload_len: usize,
    ) SendError!SendClaim {
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

        return self.tryClaimToInstance(instance, template_id, correlation_id, payload_len, true);
    }

    pub fn tryClaimTo(
        self: *Self,
        target_node_id: i16,
        target_service_id: i32,
        template_id: u16,
        payload_len: usize,
    ) SendError!SendClaim {
        return self.tryClaimToWithCorrelation(target_node_id, target_service_id, template_id, 0, payload_len, false);
    }

    pub fn tryClaimToRequest(
        self: *Self,
        target_node_id: i16,
        target_service_id: i32,
        template_id: u16,
        correlation_id: i64,
        payload_len: usize,
    ) SendError!SendClaim {
        return self.tryClaimToWithCorrelation(target_node_id, target_service_id, template_id, correlation_id, payload_len, true);
    }

    pub fn tryClaimToLeader(self: *Self, template_id: u16, payload_len: usize) SendError!SendClaim {
        return self.tryClaimToLeaderWithCorrelation(template_id, 0, payload_len, false);
    }

    pub fn tryClaimToLeaderRequest(
        self: *Self,
        template_id: u16,
        correlation_id: i64,
        payload_len: usize,
    ) SendError!SendClaim {
        return self.tryClaimToLeaderWithCorrelation(template_id, correlation_id, payload_len, true);
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
        if (self.remote_frame_scratch) |scratch| {
            self.allocator.free(scratch);
        }
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

    /// Retained for legacy callers; v2 remote sends use Aeron publication flow control.
    pub fn sendBufferRemaining(self: *const Self) usize {
        _ = self;
        return 0;
    }

    /// Progress service-side Aeron client work for single-threaded tools that
    /// are about to send bursts without otherwise touching the publication.
    pub fn pollTransport(self: *Self) void {
        const runtime = self.aeron_runtime orelse return;
        self.driveRemoteRuntime(runtime) catch {};
    }

    /// Returns the last observed state of the remote Aeron publication path.
    pub fn remotePublicationHealth(self: *const Self) RemotePublicationHealth {
        const runtime = self.aeron_runtime orelse {
            return .{
                .configured = false,
                .last_status = .unknown,
                .last_status_ns = 0,
            };
        };
        return .{
            .configured = true,
            .direct_peer_count = runtime.directPeerCount(),
            .last_status = self.remote_publication_status,
            .last_status_ns = self.remote_publication_status_ns,
        };
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

        // Remote instances use the remote service capacity advisory counter here.
        // Aeron remote publication pressure is authoritative only when tryClaim()
        // returns a publication status in tryClaimRemoteService().
        if (instance.node_id != self.local_node_id) {
            if (self.aeron_runtime == null)
                return error.RemoteTransportUnavailable;
        }
    }

    const BackpressurePath = enum {
        target_buffer,
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
            .drop => return error.BackPressure,
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

    fn tryClaimToWithCorrelation(
        self: *Self,
        target_node_id: i16,
        target_service_id: i32,
        template_id: u16,
        correlation_id: i64,
        payload_len: usize,
        envelope_local: bool,
    ) SendError!SendClaim {
        self.lockInstances();
        defer self.unlockInstances();

        const instance = self.findInstance(target_node_id, target_service_id) orelse
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

        return self.tryClaimToInstance(instance, template_id, correlation_id, payload_len, envelope_local);
    }

    fn tryClaimToLeaderWithCorrelation(
        self: *Self,
        template_id: u16,
        correlation_id: i64,
        payload_len: usize,
        envelope_local: bool,
    ) SendError!SendClaim {
        self.lockInstances();
        defer self.unlockInstances();

        for (self.instances.items) |*inst| {
            if (inst.is_leader) {
                if (self.fc_config.enabled) {
                    self.applyFlowControl(inst, payload_len) catch |err| {
                        self.recordSendError(err);
                        return err;
                    };
                }

                return self.tryClaimToInstance(inst, template_id, correlation_id, payload_len, envelope_local);
            }
        }
        self.recordSendError(error.NoAvailableInstance);
        return error.NoLeaderAvailable;
    }

    fn sendToInstance(
        self: *Self,
        instance: *ServiceInstance,
        template_id: u16,
        correlation_id: i64,
        payload: []const u8,
        envelope_local: bool,
    ) SendError!void {
        if (instance.node_id != self.local_node_id) {
            return self.sendToRemoteService(
                instance.node_id,
                instance.service_id,
                template_id,
                correlation_id,
                payload,
            );
        }

        var send_claim = try self.tryClaimToInstance(
            instance,
            template_id,
            correlation_id,
            payload.len,
            envelope_local,
        );
        if (payload.len > 0) {
            @memcpy(send_claim.payload, payload);
        }
        send_claim.commit();
    }

    fn sendToRemoteService(
        self: *Self,
        target_node_id: i16,
        target_service_id: i32,
        template_id: u16,
        correlation_id: i64,
        payload: []const u8,
    ) SendError!void {
        const runtime = self.aeron_runtime orelse {
            self.recordSendError(error.RemoteTransportUnavailable);
            return error.RemoteTransportUnavailable;
        };
        const total_len = data_header.RingLoomDataHeader.encoded_length + payload.len;
        if (payload.len > std.math.maxInt(u32)) return error.MessageTooLong;

        const fields = self.remoteDataFields(
            target_node_id,
            target_service_id,
            template_id,
            correlation_id,
            payload.len,
        ) catch |err| return mapDataHeaderError(err);

        if (total_len <= runtime.maxPayloadLengthForNode(target_node_id)) {
            var claim = try self.claimRemoteDirect(runtime, target_node_id, total_len);
            errdefer claim.abort() catch {};
            const claimed = claim.bytes();
            data_header.encodeHeader(
                claimed[0..data_header.RingLoomDataHeader.encoded_length],
                fields,
                payload.len,
            ) catch |err| return mapDataHeaderError(err);
            if (payload.len > 0) {
                @memcpy(claimed[data_header.RingLoomDataHeader.encoded_length..][0..payload.len], payload);
            }
            claim.commit() catch {
                self.recordSendError(error.RemoteTransportUnavailable);
                return error.RemoteTransportUnavailable;
            };
            self.recordSend(payload.len);
            return;
        }

        const frame = try self.ensureRemoteFrameScratch(total_len);
        data_header.encodeHeader(
            frame[0..data_header.RingLoomDataHeader.encoded_length],
            fields,
            payload.len,
        ) catch |err| return mapDataHeaderError(err);

        if (payload.len > 0) {
            @memcpy(frame[data_header.RingLoomDataHeader.encoded_length..], payload);
        }

        try self.offerRemoteDirect(runtime, target_node_id, frame);
        self.recordSend(payload.len);
    }

    fn ensureRemoteFrameScratch(self: *Self, total_len: usize) SendError![]u8 {
        if (self.remote_frame_scratch) |scratch| {
            if (scratch.len >= total_len) return scratch[0..total_len];
            const resized = try self.allocator.realloc(scratch, total_len);
            self.remote_frame_scratch = resized;
            return resized;
        }

        const scratch = try self.allocator.alloc(u8, total_len);
        self.remote_frame_scratch = scratch;
        return scratch;
    }

    fn tryClaimToInstance(
        self: *Self,
        instance: *ServiceInstance,
        template_id: u16,
        correlation_id: i64,
        payload_len: usize,
        envelope_local: bool,
    ) SendError!SendClaim {
        if (instance.node_id == self.local_node_id) {
            const producer = instance.ipc_producer orelse
                return error.ProducerNotInitialized;
            if (envelope_local) {
                return self.tryClaimLocalEnvelope(
                    producer,
                    instance,
                    template_id,
                    correlation_id,
                    payload_len,
                );
            }

            const msg_type_id = message_header.msgTypeFromTemplateId(template_id);

            if (payload_len > producer.ring_buffer.maxMessageLength()) {
                return error.MessageTooLong;
            }

            const claim = producer.tryClaim(msg_type_id, payload_len) orelse
                {
                    self.recordSendError(error.SendBufferFull);
                    return error.SendBufferFull;
                };
            return .{
                .claim = .{ .local = claim },
                .payload = claim.buffer,
                .service_counters = self.service_counters,
                .logical_payload_len = payload_len,
            };
        }

        return self.tryClaimRemoteService(
            instance.node_id,
            instance.service_id,
            template_id,
            correlation_id,
            payload_len,
        );
    }

    fn tryClaimLocalEnvelope(
        self: *Self,
        producer: *IpcProducer,
        instance: *ServiceInstance,
        template_id: u16,
        correlation_id: i64,
        payload_len: usize,
    ) SendError!SendClaim {
        if (payload_len > std.math.maxInt(i32)) return error.MessageTooLong;
        const total_len = MessageHeader.encoded_length + payload_len;
        if (total_len > producer.ring_buffer.maxMessageLength()) {
            return error.MessageTooLong;
        }

        const claim = producer.tryClaim(constants.message_envelope_msg_type_id, total_len) orelse
            {
                self.recordSendError(error.SendBufferFull);
                return error.SendBufferFull;
            };
        MessageHeader.encode(claim.buffer[0..MessageHeader.encoded_length], .{
            .source_node_id = self.local_node_id,
            .source_service_id = @intCast(self.local_service_id),
            .target_node_id = instance.node_id,
            .target_service_id = @intCast(instance.service_id),
            .template_id = template_id,
            .correlation_id = correlation_id,
            .flags = constants.flag_unfragmented,
            .payload_length = @intCast(payload_len),
        });

        return .{
            .claim = .{ .local = claim },
            .payload = claim.buffer[MessageHeader.encoded_length..][0..payload_len],
            .service_counters = self.service_counters,
            .logical_payload_len = payload_len,
        };
    }

    fn tryClaimRemoteService(
        self: *Self,
        target_node_id: i16,
        target_service_id: i32,
        template_id: u16,
        correlation_id: i64,
        payload_len: usize,
    ) SendError!SendClaim {
        const runtime = self.aeron_runtime orelse {
            self.recordSendError(error.RemoteTransportUnavailable);
            return error.RemoteTransportUnavailable;
        };
        const total_len = data_header.RingLoomDataHeader.encoded_length + payload_len;

        if (payload_len > std.math.maxInt(u32)) {
            return error.MessageTooLong;
        }

        var claim = try self.claimRemoteDirect(runtime, target_node_id, total_len);

        const claimed = claim.bytes();
        const fields = self.remoteDataFields(
            target_node_id,
            target_service_id,
            template_id,
            correlation_id,
            payload_len,
        ) catch |err| {
            claim.abort() catch {};
            return mapDataHeaderError(err);
        };

        data_header.encodeHeader(
            claimed[0..data_header.RingLoomDataHeader.encoded_length],
            fields,
            payload_len,
        ) catch |err| {
            claim.abort() catch {};
            return mapDataHeaderError(err);
        };

        return .{
            .claim = .{ .remote = claim },
            .payload = claimed[data_header.RingLoomDataHeader.encoded_length..][0..payload_len],
            .service_counters = self.service_counters,
            .logical_payload_len = payload_len,
        };
    }

    fn claimRemoteDirect(
        self: *Self,
        runtime: *ServiceAeronRuntime,
        target_node_id: i16,
        total_len: usize,
    ) SendError!ringloom_aeron.BufferClaim {
        const timeout_ns: i64 = @as(i64, @intCast(self.fc_config.spin_timeout_ms)) * std.time.ns_per_ms;
        const start = Clock.monotonicNanos();

        while (true) {
            try self.driveRemoteRuntime(runtime);
            switch (runtime.tryClaimToNode(target_node_id, total_len)) {
                .claim => |claim| {
                    self.recordRemotePublicationStatus(.claimed);
                    return claim;
                },
                .not_connected => {
                    self.recordRemotePublicationStatus(.not_connected);
                    if (self.shouldRetryAeronClaim(start, timeout_ns)) {
                        std.atomic.spinLoopHint();
                        continue;
                    }
                    self.recordSendError(error.RemoteTransportUnavailable);
                    return error.RemoteTransportUnavailable;
                },
                .back_pressured => {
                    self.recordRemotePublicationStatus(.back_pressured);
                    if (!self.shouldRetryAeronClaim(start, timeout_ns)) {
                        const err = self.aeronBackpressureError();
                        self.recordSendError(err);
                        return err;
                    }
                },
                .admin_action => {
                    self.recordRemotePublicationStatus(.admin_action);
                    if (!self.shouldRetryAeronClaim(start, timeout_ns)) {
                        const err = self.aeronBackpressureError();
                        self.recordSendError(err);
                        return err;
                    }
                },
                .closed => {
                    self.recordRemotePublicationStatus(.closed);
                    self.recordSendError(error.RemoteTransportUnavailable);
                    return error.RemoteTransportUnavailable;
                },
                .max_position_exceeded => {
                    self.recordRemotePublicationStatus(.max_position_exceeded);
                    self.recordSendError(error.RemoteTransportUnavailable);
                    return error.RemoteTransportUnavailable;
                },
                .failed => {
                    self.recordRemotePublicationStatus(.failed);
                    self.recordSendError(error.RemoteTransportUnavailable);
                    return error.RemoteTransportUnavailable;
                },
            }
            std.atomic.spinLoopHint();
        }
    }

    fn shouldRetryAeronClaim(self: *const Self, start_ns: i64, timeout_ns: i64) bool {
        if (self.fc_config.strategy != .spin) return false;
        if (timeout_ns <= 0) return false;
        return Clock.monotonicNanos() - start_ns < timeout_ns;
    }

    fn aeronBackpressureError(self: *const Self) SendError {
        return if (self.fc_config.strategy == .spin) error.BackPressureTimeout else error.BackPressure;
    }

    fn offerRemoteDirect(
        self: *Self,
        runtime: *ServiceAeronRuntime,
        target_node_id: i16,
        frame: []const u8,
    ) SendError!void {
        const timeout_ns: i64 = @as(i64, @intCast(self.fc_config.spin_timeout_ms)) * std.time.ns_per_ms;
        const start = Clock.monotonicNanos();

        while (true) {
            try self.driveRemoteRuntime(runtime);
            switch (runtime.offerToNode(target_node_id, frame)) {
                .position => {
                    self.recordRemotePublicationStatus(.claimed);
                    return;
                },
                .not_connected => {
                    self.recordRemotePublicationStatus(.not_connected);
                    if (self.shouldRetryAeronClaim(start, timeout_ns)) {
                        std.atomic.spinLoopHint();
                        continue;
                    }
                    self.recordSendError(error.RemoteTransportUnavailable);
                    return error.RemoteTransportUnavailable;
                },
                .back_pressured => {
                    self.recordRemotePublicationStatus(.back_pressured);
                    if (!self.shouldRetryAeronClaim(start, timeout_ns)) {
                        const err = self.aeronBackpressureError();
                        self.recordSendError(err);
                        return err;
                    }
                },
                .admin_action => {
                    self.recordRemotePublicationStatus(.admin_action);
                    if (!self.shouldRetryAeronClaim(start, timeout_ns)) {
                        const err = self.aeronBackpressureError();
                        self.recordSendError(err);
                        return err;
                    }
                },
                .closed => {
                    self.recordRemotePublicationStatus(.closed);
                    self.recordSendError(error.RemoteTransportUnavailable);
                    return error.RemoteTransportUnavailable;
                },
                .max_position_exceeded => {
                    self.recordRemotePublicationStatus(.max_position_exceeded);
                    self.recordSendError(error.RemoteTransportUnavailable);
                    return error.RemoteTransportUnavailable;
                },
                .failed => {
                    self.recordRemotePublicationStatus(.failed);
                    self.recordSendError(error.RemoteTransportUnavailable);
                    return error.RemoteTransportUnavailable;
                },
            }
            std.atomic.spinLoopHint();
        }
    }

    fn driveRemoteRuntime(self: *Self, runtime: *ServiceAeronRuntime) SendError!void {
        _ = runtime.doWork() catch {
            self.recordRemotePublicationStatus(.failed);
            self.recordSendError(error.RemoteTransportUnavailable);
            return error.RemoteTransportUnavailable;
        };
    }

    fn recordRemotePublicationStatus(self: *Self, status: RemotePublicationStatus) void {
        self.remote_publication_status = status;
        self.remote_publication_status_ns = Clock.monotonicNanos();
        const counters = self.service_counters orelse return;
        switch (status) {
            .unknown => {},
            .claimed => counters.increment(.aeron_remote_claimed),
            .not_connected => counters.increment(.aeron_remote_not_connected),
            .back_pressured => counters.increment(.aeron_remote_back_pressured),
            .admin_action => counters.increment(.aeron_remote_admin_action),
            .closed => counters.increment(.aeron_remote_closed),
            .max_position_exceeded => counters.increment(.aeron_remote_max_position_exceeded),
            .failed => counters.increment(.aeron_remote_failed),
        }
    }

    fn recordSend(self: *const Self, payload_len: usize) void {
        const counters = self.service_counters orelse return;
        counters.increment(.messages_sent);
        counters.add(.bytes_sent, @intCast(payload_len));
    }

    fn remoteDataFields(
        self: *const Self,
        target_node_id: i16,
        target_service_id: i32,
        template_id: u16,
        correlation_id: i64,
        payload_len: usize,
    ) data_header.CodecError!data_header.EncodeFields {
        if (self.local_node_id < 0 or target_node_id < 0) return error.InvalidNodeId;
        if (@as(u16, @intCast(self.local_node_id)) > data_header.max_node_id or
            @as(u16, @intCast(target_node_id)) > data_header.max_node_id)
            return error.InvalidNodeId;
        if (self.local_service_id < 0 or target_service_id < 0) return error.InvalidServiceId;

        return .{
            .source_node_id = @intCast(self.local_node_id),
            .source_service_id = @intCast(self.local_service_id),
            .target_node_id = @intCast(target_node_id),
            .target_service_id = @intCast(target_service_id),
            .template_id = template_id,
            .correlation_id = correlation_id,
            .payload_length = payload_len,
        };
    }

    fn mapDataHeaderError(err: data_header.CodecError) SendError {
        return switch (err) {
            error.InvalidNodeId,
            error.InvalidServiceId,
            error.InvalidPayloadLength,
            error.MessageTooLong,
            => error.MessageTooLong,
            else => error.RemoteTransportUnavailable,
        };
    }

    fn recordSendError(self: *const Self, err: anyerror) void {
        const counters = self.service_counters orelse return;
        switch (err) {
            error.SendBufferFull, error.BufferFull => counters.increment(.send_buffer_full),
            error.RemoteTransportUnavailable => counters.increment(.remote_transport_unavailable),
            error.BackPressure => counters.increment(.backpressure),
            error.BackPressureTimeout => counters.increment(.backpressure_timeouts),
            error.NoAvailableInstance => counters.increment(.no_available_instance),
            else => {},
        }
    }

    fn initFlowControlRegion(broker_meta: ?*BrokerMetadataFile) ?FlowControlRegion {
        const broker = broker_meta orelse return null;
        return broker.getFlowControlRegion();
    }
};

test "remote sends require service Aeron runtime" {
    var client = ServiceClient.init(
        std.testing.allocator,
        "remote-runtime-test",
        null,
        null,
        1,
        100,
        null,
    );
    defer client.deinit();

    try client.addInstance(.{
        .service_id = 200,
        .service_name = "remote-runtime-test",
        .node_id = 2,
    });

    try std.testing.expectError(
        error.RemoteTransportUnavailable,
        client.sendMessage(42, "payload"),
    );
}

test "remote data fields encode RingLoomDataHeader fields" {
    var client = ServiceClient.init(
        std.testing.allocator,
        "remote-header-test",
        null,
        null,
        1,
        100,
        null,
    );
    defer client.deinit();

    const fields = try client.remoteDataFields(2, 200, 77, 1234, 7);
    var buf: [data_header.RingLoomDataHeader.encoded_length]u8 = undefined;
    try data_header.encodeHeader(&buf, fields, 7);
    const decoded = try data_header.decodeHeader(&buf, 7);

    try std.testing.expectEqual(@as(u16, 1), decoded.source_node_id);
    try std.testing.expectEqual(@as(u16, 100), decoded.source_service_id);
    try std.testing.expectEqual(@as(u16, 2), decoded.target_node_id);
    try std.testing.expectEqual(@as(u16, 200), decoded.target_service_id);
    try std.testing.expectEqual(@as(u16, 77), decoded.template_id);
    try std.testing.expectEqual(@as(i64, 1234), decoded.correlation_id);
    try std.testing.expectEqual(@as(u32, 7), decoded.payload_length);
}

test "remote publication status updates health and counters" {
    var values_buf: [128 * 64]u8 align(128) = [_]u8{0} ** (128 * 64);
    var meta_buf: [256 * 64]u8 align(4) = [_]u8{0} ** (256 * 64);
    var counters_mgr = ringloom_common.concurrent.counters.CountersManager.init(&values_buf, &meta_buf);
    var service_counters = try ServiceCounters.init(&counters_mgr);

    var client = ServiceClient.init(
        std.testing.allocator,
        "remote-status-test",
        null,
        null,
        1,
        100,
        &service_counters,
    );
    defer client.deinit();

    client.recordRemotePublicationStatus(.back_pressured);
    try std.testing.expectEqual(ServiceClient.RemotePublicationStatus.back_pressured, client.remote_publication_status);
    try std.testing.expect(client.remote_publication_status_ns > 0);
    try std.testing.expectEqual(@as(i64, 1), service_counters.get(.aeron_remote_back_pressured));

    client.recordRemotePublicationStatus(.not_connected);
    try std.testing.expectEqual(ServiceClient.RemotePublicationStatus.not_connected, client.remote_publication_status);
    try std.testing.expectEqual(@as(i64, 1), service_counters.get(.aeron_remote_not_connected));

    const health = client.remotePublicationHealth();
    try std.testing.expect(!health.configured);
    try std.testing.expectEqual(ServiceClient.RemotePublicationStatus.unknown, health.last_status);
}

test "Aeron claim backpressure error follows strategy" {
    var client = ServiceClient.init(
        std.testing.allocator,
        "remote-error-test",
        null,
        null,
        1,
        100,
        null,
    );
    defer client.deinit();

    client.fc_config = .{ .enabled = true, .strategy = .drop };
    try std.testing.expectEqual(error.BackPressure, client.aeronBackpressureError());

    client.fc_config = .{ .enabled = true, .strategy = .spin, .spin_timeout_ms = 1 };
    try std.testing.expectEqual(error.BackPressureTimeout, client.aeronBackpressureError());
    try std.testing.expect(!client.shouldRetryAeronClaim(Clock.monotonicNanos(), 0));
}
