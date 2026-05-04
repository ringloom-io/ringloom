//! Control Loop — the main duty-cycle event loop for the broker control plane.
//!
//! Runs on a dedicated thread (Thread 1) and performs:
//!   1. Drains the inter-event-loop command queue
//!   2. Polls the broker's control ring buffer for service messages
//!   3. Checks service heartbeats (rate-limited to every 3s)
//!   4. Delegates to the cluster manager (rate-limited to every 1s)
//!   5. Updates monitoring counters
//!
//! The control loop never allocates on the hot path. All data structures are
//! pre-allocated at startup. Message encoding uses a pre-allocated scratch buffer.

const std = @import("std");
const ringloom_common = @import("ringloom_common");
const platform = ringloom_common.platform;
const constants = platform.constants;
const RingBuffer = ringloom_common.concurrent.ring_buffer.RingBuffer;
const ServiceRegistry = @import("service_registry.zig").ServiceRegistry;
const ServiceHeartbeatChecker = @import("service_heartbeat_checker.zig").ServiceHeartbeatChecker;
const ServiceLeaderElection = @import("service_leader_election.zig").ServiceLeaderElection;
const BuffersProvider = ringloom_common.memory.buffers_provider.BuffersProvider;
const CommandQueue = ringloom_common.concurrent.command_queue.CommandQueue;
const Command = ringloom_common.concurrent.command_queue.Command;
const ServiceInstance = @import("service_registry.zig").ServiceInstance;
const ClusterManager = @import("../cluster/cluster_manager.zig").ClusterManager;
const CountersManager = ringloom_common.concurrent.counters.CountersManager;
const encoding = ringloom_common.message.control_encoding;
const fc_messages = ringloom_common.message.flow_control_messages;
const memory = ringloom_common.memory;
const admin = @import("../cluster/admin_messages.zig");
const admin_dispatch = @import("../cluster/admin_dispatch.zig");
const AdminCommandQueue = admin_dispatch.AdminCommandQueue;
const AdminCommand = admin_dispatch.AdminCommand;
const TcpFrameHeader = ringloom_common.protocol.frame_parser.TcpFrameHeader;
const RoutingRegistry = @import("../receiver/message_router.zig").ServiceRegistry;
const FlowControlRegion = memory.FlowControlRegion;
const PressureState = memory.PressureState;
const log = std.log.scoped(.control_loop);

// ── File-level state for RingBuffer callback dispatch ─────────────────
//
// RingBuffer.read() takes a bare function pointer (*const fn(i32, []const u8) void)
// with no context parameter. We use file-level state to bridge the gap,
// following the same pattern as control_agent.zig. This is safe because
// the control loop runs on a single dedicated thread.
threadlocal var tls_control_loop: ?*ControlLoop = null;

pub const ControlLoop = struct {
    // ── Core dependencies ────────────────────────────────────────
    /// The broker's control ring buffer. Services write here; we read.
    control_rb: *RingBuffer,

    /// Inter-event-loop command queue. Sender/receiver threads post
    /// commands here; we drain them.
    cmd_queue: *CommandQueue,

    /// Cluster manager — drives leader election, state sync, broker heartbeats.
    cluster_manager: *ClusterManager,

    /// Counters buffer for monitoring metrics.
    counters: *CountersManager,

    // ── Owned state ──────────────────────────────────────────────
    /// All known service instances (local and remote).
    service_registry: ServiceRegistry,

    /// Heartbeat checker — stateless, operates on the registry.
    heartbeat_checker: ServiceHeartbeatChecker,

    /// Leader election evaluator.
    leader_election: ServiceLeaderElection,

    // ── Configuration ────────────────────────────────────────────
    /// This broker's node ID.
    local_node_id: u8,

    /// Storage path for metadata files (e.g. "/dev/shm").
    storage_path: []const u8,

    /// Group name (e.g. "ringloom-default").
    group: []const u8,

    // ── Timing ───────────────────────────────────────────────────
    /// Next time (monotonic ns) to run the periodic-task block.
    next_timeout_check_ns: i64,

    /// Next time (monotonic ns) to check service heartbeats.
    next_heartbeat_check_ns: i64,

    /// Next time (monotonic ns) to scan local service buffers for FC updates.
    next_fc_check_ns: i64,

    /// Next time (monotonic ns) to re-broadcast local service discovery.
    next_service_rebroadcast_ns: i64,

    // ── Scratch buffer for message encoding ──────────────────────
    /// Pre-allocated buffer for encoding outbound control messages.
    /// 4096 bytes is more than enough for any single control message
    /// (the largest is ServiceInstances, which caps at ~256 instances
    /// × 8 bytes + fixed header + service name ≈ 2100 bytes).
    encode_buf: [4096]u8 = undefined,

    /// Allocator used for BuffersProvider instances (page allocator).
    allocator: std.mem.Allocator,

    /// Admin command queue — receiver posts admin messages here.
    admin_cmd_queue: ?*AdminCommandQueue(64),

    /// Send ring buffer — used for broadcasting admin messages to peers.
    send_ring_buffer: ?*RingBuffer,

    /// Peer node IDs — used to iterate peers for broadcasting.
    peer_node_ids: []const u8,

    /// Receiver's routing registry — maps service IDs to message ring buffers.
    /// Updated on service register/deregister so incoming TCP messages can be
    /// routed to local services. Accessed from the receiver thread (reads) and
    /// the control thread (writes). The receiver is stopped before the control
    /// loop during shutdown, so cleanup in onClose is safe.
    routing_registry: ?*RoutingRegistry,

    /// Tracks heap-allocated RingBuffer instances created for routing registry
    /// entries. On deregistration the routing entry is nullified but the
    /// RingBuffer is kept alive (deferred free) to avoid a race with the
    /// receiver thread. All are freed in onClose after the receiver has stopped.
    allocated_routing_rbs: std.AutoHashMap(u16, *RingBuffer),

    /// Flow-control counters region visible to local ServiceClients.
    fc_region: ?FlowControlRegion,

    fc_enabled: bool,
    fc_low_watermark_pct: u8,
    fc_high_watermark_pct: u8,
    fc_check_interval_ns: i64,
    fc_refresh_interval_ns: i64,
    fc_normal_refresh_interval_ns: i64,

    const Self = @This();

    // ── Timing constants (imported from platform/constants.zig) ──
    const COMMAND_DRAIN_LIMIT: u32 = constants.command_drain_limit;
    const CONTROL_READ_LIMIT: u32 = constants.control_read_limit;
    const TIMEOUT_CHECK_INTERVAL_NS: i64 = constants.control_loop_timeout_check_interval_ns;
    const HEARTBEAT_CHECK_INTERVAL_NS: i64 = constants.service_heartbeat_check_interval_ms * std.time.ns_per_ms;
    const CONTROL_MSG_TYPE: i32 = 1; // ring buffer msg_type_id for control messages
    const SERVICE_REBROADCAST_INTERVAL_NS: i64 = 1 * std.time.ns_per_s;

    // ─────────────────────────────────────────────────────────────
    // Construction
    // ─────────────────────────────────────────────────────────────

    pub const InitOptions = struct {
        control_rb: *RingBuffer,
        cmd_queue: *CommandQueue,
        cluster_manager: *ClusterManager,
        counters: *CountersManager,
        local_node_id: u8,
        storage_path: []const u8,
        group: []const u8,
        allocator: std.mem.Allocator,
        admin_cmd_queue: ?*AdminCommandQueue(64) = null,
        send_ring_buffer: ?*RingBuffer = null,
        peer_node_ids: []const u8 = &.{},
        routing_registry: ?*RoutingRegistry = null,
        fc_region: ?FlowControlRegion = null,
        fc_enabled: bool = false,
        fc_low_watermark_pct: u8 = 25,
        fc_high_watermark_pct: u8 = 50,
        fc_check_interval_ms: u32 = 1,
        fc_refresh_interval_ms: u32 = 200,
        fc_normal_refresh_interval_ms: u32 = 2000,
    };

    pub fn init(opts: InitOptions) Self {
        return .{
            .control_rb = opts.control_rb,
            .cmd_queue = opts.cmd_queue,
            .cluster_manager = opts.cluster_manager,
            .counters = opts.counters,
            .service_registry = ServiceRegistry.init(opts.allocator),
            .heartbeat_checker = ServiceHeartbeatChecker.init(),
            .leader_election = ServiceLeaderElection.init(),
            .local_node_id = opts.local_node_id,
            .storage_path = opts.storage_path,
            .group = opts.group,
            .next_timeout_check_ns = 0,
            .next_heartbeat_check_ns = 0,
            .next_fc_check_ns = 0,
            .next_service_rebroadcast_ns = 0,
            .allocator = opts.allocator,
            .admin_cmd_queue = opts.admin_cmd_queue,
            .send_ring_buffer = opts.send_ring_buffer,
            .peer_node_ids = opts.peer_node_ids,
            .routing_registry = opts.routing_registry,
            .allocated_routing_rbs = std.AutoHashMap(u16, *RingBuffer).init(opts.allocator),
            .fc_region = opts.fc_region,
            .fc_enabled = opts.fc_enabled and opts.fc_region != null,
            .fc_low_watermark_pct = opts.fc_low_watermark_pct,
            .fc_high_watermark_pct = opts.fc_high_watermark_pct,
            .fc_check_interval_ns = @as(i64, @intCast(opts.fc_check_interval_ms)) * std.time.ns_per_ms,
            .fc_refresh_interval_ns = @as(i64, @intCast(opts.fc_refresh_interval_ms)) * std.time.ns_per_ms,
            .fc_normal_refresh_interval_ns = @as(i64, @intCast(opts.fc_normal_refresh_interval_ms)) * std.time.ns_per_ms,
        };
    }

    // ─────────────────────────────────────────────────────────────
    // EventLoop interface
    // ─────────────────────────────────────────────────────────────

    /// Called by the ThreadRunner on every iteration of the event loop.
    /// Returns the number of work items processed. If zero, the idle
    /// strategy will engage.
    pub fn doWork(self: *Self) u32 {
        var work_count: u32 = 0;
        const now_ns = platform.Clock.monotonicNanos();

        // 1. Drain inter-event-loop commands (max 1 per cycle to limit jitter)
        work_count += self.cmd_queue.drain(@ptrCast(self), dispatchCommandThunk, COMMAND_DRAIN_LIMIT);

        // 1b. Drain admin commands from receiver event loop
        work_count += self.processAdminCommands();

        // 2. Poll broker's control ring buffer for service messages.
        //    We set file-level state so the bare function pointer can find us.
        tls_control_loop = self;
        work_count += self.control_rb.read(&onControlMessageThunk, CONTROL_READ_LIMIT);
        tls_control_loop = null;

        // 3. Periodic tasks — rate-limited to every ~1 second
        if (now_ns > self.next_timeout_check_ns) {

            // 3a. Heartbeat checking — every 3 seconds
            if (now_ns > self.next_heartbeat_check_ns) {
                self.checkServiceHeartbeats(now_ns);
                self.next_heartbeat_check_ns = now_ns + HEARTBEAT_CHECK_INTERVAL_NS;
            }

            // 3b. Cluster protocol tasks (leader election, state sync, broker heartbeats)
            self.cluster_manager.doWork(now_ns);

            // 3c. Reconcile service discovery. ServiceAdded admin messages are
            // idempotent, so periodic broadcasts repair announcements dropped
            // during TCP setup or broker startup races.
            if (self.peer_node_ids.len > 0 and now_ns > self.next_service_rebroadcast_ns) {
                self.broadcastAllLocalServices();
                self.broadcastAllLocalCapacities();
                self.next_service_rebroadcast_ns = now_ns + SERVICE_REBROADCAST_INTERVAL_NS;
            }

            self.next_timeout_check_ns = now_ns + TIMEOUT_CHECK_INTERVAL_NS;
        }

        // 4. Update monitoring counters
        self.updateCounters();

        if (self.fc_enabled and now_ns > self.next_fc_check_ns) {
            work_count += self.updateFlowControlState(now_ns);
            self.next_fc_check_ns = now_ns + self.fc_check_interval_ns;
        }

        return work_count;
    }

    /// EventLoop-compatible function pointer.
    pub fn doWorkFn(ctx: *anyopaque) u32 {
        const self: *Self = @ptrCast(@alignCast(ctx));
        return self.doWork();
    }

    /// Called once when the event loop is shutting down.
    pub fn onClose(self: *Self) void {
        log.info("control loop shutting down, closing {} local service mappings", .{
            self.service_registry.localServiceCount(),
        });

        // Free heap-allocated RingBuffers used for routing registry entries.
        // Safe: the receiver thread has already been stopped before the
        // control loop during shutdown (see broker_threads.zig).
        var rb_iter = self.allocated_routing_rbs.valueIterator();
        while (rb_iter.next()) |rb_ptr| {
            self.allocator.destroy(rb_ptr.*);
        }
        self.allocated_routing_rbs.deinit();

        self.service_registry.deinit();
    }

    /// EventLoop-compatible onClose function pointer.
    pub fn onCloseFn(ctx: *anyopaque) void {
        const self: *Self = @ptrCast(@alignCast(ctx));
        self.onClose();
    }

    // ─────────────────────────────────────────────────────────────
    // Control message dispatch
    // ─────────────────────────────────────────────────────────────

    /// Bare function pointer thunk for RingBuffer.read(). Retrieves the
    /// ControlLoop instance from file-level state and dispatches.
    fn onControlMessageThunk(_msg_type_id: i32, payload: []const u8) void {
        if (tls_control_loop) |self| {
            self.onControlMessage(_msg_type_id, payload);
        }
    }

    /// Ring buffer read callback. Called for each record in the broker's
    /// control ring buffer.
    fn onControlMessage(self: *Self, _msg_type_id: i32, payload: []const u8) void {
        _ = _msg_type_id;

        if (payload.len < 2) {
            log.warn("control message too short: {} bytes", .{payload.len});
            return;
        }

        const template_id = encoding.readTemplateId(payload);

        switch (template_id) {
            1 => self.handleRegisterService(payload),
            3 => self.handleSubscribeToServiceUpdates(payload),
            5 => self.handleUnregisterService(payload),
            else => {
                log.warn("unknown control template_id: {}", .{template_id});
            },
        }
    }

    // ─────────────────────────────────────────────────────────────
    // Command dispatch
    // ─────────────────────────────────────────────────────────────

    /// Thunk for CommandQueue.drain() — matches the expected signature.
    fn dispatchCommandThunk(context: *anyopaque, cmd: *Command) void {
        // The command's handler already knows what to do. Pass the
        // control loop context through directly.
        cmd.handler(context, cmd);
    }

    // ─────────────────────────────────────────────────────────────
    // Registration (Section 4)
    // ─────────────────────────────────────────────────────────────

    fn handleRegisterService(self: *Self, payload: []const u8) void {
        // Minimum size: template_id(2) + service_id(4) + leader_election(1) + name_len(2) = 9
        if (payload.len < 9) {
            log.warn("RegisterService message too short: {} bytes", .{payload.len});
            return;
        }

        const data = encoding.decodeRegisterService(payload);
        const service_name = data.service_name;

        log.info("registering service: name={s}, id={}, leader_election={}", .{
            service_name,
            data.service_id,
            data.leader_election_enabled,
        });

        // 1. Register in ServiceRegistry
        self.service_registry.register(.{
            .service_id = data.service_id,
            .node_id = self.local_node_id,
            .service_name = service_name,
            .leader_election_enabled = data.leader_election_enabled,
            .is_local = true,
        }) catch |err| {
            log.err("failed to register service {s} (id={}): {}", .{
                service_name,
                data.service_id,
                err,
            });
            return;
        };

        // 2. Open the service's metadata file and create a BuffersProvider
        const buffers = BuffersProvider.getInstance(
            self.allocator,
            data.service_id,
            self.local_node_id,
            service_name,
            self.storage_path,
            self.group,
        ) catch |err| {
            log.err("failed to open metadata file for service {s} (id={}): {}", .{
                service_name,
                data.service_id,
                err,
            });
            // Undo the registration.
            if (self.service_registry.remove(data.service_id, self.local_node_id)) |removed| {
                self.allocator.free(@constCast(removed.service_name));
            }
            return;
        };

        // 3. Associate the BuffersProvider with the service in the registry
        self.service_registry.setLocalBuffers(data.service_id, buffers);
        const messages_capacity: u32 = @intCast(buffers.service_file.header.messages_buffer_length);
        self.service_registry.updateFlowControlState(
            data.service_id,
            self.local_node_id,
            messages_capacity,
            messages_capacity,
            .unknown,
            0,
        );

        // 3.5. Register in the receiver's routing registry so incoming TCP
        //      messages for this service can be delivered to its ring buffer.
        self.registerInRoutingRegistry(data.service_id, service_name, buffers);

        // 4. Evaluate service leader (if leader election is enabled for this service)
        var is_leader = false;
        if (data.leader_election_enabled) {
            if (self.cluster_manager.isClusterLeader()) {
                is_leader = self.leader_election.evaluate(
                    &self.service_registry,
                    service_name,
                    self.local_node_id,
                    data.service_id,
                );
            }
        }

        // 5. Send RegistrationResponse to the service's control ring buffer
        self.sendRegistrationResponse(buffers, data.service_id, is_leader);

        // 6. Broadcast ServiceAdded to peer brokers (via send ring buffer)
        self.broadcastServiceAdded(
            data.service_id,
            service_name,
            data.leader_election_enabled,
        );
        self.broadcastServiceCapacity(data.service_id, messages_capacity, messages_capacity);

        // 7. Notify all local subscribers watching this service name
        self.notifySubscribers(service_name);

        log.info("service registered: name={s}, id={}", .{ service_name, data.service_id });
    }

    /// Write a RegistrationResponse to the service's control ring buffer.
    fn sendRegistrationResponse(
        self: *Self,
        buffers: *BuffersProvider,
        service_id: i32,
        is_leader: bool,
    ) void {
        _ = is_leader;
        const len = encoding.encodeRegistrationResponse(&self.encode_buf, .{
            .service_id = service_id,
            .node_id = @intCast(self.local_node_id),
            .success = true,
        });

        var control_rb = RingBuffer.init(
            @alignCast(buffers.getControlBuffer()),
            false,
            null,
            null,
        ) catch |err| {
            log.err("failed to init control ring buffer for service {}: {}", .{
                service_id,
                err,
            });
            return;
        };
        control_rb.write(CONTROL_MSG_TYPE, self.encode_buf[0..len]) catch |err| {
            // The service's control ring buffer is full. This should not happen
            // during registration because the service hasn't started processing
            // other control messages yet. Log and move on — the service will
            // eventually time out waiting for the response.
            log.err("failed to write RegistrationResponse to service {}: {}", .{
                service_id,
                err,
            });
        };
    }

    // ─────────────────────────────────────────────────────────────
    // Deregistration (Section 5)
    // ─────────────────────────────────────────────────────────────

    fn handleUnregisterService(self: *Self, payload: []const u8) void {
        // Minimum size: template_id(2) + service_id(4) = 6
        if (payload.len < 6) {
            log.warn("UnregisterService message too short: {} bytes", .{payload.len});
            return;
        }

        const data = encoding.decodeUnregisterService(payload);

        log.info("unregistering service: id={}", .{data.service_id});
        self.handleServiceRemoved(data.service_id);
        log.info("service unregistered: id={}", .{data.service_id});
    }

    /// Shared path for both graceful and forced deregistration.
    /// Performs all cleanup and notifications.
    fn handleServiceRemoved(self: *Self, service_id: i32) void {
        // 1. Remove from ServiceRegistry. Returns the instance if it existed.
        //    The returned instance's service_name is an owned allocation that
        //    we must free when done.
        const removed = self.service_registry.remove(service_id, self.local_node_id) orelse {
            log.warn("attempted to remove unknown service: id={}", .{service_id});
            return;
        };
        defer self.allocator.free(@constCast(removed.service_name));

        log.info("service removed: name={s}, id={}", .{
            removed.service_name,
            service_id,
        });

        // 2. Deregister from the receiver's routing registry (prevents new TCP
        //    lookups). The heap-allocated RingBuffer is NOT freed here — the
        //    receiver thread may still hold a cached pointer. It will be freed
        //    on re-registration of the same service ID or during onClose (after
        //    the receiver thread has stopped).
        self.deregisterFromRoutingRegistry(service_id);

        // 3. Close the BuffersProvider (unmaps the service's metadata file).
        if (self.service_registry.getLocalBuffers(service_id)) |buffers| {
            buffers.close(self.allocator);
        }
        self.service_registry.removeLocalBuffers(service_id);

        // 3. Broadcast ServiceRemoved to peer brokers (via send ring buffer).
        self.broadcastServiceRemoved(service_id, removed.service_name);

        // 4. Notify all local subscribers watching this service name.
        //    They'll receive an updated ServiceInstances list (possibly empty).
        self.notifySubscribers(removed.service_name);

        // 5. Re-evaluate service leader if this service had leader election enabled.
        if (removed.leader_election_enabled) {
            if (self.cluster_manager.isClusterLeader()) {
                _ = self.leader_election.evaluate(
                    &self.service_registry,
                    removed.service_name,
                    self.local_node_id,
                    -1, // no specific local candidate — re-evaluate globally
                );
            }
        }
    }

    // ─────────────────────────────────────────────────────────────
    // Admin Command Processing (cross-broker service discovery)
    // ─────────────────────────────────────────────────────────────

    fn processAdminCommands(self: *Self) u32 {
        const queue = self.admin_cmd_queue orelse return 0;
        var count: u32 = 0;
        while (queue.dequeue()) |cmd| {
            switch (cmd) {
                .service_added => |e| self.handleRemoteServiceAdded(e.data),
                .service_removed => |e| self.handleRemoteServiceRemoved(e.data),
                .peer_connected => |e| self.handlePeerConnected(e.node_id),
                .remaining_bytes_update => |e| self.handleRemainingBytesUpdate(
                    e.source_node_id,
                    e.data[0..e.len],
                ),
                .flow_control_snapshot => |e| self.handleFlowControlSnapshot(e.data[0..e.len]),
                .service_capacity_update => |e| self.handleServiceCapacityUpdate(e.data),
                else => {},
            }
            count += 1;
        }
        return count;
    }

    fn handleRemoteServiceAdded(self: *Self, data: [@sizeOf(admin.ServiceAddedBody)]u8) void {
        var body: admin.ServiceAddedBody = undefined;
        @memcpy(@as(*[@sizeOf(admin.ServiceAddedBody)]u8, @ptrCast(&body)), &data);

        const name = admin.trimServiceName(&body.service_name);

        if (self.service_registry.getInstancePtr(
            @as(i32, @intCast(body.service_id)),
            body.node_id,
        ) != null) {
            self.notifySubscribers(name);
            return;
        }

        log.info("remote service added: name={s}, id={}, node={}", .{
            name, body.service_id, body.node_id,
        });

        self.service_registry.register(.{
            .service_id = @as(i32, @intCast(body.service_id)),
            .node_id = body.node_id,
            .service_name = name,
            .leader_election_enabled = body.leader_election_enabled == 1,
            .is_local = false,
        }) catch |err| {
            log.err("failed to register remote service {s}: {}", .{ name, err });
            return;
        };

        if (self.ensureFcSlot(@intCast(body.service_id), body.node_id, 0)) |slot| {
            self.service_registry.setFlowControlSlot(
                @intCast(body.service_id),
                body.node_id,
                @intCast(slot.index),
                slot.generation,
                0,
            );
        }

        self.notifySubscribers(name);
    }

    fn handleRemoteServiceRemoved(self: *Self, data: [@sizeOf(admin.ServiceRemovedBody)]u8) void {
        var body: admin.ServiceRemovedBody = undefined;
        @memcpy(@as(*[@sizeOf(admin.ServiceRemovedBody)]u8, @ptrCast(&body)), &data);

        const name = admin.trimServiceName(&body.service_name);
        const removed = self.service_registry.remove(
            @as(i32, @intCast(body.service_id)),
            body.node_id,
        ) orelse return;

        defer self.allocator.free(@constCast(removed.service_name));

        log.info("remote service removed: name={s}, id={}, node={}", .{
            name, body.service_id, body.node_id,
        });

        self.notifySubscribers(name);
    }

    /// When a new peer connects, re-broadcast all local services so they
    /// learn about us (handles the late-join case where the initial broadcast
    /// was lost because the TCP link wasn't up yet).
    fn handlePeerConnected(self: *Self, node_id: u8) void {
        log.info("peer connected notification: node={}, re-broadcasting {} local services", .{
            node_id,
            self.service_registry.localServiceCount(),
        });
        self.broadcastAllLocalServices();
        self.broadcastAllLocalCapacities();
    }

    // ─────────────────────────────────────────────────────────────
    // Admin Broadcast (outbound to peers)
    // ─────────────────────────────────────────────────────────────

    fn broadcastServiceAdded(self: *Self, service_id: i32, service_name: []const u8, leader_election_enabled: bool) void {
        self.broadcastAdminMessage(
            admin.ServiceAddedBody,
            admin.TEMPLATE_SERVICE_ADDED,
            admin.ServiceAddedBody{
                .node_id = self.local_node_id,
                .service_id = @intCast(service_id),
                .service_name = admin.padServiceName(service_name),
                .leader_election_enabled = if (leader_election_enabled) @as(u8, 1) else @as(u8, 0),
            },
        );
    }

    fn broadcastServiceRemoved(self: *Self, service_id: i32, service_name: []const u8) void {
        self.broadcastAdminMessage(
            admin.ServiceRemovedBody,
            admin.TEMPLATE_SERVICE_REMOVED,
            admin.ServiceRemovedBody{
                .node_id = self.local_node_id,
                .service_id = @intCast(service_id),
                .service_name = admin.padServiceName(service_name),
            },
        );
    }

    fn broadcastServiceCapacity(
        self: *Self,
        service_id: i32,
        capacity: u32,
        remaining: u32,
    ) void {
        self.broadcastAdminMessage(
            fc_messages.ServiceCapacityUpdatePayload,
            admin.TEMPLATE_SERVICE_CAPACITY_UPDATE,
            fc_messages.ServiceCapacityUpdatePayload{
                .source_node_id = self.local_node_id,
                .service_id = service_id,
                .messages_buffer_capacity = capacity,
                .current_remaining_bytes = remaining,
            },
        );
    }

    fn broadcastAdminMessage(
        self: *Self,
        comptime BodyType: type,
        template_id: u16,
        body: BodyType,
    ) void {
        const srb = self.send_ring_buffer orelse return;
        if (self.peer_node_ids.len == 0) return;

        // Encode admin payload (AdminMessageHeader + body) into scratch buffer
        const admin_payload_len = admin.encodeAdminMessage(
            self.encode_buf[TcpFrameHeader.size..],
            BodyType,
            template_id,
            body,
        );

        const frame_length: u32 = @intCast(TcpFrameHeader.size + admin_payload_len);

        for (self.peer_node_ids) |peer_id| {
            const header = TcpFrameHeader{
                .frame_length = frame_length,
                .flags = constants.flag_admin,
                .source_node_id = self.local_node_id,
                .target_node_id = peer_id,
                .source_service_id = 0,
                .target_service_id = 0,
            };
            const header_bytes: *const [TcpFrameHeader.size]u8 = @ptrCast(&header);
            @memcpy(self.encode_buf[0..TcpFrameHeader.size], header_bytes);

            srb.write(1, self.encode_buf[0..frame_length]) catch {
                log.warn("send ring buffer full — dropping admin broadcast to node {}", .{peer_id});
            };
        }
    }

    fn broadcastAdminPayload(self: *Self, template_id: u16, payload: []const u8) void {
        const srb = self.send_ring_buffer orelse return;
        if (self.peer_node_ids.len == 0) return;

        const header_len = @sizeOf(admin.AdminMessageHeader);
        const admin_payload_len = header_len + payload.len;
        const header = admin.AdminMessageHeader{
            .block_length = @intCast(payload.len),
            .template_id = template_id,
            .schema_id = admin.SCHEMA_ID,
            .version = admin.SCHEMA_VERSION,
        };
        const header_bytes: *const [header_len]u8 = @ptrCast(&header);
        @memcpy(self.encode_buf[TcpFrameHeader.size..][0..header_len], header_bytes);
        @memcpy(self.encode_buf[TcpFrameHeader.size + header_len ..][0..payload.len], payload);

        const frame_length: u32 = @intCast(TcpFrameHeader.size + admin_payload_len);
        for (self.peer_node_ids) |peer_id| {
            const tcp_header = TcpFrameHeader{
                .frame_length = frame_length,
                .flags = constants.flag_admin,
                .source_node_id = self.local_node_id,
                .target_node_id = peer_id,
                .source_service_id = 0,
                .target_service_id = 0,
            };
            const tcp_header_bytes: *const [TcpFrameHeader.size]u8 = @ptrCast(&tcp_header);
            @memcpy(self.encode_buf[0..TcpFrameHeader.size], tcp_header_bytes);
            srb.write(1, self.encode_buf[0..frame_length]) catch {
                log.warn("send ring buffer full — dropping admin payload {} to node {}", .{
                    template_id,
                    peer_id,
                });
            };
        }
    }

    fn broadcastAllLocalServices(self: *Self) void {
        var inst_iter = self.service_registry.instances.valueIterator();
        while (inst_iter.next()) |inst| {
            if (inst.is_local) {
                self.broadcastServiceAdded(inst.service_id, inst.service_name, inst.leader_election_enabled);
            }
        }
    }

    fn broadcastAllLocalCapacities(self: *Self) void {
        var inst_iter = self.service_registry.instances.valueIterator();
        while (inst_iter.next()) |inst| {
            if (!inst.is_local or inst.messages_buffer_capacity == 0) continue;
            self.broadcastServiceCapacity(
                inst.service_id,
                inst.messages_buffer_capacity,
                inst.last_fc_remaining,
            );
        }
    }

    // ─────────────────────────────────────────────────────────────
    // Service Discovery (Section 6)
    // ─────────────────────────────────────────────────────────────

    fn handleSubscribeToServiceUpdates(self: *Self, payload: []const u8) void {
        // Minimum size: template_id(2) + subscriber_id(4) + name_len(2) = 8
        if (payload.len < 8) {
            log.warn("SubscribeToServiceUpdates message too short: {} bytes", .{payload.len});
            return;
        }

        const data = encoding.decodeSubscribeToServiceUpdates(payload);

        log.info("subscription: service {} subscribing to '{s}'", .{
            data.subscriber_service_id,
            data.target_service_name,
        });

        // 1. Register the subscription in the registry.
        self.service_registry.addSubscription(data.target_service_name, data.subscriber_service_id) catch |err| {
            log.err("failed to register subscription for service {}: {}", .{
                data.subscriber_service_id,
                err,
            });
            return;
        };

        // 2. Immediately send the current instance list.
        //    Even if there are zero instances, the service needs to know.
        self.sendServiceInstances(data.subscriber_service_id, data.target_service_name);
    }

    /// Send the complete current instance list for a service name to a single subscriber.
    fn sendServiceInstances(self: *Self, subscriber_id: i32, service_name: []const u8) void {
        // Look up the subscriber's BuffersProvider to find its control ring buffer.
        const subscriber_buffers = self.service_registry.getLocalBuffers(subscriber_id) orelse {
            log.warn("subscriber {} has no local buffers — cannot send ServiceInstances", .{
                subscriber_id,
            });
            return;
        };

        // Collect all instances of this service name using the allocation-free variant.
        var instance_buf: [constants.default_max_services]ServiceInstance = undefined;
        const count = self.service_registry.getInstancesByNameBuf(service_name, &instance_buf);

        // Open the subscriber's control ring buffer.
        var control_rb = RingBuffer.init(
            @alignCast(subscriber_buffers.getControlBuffer()),
            false,
            null,
            null,
        ) catch |err| {
            log.warn("failed to init control ring buffer for subscriber {}: {}", .{
                subscriber_id,
                err,
            });
            return;
        };

        // Send one complete snapshot, including empty snapshots so subscribers
        // can remove stale instances when a service becomes unavailable.
        var entry_buf: [constants.default_max_services]encoding.ServiceInstanceEntry = undefined;
        for (instance_buf[0..count], 0..) |inst, i| {
            const is_leader_id = self.service_registry.getLeader(service_name);
            const is_leader = is_leader_id != null and is_leader_id.? == inst.service_id;

            entry_buf[i] = .{
                .service_id = inst.service_id,
                .node_id = @intCast(inst.node_id),
                .is_leader = if (is_leader) 1 else 0,
                .fc_slot_id = inst.fc_slot_id,
                .fc_slot_generation = inst.fc_slot_generation,
                .messages_buffer_capacity = inst.messages_buffer_capacity,
            };
        }

        const len = encoding.encodeServiceInstances(
            &self.encode_buf,
            service_name,
            entry_buf[0..count],
        );
        control_rb.write(CONTROL_MSG_TYPE, self.encode_buf[0..len]) catch {
            log.warn("subscriber {} control ring buffer full — dropping ServiceInstances", .{
                subscriber_id,
            });
        };
    }

    /// Called whenever the instance set for a service name changes. Sends the
    /// updated complete list to ALL local subscribers of that name.
    fn notifySubscribers(self: *Self, service_name: []const u8) void {
        const subscriber_set = self.service_registry.getSubscribers(service_name) orelse return;

        var key_iter = subscriber_set.keyIterator();
        while (key_iter.next()) |subscriber_id_ptr| {
            self.sendServiceInstances(subscriber_id_ptr.*, service_name);
        }
    }

    // ─────────────────────────────────────────────────────────────
    // Leader Changed Notification (Section 9.5)
    // ─────────────────────────────────────────────────────────────

    /// Send a LeaderChanged message to all local subscribers of a service name.
    pub fn sendLeaderChangedToLocalSubscribers(
        self: *Self,
        leader_service_id: i32,
        leader_node_id: u8,
        service_name: []const u8,
    ) void {
        _ = leader_node_id;
        const subscriber_set = self.service_registry.getSubscribers(service_name) orelse return;

        const len = encoding.encodeLeaderChanged(&self.encode_buf, .{
            .leader_service_id = leader_service_id,
            .service_name = service_name,
        });

        var sub_iter = subscriber_set.keyIterator();
        while (sub_iter.next()) |subscriber_id_ptr| {
            const subscriber_id = subscriber_id_ptr.*;
            const buffers = self.service_registry.getLocalBuffers(subscriber_id) orelse continue;
            var control_rb = RingBuffer.init(
                @alignCast(buffers.getControlBuffer()),
                false,
                null,
                null,
            ) catch continue;
            control_rb.write(CONTROL_MSG_TYPE, self.encode_buf[0..len]) catch {
                log.warn("subscriber {} control buffer full — dropping LeaderChanged", .{
                    subscriber_id,
                });
            };
        }
    }

    // ─────────────────────────────────────────────────────────────
    // Routing Registry Integration
    // ─────────────────────────────────────────────────────────────

    /// Register a local service in the receiver's routing registry so that
    /// incoming TCP messages can be delivered to its shared-memory ring buffer.
    fn registerInRoutingRegistry(
        self: *Self,
        service_id: i32,
        service_name: []const u8,
        buffers: *BuffersProvider,
    ) void {
        const rr = self.routing_registry orelse return;

        // Service IDs must fit in the routing registry's fixed-size table.
        if (service_id < 0 or service_id >= constants.default_max_services) {
            log.warn("service id {} out of routing range [0, {}); TCP routing disabled for {s}", .{
                service_id,
                constants.default_max_services,
                service_name,
            });
            return;
        }
        const sid: u16 = @intCast(service_id);

        // Free any previously allocated RingBuffer for this slot (handles
        // re-registration of the same service ID after deregistration).
        if (self.allocated_routing_rbs.get(sid)) |old_rb| {
            self.allocator.destroy(old_rb);
            _ = self.allocated_routing_rbs.remove(sid);
        }

        // Allocate a RingBuffer view over the service's messages shared memory.
        const rb = self.allocator.create(RingBuffer) catch {
            log.err("OOM allocating routing RingBuffer for service {s} (id={})", .{
                service_name,
                service_id,
            });
            return;
        };
        rb.* = RingBuffer.init(
            @alignCast(buffers.getMessagesBuffer()),
            false,
            null,
            null,
        ) catch |err| {
            log.err("failed to init routing RingBuffer for service {s} (id={}): {}", .{
                service_name,
                service_id,
                err,
            });
            self.allocator.destroy(rb);
            return;
        };

        self.allocated_routing_rbs.put(sid, rb) catch {
            log.err("OOM tracking routing RingBuffer for service {s} (id={})", .{
                service_name,
                service_id,
            });
            self.allocator.destroy(rb);
            return;
        };

        rr.register(.{
            .service_id = sid,
            .service_name = service_name,
            .node_id = self.local_node_id,
            .messages_ring_buffer = rb,
        });

        log.info("service {s} (id={}) registered in routing registry", .{
            service_name,
            service_id,
        });
    }

    /// Deregister a service from the receiver's routing registry.
    /// The heap-allocated RingBuffer is NOT freed here to avoid a race with
    /// the receiver thread — it will be freed on re-registration or shutdown.
    fn deregisterFromRoutingRegistry(self: *Self, service_id: i32) void {
        const rr = self.routing_registry orelse return;

        if (service_id < 0 or service_id >= constants.default_max_services) return;
        const sid: u16 = @intCast(service_id);

        rr.deregister(sid);
        log.info("service id={} deregistered from routing registry", .{service_id});
    }

    // ─────────────────────────────────────────────────────────────
    // Heartbeat Checking (Section 8)
    // ─────────────────────────────────────────────────────────────

    fn checkServiceHeartbeats(self: *Self, now_ns: i64) void {
        const dead_services = self.heartbeat_checker.check(&self.service_registry, now_ns);

        for (dead_services) |service_id| {
            self.handleServiceRemoved(service_id);
        }
    }

    // ─────────────────────────────────────────────────────────────
    // Counters (Section 11)
    // ─────────────────────────────────────────────────────────────

    fn updateCounters(self: *Self) void {
        _ = self;
        // Counters are updated in-line where events occur.
        // The service count is tracked via the registry and will be
        // wired to counter IDs once the counter allocation is done
        // during broker startup.
    }

    // ─────────────────────────────────────────────────────────────
    // Flow Control
    // ─────────────────────────────────────────────────────────────

    const FcSlot = struct {
        index: u32,
        generation: u16,
    };

    fn updateFlowControlState(self: *Self, now_ns: i64) u32 {
        var entries: [constants.default_max_services]fc_messages.RemainingBytesUpdateEntry = undefined;
        var entry_count: usize = 0;

        var inst_iter = self.service_registry.instances.valueIterator();
        while (inst_iter.next()) |inst| {
            if (!inst.is_local) continue;
            if (inst.service_id < 0 or inst.service_id >= constants.default_max_services) continue;

            const rb = self.allocated_routing_rbs.get(@intCast(inst.service_id)) orelse continue;
            const capacity: u32 = @intCast(rb.getCapacity());
            const remaining_usize = rb.getCapacity() - rb.size();
            const remaining: u32 = @intCast(@min(remaining_usize, std.math.maxInt(u32)));
            const new_state = self.nextPressureState(inst.fc_pressure_state, remaining, capacity);

            const should_broadcast =
                inst.fc_pressure_state == .unknown or
                (inst.fc_pressure_state == .normal and new_state == .pressured) or
                (inst.fc_pressure_state == .pressured and new_state == .normal) or
                (new_state == .pressured and now_ns - inst.last_fc_broadcast_ns >= self.fc_refresh_interval_ns) or
                (new_state == .normal and now_ns - inst.last_fc_broadcast_ns >= self.fc_normal_refresh_interval_ns);

            inst.messages_buffer_capacity = capacity;
            inst.last_fc_remaining = remaining;
            inst.fc_pressure_state = new_state;

            if (!should_broadcast) continue;
            inst.last_fc_broadcast_ns = now_ns;
            entries[entry_count] = .{
                .service_id = inst.service_id,
                .remaining_bytes = remaining,
                .capacity = capacity,
            };
            entry_count += 1;
        }

        if (entry_count == 0) return 0;

        var body_buf: [4 + constants.default_max_services * @sizeOf(fc_messages.RemainingBytesUpdateEntry)]u8 align(4) = undefined;
        const body_len = fc_messages.encodeRemainingBytesUpdate(&body_buf, entries[0..entry_count]) orelse return 0;
        self.broadcastAdminPayload(admin.TEMPLATE_REMAINING_BYTES_UPDATE, body_buf[0..body_len]);
        return 1;
    }

    fn handleRemainingBytesUpdate(self: *Self, source_node_id: u8, payload: []const u8) void {
        const update = fc_messages.decodeRemainingBytesUpdate(payload) orelse return;
        for (update.entries) |entry| {
            self.updateRemoteFcEntry(
                source_node_id,
                entry.service_id,
                entry.capacity,
                entry.remaining_bytes,
            );
        }
    }

    fn handleFlowControlSnapshot(self: *Self, payload: []const u8) void {
        const snapshot = fc_messages.decodeFlowControlSnapshot(payload) orelse return;
        for (snapshot.entries) |entry| {
            self.updateRemoteFcEntry(
                snapshot.source_node_id,
                entry.service_id,
                entry.messages_buffer_capacity,
                entry.current_remaining_bytes,
            );
        }
    }

    fn handleServiceCapacityUpdate(
        self: *Self,
        data: [@sizeOf(fc_messages.ServiceCapacityUpdatePayload)]u8,
    ) void {
        const update: fc_messages.ServiceCapacityUpdatePayload = @bitCast(data);
        self.updateRemoteFcEntry(
            update.source_node_id,
            update.service_id,
            update.messages_buffer_capacity,
            update.current_remaining_bytes,
        );
    }

    fn updateRemoteFcEntry(
        self: *Self,
        source_node_id: u8,
        service_id: i32,
        capacity: u32,
        remaining: u32,
    ) void {
        const slot = self.ensureFcSlot(service_id, source_node_id, capacity) orelse return;
        const region = self.fc_region orelse return;
        const entry = region.getEntry(slot.index) orelse return;

        if (capacity > 0) entry.capacity = capacity;
        entry.storeRemainingBytes(remaining);
        const prev_pressure_state = entry.loadPressureState();
        const pressure_state = self.nextPressureState(prev_pressure_state, remaining, entry.capacity);
        entry.storePressureState(pressure_state);
        entry.storeLastUpdateNs(platform.Clock.monotonicNanos());

        self.service_registry.setFlowControlSlot(
            service_id,
            source_node_id,
            @intCast(slot.index),
            slot.generation,
            capacity,
        );
        self.service_registry.updateFlowControlState(
            service_id,
            source_node_id,
            remaining,
            capacity,
            pressure_state,
            platform.Clock.monotonicNanos(),
        );

        if (self.service_registry.getInstancePtr(service_id, source_node_id)) |inst| {
            self.notifySubscribers(inst.service_name);
        }
    }

    fn ensureFcSlot(self: *Self, service_id: i32, node_id: u8, capacity: u32) ?FcSlot {
        const region = self.fc_region orelse return null;
        if (region.findByService(service_id, node_id)) |found| {
            if (capacity > 0) found.entry.capacity = capacity;
            return .{ .index = found.index, .generation = found.generation };
        }

        const slot_index = region.allocateSlot(service_id, node_id, capacity) orelse return null;
        const entry = region.getEntry(slot_index) orelse return null;
        return .{ .index = slot_index, .generation = entry.loadGeneration() };
    }

    fn nextPressureState(
        self: *const Self,
        previous: PressureState,
        remaining: u32,
        capacity: u32,
    ) PressureState {
        if (capacity == 0) return .unknown;
        const low = (@as(u64, capacity) * self.fc_low_watermark_pct) / 100;
        const high = (@as(u64, capacity) * self.fc_high_watermark_pct) / 100;
        return switch (previous) {
            .pressured => if (remaining >= high) .normal else .pressured,
            .normal => if (remaining < low) .pressured else .normal,
            .unknown => if (remaining < low) .pressured else .normal,
        };
    }

    // ─────────────────────────────────────────────────────────────
    // Public accessors (for commands and cluster integration)
    // ─────────────────────────────────────────────────────────────

    /// Returns a mutable reference to the service registry.
    /// Used by inter-event-loop command handlers.
    pub fn getServiceRegistry(self: *Self) *ServiceRegistry {
        return &self.service_registry;
    }

    /// Returns a mutable reference to the leader election module.
    pub fn getLeaderElection(self: *Self) *ServiceLeaderElection {
        return &self.leader_election;
    }
};

// ── Tests ─────────────────────────────────────────────────────────────
//
// Note: Tests that exercise code paths producing log.warn or log.err output
// are intentionally omitted here because the Zig 0.16 test runner retries
// tests with unexpected log output, causing hangs. Those code paths are
// tested indirectly via the service_registry, control_messages, heartbeat
// checker, and leader election unit tests.

const testing = std.testing;

/// Helper to create a ControlLoop wired to stack-allocated test fixtures.
/// The caller must `defer result.registry_cleanup()` and keep all returned
/// pointers alive for the duration of the test.
const TestFixture = struct {
    values_buf: [128 * 4]u8 align(128),
    meta_buf: [256 * 4]u8 align(4),
    counters_mgr: CountersManager,
    cluster_mgr: ClusterManager,
    cmd_buf: [4]Command,
    cmd_queue: CommandQueue,
    rb_buf: [1024 + @import("ringloom_common").concurrent.ring_buffer.trailer_length]u8 align(8),
    rb: RingBuffer,

    fn create() TestFixture {
        var f: TestFixture = undefined;
        @memset(&f.values_buf, 0);
        @memset(&f.meta_buf, 0);
        @memset(&f.rb_buf, 0);
        f.cluster_mgr = ClusterManager.initSingleNode(1);
        // Do NOT init counters_mgr, cmd_queue, rb here — their internal
        // pointers would dangle after the return-by-value copy.
        // Call fixup() after the fixture is at its final address.
        return f;
    }

    /// Must be called after create() to fix up internal pointers.
    fn fixup(self: *TestFixture) void {
        self.counters_mgr = CountersManager.init(&self.values_buf, &self.meta_buf);
        self.cmd_queue = CommandQueue.init(&self.cmd_buf);
        self.rb = RingBuffer.init(&self.rb_buf, false, null, null) catch unreachable;
    }

    fn makeControlLoop(self: *TestFixture) ControlLoop {
        self.fixup();
        return ControlLoop.init(.{
            .control_rb = &self.rb,
            .cmd_queue = &self.cmd_queue,
            .cluster_manager = &self.cluster_mgr,
            .counters = &self.counters_mgr,
            .local_node_id = 1,
            .storage_path = "/dev/shm",
            .group = "test-group",
            .allocator = testing.allocator,
        });
    }
};

test "ControlLoop init creates valid state" {
    // Given
    var fix = TestFixture.create();

    // When
    var control_loop = fix.makeControlLoop();
    defer control_loop.service_registry.deinit();

    // Then
    try testing.expectEqual(@as(u8, 1), control_loop.local_node_id);
    try testing.expectEqual(@as(u32, 0), control_loop.service_registry.localServiceCount());
}

test "doWork returns zero when idle" {
    // Given
    var fix = TestFixture.create();
    var control_loop = fix.makeControlLoop();
    defer control_loop.service_registry.deinit();

    // When — nothing in the queues
    const work = control_loop.doWork();

    // Then
    try testing.expectEqual(@as(u32, 0), work);
}
