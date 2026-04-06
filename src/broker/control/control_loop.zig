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
const brz_common = @import("brz_common");
const platform = brz_common.platform;
const constants = platform.constants;
const RingBuffer = brz_common.concurrent.ring_buffer.RingBuffer;
const ServiceRegistry = @import("service_registry.zig").ServiceRegistry;
const ServiceHeartbeatChecker = @import("service_heartbeat_checker.zig").ServiceHeartbeatChecker;
const ServiceLeaderElection = @import("service_leader_election.zig").ServiceLeaderElection;
const BuffersProvider = brz_common.memory.buffers_provider.BuffersProvider;
const CommandQueue = brz_common.concurrent.command_queue.CommandQueue;
const Command = brz_common.concurrent.command_queue.Command;
const ServiceInstance = @import("service_registry.zig").ServiceInstance;
const ClusterManager = @import("../cluster/cluster_manager.zig").ClusterManager;
const CountersManager = brz_common.concurrent.counters.CountersManager;
const msg = @import("control_messages.zig");
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

    /// Group name (e.g. "brz-default").
    group: []const u8,

    // ── Timing ───────────────────────────────────────────────────
    /// Next time (monotonic ns) to run the periodic-task block.
    next_timeout_check_ns: i64,

    /// Next time (monotonic ns) to check service heartbeats.
    next_heartbeat_check_ns: i64,

    // ── Scratch buffer for message encoding ──────────────────────
    /// Pre-allocated buffer for encoding outbound control messages.
    /// 4096 bytes is more than enough for any single control message
    /// (the largest is ServiceInstances, which caps at ~256 instances
    /// × 8 bytes + fixed header + service name ≈ 2100 bytes).
    encode_buf: [4096]u8 = undefined,

    /// Allocator used for BuffersProvider instances (page allocator).
    allocator: std.mem.Allocator,

    const Self = @This();

    // ── Timing constants (imported from platform/constants.zig) ──
    const COMMAND_DRAIN_LIMIT: u32 = constants.command_drain_limit;
    const CONTROL_READ_LIMIT: u32 = constants.control_read_limit;
    const TIMEOUT_CHECK_INTERVAL_NS: i64 = constants.control_loop_timeout_check_interval_ns;
    const HEARTBEAT_CHECK_INTERVAL_NS: i64 = constants.service_heartbeat_check_interval_ms * std.time.ns_per_ms;
    const CONTROL_MSG_TYPE: i32 = 1; // ring buffer msg_type_id for control messages

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
            .allocator = opts.allocator,
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

            self.next_timeout_check_ns = now_ns + TIMEOUT_CHECK_INTERVAL_NS;
        }

        // 4. Update monitoring counters
        self.updateCounters();

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

        if (payload.len < msg.header_size) {
            log.warn("control message too short: {} bytes", .{payload.len});
            return;
        }

        const header: *const msg.ControlMessageHeader = @ptrCast(@alignCast(payload.ptr));

        switch (header.template_id) {
            1 => self.handleRegisterService(payload),
            3 => self.handleSubscribeToServiceUpdates(payload),
            5 => self.handleUnregisterService(payload),
            else => {
                log.warn("unknown control template_id: {}", .{header.template_id});
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
        if (payload.len < @sizeOf(msg.RegisterServiceMsg)) {
            log.warn("RegisterService message too short: {} bytes", .{payload.len});
            return;
        }

        const register_msg: *const msg.RegisterServiceMsg = @ptrCast(@alignCast(payload.ptr));
        const service_name = msg.decodeRegisterServiceName(payload);

        log.info("registering service: name={s}, id={}, leader_election={}", .{
            service_name,
            register_msg.service_id,
            register_msg.leader_election_enabled != 0,
        });

        // 1. Register in ServiceRegistry
        self.service_registry.register(.{
            .service_id = register_msg.service_id,
            .node_id = self.local_node_id,
            .service_name = service_name,
            .leader_election_enabled = register_msg.leader_election_enabled != 0,
            .is_local = true,
        }) catch |err| {
            log.err("failed to register service {s} (id={}): {}", .{
                service_name,
                register_msg.service_id,
                err,
            });
            return;
        };

        // 2. Open the service's metadata file and create a BuffersProvider
        const buffers = BuffersProvider.getInstance(
            self.allocator,
            register_msg.service_id,
            service_name,
            self.storage_path,
            self.group,
        ) catch |err| {
            log.err("failed to open metadata file for service {s} (id={}): {}", .{
                service_name,
                register_msg.service_id,
                err,
            });
            // Undo the registration.
            _ = self.service_registry.remove(register_msg.service_id, self.local_node_id);
            return;
        };

        // 3. Associate the BuffersProvider with the service in the registry
        self.service_registry.setLocalBuffers(register_msg.service_id, buffers);

        // 4. Evaluate service leader (if leader election is enabled for this service)
        var is_leader = false;
        if (register_msg.leader_election_enabled != 0) {
            if (self.cluster_manager.isClusterLeader()) {
                is_leader = self.leader_election.evaluate(
                    &self.service_registry,
                    service_name,
                    self.local_node_id,
                    register_msg.service_id,
                );
            }
        }

        // 5. Send RegistrationResponse to the service's control ring buffer
        self.sendRegistrationResponse(buffers, register_msg.service_id, is_leader);

        // 6. Broadcast ServiceAdded to peer brokers (via cluster manager)
        self.cluster_manager.broadcastServiceAdded(
            register_msg.service_id,
            service_name,
            register_msg.leader_election_enabled != 0,
        );

        // 7. Notify all local subscribers watching this service name
        self.notifySubscribers(service_name);
    }

    /// Write a RegistrationResponse to the service's control ring buffer.
    fn sendRegistrationResponse(
        self: *Self,
        buffers: *BuffersProvider,
        service_id: i32,
        is_leader: bool,
    ) void {
        const len = msg.encodeRegistrationResponse(
            &self.encode_buf,
            service_id,
            @intCast(self.local_node_id),
            is_leader,
        );

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
        if (payload.len < @sizeOf(msg.UnregisterServiceMsg)) {
            log.warn("UnregisterService message too short: {} bytes", .{payload.len});
            return;
        }

        const unregister_msg: *const msg.UnregisterServiceMsg = @ptrCast(
            @alignCast(payload.ptr),
        );

        log.info("unregistering service: id={}", .{unregister_msg.service_id});
        self.handleServiceRemoved(unregister_msg.service_id);
    }

    /// Shared path for both graceful and forced deregistration.
    /// Performs all cleanup and notifications.
    fn handleServiceRemoved(self: *Self, service_id: i32) void {
        // 1. Remove from ServiceRegistry. Returns the instance if it existed.
        const removed = self.service_registry.remove(service_id, self.local_node_id) orelse {
            log.warn("attempted to remove unknown service: id={}", .{service_id});
            return;
        };

        log.info("service removed: name={s}, id={}", .{
            removed.service_name,
            service_id,
        });

        // 2. Close the BuffersProvider (unmaps the service's metadata file).
        if (self.service_registry.getLocalBuffers(service_id)) |buffers| {
            buffers.close(self.allocator);
        }
        self.service_registry.removeLocalBuffers(service_id);

        // 3. Broadcast ServiceRemoved to peer brokers.
        self.cluster_manager.broadcastServiceRemoved(service_id, removed.service_name);

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
    // Service Discovery (Section 6)
    // ─────────────────────────────────────────────────────────────

    fn handleSubscribeToServiceUpdates(self: *Self, payload: []const u8) void {
        if (payload.len < @sizeOf(msg.SubscribeMsg)) {
            log.warn("SubscribeToServiceUpdates message too short: {} bytes", .{payload.len});
            return;
        }

        const subscribe_msg: *const msg.SubscribeMsg = @ptrCast(@alignCast(payload.ptr));
        const service_name = msg.decodeSubscribeServiceName(payload);

        log.info("subscription: service {} subscribing to '{s}'", .{
            subscribe_msg.local_service_id,
            service_name,
        });

        // 1. Register the subscription in the registry.
        self.service_registry.addSubscription(service_name, subscribe_msg.local_service_id) catch |err| {
            log.err("failed to register subscription for service {}: {}", .{
                subscribe_msg.local_service_id,
                err,
            });
            return;
        };

        // 2. Immediately send the current instance list.
        //    Even if there are zero instances, the service needs to know.
        self.sendServiceInstances(subscribe_msg.local_service_id, service_name);
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

        // Build ServiceInstanceEntry array on the stack.
        var entries: [constants.default_max_services]msg.ServiceInstanceEntry = undefined;
        for (instance_buf[0..count], 0..) |inst, i| {
            const is_leader_id = self.service_registry.getLeader(service_name);
            entries[i] = .{
                .service_id = inst.service_id,
                .node_id = @intCast(inst.node_id),
                .is_leader = if (is_leader_id != null and is_leader_id.? == inst.service_id) @as(u8, 1) else @as(u8, 0),
            };
        }

        // Encode the ServiceInstances message.
        const len = msg.encodeServiceInstances(
            &self.encode_buf,
            subscriber_id,
            service_name,
            entries[0..count],
        );

        // Write to the subscriber's control ring buffer.
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
        const subscriber_set = self.service_registry.getSubscribers(service_name) orelse return;

        const len = msg.encodeLeaderChanged(
            &self.encode_buf,
            leader_service_id,
            @intCast(leader_node_id),
            service_name,
        );

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
// are intentionally omitted here because the Zig 0.15 test runner retries
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
    rb_buf: [1024 + @import("brz_common").concurrent.ring_buffer.trailer_length]u8 align(8),
    rb: RingBuffer,

    fn create() TestFixture {
        var f: TestFixture = undefined;
        @memset(&f.values_buf, 0);
        @memset(&f.meta_buf, 0);
        f.counters_mgr = CountersManager.init(&f.values_buf, &f.meta_buf);
        f.cluster_mgr = ClusterManager.initSingleNode(1);
        f.cmd_queue = CommandQueue.init(&f.cmd_buf);
        @memset(&f.rb_buf, 0);
        f.rb = RingBuffer.init(&f.rb_buf, false, null, null) catch unreachable;
        return f;
    }

    fn makeControlLoop(self: *TestFixture) ControlLoop {
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
