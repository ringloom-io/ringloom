const std = @import("std");

const ringloom_common = @import("ringloom_common");
const udp = @import("ringloom_udp");
const platform = ringloom_common.platform;
const memory = ringloom_common.memory;
const concurrent = ringloom_common.concurrent;
const control = @import("../control.zig");
const sender = @import("../sender.zig");
const receiver = @import("../receiver.zig");
const cluster = @import("../cluster.zig");
const threading = @import("../threading.zig");
const config_mod = ringloom_common.config.broker_config;

const BrokerMetadataFile = memory.BrokerMetadataFile;
const CountersManager = concurrent.CountersManager;
const RingBuffer = concurrent.RingBuffer;
const CommandQueue = concurrent.CommandQueue;
const Command = concurrent.Command;
const ControlLoop = control.ControlLoop;
const SenderEventLoop = sender.SenderEventLoop;
const ReceiverEventLoop = receiver.ReceiverEventLoop;
const RoutingRegistry = receiver.ServiceRegistry;
const ClusterManager = cluster.ClusterManager;
const BrokerThreads = threading.BrokerThreads;
const BrokerConfig = config_mod.BrokerConfig;
const IdleStrategyName = config_mod.IdleStrategyName;

/// BrokerRuntime is the process-local owner of broker resources and worker threads.
///
/// It is intentionally focused on runtime lifecycle:
/// - create/open broker-owned shared memory
/// - allocate command queues and counters
/// - construct event loops
/// - start and stop worker threads
/// - release owned resources
///
/// It does not parse CLI arguments or install signal handlers. Those concerns belong
/// to the application / executable layer.
pub const BrokerRuntime = struct {
    allocator: std.mem.Allocator,
    config: BrokerConfig,

    broker_metadata: ?BrokerMetadataFile,
    counters: ?CountersManager,

    control_cmd_buffer: ?[]Command,
    sender_cmd_buffer: ?[]Command,
    receiver_cmd_buffer: ?[]Command,

    control_cmd_queue: ?CommandQueue,
    sender_cmd_queue: ?CommandQueue,
    receiver_cmd_queue: ?CommandQueue,

    cluster_manager: ?ClusterManager,
    routing_registry: ?RoutingRegistry,

    // Ring buffers stored as fields so event loop pointers remain valid.
    control_rb: ?RingBuffer,

    control_loop: ?ControlLoop,
    sender_loop: ?SenderEventLoop,
    receiver_loop: ?ReceiverEventLoop,
    broker_threads: ?BrokerThreads,

    started: bool,

    const Self = @This();

    /// Initialize broker-owned resources but do not start worker threads yet.
    ///
    /// Event loops and threads are created in `start()` to avoid dangling
    /// self-referential pointers (init returns by value).
    pub fn init(allocator: std.mem.Allocator, config: BrokerConfig) !Self {
        var self = Self{
            .allocator = allocator,
            .config = config,
            .broker_metadata = null,
            .counters = null,
            .control_cmd_buffer = null,
            .sender_cmd_buffer = null,
            .receiver_cmd_buffer = null,
            .control_cmd_queue = null,
            .sender_cmd_queue = null,
            .receiver_cmd_queue = null,
            .cluster_manager = null,
            .routing_registry = null,
            .control_rb = null,
            .control_loop = null,
            .sender_loop = null,
            .receiver_loop = null,
            .broker_threads = null,
            .started = false,
        };
        errdefer self.deinit();

        const broker_metadata = try BrokerMetadataFile.createWithFlowControl(
            config.storage_path,
            config.group_name,
            config.node_id,
            config.control_buffer_size,
            config.messages_buffer_size,
            .{
                .fc_max_entries = if (config.fc_enabled) config.fc_max_entries else 0,
                .peer_send_max_peers = if (config.fc_peer_send_counters_enabled)
                    config.fc_peer_send_counters_max_peers
                else
                    0,
                .counter_values_buffer_length = config.counter_values_buffer_size,
                .counter_metadata_buffer_length = config.counter_metadata_buffer_size,
                .error_log_buffer_length = config.error_log_buffer_size,
            },
        );
        self.broker_metadata = broker_metadata;

        self.counters = CountersManager.init(
            self.broker_metadata.?.getCounterValuesBuffer(),
            self.broker_metadata.?.getCounterMetadataBuffer(),
        );

        const control_cmd_buffer = try allocator.alloc(Command, commandQueueCapacity());
        self.control_cmd_buffer = control_cmd_buffer;

        const sender_cmd_buffer = try allocator.alloc(Command, commandQueueCapacity());
        self.sender_cmd_buffer = sender_cmd_buffer;

        const receiver_cmd_buffer = try allocator.alloc(Command, commandQueueCapacity());
        self.receiver_cmd_buffer = receiver_cmd_buffer;

        self.control_cmd_queue = CommandQueue.init(control_cmd_buffer);
        self.sender_cmd_queue = CommandQueue.init(sender_cmd_buffer);
        self.receiver_cmd_queue = CommandQueue.init(receiver_cmd_buffer);

        self.cluster_manager = ClusterManager.initSingleNode(config.node_id);
        self.routing_registry = RoutingRegistry.init();

        self.control_rb = try RingBuffer.init(
            @alignCast(self.broker_metadata.?.getControlBuffer()),
            false,
            null,
            null,
        );

        return self;
    }

    /// Create event loops and start broker worker threads.
    ///
    /// Event loops are created here (not in init) because `self` is now at its
    /// final address, so internal pointers (&self.control_rb, &self.counters, etc.)
    /// remain valid.
    pub fn start(self: *Self) !void {
        if (self.started) return error.AlreadyStarted;

        self.control_loop = ControlLoop.init(.{
            .control_rb = &self.control_rb.?,
            .cmd_queue = &self.control_cmd_queue.?,
            .cluster_manager = &self.cluster_manager.?,
            .counters = &self.counters.?,
            .local_node_id = self.config.node_id,
            .storage_path = self.config.storage_path,
            .group = self.config.group_name,
            .allocator = self.allocator,
            .send_buffer_directory = self.broker_metadata.?.getMutableSendBufferDirectory(),
            .broker_mapped_bytes = self.broker_metadata.?.mapped_bytes,
            .routing_registry = &self.routing_registry.?,
            .fc_region = self.broker_metadata.?.getFlowControlRegion(),
            .fc_enabled = self.config.fc_enabled,
            .fc_low_watermark_pct = self.config.fc_low_watermark_pct,
            .fc_high_watermark_pct = self.config.fc_high_watermark_pct,
            .fc_check_interval_ms = self.config.fc_check_interval_ms,
            .fc_refresh_interval_ms = self.config.fc_refresh_interval_ms,
            .fc_normal_refresh_interval_ms = self.config.fc_normal_refresh_interval_ms,
        });

        self.sender_loop = try SenderEventLoop.initWithDirectoryAndOptions(
            self.broker_metadata.?.getMutableSendBufferDirectory(),
            self.broker_metadata.?.mapped_bytes,
            &self.counters.?,
            self.config.node_id,
            self.allocator,
            self.config.group_name,
            self.config.benchmark_latency_tracing_enabled,
            .{
                .mtu = self.config.udp_mtu,
                .term_length = self.config.udp_term_length,
                .receiver_window_length = self.config.udp_receiver_window_length,
                .heartbeat_interval_ns = @as(i64, @intCast(self.config.udp_heartbeat_interval_ms)) * std.time.ns_per_ms,
                .setup_interval_ns = @as(i64, @intCast(self.config.udp_heartbeat_interval_ms)) * std.time.ns_per_ms,
            },
        );
        try self.sender_loop.?.configureEndpoint(self.config.local_host, 0);
        if (self.config.fc_peer_send_counters_enabled) {
            self.sender_loop.?.setPeerSendCountersRegion(
                self.broker_metadata.?.getPeerSendCountersRegion(),
            );
        }

        self.receiver_loop = ReceiverEventLoop.initWithGroupAndIoUring(
            &self.routing_registry.?,
            &self.counters.?,
            self.config.node_id,
            self.allocator,
            self.config.group_name,
            null,
            self.config.benchmark_latency_tracing_enabled,
            .{
                .mtu = self.config.udp_mtu,
                .term_length = self.config.udp_term_length,
                .receiver_window_length = self.config.udp_receiver_window_length,
                .max_message_length = self.config.messages_buffer_size / 8,
                .heartbeat_timeout_ns = @as(i64, @intCast(self.config.udp_session_timeout_ms)) * std.time.ns_per_ms,
                .nak_initial_delay_ns = @as(i64, @intCast(self.config.udp_nak_initial_delay_us)) * std.time.ns_per_us,
                .nak_retry_delay_ns = @as(i64, @intCast(self.config.udp_nak_retry_delay_us)) * std.time.ns_per_us,
                .status_interval_ns = @as(i64, @intCast(self.config.udp_heartbeat_interval_ms)) * std.time.ns_per_ms,
            },
        );
        for (self.config.peer_endpoints) |ep| {
            const addr = udp.Address.parseIp4(ep.host, ep.port) catch continue;
            self.sender_loop.?.addPeer(ep.node_id, addr) catch continue;
        }
        if (self.config.peer_endpoints.len > 0) {
            try self.receiver_loop.?.configureEndpoint(self.config.local_host, self.config.local_port);
        }

        self.broker_threads = BrokerThreads.init(
            platform.EventLoop{
                .context = @ptrCast(&self.control_loop.?),
                .doWorkFn = &ControlLoop.doWorkFn,
                .onCloseFn = &ControlLoop.onCloseFn,
            },
            platform.EventLoop{
                .context = @ptrCast(&self.sender_loop.?),
                .doWorkFn = &senderDoWorkFn,
                .onCloseFn = &senderOnCloseFn,
            },
            platform.EventLoop{
                .context = @ptrCast(&self.receiver_loop.?),
                .doWorkFn = &receiverDoWorkFn,
                .onCloseFn = &receiverOnCloseFn,
            },
            idleStrategyFromConfig(self.config.idle_strategy_name),
            idleStrategyFromConfig(self.config.idle_strategy_name),
            idleStrategyFromConfig(self.config.idle_strategy_name),
        );

        try self.broker_threads.?.start();
        self.started = true;
    }

    /// Stop worker threads if they are running.
    ///
    /// Safe to call multiple times.
    pub fn shutdown(self: *Self) void {
        if (!self.started) return;
        if (self.broker_threads) |*threads| {
            threads.shutdown();
        }
        self.started = false;
    }

    /// Release all owned resources.
    ///
    /// Call `shutdown()` before `deinit()` during normal lifecycle. `deinit()` is
    /// defensive and will stop threads first if needed.
    pub fn deinit(self: *Self) void {
        self.shutdown();

        if (self.sender_loop) |*sender_loop| {
            sender_loop.deinit();
        }
        self.sender_loop = null;
        if (self.receiver_loop) |*receiver_loop| {
            receiver_loop.deinit();
        }
        self.receiver_loop = null;
        self.control_loop = null;
        self.routing_registry = null;
        self.cluster_manager = null;
        self.broker_threads = null;

        self.control_rb = null;

        self.control_cmd_queue = null;
        self.sender_cmd_queue = null;
        self.receiver_cmd_queue = null;

        if (self.control_cmd_buffer) |buf| {
            self.allocator.free(buf);
        }
        self.control_cmd_buffer = null;

        if (self.sender_cmd_buffer) |buf| {
            self.allocator.free(buf);
        }
        self.sender_cmd_buffer = null;

        if (self.receiver_cmd_buffer) |buf| {
            self.allocator.free(buf);
        }
        self.receiver_cmd_buffer = null;

        self.counters = null;

        if (self.broker_metadata) |*meta| {
            meta.close();
        }
        self.broker_metadata = null;
    }

    pub fn isRunning(self: *const Self) bool {
        return self.started;
    }

    fn commandQueueCapacity() usize {
        return 128;
    }

    fn idleStrategyFromConfig(name: IdleStrategyName) platform.IdleStrategy {
        return switch (name) {
            .busy_spin => .busy_spin,
            .yielding => .yielding,
            .sleeping => .sleeping,
            .backoff => .{ .backoff = .{} },
            .blocking => .sleeping,
        };
    }
};

// ── EventLoop thunk functions ─────────────────────────────────────────
// SenderEventLoop and ReceiverEventLoop expose instance methods (doWork, deinit)
// but not static anyopaque thunks. These free functions bridge the gap.

fn senderDoWorkFn(ctx: *anyopaque) u32 {
    const loop: *SenderEventLoop = @ptrCast(@alignCast(ctx));
    return loop.doWork();
}

fn senderOnCloseFn(_: *anyopaque) void {}

fn receiverDoWorkFn(ctx: *anyopaque) u32 {
    const loop: *ReceiverEventLoop = @ptrCast(@alignCast(ctx));
    return loop.doWork();
}

fn receiverOnCloseFn(_: *anyopaque) void {}

test "BrokerRuntime init and deinit without start" {
    // Given
    const allocator = std.testing.allocator;
    const peers = [_]config_mod.PeerEndpoint{};
    const config = BrokerConfig{
        .node_id = 1,
        .local_host = "127.0.0.1",
        .local_port = 19001,
        .peer_endpoints = peers[0..],
        .group_name = "ringloom-test-runtime-init",
        .storage_path = "/tmp",
        .counter_metadata_buffer_size = 4096,
    };

    // When
    var runtime = try BrokerRuntime.init(allocator, config);
    defer runtime.deinit();

    // Then — init creates metadata, counters, and the control ring; event loops are deferred to start()
    try std.testing.expect(!runtime.isRunning());
    try std.testing.expect(runtime.broker_metadata != null);
    try std.testing.expect(runtime.counters != null);
    try std.testing.expect(runtime.control_rb != null);
    try std.testing.expect(runtime.broker_metadata.?.getSendBufferDirectory().entries.len > 0);
}

test "BrokerRuntime start and shutdown" {
    // Given
    const allocator = std.testing.allocator;
    const peers = [_]config_mod.PeerEndpoint{};
    const config = BrokerConfig{
        .node_id = 1,
        .local_host = "127.0.0.1",
        .local_port = 19002,
        .peer_endpoints = peers[0..],
        .group_name = "ringloom-test-runtime-start",
        .storage_path = "/tmp",
        .counter_metadata_buffer_size = 4096,
    };

    var runtime = try BrokerRuntime.init(allocator, config);
    defer runtime.deinit();

    // When
    try runtime.start();

    // Then
    try std.testing.expect(runtime.isRunning());

    // When
    runtime.shutdown();

    // Then
    try std.testing.expect(!runtime.isRunning());
}
