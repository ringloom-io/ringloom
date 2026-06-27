const std = @import("std");
const ringloom_aeron = @import("ringloom_aeron");

const ringloom_common = @import("ringloom_common");
const config_mod = @import("ringloom_common").config.broker_config;
const config_loader_mod = ringloom_common.config.config_loader;
const platform = ringloom_common.platform;
const memory = ringloom_common.memory;
const concurrent = ringloom_common.concurrent;
const control = @import("../control.zig");
const cluster = @import("../cluster.zig");
const receiver = @import("../receiver.zig");
const threading = @import("../threading.zig");
const broker_aeron = @import("../aeron.zig");

const BrokerConfig = config_mod.BrokerConfig;
const ConfigLoader = config_loader_mod.ConfigLoader;
const ThreadRunner = platform.ThreadRunner;
const EventLoop = platform.EventLoop;
const IdleStrategy = platform.IdleStrategy;
const BrokerMetadataFile = memory.BrokerMetadataFile;
const RingBuffer = concurrent.ring_buffer.RingBuffer;
const CountersManager = concurrent.counters.CountersManager;
const ControlLoop = control.ControlLoop;
const ClusterManager = cluster.ClusterManager;
const SenderEventLoop = @import("../sender.zig").SenderEventLoop;
const ReceiverEventLoop = receiver.ReceiverEventLoop;
const BrokerThreads = threading.BrokerThreads;
const CompositeEventLoop = threading.CompositeEventLoop;
const RoutingRegistry = receiver.ServiceRegistry;
const CommandQueue = ringloom_common.concurrent.command_queue.CommandQueue;
const Command = ringloom_common.concurrent.command_queue.Command;
const admin_dispatch = cluster.admin_dispatch;
const AdminCommandQueue = admin_dispatch.AdminCommandQueue;
const BrokerAeronClient = broker_aeron.BrokerAeronClient;
const BrokerUdpTransport = broker_aeron.BrokerUdpTransport;
const topics = @import("../topics.zig");

pub const ExitCode = enum(u8) {
    success = 0,
    usage_error = 1,
    config_error = 2,
    startup_error = 3,
    runtime_error = 4,
};

/// Global signal flag for SIGTERM — async-signal-safe.
var g_shutdown_signal: std.atomic.Value(bool) = std.atomic.Value(bool).init(false);

pub const BrokerApplication = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    config: BrokerConfig,

    broker_metadata: ?BrokerMetadataFile = null,

    control_ring_buffer: ?RingBuffer = null,

    counters: ?CountersManager = null,

    control_command_storage: ?[]Command = null,
    control_command_queue: ?CommandQueue = null,

    admin_cmd_queue: ?AdminCommandQueue(64) = null,
    peer_node_ids: ?[]u8 = null,

    routing_registry: ?RoutingRegistry = null,
    cluster_manager: ?ClusterManager = null,
    control_loop: ?ControlLoop = null,
    sender_loop: ?SenderEventLoop = null,
    receiver_loop: ?ReceiverEventLoop = null,

    broker_threads: ?BrokerThreads = null,
    network_composite_loop: ?CompositeEventLoop = null,
    shared_composite_loop: ?CompositeEventLoop = null,

    aeron_directory_buf: [config_mod.max_aeron_directory_length]u8 = undefined,
    aeron_driver: ?ringloom_aeron.Driver = null,
    aeron_driver_agents: ?ringloom_aeron.DriverAgents = null,
    aeron_client: ?BrokerAeronClient = null,
    aeron_udp_transport: ?BrokerUdpTransport = null,

    /// Persistent topics subsystem (null when topics are disabled).
    topic_subsystem: ?*topics.TopicSubsystem = null,

    /// Adapter that bridges topic replication channels to Aeron (owned, stable pointer).
    repl_publisher_adapter: broker_aeron.ReplPublisherAdapter = .{},

    started: bool = false,
    shutdown_requested: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, config: BrokerConfig, io: std.Io) !Self {
        var self = Self{
            .allocator = allocator,
            .io = io,
            .config = config,
        };

        try self.bootstrap();
        return self;
    }

    pub fn initFromDefaultConfig(
        allocator: std.mem.Allocator,
        io: std.Io,
        environ_map: ?*const std.process.Environ.Map,
    ) !Self {
        var loader = ConfigLoader.init(allocator, io, environ_map);
        const config = try loader.load();
        return Self.init(allocator, config, io);
    }

    pub fn initFromConfigFile(
        allocator: std.mem.Allocator,
        io: std.Io,
        path: []const u8,
    ) !Self {
        var loader = ConfigLoader.init(allocator, io, null);
        const config = try loader.loadFromFile(path);
        return Self.init(allocator, config, io);
    }

    pub fn deinit(self: *Self) void {
        if (self.started) {
            self.shutdown();
        }

        // After shutdown(), threads have stopped and their onClose callbacks
        // have already run (ControlLoop.onClose cleans up its own registry).
        // We only clean up shared resources here — never re-call onClose/deinit
        // on event loops that threads already finalized.

        if (self.receiver_loop) |*loop| {
            loop.deinit();
            self.receiver_loop = null;
        }
        if (self.sender_loop) |*loop| {
            loop.deinit();
            self.sender_loop = null;
        }

        self.control_loop = null;
        self.network_composite_loop = null;
        self.shared_composite_loop = null;
        self.broker_threads = null;
        self.stopAeronUdpTransport();
        self.stopAeronClient();
        self.stopAeronDriver();

        if (self.topic_subsystem) |ts| {
            ts.deinit();
            self.topic_subsystem = null;
        }

        self.cluster_manager = null;
        self.routing_registry = null;

        self.control_command_queue = null;

        self.admin_cmd_queue = null;

        if (self.peer_node_ids) |ids| {
            self.allocator.free(ids);
            self.peer_node_ids = null;
        }

        if (self.control_command_storage) |storage| {
            self.allocator.free(storage);
            self.control_command_storage = null;
        }

        self.counters = null;

        if (self.broker_metadata) |*meta| {
            meta.close();
            self.broker_metadata = null;
        }
    }

    pub fn run(self: *Self) !u8 {
        // Validate configuration
        if (self.config.node_id == 0) {
            std.log.err("invalid configuration: node_id must be > 0", .{});
            return @intFromEnum(ExitCode.config_error);
        }

        self.installSignalHandler();

        try self.start();
        defer self.shutdown();

        self.logStartup();

        while (!self.shutdown_requested.load(.acquire) and
            !g_shutdown_signal.load(.acquire))
        {
            std.Io.sleep(self.io, .fromMilliseconds(50), .awake) catch unreachable;
        }

        return @intFromEnum(ExitCode.success);
    }

    /// Create event loops and start worker threads.
    ///
    /// Event loops are created here (not in bootstrap/init) because `self`
    /// is now at its final address, so internal pointers remain valid.
    pub fn start(self: *Self) !void {
        if (self.started) return;

        try self.startAeronDriver();
        errdefer self.stopAeronDriver();
        try self.startAeronClient();
        errdefer self.stopAeronClient();
        try self.startAeronUdpTransport();
        errdefer self.stopAeronUdpTransport();

        // ── Persistent topics subsystem ────────────────────────────
        // Must come after Aeron transport (for repl publications) and
        // before control/receiver loops (so they can reference it).
        const topics_cfg = self.config.topics;
        if (topics_cfg.enabled) {
            var topics_path_buf: [512]u8 = undefined;
            const topics_path = try topics_cfg.resolvePath(self.config.storage_path, &topics_path_buf);

            // Bind the repl publisher adapter to the Aeron transport.
            // Single-node brokers may not have a UDP transport; the adapter
            // handles the null transport gracefully (isConnected returns false).
            if (self.aeron_udp_transport) |*transport_ref| {
                self.repl_publisher_adapter.bind(transport_ref);
            }
            const repl_pub = self.repl_publisher_adapter.publisher();

            self.topic_subsystem = try topics.TopicSubsystem.init(self.allocator, .{
                .enabled = true,
                .base_dir = topics_path,
                .write_runway_bytes = topics_cfg.write_runway_bytes,
                .read_runway_bytes = topics_cfg.read_runway_bytes,
                .prefetcher_cpu_affinity = topics_cfg.prefetcher_cpu_affinity,
                .local_node_id = self.config.node_id,
                .io = self.io,
            }, repl_pub, platform.Clock.monotonicNanos());
            errdefer self.topic_subsystem.?.deinit();

            try self.topic_subsystem.?.start();
            errdefer self.topic_subsystem.?.prefetcher.stop();
        }

        const aeron_assignment = broker_aeron.assignAgents(&self.aeron_driver_agents.?);
        var local_host_port_buf: [64]u8 = undefined;
        const local_host_port = cluster.admin_messages.padHostPort(try std.fmt.bufPrint(
            &local_host_port_buf,
            "{s}:{}",
            .{ self.config.local_host, self.config.local_port },
        ));

        self.control_loop = ControlLoop.init(.{
            .control_rb = &self.control_ring_buffer.?,
            .cmd_queue = &self.control_command_queue.?,
            .cluster_manager = &self.cluster_manager.?,
            .counters = &self.counters.?,
            .local_node_id = self.config.node_id,
            .storage_path = self.config.storage_path,
            .group = self.config.group_name,
            .allocator = self.allocator,
            .admin_cmd_queue = &self.admin_cmd_queue.?,
            .broker_udp_transport = if (self.aeron_udp_transport) |*transport_ref| transport_ref else null,
            .peer_node_ids = self.peer_node_ids orelse &.{},
            .local_host_and_port = local_host_port,
            .routing_registry = &self.routing_registry.?,
            .fc_region = self.broker_metadata.?.getFlowControlRegion(),
            .fc_enabled = self.config.fc_enabled,
            .fc_low_watermark_pct = self.config.fc_low_watermark_pct,
            .fc_high_watermark_pct = self.config.fc_high_watermark_pct,
            .fc_check_interval_ms = self.config.fc_check_interval_ms,
            .fc_refresh_interval_ms = self.config.fc_refresh_interval_ms,
            .fc_normal_refresh_interval_ms = self.config.fc_normal_refresh_interval_ms,
            .aeron_agent = aeron_assignment.control,
            .topic_subsystem = self.topic_subsystem,
            .topic_ack_feedback_interval_ns = @as(i64, @intCast(topics_cfg.ack_feedback_interval_us)) * std.time.ns_per_us,
        });

        self.sender_loop = SenderEventLoop.init();
        self.sender_loop.?.setAeronAgent(aeron_assignment.sender);

        self.receiver_loop = ReceiverEventLoop.initWithAeron(
            &self.routing_registry.?,
            &self.counters.?,
            self.config.node_id,
            &self.admin_cmd_queue.?,
            self.config.benchmark_latency_tracing_enabled,
            .{
                .broker_udp_transport = if (self.aeron_udp_transport) |*transport_ref| transport_ref else null,
                .max_data_payload_length = self.config.max_frame_length,
                .peer_node_ids = self.peer_node_ids orelse &.{},
            },
        );
        self.receiver_loop.?.setAeronAgent(aeron_assignment.receiver);

        // Wire persistent topics into the receiver loop (spec 05).
        if (self.topic_subsystem) |ts| {
            self.receiver_loop.?.setTopicSubsystem(ts);
        }

        try self.initBrokerThreads();
        self.broker_threads.?.setCpuAffinities(
            self.config.sender_cpu_affinity,
            self.config.receiver_cpu_affinity,
        );

        try self.broker_threads.?.start();
        self.started = true;
    }

    pub fn shutdown(self: *Self) void {
        self.requestShutdown();

        if (self.broker_threads) |*threads| {
            threads.shutdown();
            self.broker_threads = null;
        }

        self.started = false;
    }

    fn startAeronDriver(self: *Self) !void {
        if (self.aeron_driver != null) return;

        const directory = try self.config.buildAeronDirectory(&self.aeron_directory_buf);
        const mode = toAeronThreadingMode(self.config.aeron_threading_mode);

        self.aeron_driver = try ringloom_aeron.Driver.initEmbedded(.{
            .directory = directory,
            .delete_dir_on_start = self.config.aeron_delete_directory_on_start,
            .delete_dir_on_shutdown = self.config.aeron_delete_directory_on_shutdown,
            .term_buffer_length = @intCast(self.config.aeron_udp_term_length),
            .ipc_term_buffer_length = @intCast(self.config.aeron_ipc_term_length),
            .mtu_length = @intCast(self.config.aeron_mtu_length),
            .ipc_mtu_length = @intCast(self.config.aeron_ipc_mtu_length),
            .term_buffer_sparse_file = self.config.aeron_sparse_files,
            .publication_linger_timeout_ns = self.config.aeron_publication_linger_timeout_ns,
            .client_liveness_timeout_ns = self.config.aeron_client_liveness_timeout_ns,
            .network_publication_max_messages_per_send = self.config.aeron_network_publication_max_messages_per_send,
        }, mode);
        errdefer {
            self.aeron_driver.?.deinit();
            self.aeron_driver = null;
        }

        self.aeron_driver_agents = try self.aeron_driver.?.agents(mode);
    }

    fn stopAeronDriver(self: *Self) void {
        self.aeron_driver_agents = null;
        if (self.aeron_driver) |*driver| {
            driver.deinit();
        }
        self.aeron_driver = null;
    }

    fn startAeronClient(self: *Self) !void {
        if (self.aeron_client != null) return;

        const directory = try self.config.buildAeronDirectory(&self.aeron_directory_buf);
        self.aeron_client = try BrokerAeronClient.open(directory);
    }

    fn stopAeronClient(self: *Self) void {
        if (self.aeron_client) |*client| {
            client.deinit();
        }
        self.aeron_client = null;
    }

    fn startAeronUdpTransport(self: *Self) !void {
        if (self.aeron_udp_transport != null) return;
        if (self.config.peer_endpoints.len == 0) return;

        self.aeron_udp_transport = try BrokerUdpTransport.open(
            self.allocator,
            &self.config,
            &self.aeron_client.?.client,
            &self.aeron_driver_agents.?,
        );
    }

    fn stopAeronUdpTransport(self: *Self) void {
        if (self.aeron_udp_transport) |*transport_ref| {
            transport_ref.deinit();
        }
        self.aeron_udp_transport = null;
    }

    fn initBrokerThreads(self: *Self) !void {
        const control_event = EventLoop{
            .context = @ptrCast(&self.control_loop.?),
            .doWorkFn = &ControlLoop.doWorkFn,
            .onCloseFn = &ControlLoop.onCloseFn,
        };
        const receiver_event = EventLoop{
            .context = @ptrCast(&self.receiver_loop.?),
            .doWorkFn = &receiverDoWorkFn,
            .onCloseFn = &receiverOnCloseFn,
        };
        const sender_event = EventLoop{
            .context = @ptrCast(&self.sender_loop.?),
            .doWorkFn = &senderDoWorkFn,
            .onCloseFn = &senderOnCloseFn,
        };
        const idle = toIdleStrategy(self.config.idle_strategy_name);

        switch (toBrokerThreadingMode(self.config.threading_mode)) {
            .dedicated => {
                self.broker_threads = BrokerThreads.initDedicated(
                    control_event,
                    sender_event,
                    receiver_event,
                    idle,
                    toIdleStrategy(self.config.idle_strategy_name),
                    toIdleStrategy(self.config.idle_strategy_name),
                );
            },
            .shared_network => {
                self.network_composite_loop = .{
                    .first = sender_event,
                    .second = receiver_event,
                };
                self.broker_threads = BrokerThreads.initSharedNetwork(
                    control_event,
                    self.network_composite_loop.?.eventLoop(),
                    idle,
                    toIdleStrategy(self.config.idle_strategy_name),
                );
            },
            .shared => {
                self.network_composite_loop = .{
                    .first = sender_event,
                    .second = receiver_event,
                };
                self.shared_composite_loop = .{
                    .first = self.network_composite_loop.?.eventLoop(),
                    .second = control_event,
                };
                self.broker_threads = BrokerThreads.initShared(
                    self.shared_composite_loop.?.eventLoop(),
                    idle,
                );
            },
        }
    }

    pub fn requestShutdown(self: *Self) void {
        self.shutdown_requested.store(true, .release);
    }

    pub fn isRunning(self: *const Self) bool {
        return self.started and !self.shutdown_requested.load(.acquire);
    }

    /// Allocate shared resources: metadata, ring buffers, counters, command
    /// queues, registries.  Event loops are NOT created here — they are
    /// deferred to `start()` so that `self` is at its final address and
    /// internal pointers (e.g. `&self.control_ring_buffer.?`) remain valid.
    fn bootstrap(self: *Self) !void {
        var peer_discovery_buf: [memory.constants.default_max_peers]memory.BrokerAeronPeerConfig = undefined;
        var peer_channel_bufs: [memory.constants.default_max_peers][memory.constants.max_aeron_channel_length]u8 = undefined;
        const peer_discovery = try broker_aeron.buildPeerDiscovery(
            &peer_discovery_buf,
            &peer_channel_bufs,
            &self.config,
        );

        var broker_metadata = try BrokerMetadataFile.createWithFlowControl(
            self.config.storage_path,
            self.config.group_name,
            self.config.node_id,
            self.config.control_buffer_size,
            self.config.messages_buffer_size,
            .{
                .fc_max_entries = if (self.config.fc_enabled) self.config.fc_max_entries else 0,
                .counter_values_buffer_length = self.config.counter_values_buffer_size,
                .counter_metadata_buffer_length = self.config.counter_metadata_buffer_size,
                .error_log_buffer_length = self.config.error_log_buffer_size,
                .aeron_directory = try self.config.buildAeronDirectory(&self.aeron_directory_buf),
                .broker_ingress_stream_id = 0,
                .admin_stream_base = self.config.aeron_admin_stream_base,
                .data_stream_base = self.config.aeron_data_stream_base,
                .peer_data_channels = peer_discovery,
            },
        );
        errdefer broker_metadata.close();

        self.control_ring_buffer = try RingBuffer.init(
            @alignCast(broker_metadata.getControlBuffer()),
            false,
            null,
            null,
        );

        self.counters = CountersManager.init(
            broker_metadata.getCounterValuesBuffer(),
            broker_metadata.getCounterMetadataBuffer(),
        );

        const command_capacity = commandQueueCapacity();
        const command_storage = try self.allocator.alloc(Command, command_capacity);
        errdefer self.allocator.free(command_storage);

        self.control_command_queue = CommandQueue.init(command_storage);

        self.admin_cmd_queue = .{};

        // Build peer_node_ids slice from config.
        if (self.config.peer_endpoints.len > 0) {
            const ids = try self.allocator.alloc(u8, self.config.peer_endpoints.len);
            for (self.config.peer_endpoints, 0..) |ep, i| {
                ids[i] = ep.node_id;
            }
            self.peer_node_ids = ids;
        }

        self.routing_registry = RoutingRegistry.init();
        self.cluster_manager = ClusterManager.initSingleNode(self.config.node_id);

        self.broker_metadata = broker_metadata;
        self.control_command_storage = command_storage;
    }

    /// Write the readiness marker to stdout (for the test harness) and
    /// a structured message to stderr (via std.log).
    fn logStartup(self: *const Self) void {
        // stdout marker — the e2e test harness polls stdout for "broker started".
        var buf: [4096]u8 = undefined;
        var stdout_w = std.Io.File.stdout().writer(self.io, &buf);
        const stdout = &stdout_w.interface;
        stdout.print("broker started: node_id={}, bind={s}:{}, group={s}\n", .{
            self.config.node_id,
            self.config.local_host,
            self.config.local_port,
            self.config.group_name,
        }) catch {};
        stdout.flush() catch {};

        // stderr structured log
        std.log.info(
            "broker started: node_id={}, bind={s}:{}, group={s}, storage={s}, threading={s}, peers={}",
            .{
                self.config.node_id,
                self.config.local_host,
                self.config.local_port,
                self.config.group_name,
                self.config.storage_path,
                @tagName(self.config.threading_mode),
                self.config.peer_endpoints.len,
            },
        );
    }

    fn installSignalHandler(self: *Self) void {
        _ = self;
        const act = std.posix.Sigaction{
            .handler = .{ .handler = sigterm_handler },
            .mask = std.posix.sigemptyset(),
            .flags = 0,
        };
        std.posix.sigaction(std.posix.SIG.TERM, &act, null);
        std.posix.sigaction(std.posix.SIG.INT, &act, null);
    }

    fn toIdleStrategy(name: config_mod.IdleStrategyName) IdleStrategy {
        return switch (name) {
            .busy_spin => .busy_spin,
            .yielding => .yielding,
            .sleeping => .sleeping,
            .backoff => .{ .backoff = .{} },
            .blocking => .sleeping,
        };
    }

    fn toAeronThreadingMode(mode: config_mod.ThreadingMode) ringloom_aeron.ThreadingMode {
        return switch (mode) {
            .dedicated => .dedicated,
            .shared_network => .shared_network,
            .shared => .shared,
        };
    }

    fn toBrokerThreadingMode(mode: config_mod.ThreadingMode) threading.ThreadingMode {
        return switch (mode) {
            .dedicated => .dedicated,
            .shared_network => .shared_network,
            .shared => .shared,
        };
    }
};

fn sigterm_handler(_: std.posix.SIG) callconv(.c) void {
    g_shutdown_signal.store(true, .release);
}

fn receiverDoWorkFn(ctx: *anyopaque) u32 {
    const loop: *ReceiverEventLoop = @ptrCast(@alignCast(ctx));
    return loop.doWork();
}

fn receiverOnCloseFn(_: *anyopaque) void {}

fn senderDoWorkFn(ctx: *anyopaque) u32 {
    const loop: *SenderEventLoop = @ptrCast(@alignCast(ctx));
    return loop.doWork();
}

fn senderOnCloseFn(_: *anyopaque) void {}

fn commandQueueCapacity() usize {
    return 128;
}

test "BrokerApplication initializes and can request shutdown" {
    const allocator = std.testing.allocator;

    const config = BrokerConfig{
        .node_id = 1,
        .local_host = "127.0.0.1",
        .local_port = 19001,
        .peer_endpoints = &.{},
        .group_name = "ringloom-test-app",
        .storage_path = "/tmp",
    };

    var app = try BrokerApplication.init(allocator, config, std.testing.io);
    defer app.deinit();

    try std.testing.expect(!app.shutdown_requested.load(.acquire));

    app.requestShutdown();

    try std.testing.expect(app.shutdown_requested.load(.acquire));
}

test "BrokerApplication starts embedded Aeron driver without worker threads" {
    const allocator = std.testing.allocator;

    const config = BrokerConfig{
        .node_id = 2,
        .local_host = "127.0.0.1",
        .local_port = 19002,
        .peer_endpoints = &.{},
        .group_name = "ringloom-test-app-aeron",
        .storage_path = "/tmp",
        .aeron_threading_mode = .shared,
        .threading_mode = .shared,
    };

    var app = try BrokerApplication.init(allocator, config, std.testing.io);
    defer app.deinit();

    try app.startAeronDriver();
    try std.testing.expect(app.aeron_driver != null);
    try std.testing.expect(app.aeron_driver_agents != null);
}
