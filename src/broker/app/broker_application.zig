const std = @import("std");
const udp = @import("ringloom_udp");

const ringloom_common = @import("ringloom_common");
const config_mod = @import("ringloom_common").config.broker_config;
const config_loader_mod = ringloom_common.config.config_loader;
const platform = ringloom_common.platform;
const memory = ringloom_common.memory;
const concurrent = ringloom_common.concurrent;
const control = @import("../control.zig");
const cluster = @import("../cluster.zig");
const sender = @import("../sender.zig");
const receiver = @import("../receiver.zig");
const threading = @import("../threading.zig");

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
const SenderEventLoop = sender.SenderEventLoop;
const ReceiverEventLoop = receiver.ReceiverEventLoop;
const BrokerThreads = threading.BrokerThreads;
const RoutingRegistry = receiver.ServiceRegistry;
const CommandQueue = ringloom_common.concurrent.command_queue.CommandQueue;
const Command = ringloom_common.concurrent.command_queue.Command;
const admin_dispatch = cluster.admin_dispatch;
const AdminCommandQueue = admin_dispatch.AdminCommandQueue;

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

        if (self.sender_loop) |*loop| {
            loop.deinit();
            self.sender_loop = null;
        }

        if (self.receiver_loop) |*loop| {
            loop.deinit();
            self.receiver_loop = null;
        }

        self.control_loop = null;
        self.broker_threads = null;

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
            .send_buffer_directory = self.broker_metadata.?.getMutableSendBufferDirectory(),
            .broker_mapped_bytes = self.broker_metadata.?.mapped_bytes,
            .peer_node_ids = self.peer_node_ids orelse &.{},
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
        errdefer {
            self.sender_loop.?.deinit();
            self.sender_loop = null;
        }

        self.receiver_loop = ReceiverEventLoop.initWithGroupAndIoUring(
            &self.routing_registry.?,
            &self.counters.?,
            self.config.node_id,
            self.allocator,
            self.config.group_name,
            &self.admin_cmd_queue.?,
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

        // Wire up reliable UDP peers and receiver endpoint.
        for (self.config.peer_endpoints) |ep| {
            const addr = udp.Address.parseIp4(ep.host, ep.port) catch continue;
            self.sender_loop.?.addPeer(ep.node_id, addr) catch continue;
        }
        if (self.config.peer_endpoints.len > 0) {
            try configureReceiverEndpoint(&self.receiver_loop.?, self.config);
        }

        self.broker_threads = BrokerThreads.init(
            EventLoop{
                .context = @ptrCast(&self.control_loop.?),
                .doWorkFn = &ControlLoop.doWorkFn,
                .onCloseFn = &ControlLoop.onCloseFn,
            },
            EventLoop{
                .context = @ptrCast(&self.sender_loop.?),
                .doWorkFn = &senderDoWorkFn,
                .onCloseFn = &senderOnCloseFn,
            },
            EventLoop{
                .context = @ptrCast(&self.receiver_loop.?),
                .doWorkFn = &receiverDoWorkFn,
                .onCloseFn = &receiverOnCloseFn,
            },
            toIdleStrategy(self.config.idle_strategy_name),
            toIdleStrategy(self.config.idle_strategy_name),
            toIdleStrategy(self.config.idle_strategy_name),
        );

        self.broker_threads.?.sender_runner.cpu_affinity = self.config.sender_cpu_affinity;
        self.broker_threads.?.receiver_runner.cpu_affinity = self.config.receiver_cpu_affinity;

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
        var broker_metadata = try BrokerMetadataFile.createWithFlowControl(
            self.config.storage_path,
            self.config.group_name,
            self.config.node_id,
            self.config.control_buffer_size,
            self.config.messages_buffer_size,
            .{
                .fc_max_entries = if (self.config.fc_enabled) self.config.fc_max_entries else 0,
                .peer_send_max_peers = if (self.config.fc_peer_send_counters_enabled)
                    self.config.fc_peer_send_counters_max_peers
                else
                    0,
                .send_buffer_entry_count = self.config.send_buffers_max_entries,
                .send_buffer_capacity = self.config.send_buffers_default_size,
                .counter_values_buffer_length = self.config.counter_values_buffer_size,
                .counter_metadata_buffer_length = self.config.counter_metadata_buffer_size,
                .error_log_buffer_length = self.config.error_log_buffer_size,
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
};

fn sigterm_handler(_: std.posix.SIG) callconv(.c) void {
    g_shutdown_signal.store(true, .release);
}

fn senderDoWorkFn(ctx: *anyopaque) u32 {
    const loop: *SenderEventLoop = @ptrCast(@alignCast(ctx));
    return loop.doWork();
}

// No-op: sender cleanup is handled by BrokerApplication.deinit()
fn senderOnCloseFn(_: *anyopaque) void {}

fn receiverDoWorkFn(ctx: *anyopaque) u32 {
    const loop: *ReceiverEventLoop = @ptrCast(@alignCast(ctx));
    return loop.doWork();
}

fn receiverOnCloseFn(_: *anyopaque) void {}

fn configureReceiverEndpoint(loop: *ReceiverEventLoop, config: BrokerConfig) !void {
    var interface_storage: [1][]const u8 = undefined;
    var interfaces: []const []const u8 = &.{};
    if (config.af_xdp_interface) |interface| {
        interface_storage[0] = interface;
        interfaces = &interface_storage;
    }

    try loop.configureEndpointWithConfig(.{
        .local_address = udp.Address.parseIp4(config.local_host, config.local_port) catch unreachable,
        .mtu = config.udp_mtu,
        .engine_mode = transportEngineMode(config.transport_engine),
        .af_xdp = .{
            .interfaces = interfaces,
            .ports = config.af_xdp_ports,
            .rx_queue = config.af_xdp_rx_queue,
            .umem_frame_count = config.af_xdp_umem_frame_count,
            .umem_frame_size = config.af_xdp_umem_frame_size,
        },
    });
}

fn transportEngineMode(engine: config_mod.TransportEngine) udp.endpoint.EngineMode {
    return switch (engine) {
        .posix => .posix,
        .prefer_af_xdp => .prefer_af_xdp,
        .require_af_xdp => .require_af_xdp,
    };
}

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
