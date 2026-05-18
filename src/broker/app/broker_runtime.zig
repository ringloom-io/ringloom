const std = @import("std");
const ringloom_aeron = @import("ringloom_aeron");

const ringloom_common = @import("ringloom_common");
const platform = ringloom_common.platform;
const memory = ringloom_common.memory;
const concurrent = ringloom_common.concurrent;
const control = @import("../control.zig");
const receiver = @import("../receiver.zig");
const cluster = @import("../cluster.zig");
const threading = @import("../threading.zig");
const broker_aeron = @import("../aeron.zig");
const config_mod = ringloom_common.config.broker_config;

const BrokerMetadataFile = memory.BrokerMetadataFile;
const CountersManager = concurrent.CountersManager;
const RingBuffer = concurrent.RingBuffer;
const CommandQueue = concurrent.CommandQueue;
const Command = concurrent.Command;
const ControlLoop = control.ControlLoop;
const SenderEventLoop = @import("../sender.zig").SenderEventLoop;
const ReceiverEventLoop = receiver.ReceiverEventLoop;
const RoutingRegistry = receiver.ServiceRegistry;
const ClusterManager = cluster.ClusterManager;
const BrokerThreads = threading.BrokerThreads;
const CompositeEventLoop = threading.CompositeEventLoop;
const BrokerConfig = config_mod.BrokerConfig;
const IdleStrategyName = config_mod.IdleStrategyName;
const BrokerAeronClient = broker_aeron.BrokerAeronClient;
const BrokerUdpTransport = broker_aeron.BrokerUdpTransport;

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
    control_cmd_queue: ?CommandQueue,
    admin_cmd_queue: ?cluster.admin_dispatch.AdminCommandQueue(64),
    peer_node_ids: ?[]u8,

    cluster_manager: ?ClusterManager,
    routing_registry: ?RoutingRegistry,

    // Ring buffers stored as fields so event loop pointers remain valid.
    control_rb: ?RingBuffer,

    control_loop: ?ControlLoop,
    sender_loop: ?SenderEventLoop,
    receiver_loop: ?ReceiverEventLoop,
    broker_threads: ?BrokerThreads,
    network_composite_loop: ?CompositeEventLoop,
    shared_composite_loop: ?CompositeEventLoop,

    aeron_directory_buf: [config_mod.max_aeron_directory_length]u8,
    aeron_driver: ?ringloom_aeron.Driver,
    aeron_driver_agents: ?ringloom_aeron.DriverAgents,
    aeron_client: ?BrokerAeronClient,
    aeron_udp_transport: ?BrokerUdpTransport,

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
            .control_cmd_queue = null,
            .admin_cmd_queue = null,
            .peer_node_ids = null,
            .cluster_manager = null,
            .routing_registry = null,
            .control_rb = null,
            .control_loop = null,
            .sender_loop = null,
            .receiver_loop = null,
            .broker_threads = null,
            .network_composite_loop = null,
            .shared_composite_loop = null,
            .aeron_directory_buf = undefined,
            .aeron_driver = null,
            .aeron_driver_agents = null,
            .aeron_client = null,
            .aeron_udp_transport = null,
            .started = false,
        };
        errdefer self.deinit();

        var peer_discovery_buf: [memory.constants.default_max_peers]memory.BrokerAeronPeerConfig = undefined;
        var peer_channel_bufs: [memory.constants.default_max_peers][memory.constants.max_aeron_channel_length]u8 = undefined;
        const peer_discovery = try broker_aeron.buildPeerDiscovery(
            &peer_discovery_buf,
            &peer_channel_bufs,
            &config,
        );

        const broker_metadata = try BrokerMetadataFile.createWithFlowControl(
            config.storage_path,
            config.group_name,
            config.node_id,
            config.control_buffer_size,
            config.messages_buffer_size,
            .{
                .fc_max_entries = if (config.fc_enabled) config.fc_max_entries else 0,
                .counter_values_buffer_length = config.counter_values_buffer_size,
                .counter_metadata_buffer_length = config.counter_metadata_buffer_size,
                .error_log_buffer_length = config.error_log_buffer_size,
                .aeron_directory = try config.buildAeronDirectory(&self.aeron_directory_buf),
                .broker_ingress_stream_id = 0,
                .admin_stream_base = config.aeron_admin_stream_base,
                .data_stream_base = config.aeron_data_stream_base,
                .peer_data_channels = peer_discovery,
            },
        );
        self.broker_metadata = broker_metadata;

        self.counters = CountersManager.init(
            self.broker_metadata.?.getCounterValuesBuffer(),
            self.broker_metadata.?.getCounterMetadataBuffer(),
        );

        const control_cmd_buffer = try allocator.alloc(Command, commandQueueCapacity());
        self.control_cmd_buffer = control_cmd_buffer;

        self.control_cmd_queue = CommandQueue.init(control_cmd_buffer);
        self.admin_cmd_queue = .{};

        if (config.peer_endpoints.len > 0) {
            const ids = try allocator.alloc(u8, config.peer_endpoints.len);
            errdefer allocator.free(ids);
            for (config.peer_endpoints, 0..) |ep, i| {
                ids[i] = ep.node_id;
            }
            self.peer_node_ids = ids;
        }

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

        try self.startAeronDriver();
        errdefer self.stopAeronDriver();
        try self.startAeronClient();
        errdefer self.stopAeronClient();
        try self.startAeronUdpTransport();
        errdefer self.stopAeronUdpTransport();

        const aeron_assignment = broker_aeron.assignAgents(&self.aeron_driver_agents.?);
        var local_host_port_buf: [64]u8 = undefined;
        const local_host_port = cluster.admin_messages.padHostPort(try std.fmt.bufPrint(
            &local_host_port_buf,
            "{s}:{}",
            .{ self.config.local_host, self.config.local_port },
        ));

        self.control_loop = ControlLoop.init(.{
            .control_rb = &self.control_rb.?,
            .cmd_queue = &self.control_cmd_queue.?,
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
        });

        self.sender_loop = SenderEventLoop.init();
        self.sender_loop.?.setAeronAgent(aeron_assignment.sender);

        self.receiver_loop = ReceiverEventLoop.initWithAeron(
            &self.routing_registry.?,
            &self.counters.?,
            self.config.node_id,
            &self.admin_cmd_queue.?,
            false,
            .{
                .broker_udp_transport = if (self.aeron_udp_transport) |*transport_ref| transport_ref else null,
                .max_data_payload_length = self.config.max_frame_length,
                .peer_node_ids = self.peer_node_ids orelse &.{},
            },
        );
        self.receiver_loop.?.setAeronAgent(aeron_assignment.receiver);

        self.initBrokerThreads();
        self.broker_threads.?.setCpuAffinities(
            self.config.sender_cpu_affinity,
            self.config.receiver_cpu_affinity,
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

        if (self.receiver_loop) |*receiver_loop| {
            receiver_loop.deinit();
        }
        self.receiver_loop = null;
        if (self.sender_loop) |*sender_loop| {
            sender_loop.deinit();
        }
        self.sender_loop = null;
        self.control_loop = null;
        self.network_composite_loop = null;
        self.shared_composite_loop = null;
        self.routing_registry = null;
        self.cluster_manager = null;
        self.broker_threads = null;
        self.stopAeronUdpTransport();
        self.stopAeronClient();
        self.stopAeronDriver();

        self.control_rb = null;

        self.control_cmd_queue = null;
        self.admin_cmd_queue = null;

        if (self.peer_node_ids) |ids| {
            self.allocator.free(ids);
        }
        self.peer_node_ids = null;

        if (self.control_cmd_buffer) |buf| {
            self.allocator.free(buf);
        }
        self.control_cmd_buffer = null;

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

    fn initBrokerThreads(self: *Self) void {
        const control_event = platform.EventLoop{
            .context = @ptrCast(&self.control_loop.?),
            .doWorkFn = &ControlLoop.doWorkFn,
            .onCloseFn = &ControlLoop.onCloseFn,
        };
        const receiver_event = platform.EventLoop{
            .context = @ptrCast(&self.receiver_loop.?),
            .doWorkFn = &receiverDoWorkFn,
            .onCloseFn = &receiverOnCloseFn,
        };
        const sender_event = platform.EventLoop{
            .context = @ptrCast(&self.sender_loop.?),
            .doWorkFn = &senderDoWorkFn,
            .onCloseFn = &senderOnCloseFn,
        };
        const idle = idleStrategyFromConfig(self.config.idle_strategy_name);

        switch (toBrokerThreadingMode(self.config.threading_mode)) {
            .dedicated => {
                self.broker_threads = BrokerThreads.initDedicated(
                    control_event,
                    sender_event,
                    receiver_event,
                    idle,
                    idleStrategyFromConfig(self.config.idle_strategy_name),
                    idleStrategyFromConfig(self.config.idle_strategy_name),
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
                    idleStrategyFromConfig(self.config.idle_strategy_name),
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

// ── EventLoop thunk functions ─────────────────────────────────────────
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

    // Then — init creates metadata, counters, and control ring; event loops are deferred to start()
    try std.testing.expect(!runtime.isRunning());
    try std.testing.expect(runtime.broker_metadata != null);
    try std.testing.expect(runtime.counters != null);
    try std.testing.expect(runtime.control_rb != null);
    try std.testing.expect(runtime.aeron_driver == null);
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
        .threading_mode = .shared,
        .aeron_threading_mode = .shared,
    };

    var runtime = try BrokerRuntime.init(allocator, config);
    defer runtime.deinit();

    // When
    try runtime.start();

    // Then
    try std.testing.expect(runtime.isRunning());
    try std.testing.expect(runtime.aeron_driver != null);

    // When
    runtime.shutdown();

    // Then
    try std.testing.expect(!runtime.isRunning());
}
