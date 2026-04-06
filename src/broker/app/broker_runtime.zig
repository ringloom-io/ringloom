const std = @import("std");

const brz_common = @import("brz_common");
const platform = brz_common.platform;
const memory = brz_common.memory;
const concurrent = brz_common.concurrent;
const control = @import("../control.zig");
const sender = @import("../sender.zig");
const receiver = @import("../receiver.zig");
const cluster = @import("../cluster.zig");
const threading = @import("../threading.zig");
const config_mod = brz_common.config.broker_config;

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
    counters_values_buffer: ?[]align(platform.constants.cache_line_pad) u8,
    counters_metadata_buffer: ?[]u8,
    counters: ?CountersManager,

    control_cmd_buffer: ?[]Command,
    sender_cmd_buffer: ?[]Command,
    receiver_cmd_buffer: ?[]Command,

    control_cmd_queue: ?CommandQueue,
    sender_cmd_queue: ?CommandQueue,
    receiver_cmd_queue: ?CommandQueue,

    cluster_manager: ?ClusterManager,
    routing_registry: ?RoutingRegistry,
    control_loop: ?ControlLoop,
    sender_loop: ?SenderEventLoop,
    receiver_loop: ?ReceiverEventLoop,
    broker_threads: ?BrokerThreads,

    started: bool,

    const Self = @This();

    /// Initialize broker-owned resources but do not start worker threads yet.
    pub fn init(allocator: std.mem.Allocator, config: BrokerConfig) !Self {
        var self = Self{
            .allocator = allocator,
            .config = config,
            .broker_metadata = null,
            .counters_values_buffer = null,
            .counters_metadata_buffer = null,
            .counters = null,
            .control_cmd_buffer = null,
            .sender_cmd_buffer = null,
            .receiver_cmd_buffer = null,
            .control_cmd_queue = null,
            .sender_cmd_queue = null,
            .receiver_cmd_queue = null,
            .cluster_manager = null,
            .routing_registry = null,
            .control_loop = null,
            .sender_loop = null,
            .receiver_loop = null,
            .broker_threads = null,
            .started = false,
        };
        errdefer self.deinit();

        const broker_metadata = try BrokerMetadataFile.create(
            config.storage_path,
            config.group_name,
            config.node_id,
            config.control_buffer_size,
            config.messages_buffer_size,
        );
        self.broker_metadata = broker_metadata;

        const values_buffer = try allocator.alignedAlloc(
            u8,
            @enumFromInt(std.math.log2(@as(usize, platform.constants.cache_line_pad))),
            config.counter_values_buffer_size,
        );
        errdefer allocator.free(values_buffer);
        @memset(values_buffer, 0);
        self.counters_values_buffer = values_buffer;

        const metadata_buffer = try allocator.alloc(
            u8,
            config.counter_metadata_buffer_size,
        );
        errdefer allocator.free(metadata_buffer);
        @memset(metadata_buffer, 0);
        self.counters_metadata_buffer = metadata_buffer;

        self.counters = CountersManager.init(values_buffer, metadata_buffer);

        const control_cmd_buffer = try allocator.alloc(Command, commandQueueCapacity());
        errdefer allocator.free(control_cmd_buffer);
        self.control_cmd_buffer = control_cmd_buffer;

        const sender_cmd_buffer = try allocator.alloc(Command, commandQueueCapacity());
        errdefer allocator.free(sender_cmd_buffer);
        self.sender_cmd_buffer = sender_cmd_buffer;

        const receiver_cmd_buffer = try allocator.alloc(Command, commandQueueCapacity());
        errdefer allocator.free(receiver_cmd_buffer);
        self.receiver_cmd_buffer = receiver_cmd_buffer;

        self.control_cmd_queue = CommandQueue.init(control_cmd_buffer);
        self.sender_cmd_queue = CommandQueue.init(sender_cmd_buffer);
        self.receiver_cmd_queue = CommandQueue.init(receiver_cmd_buffer);

        self.cluster_manager = ClusterManager.initSingleNode(config.node_id);
        self.routing_registry = RoutingRegistry.init();

        var control_rb = try RingBuffer.init(
            @alignCast(self.broker_metadata.?.getControlBuffer()),
            false,
            null,
            null,
        );

        var send_rb = try RingBuffer.init(
            @alignCast(self.broker_metadata.?.getSendBuffer()),
            false,
            null,
            null,
        );

        self.control_loop = ControlLoop.init(.{
            .control_rb = &control_rb,
            .cmd_queue = &self.control_cmd_queue.?,
            .cluster_manager = &self.cluster_manager.?,
            .counters = &self.counters.?,
            .local_node_id = config.node_id,
            .storage_path = config.storage_path,
            .group = config.group_name,
            .allocator = allocator,
        });

        self.sender_loop = try SenderEventLoop.init(
            &send_rb,
            &self.counters.?,
            config.node_id,
            allocator,
        );

        self.receiver_loop = ReceiverEventLoop.init(
            &self.routing_registry.?,
            &self.counters.?,
            config.node_id,
        );

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
            idleStrategyFromConfig(config.idle_strategy_name),
            idleStrategyFromConfig(config.idle_strategy_name),
            idleStrategyFromConfig(config.idle_strategy_name),
        );

        return self;
    }

    /// Start broker worker threads according to the configured threading model.
    ///
    /// For now, this runtime uses the dedicated three-thread broker model already
    /// implemented by `BrokerThreads`.
    pub fn start(self: *Self) !void {
        if (self.started) return error.AlreadyStarted;
        if (self.broker_threads == null) return error.NotInitialized;

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
        self.receiver_loop = null;
        self.control_loop = null;
        self.routing_registry = null;
        self.cluster_manager = null;
        self.broker_threads = null;

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

        if (self.counters_metadata_buffer) |buf| {
            self.allocator.free(buf);
        }
        self.counters_metadata_buffer = null;

        if (self.counters_values_buffer) |buf| {
            self.allocator.free(buf);
        }
        self.counters_values_buffer = null;

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

fn senderOnCloseFn(ctx: *anyopaque) void {
    const loop: *SenderEventLoop = @ptrCast(@alignCast(ctx));
    loop.deinit();
}

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
        .group_name = "brz-test-runtime-init",
        .storage_path = "/tmp",
        .counter_metadata_buffer_size = 4096,
    };

    // When
    var runtime = try BrokerRuntime.init(allocator, config);
    defer runtime.deinit();

    // Then
    try std.testing.expect(!runtime.isRunning());
    try std.testing.expect(runtime.broker_metadata != null);
    try std.testing.expect(runtime.counters != null);
    try std.testing.expect(runtime.control_loop != null);
    try std.testing.expect(runtime.sender_loop != null);
    try std.testing.expect(runtime.receiver_loop != null);
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
        .group_name = "brz-test-runtime-start",
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
