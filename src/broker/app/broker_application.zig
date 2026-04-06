const std = @import("std");

const brz_common = @import("brz_common");
const config_mod = @import("brz_common").config.broker_config;
const config_loader_mod = brz_common.config.config_loader;
const platform = brz_common.platform;
const memory = brz_common.memory;
const concurrent = brz_common.concurrent;
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
const ServiceRegistry = control.ServiceRegistry;
const RoutingRegistry = receiver.ServiceRegistry;
const CommandQueue = brz_common.concurrent.command_queue.CommandQueue;
const Command = brz_common.concurrent.command_queue.Command;

pub const ExitCode = enum(u8) {
    success = 0,
    usage_error = 1,
    config_error = 2,
    startup_error = 3,
    runtime_error = 4,
};

pub const BrokerApplication = struct {
    allocator: std.mem.Allocator,
    config: BrokerConfig,

    broker_metadata: ?BrokerMetadataFile = null,

    control_ring_buffer: ?RingBuffer = null,
    send_ring_buffer: ?RingBuffer = null,

    counter_values_buffer: ?[]align(128) u8 = null,
    counter_metadata_buffer: ?[]u8 = null,
    counters: ?CountersManager = null,

    control_command_storage: ?[]Command = null,
    control_command_queue: ?CommandQueue = null,

    service_registry: ?ServiceRegistry = null,
    routing_registry: ?RoutingRegistry = null,
    cluster_manager: ?ClusterManager = null,
    control_loop: ?ControlLoop = null,
    sender_loop: ?SenderEventLoop = null,
    receiver_loop: ?ReceiverEventLoop = null,

    broker_threads: ?BrokerThreads = null,

    started: bool = false,
    shutdown_requested: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, config: BrokerConfig) !Self {
        var self = Self{
            .allocator = allocator,
            .config = config,
        };

        try self.bootstrap();
        return self;
    }

    pub fn initFromDefaultConfig(allocator: std.mem.Allocator) !Self {
        var loader = ConfigLoader.init(allocator);
        const config = try loader.load();
        return Self.init(allocator, config);
    }

    pub fn initFromConfigFile(allocator: std.mem.Allocator, path: []const u8) !Self {
        var loader = ConfigLoader.init(allocator);
        const config = try loader.loadFromFile(path);
        return Self.init(allocator, config);
    }

    pub fn deinit(self: *Self) void {
        if (self.started) {
            self.shutdown();
        }

        if (self.control_command_queue) |*queue| {
            _ = queue;
            self.control_command_queue = null;
        }

        if (self.control_command_storage) |storage| {
            self.allocator.free(storage);
            self.control_command_storage = null;
        }

        self.receiver_loop = null;

        if (self.sender_loop) |*loop| {
            loop.deinit();
            self.sender_loop = null;
        }

        if (self.control_loop) |*loop| {
            loop.onClose();
            self.control_loop = null;
        }

        if (self.cluster_manager) |_| {
            self.cluster_manager = null;
        }

        if (self.service_registry) |*registry| {
            registry.deinit();
            self.service_registry = null;
        }

        self.routing_registry = null;

        if (self.counter_values_buffer) |buf| {
            self.allocator.free(buf);
            self.counter_values_buffer = null;
        }

        if (self.counter_metadata_buffer) |buf| {
            self.allocator.free(buf);
            self.counter_metadata_buffer = null;
        }

        if (self.broker_metadata) |*meta| {
            meta.close();
            self.broker_metadata = null;
        }
    }

    pub fn run(self: *Self) !u8 {
        try self.start();
        defer self.shutdown();

        self.logStartup();

        while (!self.shutdown_requested.load(.acquire)) {
            std.Thread.sleep(100 * std.time.ns_per_ms);
        }

        return @intFromEnum(ExitCode.success);
    }

    pub fn start(self: *Self) !void {
        if (self.started) return;

        var threads = BrokerThreads.init(
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
            self.toIdleStrategy(self.config.idle_strategy_name),
            self.toIdleStrategy(self.config.idle_strategy_name),
            self.toIdleStrategy(self.config.idle_strategy_name),
        );

        try threads.start();
        self.broker_threads = threads;
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

    fn bootstrap(self: *Self) !void {
        var broker_metadata = try BrokerMetadataFile.create(
            self.config.storage_path,
            self.config.group_name,
            self.config.node_id,
            self.config.control_buffer_size,
            self.config.messages_buffer_size,
        );
        errdefer broker_metadata.close();

        var control_rb = try RingBuffer.init(
            @alignCast(broker_metadata.getControlBuffer()),
            false,
            null,
            null,
        );

        var send_rb = try RingBuffer.init(
            @alignCast(broker_metadata.getSendBuffer()),
            false,
            null,
            null,
        );

        const values_len = alignedCounterValuesLength(self.config.counter_values_buffer_size);
        const values_buffer = try self.allocator.alignedAlloc(
            u8,
            @enumFromInt(std.math.log2(@as(usize, 128))),
            values_len,
        );
        errdefer self.allocator.free(values_buffer);
        @memset(values_buffer, 0);

        const metadata_len = counterMetadataLength(self.config.counter_values_buffer_size);
        const metadata_buffer = try self.allocator.alloc(u8, metadata_len);
        errdefer self.allocator.free(metadata_buffer);
        @memset(metadata_buffer, 0);

        var counters = CountersManager.init(values_buffer, metadata_buffer);

        const command_capacity = commandQueueCapacity();
        const command_storage = try self.allocator.alloc(Command, command_capacity);
        errdefer self.allocator.free(command_storage);

        var command_queue = CommandQueue.init(command_storage);

        var service_registry = ServiceRegistry.init(self.allocator);
        errdefer service_registry.deinit();

        var routing_registry = RoutingRegistry.init();

        var cluster_manager = ClusterManager.initSingleNode(self.config.node_id);

        const control_loop = ControlLoop.init(.{
            .control_rb = &control_rb,
            .cmd_queue = &command_queue,
            .cluster_manager = &cluster_manager,
            .counters = &counters,
            .local_node_id = self.config.node_id,
            .storage_path = self.config.storage_path,
            .group = self.config.group_name,
            .allocator = self.allocator,
        });

        var sender_loop = try SenderEventLoop.init(
            &send_rb,
            &counters,
            self.config.node_id,
            self.allocator,
        );
        errdefer sender_loop.deinit();

        const receiver_loop = ReceiverEventLoop.init(
            &routing_registry,
            &counters,
            self.config.node_id,
        );

        self.broker_metadata = broker_metadata;
        self.control_ring_buffer = control_rb;
        self.send_ring_buffer = send_rb;
        self.counter_values_buffer = values_buffer;
        self.counter_metadata_buffer = metadata_buffer;
        self.counters = counters;
        self.control_command_storage = command_storage;
        self.control_command_queue = command_queue;
        self.service_registry = service_registry;
        self.routing_registry = routing_registry;
        self.cluster_manager = cluster_manager;
        self.control_loop = control_loop;
        self.sender_loop = sender_loop;
        self.receiver_loop = receiver_loop;
    }

    fn logStartup(self: *const Self) void {
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

    fn toIdleStrategy(_: *const Self, name: config_mod.IdleStrategyName) IdleStrategy {
        return switch (name) {
            .busy_spin => .busy_spin,
            .yielding => .yielding,
            .sleeping => .sleeping,
            .backoff => .{ .backoff = .{} },
            .blocking => .sleeping,
        };
    }
};

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

fn alignedCounterValuesLength(counter_values_buffer_size: u32) usize {
    const raw: usize = counter_values_buffer_size;
    const alignment: usize = 128;
    return std.mem.alignForward(usize, raw, alignment);
}

fn counterMetadataLength(counter_values_buffer_size: u32) usize {
    const slots = @max(@as(usize, 1), @as(usize, counter_values_buffer_size) / 128);
    return slots * 256;
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
        .group_name = "brz-test-app",
        .storage_path = "/tmp",
    };

    var app = try BrokerApplication.init(allocator, config);
    defer app.deinit();

    try std.testing.expect(!app.shutdown_requested.load(.acquire));

    app.requestShutdown();

    try std.testing.expect(app.shutdown_requested.load(.acquire));
}
