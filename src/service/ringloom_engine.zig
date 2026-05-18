//! RingLoomEngine — the service's main entry point.
//!
//! Orchestrates the startup sequence, owns the metadata files and agent threads,
//! and provides the application-facing API for creating clients and registering
//! message handlers.

const std = @import("std");
const ringloom_common = @import("ringloom_common");
const platform = ringloom_common.platform;
const memory = ringloom_common.memory;
const ring_buffer = ringloom_common.concurrent.ring_buffer;
const CountersManager = ringloom_common.concurrent.CountersManager;
const ServiceCounters = ringloom_common.monitoring.ServiceCounters;
const ServiceCounter = ringloom_common.monitoring.ServiceCounter;
const constants = ringloom_common.memory.constants;
const MessageConsumer = @import("message_consumer.zig").MessageConsumer;
const control_agent_mod = @import("control_agent.zig");
const ControlAgent = control_agent_mod.ControlAgent;
const ServiceAeronRuntime = @import("aeron_runtime.zig").ServiceAeronRuntime;
const ServiceClient = @import("service_client.zig").ServiceClient;
const ServiceClientRegistry = @import("service_client_registry.zig").ServiceClientRegistry;

const BrokerMetadataFile = memory.BrokerMetadataFile;
const ServiceMetadataFile = memory.ServiceMetadataFile;
const BuffersProvider = memory.BuffersProvider;
const ThreadRunner = platform.ThreadRunner;
const EventLoop = platform.EventLoop;
const Clock = platform.Clock;
const RingBuffer = ring_buffer.RingBuffer;

pub const ServiceConfig = struct {
    storage_path: []const u8 = constants.default_storage_path,
    group: []const u8 = "default",
    service_name: []const u8,
    broker_node_id: i16 = 1,
    blocking_mode: bool = false,
    heartbeat_timeout_ms: i32 = @intCast(constants.default_svc_heartbeat_timeout_ms),
    control_buffer_length: usize = constants.default_control_buffer_length,
    messages_buffer_length: usize = constants.default_messages_buffer_length,
    leader_election_enabled: bool = false,
    idle_strategy: platform.IdleStrategy = .{ .backoff = .{} },
};

pub const MessageConsumerMode = enum {
    threaded,
    external_polling,
};

pub const ControlAgentMode = enum {
    threaded,
    manual,
};

pub const StartOptions = struct {
    message_consumer_mode: MessageConsumerMode = .threaded,
    control_agent_mode: ControlAgentMode = .threaded,
};

pub const RingLoomEngine = struct {
    config: ServiceConfig,
    allocator: std.mem.Allocator,

    // ── Metadata files ────────────────────────────────────────────────
    service_meta: *ServiceMetadataFile,
    broker_meta: *BrokerMetadataFile,

    // ── Identity ──────────────────────────────────────────────────────
    service_id: i32,
    node_id: i16,

    // ── Service discovery ─────────────────────────────────────────────
    service_registry: ServiceClientRegistry,

    // ── Agent threads ─────────────────────────────────────────────────
    message_consumer: ?*MessageConsumer,
    message_consumer_runner: ?ThreadRunner,
    control_agent: *ControlAgent,
    control_agent_runner: ?ThreadRunner,
    message_consumer_mode: MessageConsumerMode,
    aeron_runtime: *ServiceAeronRuntime,

    // ── Observability ─────────────────────────────────────────────────
    counters: CountersManager,
    service_counters: ServiceCounters,

    // ── State ─────────────────────────────────────────────────────────
    running: platform.AtomicBool,

    const Self = @This();

    /// Start the RingLoomEngine: create metadata, register with broker,
    /// start heartbeat, launch agent threads.
    pub fn start(allocator: std.mem.Allocator, config: ServiceConfig) !*Self {
        return startWithOptions(allocator, config, .{});
    }

    /// Start the engine with explicit runtime options.
    pub fn startWithOptions(
        allocator: std.mem.Allocator,
        config: ServiceConfig,
        options: StartOptions,
    ) !*Self {
        var engine = try allocator.create(Self);
        errdefer allocator.destroy(engine);

        // ── Step 1–4: Create metadata and register ────────────────────
        const meta = try createServiceMetadata(allocator, config);
        errdefer {
            meta.service_meta.close();
            allocator.destroy(meta.service_meta);
            meta.broker_meta.close();
            allocator.destroy(meta.broker_meta);
        }
        engine.service_meta = meta.service_meta;
        engine.broker_meta = meta.broker_meta;
        engine.service_id = meta.service_id;
        engine.node_id = meta.node_id;
        engine.config = config;
        engine.allocator = allocator;
        engine.counters = CountersManager.init(
            meta.service_meta.getCounterValuesBuffer(),
            meta.service_meta.getCounterMetadataBuffer(),
        );
        engine.service_counters = try ServiceCounters.init(&engine.counters);
        engine.running = platform.AtomicBool.init(true);
        engine.message_consumer = null;
        engine.message_consumer_runner = null;
        engine.control_agent_runner = null;
        engine.message_consumer_mode = options.message_consumer_mode;

        // Register with the broker.
        try control_agent_mod.registerWithBroker(
            meta.broker_meta,
            meta.service_id,
            config.service_name,
            config.leader_election_enabled,
        );
        engine.service_counters.increment(.registrations_sent);

        // Wait for registration response.
        _ = try control_agent_mod.waitForRegistrationResponse(meta.service_meta, 5000);

        // Write initial heartbeat.
        meta.service_meta.storeHeartbeat(Clock.epochMillis());

        const aeron_runtime = try allocator.create(ServiceAeronRuntime);
        errdefer allocator.destroy(aeron_runtime);
        aeron_runtime.* = try ServiceAeronRuntime.connect(
            allocator,
            meta.broker_meta.getAeronDiscovery(),
        );
        errdefer aeron_runtime.deinit();
        engine.aeron_runtime = aeron_runtime;

        // ── Step 5: Initialize service registry ───────────────────────
        engine.service_registry = ServiceClientRegistry.init(
            allocator,
            meta.broker_meta,
            engine.aeron_runtime,
            meta.node_id,
            meta.service_id,
            config.storage_path,
            config.group,
            &engine.service_counters,
        );

        // ── Step 6: Start message consumer thread when requested ──────
        if (options.message_consumer_mode == .threaded) {
            const message_consumer = try allocator.create(MessageConsumer);
            errdefer allocator.destroy(message_consumer);

            message_consumer.* = try MessageConsumer.init(
                @alignCast(meta.service_meta.getMessagesBuffer()),
                &engine.service_counters,
            );
            engine.message_consumer = message_consumer;

            engine.message_consumer_runner = ThreadRunner.init(
                "msg-consumer",
                EventLoop{
                    .context = @ptrCast(message_consumer),
                    .doWorkFn = &MessageConsumer.doWorkFn,
                    .onCloseFn = &MessageConsumer.onCloseFn,
                },
                config.idle_strategy,
            );
            errdefer if (engine.message_consumer_runner) |*runner| {
                runner.stopAndJoin();
            };

            try engine.message_consumer_runner.?.start();
        }

        // ── Step 7: Start control agent thread ────────────────────────
        const ctrl_agent = try allocator.create(ControlAgent);
        errdefer allocator.destroy(ctrl_agent);
        ctrl_agent.* = try ControlAgent.init(
            meta.service_meta,
            meta.broker_meta,
            &engine.service_registry,
            &engine.service_counters,
        );
        engine.control_agent = ctrl_agent;

        if (options.control_agent_mode == .threaded) {
            engine.control_agent_runner = ThreadRunner.init(
                "control-agent",
                EventLoop{
                    .context = @ptrCast(ctrl_agent),
                    .doWorkFn = &ControlAgent.doWorkFn,
                    .onCloseFn = &ControlAgent.onCloseFn,
                },
                config.idle_strategy,
            );
            try engine.control_agent_runner.?.start();
        }

        return engine;
    }

    /// Graceful shutdown.
    pub fn stop(self: *Self) void {
        if (!self.running.load()) return;
        self.running.store(false);

        // 1. Stop the message consumer first.
        if (self.message_consumer_runner) |*r| {
            r.stopAndJoin();
        }

        // 2. Stop the control agent.
        if (self.control_agent_runner) |*r| {
            r.stopAndJoin();
        }

        // 3. Send UnregisterService to the broker.
        control_agent_mod.unregisterFromBroker(self.broker_meta, self.service_id) catch {};
        self.service_counters.increment(.unregisters_sent);

        // 4. Close Aeron client resources before unmapping metadata.
        self.aeron_runtime.deinit();
        self.allocator.destroy(self.aeron_runtime);

        // 5. Close metadata files and cached BuffersProviders.
        self.service_meta.close();
        self.broker_meta.close();
        BuffersProvider.closeAll(self.allocator);

        // 6. Clean up.
        self.service_registry.deinit();
        if (self.message_consumer) |message_consumer| {
            self.allocator.destroy(message_consumer);
            self.message_consumer = null;
        }
        self.allocator.destroy(self.control_agent);
    }

    /// Release heap allocations owned by the engine handle itself.
    ///
    /// `stop()` performs graceful runtime shutdown and closes mapped files.
    /// `deinit()` releases the handle allocations returned by `start()`.
    pub fn deinit(self: *Self) void {
        if (self.running.load()) {
            self.stop();
        }
        self.allocator.destroy(self.service_meta);
        self.allocator.destroy(self.broker_meta);
        self.allocator.destroy(self);
    }

    // ── Application API ───────────────────────────────────────────────

    /// Create a client proxy for the named service.
    pub fn createClient(self: *Self, service_name: []const u8) !*ServiceClient {
        const client = try self.service_registry.getOrCreate(service_name);

        // Subscribe to updates from the broker for this service.
        try control_agent_mod.subscribeToServiceUpdates(
            self.broker_meta,
            self.service_id,
            service_name,
        );
        self.service_counters.increment(.subscriptions_sent);

        return client;
    }

    /// Register a message handler for incoming messages.
    pub fn setMessageHandler(self: *Self, handler: RingBuffer.MessageHandler) void {
        const message_consumer = self.message_consumer orelse {
            @panic("setMessageHandler requires threaded message consumer mode");
        };
        message_consumer.setHandler(handler);
    }
};

/// Opens the broker's metadata file, allocates a unique service_id,
/// and creates this service's metadata file.
fn createServiceMetadata(allocator: std.mem.Allocator, config: ServiceConfig) !struct {
    broker_meta: *BrokerMetadataFile,
    service_meta: *ServiceMetadataFile,
    service_id: i32,
    node_id: i16,
} {
    // 1. Open the broker's metadata file (must already exist).
    const broker_meta = try allocator.create(BrokerMetadataFile);
    errdefer allocator.destroy(broker_meta);
    broker_meta.* = try BrokerMetadataFile.open(
        config.storage_path,
        config.group,
        config.broker_node_id,
    );

    // 2. Atomically allocate a unique service_id.
    const service_id = broker_meta.incrementAndGetNextServiceId();

    // 3. Read the node_id from the broker's header.
    const node_id = broker_meta.header.node_id;

    // 4. Create the service's metadata file.
    const discovery = broker_meta.getAeronDiscovery();
    const service_meta = try allocator.create(ServiceMetadataFile);
    errdefer allocator.destroy(service_meta);
    service_meta.* = try ServiceMetadataFile.create(.{
        .storage_path = config.storage_path,
        .group = config.group,
        .service_name = config.service_name,
        .service_id = service_id,
        .node_id = node_id,
        .blocking_mode = config.blocking_mode,
        .heartbeat_timeout_ms = config.heartbeat_timeout_ms,
        .control_buffer_length = config.control_buffer_length,
        .messages_buffer_length = config.messages_buffer_length,
        .aeron_directory = discovery.directory(),
        .broker_ingress_stream_id = discovery.broker_ingress_stream_id,
        .broker_start_timestamp_ms = broker_meta.header.start_timestamp_ms,
    });

    return .{
        .broker_meta = broker_meta,
        .service_meta = service_meta,
        .service_id = service_id,
        .node_id = node_id,
    };
}
