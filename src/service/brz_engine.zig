//! BrzEngine — the service's main entry point.
//!
//! Orchestrates the startup sequence, owns the metadata files and agent threads,
//! and provides the application-facing API for creating clients and registering
//! message handlers.

const std = @import("std");
const platform = @import("../platform.zig");
const memory = @import("../memory.zig");
const ring_buffer = @import("../concurrent/ring_buffer.zig");
const constants = @import("../memory/constants.zig");
const MessageConsumer = @import("message_consumer.zig").MessageConsumer;
const control_agent_mod = @import("control_agent.zig");
const ControlAgent = control_agent_mod.ControlAgent;
const ServiceClient = @import("service_client.zig").ServiceClient;
const ServiceClientRegistry = @import("service_client_registry.zig").ServiceClientRegistry;

const BrokerMetadataFile = memory.BrokerMetadataFile;
const ServiceMetadataFile = memory.ServiceMetadataFile;
const ThreadRunner = platform.ThreadRunner;
const EventLoop = platform.EventLoop;
const Clock = platform.Clock;
const RingBuffer = ring_buffer.RingBuffer;

pub const ServiceConfig = struct {
    storage_path: []const u8 = constants.default_storage_path,
    group: []const u8 = "default",
    service_name: []const u8,
    blocking_mode: bool = false,
    heartbeat_timeout_ms: i32 = @intCast(constants.default_heartbeat_timeout_ms),
    control_buffer_length: usize = constants.default_control_buffer_length,
    messages_buffer_length: usize = constants.default_messages_buffer_length,
    leader_election_enabled: bool = false,
};

pub const BrzEngine = struct {
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
    message_consumer: *MessageConsumer,
    message_consumer_runner: ?ThreadRunner,
    control_agent: *ControlAgent,
    control_agent_runner: ?ThreadRunner,

    // ── State ─────────────────────────────────────────────────────────
    running: platform.AtomicBool,

    const Self = @This();

    /// Start the BrzEngine: create metadata, register with broker,
    /// start heartbeat, launch agent threads.
    pub fn start(allocator: std.mem.Allocator, config: ServiceConfig) !*Self {
        var engine = try allocator.create(Self);
        errdefer allocator.destroy(engine);

        // ── Step 1–4: Create metadata and register ────────────────────
        const meta = try createServiceMetadata(allocator, config);
        engine.service_meta = meta.service_meta;
        engine.broker_meta = meta.broker_meta;
        engine.service_id = meta.service_id;
        engine.node_id = meta.node_id;
        engine.config = config;
        engine.allocator = allocator;
        engine.running = platform.AtomicBool.init(true);

        // Register with the broker.
        try control_agent_mod.registerWithBroker(
            meta.broker_meta,
            meta.service_id,
            config.service_name,
            config.leader_election_enabled,
        );

        // Wait for registration response.
        _ = try control_agent_mod.waitForRegistrationResponse(meta.service_meta, 5000);

        // Write initial heartbeat.
        meta.service_meta.storeHeartbeat(Clock.epochMillis());

        // ── Step 5: Initialize service registry ───────────────────────
        engine.service_registry = ServiceClientRegistry.init(
            allocator,
            meta.broker_meta,
            meta.node_id,
            meta.service_id,
        );

        // ── Step 6: Start message consumer thread ─────────────────────
        const message_consumer = try allocator.create(MessageConsumer);
        message_consumer.* = try MessageConsumer.init(
            @alignCast(meta.service_meta.getMessagesBuffer()),
        );
        engine.message_consumer = message_consumer;

        engine.message_consumer_runner = ThreadRunner.init(
            "msg-consumer",
            EventLoop{
                .context = @ptrCast(message_consumer),
                .doWorkFn = &MessageConsumer.doWorkFn,
                .onCloseFn = &MessageConsumer.onCloseFn,
            },
            .{ .backoff = .{} },
        );
        try engine.message_consumer_runner.?.start();

        // ── Step 7: Start control agent thread ────────────────────────
        const ctrl_agent = try allocator.create(ControlAgent);
        ctrl_agent.* = try ControlAgent.init(
            meta.service_meta,
            meta.broker_meta,
            &engine.service_registry,
        );
        engine.control_agent = ctrl_agent;

        engine.control_agent_runner = ThreadRunner.init(
            "control-agent",
            EventLoop{
                .context = @ptrCast(ctrl_agent),
                .doWorkFn = &ControlAgent.doWorkFn,
                .onCloseFn = &ControlAgent.onCloseFn,
            },
            .{ .backoff = .{} },
        );
        try engine.control_agent_runner.?.start();

        return engine;
    }

    /// Graceful shutdown.
    pub fn stop(self: *Self) void {
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

        // 4. Close metadata files.
        self.service_meta.close();
        self.broker_meta.close();

        // 5. Clean up.
        self.service_registry.deinit();
        self.allocator.destroy(self.message_consumer);
        self.allocator.destroy(self.control_agent);
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

        return client;
    }

    /// Register a message handler for incoming messages.
    pub fn setMessageHandler(self: *Self, handler: RingBuffer.MessageHandler) void {
        self.message_consumer.setHandler(handler);
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
    _ = allocator;

    // 1. Open the broker's metadata file (must already exist).
    const broker_meta = try BrokerMetadataFile.open(
        config.storage_path,
        config.group,
    );

    // 2. Atomically allocate a unique service_id.
    const service_id = broker_meta.incrementAndGetNextServiceId();

    // 3. Read the node_id from the broker's header.
    const node_id = broker_meta.header.node_id;

    // 4. Create the service's metadata file.
    const service_meta = try ServiceMetadataFile.create(.{
        .storage_path = config.storage_path,
        .group = config.group,
        .service_name = config.service_name,
        .service_id = service_id,
        .node_id = node_id,
        .blocking_mode = config.blocking_mode,
        .heartbeat_timeout_ms = config.heartbeat_timeout_ms,
        .control_buffer_length = config.control_buffer_length,
        .messages_buffer_length = config.messages_buffer_length,
    });

    return .{
        .broker_meta = broker_meta,
        .service_meta = service_meta,
        .service_id = service_id,
        .node_id = node_id,
    };
}
