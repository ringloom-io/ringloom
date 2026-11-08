// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const builtin = @import("builtin");
const ringloom_common = @import("ringloom_common");

const memory_constants = ringloom_common.memory.constants;
const message_header = ringloom_common.message.message_header;
const MessageHeader = message_header.MessageHeader;
const counters_mod = ringloom_common.concurrent.counters;
const error_state_mod = ringloom_common.concurrent.error_state;
const RingBuffer = ringloom_common.concurrent.ring_buffer.RingBuffer;

const engine_mod = @import("ringloom_engine.zig");
const RingLoomEngine = engine_mod.RingLoomEngine;
const EngineServiceConfig = engine_mod.ServiceConfig;
const StartOptions = engine_mod.StartOptions;
const ServiceClient = @import("service_client.zig").ServiceClient;
const control_agent_mod = @import("control_agent.zig");
const Clock = ringloom_common.platform.Clock;

pub const ringloom_service = opaque {};
pub const ringloom_client = opaque {};
pub const ringloom_message_consumer = opaque {};
pub const ringloom_metrics_reader = opaque {};

pub const RINGLOOM_SERVICE_ABI_VERSION: u32 = 7;
const max_error_message_length = error_state_mod.max_error_message_length;
const claim_handle_record_length = std.math.minInt(i32);

pub const ringloom_status_t = enum(c_int) {
    RINGLOOM_OK = 0,
    RINGLOOM_ERR_INVALID_ARGUMENT = 1,
    RINGLOOM_ERR_OUT_OF_MEMORY = 2,
    RINGLOOM_ERR_BROKER_NOT_FOUND = 3,
    RINGLOOM_ERR_REGISTRATION_TIMEOUT = 4,
    RINGLOOM_ERR_BUFFER_FULL = 5,
    RINGLOOM_ERR_NO_AVAILABLE_INSTANCE = 6,
    RINGLOOM_ERR_BACKPRESSURE = 7,
    RINGLOOM_ERR_PEER_DISCONNECTED = 8,
    RINGLOOM_ERR_CLAIM_NOT_ACTIVE = 9,
    RINGLOOM_ERR_MESSAGE_TOO_LONG = 10,
    RINGLOOM_NOT_READY = 11,
    RINGLOOM_ERR_TOPIC_LEADER_UNAVAILABLE = 12,
    RINGLOOM_ERR_TOPIC_DISABLED = 13,
    RINGLOOM_ERR_TOPIC_CONFIG_MISMATCH = 14,
    RINGLOOM_ERR_TOPIC_UNKNOWN = 15,
    RINGLOOM_KV_MISSING = 32,
    RINGLOOM_ERR_KV_BUFFER_TOO_SMALL = 33,
    RINGLOOM_ERR_KV_NOT_WRITER = 34,
    RINGLOOM_ERR_KV_HELD_BY_OTHER = 35,
    RINGLOOM_ERR_KV_NOT_AVAILABLE_LOCALLY = 36,
    RINGLOOM_ERR_KV_ENGINE_MISMATCH = 37,
    RINGLOOM_ERR_KV_KEY_TOO_LARGE = 38,
    RINGLOOM_ERR_KV_VALUE_TOO_LARGE = 39,
    RINGLOOM_ERR_KV_BATCH_FULL = 40,
    RINGLOOM_ERR_INTERNAL = 255,
};

const Status = ringloom_status_t;
const custom_metric_category: u28 = 0;

pub const ringloom_service_config_t = extern struct {
    storage_path: ?[*]const u8,
    storage_path_len: usize,
    group: ?[*]const u8,
    group_len: usize,
    service_name: ?[*]const u8,
    service_name_len: usize,
    broker_node_id: i16,
    blocking_mode: bool,
    heartbeat_timeout_ms: i32,
    control_buffer_length: usize,
    messages_buffer_length: usize,
    leader_election_enabled: bool,
};

pub const ringloom_message_t = extern struct {
    correlation_id: i64,
    source_node_id: i16,
    source_service_id: i16,
    target_node_id: i16,
    target_service_id: i16,
    template_id: u16,
    flags: u8,
    payload: ?[*]const u8,
    payload_len: usize,
};

pub const ringloom_buffer_claim_t = extern struct {
    payload: ?[*]u8,
    payload_len: usize,
    _ring_buffer: usize,
    _header_index: usize,
    _record_length: i32,
    _active: u8,
};

pub const ringloom_client_target_t = ServiceClient.TargetInstanceInfo;

pub const ringloom_message_handler_t = ?*const anyopaque;
pub const ringloom_service_lifecycle_handler_t = ?*const anyopaque;

pub const ringloom_service_lifecycle_event_type_t = enum(c_int) {
    RINGLOOM_SERVICE_AVAILABLE = 1,
    RINGLOOM_SERVICE_UNAVAILABLE = 2,
};

pub const ringloom_service_lifecycle_event_t = extern struct {
    event_type: ringloom_service_lifecycle_event_type_t,
    service_id: i32,
    node_id: i16,
    is_leader: bool,
    service_name: ?[*]const u8,
    service_name_len: usize,
};

pub const ringloom_metric_kind_t = enum(c_int) {
    RINGLOOM_METRIC_COUNTER = 1,
    RINGLOOM_METRIC_GAUGE = 2,
};

pub const ringloom_metric_descriptor_t = extern struct {
    name: ?[*]const u8,
    name_len: usize,
    kind: ringloom_metric_kind_t,
    value: i64,
};

pub const ringloom_metric_slot_t = extern struct {
    metric_id: i32,
    reserved: u32,
    value: ?*i64,
    value_len: usize,
};

pub const ringloom_ring_stats_t = extern struct {
    capacity_bytes: u64,
    used_bytes: u64,
    free_bytes: u64,
    producer_position: u64,
    consumer_position: u64,
};

pub const ringloom_aeron_publication_status_t = enum(c_int) {
    RINGLOOM_AERON_PUBLICATION_UNKNOWN = 0,
    RINGLOOM_AERON_PUBLICATION_CLAIMED = 1,
    RINGLOOM_AERON_PUBLICATION_NOT_CONNECTED = 2,
    RINGLOOM_AERON_PUBLICATION_BACK_PRESSURED = 3,
    RINGLOOM_AERON_PUBLICATION_ADMIN_ACTION = 4,
    RINGLOOM_AERON_PUBLICATION_CLOSED = 5,
    RINGLOOM_AERON_PUBLICATION_MAX_POSITION_EXCEEDED = 6,
    RINGLOOM_AERON_PUBLICATION_FAILED = 7,
};

const ServiceHandle = struct {
    allocator: std.mem.Allocator,
    engine: ?*RingLoomEngine,
    storage_path: []u8,
    group: []u8,
    service_name: []u8,
    ref_count: std.atomic.Value(u32),
    stopped: std.atomic.Value(bool),
    destroy_requested: std.atomic.Value(bool),
    polling_consumer_active: std.atomic.Value(bool),

    fn init(
        allocator: std.mem.Allocator,
        engine: *RingLoomEngine,
        storage_path: []u8,
        group: []u8,
        service_name: []u8,
    ) ServiceHandle {
        return .{
            .allocator = allocator,
            .engine = engine,
            .storage_path = storage_path,
            .group = group,
            .service_name = service_name,
            .ref_count = std.atomic.Value(u32).init(1),
            .stopped = std.atomic.Value(bool).init(false),
            .destroy_requested = std.atomic.Value(bool).init(false),
            .polling_consumer_active = std.atomic.Value(bool).init(false),
        };
    }

    fn retain(self: *ServiceHandle) void {
        _ = self.ref_count.fetchAdd(1, .acq_rel);
    }

    fn release(self: *ServiceHandle) void {
        const previous = self.ref_count.fetchSub(1, .acq_rel);
        if (previous == 1) {
            self.allocator.free(self.storage_path);
            self.allocator.free(self.group);
            self.allocator.free(self.service_name);
            self.allocator.destroy(self);
        }
    }

    fn stopOnce(self: *ServiceHandle) void {
        if (self.stopped.cmpxchgWeak(false, true, .acq_rel, .acquire) != null) {
            return;
        }

        if (self.engine) |engine| {
            engine.stop();
        }
    }

    fn destroyEngine(self: *ServiceHandle) void {
        const engine = self.engine orelse return;
        engine.deinit();
        self.engine = null;
    }
};

const ClientHandle = struct {
    service: *ServiceHandle,
    client: *ServiceClient,
    closed: std.atomic.Value(bool),
    lifecycle_handler: ringloom_service_lifecycle_handler_t,
    lifecycle_user_data: ?*anyopaque,

    fn init(service: *ServiceHandle, client: *ServiceClient) ClientHandle {
        return .{
            .service = service,
            .client = client,
            .closed = std.atomic.Value(bool).init(false),
            .lifecycle_handler = null,
            .lifecycle_user_data = null,
        };
    }

    fn clearLifecycleHandler(self: *ClientHandle) void {
        self.lifecycle_handler = null;
        self.lifecycle_user_data = null;
        self.client.setLifecycleHandler(null, null);
    }
};

const MessageConsumerHandle = struct {
    service: *ServiceHandle,
    ring_buffer: RingBuffer,
    closed: std.atomic.Value(bool),

    fn init(service: *ServiceHandle, ring_buffer: RingBuffer) MessageConsumerHandle {
        return .{
            .service = service,
            .ring_buffer = ring_buffer,
            .closed = std.atomic.Value(bool).init(false),
        };
    }
};

const ClaimHandle = struct {
    allocator: std.mem.Allocator,
    send_claim: ServiceClient.SendClaim,

    fn init(allocator: std.mem.Allocator, send_claim: ServiceClient.SendClaim) ClaimHandle {
        return .{
            .allocator = allocator,
            .send_claim = send_claim,
        };
    }
};

const MetricsReaderHandle = struct {
    service: *ServiceHandle,
    name_scratch: [244]u8 = undefined,

    fn init(service: *ServiceHandle) MetricsReaderHandle {
        return .{ .service = service };
    }
};

const OwnedServiceConfig = struct {
    storage_path: []u8,
    group: []u8,
    service_name: []u8,
    zig_config: EngineServiceConfig,

    fn deinit(self: OwnedServiceConfig, allocator: std.mem.Allocator) void {
        allocator.free(self.storage_path);
        allocator.free(self.group);
        allocator.free(self.service_name);
    }
};

const PollDispatchContext = struct {
    handler: ringloom_message_handler_t,
    user_data: ?*anyopaque,
    out_count: *u32,
};

threadlocal var last_error_message_buffer: [max_error_message_length + 1:0]u8 =
    @splat(0);
threadlocal var poll_dispatch_context: ?*PollDispatchContext = null;

var abi_debug_allocator: std.heap.SafeAllocator = .init(std.heap.page_allocator, .{});

fn abiAllocator() std.mem.Allocator {
    return switch (builtin.mode) {
        .debug, .safe => abi_debug_allocator.allocator(),
        .fast, .small => std.heap.smp_allocator,
    };
}

fn clearLastError() void {
    error_state_mod.err_state.clear();
    last_error_message_buffer[0] = 0;
}

fn setLastError(status: Status, message: []const u8) Status {
    const len = @min(message.len, max_error_message_length);
    if (len > 0) {
        @memcpy(last_error_message_buffer[0..len], message[0..len]);
    }
    last_error_message_buffer[len] = 0;
    error_state_mod.err_state.set(@backingInt(status), last_error_message_buffer[0..len]);
    return status;
}

fn setLastErrorFmt(status: Status, comptime fmt: []const u8, args: anytype) Status {
    var buf: [max_error_message_length]u8 = undefined;
    const rendered = std.fmt.bufPrint(&buf, fmt, args) catch |err| switch (err) {
        error.NoSpaceLeft => buf[0..max_error_message_length],
    };
    return setLastError(status, rendered);
}

fn serviceHandleFromOpaque(service: ?*ringloom_service) ?*ServiceHandle {
    return if (service) |ptr| @ptrCast(@alignCast(ptr)) else null;
}

fn clientHandleFromOpaque(client: ?*ringloom_client) ?*ClientHandle {
    return if (client) |ptr| @ptrCast(@alignCast(ptr)) else null;
}

fn messageConsumerHandleFromOpaque(consumer: ?*ringloom_message_consumer) ?*MessageConsumerHandle {
    return if (consumer) |ptr| @ptrCast(@alignCast(ptr)) else null;
}

fn metricsReaderHandleFromOpaque(reader: ?*ringloom_metrics_reader) ?*MetricsReaderHandle {
    return if (reader) |ptr| @ptrCast(@alignCast(ptr)) else null;
}

fn serviceHandleToOpaque(service: *ServiceHandle) *ringloom_service {
    return @ptrCast(service);
}

fn clientHandleToOpaque(client: *ClientHandle) *ringloom_client {
    return @ptrCast(client);
}

fn messageConsumerHandleToOpaque(consumer: *MessageConsumerHandle) *ringloom_message_consumer {
    return @ptrCast(consumer);
}

fn metricsReaderHandleToOpaque(reader: *MetricsReaderHandle) *ringloom_metrics_reader {
    return @ptrCast(reader);
}

fn requiredBytes(ptr: ?[*]const u8, len: usize, field_name: []const u8) ![]const u8 {
    if (len == 0) return error.InvalidArgument;
    const non_null = ptr orelse {
        _ = field_name;
        return error.InvalidArgument;
    };
    return non_null[0..len];
}

fn optionalBytes(ptr: ?[*]const u8, len: usize, default_value: []const u8) []const u8 {
    if (len == 0 or ptr == null) return default_value;
    return ptr.?[0..len];
}

fn payloadBytes(ptr: ?[*]const u8, len: usize) ?[]const u8 {
    if (len == 0) return "";
    const non_null = ptr orelse return null;
    return non_null[0..len];
}

fn duplicateConfig(allocator: std.mem.Allocator, config: *const ringloom_service_config_t) !OwnedServiceConfig {
    const service_name = try allocator.dupe(u8, try requiredBytes(
        config.service_name,
        config.service_name_len,
        "service_name",
    ));
    errdefer allocator.free(service_name);

    const storage_path = try allocator.dupe(u8, optionalBytes(
        config.storage_path,
        config.storage_path_len,
        memory_constants.default_storage_path,
    ));
    errdefer allocator.free(storage_path);

    const group = try allocator.dupe(u8, optionalBytes(
        config.group,
        config.group_len,
        "default",
    ));
    errdefer allocator.free(group);

    const control_buffer_length = if (config.control_buffer_length == 0)
        memory_constants.default_control_buffer_length
    else
        config.control_buffer_length;
    if (!memory_constants.isPowerOfTwo(control_buffer_length)) {
        return error.InvalidArgument;
    }

    const messages_buffer_length = if (config.messages_buffer_length == 0)
        memory_constants.default_messages_buffer_length
    else
        config.messages_buffer_length;
    if (!memory_constants.isPowerOfTwo(messages_buffer_length)) {
        return error.InvalidArgument;
    }

    return .{
        .storage_path = storage_path,
        .group = group,
        .service_name = service_name,
        .zig_config = .{
            .storage_path = storage_path,
            .group = group,
            .service_name = service_name,
            .broker_node_id = if (config.broker_node_id == 0) 1 else config.broker_node_id,
            .blocking_mode = config.blocking_mode,
            .heartbeat_timeout_ms = if (config.heartbeat_timeout_ms == 0)
                @intCast(memory_constants.default_svc_heartbeat_timeout_ms)
            else
                config.heartbeat_timeout_ms,
            .control_buffer_length = control_buffer_length,
            .messages_buffer_length = messages_buffer_length,
            .leader_election_enabled = config.leader_election_enabled,
        },
    };
}

fn brokerMetadataPath(
    buf: *[std.fs.max_path_bytes]u8,
    storage_path: []const u8,
    group: []const u8,
    broker_node_id: i16,
) []const u8 {
    return std.fmt.bufPrint(buf, "{s}/{s}/services/broker_{d}.dat", .{
        storage_path,
        group,
        broker_node_id,
    }) catch "broker metadata path unavailable";
}

fn mapStartError(err: anyerror, config: OwnedServiceConfig) Status {
    return switch (err) {
        error.OutOfMemory => setLastError(.RINGLOOM_ERR_OUT_OF_MEMORY, "native allocation failed"),
        error.InvalidArgument => setLastError(.RINGLOOM_ERR_INVALID_ARGUMENT, "invalid service configuration"),
        error.RegistrationTimeout => setLastErrorFmt(
            .RINGLOOM_ERR_REGISTRATION_TIMEOUT,
            "timed out waiting for broker registration response for service '{s}'",
            .{config.service_name},
        ),
        error.FileNotFound,
        error.FileTooSmall,
        error.FileSizeMismatch,
        error.ControlBufferNotPowerOfTwo,
        error.MessagesBufferNotPowerOfTwo,
        => blk: {
            var path_buf: [std.fs.max_path_bytes]u8 = undefined;
            const path = brokerMetadataPath(
                &path_buf,
                config.storage_path,
                config.group,
                config.zig_config.broker_node_id,
            );
            break :blk setLastErrorFmt(
                .RINGLOOM_ERR_BROKER_NOT_FOUND,
                "could not open broker metadata at {s}",
                .{path},
            );
        },
        else => setLastErrorFmt(.RINGLOOM_ERR_INTERNAL, "native service start failed: {}", .{err}),
    };
}

fn mapRuntimeError(err: anyerror, context: []const u8) Status {
    return switch (err) {
        error.OutOfMemory => setLastError(.RINGLOOM_ERR_OUT_OF_MEMORY, "native allocation failed"),
        error.InvalidArgument => setLastError(.RINGLOOM_ERR_INVALID_ARGUMENT, context),
        error.BufferFull,
        error.SendBufferFull,
        => setLastError(.RINGLOOM_ERR_BUFFER_FULL, context),
        error.NoAvailableInstance,
        error.NoLeaderAvailable,
        => setLastError(.RINGLOOM_ERR_NO_AVAILABLE_INSTANCE, context),
        error.BackPressure,
        error.BackPressureTimeout,
        => setLastError(.RINGLOOM_ERR_BACKPRESSURE, context),
        error.MessageTooLong => setLastError(.RINGLOOM_ERR_MESSAGE_TOO_LONG, context),
        else => setLastErrorFmt(.RINGLOOM_ERR_INTERNAL, "{s}: {}", .{ context, err }),
    };
}

fn requireService(service: ?*ringloom_service) ?*ServiceHandle {
    const handle = serviceHandleFromOpaque(service) orelse return null;
    if (handle.destroy_requested.load(.acquire) or handle.engine == null) {
        _ = setLastError(.RINGLOOM_ERR_INVALID_ARGUMENT, "service handle has been destroyed");
        return null;
    }
    return handle;
}

fn requireActiveService(service: ?*ringloom_service) ?*ServiceHandle {
    const handle = requireService(service) orelse return null;
    if (handle.stopped.load(.acquire)) {
        _ = setLastError(.RINGLOOM_ERR_INVALID_ARGUMENT, "service handle has been stopped");
        return null;
    }
    return handle;
}

fn requireActiveClient(client: ?*ringloom_client) ?*ClientHandle {
    const handle = clientHandleFromOpaque(client) orelse return null;
    if (handle.closed.load(.acquire)) {
        _ = setLastError(.RINGLOOM_ERR_INVALID_ARGUMENT, "client handle has been destroyed");
        return null;
    }
    if (handle.service.destroy_requested.load(.acquire) or
        handle.service.stopped.load(.acquire) or
        handle.service.engine == null)
    {
        _ = setLastError(.RINGLOOM_ERR_INVALID_ARGUMENT, "parent service is not active");
        return null;
    }
    return handle;
}

fn requireActiveConsumer(consumer: ?*ringloom_message_consumer) ?*MessageConsumerHandle {
    const handle = messageConsumerHandleFromOpaque(consumer) orelse return null;
    if (handle.closed.load(.acquire)) {
        _ = setLastError(.RINGLOOM_ERR_INVALID_ARGUMENT, "message consumer has been destroyed");
        return null;
    }
    if (handle.service.destroy_requested.load(.acquire) or
        handle.service.stopped.load(.acquire) or
        handle.service.engine == null)
    {
        _ = setLastError(.RINGLOOM_ERR_INVALID_ARGUMENT, "parent service is not active");
        return null;
    }
    return handle;
}

fn requireMetricsReader(reader: ?*ringloom_metrics_reader) ?*MetricsReaderHandle {
    const handle = metricsReaderHandleFromOpaque(reader) orelse return null;
    if (handle.service.destroy_requested.load(.acquire) or
        handle.service.stopped.load(.acquire) or
        handle.service.engine == null)
    {
        _ = setLastError(.RINGLOOM_ERR_INVALID_ARGUMENT, "parent service is not active");
        return null;
    }
    return handle;
}

fn progressControlPlane(service: *ServiceHandle) void {
    const engine = service.engine orelse return;
    if (engine.control_agent_runner == null) {
        _ = engine.control_agent.doWork();
    }
}

fn lifecycleBridge(context: ?*anyopaque, event: ServiceClient.LifecycleEvent) void {
    const handle: *ClientHandle = if (context) |ctx|
        @ptrCast(@alignCast(ctx))
    else
        return;

    if (handle.closed.load(.acquire)) return;
    const handler_ptr = handle.lifecycle_handler orelse return;
    const handler: *const fn (
        user_data: ?*anyopaque,
        event: *const ringloom_service_lifecycle_event_t,
    ) callconv(.c) void = @ptrCast(handler_ptr);

    const event_type: ringloom_service_lifecycle_event_type_t = switch (event.event_type) {
        .available => .RINGLOOM_SERVICE_AVAILABLE,
        .unavailable => .RINGLOOM_SERVICE_UNAVAILABLE,
    };
    var native_event = ringloom_service_lifecycle_event_t{
        .event_type = event_type,
        .service_id = event.service_id,
        .node_id = event.node_id,
        .is_leader = event.is_leader,
        .service_name = if (event.service_name.len == 0) null else event.service_name.ptr,
        .service_name_len = event.service_name.len,
    };

    handler(handle.lifecycle_user_data, &native_event);
}

fn pollDispatch(msg_type_id: i32, payload: []const u8) void {
    const ctx = poll_dispatch_context orelse return;
    const handler: *const fn (
        user_data: ?*anyopaque,
        message: *const ringloom_message_t,
    ) callconv(.c) void = @ptrCast(ctx.handler.?);

    var message = if (message_header.tryDecodeEnvelope(msg_type_id, payload)) |envelope|
        ringloom_message_t{
            .correlation_id = envelope.header.correlation_id,
            .source_node_id = envelope.header.source_node_id,
            .source_service_id = envelope.header.source_service_id,
            .target_node_id = envelope.header.target_node_id,
            .target_service_id = envelope.header.target_service_id,
            .template_id = envelope.header.template_id,
            .flags = envelope.header.flags,
            .payload = if (envelope.payload.len == 0) null else envelope.payload.ptr,
            .payload_len = envelope.payload.len,
        }
    else
        ringloom_message_t{
            .correlation_id = 0,
            .source_node_id = 0,
            .source_service_id = 0,
            .target_node_id = 0,
            .target_service_id = 0,
            .template_id = message_header.templateIdFromMsgTypeId(msg_type_id),
            .flags = 0,
            .payload = if (payload.len == 0) null else payload.ptr,
            .payload_len = payload.len,
        };

    handler(ctx.user_data, &message);
    ctx.out_count.* += 1;
}

export fn ringloom_service_abi_version() u32 {
    return RINGLOOM_SERVICE_ABI_VERSION;
}

export fn ringloom_service_start(
    config: ?*const ringloom_service_config_t,
    out_service: ?*?*ringloom_service,
) Status {
    clearLastError();

    const out_service_ptr = out_service orelse
        return setLastError(.RINGLOOM_ERR_INVALID_ARGUMENT, "out_service must not be NULL");
    out_service_ptr.* = null;

    const config_ptr = config orelse
        return setLastError(.RINGLOOM_ERR_INVALID_ARGUMENT, "config must not be NULL");

    const allocator = abiAllocator();
    const owned = duplicateConfig(allocator, config_ptr) catch |err| switch (err) {
        error.OutOfMemory => return setLastError(.RINGLOOM_ERR_OUT_OF_MEMORY, "native allocation failed"),
        error.InvalidArgument => return setLastError(
            .RINGLOOM_ERR_INVALID_ARGUMENT,
            "service configuration contains missing or invalid fields",
        ),
    };
    errdefer owned.deinit(allocator);

    const engine = RingLoomEngine.startWithOptions(
        allocator,
        owned.zig_config,
        StartOptions{
            .message_consumer_mode = .external_polling,
            .control_agent_mode = .manual,
        },
    ) catch |err| return mapStartError(err, owned);
    errdefer {
        engine.stop();
        engine.deinit();
    }

    const service = allocator.create(ServiceHandle) catch
        return setLastError(.RINGLOOM_ERR_OUT_OF_MEMORY, "native allocation failed");
    service.* = ServiceHandle.init(
        allocator,
        engine,
        owned.storage_path,
        owned.group,
        owned.service_name,
    );

    out_service_ptr.* = serviceHandleToOpaque(service);
    return .RINGLOOM_OK;
}

export fn ringloom_service_stop(service: ?*ringloom_service) void {
    clearLastError();
    const handle = serviceHandleFromOpaque(service) orelse return;
    handle.stopOnce();
}

export fn ringloom_service_destroy(service: ?*ringloom_service) void {
    clearLastError();
    const handle = serviceHandleFromOpaque(service) orelse return;
    handle.destroy_requested.store(true, .release);
    handle.stopOnce();
    handle.destroyEngine();
    handle.release();
}

export fn ringloom_service_id(
    service: ?*const ringloom_service,
    out_service_id: ?*i32,
) Status {
    clearLastError();
    const out = out_service_id orelse
        return setLastError(.RINGLOOM_ERR_INVALID_ARGUMENT, "out_service_id must not be NULL");
    const handle = requireService(@constCast(service)) orelse
        return .RINGLOOM_ERR_INVALID_ARGUMENT;

    out.* = handle.engine.?.service_id;
    return .RINGLOOM_OK;
}

export fn ringloom_service_node_id(
    service: ?*const ringloom_service,
    out_node_id: ?*i16,
) Status {
    clearLastError();
    const out = out_node_id orelse
        return setLastError(.RINGLOOM_ERR_INVALID_ARGUMENT, "out_node_id must not be NULL");
    const handle = requireService(@constCast(service)) orelse
        return .RINGLOOM_ERR_INVALID_ARGUMENT;

    out.* = handle.engine.?.node_id;
    return .RINGLOOM_OK;
}

export fn ringloom_service_aeron_directory(
    service: ?*const ringloom_service,
    out_directory: ?*?[*]const u8,
    out_directory_len: ?*usize,
) Status {
    clearLastError();
    const out_ptr = out_directory orelse
        return setLastError(.RINGLOOM_ERR_INVALID_ARGUMENT, "out_directory must not be NULL");
    const out_len = out_directory_len orelse
        return setLastError(.RINGLOOM_ERR_INVALID_ARGUMENT, "out_directory_len must not be NULL");
    out_ptr.* = null;
    out_len.* = 0;

    const handle = requireActiveService(@constCast(service)) orelse
        return .RINGLOOM_ERR_INVALID_ARGUMENT;
    const directory = handle.engine.?.aeron_runtime.directory;
    out_ptr.* = if (directory.len == 0) null else directory.ptr;
    out_len.* = directory.len;
    return .RINGLOOM_OK;
}

export fn ringloom_service_aeron_inbound_stream_id(
    service: ?*const ringloom_service,
    out_stream_id: ?*i32,
) Status {
    clearLastError();
    const out = out_stream_id orelse
        return setLastError(.RINGLOOM_ERR_INVALID_ARGUMENT, "out_stream_id must not be NULL");
    out.* = 0;

    _ = requireActiveService(@constCast(service)) orelse
        return .RINGLOOM_ERR_INVALID_ARGUMENT;
    // Direct-UDP remote sends do not use a service -> broker IPC ingress stream.
    out.* = 0;
    return .RINGLOOM_OK;
}

export fn ringloom_service_publication_connected(
    service: ?*const ringloom_service,
    out_connected: ?*bool,
) Status {
    clearLastError();
    const out = out_connected orelse
        return setLastError(.RINGLOOM_ERR_INVALID_ARGUMENT, "out_connected must not be NULL");
    out.* = false;

    const handle = requireActiveService(@constCast(service)) orelse
        return .RINGLOOM_ERR_INVALID_ARGUMENT;
    out.* = handle.engine.?.aeron_runtime.publicationConnected();
    return .RINGLOOM_OK;
}

export fn ringloom_service_poll_control(
    service: ?*ringloom_service,
    limit: u32,
    out_count: ?*u32,
) Status {
    clearLastError();

    const out = out_count orelse
        return setLastError(.RINGLOOM_ERR_INVALID_ARGUMENT, "out_count must not be NULL");
    out.* = 0;

    const handle = requireActiveService(service) orelse
        return .RINGLOOM_ERR_INVALID_ARGUMENT;

    const engine = handle.engine orelse
        return setLastError(.RINGLOOM_ERR_INVALID_ARGUMENT, "service handle has been destroyed");
    out.* = engine.control_agent.poll(limit);
    return .RINGLOOM_OK;
}

export fn ringloom_service_create_message_consumer(
    service: ?*ringloom_service,
    out_consumer: ?*?*ringloom_message_consumer,
) Status {
    clearLastError();

    const out = out_consumer orelse
        return setLastError(.RINGLOOM_ERR_INVALID_ARGUMENT, "out_consumer must not be NULL");
    out.* = null;

    const service_handle = requireActiveService(service) orelse
        return .RINGLOOM_ERR_INVALID_ARGUMENT;

    if (service_handle.polling_consumer_active.cmpxchgWeak(false, true, .acq_rel, .acquire) != null) {
        return setLastError(
            .RINGLOOM_ERR_INVALID_ARGUMENT,
            "service already has an active polling message consumer",
        );
    }
    errdefer service_handle.polling_consumer_active.store(false, .release);

    const ring_buffer = RingBuffer.init(
        @alignCast(service_handle.engine.?.service_meta.getMessagesBuffer()),
        false,
        null,
        null,
    ) catch |err| return setLastErrorFmt(
        .RINGLOOM_ERR_INTERNAL,
        "failed to initialize service messages ring buffer: {}",
        .{err},
    );

    const consumer = service_handle.allocator.create(MessageConsumerHandle) catch
        return setLastError(.RINGLOOM_ERR_OUT_OF_MEMORY, "native allocation failed");
    consumer.* = MessageConsumerHandle.init(service_handle, ring_buffer);
    service_handle.retain();

    out.* = messageConsumerHandleToOpaque(consumer);
    return .RINGLOOM_OK;
}

export fn ringloom_message_consumer_destroy(consumer: ?*ringloom_message_consumer) void {
    clearLastError();
    const handle = messageConsumerHandleFromOpaque(consumer) orelse return;
    if (handle.closed.cmpxchgWeak(false, true, .acq_rel, .acquire) != null) return;

    handle.service.polling_consumer_active.store(false, .release);
    const service = handle.service;
    const allocator = service.allocator;
    allocator.destroy(handle);
    service.release();
}

export fn ringloom_message_consumer_poll(
    consumer: ?*ringloom_message_consumer,
    handler: ringloom_message_handler_t,
    user_data: ?*anyopaque,
    limit: u32,
    out_count: ?*u32,
) Status {
    clearLastError();

    const out = out_count orelse
        return setLastError(.RINGLOOM_ERR_INVALID_ARGUMENT, "out_count must not be NULL");
    out.* = 0;

    if (handler == null) {
        return setLastError(.RINGLOOM_ERR_INVALID_ARGUMENT, "handler must not be NULL");
    }

    const consumer_handle = requireActiveConsumer(consumer) orelse
        return .RINGLOOM_ERR_INVALID_ARGUMENT;
    progressControlPlane(consumer_handle.service);

    var context = PollDispatchContext{
        .handler = handler,
        .user_data = user_data,
        .out_count = out,
    };
    poll_dispatch_context = &context;
    defer poll_dispatch_context = null;

    _ = consumer_handle.ring_buffer.read(&pollDispatch, limit);
    return .RINGLOOM_OK;
}

export fn ringloom_service_create_metrics_reader(
    service: ?*ringloom_service,
    out_reader: ?*?*ringloom_metrics_reader,
) Status {
    clearLastError();

    const out = out_reader orelse
        return setLastError(.RINGLOOM_ERR_INVALID_ARGUMENT, "out_reader must not be NULL");
    out.* = null;

    const service_handle = requireActiveService(service) orelse
        return .RINGLOOM_ERR_INVALID_ARGUMENT;

    const reader = service_handle.allocator.create(MetricsReaderHandle) catch
        return setLastError(.RINGLOOM_ERR_OUT_OF_MEMORY, "native allocation failed");
    reader.* = MetricsReaderHandle.init(service_handle);
    service_handle.retain();

    out.* = metricsReaderHandleToOpaque(reader);
    return .RINGLOOM_OK;
}

export fn ringloom_metrics_reader_destroy(reader: ?*ringloom_metrics_reader) void {
    clearLastError();
    const handle = metricsReaderHandleFromOpaque(reader) orelse return;
    const service = handle.service;
    const allocator = service.allocator;
    allocator.destroy(handle);
    service.release();
}

export fn ringloom_metrics_reader_counter_count(
    reader: ?*ringloom_metrics_reader,
    out_count: ?*usize,
) Status {
    clearLastError();

    const out = out_count orelse
        return setLastError(.RINGLOOM_ERR_INVALID_ARGUMENT, "out_count must not be NULL");
    out.* = 0;

    const handle = requireMetricsReader(reader) orelse
        return .RINGLOOM_ERR_INVALID_ARGUMENT;
    out.* = handle.service.engine.?.counters.allocatedCount();
    return .RINGLOOM_OK;
}

export fn ringloom_metrics_reader_counter_at(
    reader: ?*ringloom_metrics_reader,
    index: usize,
    out_metric: ?*ringloom_metric_descriptor_t,
) Status {
    clearLastError();

    const out = out_metric orelse
        return setLastError(.RINGLOOM_ERR_INVALID_ARGUMENT, "out_metric must not be NULL");
    out.* = .{
        .name = null,
        .name_len = 0,
        .kind = .RINGLOOM_METRIC_COUNTER,
        .value = 0,
    };

    const handle = requireMetricsReader(reader) orelse
        return .RINGLOOM_ERR_INVALID_ARGUMENT;
    const snapshot = handle.service.engine.?.counters.snapshotAllocatedAt(index, &handle.name_scratch) orelse
        return setLastError(.RINGLOOM_ERR_INVALID_ARGUMENT, "counter index is out of range");

    out.* = .{
        .name = if (snapshot.label.len == 0) null else snapshot.label.ptr,
        .name_len = snapshot.label.len,
        .kind = nativeMetricKind(counters_mod.metricKindFromTypeId(snapshot.type_id)),
        .value = snapshot.value,
    };
    return .RINGLOOM_OK;
}

fn nativeMetricKind(kind: counters_mod.MetricKind) ringloom_metric_kind_t {
    return switch (kind) {
        .counter => .RINGLOOM_METRIC_COUNTER,
        .gauge => .RINGLOOM_METRIC_GAUGE,
    };
}

export fn ringloom_metrics_reader_ring_stats(
    reader: ?*ringloom_metrics_reader,
    ring_name: ?[*]const u8,
    ring_name_len: usize,
    out_stats: ?*ringloom_ring_stats_t,
) Status {
    clearLastError();

    const out = out_stats orelse
        return setLastError(.RINGLOOM_ERR_INVALID_ARGUMENT, "out_stats must not be NULL");
    out.* = .{
        .capacity_bytes = 0,
        .used_bytes = 0,
        .free_bytes = 0,
        .producer_position = 0,
        .consumer_position = 0,
    };

    const handle = requireMetricsReader(reader) orelse
        return .RINGLOOM_ERR_INVALID_ARGUMENT;
    const name = requiredBytes(ring_name, ring_name_len, "ring_name") catch
        return setLastError(.RINGLOOM_ERR_INVALID_ARGUMENT, "ring_name must not be empty");
    const service_meta = handle.service.engine.?.service_meta;

    const buffer = if (std.mem.eql(u8, name, "control"))
        service_meta.getControlBuffer()
    else if (std.mem.eql(u8, name, "messages"))
        service_meta.getMessagesBuffer()
    else
        return setLastError(.RINGLOOM_ERR_INVALID_ARGUMENT, "unknown ring name");

    out.* = ringStatsForBuffer(buffer) catch |err|
        return mapRuntimeError(err, "failed to read ring stats");
    return .RINGLOOM_OK;
}

fn ringStatsForBuffer(buffer: []u8) !ringloom_ring_stats_t {
    var ring = try RingBuffer.init(@alignCast(buffer), false, null, null);
    const capacity = ring.getCapacity();
    const raw_used = ring.size();
    const used = @min(raw_used, capacity);
    const producer = ring.producerPosition();
    const consumer = ring.consumerPosition();

    return .{
        .capacity_bytes = @intCast(capacity),
        .used_bytes = @intCast(used),
        .free_bytes = @intCast(capacity - used),
        .producer_position = if (producer < 0) 0 else @intCast(producer),
        .consumer_position = if (consumer < 0) 0 else @intCast(consumer),
    };
}

export fn ringloom_service_counter_register(
    service: ?*ringloom_service,
    name: ?[*]const u8,
    name_len: usize,
    out_slot: ?*ringloom_metric_slot_t,
) Status {
    return registerServiceMetric(service, name, name_len, .counter, out_slot);
}

export fn ringloom_service_gauge_register(
    service: ?*ringloom_service,
    name: ?[*]const u8,
    name_len: usize,
    out_slot: ?*ringloom_metric_slot_t,
) Status {
    return registerServiceMetric(service, name, name_len, .gauge, out_slot);
}

fn registerServiceMetric(
    service: ?*ringloom_service,
    name: ?[*]const u8,
    name_len: usize,
    kind: counters_mod.MetricKind,
    out_metric_slot: ?*ringloom_metric_slot_t,
) Status {
    clearLastError();
    const out = out_metric_slot orelse
        return setLastError(.RINGLOOM_ERR_INVALID_ARGUMENT, "out_slot must not be NULL");
    out.* = .{
        .metric_id = -1,
        .reserved = 0,
        .value = null,
        .value_len = 0,
    };

    const handle = requireActiveService(service) orelse
        return .RINGLOOM_ERR_INVALID_ARGUMENT;
    const metric_name = requiredBytes(name, name_len, "name") catch
        return setLastError(.RINGLOOM_ERR_INVALID_ARGUMENT, "metric name must not be empty");
    if (metric_name.len > counters_mod.max_label_length) {
        return setLastError(.RINGLOOM_ERR_INVALID_ARGUMENT, "metric name is too long");
    }

    const type_id = counters_mod.encodeTypeId(kind, custom_metric_category);
    const id = handle.engine.?.counters.allocate(type_id, metric_name) orelse
        return setLastError(.RINGLOOM_ERR_OUT_OF_MEMORY, "native metric slots exhausted");
    if (id > std.math.maxInt(i32)) {
        return setLastError(.RINGLOOM_ERR_INTERNAL, "native metric id is out of range");
    }
    const value_ptr = handle.engine.?.counters.allocatedValuePtr(id) catch |err|
        return mapMetricAccessError(err, "metric slot allocation failed");

    out.* = .{
        .metric_id = @intCast(id),
        .reserved = 0,
        .value = value_ptr,
        .value_len = @sizeOf(i64),
    };
    return .RINGLOOM_OK;
}

fn mapMetricAccessError(err: counters_mod.CountersManager.CounterAccessError, message: []const u8) Status {
    return switch (err) {
        error.InvalidCounterId,
        error.CounterNotAllocated,
        => setLastError(.RINGLOOM_ERR_INVALID_ARGUMENT, message),
    };
}

export fn ringloom_service_create_client(
    service: ?*ringloom_service,
    target_service_name: ?[*]const u8,
    target_service_name_len: usize,
    out_client: ?*?*ringloom_client,
) Status {
    clearLastError();

    const out = out_client orelse
        return setLastError(.RINGLOOM_ERR_INVALID_ARGUMENT, "out_client must not be NULL");
    out.* = null;

    const service_handle = requireActiveService(service) orelse
        return .RINGLOOM_ERR_INVALID_ARGUMENT;
    const target_name = requiredBytes(
        target_service_name,
        target_service_name_len,
        "target_service_name",
    ) catch {
        return setLastError(.RINGLOOM_ERR_INVALID_ARGUMENT, "target_service_name must not be empty");
    };

    const client = service_handle.engine.?.createClient(target_name) catch |err|
        return mapRuntimeError(err, "failed to create service client");
    progressControlPlane(service_handle);

    const client_handle = service_handle.allocator.create(ClientHandle) catch
        return setLastError(.RINGLOOM_ERR_OUT_OF_MEMORY, "native allocation failed");
    client_handle.* = ClientHandle.init(service_handle, client);
    service_handle.retain();

    out.* = clientHandleToOpaque(client_handle);
    return .RINGLOOM_OK;
}

export fn ringloom_client_destroy(client: ?*ringloom_client) void {
    clearLastError();
    const handle = clientHandleFromOpaque(client) orelse return;
    if (handle.closed.cmpxchgWeak(false, true, .acq_rel, .acquire) != null) return;

    if (!handle.service.destroy_requested.load(.acquire) and handle.service.engine != null) {
        handle.clearLifecycleHandler();
    } else {
        handle.lifecycle_handler = null;
        handle.lifecycle_user_data = null;
    }
    const service = handle.service;
    const allocator = service.allocator;
    allocator.destroy(handle);
    service.release();
}

export fn ringloom_client_set_lifecycle_handler(
    client: ?*ringloom_client,
    handler: ringloom_service_lifecycle_handler_t,
    user_data: ?*anyopaque,
) Status {
    clearLastError();
    const handle = requireActiveClient(client) orelse
        return .RINGLOOM_ERR_INVALID_ARGUMENT;

    handle.lifecycle_handler = handler;
    handle.lifecycle_user_data = if (handler == null) null else user_data;
    handle.client.setLifecycleHandler(
        if (handler == null) null else lifecycleBridge,
        if (handler == null) null else handle,
    );
    return .RINGLOOM_OK;
}

export fn ringloom_client_send(
    client: ?*ringloom_client,
    payload: ?[*]const u8,
    payload_len: usize,
) Status {
    clearLastError();
    const handle = requireActiveClient(client) orelse
        return .RINGLOOM_ERR_INVALID_ARGUMENT;
    progressControlPlane(handle.service);
    const payload_slice = payloadBytes(payload, payload_len) orelse
        return setLastError(.RINGLOOM_ERR_INVALID_ARGUMENT, "payload pointer is NULL for non-zero payload length");

    handle.client.send(payload_slice) catch |err|
        return mapRuntimeError(err, "failed to send payload");
    return .RINGLOOM_OK;
}

export fn ringloom_client_send_message(
    client: ?*ringloom_client,
    template_id: u16,
    payload: ?[*]const u8,
    payload_len: usize,
) Status {
    clearLastError();
    const handle = requireActiveClient(client) orelse
        return .RINGLOOM_ERR_INVALID_ARGUMENT;
    progressControlPlane(handle.service);
    const payload_slice = payloadBytes(payload, payload_len) orelse
        return setLastError(.RINGLOOM_ERR_INVALID_ARGUMENT, "payload pointer is NULL for non-zero payload length");

    handle.client.sendMessage(template_id, payload_slice) catch |err|
        return mapRuntimeError(err, "failed to send template-aware payload");
    return .RINGLOOM_OK;
}

export fn ringloom_client_send_message_request(
    client: ?*ringloom_client,
    template_id: u16,
    correlation_id: i64,
    payload: ?[*]const u8,
    payload_len: usize,
) Status {
    clearLastError();
    const handle = requireActiveClient(client) orelse
        return .RINGLOOM_ERR_INVALID_ARGUMENT;
    progressControlPlane(handle.service);
    const payload_slice = payloadBytes(payload, payload_len) orelse
        return setLastError(.RINGLOOM_ERR_INVALID_ARGUMENT, "payload pointer is NULL for non-zero payload length");

    handle.client.sendMessageRequest(template_id, correlation_id, payload_slice) catch |err|
        return mapRuntimeError(err, "failed to send request payload");
    return .RINGLOOM_OK;
}

export fn ringloom_client_send_to(
    client: ?*ringloom_client,
    target_node_id: i16,
    target_service_id: i32,
    payload: ?[*]const u8,
    payload_len: usize,
) Status {
    clearLastError();
    const handle = requireActiveClient(client) orelse
        return .RINGLOOM_ERR_INVALID_ARGUMENT;
    progressControlPlane(handle.service);
    const payload_slice = payloadBytes(payload, payload_len) orelse
        return setLastError(.RINGLOOM_ERR_INVALID_ARGUMENT, "payload pointer is NULL for non-zero payload length");

    handle.client.sendTo(target_node_id, target_service_id, payload_slice) catch |err|
        return mapRuntimeError(err, "failed to send payload to target service id");
    return .RINGLOOM_OK;
}

export fn ringloom_client_send_to_message(
    client: ?*ringloom_client,
    target_node_id: i16,
    target_service_id: i32,
    template_id: u16,
    payload: ?[*]const u8,
    payload_len: usize,
) Status {
    clearLastError();
    const handle = requireActiveClient(client) orelse
        return .RINGLOOM_ERR_INVALID_ARGUMENT;
    progressControlPlane(handle.service);
    const payload_slice = payloadBytes(payload, payload_len) orelse
        return setLastError(.RINGLOOM_ERR_INVALID_ARGUMENT, "payload pointer is NULL for non-zero payload length");

    handle.client.sendToMessage(target_node_id, target_service_id, template_id, payload_slice) catch |err|
        return mapRuntimeError(err, "failed to send template-aware payload to target service id");
    return .RINGLOOM_OK;
}

export fn ringloom_client_send_to_message_request(
    client: ?*ringloom_client,
    target_node_id: i16,
    target_service_id: i32,
    template_id: u16,
    correlation_id: i64,
    payload: ?[*]const u8,
    payload_len: usize,
) Status {
    clearLastError();
    const handle = requireActiveClient(client) orelse
        return .RINGLOOM_ERR_INVALID_ARGUMENT;
    progressControlPlane(handle.service);
    const payload_slice = payloadBytes(payload, payload_len) orelse
        return setLastError(.RINGLOOM_ERR_INVALID_ARGUMENT, "payload pointer is NULL for non-zero payload length");

    handle.client.sendToMessageRequest(
        target_node_id,
        target_service_id,
        template_id,
        correlation_id,
        payload_slice,
    ) catch |err| return mapRuntimeError(err, "failed to send request payload to target service id");
    return .RINGLOOM_OK;
}

export fn ringloom_client_send_to_leader(
    client: ?*ringloom_client,
    payload: ?[*]const u8,
    payload_len: usize,
) Status {
    clearLastError();
    const handle = requireActiveClient(client) orelse
        return .RINGLOOM_ERR_INVALID_ARGUMENT;
    progressControlPlane(handle.service);
    const payload_slice = payloadBytes(payload, payload_len) orelse
        return setLastError(.RINGLOOM_ERR_INVALID_ARGUMENT, "payload pointer is NULL for non-zero payload length");

    handle.client.sendToLeader(payload_slice) catch |err|
        return mapRuntimeError(err, "failed to send payload to leader");
    return .RINGLOOM_OK;
}

export fn ringloom_client_send_to_leader_message(
    client: ?*ringloom_client,
    template_id: u16,
    payload: ?[*]const u8,
    payload_len: usize,
) Status {
    clearLastError();
    const handle = requireActiveClient(client) orelse
        return .RINGLOOM_ERR_INVALID_ARGUMENT;
    progressControlPlane(handle.service);
    const payload_slice = payloadBytes(payload, payload_len) orelse
        return setLastError(.RINGLOOM_ERR_INVALID_ARGUMENT, "payload pointer is NULL for non-zero payload length");

    handle.client.sendToLeaderMessage(template_id, payload_slice) catch |err|
        return mapRuntimeError(err, "failed to send template-aware payload to leader");
    return .RINGLOOM_OK;
}

export fn ringloom_client_send_to_leader_message_request(
    client: ?*ringloom_client,
    template_id: u16,
    correlation_id: i64,
    payload: ?[*]const u8,
    payload_len: usize,
) Status {
    clearLastError();
    const handle = requireActiveClient(client) orelse
        return .RINGLOOM_ERR_INVALID_ARGUMENT;
    progressControlPlane(handle.service);
    const payload_slice = payloadBytes(payload, payload_len) orelse
        return setLastError(.RINGLOOM_ERR_INVALID_ARGUMENT, "payload pointer is NULL for non-zero payload length");

    handle.client.sendToLeaderMessageRequest(template_id, correlation_id, payload_slice) catch |err|
        return mapRuntimeError(err, "failed to send request payload to leader");
    return .RINGLOOM_OK;
}

export fn ringloom_client_list_targets(
    client: ?*ringloom_client,
    out_targets: ?[*]ringloom_client_target_t,
    target_capacity: usize,
    out_count: ?*usize,
) Status {
    clearLastError();

    const out = out_count orelse
        return setLastError(.RINGLOOM_ERR_INVALID_ARGUMENT, "out_count must not be NULL");
    const handle = requireActiveClient(client) orelse
        return .RINGLOOM_ERR_INVALID_ARGUMENT;
    progressControlPlane(handle.service);

    const total = if (out_targets) |targets_ptr|
        handle.client.copyTargetInstances(targets_ptr[0..target_capacity])
    else blk: {
        if (target_capacity != 0) {
            return setLastError(
                .RINGLOOM_ERR_INVALID_ARGUMENT,
                "out_targets must not be NULL when target_capacity is non-zero",
            );
        }
        break :blk handle.client.copyTargetInstances(&[_]ringloom_client_target_t{});
    };

    out.* = total;
    return .RINGLOOM_OK;
}

export fn ringloom_client_last_aeron_send_status(
    client: ?*ringloom_client,
    out_status: ?*ringloom_aeron_publication_status_t,
) Status {
    clearLastError();
    const out = out_status orelse
        return setLastError(.RINGLOOM_ERR_INVALID_ARGUMENT, "out_status must not be NULL");
    out.* = .RINGLOOM_AERON_PUBLICATION_UNKNOWN;

    const handle = requireActiveClient(client) orelse
        return .RINGLOOM_ERR_INVALID_ARGUMENT;
    out.* = nativePublicationStatus(handle.client.remotePublicationHealth().last_status);
    return .RINGLOOM_OK;
}

fn nativePublicationStatus(status: ServiceClient.RemotePublicationStatus) ringloom_aeron_publication_status_t {
    return switch (status) {
        .unknown => .RINGLOOM_AERON_PUBLICATION_UNKNOWN,
        .claimed => .RINGLOOM_AERON_PUBLICATION_CLAIMED,
        .not_connected => .RINGLOOM_AERON_PUBLICATION_NOT_CONNECTED,
        .back_pressured => .RINGLOOM_AERON_PUBLICATION_BACK_PRESSURED,
        .admin_action => .RINGLOOM_AERON_PUBLICATION_ADMIN_ACTION,
        .closed => .RINGLOOM_AERON_PUBLICATION_CLOSED,
        .max_position_exceeded => .RINGLOOM_AERON_PUBLICATION_MAX_POSITION_EXCEEDED,
        .failed => .RINGLOOM_AERON_PUBLICATION_FAILED,
    };
}

export fn ringloom_client_try_claim(
    client: ?*ringloom_client,
    template_id: u16,
    payload_len: usize,
    out_claim: ?*ringloom_buffer_claim_t,
) Status {
    clearLastError();

    const out = prepareClaimOut(out_claim) orelse
        return .RINGLOOM_ERR_INVALID_ARGUMENT;

    const handle = requireActiveClient(client) orelse
        return .RINGLOOM_ERR_INVALID_ARGUMENT;
    progressControlPlane(handle.service);

    const claim = handle.client.tryClaim(template_id, payload_len) catch |err|
        return mapRuntimeError(err, "failed to claim writable send buffer");

    return writeClaimOut(handle.service.allocator, out, claim);
}

export fn ringloom_client_try_claim_request(
    client: ?*ringloom_client,
    template_id: u16,
    correlation_id: i64,
    payload_len: usize,
    out_claim: ?*ringloom_buffer_claim_t,
) Status {
    clearLastError();

    const out = prepareClaimOut(out_claim) orelse
        return .RINGLOOM_ERR_INVALID_ARGUMENT;

    const handle = requireActiveClient(client) orelse
        return .RINGLOOM_ERR_INVALID_ARGUMENT;
    progressControlPlane(handle.service);

    const claim = handle.client.tryClaimRequest(template_id, correlation_id, payload_len) catch |err|
        return mapRuntimeError(err, "failed to claim writable request send buffer");

    return writeClaimOut(handle.service.allocator, out, claim);
}

export fn ringloom_client_try_claim_to(
    client: ?*ringloom_client,
    target_node_id: i16,
    target_service_id: i32,
    template_id: u16,
    payload_len: usize,
    out_claim: ?*ringloom_buffer_claim_t,
) Status {
    clearLastError();

    const out = prepareClaimOut(out_claim) orelse
        return .RINGLOOM_ERR_INVALID_ARGUMENT;

    const handle = requireActiveClient(client) orelse
        return .RINGLOOM_ERR_INVALID_ARGUMENT;
    progressControlPlane(handle.service);

    const claim = handle.client.tryClaimTo(target_node_id, target_service_id, template_id, payload_len) catch |err|
        return mapRuntimeError(err, "failed to claim writable target send buffer");

    return writeClaimOut(handle.service.allocator, out, claim);
}

export fn ringloom_client_try_claim_to_request(
    client: ?*ringloom_client,
    target_node_id: i16,
    target_service_id: i32,
    template_id: u16,
    correlation_id: i64,
    payload_len: usize,
    out_claim: ?*ringloom_buffer_claim_t,
) Status {
    clearLastError();

    const out = prepareClaimOut(out_claim) orelse
        return .RINGLOOM_ERR_INVALID_ARGUMENT;

    const handle = requireActiveClient(client) orelse
        return .RINGLOOM_ERR_INVALID_ARGUMENT;
    progressControlPlane(handle.service);

    const claim = handle.client.tryClaimToRequest(
        target_node_id,
        target_service_id,
        template_id,
        correlation_id,
        payload_len,
    ) catch |err| return mapRuntimeError(err, "failed to claim writable target request send buffer");

    return writeClaimOut(handle.service.allocator, out, claim);
}

export fn ringloom_client_try_claim_to_leader(
    client: ?*ringloom_client,
    template_id: u16,
    payload_len: usize,
    out_claim: ?*ringloom_buffer_claim_t,
) Status {
    clearLastError();

    const out = prepareClaimOut(out_claim) orelse
        return .RINGLOOM_ERR_INVALID_ARGUMENT;

    const handle = requireActiveClient(client) orelse
        return .RINGLOOM_ERR_INVALID_ARGUMENT;
    progressControlPlane(handle.service);

    const claim = handle.client.tryClaimToLeader(template_id, payload_len) catch |err|
        return mapRuntimeError(err, "failed to claim writable leader send buffer");

    return writeClaimOut(handle.service.allocator, out, claim);
}

export fn ringloom_client_try_claim_to_leader_request(
    client: ?*ringloom_client,
    template_id: u16,
    correlation_id: i64,
    payload_len: usize,
    out_claim: ?*ringloom_buffer_claim_t,
) Status {
    clearLastError();

    const out = prepareClaimOut(out_claim) orelse
        return .RINGLOOM_ERR_INVALID_ARGUMENT;

    const handle = requireActiveClient(client) orelse
        return .RINGLOOM_ERR_INVALID_ARGUMENT;
    progressControlPlane(handle.service);

    const claim = handle.client.tryClaimToLeaderRequest(template_id, correlation_id, payload_len) catch |err|
        return mapRuntimeError(err, "failed to claim writable leader request send buffer");

    return writeClaimOut(handle.service.allocator, out, claim);
}

fn prepareClaimOut(out_claim: ?*ringloom_buffer_claim_t) ?*ringloom_buffer_claim_t {
    const out = out_claim orelse {
        _ = setLastError(.RINGLOOM_ERR_INVALID_ARGUMENT, "out_claim must not be NULL");
        return null;
    };
    out.* = .{
        .payload = null,
        .payload_len = 0,
        ._ring_buffer = 0,
        ._header_index = 0,
        ._record_length = 0,
        ._active = 0,
    };
    return out;
}

fn writeClaimOut(
    allocator: std.mem.Allocator,
    out: *ringloom_buffer_claim_t,
    claim: ServiceClient.SendClaim,
) Status {
    const handle = allocator.create(ClaimHandle) catch {
        var mutable = claim;
        mutable.abort();
        return setLastError(.RINGLOOM_ERR_OUT_OF_MEMORY, "native allocation failed");
    };
    handle.* = ClaimHandle.init(allocator, claim);
    out.* = .{
        .payload = if (claim.payload.len == 0) null else claim.payload.ptr,
        .payload_len = claim.payload.len,
        ._ring_buffer = @intFromPtr(handle),
        ._header_index = 0,
        ._record_length = claim_handle_record_length,
        ._active = 1,
    };
    return .RINGLOOM_OK;
}

fn clearClaim(claim: *ringloom_buffer_claim_t) void {
    claim.payload = null;
    claim.payload_len = 0;
    claim._ring_buffer = 0;
    claim._header_index = 0;
    claim._record_length = 0;
    claim._active = 0;
}

export fn ringloom_buffer_claim_commit(claim: ?*ringloom_buffer_claim_t) Status {
    clearLastError();
    const claim_ptr = claim orelse
        return setLastError(.RINGLOOM_ERR_INVALID_ARGUMENT, "claim must not be NULL");
    if (claim_ptr._active == 0 or claim_ptr._ring_buffer == 0) {
        return setLastError(.RINGLOOM_ERR_CLAIM_NOT_ACTIVE, "claim is not active");
    }

    if (claim_ptr._record_length == claim_handle_record_length) {
        const handle: *ClaimHandle = @ptrFromInt(claim_ptr._ring_buffer);
        handle.send_claim.commit();
        const allocator = handle.allocator;
        allocator.destroy(handle);
        clearClaim(claim_ptr);
        return .RINGLOOM_OK;
    }

    const ring_buffer: *RingBuffer = @ptrFromInt(claim_ptr._ring_buffer);
    var internal = RingBuffer.Claim{
        .buffer = &[_]u8{},
        .ring_buffer = ring_buffer,
        .header_index = claim_ptr._header_index,
        .record_length = claim_ptr._record_length,
    };
    internal.commit();
    clearClaim(claim_ptr);
    return .RINGLOOM_OK;
}

export fn ringloom_buffer_claim_abort(claim: ?*ringloom_buffer_claim_t) Status {
    clearLastError();
    const claim_ptr = claim orelse
        return setLastError(.RINGLOOM_ERR_INVALID_ARGUMENT, "claim must not be NULL");
    if (claim_ptr._active == 0 or claim_ptr._ring_buffer == 0) {
        return setLastError(.RINGLOOM_ERR_CLAIM_NOT_ACTIVE, "claim is not active");
    }

    if (claim_ptr._record_length == claim_handle_record_length) {
        const handle: *ClaimHandle = @ptrFromInt(claim_ptr._ring_buffer);
        handle.send_claim.abort();
        const allocator = handle.allocator;
        allocator.destroy(handle);
        clearClaim(claim_ptr);
        return .RINGLOOM_OK;
    }

    const ring_buffer: *RingBuffer = @ptrFromInt(claim_ptr._ring_buffer);
    var internal = RingBuffer.Claim{
        .buffer = &[_]u8{},
        .ring_buffer = ring_buffer,
        .header_index = claim_ptr._header_index,
        .record_length = claim_ptr._record_length,
    };
    internal.abort();
    clearClaim(claim_ptr);
    return .RINGLOOM_OK;
}

// ── Persistent topics ──────────────────────────────────────────────────

pub const ringloom_topic_publisher = opaque {};
pub const ringloom_topic_subscription = opaque {};

pub const ringloom_topic_config_t = extern struct {
    size: u32,
    roll_scheme: [16]u8,
    retention_cycles: u32,
    flags: u32,
};

pub const ringloom_topic_start_t = enum(c_int) {
    RINGLOOM_TOPIC_START_EARLIEST = 0,
    RINGLOOM_TOPIC_START_LATEST = 1,
};

const TopicPublisherHandle = struct {
    allocator: std.mem.Allocator,
    publisher: topic_publisher_mod.TopicPublisher,
    /// Engine that owns the publisher registry + aeron runtime; used at close
    /// to unregister from the registry. Null only if allocation failed mid-way.
    engine: ?*RingLoomEngine = null,
};

const TopicSubscriptionHandle = struct {
    allocator: std.mem.Allocator,
    subscription: topic_subscription_mod.TopicSubscription,
};

const topic_publisher_mod = @import("topics/topic_publisher.zig");
const topic_subscription_mod = @import("topics/topic_subscription.zig");
const topic_publisher_registry_mod = @import("topics/topic_publisher_registry.zig");
const topic_messages = ringloom_common.message.topic_control_messages;

fn topicConfigFromC(cfg: *const ringloom_topic_config_t) ringloom_common.topics.TopicConfig {
    var out = ringloom_common.topics.TopicConfig{};
    out.retention_cycles = cfg.retention_cycles;
    out.flags = cfg.flags;
    const n = @min(cfg.roll_scheme.len, out.roll_scheme_name.len);
    @memcpy(out.roll_scheme_name[0..n], cfg.roll_scheme[0..n]);
    return out;
}

export fn ringloom_register_topic_publication(
    client: ?*ringloom_client,
    cfg: ?*const ringloom_topic_config_t,
    name: ?[*]const u8,
    name_len: usize,
    out_publisher: ?*?*ringloom_topic_publisher,
) Status {
    clearLastError();
    const client_handle = clientHandleFromOpaque(client) orelse
        return setLastError(.RINGLOOM_ERR_INVALID_ARGUMENT, "client must not be NULL");
    const cfg_ptr = cfg orelse
        return setLastError(.RINGLOOM_ERR_INVALID_ARGUMENT, "cfg must not be NULL");
    const name_ptr = name orelse
        return setLastError(.RINGLOOM_ERR_INVALID_ARGUMENT, "name must not be NULL");
    const out = out_publisher orelse
        return setLastError(.RINGLOOM_ERR_INVALID_ARGUMENT, "out_publisher must not be NULL");
    out.* = null;

    const engine = client_handle.service.engine orelse
        return setLastError(.RINGLOOM_ERR_INTERNAL, "service engine is not running");
    const name_slice = if (name_len == 0) "" else name_ptr[0..name_len];
    const topic_cfg = topicConfigFromC(cfg_ptr);

    const publisher = engine.registerTopicPublication(topic_cfg, name_slice) catch |err| return switch (err) {
        error.TopicConfigMismatch => setLastError(.RINGLOOM_ERR_TOPIC_CONFIG_MISMATCH, "topic config mismatch or id collision"),
        error.TopicDisabled => setLastError(.RINGLOOM_ERR_TOPIC_DISABLED, "topics disabled on broker"),
        error.RegistrationTimeout => setLastError(.RINGLOOM_ERR_REGISTRATION_TIMEOUT, "topic registration timed out"),
        error.SendFailed => setLastError(.RINGLOOM_ERR_INTERNAL, "failed to send topic registration"),
        error.LeaderPublicationFailed => setLastError(.RINGLOOM_ERR_INTERNAL, "failed to open topic leader IPC publication"),
        error.ControlAgentThreaded, error.RegistrationFailed => setLastError(.RINGLOOM_ERR_INTERNAL, "topic registration failed"),
    };

    const allocator = topic_publisher_handle_allocator.allocator();
    const handle = allocator.create(TopicPublisherHandle) catch
        return setLastError(.RINGLOOM_ERR_OUT_OF_MEMORY, "native allocation failed");
    handle.* = .{
        .allocator = allocator,
        .engine = engine,
        .publisher = publisher,
    };
    // Register in the engine's publisher registry so ack-feedback (template
    //    15) and leader-change (template 13) frames reach this publisher.
    engine.topic_publishers.register(&handle.publisher) catch {};
    out.* = @ptrCast(handle);
    return .RINGLOOM_OK;
}

var topic_publisher_handle_allocator: std.heap.SafeAllocator = .init(std.heap.page_allocator, .{});
var topic_subscription_handle_allocator: std.heap.SafeAllocator = .init(std.heap.page_allocator, .{});

fn topicPublisherFromOpaque(p: ?*ringloom_topic_publisher) ?*TopicPublisherHandle {
    return if (p) |ptr| @ptrCast(@alignCast(ptr)) else null;
}

export fn ringloom_publish_to_topic(
    publisher: ?*ringloom_topic_publisher,
    payload: ?[*]const u8,
    payload_len: usize,
    correlation_id: i64,
    ack_mode: u8,
    out_index: ?*u64,
) Status {
    clearLastError();
    const handle = topicPublisherFromOpaque(publisher) orelse
        return setLastError(.RINGLOOM_ERR_INVALID_ARGUMENT, "publisher must not be NULL");
    const payload_ptr = if (payload_len == 0) null else payload orelse return setLastError(.RINGLOOM_ERR_INVALID_ARGUMENT, "payload must not be NULL");

    const mode = ringloom_common.topics.AckMode.fromU8(ack_mode) orelse .fire_and_forget;
    const payload_slice = if (payload_ptr) |p| p[0..payload_len] else &[0]u8{};

    // Drive the Aeron conductor (agent invoker mode) so the leader IPC
    // publication stays linked to the co-located broker's topic subscription.
    // Mirrors the service-client send path, which invokes the conductor per send.
    if (handle.engine) |engine| {
        _ = engine.aeron_runtime.doWork() catch {};
    }
    const result = handle.publisher.publish(payload_slice, correlation_id, mode);

    // On success, surface the per-topic sequence token assigned at offer time
    // so replicate_once callers can poll isAcked(token). The token is stable and
    // client-comparable (unlike the broker queue index).
    if (out_index) |idx| idx.* = if (result == .ok) handle.publisher.lastAssignedToken() else 0;
    return switch (result) {
        .ok => .RINGLOOM_OK,
        .not_connected => .RINGLOOM_ERR_PEER_DISCONNECTED,
        .back_pressured => .RINGLOOM_ERR_BACKPRESSURE,
        else => .RINGLOOM_ERR_INTERNAL,
    };
}

export fn ringloom_topic_is_acked(
    publisher: ?*ringloom_topic_publisher,
    publish_index: u64,
) c_int {
    const handle = topicPublisherFromOpaque(publisher) orelse return 0;
    return if (handle.publisher.isAcked(publish_index)) @as(c_int, 1) else @as(c_int, 0);
}

export fn ringloom_unregister_topic_publication(
    publisher: ?*ringloom_topic_publisher,
) void {
    const handle = topicPublisherFromOpaque(publisher) orelse return;
    // Unregister from the engine's publisher registry so ack-feedback stops
    // targeting this (now-closing) publisher.
    if (handle.engine) |engine| {
        engine.topic_publishers.unregister(&handle.publisher);
        // Best-effort: notify the broker (template 11; broker no-ops on full mesh).
        control_agent_mod.unregisterTopicPublication(engine.broker_meta, engine.service_id, handle.publisher.topic_id) catch {};
    }
    handle.publisher.deinit();
    handle.allocator.destroy(handle);
}

fn topicSubscriptionFromOpaque(s: ?*ringloom_topic_subscription) ?*TopicSubscriptionHandle {
    return if (s) |ptr| @ptrCast(@alignCast(ptr)) else null;
}

export fn ringloom_subscribe_topic(
    client: ?*ringloom_client,
    name: ?[*]const u8,
    name_len: usize,
    start: ringloom_topic_start_t,
    out_subscription: ?*?*ringloom_topic_subscription,
) Status {
    clearLastError();
    const client_handle = clientHandleFromOpaque(client) orelse
        return setLastError(.RINGLOOM_ERR_INVALID_ARGUMENT, "client must not be NULL");
    const name_ptr = name orelse
        return setLastError(.RINGLOOM_ERR_INVALID_ARGUMENT, "name must not be NULL");
    const out = out_subscription orelse
        return setLastError(.RINGLOOM_ERR_INVALID_ARGUMENT, "out_subscription must not be NULL");
    out.* = null;

    const engine = client_handle.service.engine orelse
        return setLastError(.RINGLOOM_ERR_INTERNAL, "service engine is not running");
    const name_slice = if (name_len == 0) "" else name_ptr[0..name_len];
    const start_position: topic_messages.StartPosition = switch (start) {
        .RINGLOOM_TOPIC_START_EARLIEST => .earliest,
        .RINGLOOM_TOPIC_START_LATEST => .latest,
    };

    // 1. Send SubscribeTopic (template 9) and await the response (template 10).
    control_agent_mod.subscribeTopic(engine.broker_meta, engine.service_id, start_position, name_slice) catch
        return setLastError(.RINGLOOM_ERR_INTERNAL, "failed to send topic subscription");
    const response = control_agent_mod.waitForTopicSubscriptionResponse(engine.service_meta, 5000) catch
        return setLastError(.RINGLOOM_ERR_REGISTRATION_TIMEOUT, "topic subscription timed out");

    const ok_status: u8 = @backingInt(topic_messages.SubscriptionStatus.ok);
    if (response.status != ok_status) {
        return switch (response.status) {
            @backingInt(topic_messages.SubscriptionStatus.unknown_topic) => setLastError(.RINGLOOM_ERR_TOPIC_UNKNOWN, "unknown topic"),
            @backingInt(topic_messages.SubscriptionStatus.disabled) => setLastError(.RINGLOOM_ERR_TOPIC_DISABLED, "topics disabled on broker"),
            else => setLastError(.RINGLOOM_ERR_INTERNAL, "topic subscription failed"),
        };
    }

    // 2. Open a tailer on the broker-supplied queue_dir. The response's queue_dir
    //    borrows into the control ring buffer; dup it onto a stable allocation.
    const allocator = topic_subscription_handle_allocator.allocator();
    const queue_dir = allocator.dupe(u8, response.queueDir()) catch
        return setLastError(.RINGLOOM_ERR_OUT_OF_MEMORY, "failed to copy queue_dir");

    const sub = topic_subscription_mod.TopicSubscription.open(
        allocator,
        queue_dir,
        response.start_index,
        // Geometry from the broker response (roll scheme etc.); the tailer
        // opens the existing queue file matching this geometry.
        response.geometry,
    ) catch {
        allocator.free(queue_dir);
        return setLastError(.RINGLOOM_ERR_INTERNAL, "failed to open topic tailer");
    };

    const handle = allocator.create(TopicSubscriptionHandle) catch {
        allocator.free(queue_dir);
        return setLastError(.RINGLOOM_ERR_OUT_OF_MEMORY, "native allocation failed");
    };
    handle.* = .{ .allocator = allocator, .subscription = sub };
    // Store the topic_id on the subscription for the accessor.
    handle.subscription.topic_id = response.topic_id;
    out.* = @ptrCast(handle);
    return .RINGLOOM_OK;
}

export fn ringloom_topic_poll(
    subscription: ?*ringloom_topic_subscription,
    out_payload: ?*?[*]const u8,
    out_len: ?*usize,
    out_index: ?*i64,
) Status {
    clearLastError();
    const handle = topicSubscriptionFromOpaque(subscription) orelse
        return setLastError(.RINGLOOM_ERR_INVALID_ARGUMENT, "subscription must not be NULL");
    const payload_out = out_payload orelse
        return setLastError(.RINGLOOM_ERR_INVALID_ARGUMENT, "out_payload must not be NULL");
    const len_out = out_len orelse
        return setLastError(.RINGLOOM_ERR_INVALID_ARGUMENT, "out_len must not be NULL");

    if (handle.subscription.poll() catch return .RINGLOOM_ERR_INTERNAL) |msg| {
        payload_out.* = msg.ptr;
        len_out.* = msg.len;
        if (out_index) |idx| idx.* = @intCast(handle.subscription.index());
        return .RINGLOOM_OK;
    }
    payload_out.* = null;
    len_out.* = 0;
    return .RINGLOOM_NOT_READY;
}

export fn ringloom_topic_subscription_maintenance_poll(
    subscription: ?*ringloom_topic_subscription,
    max_work_units: c_int,
) Status {
    clearLastError();
    if (max_work_units <= 0) return .RINGLOOM_OK;
    const handle = topicSubscriptionFromOpaque(subscription) orelse
        return setLastError(.RINGLOOM_ERR_INVALID_ARGUMENT, "subscription must not be NULL");
    handle.subscription.maintenancePoll(@intCast(max_work_units));
    return .RINGLOOM_OK;
}

export fn ringloom_unsubscribe_topic(
    subscription: ?*ringloom_topic_subscription,
) void {
    const handle = topicSubscriptionFromOpaque(subscription) orelse return;
    handle.subscription.deinit();
    handle.allocator.destroy(handle);
}

// ── Topic accessors ────────────────────────────────────────────────────

export fn ringloom_topic_publisher_id(publisher: ?*const ringloom_topic_publisher) u64 {
    const handle = topicPublisherFromOpaque(@constCast(publisher)) orelse return 0;
    return handle.publisher.topic_id;
}

export fn ringloom_topic_publisher_leader_epoch(publisher: ?*const ringloom_topic_publisher) u64 {
    const handle = topicPublisherFromOpaque(@constCast(publisher)) orelse return 0;
    return handle.publisher.leader_epoch;
}

/// Highest of this publisher's tokens acknowledged as replicated; every token
/// <= the returned value is acked.
export fn ringloom_topic_publisher_replicated_count(publisher: ?*const ringloom_topic_publisher) u64 {
    const handle = topicPublisherFromOpaque(@constCast(publisher)) orelse return 0;
    return handle.publisher.ackedSeq();
}

export fn ringloom_topic_subscription_id(subscription: ?*const ringloom_topic_subscription) u64 {
    const handle = topicSubscriptionFromOpaque(@constCast(subscription)) orelse return 0;
    return handle.subscription.topic_id;
}

export fn ringloom_status_string(status: Status) [*:0]const u8 {
    return switch (status) {
        .RINGLOOM_OK => "ok",
        .RINGLOOM_ERR_INVALID_ARGUMENT => "invalid_argument",
        .RINGLOOM_ERR_OUT_OF_MEMORY => "out_of_memory",
        .RINGLOOM_ERR_BROKER_NOT_FOUND => "broker_not_found",
        .RINGLOOM_ERR_REGISTRATION_TIMEOUT => "registration_timeout",
        .RINGLOOM_ERR_BUFFER_FULL => "buffer_full",
        .RINGLOOM_ERR_NO_AVAILABLE_INSTANCE => "no_available_instance",
        .RINGLOOM_ERR_BACKPRESSURE => "backpressure",
        .RINGLOOM_ERR_PEER_DISCONNECTED => "peer_disconnected",
        .RINGLOOM_ERR_CLAIM_NOT_ACTIVE => "claim_not_active",
        .RINGLOOM_ERR_MESSAGE_TOO_LONG => "message_too_long",
        .RINGLOOM_NOT_READY => "not_ready",
        .RINGLOOM_ERR_TOPIC_LEADER_UNAVAILABLE => "topic_leader_unavailable",
        .RINGLOOM_ERR_TOPIC_DISABLED => "topic_disabled",
        .RINGLOOM_ERR_TOPIC_CONFIG_MISMATCH => "topic_config_mismatch",
        .RINGLOOM_ERR_TOPIC_UNKNOWN => "topic_unknown",
        .RINGLOOM_KV_MISSING => "RINGLOOM_KV_MISSING",
        .RINGLOOM_ERR_KV_BUFFER_TOO_SMALL => "RINGLOOM_ERR_KV_BUFFER_TOO_SMALL",
        .RINGLOOM_ERR_KV_NOT_WRITER => "RINGLOOM_ERR_KV_NOT_WRITER",
        .RINGLOOM_ERR_KV_HELD_BY_OTHER => "RINGLOOM_ERR_KV_HELD_BY_OTHER",
        .RINGLOOM_ERR_KV_NOT_AVAILABLE_LOCALLY => "RINGLOOM_ERR_KV_NOT_AVAILABLE_LOCALLY",
        .RINGLOOM_ERR_KV_ENGINE_MISMATCH => "RINGLOOM_ERR_KV_ENGINE_MISMATCH",
        .RINGLOOM_ERR_KV_KEY_TOO_LARGE => "RINGLOOM_ERR_KV_KEY_TOO_LARGE",
        .RINGLOOM_ERR_KV_VALUE_TOO_LARGE => "RINGLOOM_ERR_KV_VALUE_TOO_LARGE",
        .RINGLOOM_ERR_KV_BATCH_FULL => "RINGLOOM_ERR_KV_BATCH_FULL",
        .RINGLOOM_ERR_INTERNAL => "internal",
    };
}

export fn ringloom_aeron_publication_status_string(status: ringloom_aeron_publication_status_t) [*:0]const u8 {
    return switch (status) {
        .RINGLOOM_AERON_PUBLICATION_UNKNOWN => "unknown",
        .RINGLOOM_AERON_PUBLICATION_CLAIMED => "claimed",
        .RINGLOOM_AERON_PUBLICATION_NOT_CONNECTED => "not_connected",
        .RINGLOOM_AERON_PUBLICATION_BACK_PRESSURED => "back_pressured",
        .RINGLOOM_AERON_PUBLICATION_ADMIN_ACTION => "admin_action",
        .RINGLOOM_AERON_PUBLICATION_CLOSED => "closed",
        .RINGLOOM_AERON_PUBLICATION_MAX_POSITION_EXCEEDED => "max_position_exceeded",
        .RINGLOOM_AERON_PUBLICATION_FAILED => "failed",
    };
}

export fn ringloom_last_error_message() [*:0]const u8 {
    return @ptrCast(&last_error_message_buffer);
}

comptime {
    std.debug.assert(@offsetOf(ringloom_service_config_t, "storage_path") == 0);
    std.debug.assert(@offsetOf(ringloom_service_config_t, "service_name") == 32);
    std.debug.assert(@offsetOf(ringloom_message_t, "correlation_id") == 0);
    std.debug.assert(@offsetOf(ringloom_message_t, "payload") == 24);
    std.debug.assert(@offsetOf(ringloom_buffer_claim_t, "_ring_buffer") == 16);
    std.debug.assert(@offsetOf(ringloom_client_target_t, "target_service_id") == 0);
    std.debug.assert(@offsetOf(ringloom_client_target_t, "target_node_id") == 4);
    std.debug.assert(@offsetOf(ringloom_client_target_t, "is_leader") == 6);
    std.debug.assert(@sizeOf(ringloom_client_target_t) == 8);
    std.debug.assert(@offsetOf(ringloom_service_lifecycle_event_t, "event_type") == 0);
    std.debug.assert(@offsetOf(ringloom_service_lifecycle_event_t, "service_id") == 4);
    std.debug.assert(@offsetOf(ringloom_service_lifecycle_event_t, "node_id") == 8);
    std.debug.assert(@offsetOf(ringloom_service_lifecycle_event_t, "is_leader") == 10);
    std.debug.assert(@offsetOf(ringloom_service_lifecycle_event_t, "service_name") == 16);
    std.debug.assert(@offsetOf(ringloom_service_lifecycle_event_t, "service_name_len") == 24);
    std.debug.assert(@sizeOf(ringloom_service_lifecycle_event_t) == 32);
    std.debug.assert(@offsetOf(ringloom_metric_descriptor_t, "name") == 0);
    std.debug.assert(@offsetOf(ringloom_metric_descriptor_t, "name_len") == 8);
    std.debug.assert(@offsetOf(ringloom_metric_descriptor_t, "kind") == 16);
    std.debug.assert(@offsetOf(ringloom_metric_descriptor_t, "value") == 24);
    std.debug.assert(@sizeOf(ringloom_metric_descriptor_t) == 32);
    std.debug.assert(@offsetOf(ringloom_metric_slot_t, "metric_id") == 0);
    std.debug.assert(@offsetOf(ringloom_metric_slot_t, "value") == 8);
    std.debug.assert(@offsetOf(ringloom_metric_slot_t, "value_len") == 16);
    std.debug.assert(@sizeOf(ringloom_metric_slot_t) == 24);
    std.debug.assert(@sizeOf(ringloom_ring_stats_t) == 40);
    std.debug.assert(@backingInt(ringloom_aeron_publication_status_t.RINGLOOM_AERON_PUBLICATION_FAILED) == 7);
}

test "abi version matches header constant" {
    try std.testing.expectEqual(RINGLOOM_SERVICE_ABI_VERSION, ringloom_service_abi_version());
}

test "invalid start arguments return invalid argument status" {
    try std.testing.expectEqual(
        Status.RINGLOOM_ERR_INVALID_ARGUMENT,
        ringloom_service_start(null, null),
    );
}

test "status strings are non-empty" {
    try std.testing.expect(std.mem.span(ringloom_status_string(.RINGLOOM_OK)).len > 0);
    try std.testing.expect(std.mem.span(ringloom_status_string(.RINGLOOM_ERR_INTERNAL)).len > 0);
    try std.testing.expect(std.mem.span(ringloom_aeron_publication_status_string(
        .RINGLOOM_AERON_PUBLICATION_NOT_CONNECTED,
    )).len > 0);
}

test "last error message never returns null" {
    clearLastError();
    try std.testing.expectEqualStrings("", std.mem.span(ringloom_last_error_message()));
}

test "config conversion applies defaults" {
    const allocator = std.testing.allocator;
    const service_name = "java-svc";
    const config = ringloom_service_config_t{
        .storage_path = null,
        .storage_path_len = 0,
        .group = null,
        .group_len = 0,
        .service_name = service_name.ptr,
        .service_name_len = service_name.len,
        .broker_node_id = 0,
        .blocking_mode = false,
        .heartbeat_timeout_ms = 0,
        .control_buffer_length = 0,
        .messages_buffer_length = 0,
        .leader_election_enabled = false,
    };

    const owned = try duplicateConfig(allocator, &config);
    defer owned.deinit(allocator);

    try std.testing.expectEqualStrings(memory_constants.default_storage_path, owned.storage_path);
    try std.testing.expectEqualStrings("default", owned.group);
    try std.testing.expectEqual(@as(i16, 1), owned.zig_config.broker_node_id);
    try std.testing.expectEqual(
        memory_constants.default_control_buffer_length,
        owned.zig_config.control_buffer_length,
    );
    try std.testing.expectEqual(
        memory_constants.default_messages_buffer_length,
        owned.zig_config.messages_buffer_length,
    );
}

test "claim commit rejects inactive claims" {
    var claim = ringloom_buffer_claim_t{
        .payload = null,
        .payload_len = 0,
        ._ring_buffer = 0,
        ._header_index = 0,
        ._record_length = 0,
        ._active = 0,
    };

    try std.testing.expectEqual(
        Status.RINGLOOM_ERR_CLAIM_NOT_ACTIVE,
        ringloom_buffer_claim_commit(&claim),
    );
}

test "client target layout matches expected offsets" {
    try std.testing.expectEqual(@as(usize, 0), @offsetOf(ringloom_client_target_t, "target_service_id"));
    try std.testing.expectEqual(@as(usize, 4), @offsetOf(ringloom_client_target_t, "target_node_id"));
    try std.testing.expectEqual(@as(usize, 6), @offsetOf(ringloom_client_target_t, "is_leader"));
    try std.testing.expectEqual(@as(usize, 8), @sizeOf(ringloom_client_target_t));
}

// ── Replicated KV stores (ABI 7) ──────────────────────────────────────
const kv_api = @import("kv.zig");
pub const ringloom_kv_reader = opaque {};
pub const ringloom_kv_session = opaque {};
pub const ringloom_kv_writer = opaque {};
pub const ringloom_kv_store_config_t = kv_api.KvStoreConfig;
pub const ringloom_kv_reader_options_t = extern struct {
    size: u32 = @sizeOf(@This()),
    wait_ready_timeout_ms: u32 = 0,
    prefault: u8 = 0,
    ttl_clock: u8 = 0,
    _pad: [2]u8 = @splat(0),
    max_readers: u32 = 1024,
};
pub const ringloom_kv_state_fn = ?*const fn (?*anyopaque, u8, u64) callconv(.c) void;
pub const ringloom_kv_writer_options_t = extern struct {
    size: u32 = @sizeOf(@This()),
    acquire: u8 = 0,
    _pad: [3]u8 = @splat(0),
    open_timeout_ms: u32 = 5000,
    max_frame_length: u32 = ringloom_common.platform.constants.default_max_frame_length,
    on_state: ringloom_kv_state_fn = null,
    user_data: ?*anyopaque = null,
};
pub const ringloom_kv_status_t = extern struct {
    size: u32 = @sizeOf(@This()),
    state: u8,
    replication_live: u8,
    _pad: [2]u8 = @splat(0),
    generation: u64,
    applied_index: u64,
    applied_leader_epoch: u64,
    applied_at_ns: u64,
    local_log_tip_index: u64,
    lag_entries: u64,
    heartbeat_age_ms: u64,
    generation_switches: u64,
};
pub const ringloom_kv_visit_fn = ?*const fn (?*anyopaque, [*]const u8, usize, [*]const u8, usize) callconv(.c) c_int;
const KvCWriter = struct {
    writer: *kv_api.KvWriter,
    callback: ringloom_kv_state_fn,
    user_data: ?*anyopaque,
};
const kv_allocator = std.heap.page_allocator;
fn kvReader(p: ?*ringloom_kv_reader) ?*kv_api.KvReader {
    return if (p) |v| @ptrCast(@alignCast(v)) else null;
}
fn kvSession(p: ?*ringloom_kv_session) ?*kv_api.KvReadSession {
    return if (p) |v| @ptrCast(@alignCast(v)) else null;
}
fn kvWriter(p: ?*ringloom_kv_writer) ?*KvCWriter {
    return if (p) |v| @ptrCast(@alignCast(v)) else null;
}
fn kvError(err: anyerror) Status {
    return setLastErrorFmt(switch (err) {
        error.OutOfMemory => .RINGLOOM_ERR_OUT_OF_MEMORY,
        error.InvalidName, error.InvalidConfig, error.StoreDirTooLong => .RINGLOOM_ERR_INVALID_ARGUMENT,
        error.Timeout => .RINGLOOM_ERR_REGISTRATION_TIMEOUT,
        error.NotReady => .RINGLOOM_NOT_READY,
        error.KvNotAvailableLocally => .RINGLOOM_ERR_KV_NOT_AVAILABLE_LOCALLY,
        error.UnknownStore => .RINGLOOM_ERR_TOPIC_UNKNOWN,
        error.EngineMismatch => .RINGLOOM_ERR_KV_ENGINE_MISMATCH,
        error.HeldByOther => .RINGLOOM_ERR_KV_HELD_BY_OTHER,
        error.ConfigMismatch => .RINGLOOM_ERR_TOPIC_CONFIG_MISMATCH,
        error.LeaderUnavailable, error.LeaderPublicationFailed => .RINGLOOM_ERR_TOPIC_LEADER_UNAVAILABLE,
        error.TopicsDisabled => .RINGLOOM_ERR_TOPIC_DISABLED,
        error.NotWriter => .RINGLOOM_ERR_KV_NOT_WRITER,
        else => .RINGLOOM_ERR_INTERNAL,
    }, "KV: {s}", .{@errorName(err)});
}
fn kvInvalid() Status {
    return setLastError(.RINGLOOM_ERR_INVALID_ARGUMENT, "invalid KV argument or option size");
}
fn kvBytes(p: ?[*]const u8, n: usize) ?[]const u8 {
    if (n == 0) return &.{};
    return if (p) |v| v[0..n] else null;
}
fn kvWriteResult(r: kv_api.KvWriteResult, seq: ?*u64) Status {
    if (seq) |s| s.* = 0;
    return switch (r) {
        .ok => |n| blk: {
            if (seq) |s| s.* = n;
            break :blk .RINGLOOM_OK;
        },
        .back_pressured => setLastError(.RINGLOOM_ERR_BACKPRESSURE, "KV back pressured"),
        .not_writer => setLastError(.RINGLOOM_ERR_KV_NOT_WRITER, "KV lease is not granted"),
        .leader_unavailable => setLastError(.RINGLOOM_ERR_TOPIC_LEADER_UNAVAILABLE, "KV leader unavailable"),
        .key_too_large => setLastError(.RINGLOOM_ERR_KV_KEY_TOO_LARGE, "KV key empty or too large"),
        .value_too_large => setLastError(.RINGLOOM_ERR_KV_VALUE_TOO_LARGE, "KV value too large"),
        .batch_full => setLastError(.RINGLOOM_ERR_KV_BATCH_FULL, "KV frame capacity exceeded"),
    };
}
export fn ringloom_kv_open_reader(client: ?*ringloom_client, name: ?[*:0]const u8, opts: ?*const ringloom_kv_reader_options_t, out: ?*?*ringloom_kv_reader) Status {
    clearLastError();
    const dest = out orelse return kvInvalid();
    dest.* = null;
    const h = requireActiveClient(client) orelse return kvInvalid();
    const n = name orelse return kvInvalid();
    var options = kv_api.KvReaderOptions{};
    if (opts) |o| {
        if (o.size < @sizeOf(ringloom_kv_reader_options_t) or o.prefault > 1 or o.ttl_clock > 1 or o.max_readers == 0) return kvInvalid();
        options = .{ .wait_ready_timeout_ms = o.wait_ready_timeout_ms, .prefault = o.prefault != 0, .ttl_clock = @fromBackingInt(o.ttl_clock), .max_readers = o.max_readers };
    }
    const engine = h.service.engine orelse return kvError(error.EngineNotRunning);
    dest.* = @ptrCast(engine.openKvReader(std.mem.sliceTo(n, 0), options) catch |err| return kvError(err));
    return .RINGLOOM_OK;
}
export fn ringloom_kv_reader_status(reader: ?*const ringloom_kv_reader, out: ?*ringloom_kv_status_t) Status {
    clearLastError();
    const p = reader orelse return kvInvalid();
    const r: *const kv_api.KvReader = @ptrCast(@alignCast(p));
    const dest = out orelse return kvInvalid();
    if (dest.size < @sizeOf(ringloom_kv_status_t)) return kvInvalid();
    const s = r.status();
    dest.* = .{ .state = @truncate(@backingInt(s.state)), .replication_live = @intFromBool(s.replication_live), .generation = s.generation, .applied_index = s.applied_index, .applied_leader_epoch = s.applied_leader_epoch, .applied_at_ns = s.applied_at_ns, .local_log_tip_index = s.local_log_tip_index, .lag_entries = s.lag_entries, .heartbeat_age_ms = s.heartbeat_age_ms, .generation_switches = s.generation_switches };
    return .RINGLOOM_OK;
}
export fn ringloom_kv_reader_config(reader: ?*const ringloom_kv_reader, out: ?*ringloom_kv_store_config_t) Status {
    clearLastError();
    const p = reader orelse return kvInvalid();
    const r: *const kv_api.KvReader = @ptrCast(@alignCast(p));
    const dest = out orelse return kvInvalid();
    if (dest.size < @sizeOf(ringloom_kv_store_config_t)) return kvInvalid();
    dest.* = r.effectiveConfig();
    dest.size = @sizeOf(ringloom_kv_store_config_t);
    return .RINGLOOM_OK;
}
export fn ringloom_kv_session_new(reader: ?*ringloom_kv_reader, out: ?*?*ringloom_kv_session) Status {
    clearLastError();
    const dest = out orelse return kvInvalid();
    dest.* = null;
    const r = kvReader(reader) orelse return kvInvalid();
    const s = kv_allocator.create(kv_api.KvReadSession) catch return kvError(error.OutOfMemory);
    s.* = r.newSession() catch |err| {
        kv_allocator.destroy(s);
        return kvError(err);
    };
    dest.* = @ptrCast(s);
    return .RINGLOOM_OK;
}
export fn ringloom_kv_get(session: ?*ringloom_kv_session, key: ?[*]const u8, key_len: usize, out: ?[*]u8, out_cap: usize, out_len: ?*usize) Status {
    clearLastError();
    const len = out_len orelse return kvInvalid();
    len.* = 0;
    const s = kvSession(session) orelse return kvInvalid();
    const k = kvBytes(key, key_len) orelse return kvInvalid();
    var empty: [0]u8 = .{};
    const buffer = if (out) |v| v[0..out_cap] else if (out_cap == 0) &empty else return kvInvalid();
    return switch (s.get(k, buffer)) {
        .found => |n| blk: {
            len.* = n;
            break :blk .RINGLOOM_OK;
        },
        .missing => .RINGLOOM_KV_MISSING,
        .not_ready => .RINGLOOM_NOT_READY,
        .buffer_too_small => |n| blk: {
            len.* = n;
            break :blk .RINGLOOM_ERR_KV_BUFFER_TOO_SMALL;
        },
    };
}
export fn ringloom_kv_view(session: ?*ringloom_kv_session, key: ?[*]const u8, key_len: usize, out_val: ?*?[*]const u8, out_len: ?*usize) Status {
    clearLastError();
    const value = out_val orelse return kvInvalid();
    value.* = null;
    const len = out_len orelse return kvInvalid();
    len.* = 0;
    const s = kvSession(session) orelse return kvInvalid();
    const k = kvBytes(key, key_len) orelse return kvInvalid();
    return switch (s.view(k)) {
        .found => |v| blk: {
            value.* = v.ptr;
            len.* = v.len;
            break :blk .RINGLOOM_OK;
        },
        .missing => .RINGLOOM_KV_MISSING,
        .not_ready => .RINGLOOM_NOT_READY,
    };
}
export fn ringloom_kv_release(session: ?*ringloom_kv_session) void {
    if (kvSession(session)) |s| s.release();
}
const KvVisit = struct {
    callback: *const fn (?*anyopaque, [*]const u8, usize, [*]const u8, usize) callconv(.c) c_int,
    ctx: ?*anyopaque,
    fn visit(self: *@This(), key: []const u8, value: []const u8) bool {
        return self.callback(self.ctx, key.ptr, key.len, value.ptr, value.len) != 0;
    }
};
export fn ringloom_kv_for_each(session: ?*ringloom_kv_session, callback: ringloom_kv_visit_fn, ctx: ?*anyopaque) Status {
    clearLastError();
    const s = kvSession(session) orelse return kvInvalid();
    var visitor = KvVisit{ .callback = callback orelse return kvInvalid(), .ctx = ctx };
    return switch (s.forEach(&visitor, KvVisit.visit).status) {
        .done, .stopped => .RINGLOOM_OK,
        .not_ready => .RINGLOOM_NOT_READY,
        .failed => kvError(error.EngineFailure),
    };
}
export fn ringloom_kv_session_free(session: ?*ringloom_kv_session) void {
    if (kvSession(session)) |s| {
        s.deinit();
        kv_allocator.destroy(s);
    }
}
export fn ringloom_kv_close_reader(reader: ?*ringloom_kv_reader) void {
    if (kvReader(reader)) |r| r.close();
}
fn kvStateChanged(ctx: ?*anyopaque, state: kv_api.KvWriterState, epoch: u64) void {
    const h: *KvCWriter = @ptrCast(@alignCast(ctx.?));
    if (h.callback) |cb| cb(h.user_data, @backingInt(state), epoch);
}
export fn ringloom_kv_open_writer(client: ?*ringloom_client, name: ?[*:0]const u8, cfg: ?*const ringloom_kv_store_config_t, opts: ?*const ringloom_kv_writer_options_t, out: ?*?*ringloom_kv_writer) Status {
    clearLastError();
    const dest = out orelse return kvInvalid();
    dest.* = null;
    const c = requireActiveClient(client) orelse return kvInvalid();
    const n = name orelse return kvInvalid();
    const config = cfg orelse return kvInvalid();
    if (config.size < @sizeOf(ringloom_kv_store_config_t)) return kvInvalid();
    const h = kv_allocator.create(KvCWriter) catch return kvError(error.OutOfMemory);
    h.* = .{ .writer = undefined, .callback = null, .user_data = null };
    var options = kv_api.KvWriterOptions{};
    if (opts) |o| {
        if (o.size < @sizeOf(ringloom_kv_writer_options_t) or o.acquire > 1) {
            kv_allocator.destroy(h);
            return kvInvalid();
        }
        options.acquire = @fromBackingInt(o.acquire);
        options.open_timeout_ms = o.open_timeout_ms;
        options.max_frame_length = o.max_frame_length;
        h.callback = o.on_state;
        h.user_data = o.user_data;
    }
    options.on_state = kvStateChanged;
    options.on_state_user_data = h;
    const engine = c.service.engine orelse {
        kv_allocator.destroy(h);
        return kvError(error.EngineNotRunning);
    };
    h.writer = engine.openKvWriter(std.mem.sliceTo(n, 0), config.*, options) catch |err| {
        kv_allocator.destroy(h);
        return kvError(err);
    };
    dest.* = @ptrCast(h);
    return .RINGLOOM_OK;
}
export fn ringloom_kv_put(writer: ?*ringloom_kv_writer, key: ?[*]const u8, key_len: usize, value: ?[*]const u8, value_len: usize, ttl_ms: i64, ack_mode: u8, out_seq: ?*u64) Status {
    clearLastError();
    if (out_seq) |s| s.* = 0;
    const w = kvWriter(writer) orelse return kvInvalid();
    const k = kvBytes(key, key_len) orelse return kvInvalid();
    const v = kvBytes(value, value_len) orelse return kvInvalid();
    if (ttl_ms < -1) return kvInvalid();
    const ack = kv_api.AckMode.fromU8(ack_mode) orelse return kvInvalid();
    return kvWriteResult(w.writer.put(k, v, .{ .ttl_ms = if (ttl_ms == -1) null else @intCast(ttl_ms), .ack_mode = ack }), out_seq);
}
export fn ringloom_kv_delete(writer: ?*ringloom_kv_writer, key: ?[*]const u8, key_len: usize, ack_mode: u8, out_seq: ?*u64) Status {
    clearLastError();
    if (out_seq) |s| s.* = 0;
    const w = kvWriter(writer) orelse return kvInvalid();
    const k = kvBytes(key, key_len) orelse return kvInvalid();
    const ack = kv_api.AckMode.fromU8(ack_mode) orelse return kvInvalid();
    return kvWriteResult(w.writer.delete(k, .{ .ack_mode = ack }), out_seq);
}
export fn ringloom_kv_clear(writer: ?*ringloom_kv_writer, ack_mode: u8, out_seq: ?*u64) Status {
    clearLastError();
    if (out_seq) |s| s.* = 0;
    const w = kvWriter(writer) orelse return kvInvalid();
    const ack = kv_api.AckMode.fromU8(ack_mode) orelse return kvInvalid();
    return kvWriteResult(w.writer.clear(.{ .ack_mode = ack }), out_seq);
}
export fn ringloom_kv_begin_batch(writer: ?*ringloom_kv_writer) Status {
    clearLastError();
    const w = kvWriter(writer) orelse return kvInvalid();
    switch (w.writer.state()) {
        .granted => {},
        .reasserting => return .RINGLOOM_ERR_TOPIC_LEADER_UNAVAILABLE,
        else => return .RINGLOOM_ERR_KV_NOT_WRITER,
    }
    w.writer.beginBatch();
    return .RINGLOOM_OK;
}
export fn ringloom_kv_commit_batch(writer: ?*ringloom_kv_writer, ack_mode: u8, out_seq: ?*u64) Status {
    clearLastError();
    if (out_seq) |s| s.* = 0;
    const w = kvWriter(writer) orelse return kvInvalid();
    const ack = kv_api.AckMode.fromU8(ack_mode) orelse return kvInvalid();
    return kvWriteResult(w.writer.commitBatch(.{ .ack_mode = ack }), out_seq);
}
export fn ringloom_kv_abandon_batch(writer: ?*ringloom_kv_writer) Status {
    clearLastError();
    const w = kvWriter(writer) orelse return kvInvalid();
    w.writer.abandonBatch();
    return .RINGLOOM_OK;
}
export fn ringloom_kv_is_acked(writer: ?*ringloom_kv_writer, seq: u64) c_int {
    const w = kvWriter(writer) orelse return 0;
    return @intFromBool(w.writer.isAcked(seq));
}
export fn ringloom_kv_writer_state(writer: ?*const ringloom_kv_writer) c_int {
    const p = writer orelse return -1;
    const h: *const KvCWriter = @ptrCast(@alignCast(p));
    return @backingInt(h.writer.state());
}
fn kvConstWriter(p: ?*const ringloom_kv_writer) ?*const KvCWriter {
    return if (p) |v| @ptrCast(@alignCast(v)) else null;
}
export fn ringloom_kv_writer_acked_seq(writer: ?*const ringloom_kv_writer) u64 {
    const h = kvConstWriter(writer) orelse return 0;
    return h.writer.ackedSeq();
}
export fn ringloom_kv_writer_leader_epoch(writer: ?*const ringloom_kv_writer) u64 {
    const h = kvConstWriter(writer) orelse return 0;
    return h.writer.leaderEpoch();
}
export fn ringloom_kv_writer_epoch(writer: ?*const ringloom_kv_writer) u64 {
    const h = kvConstWriter(writer) orelse return 0;
    return h.writer.writerEpoch();
}
export fn ringloom_kv_writer_config(writer: ?*const ringloom_kv_writer, out: ?*ringloom_kv_store_config_t) Status {
    clearLastError();
    const h = kvConstWriter(writer) orelse return kvInvalid();
    const dest = out orelse return kvInvalid();
    if (dest.size < @sizeOf(ringloom_kv_store_config_t)) return kvInvalid();
    dest.* = h.writer.effectiveConfig();
    dest.size = @sizeOf(ringloom_kv_store_config_t);
    return .RINGLOOM_OK;
}
export fn ringloom_kv_close_writer(writer: ?*ringloom_kv_writer) void {
    if (kvWriter(writer)) |h| {
        h.writer.close();
        kv_allocator.destroy(h);
    }
}
export fn ringloom_kv_store_config_init(out: ?*ringloom_kv_store_config_t) void {
    if (out) |v| {
        v.* = .{};
    }
}
export fn ringloom_kv_reader_options_init(out: ?*ringloom_kv_reader_options_t) void {
    if (out) |v| v.* = .{};
}
export fn ringloom_kv_writer_options_init(out: ?*ringloom_kv_writer_options_t) void {
    if (out) |v| v.* = .{};
}

test "KV ABI sizes, defaults and validation" {
    const t = std.testing;
    try t.expectEqual(@as(usize, 80), @sizeOf(ringloom_kv_store_config_t));
    try t.expectEqual(@as(usize, 16), @sizeOf(ringloom_kv_reader_options_t));
    try t.expectEqual(@as(usize, 32), @sizeOf(ringloom_kv_writer_options_t));
    try t.expectEqual(@as(usize, 72), @sizeOf(ringloom_kv_status_t));
    var cfg: ringloom_kv_store_config_t = undefined;
    ringloom_kv_store_config_init(&cfg);
    try cfg.validate(65536 - 48);
    var ro: ringloom_kv_reader_options_t = undefined;
    ringloom_kv_reader_options_init(&ro);
    try t.expectEqual(@as(u32, 1024), ro.max_readers);
    var wo: ringloom_kv_writer_options_t = undefined;
    ringloom_kv_writer_options_init(&wo);
    try t.expectEqual(@as(u32, 5000), wo.open_timeout_ms);
    var len: usize = 99;
    try t.expectEqual(Status.RINGLOOM_ERR_INVALID_ARGUMENT, ringloom_kv_get(null, null, 1, null, 0, &len));
    try t.expectEqual(@as(usize, 0), len);
    try t.expectEqual(Status.RINGLOOM_ERR_INVALID_ARGUMENT, ringloom_kv_open_reader(null, "prices", null, null));
    try t.expectEqual(@as(c_int, -1), ringloom_kv_writer_state(null));
    try t.expectEqual(@as(c_int, 0), ringloom_kv_is_acked(null, 1));
    try t.expectEqual(@as(u64, 0), ringloom_kv_writer_acked_seq(null));
    try t.expectEqual(@as(u64, 0), ringloom_kv_writer_leader_epoch(null));
    try t.expectEqual(@as(u64, 0), ringloom_kv_writer_epoch(null));
    try t.expectEqual(Status.RINGLOOM_ERR_INVALID_ARGUMENT, ringloom_kv_writer_config(null, &cfg));
    try t.expectEqual(Status.RINGLOOM_ERR_INVALID_ARGUMENT, ringloom_kv_reader_config(null, &cfg));
    try t.expectEqual(Status.RINGLOOM_ERR_INVALID_ARGUMENT, ringloom_kv_abandon_batch(null));
    ringloom_kv_release(null);
    ringloom_kv_session_free(null);
    ringloom_kv_close_reader(null);
    ringloom_kv_close_writer(null);
}

test "KV ABI copying, borrowed views, expiry and iterator stop" {
    const t = std.testing;
    const common_kv = ringloom_common.kv;
    const io = ringloom_common.platform.defaultIo();
    var dir_buf: [160]u8 = undefined;
    const dir = try std.fmt.bufPrint(&dir_buf, "/tmp/ringloom-k07-abi-{d}", .{Clock.monotonicNanos()});
    defer std.Io.Dir.cwd().deleteTree(io, dir) catch {};
    var cf = try common_kv.control.ControlFile.createOrOpen(dir, 77, common_kv.engine.engine_kind, 511, 4096);
    defer cf.close();
    var gen_buf: [200]u8 = undefined;
    const gdir = try common_kv.control.genDir(&gen_buf, dir, 1);
    var env = try common_kv.engine.WriteEnv.create(gdir, 8 * 1024 * 1024, 64);
    defer env.close();
    var txn = try env.begin();
    try txn.put("k", .{ .write_index = 1, .expire_at_ns = 0 }, "value");
    try txn.put("empty", .{ .write_index = 1, .expire_at_ns = 0 }, "");
    try txn.put("expired", .{ .write_index = 1, .expire_at_ns = 1 }, "dead");
    try txn.commit();
    cf.publishGeneration(1);
    const reader = try kv_api.KvReader.openLocal(t.allocator, dir, .{}, .{});
    defer ringloom_kv_close_reader(@ptrCast(reader));
    var reader_cfg: ringloom_kv_store_config_t = .{};
    reader_cfg.size = 4;
    try t.expectEqual(Status.RINGLOOM_ERR_INVALID_ARGUMENT, ringloom_kv_reader_config(@ptrCast(reader), &reader_cfg));
    reader_cfg = .{ .max_key_len = 1 };
    try t.expectEqual(Status.RINGLOOM_OK, ringloom_kv_reader_config(@ptrCast(reader), &reader_cfg));
    try t.expectEqual(@as(u16, 511), reader_cfg.max_key_len);
    try t.expectEqual(@as(u32, 80), reader_cfg.size);
    var session: ?*ringloom_kv_session = null;
    try t.expectEqual(Status.RINGLOOM_OK, ringloom_kv_session_new(@ptrCast(reader), &session));
    defer ringloom_kv_session_free(session);
    var out: [16]u8 = undefined;
    var len: usize = 0;
    try t.expectEqual(Status.RINGLOOM_ERR_KV_BUFFER_TOO_SMALL, ringloom_kv_get(session, "k", 1, &out, 1, &len));
    try t.expectEqual(@as(usize, 5), len);
    try t.expectEqual(Status.RINGLOOM_OK, ringloom_kv_get(session, "empty", 5, null, 0, &len));
    try t.expectEqual(@as(usize, 0), len);
    var value: ?[*]const u8 = null;
    try t.expectEqual(Status.RINGLOOM_OK, ringloom_kv_view(session, "k", 1, &value, &len));
    try t.expectEqualStrings("value", value.?[0..len]);
    var update = try env.begin();
    try update.put("k", .{ .write_index = 2, .expire_at_ns = 0 }, "new");
    try update.commit();
    try t.expectEqual(Status.RINGLOOM_OK, ringloom_kv_get(session, "k", 1, &out, out.len, &len));
    try t.expectEqualStrings("value", out[0..len]);
    ringloom_kv_release(session);
    try t.expectEqual(Status.RINGLOOM_OK, ringloom_kv_get(session, "k", 1, &out, out.len, &len));
    try t.expectEqualStrings("new", out[0..len]);
    try t.expectEqual(Status.RINGLOOM_KV_MISSING, ringloom_kv_get(session, "expired", 7, &out, out.len, &len));
    const Visit = struct {
        fn callback(ctx: ?*anyopaque, _: [*]const u8, _: usize, _: [*]const u8, _: usize) callconv(.c) c_int {
            const count: *usize = @ptrCast(@alignCast(ctx.?));
            count.* += 1;
            return 0;
        }
    };
    var visits: usize = 0;
    try t.expectEqual(Status.RINGLOOM_OK, ringloom_kv_for_each(session, Visit.callback, &visits));
    try t.expectEqual(@as(usize, 1), visits);
}
