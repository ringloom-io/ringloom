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

pub const ringloom_service = opaque {};
pub const ringloom_client = opaque {};
pub const ringloom_message_consumer = opaque {};
pub const ringloom_metrics_reader = opaque {};

pub const RINGLOOM_SERVICE_ABI_VERSION: u32 = 3;
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
    [_:0]u8{0} ** (max_error_message_length + 1);
threadlocal var poll_dispatch_context: ?*PollDispatchContext = null;

var abi_debug_allocator: std.heap.DebugAllocator(.{}) = .init;

fn abiAllocator() std.mem.Allocator {
    return switch (builtin.mode) {
        .Debug, .ReleaseSafe => abi_debug_allocator.allocator(),
        .ReleaseFast, .ReleaseSmall => std.heap.smp_allocator,
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
    error_state_mod.err_state.set(@intFromEnum(status), last_error_message_buffer[0..len]);
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
    out_counter_id: ?*i32,
) Status {
    return registerServiceMetric(service, name, name_len, .counter, out_counter_id);
}

export fn ringloom_service_gauge_register(
    service: ?*ringloom_service,
    name: ?[*]const u8,
    name_len: usize,
    out_gauge_id: ?*i32,
) Status {
    return registerServiceMetric(service, name, name_len, .gauge, out_gauge_id);
}

export fn ringloom_service_counter_add(
    service: ?*ringloom_service,
    counter_id: i32,
    delta: i64,
) Status {
    clearLastError();
    if (counter_id < 0) {
        return setLastError(.RINGLOOM_ERR_INVALID_ARGUMENT, "counter_id must be non-negative");
    }

    const handle = requireActiveService(service) orelse
        return .RINGLOOM_ERR_INVALID_ARGUMENT;
    const kind = handle.engine.?.counters.allocatedKind(@intCast(counter_id)) catch |err|
        return mapMetricAccessError(err, "counter_id does not reference an allocated counter");
    if (kind != .counter) {
        return setLastError(.RINGLOOM_ERR_INVALID_ARGUMENT, "counter_id references a gauge");
    }
    handle.engine.?.counters.tryAdd(@intCast(counter_id), delta) catch |err|
        return mapMetricAccessError(err, "counter_id does not reference an allocated counter");
    return .RINGLOOM_OK;
}

export fn ringloom_service_counter_set(
    service: ?*ringloom_service,
    counter_id: i32,
    value: i64,
) Status {
    clearLastError();
    if (counter_id < 0) {
        return setLastError(.RINGLOOM_ERR_INVALID_ARGUMENT, "counter_id must be non-negative");
    }

    const handle = requireActiveService(service) orelse
        return .RINGLOOM_ERR_INVALID_ARGUMENT;
    const kind = handle.engine.?.counters.allocatedKind(@intCast(counter_id)) catch |err|
        return mapMetricAccessError(err, "counter_id does not reference an allocated counter");
    if (kind != .counter) {
        return setLastError(.RINGLOOM_ERR_INVALID_ARGUMENT, "counter_id references a gauge");
    }
    handle.engine.?.counters.trySet(@intCast(counter_id), value) catch |err|
        return mapMetricAccessError(err, "counter_id does not reference an allocated counter");
    return .RINGLOOM_OK;
}

export fn ringloom_service_gauge_set(
    service: ?*ringloom_service,
    gauge_id: i32,
    value: i64,
) Status {
    clearLastError();
    if (gauge_id < 0) {
        return setLastError(.RINGLOOM_ERR_INVALID_ARGUMENT, "gauge_id must be non-negative");
    }

    const handle = requireActiveService(service) orelse
        return .RINGLOOM_ERR_INVALID_ARGUMENT;
    const kind = handle.engine.?.counters.allocatedKind(@intCast(gauge_id)) catch |err|
        return mapMetricAccessError(err, "gauge_id does not reference an allocated gauge");
    if (kind != .gauge) {
        return setLastError(.RINGLOOM_ERR_INVALID_ARGUMENT, "gauge_id references a counter");
    }
    handle.engine.?.counters.trySet(@intCast(gauge_id), value) catch |err|
        return mapMetricAccessError(err, "gauge_id does not reference an allocated gauge");
    return .RINGLOOM_OK;
}

fn registerServiceMetric(
    service: ?*ringloom_service,
    name: ?[*]const u8,
    name_len: usize,
    kind: counters_mod.MetricKind,
    out_metric_id: ?*i32,
) Status {
    clearLastError();
    const out = out_metric_id orelse
        return setLastError(.RINGLOOM_ERR_INVALID_ARGUMENT, "out_metric_id must not be NULL");
    out.* = -1;

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

    out.* = @intCast(id);
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
    std.debug.assert(@sizeOf(ringloom_ring_stats_t) == 40);
    std.debug.assert(@intFromEnum(ringloom_aeron_publication_status_t.RINGLOOM_AERON_PUBLICATION_FAILED) == 7);
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
