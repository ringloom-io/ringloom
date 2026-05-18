// SPDX-License-Identifier: Apache-2.0
//! ringloom_aeron - Zig wrapper for embedded Aeron driver and client APIs.

const std = @import("std");
const testing = std.testing;

const c = @cImport({
    @cInclude("aeron_zig_api.h");
});

pub const Error = error{
    AeronCallFailed,
    ResourceClosed,
    Timeout,
    ThreadingModeMismatch,
};

pub const ThreadingMode = enum {
    dedicated,
    shared_network,
    shared,

    fn toC(self: ThreadingMode) c.aeron_threading_mode_t {
        return switch (self) {
            .dedicated => c.AERON_THREADING_MODE_DEDICATED,
            .shared_network => c.AERON_THREADING_MODE_SHARED_NETWORK,
            .shared => c.AERON_THREADING_MODE_SHARED,
        };
    }
};

const AgentKind = enum {
    conductor,
    sender,
    receiver,
    shared_network,
    shared,

    fn toC(self: AgentKind) c.ringloom_aeron_agent_kind_t {
        return switch (self) {
            .conductor => c.RINGLOOM_AERON_AGENT_CONDUCTOR,
            .sender => c.RINGLOOM_AERON_AGENT_SENDER,
            .receiver => c.RINGLOOM_AERON_AGENT_RECEIVER,
            .shared_network => c.RINGLOOM_AERON_AGENT_SHARED_NETWORK,
            .shared => c.RINGLOOM_AERON_AGENT_SHARED,
        };
    }
};

pub const ErrorInfo = struct {
    code: i32,
    message_len: usize,
    message_buf: [512]u8,
    truncated: bool,

    pub fn message(self: *const ErrorInfo) []const u8 {
        return self.message_buf[0..self.message_len];
    }
};

pub fn lastError() ErrorInfo {
    var info = ErrorInfo{
        .code = @intCast(c.aeron_errcode()),
        .message_len = 0,
        .message_buf = undefined,
        .truncated = false,
    };
    @memset(&info.message_buf, 0);

    const ptr = c.aeron_errmsg();
    if (ptr == null) {
        return info;
    }

    const msg = std.mem.span(ptr);
    const limit = info.message_buf.len;
    const copied_len = @min(msg.len, limit);
    @memcpy(info.message_buf[0..copied_len], msg[0..copied_len]);
    info.message_len = copied_len;
    info.truncated = msg.len > limit;
    return info;
}

fn check(rc: c_int) Error!void {
    if (rc < 0) {
        return error.AeronCallFailed;
    }
}

pub const ChannelUri = struct {
    pub fn ipc(buffer: []u8, term_length: ?usize) std.fmt.BufPrintError![:0]u8 {
        if (term_length) |length| {
            return std.fmt.bufPrintZ(buffer, "aeron:ipc?term-length={d}", .{length});
        }
        return std.fmt.bufPrintZ(buffer, "aeron:ipc", .{});
    }

    pub fn udpEndpoint(
        buffer: []u8,
        endpoint_host: []const u8,
        endpoint_port: u16,
        term_length: ?usize,
    ) std.fmt.BufPrintError![:0]u8 {
        if (term_length) |length| {
            return std.fmt.bufPrintZ(
                buffer,
                "aeron:udp?endpoint={s}:{d}|term-length={d}",
                .{ endpoint_host, endpoint_port, length },
            );
        }
        return std.fmt.bufPrintZ(
            buffer,
            "aeron:udp?endpoint={s}:{d}",
            .{ endpoint_host, endpoint_port },
        );
    }
};

pub const OfferResult = union(enum) {
    position: i64,
    not_connected,
    back_pressured,
    admin_action,
    closed,
    max_position_exceeded,
    failed: ErrorInfo,
};

pub fn mapOfferResult(value: i64) OfferResult {
    if (value >= 0) {
        return .{ .position = value };
    }

    if (value == @as(i64, c.AERON_PUBLICATION_NOT_CONNECTED)) return .not_connected;
    if (value == @as(i64, c.AERON_PUBLICATION_BACK_PRESSURED)) return .back_pressured;
    if (value == @as(i64, c.AERON_PUBLICATION_ADMIN_ACTION)) return .admin_action;
    if (value == @as(i64, c.AERON_PUBLICATION_CLOSED)) return .closed;
    if (value == @as(i64, c.AERON_PUBLICATION_MAX_POSITION_EXCEEDED)) return .max_position_exceeded;

    return .{ .failed = lastError() };
}

pub const ClaimResult = union(enum) {
    claim: BufferClaim,
    not_connected,
    back_pressured,
    admin_action,
    closed,
    max_position_exceeded,
    failed: ErrorInfo,
};

fn mapClaimResult(value: i64, raw: c.aeron_buffer_claim_t) ClaimResult {
    if (value >= 0) {
        return .{ .claim = .{ .raw = raw } };
    }

    if (value == @as(i64, c.AERON_PUBLICATION_NOT_CONNECTED)) return .not_connected;
    if (value == @as(i64, c.AERON_PUBLICATION_BACK_PRESSURED)) return .back_pressured;
    if (value == @as(i64, c.AERON_PUBLICATION_ADMIN_ACTION)) return .admin_action;
    if (value == @as(i64, c.AERON_PUBLICATION_CLOSED)) return .closed;
    if (value == @as(i64, c.AERON_PUBLICATION_MAX_POSITION_EXCEEDED)) return .max_position_exceeded;

    return .{ .failed = lastError() };
}

pub const DriverOptions = struct {
    directory: [:0]const u8,
    delete_dir_on_start: bool = true,
    delete_dir_on_shutdown: bool = true,
    term_buffer_length: ?usize = null,
    ipc_term_buffer_length: ?usize = null,
    mtu_length: ?usize = null,
    ipc_mtu_length: ?usize = null,
    term_buffer_sparse_file: ?bool = null,
    publication_linger_timeout_ns: ?u64 = null,
    client_liveness_timeout_ns: ?u64 = null,
    network_publication_max_messages_per_send: ?u32 = null,
};

pub const Driver = struct {
    handle: *anyopaque,
    context: *anyopaque,
    mode: ThreadingMode,

    pub fn initEmbedded(options: DriverOptions, mode: ThreadingMode) Error!Driver {
        var context_raw: ?*c.aeron_driver_context_t = null;
        try check(c.aeron_driver_context_init(&context_raw));
        const context = context_raw.?;
        errdefer _ = c.aeron_driver_context_close(context);

        try check(c.aeron_driver_context_set_dir(context, options.directory.ptr));
        try check(c.aeron_driver_context_set_threading_mode(context, mode.toC()));
        try check(c.aeron_driver_context_set_dir_delete_on_start(context, options.delete_dir_on_start));
        try check(c.aeron_driver_context_set_dir_delete_on_shutdown(context, options.delete_dir_on_shutdown));
        try check(c.aeron_driver_context_set_dir_warn_if_exists(context, false));

        if (options.term_buffer_length) |length| {
            try check(c.aeron_driver_context_set_term_buffer_length(context, length));
        }
        if (options.ipc_term_buffer_length) |length| {
            try check(c.aeron_driver_context_set_ipc_term_buffer_length(context, length));
        }
        if (options.mtu_length) |length| {
            try check(c.aeron_driver_context_set_mtu_length(context, length));
        }
        if (options.ipc_mtu_length) |length| {
            try check(c.aeron_driver_context_set_ipc_mtu_length(context, length));
        }
        if (options.term_buffer_sparse_file) |sparse| {
            try check(c.aeron_driver_context_set_term_buffer_sparse_file(context, sparse));
        }
        if (options.publication_linger_timeout_ns) |timeout_ns| {
            try check(c.aeron_driver_context_set_publication_linger_timeout_ns(context, timeout_ns));
        }
        if (options.client_liveness_timeout_ns) |timeout_ns| {
            try check(c.aeron_driver_context_set_client_liveness_timeout_ns(context, timeout_ns));
        }
        if (options.network_publication_max_messages_per_send) |value| {
            try check(c.aeron_driver_context_set_network_publication_max_messages_per_send(context, value));
        }

        var driver_raw: ?*c.aeron_driver_t = null;
        try check(c.aeron_driver_init(&driver_raw, context));
        errdefer _ = c.aeron_driver_close(driver_raw.?);

        try check(c.ringloom_aeron_driver_start_manual(driver_raw.?));

        return .{
            .handle = @ptrCast(driver_raw.?),
            .context = @ptrCast(context),
            .mode = mode,
        };
    }

    pub fn agents(self: *Driver, mode: ThreadingMode) Error!DriverAgents {
        if (mode != self.mode) {
            return error.ThreadingModeMismatch;
        }

        return switch (mode) {
            .dedicated => .{
                .dedicated = .{
                    .conductor = .{ .driver = self, .kind = .conductor },
                    .sender = .{ .driver = self, .kind = .sender },
                    .receiver = .{ .driver = self, .kind = .receiver },
                },
            },
            .shared_network => .{
                .shared_network = .{
                    .conductor = .{ .driver = self, .kind = .conductor },
                    .network = .{ .driver = self, .kind = .shared_network },
                },
            },
            .shared => .{ .shared = .{ .driver = self, .kind = .shared } },
        };
    }

    pub fn deinit(self: *Driver) void {
        _ = c.aeron_driver_close(self.rawDriver());
        _ = c.aeron_driver_context_close(self.rawContext());
    }

    fn rawDriver(self: *Driver) *c.aeron_driver_t {
        return @ptrCast(@alignCast(self.handle));
    }

    fn rawContext(self: *Driver) *c.aeron_driver_context_t {
        return @ptrCast(@alignCast(self.context));
    }
};

pub const AgentInvoker = struct {
    driver: *Driver,
    kind: AgentKind,

    pub fn invoke(self: *AgentInvoker) Error!i32 {
        const work_count = c.ringloom_aeron_driver_do_work(self.driver.rawDriver(), self.kind.toC());
        if (work_count < 0) {
            return error.AeronCallFailed;
        }
        return @intCast(work_count);
    }

    pub fn idle(self: *AgentInvoker, work_count: i32) void {
        c.ringloom_aeron_driver_idle(self.driver.rawDriver(), self.kind.toC(), work_count);
    }
};

pub const DriverAgents = union(enum) {
    dedicated: struct {
        conductor: AgentInvoker,
        sender: AgentInvoker,
        receiver: AgentInvoker,
    },
    shared_network: struct {
        conductor: AgentInvoker,
        network: AgentInvoker,
    },
    shared: AgentInvoker,

    pub fn invokeAll(self: *DriverAgents) Error!i32 {
        return switch (self.*) {
            .dedicated => |*agents| blk: {
                var total: i32 = 0;
                total += try agents.conductor.invoke();
                total += try agents.sender.invoke();
                total += try agents.receiver.invoke();
                break :blk total;
            },
            .shared_network => |*agents| blk: {
                var total: i32 = 0;
                total += try agents.conductor.invoke();
                total += try agents.network.invoke();
                break :blk total;
            },
            .shared => |*agent| agent.invoke(),
        };
    }
};

pub const ClientOptions = struct {
    directory: [:0]const u8,
    use_conductor_agent_invoker: bool = true,
    driver_timeout_ms: ?u64 = null,
};

pub const Client = struct {
    handle: *anyopaque,
    context: *anyopaque,
    use_conductor_agent_invoker: bool,

    pub fn connect(options: ClientOptions) Error!Client {
        var context_raw: ?*c.aeron_context_t = null;
        try check(c.aeron_context_init(&context_raw));
        const context = context_raw.?;
        errdefer _ = c.aeron_context_close(context);

        try check(c.aeron_context_set_dir(context, options.directory.ptr));
        try check(c.aeron_context_set_use_conductor_agent_invoker(context, options.use_conductor_agent_invoker));
        if (options.driver_timeout_ms) |timeout_ms| {
            try check(c.aeron_context_set_driver_timeout_ms(context, timeout_ms));
        }

        var client_raw: ?*c.aeron_t = null;
        try check(c.aeron_init(&client_raw, context));
        errdefer _ = c.aeron_close(client_raw.?);

        try check(c.aeron_start(client_raw.?));

        return .{
            .handle = @ptrCast(client_raw.?),
            .context = @ptrCast(context),
            .use_conductor_agent_invoker = options.use_conductor_agent_invoker,
        };
    }

    pub fn invokeConductor(self: *Client) Error!i32 {
        if (!self.use_conductor_agent_invoker) {
            return 0;
        }
        const work_count = c.aeron_main_do_work(self.rawClient());
        if (work_count < 0) {
            return error.AeronCallFailed;
        }
        return @intCast(work_count);
    }

    pub fn addPublication(
        self: *Client,
        channel: [:0]const u8,
        stream_id: i32,
        driver_agents: ?*DriverAgents,
    ) Error!Publication {
        var async: ?*c.aeron_async_add_publication_t = null;
        try check(c.aeron_async_add_publication(&async, self.rawClient(), channel.ptr, stream_id));

        var publication: ?*c.aeron_publication_t = null;
        try self.awaitAsync(async.?, driver_agents, struct {
            fn poll(raw: *c.aeron_async_add_publication_t, out: *?*c.aeron_publication_t) c_int {
                return c.aeron_async_add_publication_poll(out, raw);
            }
        }.poll, &publication);

        return .{ .handle = @ptrCast(publication.?) };
    }

    pub fn addExclusivePublication(
        self: *Client,
        channel: [:0]const u8,
        stream_id: i32,
        driver_agents: ?*DriverAgents,
    ) Error!ExclusivePublication {
        var async: ?*c.aeron_async_add_exclusive_publication_t = null;
        try check(c.aeron_async_add_exclusive_publication(&async, self.rawClient(), channel.ptr, stream_id));

        var publication: ?*c.aeron_exclusive_publication_t = null;
        try self.awaitAsync(async.?, driver_agents, struct {
            fn poll(
                raw: *c.aeron_async_add_exclusive_publication_t,
                out: *?*c.aeron_exclusive_publication_t,
            ) c_int {
                return c.aeron_async_add_exclusive_publication_poll(out, raw);
            }
        }.poll, &publication);

        return .{ .handle = @ptrCast(publication.?) };
    }

    pub fn addSubscription(
        self: *Client,
        channel: [:0]const u8,
        stream_id: i32,
        driver_agents: ?*DriverAgents,
    ) Error!Subscription {
        var async: ?*c.aeron_async_add_subscription_t = null;
        try check(c.aeron_async_add_subscription(
            &async,
            self.rawClient(),
            channel.ptr,
            stream_id,
            null,
            null,
            null,
            null,
        ));

        var subscription: ?*c.aeron_subscription_t = null;
        try self.awaitAsync(async.?, driver_agents, struct {
            fn poll(raw: *c.aeron_async_add_subscription_t, out: *?*c.aeron_subscription_t) c_int {
                return c.aeron_async_add_subscription_poll(out, raw);
            }
        }.poll, &subscription);

        return .{ .handle = @ptrCast(subscription.?) };
    }

    pub fn deinit(self: *Client) void {
        _ = c.aeron_close(self.rawClient());
        _ = c.aeron_context_close(self.rawContext());
    }

    fn awaitAsync(
        self: *Client,
        async: anytype,
        driver_agents: ?*DriverAgents,
        comptime poll: anytype,
        out: anytype,
    ) Error!void {
        for (0..5000) |_| {
            const rc = poll(async, out);
            if (rc == 1) return;
            if (rc < 0) return error.AeronCallFailed;

            _ = try self.invokeConductor();
            if (driver_agents) |agents| {
                _ = try agents.invokeAll();
            }
            sleepOneMs();
        }

        return error.Timeout;
    }

    fn rawClient(self: *Client) *c.aeron_t {
        return @ptrCast(@alignCast(self.handle));
    }

    fn rawContext(self: *Client) *c.aeron_context_t {
        return @ptrCast(@alignCast(self.context));
    }
};

pub const BufferClaim = struct {
    raw: c.aeron_buffer_claim_t,

    pub fn bytes(self: *BufferClaim) []u8 {
        return self.raw.data[0..self.raw.length];
    }

    pub fn commit(self: *BufferClaim) Error!void {
        try check(c.aeron_buffer_claim_commit(&self.raw));
    }

    pub fn abort(self: *BufferClaim) Error!void {
        try check(c.aeron_buffer_claim_abort(&self.raw));
    }
};

pub const Publication = struct {
    handle: ?*anyopaque,

    pub fn offer(self: *Publication, bytes: []const u8) OfferResult {
        const handle = self.rawPublication() catch return .closed;
        return mapOfferResult(c.aeron_publication_offer(
            handle,
            bytes.ptr,
            bytes.len,
            null,
            null,
        ));
    }

    pub fn tryClaim(self: *Publication, length: usize) ClaimResult {
        const handle = self.rawPublication() catch return .closed;
        var raw: c.aeron_buffer_claim_t = undefined;
        return mapClaimResult(c.aeron_publication_try_claim(handle, length, &raw), raw);
    }

    pub fn close(self: *Publication) Error!void {
        const handle = self.rawPublication() catch return;
        try check(c.aeron_publication_close(handle, null, null));
        self.handle = null;
    }

    fn rawPublication(self: *Publication) Error!*c.aeron_publication_t {
        const handle = self.handle orelse return error.ResourceClosed;
        return @ptrCast(@alignCast(handle));
    }
};

pub const ExclusivePublication = struct {
    handle: ?*anyopaque,

    pub fn offer(self: *ExclusivePublication, bytes: []const u8) OfferResult {
        const handle = self.rawPublication() catch return .closed;
        return mapOfferResult(c.aeron_exclusive_publication_offer(
            handle,
            bytes.ptr,
            bytes.len,
            null,
            null,
        ));
    }

    pub fn tryClaim(self: *ExclusivePublication, length: usize) ClaimResult {
        const handle = self.rawPublication() catch return .closed;
        var raw: c.aeron_buffer_claim_t = undefined;
        return mapClaimResult(c.aeron_exclusive_publication_try_claim(handle, length, &raw), raw);
    }

    pub fn close(self: *ExclusivePublication) Error!void {
        const handle = self.rawPublication() catch return;
        try check(c.aeron_exclusive_publication_close(handle, null, null));
        self.handle = null;
    }

    pub fn isConnected(self: *ExclusivePublication) bool {
        const handle = self.rawPublication() catch return false;
        return c.ringloom_aeron_exclusive_publication_is_connected(handle);
    }

    pub fn maxPayloadLength(self: *ExclusivePublication) usize {
        const handle = self.rawPublication() catch return 0;
        return c.ringloom_aeron_exclusive_publication_max_payload_length(handle);
    }

    fn rawPublication(self: *ExclusivePublication) Error!*c.aeron_exclusive_publication_t {
        const handle = self.handle orelse return error.ResourceClosed;
        return @ptrCast(@alignCast(handle));
    }
};

pub const FragmentHandler = struct {
    context: ?*anyopaque = null,
    callback: *const fn (context: ?*anyopaque, bytes: []const u8) void,
};

pub const ControlledAction = enum {
    abort,
    break_poll,
    commit,
    continue_poll,

    fn toC(self: ControlledAction) c.aeron_controlled_fragment_handler_action_t {
        return switch (self) {
            .abort => c.AERON_ACTION_ABORT,
            .break_poll => c.AERON_ACTION_BREAK,
            .commit => c.AERON_ACTION_COMMIT,
            .continue_poll => c.AERON_ACTION_CONTINUE,
        };
    }
};

pub const ControlledFragmentHandler = struct {
    context: ?*anyopaque = null,
    callback: *const fn (context: ?*anyopaque, bytes: []const u8) ControlledAction,
};

pub const FragmentAssembler = struct {
    handle: ?*c.aeron_fragment_assembler_t = null,
    handler: FragmentHandler,

    pub fn init(handler: FragmentHandler) FragmentAssembler {
        return .{
            .handler = handler,
        };
    }

    pub fn deinit(self: *FragmentAssembler) void {
        if (self.handle) |handle| {
            _ = c.aeron_fragment_assembler_delete(handle);
            self.handle = null;
        }
    }

    pub fn poll(self: *FragmentAssembler, subscription: *Subscription, fragment_limit: usize) Error!i32 {
        const handle = try self.ensureHandle();
        const subscription_handle = subscription.rawSubscription() catch return error.ResourceClosed;
        const fragments = c.aeron_subscription_poll(
            subscription_handle,
            c.aeron_fragment_assembler_handler,
            handle,
            fragment_limit,
        );
        if (fragments < 0) {
            return error.AeronCallFailed;
        }
        return @intCast(fragments);
    }

    fn ensureHandle(self: *FragmentAssembler) Error!*c.aeron_fragment_assembler_t {
        if (self.handle) |handle| return handle;
        var raw: ?*c.aeron_fragment_assembler_t = null;
        try check(c.aeron_fragment_assembler_create(&raw, fragmentTrampoline, &self.handler));
        self.handle = raw.?;
        return self.handle.?;
    }
};

pub const ControlledFragmentAssembler = struct {
    handle: ?*c.aeron_controlled_fragment_assembler_t = null,
    handler: ControlledFragmentHandler,

    pub fn init(handler: ControlledFragmentHandler) ControlledFragmentAssembler {
        return .{
            .handler = handler,
        };
    }

    pub fn deinit(self: *ControlledFragmentAssembler) void {
        if (self.handle) |handle| {
            _ = c.aeron_controlled_fragment_assembler_delete(handle);
            self.handle = null;
        }
    }

    pub fn poll(self: *ControlledFragmentAssembler, subscription: *Subscription, fragment_limit: usize) Error!i32 {
        const handle = try self.ensureHandle();
        const subscription_handle = subscription.rawSubscription() catch return error.ResourceClosed;
        const fragments = c.aeron_subscription_controlled_poll(
            subscription_handle,
            c.aeron_controlled_fragment_assembler_handler,
            handle,
            fragment_limit,
        );
        if (fragments < 0) {
            return error.AeronCallFailed;
        }
        return @intCast(fragments);
    }

    fn ensureHandle(self: *ControlledFragmentAssembler) Error!*c.aeron_controlled_fragment_assembler_t {
        if (self.handle) |handle| return handle;
        var raw: ?*c.aeron_controlled_fragment_assembler_t = null;
        try check(c.aeron_controlled_fragment_assembler_create(&raw, controlledFragmentTrampoline, &self.handler));
        self.handle = raw.?;
        return self.handle.?;
    }
};

pub const Subscription = struct {
    handle: ?*anyopaque,

    pub fn poll(self: *Subscription, handler: FragmentHandler, fragment_limit: usize) Error!i32 {
        const handle = self.rawSubscription() catch return error.ResourceClosed;
        var local_handler = handler;
        const fragments = c.aeron_subscription_poll(
            handle,
            fragmentTrampoline,
            &local_handler,
            fragment_limit,
        );
        if (fragments < 0) {
            return error.AeronCallFailed;
        }
        return @intCast(fragments);
    }

    pub fn controlledPoll(self: *Subscription, handler: ControlledFragmentHandler, fragment_limit: usize) Error!i32 {
        const handle = self.rawSubscription() catch return error.ResourceClosed;
        var local_handler = handler;
        const fragments = c.aeron_subscription_controlled_poll(
            handle,
            controlledFragmentTrampoline,
            &local_handler,
            fragment_limit,
        );
        if (fragments < 0) {
            return error.AeronCallFailed;
        }
        return @intCast(fragments);
    }

    pub fn close(self: *Subscription) Error!void {
        const handle = self.rawSubscription() catch return;
        try check(c.aeron_subscription_close(handle, null, null));
        self.handle = null;
    }

    fn rawSubscription(self: *Subscription) Error!*c.aeron_subscription_t {
        const handle = self.handle orelse return error.ResourceClosed;
        return @ptrCast(@alignCast(handle));
    }
};

fn fragmentTrampoline(
    clientd: ?*anyopaque,
    buffer: [*c]const u8,
    length: usize,
    header: ?*c.aeron_header_t,
) callconv(.c) void {
    _ = header;
    const handler: *FragmentHandler = @ptrCast(@alignCast(clientd.?));
    handler.callback(handler.context, buffer[0..length]);
}

fn controlledFragmentTrampoline(
    clientd: ?*anyopaque,
    buffer: [*c]const u8,
    length: usize,
    header: ?*c.aeron_header_t,
) callconv(.c) c.aeron_controlled_fragment_handler_action_t {
    _ = header;
    const handler: *ControlledFragmentHandler = @ptrCast(@alignCast(clientd.?));
    return handler.callback(handler.context, buffer[0..length]).toC();
}

test "ringloom_aeron module compiles" {
    _ = Driver;
    _ = Client;
    _ = Publication;
    _ = ExclusivePublication;
    _ = Subscription;
    _ = FragmentAssembler;
    _ = ControlledFragmentAssembler;
    _ = BufferClaim;
    _ = ControlledAction;
}

test "publication result mapping covers Aeron statuses" {
    try testing.expectEqual(@as(i64, 42), mapOfferResult(42).position);
    try testing.expect(mapOfferResult(c.AERON_PUBLICATION_NOT_CONNECTED) == .not_connected);
    try testing.expect(mapOfferResult(c.AERON_PUBLICATION_BACK_PRESSURED) == .back_pressured);
    try testing.expect(mapOfferResult(c.AERON_PUBLICATION_ADMIN_ACTION) == .admin_action);
    try testing.expect(mapOfferResult(c.AERON_PUBLICATION_CLOSED) == .closed);
    try testing.expect(mapOfferResult(c.AERON_PUBLICATION_MAX_POSITION_EXCEEDED) == .max_position_exceeded);
}

test "channel URI builder writes IPC and UDP endpoint URIs" {
    var buffer: [128]u8 = undefined;

    try testing.expectEqualStrings(
        "aeron:ipc?term-length=1048576",
        try ChannelUri.ipc(&buffer, 1 * 1024 * 1024),
    );
    try testing.expectEqualStrings(
        "aeron:udp?endpoint=127.0.0.1:40123|term-length=1048576",
        try ChannelUri.udpEndpoint(&buffer, "127.0.0.1", 40123, 1 * 1024 * 1024),
    );
}

test "embedded driver IPC exclusive publication round trip" {
    var dir_buffer: [128]u8 = undefined;
    const dir = try std.fmt.bufPrintZ(&dir_buffer, "/tmp/ringloom-aeron-test-{d}", .{std.os.linux.getpid()});
    try std.Io.Dir.cwd().createDirPath(std.testing.io, dir);
    defer std.Io.Dir.cwd().deleteTree(std.testing.io, dir) catch {};

    var driver = try Driver.initEmbedded(.{
        .directory = dir,
        .delete_dir_on_start = true,
        .delete_dir_on_shutdown = true,
        .ipc_term_buffer_length = 1024 * 1024,
    }, .shared);
    defer driver.deinit();

    var agents = try driver.agents(.shared);
    var client = try Client.connect(.{
        .directory = dir,
        .use_conductor_agent_invoker = true,
        .driver_timeout_ms = 5000,
    });
    defer client.deinit();

    var uri_buffer: [64]u8 = undefined;
    const channel = try ChannelUri.ipc(&uri_buffer, 1024 * 1024);
    const stream_id: i32 = 1001;

    var subscription = try client.addSubscription(channel, stream_id, &agents);
    defer subscription.close() catch {};

    var publication = try client.addExclusivePublication(channel, stream_id, &agents);
    defer publication.close() catch {};

    const payload = "ringloom-aeron";
    try offerUntilAccepted(&publication, payload, &client, &agents);

    const Capture = struct {
        bytes: [64]u8 = undefined,
        len: usize = 0,
        fragments: usize = 0,

        fn onFragment(context: ?*anyopaque, bytes: []const u8) void {
            const self: *@This() = @ptrCast(@alignCast(context.?));
            const copied_len = @min(bytes.len, self.bytes.len);
            @memcpy(self.bytes[0..copied_len], bytes[0..copied_len]);
            self.len = copied_len;
            self.fragments += 1;
        }
    };

    var capture = Capture{};
    const handler = FragmentHandler{
        .context = &capture,
        .callback = Capture.onFragment,
    };

    for (0..5000) |_| {
        if (capture.fragments != 0) break;
        _ = try client.invokeConductor();
        _ = try agents.invokeAll();
        _ = try subscription.poll(handler, 10);
        sleepOneMs();
    }

    try testing.expectEqual(@as(usize, 1), capture.fragments);
    try testing.expectEqualStrings(payload, capture.bytes[0..capture.len]);
}

fn offerUntilAccepted(
    publication: *ExclusivePublication,
    payload: []const u8,
    client: *Client,
    agents: *DriverAgents,
) Error!void {
    for (0..5000) |_| {
        switch (publication.offer(payload)) {
            .position => return,
            .not_connected, .back_pressured, .admin_action => {},
            .closed, .max_position_exceeded, .failed => return error.AeronCallFailed,
        }

        _ = try client.invokeConductor();
        _ = try agents.invokeAll();
        sleepOneMs();
    }

    return error.Timeout;
}

fn sleepOneMs() void {
    const io = std.Io.Threaded.global_single_threaded.io();
    std.Io.sleep(io, .fromMilliseconds(1), .awake) catch unreachable;
}
