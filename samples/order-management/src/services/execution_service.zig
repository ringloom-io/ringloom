// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const builtin = @import("builtin");
const ringloom_service = @import("ringloom_service");
const ringloom_common = @import("ringloom_common");
const sample = @import("order_management_sample_common");

const RingLoomEngine = ringloom_service.RingLoomEngine;
const ServiceConfig = ringloom_service.ServiceConfig;
const ServiceClient = ringloom_service.ServiceClient;
const Clock = ringloom_common.platform.Clock;
const platform = ringloom_common.platform;
const args_mod = sample.args;
const app = sample.app;
const counters_mod = sample.counters;
const protocol = sample.protocol;
const names = sample.service_names;

var runtime_io: std.Io = undefined;
var shutdown_requested: std.atomic.Value(bool) = std.atomic.Value(bool).init(false);
var state: State = .{};

const State = struct {
    counters: counters_mod.Counters = .{},
    portfolio_client: ?*ServiceClient = null,
};

fn onLifecycle(_: ?*anyopaque, event: ServiceClient.LifecycleEvent) void {
    app.logLifecycle(runtime_io, names.execution_service, event);
}

fn onMessage(msg_type_id: i32, payload: []const u8) void {
    const start = Clock.monotonicNanosStable();
    defer state.counters.observeHandlerNs(@intCast(Clock.monotonicNanosStable() - start));
    state.counters.messages_received += 1;

    const template = protocol.templateFromMsgType(msg_type_id) catch {
        state.counters.unknown_template += 1;
        return;
    };

    switch (template) {
        .fill => handleFill(payload),
        else => state.counters.unknown_template += 1,
    }
}

fn handleFill(payload: []const u8) void {
    const envelope = protocol.decodeEnvelope(payload) catch {
        state.counters.bad_payload += 1;
        return;
    };
    const fill = protocol.copyPayloadAs(protocol.Fill, payload) catch {
        state.counters.bad_payload += 1;
        return;
    };
    const client = state.portfolio_client orelse {
        state.counters.no_available_instance += 1;
        return;
    };

    const report = protocol.ExecutionReport{
        .account_id = fill.account_id,
        .order_id = fill.order_id,
        .symbol = fill.symbol,
        .side = fill.side,
        .quantity = fill.quantity,
        .price_nanos = fill.price_nanos,
        .status = .filled,
    };
    protocol.sendClaimed(protocol.ExecutionReport, client, .execution_report, .{
        .correlation_id = envelope.correlation_id,
        .created_ns = envelope.created_ns,
        .stage_ns = @intCast(Clock.monotonicNanosStable()),
        .source_stage = .execution,
    }, report) catch |err| {
        state.counters.recordSendError(err);
        return;
    };
    state.counters.execution_reports_emitted += 1;
    state.counters.messages_sent += 1;
}

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    runtime_io = io;
    var debug_alloc: std.heap.DebugAllocator(.{}) = .init;
    const allocator = switch (builtin.mode) {
        .Debug, .ReleaseSafe => debug_alloc.allocator(),
        .ReleaseFast, .ReleaseSmall => std.heap.smp_allocator,
    };
    defer if (builtin.mode == .Debug or builtin.mode == .ReleaseSafe) {
        _ = debug_alloc.deinit();
    };

    const argv = try init.minimal.args.toSlice(init.arena.allocator());
    const common = args_mod.parseCommon(argv, names.execution_service);
    app.installSignalHandler(&shutdown_requested);

    const engine = RingLoomEngine.start(allocator, ServiceConfig{
        .storage_path = common.storage_path,
        .group = common.group,
        .service_name = common.service_name,
        .broker_node_id = common.broker_node_id,
        .idle_strategy = common.idle_strategy,
    }) catch |err| fatal(io, "execution-service: failed to start engine: {}\n", .{err});
    defer engine.deinit();
    engine.setMessageHandler(&onMessage);

    const portfolio_client = engine.createClient(names.portfolio_service) catch |err|
        fatal(io, "execution-service: failed to create portfolio client: {}\n", .{err});
    state.portfolio_client = portfolio_client;
    _ = app.waitForClient(portfolio_client, 5000);

    try app.printReady(io, common.service_name);
    while (!shutdown_requested.load(.acquire)) {
        platform.sleepNanos(100 * std.time.ns_per_ms);
    }

    engine.stop();
    try printSummary(io, common.service_name);
    if (common.result_file) |path| {
        try counters_mod.writeJson(io, path, common.service_name, state.counters, "reports_dropped", state.counters.send_failures);
    }
}

fn printSummary(io: std.Io, service_name: []const u8) !void {
    var buf: [1024]u8 = undefined;
    var stdout_state = std.Io.File.stdout().writer(io, &buf);
    const stdout = &stdout_state.interface;
    try stdout.print(
        "{s}: fills_received={d} execution_reports_emitted={d} send_failures={d} max_handler_ns={d}\n",
        .{
            service_name,
            state.counters.messages_received,
            state.counters.execution_reports_emitted,
            state.counters.send_failures,
            state.counters.max_handler_ns,
        },
    );
    try stdout.flush();
}

fn fatal(io: std.Io, comptime fmt: []const u8, values: anytype) noreturn {
    var buf: [1024]u8 = undefined;
    var stderr_state = std.Io.File.stderr().writer(io, &buf);
    const stderr = &stderr_state.interface;
    stderr.print(fmt, values) catch {};
    stderr.flush() catch {};
    std.process.exit(1);
}
