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
const tables = sample.static_tables;

var runtime_io: std.Io = undefined;
var shutdown_requested: std.atomic.Value(bool) = std.atomic.Value(bool).init(false);
var state: State = .{};

const State = struct {
    counters: counters_mod.Counters = .{},
    matching_client: ?*ServiceClient = null,
    gateway_client: ?*ServiceClient = null,
    leader_routing: bool = false,
    account_remaining: [tables.account_count]i64 = initAccountRemaining(),
    symbol_notional: [tables.symbol_count]i64 = [_]i64{0} ** tables.symbol_count,
};

fn initAccountRemaining() [tables.account_count]i64 {
    var out: [tables.account_count]i64 = undefined;
    for (tables.accounts, 0..) |account, i| out[i] = account.credit_nanos;
    return out;
}

fn onLifecycle(_: ?*anyopaque, event: ServiceClient.LifecycleEvent) void {
    app.logLifecycle(runtime_io, names.risk_service, event);
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
        .risk_check_request => handleRiskCheck(payload),
        else => state.counters.unknown_template += 1,
    }
}

fn handleRiskCheck(payload: []const u8) void {
    const envelope = protocol.decodeEnvelope(payload) catch {
        state.counters.bad_payload += 1;
        return;
    };
    const request = protocol.copyPayloadAs(protocol.RiskCheckRequest, payload) catch {
        state.counters.bad_payload += 1;
        return;
    };
    const account_idx = tables.accountIndex(request.order.account_id) orelse {
        rejectToGateway(envelope, request.order, .risk_credit);
        return;
    };
    const symbol_idx = tables.symbolIndex(request.order.symbol) orelse {
        rejectToGateway(envelope, request.order, .unknown_symbol);
        return;
    };
    const notional = @as(i64, @intCast(request.order.quantity)) * request.order.price_nanos;
    if (notional > state.account_remaining[account_idx]) {
        rejectToGateway(envelope, request.order, .risk_credit);
        return;
    }
    if (state.symbol_notional[symbol_idx] + notional > tables.symbol_notional_limits[symbol_idx]) {
        rejectToGateway(envelope, request.order, .risk_symbol_limit);
        return;
    }

    state.account_remaining[account_idx] -= notional;
    state.symbol_notional[symbol_idx] += notional;
    const accepted = protocol.RiskAccepted{
        .gateway_sequence = request.gateway_sequence,
        .order = request.order,
        .accepted_notional_nanos = notional,
    };
    const client = state.matching_client orelse {
        rejectToGateway(envelope, request.order, .system_busy);
        return;
    };

    const fields = protocol.EnvelopeFields{
        .correlation_id = envelope.correlation_id,
        .created_ns = envelope.created_ns,
        .stage_ns = @intCast(Clock.monotonicNanosStable()),
        .source_stage = .risk,
    };
    if (state.leader_routing) {
        var buf: [protocol.messageCapacity()]u8 align(8) = undefined;
        const encoded = protocol.fillMessage(protocol.RiskAccepted, &buf, fields, accepted) catch {
            state.counters.bad_payload += 1;
            return;
        };
        client.sendToLeader(encoded) catch |err| {
            state.counters.recordSendError(err);
            rejectToGateway(envelope, request.order, .system_busy);
            return;
        };
    } else {
        protocol.sendClaimed(protocol.RiskAccepted, client, .risk_accepted, fields, accepted) catch |err| {
            state.counters.recordSendError(err);
            rejectToGateway(envelope, request.order, .system_busy);
            return;
        };
    }
    state.counters.risk_accepted += 1;
    state.counters.messages_sent += 1;
}

fn rejectToGateway(envelope: *const protocol.Envelope, order: protocol.NewOrder, reason: protocol.RejectReason) void {
    state.counters.risk_rejected += 1;
    state.counters.orders_rejected += 1;
    const client = state.gateway_client orelse {
        state.counters.no_available_instance += 1;
        return;
    };
    const rejected = protocol.OrderRejected{
        .account_id = order.account_id,
        .order_id = order.order_id,
        .symbol = order.symbol,
        .reason = reason,
        .stage = .risk,
    };
    protocol.sendClaimed(protocol.OrderRejected, client, .order_rejected, .{
        .correlation_id = envelope.correlation_id,
        .created_ns = envelope.created_ns,
        .stage_ns = @intCast(Clock.monotonicNanosStable()),
        .source_stage = .risk,
    }, rejected) catch |err| state.counters.recordSendError(err);
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
    const common = args_mod.parseCommon(argv, names.risk_service);
    state.leader_routing = args_mod.parseBool(argv, "--leader-routing");
    app.installSignalHandler(&shutdown_requested);

    const engine = RingLoomEngine.start(allocator, ServiceConfig{
        .storage_path = common.storage_path,
        .group = common.group,
        .service_name = common.service_name,
        .broker_node_id = common.broker_node_id,
        .idle_strategy = common.idle_strategy,
    }) catch |err| fatal(io, "risk-service: failed to start engine: {}\n", .{err});
    engine.setMessageHandler(&onMessage);

    const matching_client = engine.createClient(names.matching_engine) catch |err|
        fatal(io, "risk-service: failed to create matching client: {}\n", .{err});
    const gateway_client = engine.createClient(names.order_gateway) catch |err|
        fatal(io, "risk-service: failed to create gateway client: {}\n", .{err});
    state.matching_client = matching_client;
    state.gateway_client = gateway_client;
    _ = app.waitForClient(matching_client, 5000);
    _ = app.waitForClient(gateway_client, 5000);

    try app.printReady(io, common.service_name);
    while (!shutdown_requested.load(.acquire)) {
        platform.sleepNanos(100 * std.time.ns_per_ms);
    }

    engine.stop();
    try printSummary(io, common.service_name);
    if (common.result_file) |path| {
        try counters_mod.writeJson(io, path, common.service_name, state.counters, "remaining_credit", remainingCredit());
    }
}

fn remainingCredit() u64 {
    var total: u64 = 0;
    for (state.account_remaining) |remaining| {
        if (remaining > 0) total += @intCast(remaining);
    }
    return total;
}

fn printSummary(io: std.Io, service_name: []const u8) !void {
    var buf: [1024]u8 = undefined;
    var stdout_state = std.Io.File.stdout().writer(io, &buf);
    const stdout = &stdout_state.interface;
    try stdout.print(
        "{s}: risk_accepted={d} risk_rejected={d} send_failures={d} back_pressure={d}\n",
        .{
            service_name,
            state.counters.risk_accepted,
            state.counters.risk_rejected,
            state.counters.send_failures,
            state.counters.back_pressure,
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
