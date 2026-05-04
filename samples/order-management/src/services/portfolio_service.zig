// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const builtin = @import("builtin");
const ringloom_service = @import("ringloom_service");
const ringloom_common = @import("ringloom_common");
const sample = @import("order_management_sample_common");

const RingLoomEngine = ringloom_service.RingLoomEngine;
const ServiceConfig = ringloom_service.ServiceConfig;
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
    positions: [tables.account_count][tables.symbol_count]i64 = [_][tables.symbol_count]i64{
        [_]i64{0} ** tables.symbol_count,
    } ** tables.account_count,
};

fn onMessage(msg_type_id: i32, payload: []const u8) void {
    const start = Clock.monotonicNanosStable();
    defer state.counters.observeHandlerNs(@intCast(Clock.monotonicNanosStable() - start));
    state.counters.messages_received += 1;

    const template = protocol.templateFromMsgType(msg_type_id) catch {
        state.counters.unknown_template += 1;
        return;
    };

    switch (template) {
        .execution_report => handleExecutionReport(payload),
        .portfolio_snapshot_request => handleSnapshotRequest(payload),
        else => state.counters.unknown_template += 1,
    }
}

fn handleExecutionReport(payload: []const u8) void {
    const report = protocol.copyPayloadAs(protocol.ExecutionReport, payload) catch {
        state.counters.bad_payload += 1;
        return;
    };
    const account_idx = tables.accountIndex(report.account_id) orelse {
        state.counters.bad_payload += 1;
        return;
    };
    const symbol_idx = tables.symbolIndex(report.symbol) orelse {
        state.counters.bad_payload += 1;
        return;
    };
    const signed_qty: i64 = if (report.side == .buy)
        @intCast(report.quantity)
    else
        -@as(i64, @intCast(report.quantity));
    state.positions[account_idx][symbol_idx] += signed_qty;
    state.counters.portfolio_updates_applied += 1;
}

fn handleSnapshotRequest(payload: []const u8) void {
    _ = protocol.copyPayloadAs(protocol.PortfolioSnapshotRequest, payload) catch {
        state.counters.bad_payload += 1;
        return;
    };
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
    const common = args_mod.parseCommon(argv, names.portfolio_service);
    app.installSignalHandler(&shutdown_requested);

    const engine = RingLoomEngine.start(allocator, ServiceConfig{
        .storage_path = common.storage_path,
        .group = common.group,
        .service_name = common.service_name,
        .broker_node_id = common.broker_node_id,
        .idle_strategy = common.idle_strategy,
    }) catch |err| {
        try fatal(io, "portfolio-service: failed to start engine: {}\n", .{err});
    };
    engine.setMessageHandler(&onMessage);
    try app.printReady(io, common.service_name);

    while (!shutdown_requested.load(.acquire)) {
        platform.sleepNanos(100 * std.time.ns_per_ms);
    }

    engine.stop();
    try printSummary(io, common.service_name);
    if (common.result_file) |path| {
        try counters_mod.writeJson(
            io,
            path,
            common.service_name,
            state.counters,
            "open_position_sum",
            openPositionSum(),
        );
    }
}

fn openPositionSum() u64 {
    var sum: u64 = 0;
    for (state.positions) |row| {
        for (row) |pos| {
            sum +|= @abs(pos);
        }
    }
    return sum;
}

fn printSummary(io: std.Io, service_name: []const u8) !void {
    var buf: [1024]u8 = undefined;
    var stdout_state = std.Io.File.stdout().writer(io, &buf);
    const stdout = &stdout_state.interface;
    try stdout.print(
        "{s}: portfolio_updates_applied={d} bad_payload={d} unknown_template={d} max_handler_ns={d}\n",
        .{
            service_name,
            state.counters.portfolio_updates_applied,
            state.counters.bad_payload,
            state.counters.unknown_template,
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
