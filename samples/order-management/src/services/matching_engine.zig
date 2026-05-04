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

const Book = struct {
    available: [tables.symbol_count]u32 = .{ 1_000_000, 1_000_000, 1_000_000, 1_000_000 },

    fn matchOrder(self: *Book, order: protocol.NewOrder) ?protocol.Fill {
        const idx = tables.symbolIndex(order.symbol) orelse return null;
        const fill_qty = @min(self.available[idx], order.quantity);
        if (fill_qty == 0) return null;
        self.available[idx] -= fill_qty;
        return .{
            .account_id = order.account_id,
            .order_id = order.order_id,
            .symbol = order.symbol,
            .side = order.side,
            .quantity = fill_qty,
            .price_nanos = order.price_nanos,
            .gateway_sequence = 0,
        };
    }
};

const State = struct {
    counters: counters_mod.Counters = .{},
    execution_client: ?*ServiceClient = null,
    book: Book = .{},
};

fn onLifecycle(_: ?*anyopaque, event: ServiceClient.LifecycleEvent) void {
    app.logLifecycle(runtime_io, names.matching_engine, event);
}

fn onMessage(msg_type_id: i32, payload: []const u8) void {
    const start = Clock.monotonicNanosStable();
    defer state.counters.observeHandlerNs(@intCast(Clock.monotonicNanosStable() - start));
    state.counters.messages_received += 1;

    const template = protocol.templateFromMsgType(msg_type_id) catch blk: {
        // sendToLeader currently preserves the payload but not the template id;
        // matching accepts this fallback only for risk-originated payloads.
        break :blk protocol.TemplateId.risk_accepted;
    };

    switch (template) {
        .risk_accepted => handleRiskAccepted(payload),
        else => state.counters.unknown_template += 1,
    }
}

fn handleRiskAccepted(payload: []const u8) void {
    const envelope = protocol.decodeEnvelope(payload) catch {
        state.counters.bad_payload += 1;
        return;
    };
    const accepted = protocol.copyPayloadAs(protocol.RiskAccepted, payload) catch {
        state.counters.bad_payload += 1;
        return;
    };
    var fill = state.book.matchOrder(accepted.order) orelse {
        state.counters.orders_rejected += 1;
        return;
    };
    fill.gateway_sequence = accepted.gateway_sequence;

    const client = state.execution_client orelse {
        state.counters.no_available_instance += 1;
        return;
    };
    protocol.sendClaimed(protocol.Fill, client, .fill, .{
        .correlation_id = envelope.correlation_id,
        .created_ns = envelope.created_ns,
        .stage_ns = @intCast(Clock.monotonicNanosStable()),
        .source_stage = .matching,
    }, fill) catch |err| {
        state.counters.recordSendError(err);
        return;
    };
    state.counters.fills_emitted += 1;
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
    const common = args_mod.parseCommon(argv, names.matching_engine);
    const leader_election = args_mod.parseBool(argv, "--leader-election");
    app.installSignalHandler(&shutdown_requested);

    const engine = RingLoomEngine.start(allocator, ServiceConfig{
        .storage_path = common.storage_path,
        .group = common.group,
        .service_name = common.service_name,
        .broker_node_id = common.broker_node_id,
        .leader_election_enabled = leader_election,
        .idle_strategy = common.idle_strategy,
    }) catch |err| fatal(io, "matching-engine: failed to start engine: {}\n", .{err});
    engine.setMessageHandler(&onMessage);

    const execution_client = engine.createClient(names.execution_service) catch |err|
        fatal(io, "matching-engine: failed to create execution client: {}\n", .{err});
    state.execution_client = execution_client;
    _ = app.waitForClient(execution_client, 5000);

    try app.printReady(io, common.service_name);
    while (!shutdown_requested.load(.acquire)) {
        platform.sleepNanos(100 * std.time.ns_per_ms);
    }

    engine.stop();
    try printSummary(io, common.service_name);
    if (common.result_file) |path| {
        try counters_mod.writeJson(io, path, common.service_name, state.counters, "book_remaining", bookRemaining());
    }
}

fn bookRemaining() u64 {
    var total: u64 = 0;
    for (state.book.available) |qty| total += qty;
    return total;
}

fn printSummary(io: std.Io, service_name: []const u8) !void {
    var buf: [1024]u8 = undefined;
    var stdout_state = std.Io.File.stdout().writer(io, &buf);
    const stdout = &stdout_state.interface;
    try stdout.print(
        "{s}: fills_emitted={d} send_failures={d} bad_payload={d} unknown_template={d}\n",
        .{
            service_name,
            state.counters.fills_emitted,
            state.counters.send_failures,
            state.counters.bad_payload,
            state.counters.unknown_template,
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
