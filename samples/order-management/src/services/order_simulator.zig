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
    gateway_client: ?*ServiceClient = null,
    terminal_rejects: u64 = 0,
};

fn onLifecycle(_: ?*anyopaque, event: ServiceClient.LifecycleEvent) void {
    app.logLifecycle(runtime_io, names.order_simulator, event);
}

fn onMessage(msg_type_id: i32, payload: []const u8) void {
    state.counters.messages_received += 1;
    const template = protocol.templateFromMsgType(msg_type_id) catch {
        state.counters.unknown_template += 1;
        return;
    };
    switch (template) {
        .order_rejected => {
            _ = protocol.copyPayloadAs(protocol.OrderRejected, payload) catch {
                state.counters.bad_payload += 1;
                return;
            };
            state.terminal_rejects += 1;
        },
        else => state.counters.unknown_template += 1,
    }
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
    const common = args_mod.parseCommon(argv, names.order_simulator);
    const orders = args_mod.parseInt(u64, argv, "--orders", 1000);
    const rate_per_sec = args_mod.parseInt(u64, argv, "--rate-per-sec", 10_000);
    const burst_size = @min(args_mod.parseInt(u16, argv, "--burst-size", 4), protocol.max_bulk_orders);
    const duration_sec = args_mod.parseInt(u64, argv, "--duration-sec", 0);
    _ = args_mod.parseInt(usize, argv, "--message-size", @sizeOf(protocol.NewOrder));
    app.installSignalHandler(&shutdown_requested);

    const engine = RingLoomEngine.start(allocator, ServiceConfig{
        .storage_path = common.storage_path,
        .group = common.group,
        .service_name = common.service_name,
        .broker_node_id = common.broker_node_id,
        .idle_strategy = common.idle_strategy,
    }) catch |err| fatal(io, "order-simulator: failed to start engine: {}\n", .{err});
    engine.setMessageHandler(&onMessage);

    const gateway_client = engine.createClient(names.order_gateway) catch |err|
        fatal(io, "order-simulator: failed to create gateway client: {}\n", .{err});
    state.gateway_client = gateway_client;
    if (!app.waitForClient(gateway_client, 10_000)) {
        fatal(io, "order-simulator: order-gateway was not discovered\n", .{});
    }

    try app.printReady(io, common.service_name);
    runScenario(orders, rate_per_sec, burst_size, duration_sec);

    platform.sleepNanos(500 * std.time.ns_per_ms);
    shutdown_requested.store(true, .release);
    engine.stop();

    try printSummary(io, common.service_name);
    if (common.result_file) |path| {
        try counters_mod.writeJson(io, path, common.service_name, state.counters, "terminal_rejects", state.terminal_rejects);
    }
}

fn runScenario(orders: u64, rate_per_sec: u64, burst_size: u16, duration_sec: u64) void {
    const start_ns = Clock.monotonicNanosStable();
    const duration_ns = duration_sec * std.time.ns_per_s;
    const interval_ns: u64 = if (rate_per_sec > 0) std.time.ns_per_s / rate_per_sec else 0;
    var next_deadline: u64 = @intCast(start_ns);
    var seq: u64 = 1;

    while (seq <= orders and !shutdown_requested.load(.acquire)) {
        if (duration_ns > 0 and @as(u64, @intCast(Clock.monotonicNanosStable() - start_ns)) >= duration_ns) break;
        pace(&next_deadline, interval_ns);

        if (seq % 53 == 0 and burst_size > 1) {
            const sent = sendBulk(seq, @min(@as(u16, @intCast(orders - seq + 1)), burst_size));
            seq += sent;
        } else if (seq % 17 == 0) {
            sendCancel(seq);
            seq += 1;
        } else {
            sendNewOrder(seq);
            seq += 1;
        }
    }
}

fn pace(next_deadline: *u64, interval_ns: u64) void {
    if (interval_ns == 0) return;
    const now: u64 = @intCast(Clock.monotonicNanosStable());
    if (next_deadline.* > now) {
        platform.sleepNanos(next_deadline.* - now);
    }
    next_deadline.* +|= interval_ns;
}

fn sendNewOrder(seq: u64) void {
    const order = deterministicOrder(seq);
    sendBody(protocol.NewOrder, .new_order, seq, order);
    state.counters.orders_generated += 1;
}

fn sendCancel(seq: u64) void {
    const order = deterministicOrder(seq);
    const cancel = protocol.CancelOrder{
        .account_id = order.account_id,
        .order_id = seq,
        .cancel_order_id = if (seq > 1) seq - 1 else seq,
        .symbol = order.symbol,
    };
    sendBody(protocol.CancelOrder, .cancel_order, seq, cancel);
    state.counters.orders_generated += 1;
}

fn sendBulk(seq: u64, count: u16) u64 {
    var batch = protocol.BulkOrderBatch{
        .batch_id = seq,
        .count = count,
        .orders = undefined,
    };
    for (0..protocol.max_bulk_orders) |i| {
        batch.orders[i] = deterministicOrder(seq + i);
    }
    sendBody(protocol.BulkOrderBatch, .bulk_order_batch, seq, batch);
    state.counters.orders_generated += count;
    return count;
}

fn sendBody(comptime T: type, template: protocol.TemplateId, correlation_id: u64, body: T) void {
    const client = state.gateway_client orelse {
        state.counters.no_available_instance += 1;
        return;
    };
    // Simulator policy: do not spin indefinitely; pacing plus counters make
    // back-pressure visible without hiding congestion from the sample output.
    protocol.sendClaimed(T, client, template, .{
        .correlation_id = @intCast(correlation_id),
        .created_ns = @intCast(Clock.monotonicNanosStable()),
        .stage_ns = @intCast(Clock.monotonicNanosStable()),
        .source_stage = .simulator,
    }, body) catch |err| {
        state.counters.recordSendError(err);
        return;
    };
    state.counters.messages_sent += 1;
}

fn deterministicOrder(seq: u64) protocol.NewOrder {
    const account = tables.accounts[seq % tables.accounts.len];
    const symbol = tables.symbols[seq % tables.symbols.len];
    const quantity: u32 = if (seq % 29 == 0) 0 else @intCast((seq % 250) + 1);
    const price = if (seq % 31 == 0)
        tables.max_price_nanos + 1_000_000_000
    else
        tables.deterministicPrice(seq, symbol);
    return .{
        .account_id = account.id,
        .order_id = seq,
        .symbol = symbol,
        .side = if (seq % 2 == 0) .buy else .sell,
        .quantity = quantity,
        .price_nanos = price,
        .tif = .day,
    };
}

fn printSummary(io: std.Io, service_name: []const u8) !void {
    var buf: [1024]u8 = undefined;
    var stdout_state = std.Io.File.stdout().writer(io, &buf);
    const stdout = &stdout_state.interface;
    try stdout.print(
        "{s}: orders_generated={d} messages_sent={d} send_failures={d} terminal_rejects={d}\n",
        .{
            service_name,
            state.counters.orders_generated,
            state.counters.messages_sent,
            state.counters.send_failures,
            state.terminal_rejects,
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
