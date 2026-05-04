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
    risk_client: ?*ServiceClient = null,
    simulator_client: ?*ServiceClient = null,
    next_gateway_sequence: u64 = 1,
};

fn onLifecycle(_: ?*anyopaque, event: ServiceClient.LifecycleEvent) void {
    app.logLifecycle(runtime_io, names.order_gateway, event);
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
        .new_order => handleNewOrder(payload),
        .cancel_order => handleCancel(payload),
        .bulk_order_batch => handleBulk(payload),
        .order_rejected => forwardReject(payload),
        else => state.counters.unknown_template += 1,
    }
}

fn handleNewOrder(payload: []const u8) void {
    const envelope = protocol.decodeEnvelope(payload) catch {
        state.counters.bad_payload += 1;
        return;
    };
    const order = protocol.copyPayloadAs(protocol.NewOrder, payload) catch {
        rejectMalformed(envelope);
        return;
    };
    processOrder(envelope, order);
}

fn handleBulk(payload: []const u8) void {
    const envelope = protocol.decodeEnvelope(payload) catch {
        state.counters.bad_payload += 1;
        return;
    };
    const batch = protocol.copyPayloadAs(protocol.BulkOrderBatch, payload) catch {
        state.counters.bad_payload += 1;
        return;
    };
    const count = @min(batch.count, protocol.max_bulk_orders);
    for (batch.orders[0..count]) |order| {
        processOrder(envelope, order);
    }
}

fn handleCancel(payload: []const u8) void {
    const envelope = protocol.decodeEnvelope(payload) catch {
        state.counters.bad_payload += 1;
        return;
    };
    const cancel = protocol.copyPayloadAs(protocol.CancelOrder, payload) catch {
        state.counters.bad_payload += 1;
        return;
    };
    const rejected = protocol.OrderRejected{
        .account_id = cancel.account_id,
        .order_id = cancel.order_id,
        .symbol = cancel.symbol,
        .reason = .unsupported_message,
        .stage = .gateway,
    };
    sendReject(envelope, rejected);
}

fn processOrder(envelope: *const protocol.Envelope, order: protocol.NewOrder) void {
    const reason = validateOrder(envelope, order);
    if (reason) |why| {
        sendReject(envelope, .{
            .account_id = order.account_id,
            .order_id = order.order_id,
            .symbol = order.symbol,
            .reason = why,
            .stage = .gateway,
        });
        return;
    }

    const risk_client = state.risk_client orelse {
        sendReject(envelope, .{
            .account_id = order.account_id,
            .order_id = order.order_id,
            .symbol = order.symbol,
            .reason = .system_busy,
            .stage = .gateway,
        });
        return;
    };
    const request = protocol.RiskCheckRequest{
        .gateway_sequence = nextGatewaySequence(&state),
        .order = order,
    };
    // Gateway policy: fail fast under pressure and turn the order into a fixed
    // system_busy reject rather than blocking the inbound handler.
    protocol.sendClaimed(protocol.RiskCheckRequest, risk_client, .risk_check_request, .{
        .correlation_id = envelope.correlation_id,
        .created_ns = envelope.created_ns,
        .stage_ns = @intCast(Clock.monotonicNanosStable()),
        .source_stage = .gateway,
    }, request) catch |err| {
        state.counters.recordSendError(err);
        sendReject(envelope, .{
            .account_id = order.account_id,
            .order_id = order.order_id,
            .symbol = order.symbol,
            .reason = .system_busy,
            .stage = .gateway,
        });
        return;
    };
    state.counters.orders_validated += 1;
    state.counters.messages_sent += 1;
}

fn validateOrder(envelope: *const protocol.Envelope, order: protocol.NewOrder) ?protocol.RejectReason {
    if (envelope.version != protocol.ProtocolVersion) return .unsupported_version;
    if (!tables.isKnownSymbol(order.symbol)) return .unknown_symbol;
    if (order.quantity == 0) return .zero_quantity;
    if (!tables.validPrice(order.price_nanos)) return .bad_price;
    return null;
}

fn forwardReject(payload: []const u8) void {
    const envelope = protocol.decodeEnvelope(payload) catch {
        state.counters.bad_payload += 1;
        return;
    };
    const rejected = protocol.copyPayloadAs(protocol.OrderRejected, payload) catch {
        state.counters.bad_payload += 1;
        return;
    };
    sendReject(envelope, rejected);
}

fn rejectMalformed(envelope: *const protocol.Envelope) void {
    sendReject(envelope, .{
        .account_id = 0,
        .order_id = @intCast(envelope.correlation_id),
        .symbol = .aapl,
        .reason = .malformed,
        .stage = .gateway,
    });
}

fn sendReject(envelope: *const protocol.Envelope, rejected: protocol.OrderRejected) void {
    state.counters.orders_rejected += 1;
    const simulator_client = state.simulator_client orelse {
        state.counters.no_available_instance += 1;
        return;
    };
    protocol.sendClaimed(protocol.OrderRejected, simulator_client, .order_rejected, .{
        .correlation_id = envelope.correlation_id,
        .created_ns = envelope.created_ns,
        .stage_ns = @intCast(Clock.monotonicNanosStable()),
        .source_stage = .gateway,
    }, rejected) catch |err| state.counters.recordSendError(err);
}

fn nextGatewaySequence(self: *State) u64 {
    const seq = self.next_gateway_sequence;
    self.next_gateway_sequence += 1;
    return seq;
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
    const common = args_mod.parseCommon(argv, names.order_gateway);
    app.installSignalHandler(&shutdown_requested);

    const engine = RingLoomEngine.start(allocator, ServiceConfig{
        .storage_path = common.storage_path,
        .group = common.group,
        .service_name = common.service_name,
        .broker_node_id = common.broker_node_id,
        .idle_strategy = common.idle_strategy,
    }) catch |err| fatal(io, "order-gateway: failed to start engine: {}\n", .{err});
    defer engine.deinit();
    engine.setMessageHandler(&onMessage);

    const risk_client = engine.createClient(names.risk_service) catch |err|
        fatal(io, "order-gateway: failed to create risk client: {}\n", .{err});
    const simulator_client = engine.createClient(names.order_simulator) catch |err|
        fatal(io, "order-gateway: failed to create simulator client: {}\n", .{err});
    state.risk_client = risk_client;
    state.simulator_client = simulator_client;
    _ = app.waitForClient(risk_client, 5000);

    try app.printReady(io, common.service_name);
    while (!shutdown_requested.load(.acquire)) {
        platform.sleepNanos(100 * std.time.ns_per_ms);
    }

    engine.stop();
    try printSummary(io, common.service_name);
    if (common.result_file) |path| {
        try counters_mod.writeJson(io, path, common.service_name, state.counters, "gateway_sequence", state.next_gateway_sequence - 1);
    }
}

fn printSummary(io: std.Io, service_name: []const u8) !void {
    var buf: [1024]u8 = undefined;
    var stdout_state = std.Io.File.stdout().writer(io, &buf);
    const stdout = &stdout_state.interface;
    try stdout.print(
        "{s}: orders_validated={d} orders_rejected={d} send_failures={d} bad_payload={d}\n",
        .{
            service_name,
            state.counters.orders_validated,
            state.counters.orders_rejected,
            state.counters.send_failures,
            state.counters.bad_payload,
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
