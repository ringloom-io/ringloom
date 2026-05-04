//! test_echo_service — receives messages and optionally measures one-way latency.
//!
//! Used by end-to-end tests to validate message delivery. Registers with the
//! broker, sets a message handler that counts and logs received messages, and
//! exits after reaching an optional max-messages limit.
//!
//! When `--result-file` is provided, enables latency measurement mode:
//! expects first 8 bytes of each payload to be a monotonic nanosecond timestamp
//! and byte 9 to be a phase flag (0=warmup, 1=measured). Records one-way
//! latency for measured-phase messages and writes results on shutdown.

const std = @import("std");
const builtin = @import("builtin");
const ringloom_service = @import("ringloom_service");
const ringloom_common = @import("ringloom_common");
const ringloom_testing = @import("ringloom_testing");

const RingLoomEngine = ringloom_service.RingLoomEngine;
const ServiceConfig = ringloom_service.ServiceConfig;
const RingBuffer = ringloom_common.concurrent.ring_buffer.RingBuffer;
const Clock = ringloom_common.Clock;
const platform = ringloom_common.platform;
const latency_trace = ringloom_common.message.latency_trace;
const Histogram = ringloom_testing.Histogram;

// ── Mutable file-level state (fine for a single-threaded test binary) ─

var received_count: u64 = 0;
var measured_count: u64 = 0;
var max_messages: u64 = 0;
var shutdown_flag: std.atomic.Value(bool) = std.atomic.Value(bool).init(false);
var reply_delay_ms: u64 = 0;
var quiet_mode: bool = false;

// Latency measurement state (set from main before handler registration).
var latency_histogram: ?*Histogram = null;
var broker_a_queue_histogram: ?*Histogram = null;
var transport_histogram: ?*Histogram = null;
var broker_b_delivery_histogram: ?*Histogram = null;
var stage_breakdown_measured: u64 = 0;
var runtime_io: std.Io = undefined;

fn messageHandler(_: i32, payload: []const u8) void {
    received_count += 1;

    if (!quiet_mode) {
        var buf: [512]u8 = undefined;
        var stdout_w = std.Io.File.stdout().writer(runtime_io, &buf);
        const stdout = &stdout_w.interface;
        stdout.print("echo: received msg {d}, len={d}\n", .{ received_count, payload.len }) catch {};
        stdout.flush() catch {};
    }

    // One-way latency measurement: extract timestamp and phase flag.
    if (latency_histogram) |hist| {
        if (latency_trace.isMeasured(payload)) {
            if (latency_trace.readSendTimestamp(payload)) |send_ts| {
                const recv_ts: u64 = @intCast(Clock.monotonicNanosStable());
                if (recv_ts > send_ts) {
                    hist.record(recv_ts - send_ts) catch {};
                    measured_count += 1;
                }

                if (latency_trace.readStageTrace(payload)) |trace| {
                    if (broker_a_queue_histogram) |sender_hist| {
                        sender_hist.record(trace.sender_dequeue_ns - trace.send_ts_ns) catch {};
                    }
                    if (transport_histogram) |network_hist| {
                        network_hist.record(trace.receiver_ingress_ns - trace.sender_dequeue_ns) catch {};
                    }
                    if (broker_b_delivery_histogram) |delivery_hist| {
                        if (recv_ts > trace.receiver_ingress_ns) {
                            delivery_hist.record(recv_ts - trace.receiver_ingress_ns) catch {};
                            stage_breakdown_measured += 1;
                        }
                    }
                }
            }
        }
    }

    if (reply_delay_ms > 0) {
        platform.sleepNanos(reply_delay_ms * std.time.ns_per_ms);
    }

    if (max_messages > 0 and received_count >= max_messages) {
        shutdown_flag.store(true, .release);
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

    const args = try init.minimal.args.toSlice(init.arena.allocator());

    const storage_path = parseStringArg(args, "--storage-path", "/dev/shm");
    const group = parseStringArg(args, "--group", "default");
    const service_name = parseStringArg(args, "--service-name", "echo");
    const broker_node_id: i16 = @intCast(parseU64Arg(args, "--broker-node-id", 1));
    max_messages = parseU64Arg(args, "--max-messages", 0);
    reply_delay_ms = parseU64Arg(args, "--reply-delay-ms", 0);
    quiet_mode = parseBoolArg(args, "--quiet");
    const result_file = parseOptionalStringArg(args, "--result-file");
    const idle_strategy_str = parseStringArg(args, "--idle-strategy", "backoff");

    var stdout_buf: [4096]u8 = undefined;
    var stdout_w = std.Io.File.stdout().writer(io, &stdout_buf);
    const stdout = &stdout_w.interface;
    var stderr_buf: [4096]u8 = undefined;
    var stderr_w = std.Io.File.stderr().writer(io, &stderr_buf);
    const stderr = &stderr_w.interface;

    try stderr.print("echo: starting with storage_path={s}, group={s}, name={s}\n", .{
        storage_path, group, service_name,
    });
    try stderr.flush();

    // Initialize latency histogram if result file is requested.
    var histogram: Histogram = if (result_file != null) blk: {
        const capacity: usize = if (max_messages > 0) max_messages else 200_000;
        break :blk Histogram.initCapacity(allocator, capacity) catch Histogram.init(allocator);
    } else Histogram.init(allocator);
    defer histogram.deinit();

    if (result_file != null) {
        latency_histogram = &histogram;
    }
    defer latency_histogram = null;

    var broker_a_queue: Histogram = if (result_file != null) blk: {
        const capacity: usize = if (max_messages > 0) max_messages else 200_000;
        break :blk Histogram.initCapacity(allocator, capacity) catch Histogram.init(allocator);
    } else Histogram.init(allocator);
    defer broker_a_queue.deinit();

    var transport_latency: Histogram = if (result_file != null) blk: {
        const capacity: usize = if (max_messages > 0) max_messages else 200_000;
        break :blk Histogram.initCapacity(allocator, capacity) catch Histogram.init(allocator);
    } else Histogram.init(allocator);
    defer transport_latency.deinit();

    var broker_b_delivery: Histogram = if (result_file != null) blk: {
        const capacity: usize = if (max_messages > 0) max_messages else 200_000;
        break :blk Histogram.initCapacity(allocator, capacity) catch Histogram.init(allocator);
    } else Histogram.init(allocator);
    defer broker_b_delivery.deinit();

    if (result_file != null) {
        broker_a_queue_histogram = &broker_a_queue;
        transport_histogram = &transport_latency;
        broker_b_delivery_histogram = &broker_b_delivery;
    }
    defer {
        broker_a_queue_histogram = null;
        transport_histogram = null;
        broker_b_delivery_histogram = null;
    }

    const idle_strategy: platform.IdleStrategy = if (std.mem.eql(u8, idle_strategy_str, "busy_spin"))
        .busy_spin
    else if (std.mem.eql(u8, idle_strategy_str, "yielding"))
        .yielding
    else if (std.mem.eql(u8, idle_strategy_str, "sleeping"))
        .sleeping
    else
        .{ .backoff = .{} };

    const engine = RingLoomEngine.start(allocator, ServiceConfig{
        .storage_path = storage_path,
        .group = group,
        .service_name = service_name,
        .broker_node_id = broker_node_id,
        .idle_strategy = idle_strategy,
    }) catch |err| {
        try stderr.print("echo: failed to start engine: {}\n", .{err});
        try stderr.flush();
        std.process.exit(1);
    };
    defer engine.deinit();

    engine.setMessageHandler(&messageHandler);

    // Install SIGTERM handler for graceful shutdown.
    installSignalHandler();

    try stdout.print("service ready: name={s}\n", .{service_name});
    try stdout.flush();

    // Main loop: sleep 100ms, check shutdown flag.
    while (!shutdown_flag.load(.acquire)) {
        platform.sleepNanos(100 * std.time.ns_per_ms);
    }

    // Stop engine first to ensure consumer thread is done before reading histogram.
    engine.stop();
    latency_histogram = null;

    try stdout.print("echo: total_received={d}, measured={d}, stage_breakdown={d}\n", .{
        received_count,
        measured_count,
        stage_breakdown_measured,
    });
    try stdout.flush();

    // Write latency results if requested.
    if (result_file) |path| {
        writeLatencyResults(
            io,
            path,
            &histogram,
            &broker_a_queue,
            &transport_latency,
            &broker_b_delivery,
            received_count,
            measured_count,
            stage_breakdown_measured,
            service_name,
        ) catch |err| {
            try stderr.print("echo: failed to write result file '{s}': {}\n", .{ path, err });
            try stderr.flush();
        };
    }

    try stderr.print("echo: shutdown complete\n", .{});
    try stderr.flush();
}

fn writeLatencyResults(
    io: std.Io,
    path: []const u8,
    histogram: *Histogram,
    broker_a_queue: *Histogram,
    transport_latency: *Histogram,
    broker_b_delivery: *Histogram,
    total_received: u64,
    total_measured: u64,
    traced_measured: u64,
    service_name: []const u8,
) !void {
    const latency = histogram.summaryPercentiles();
    const broker_a_queue_summary = broker_a_queue.summaryPercentiles();
    const transport_summary = transport_latency.summaryPercentiles();
    const broker_b_delivery_summary = broker_b_delivery.summaryPercentiles();

    const file = if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.createFileAbsolute(io, path, .{})
    else
        try std.Io.Dir.cwd().createFile(io, path, .{});
    defer file.close(io);

    var write_buf: [8192]u8 = undefined;
    var file_w = file.writer(io, &write_buf);
    const writer = &file_w.interface;
    try writer.print(
        \\{{
        \\  "service_name": "{s}",
        \\  "total_received": {d},
        \\  "total_measured": {d},
        \\  "stage_breakdown_measured": {d},
        \\  "latency_p50_ns": {d},
        \\  "latency_p95_ns": {d},
        \\  "latency_p99_ns": {d},
        \\  "latency_p99_9_ns": {d},
        \\  "latency_max_ns": {d},
        \\  "broker_a_queue_p50_ns": {d},
        \\  "broker_a_queue_p95_ns": {d},
        \\  "broker_a_queue_p99_ns": {d},
        \\  "broker_a_queue_p99_9_ns": {d},
        \\  "broker_a_queue_max_ns": {d},
        \\  "transport_p50_ns": {d},
        \\  "transport_p95_ns": {d},
        \\  "transport_p99_ns": {d},
        \\  "transport_p99_9_ns": {d},
        \\  "transport_max_ns": {d},
        \\  "broker_b_delivery_p50_ns": {d},
        \\  "broker_b_delivery_p95_ns": {d},
        \\  "broker_b_delivery_p99_ns": {d},
        \\  "broker_b_delivery_p99_9_ns": {d},
        \\  "broker_b_delivery_max_ns": {d}
        \\}}
        \\
    , .{
        service_name,
        total_received,
        total_measured,
        traced_measured,
        latency.p50,
        latency.p95,
        latency.p99,
        latency.p99_9,
        latency.max_val,
        broker_a_queue_summary.p50,
        broker_a_queue_summary.p95,
        broker_a_queue_summary.p99,
        broker_a_queue_summary.p99_9,
        broker_a_queue_summary.max_val,
        transport_summary.p50,
        transport_summary.p95,
        transport_summary.p99,
        transport_summary.p99_9,
        transport_summary.max_val,
        broker_b_delivery_summary.p50,
        broker_b_delivery_summary.p95,
        broker_b_delivery_summary.p99,
        broker_b_delivery_summary.p99_9,
        broker_b_delivery_summary.max_val,
    });
    try writer.flush();
}

// ── Signal handling ───────────────────────────────────────────────────

fn installSignalHandler() void {
    const handler: std.posix.Sigaction = .{
        .handler = .{ .handler = signalHandler },
        .mask = std.posix.sigemptyset(),
        .flags = 0,
    };
    std.posix.sigaction(std.posix.SIG.TERM, &handler, null);
    std.posix.sigaction(std.posix.SIG.INT, &handler, null);
}

fn signalHandler(_: std.posix.SIG) callconv(.c) void {
    shutdown_flag.store(true, .release);
}

// ── Argument parsing helpers ──────────────────────────────────────────

fn parseStringArg(args: []const [:0]const u8, flag: []const u8, default: []const u8) []const u8 {
    var i: usize = 0;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], flag) and i + 1 < args.len) {
            return args[i + 1];
        }
    }
    return default;
}

fn parseU64Arg(args: []const [:0]const u8, flag: []const u8, default: u64) u64 {
    var i: usize = 0;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], flag) and i + 1 < args.len) {
            return std.fmt.parseInt(u64, args[i + 1], 10) catch default;
        }
    }
    return default;
}

fn parseBoolArg(args: []const [:0]const u8, flag: []const u8) bool {
    for (args) |arg| {
        if (std.mem.eql(u8, arg, flag)) return true;
    }
    return false;
}

fn parseOptionalStringArg(args: []const [:0]const u8, flag: []const u8) ?[]const u8 {
    var i: usize = 0;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], flag) and i + 1 < args.len) {
            return args[i + 1];
        }
    }
    return null;
}
