//! Test Ping Service — sends messages to a target service and records throughput.
//!
//! Used by end-to-end tests to validate message sending, service discovery,
//! and basic throughput measurement across processes.
//!
//! Usage:
//!   test_ping_service [--storage-path PATH] [--group GROUP]
//!       [--service-name NAME] [--target-service TARGET]
//!       [--message-count N] [--message-size N]
//!       [--warmup-count N] [--result-file PATH]

const std = @import("std");
const builtin = @import("builtin");
const brz_service = @import("brz_service");
const brz_common = @import("brz_common");
const brz_testing = @import("brz_testing");

const BrzEngine = brz_service.BrzEngine;
const ServiceConfig = brz_service.ServiceConfig;
const ServiceClient = brz_service.ServiceClient;
const Clock = brz_common.Clock;
const platform = brz_common.platform;
const latency_trace = brz_common.message.latency_trace;
const Histogram = brz_testing.Histogram;

// ── Mutable file-level state ─────────────────────────────────────────

var shutdown_flag: std.atomic.Value(bool) = std.atomic.Value(bool).init(false);

// ── Signal handling ──────────────────────────────────────────────────

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

// ── Arg Parsing Helpers ──────────────────────────────────────────────

fn parseStringArg(args: []const [:0]const u8, flag: []const u8, default: []const u8) []const u8 {
    var i: usize = 0;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], flag) and i + 1 < args.len) {
            return args[i + 1];
        }
    }
    return default;
}

fn parseIntArg(comptime T: type, args: []const [:0]const u8, flag: []const u8, default: T) T {
    var i: usize = 0;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], flag) and i + 1 < args.len) {
            return std.fmt.parseInt(T, args[i + 1], 10) catch default;
        }
    }
    return default;
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

// ── Main ─────────────────────────────────────────────────────────────

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    var debug_alloc: std.heap.DebugAllocator(.{}) = .init;
    const allocator = switch (builtin.mode) {
        .Debug, .ReleaseSafe => debug_alloc.allocator(),
        .ReleaseFast, .ReleaseSmall => std.heap.smp_allocator,
    };
    defer if (builtin.mode == .Debug or builtin.mode == .ReleaseSafe) {
        _ = debug_alloc.deinit();
    };

    var stdout_buf: [4096]u8 = undefined;
    var stdout_w = std.Io.File.stdout().writer(io, &stdout_buf);
    const stdout = &stdout_w.interface;
    var stderr_buf: [4096]u8 = undefined;
    var stderr_w = std.Io.File.stderr().writer(io, &stderr_buf);
    const stderr = &stderr_w.interface;

    // ── Parse CLI args ───────────────────────────────────────────────

    const args = try init.minimal.args.toSlice(init.arena.allocator());

    const storage_path = parseStringArg(args, "--storage-path", "/dev/shm");
    const group = parseStringArg(args, "--group", "default");
    const service_name = parseStringArg(args, "--service-name", "ping");
    const target_service = parseStringArg(args, "--target-service", "echo");
    const broker_node_id: i16 = @intCast(parseIntArg(u64, args, "--broker-node-id", 1));
    const message_count = parseIntArg(u64, args, "--message-count", 100);
    const message_size = parseIntArg(usize, args, "--message-size", 64);
    const warmup_count = parseIntArg(u64, args, "--warmup-count", 10);
    const result_file = parseOptionalStringArg(args, "--result-file");
    const spin_timeout_ms = parseIntArg(u64, args, "--spin-timeout-ms", 0);
    const send_interval_ns = parseIntArg(u64, args, "--send-interval-ns", 0);
    const idle_strategy_str = parseStringArg(args, "--idle-strategy", "backoff");

    installSignalHandler();

    // ── Start engine ─────────────────────────────────────────────────

    const idle_strategy: platform.IdleStrategy = if (std.mem.eql(u8, idle_strategy_str, "busy_spin"))
        .busy_spin
    else if (std.mem.eql(u8, idle_strategy_str, "yielding"))
        .yielding
    else if (std.mem.eql(u8, idle_strategy_str, "sleeping"))
        .sleeping
    else
        .{ .backoff = .{} };

    const config = ServiceConfig{
        .storage_path = storage_path,
        .group = group,
        .service_name = service_name,
        .broker_node_id = broker_node_id,
        .idle_strategy = idle_strategy,
    };

    const engine = BrzEngine.start(allocator, config) catch |err| {
        try stderr.print("ping: failed to start engine: {}\n", .{err});
        try stderr.flush();
        std.process.exit(1);
    };
    defer engine.stop();

    // ── Create client for target service ─────────────────────────────

    const client = engine.createClient(target_service) catch |err| {
        try stderr.print("ping: failed to create client for '{s}': {}\n", .{ target_service, err });
        try stderr.flush();
        std.process.exit(1);
    };

    try stdout.print("service ready: name={s}\n", .{service_name});
    try stdout.flush();

    // ── Wait for service discovery to propagate ──────────────────────

    platform.sleepNanos(500 * std.time.ns_per_ms);

    // ── Prepare payload ──────────────────────────────────────────────

    const payload = try allocator.alloc(u8, message_size);
    defer allocator.free(payload);
    @memset(payload, 0xAB);

    // ── Warmup phase ─────────────────────────────────────────────────

    var warmup_sent: u64 = 0;
    var warmup_failed: u64 = 0;
    var next_send_deadline_ns: u64 = 0;

    for (0..warmup_count) |_| {
        paceNextSend(&next_send_deadline_ns, send_interval_ns);
        latency_trace.embedSend(payload, latency_trace.warmup_phase, @intCast(Clock.monotonicNanosStable()));
        client.send(payload) catch {
            warmup_failed += 1;
            continue;
        };
        warmup_sent += 1;
    }

    if (warmup_count > 0) {
        try stdout.print("ping: warmup complete, sent={d}, failed={d}\n", .{ warmup_sent, warmup_failed });
        try stdout.flush();
    }

    // Small pause between warmup and measurement.
    if (warmup_count > 0) {
        platform.sleepNanos(50 * std.time.ns_per_ms);
    }
    next_send_deadline_ns = 0;

    // ── Measurement phase ────────────────────────────────────────────

    var sent: u64 = 0;
    var failed: u64 = 0;

    // Per-message send latency histogram.
    var histogram = Histogram.initCapacity(allocator, message_count) catch blk: {
        break :blk Histogram.init(allocator);
    };
    defer histogram.deinit();

    const start_time_ms = Clock.epochMillis();
    const start_mono = Clock.monotonicNanos();

    for (0..message_count) |_| {
        paceNextSend(&next_send_deadline_ns, send_interval_ns);
        latency_trace.embedSend(payload, latency_trace.measured_phase, @intCast(Clock.monotonicNanosStable()));

        const send_start = Clock.monotonicNanos();
        var succeeded = false;

        client.send(payload) catch {
            // Spinning backpressure: retry for up to spin_timeout_ms.
            if (spin_timeout_ms > 0) {
                const deadline_ns: u64 = spin_timeout_ms * std.time.ns_per_ms;
                const spin_start = Clock.monotonicNanos();

                while (true) {
                    // Re-embed timestamp before each retry so echo measures
                    // pipeline latency, not including spin wait.
                    latency_trace.embedSend(payload, latency_trace.measured_phase, @intCast(Clock.monotonicNanosStable()));

                    client.send(payload) catch {
                        const elapsed = Clock.monotonicNanos() - spin_start;
                        if (elapsed >= @as(i64, @intCast(deadline_ns))) break;
                        std.atomic.spinLoopHint();
                        continue;
                    };
                    succeeded = true;
                    break;
                }
            }

            if (!succeeded) {
                failed += 1;
                continue;
            }

            // Fall through: retry succeeded.
            const send_end = Clock.monotonicNanos();
            const delta: u64 = @intCast(send_end - send_start);
            histogram.record(delta) catch {};
            sent += 1;
            continue;
        };

        // First attempt succeeded.
        const send_end = Clock.monotonicNanos();
        const delta: u64 = @intCast(send_end - send_start);
        histogram.record(delta) catch {};
        sent += 1;
    }

    const end_mono = Clock.monotonicNanos();
    const end_time_ms = Clock.epochMillis();

    const elapsed_ns: u64 = @intCast(end_mono - start_mono);
    const elapsed_ms: u64 = elapsed_ns / std.time.ns_per_ms;
    const elapsed_us: u64 = elapsed_ns / std.time.ns_per_us;

    // ── Compute throughput ───────────────────────────────────────────

    const throughput_msgs_per_sec: u64 = if (elapsed_us > 0)
        (sent * 1_000_000) / elapsed_us
    else
        0;

    const throughput_bytes_per_sec: u64 = if (elapsed_us > 0)
        (sent * message_size * 1_000_000) / elapsed_us
    else
        0;

    // ── Compute latency percentiles ──────────────────────────────────

    const latency = histogram.summaryPercentiles();

    // ── Print summary ────────────────────────────────────────────────

    try stdout.print("ping: sent={d}, failed={d}, elapsed_ms={d}\n", .{ sent, failed, elapsed_ms });
    try stdout.print("ping: throughput={d} msgs/s, {d} bytes/s\n", .{ throughput_msgs_per_sec, throughput_bytes_per_sec });
    try stdout.print("ping: send_latency p50={d}ns p95={d}ns p99={d}ns p99.9={d}ns max={d}ns\n", .{ latency.p50, latency.p95, latency.p99, latency.p99_9, latency.max_val });
    try stdout.print("ping: message_size={d}, target={s}, send_interval_ns={d}\n", .{ message_size, target_service, send_interval_ns });
    try stdout.flush();

    // ── Write JSON results file (optional) ───────────────────────────

    if (result_file) |path| {
        writeResultsJson(
            io,
            path,
            sent,
            failed,
            elapsed_ms,
            elapsed_us,
            message_size,
            throughput_msgs_per_sec,
            throughput_bytes_per_sec,
            warmup_count,
            send_interval_ns,
            start_time_ms,
            end_time_ms,
            service_name,
            target_service,
            latency.p50,
            latency.p95,
            latency.p99,
            latency.p99_9,
            latency.max_val,
        ) catch |err| {
            try stderr.print("ping: failed to write result file '{s}': {}\n", .{ path, err });
            try stderr.flush();
        };
    }

    try stdout.print("ping: shutdown complete\n", .{});
    try stdout.flush();
}

fn writeResultsJson(
    io: std.Io,
    path: []const u8,
    sent: u64,
    failed: u64,
    elapsed_ms: u64,
    elapsed_us: u64,
    message_size: usize,
    throughput_msgs_per_sec: u64,
    throughput_bytes_per_sec: u64,
    warmup_count: u64,
    send_interval_ns: u64,
    start_time_ms: i64,
    end_time_ms: i64,
    service_name: []const u8,
    target_service: []const u8,
    latency_p50_ns: u64,
    latency_p95_ns: u64,
    latency_p99_ns: u64,
    latency_p99_9_ns: u64,
    latency_max_ns: u64,
) !void {
    const file = if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.createFileAbsolute(io, path, .{})
    else
        try std.Io.Dir.cwd().createFile(io, path, .{});
    defer file.close(io);

    var write_buf: [4096]u8 = undefined;
    var file_w = file.writer(io, &write_buf);
    const writer = &file_w.interface;
    try writer.print(
        \\{{
        \\  "service_name": "{s}",
        \\  "target_service": "{s}",
        \\  "sent": {d},
        \\  "failed": {d},
        \\  "elapsed_ms": {d},
        \\  "elapsed_us": {d},
        \\  "message_size": {d},
        \\  "warmup_count": {d},
        \\  "send_interval_ns": {d},
        \\  "throughput_msgs_per_sec": {d},
        \\  "throughput_bytes_per_sec": {d},
        \\  "start_time_ms": {d},
        \\  "end_time_ms": {d},
        \\  "send_latency_p50_ns": {d},
        \\  "send_latency_p95_ns": {d},
        \\  "send_latency_p99_ns": {d},
        \\  "send_latency_p99_9_ns": {d},
        \\  "send_latency_max_ns": {d}
        \\}}
        \\
    , .{
        service_name,
        target_service,
        sent,
        failed,
        elapsed_ms,
        elapsed_us,
        message_size,
        warmup_count,
        send_interval_ns,
        throughput_msgs_per_sec,
        throughput_bytes_per_sec,
        start_time_ms,
        end_time_ms,
        latency_p50_ns,
        latency_p95_ns,
        latency_p99_ns,
        latency_p99_9_ns,
        latency_max_ns,
    });
    try writer.flush();
}

fn paceNextSend(next_send_deadline_ns: *u64, interval_ns: u64) void {
    if (interval_ns == 0) return;

    const now_ns: u64 = @intCast(Clock.monotonicNanos());
    if (next_send_deadline_ns.* == 0 or next_send_deadline_ns.* < now_ns) {
        next_send_deadline_ns.* = now_ns;
    }

    waitUntil(next_send_deadline_ns.*);
    next_send_deadline_ns.* += interval_ns;
}

fn waitUntil(target_ns: u64) void {
    while (true) {
        const now_ns: u64 = @intCast(Clock.monotonicNanos());
        if (now_ns >= target_ns) return;

        const remaining_ns = target_ns - now_ns;
        if (remaining_ns > 10_000) {
            platform.sleepNanos(remaining_ns - 5_000);
        } else if (remaining_ns > 1_000) {
            std.Thread.yield() catch {};
        } else {
            std.atomic.spinLoopHint();
        }
    }
}
