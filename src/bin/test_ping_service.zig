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
const brz_service = @import("brz_service");
const brz_common = @import("brz_common");
const brz_testing = @import("brz_testing");

const BrzEngine = brz_service.BrzEngine;
const ServiceConfig = brz_service.ServiceConfig;
const ServiceClient = brz_service.ServiceClient;
const Clock = brz_common.Clock;
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

fn signalHandler(_: c_int) callconv(.c) void {
    shutdown_flag.store(true, .release);
}

// ── Arg Parsing Helpers ──────────────────────────────────────────────

fn parseStringArg(args: []const []const u8, flag: []const u8, default: []const u8) []const u8 {
    var i: usize = 0;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], flag) and i + 1 < args.len) {
            return args[i + 1];
        }
    }
    return default;
}

fn parseIntArg(comptime T: type, args: []const []const u8, flag: []const u8, default: T) T {
    var i: usize = 0;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], flag) and i + 1 < args.len) {
            return std.fmt.parseInt(T, args[i + 1], 10) catch default;
        }
    }
    return default;
}

fn parseOptionalStringArg(args: []const []const u8, flag: []const u8) ?[]const u8 {
    var i: usize = 0;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], flag) and i + 1 < args.len) {
            return args[i + 1];
        }
    }
    return null;
}

// ── Main ─────────────────────────────────────────────────────────────

pub fn main() !void {
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var stdout_buf: [4096]u8 = undefined;
    var stdout_w = std.fs.File.stdout().writer(&stdout_buf);
    const stdout = &stdout_w.interface;
    var stderr_buf: [4096]u8 = undefined;
    var stderr_w = std.fs.File.stderr().writer(&stderr_buf);
    const stderr = &stderr_w.interface;

    // ── Parse CLI args ───────────────────────────────────────────────

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

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

    installSignalHandler();

    // ── Start engine ─────────────────────────────────────────────────

    const config = ServiceConfig{
        .storage_path = storage_path,
        .group = group,
        .service_name = service_name,
        .broker_node_id = broker_node_id,
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

    std.Thread.sleep(500 * std.time.ns_per_ms);

    // ── Prepare payload ──────────────────────────────────────────────

    const payload = try allocator.alloc(u8, message_size);
    defer allocator.free(payload);
    @memset(payload, 0xAB);

    // ── Warmup phase ─────────────────────────────────────────────────

    var warmup_sent: u64 = 0;
    var warmup_failed: u64 = 0;

    for (0..warmup_count) |_| {
        // Embed monotonic timestamp + warmup phase flag.
        if (payload.len >= 9) {
            std.mem.writeInt(u64, payload[0..8], @intCast(Clock.monotonicNanos()), .little);
            payload[8] = 0; // warmup phase
        }
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
        std.Thread.sleep(50 * std.time.ns_per_ms);
    }

    // ── Measurement phase ────────────────────────────────────────────

    var sent: u64 = 0;
    var failed: u64 = 0;

    // Per-message send latency histogram.
    var histogram = Histogram.initCapacity(allocator, message_count) catch blk: {
        break :blk Histogram.init(allocator);
    };
    defer histogram.deinit();

    const start_time_ms = Clock.epochMillis();
    const start_mono = std.time.nanoTimestamp();

    for (0..message_count) |_| {
        // Embed monotonic timestamp + measured phase flag.
        if (payload.len >= 9) {
            std.mem.writeInt(u64, payload[0..8], @intCast(Clock.monotonicNanos()), .little);
            payload[8] = 1; // measured phase
        }

        const send_start = std.time.nanoTimestamp();
        var succeeded = false;

        client.send(payload) catch {
            // Spinning backpressure: retry for up to spin_timeout_ms.
            if (spin_timeout_ms > 0) {
                const deadline_ns: u64 = spin_timeout_ms * std.time.ns_per_ms;
                const spin_start = std.time.Instant.now() catch {
                    failed += 1;
                    continue;
                };

                while (true) {
                    // Re-embed timestamp before each retry so echo measures
                    // pipeline latency, not including spin wait.
                    if (payload.len >= 9) {
                        std.mem.writeInt(u64, payload[0..8], @intCast(Clock.monotonicNanos()), .little);
                    }

                    client.send(payload) catch {
                        const elapsed = (std.time.Instant.now() catch break).since(spin_start);
                        if (elapsed >= deadline_ns) break;
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
            const send_end = std.time.nanoTimestamp();
            const delta: u64 = @intCast(send_end - send_start);
            histogram.record(delta) catch {};
            sent += 1;
            continue;
        };

        // First attempt succeeded.
        const send_end = std.time.nanoTimestamp();
        const delta: u64 = @intCast(send_end - send_start);
        histogram.record(delta) catch {};
        sent += 1;
    }

    const end_mono = std.time.nanoTimestamp();
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
    try stdout.print("ping: message_size={d}, target={s}\n", .{ message_size, target_service });
    try stdout.flush();

    // ── Write JSON results file (optional) ───────────────────────────

    if (result_file) |path| {
        writeResultsJson(
            path,
            sent,
            failed,
            elapsed_ms,
            elapsed_us,
            message_size,
            throughput_msgs_per_sec,
            throughput_bytes_per_sec,
            warmup_count,
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
    path: []const u8,
    sent: u64,
    failed: u64,
    elapsed_ms: u64,
    elapsed_us: u64,
    message_size: usize,
    throughput_msgs_per_sec: u64,
    throughput_bytes_per_sec: u64,
    warmup_count: u64,
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
    const file = try std.fs.cwd().createFile(path, .{});
    defer file.close();

    var write_buf: [4096]u8 = undefined;
    var file_w = file.writer(&write_buf);
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
