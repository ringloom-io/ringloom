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

const BrzEngine = brz_service.BrzEngine;
const ServiceConfig = brz_service.ServiceConfig;
const ServiceClient = brz_service.ServiceClient;
const Clock = brz_common.Clock;

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
    const message_count = parseIntArg(u64, args, "--message-count", 100);
    const message_size = parseIntArg(usize, args, "--message-size", 64);
    const warmup_count = parseIntArg(u64, args, "--warmup-count", 10);
    const result_file = parseOptionalStringArg(args, "--result-file");

    // ── Start engine ─────────────────────────────────────────────────

    const config = ServiceConfig{
        .storage_path = storage_path,
        .group = group,
        .service_name = service_name,
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

    const start_time_ms = Clock.epochMillis();
    const start_mono = std.time.nanoTimestamp();

    for (0..message_count) |_| {
        client.send(payload) catch {
            failed += 1;
            continue;
        };
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

    // ── Print summary ────────────────────────────────────────────────

    try stdout.print("ping: sent={d}, failed={d}, elapsed_ms={d}\n", .{ sent, failed, elapsed_ms });
    try stdout.print("ping: throughput={d} msgs/s, {d} bytes/s\n", .{ throughput_msgs_per_sec, throughput_bytes_per_sec });
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
        \\  "end_time_ms": {d}
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
    });
    try writer.flush();
}
