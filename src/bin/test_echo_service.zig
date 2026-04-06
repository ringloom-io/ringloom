//! test_echo_service — receives messages and optionally replies with the same payload.
//!
//! Used by end-to-end tests to validate message delivery. Registers with the
//! broker, sets a message handler that counts and logs received messages, and
//! exits after reaching an optional max-messages limit.

const std = @import("std");
const brz_service = @import("brz_service");
const brz_common = @import("brz_common");

const BrzEngine = brz_service.BrzEngine;
const ServiceConfig = brz_service.ServiceConfig;
const RingBuffer = brz_common.concurrent.ring_buffer.RingBuffer;

// ── I/O helpers (Zig 0.15 compatible) ─────────────────────────────────

const FdWriter = std.io.GenericWriter(std.posix.fd_t, std.posix.WriteError, std.posix.write);

fn fdWriter(fd: std.posix.fd_t) FdWriter {
    return .{ .context = fd };
}

// ── Mutable file-level state (fine for a single-threaded test binary) ─

var received_count: u64 = 0;
var max_messages: u64 = 0;
var shutdown_flag: bool = false;
var reply_delay_ms: u64 = 0;

fn messageHandler(_: i32, payload: []const u8) void {
    received_count += 1;

    const stdout = fdWriter(std.posix.STDOUT_FILENO);
    stdout.print("echo: received msg {d}, len={d}\n", .{ received_count, payload.len }) catch {};

    if (reply_delay_ms > 0) {
        std.Thread.sleep(reply_delay_ms * std.time.ns_per_ms);
    }

    if (max_messages > 0 and received_count >= max_messages) {
        shutdown_flag = true;
    }
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    const storage_path = parseStringArg(args, "--storage-path", "/dev/shm");
    const group = parseStringArg(args, "--group", "default");
    const service_name = parseStringArg(args, "--service-name", "echo");
    max_messages = parseU64Arg(args, "--max-messages", 0);
    reply_delay_ms = parseU64Arg(args, "--reply-delay-ms", 0);

    const stdout = fdWriter(std.posix.STDOUT_FILENO);
    const stderr = fdWriter(std.posix.STDERR_FILENO);

    try stderr.print("echo: starting with storage_path={s}, group={s}, name={s}\n", .{
        storage_path, group, service_name,
    });

    const engine = BrzEngine.start(allocator, ServiceConfig{
        .storage_path = storage_path,
        .group = group,
        .service_name = service_name,
    }) catch |err| {
        try stderr.print("echo: failed to start engine: {}\n", .{err});
        std.process.exit(1);
    };

    engine.setMessageHandler(&messageHandler);

    try stdout.print("service ready: name={s}\n", .{service_name});

    // Main loop: sleep 100ms, check shutdown flag.
    while (!shutdown_flag) {
        std.Thread.sleep(100 * std.time.ns_per_ms);
    }

    try stdout.print("echo: total_received={d}\n", .{received_count});

    engine.stop();

    try stderr.print("echo: shutdown complete\n", .{});
}

// ── Argument parsing helpers ──────────────────────────────────────────

fn parseStringArg(args: []const []const u8, flag: []const u8, default: []const u8) []const u8 {
    var i: usize = 0;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], flag) and i + 1 < args.len) {
            return args[i + 1];
        }
    }
    return default;
}

fn parseU64Arg(args: []const []const u8, flag: []const u8, default: u64) u64 {
    var i: usize = 0;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], flag) and i + 1 < args.len) {
            return std.fmt.parseInt(u64, args[i + 1], 10) catch default;
        }
    }
    return default;
}
