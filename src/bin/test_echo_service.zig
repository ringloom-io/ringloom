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

// ── Mutable file-level state (fine for a single-threaded test binary) ─

var received_count: u64 = 0;
var max_messages: u64 = 0;
var shutdown_flag: std.atomic.Value(bool) = std.atomic.Value(bool).init(false);
var reply_delay_ms: u64 = 0;
var quiet_mode: bool = false;

fn messageHandler(_: i32, payload: []const u8) void {
    received_count += 1;

    if (!quiet_mode) {
        var buf: [512]u8 = undefined;
        var stdout_w = std.fs.File.stdout().writer(&buf);
        const stdout = &stdout_w.interface;
        stdout.print("echo: received msg {d}, len={d}\n", .{ received_count, payload.len }) catch {};
        stdout.flush() catch {};
    }

    if (reply_delay_ms > 0) {
        std.Thread.sleep(reply_delay_ms * std.time.ns_per_ms);
    }

    if (max_messages > 0 and received_count >= max_messages) {
        shutdown_flag.store(true, .release);
    }
}

pub fn main() !void {
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    const storage_path = parseStringArg(args, "--storage-path", "/dev/shm");
    const group = parseStringArg(args, "--group", "default");
    const service_name = parseStringArg(args, "--service-name", "echo");
    const broker_node_id: i16 = @intCast(parseU64Arg(args, "--broker-node-id", 1));
    max_messages = parseU64Arg(args, "--max-messages", 0);
    reply_delay_ms = parseU64Arg(args, "--reply-delay-ms", 0);
    quiet_mode = parseBoolArg(args, "--quiet");

    var stdout_buf: [4096]u8 = undefined;
    var stdout_w = std.fs.File.stdout().writer(&stdout_buf);
    const stdout = &stdout_w.interface;
    var stderr_buf: [4096]u8 = undefined;
    var stderr_w = std.fs.File.stderr().writer(&stderr_buf);
    const stderr = &stderr_w.interface;

    try stderr.print("echo: starting with storage_path={s}, group={s}, name={s}\n", .{
        storage_path, group, service_name,
    });
    try stderr.flush();

    const engine = BrzEngine.start(allocator, ServiceConfig{
        .storage_path = storage_path,
        .group = group,
        .service_name = service_name,
        .broker_node_id = broker_node_id,
    }) catch |err| {
        try stderr.print("echo: failed to start engine: {}\n", .{err});
        try stderr.flush();
        std.process.exit(1);
    };

    engine.setMessageHandler(&messageHandler);

    // Install SIGTERM handler for graceful shutdown.
    installSignalHandler();

    try stdout.print("service ready: name={s}\n", .{service_name});
    try stdout.flush();

    // Main loop: sleep 100ms, check shutdown flag.
    while (!shutdown_flag.load(.acquire)) {
        std.Thread.sleep(100 * std.time.ns_per_ms);
    }

    try stdout.print("echo: total_received={d}\n", .{received_count});
    try stdout.flush();

    engine.stop();

    try stderr.print("echo: shutdown complete\n", .{});
    try stderr.flush();
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

fn signalHandler(_: c_int) callconv(.c) void {
    shutdown_flag.store(true, .release);
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

fn parseBoolArg(args: []const []const u8, flag: []const u8) bool {
    for (args) |arg| {
        if (std.mem.eql(u8, arg, flag)) return true;
    }
    return false;
}
