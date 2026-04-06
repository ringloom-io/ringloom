//! test_crashy_service — registers with the broker, then crashes abruptly.
//!
//! Used by end-to-end tests to verify that the broker correctly detects and
//! cleans up after a service that disappears without sending an unregister
//! message (simulating a process crash, OOM kill, etc.).
//!
//! CLI flags:
//!   --storage-path <path>          (default "/dev/shm")
//!   --group <name>                 (default "default")
//!   --service-name <name>          (default "crashy")
//!   --crash-after-ms <ms>          (default 2000; 0 = use message-based)
//!   --crash-after-messages <count> (default 0; 0 = use time-based)

const std = @import("std");
const brz_service = @import("brz_service");
const brz_common = @import("brz_common");

const BrzEngine = brz_service.BrzEngine;
const ServiceConfig = brz_service.ServiceConfig;
const RingBuffer = brz_common.concurrent.ring_buffer.RingBuffer;

// ── FdWriter — Zig 0.15 compatible stdout/stderr ─────────────────────

const FdWriter = std.io.GenericWriter(std.posix.fd_t, std.posix.WriteError, std.posix.write);

fn fdWriter(fd: std.posix.fd_t) FdWriter {
    return .{ .context = fd };
}

// ── Mutable file-level state for the function-pointer message handler ─

var received_count: u64 = 0;
var crash_after_messages: u64 = 0;

fn messageHandler(_: i32, payload: []const u8) void {
    received_count += 1;

    const stdout = fdWriter(std.posix.STDOUT_FILENO);
    stdout.print("crashy: received msg {d}, len={d}\n", .{ received_count, payload.len }) catch {};

    if (crash_after_messages > 0 and received_count >= crash_after_messages) {
        stdout.print("crashy: crashing after {d} messages (no cleanup)\n", .{received_count}) catch {};
        // Abrupt exit — deliberately skip engine.stop().
        std.process.exit(1);
    }
}

// ── Arg parsing helpers ───────────────────────────────────────────────

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

// ── Entry point ───────────────────────────────────────────────────────

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    const storage_path = parseStringArg(args, "--storage-path", "/dev/shm");
    const group = parseStringArg(args, "--group", "default");
    const service_name = parseStringArg(args, "--service-name", "crashy");
    const crash_after_ms: u64 = parseIntArg(u64, args, "--crash-after-ms", 2000);
    crash_after_messages = parseIntArg(u64, args, "--crash-after-messages", 0);

    const stdout = fdWriter(std.posix.STDOUT_FILENO);
    const stderr = fdWriter(std.posix.STDERR_FILENO);

    try stdout.print("crashy: starting service name={s} group={s}\n", .{ service_name, group });

    const config = ServiceConfig{
        .storage_path = storage_path,
        .group = group,
        .service_name = service_name,
    };

    const engine = BrzEngine.start(allocator, config) catch |err| {
        try stderr.print("crashy: failed to start engine: {}\n", .{err});
        std.process.exit(2);
    };
    // NOTE: we deliberately do NOT defer engine.stop() — the whole point of
    // this service is to exit without graceful cleanup.

    engine.setMessageHandler(&messageHandler);

    try stdout.print("service ready: name={s}\n", .{service_name});
    try stdout.print("crashy: service_id={d}, node_id={d}\n", .{ engine.service_id, engine.node_id });

    if (crash_after_messages > 0) {
        // Message-count-based crash: sit in a polling loop until the handler
        // calls std.process.exit(1).
        try stdout.print("crashy: will crash after {d} messages\n", .{crash_after_messages});

        while (true) {
            std.Thread.sleep(100 * std.time.ns_per_ms);
        }
    } else {
        // Time-based crash (the default path).
        try stdout.print("crashy: will crash after {d} ms\n", .{crash_after_ms});

        if (crash_after_ms > 0) {
            std.Thread.sleep(crash_after_ms * std.time.ns_per_ms);
        }

        try stdout.print("crashy: crashing now (no cleanup)\n", .{});
        std.process.exit(1);
    }
}
