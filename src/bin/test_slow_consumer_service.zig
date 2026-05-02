//! test_slow_consumer_service — intentionally slow consumer for backpressure testing.
//!
//! Receives messages and sleeps for a configurable delay per message,
//! allowing end-to-end tests to validate backpressure behavior when a
//! consumer cannot keep up with the producer rate.

const std = @import("std");
const builtin = @import("builtin");
const brz_service = @import("brz_service");
const brz_common = @import("brz_common");

const BrzEngine = brz_service.BrzEngine;
const ServiceConfig = brz_service.ServiceConfig;
const RingBuffer = brz_common.concurrent.ring_buffer.RingBuffer;

// ── Mutable file-level state (acceptable for a test binary) ──────────

var received_count: u64 = 0;
var shutdown_requested: std.atomic.Value(bool) = std.atomic.Value(bool).init(false);
var max_messages: u64 = 0;
var delay_per_message_ms: u64 = 100;

fn messageHandler(_: i32, payload: []const u8) void {
    received_count += 1;

    var buf: [512]u8 = undefined;
    var stdout_w = std.Io.File.stdout().writer(std.Io.Threaded.global_single_threaded.io(), &buf);
    const stdout = &stdout_w.interface;
    stdout.print("slow-consumer: received msg {d}, len={d}\n", .{ received_count, payload.len }) catch {};
    stdout.flush() catch {};

    // Artificial delay — the whole point of this service.
    if (delay_per_message_ms > 0) {
        brz_common.platform.sleepNanos(delay_per_message_ms * std.time.ns_per_ms);
    }

    if (max_messages > 0 and received_count >= max_messages) {
        shutdown_requested.store(true, .release);
    }
}

// ── Argument parsing helpers ─────────────────────────────────────────

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
    shutdown_requested.store(true, .release);
}

// ── Entry point ──────────────────────────────────────────────────────

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

    const args = try init.minimal.args.toSlice(init.arena.allocator());

    const storage_path = parseStringArg(args, "--storage-path", "/dev/shm");
    const group = parseStringArg(args, "--group", "default");
    const service_name = parseStringArg(args, "--service-name", "slow-consumer");
    const broker_node_id: i16 = @intCast(parseU64Arg(args, "--broker-node-id", 1));
    delay_per_message_ms = parseU64Arg(args, "--delay-per-message-ms", 100);
    max_messages = parseU64Arg(args, "--max-messages", 0);

    var stdout_buf: [4096]u8 = undefined;
    var stdout_w = std.Io.File.stdout().writer(io, &stdout_buf);
    const stdout = &stdout_w.interface;
    var stderr_buf: [4096]u8 = undefined;
    var stderr_w = std.Io.File.stderr().writer(io, &stderr_buf);
    const stderr = &stderr_w.interface;

    try stdout.print("slow-consumer: starting with delay_per_message_ms={d}, max_messages={d}\n", .{
        delay_per_message_ms,
        max_messages,
    });
    try stdout.flush();

    // ── Start engine ─────────────────────────────────────────────────

    const engine = BrzEngine.start(allocator, ServiceConfig{
        .storage_path = storage_path,
        .group = group,
        .service_name = service_name,
        .broker_node_id = broker_node_id,
    }) catch |err| {
        try stderr.print("slow-consumer: failed to start engine: {}\n", .{err});
        try stderr.flush();
        std.process.exit(1);
    };

    try stdout.print("service ready: name={s}\n", .{service_name});
    try stdout.print("slow-consumer: service_id={d}, node_id={d}\n", .{
        engine.service_id,
        engine.node_id,
    });
    try stdout.flush();

    // ── Install handler ──────────────────────────────────────────────

    engine.setMessageHandler(&messageHandler);
    installSignalHandler();

    // ── Main loop — sleep and check for shutdown ─────────────────────

    while (!shutdown_requested.load(.acquire)) {
        brz_common.platform.sleepNanos(100 * std.time.ns_per_ms);
    }

    // ── Shutdown ─────────────────────────────────────────────────────

    try stdout.print("slow-consumer: total_received={d}\n", .{received_count});
    try stdout.flush();

    engine.stop();

    try stdout.print("slow-consumer: stopped\n", .{});
    try stdout.flush();
}
