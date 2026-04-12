//! test_forwarder_service — receives messages and forwards them to another service.
//!
//! Used by end-to-end tests to validate message chaining across multiple services.
//! Receives a message on its own ring buffer and immediately forwards the payload
//! to a configurable target service via ServiceClient.send().

const std = @import("std");
const builtin = @import("builtin");
const brz_service = @import("brz_service");
const brz_common = @import("brz_common");

const BrzEngine = brz_service.BrzEngine;
const ServiceConfig = brz_service.ServiceConfig;
const ServiceClient = brz_service.ServiceClient;
const RingBuffer = brz_common.concurrent.ring_buffer.RingBuffer;

// ── Mutable file-level state (acceptable for a test binary) ──────────

var state = State{};
var shutdown_flag: std.atomic.Value(bool) = std.atomic.Value(bool).init(false);

const State = struct {
    received: u64 = 0,
    forwarded: u64 = 0,
    forward_errors: u64 = 0,
    max_messages: u64 = 0,
    client: ?*ServiceClient = null,
};

// ── Message handler (file-level function pointer) ────────────────────

fn onMessage(_: i32, payload: []const u8) void {
    state.received += 1;

    var buf: [512]u8 = undefined;
    var stdout_w = std.fs.File.stdout().writer(&buf);
    const stdout = &stdout_w.interface;
    stdout.print("forwarder: received msg {d}, len={d}\n", .{ state.received, payload.len }) catch {};

    if (state.client) |client| {
        client.send(payload) catch {
            state.forward_errors += 1;
            stdout.print("forwarder: forward error on msg {d}\n", .{state.received}) catch {};
            stdout.flush() catch {};
            return;
        };
        state.forwarded += 1;
    } else {
        state.forward_errors += 1;
        stdout.print("forwarder: no client available, dropping msg {d}\n", .{state.received}) catch {};
    }

    stdout.flush() catch {};

    if (state.max_messages > 0 and state.received >= state.max_messages) {
        shutdown_flag.store(true, .release);
    }
}

// ── Arg parsing helpers ──────────────────────────────────────────────

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

// ── Entry point ──────────────────────────────────────────────────────

pub fn main() !void {
    var debug_alloc: std.heap.DebugAllocator(.{}) = .init;
    const allocator = switch (builtin.mode) {
        .Debug, .ReleaseSafe => debug_alloc.allocator(),
        .ReleaseFast, .ReleaseSmall => std.heap.smp_allocator,
    };
    defer if (builtin.mode == .Debug or builtin.mode == .ReleaseSafe) {
        _ = debug_alloc.deinit();
    };

    var stdout_buf: [4096]u8 = undefined;
    var stdout_w = std.fs.File.stdout().writer(&stdout_buf);
    const stdout = &stdout_w.interface;
    var stderr_buf: [4096]u8 = undefined;
    var stderr_w = std.fs.File.stderr().writer(&stderr_buf);
    const stderr = &stderr_w.interface;

    // ── Parse command-line arguments ─────────────────────────────────

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    const storage_path = parseStringArg(args, "--storage-path", "/dev/shm");
    const group = parseStringArg(args, "--group", "default");
    const service_name = parseStringArg(args, "--service-name", "forwarder");
    const target_service = parseStringArg(args, "--target-service", "echo");
    const broker_node_id: i16 = @intCast(parseIntArg(u64, args, "--broker-node-id", 1));
    const max_messages = parseIntArg(u64, args, "--max-messages", 0);

    state.max_messages = max_messages;

    // ── Start BrzEngine ──────────────────────────────────────────────

    const config = ServiceConfig{
        .storage_path = storage_path,
        .group = group,
        .service_name = service_name,
        .broker_node_id = broker_node_id,
    };

    const engine = BrzEngine.start(allocator, config) catch |err| {
        try stderr.print("forwarder: failed to start engine: {}\n", .{err});
        try stderr.flush();
        std.process.exit(1);
    };

    // ── Create client for the target service ─────────────────────────

    const client = engine.createClient(target_service) catch |err| {
        try stderr.print("forwarder: failed to create client for '{s}': {}\n", .{ target_service, err });
        try stderr.flush();
        engine.stop();
        std.process.exit(1);
    };
    state.client = client;

    // ── Print readiness marker ───────────────────────────────────────

    try stdout.print("service ready: name={s}\n", .{service_name});
    try stdout.print("forwarder: forwarding to target={s}, max_messages={d}\n", .{ target_service, max_messages });
    try stdout.flush();

    // ── Register message handler ─────────────────────────────────────

    engine.setMessageHandler(&onMessage);
    installSignalHandler();

    // ── Main loop: sleep and check for shutdown ──────────────────────

    while (!shutdown_flag.load(.acquire)) {
        std.Thread.sleep(100 * std.time.ns_per_ms);
    }

    // ── Print final stats ────────────────────────────────────────────

    try stdout.print("forwarder: total_received={d}, total_forwarded={d}, forward_errors={d}\n", .{
        state.received,
        state.forwarded,
        state.forward_errors,
    });
    try stdout.flush();

    // ── Graceful shutdown ────────────────────────────────────────────

    engine.stop();
}
