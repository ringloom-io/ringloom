//! test_forwarder_service — receives messages and forwards them to another service.
//!
//! Used by end-to-end tests to validate message chaining across multiple services.
//! Receives a message on its own ring buffer and immediately forwards the payload
//! to a configurable target service via ServiceClient.send().

const std = @import("std");
const brz_service = @import("brz_service");
const brz_common = @import("brz_common");

const BrzEngine = brz_service.BrzEngine;
const ServiceConfig = brz_service.ServiceConfig;
const ServiceClient = brz_service.ServiceClient;
const RingBuffer = brz_common.concurrent.ring_buffer.RingBuffer;

// ── I/O helpers (Zig 0.15 compatible) ────────────────────────────────

const FdWriter = std.io.GenericWriter(std.posix.fd_t, std.posix.WriteError, std.posix.write);

fn fdWriter(fd: std.posix.fd_t) FdWriter {
    return .{ .context = fd };
}

// ── Mutable file-level state (acceptable for a test binary) ──────────

var state = State{};

const State = struct {
    received: u64 = 0,
    forwarded: u64 = 0,
    forward_errors: u64 = 0,
    max_messages: u64 = 0,
    shutdown: bool = false,
    client: ?*ServiceClient = null,
};

// ── Message handler (file-level function pointer) ────────────────────

fn onMessage(_: i32, payload: []const u8) void {
    state.received += 1;

    const stdout = fdWriter(std.posix.STDOUT_FILENO);
    stdout.print("forwarder: received msg {d}, len={d}\n", .{ state.received, payload.len }) catch {};

    if (state.client) |client| {
        client.send(payload) catch {
            state.forward_errors += 1;
            stdout.print("forwarder: forward error on msg {d}\n", .{state.received}) catch {};
            return;
        };
        state.forwarded += 1;
    } else {
        state.forward_errors += 1;
        stdout.print("forwarder: no client available, dropping msg {d}\n", .{state.received}) catch {};
    }

    if (state.max_messages > 0 and state.received >= state.max_messages) {
        state.shutdown = true;
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

// ── Entry point ──────────────────────────────────────────────────────

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const stdout = fdWriter(std.posix.STDOUT_FILENO);
    const stderr = fdWriter(std.posix.STDERR_FILENO);

    // ── Parse command-line arguments ─────────────────────────────────

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    const storage_path = parseStringArg(args, "--storage-path", "/dev/shm");
    const group = parseStringArg(args, "--group", "default");
    const service_name = parseStringArg(args, "--service-name", "forwarder");
    const target_service = parseStringArg(args, "--target-service", "echo");
    const max_messages = parseIntArg(u64, args, "--max-messages", 0);

    state.max_messages = max_messages;

    // ── Start BrzEngine ──────────────────────────────────────────────

    const config = ServiceConfig{
        .storage_path = storage_path,
        .group = group,
        .service_name = service_name,
    };

    const engine = BrzEngine.start(allocator, config) catch |err| {
        try stderr.print("forwarder: failed to start engine: {}\n", .{err});
        std.process.exit(1);
    };

    // ── Create client for the target service ─────────────────────────

    const client = engine.createClient(target_service) catch |err| {
        try stderr.print("forwarder: failed to create client for '{s}': {}\n", .{ target_service, err });
        engine.stop();
        std.process.exit(1);
    };
    state.client = client;

    // ── Print readiness marker ───────────────────────────────────────

    try stdout.print("service ready: name={s}\n", .{service_name});
    try stdout.print("forwarder: forwarding to target={s}, max_messages={d}\n", .{ target_service, max_messages });

    // ── Register message handler ─────────────────────────────────────

    engine.setMessageHandler(&onMessage);

    // ── Main loop: sleep and check for shutdown ──────────────────────

    while (!state.shutdown) {
        std.Thread.sleep(100 * std.time.ns_per_ms);
    }

    // ── Print final stats ────────────────────────────────────────────

    try stdout.print("forwarder: total_received={d}, total_forwarded={d}, forward_errors={d}\n", .{
        state.received,
        state.forwarded,
        state.forward_errors,
    });

    // ── Graceful shutdown ────────────────────────────────────────────

    engine.stop();
}
