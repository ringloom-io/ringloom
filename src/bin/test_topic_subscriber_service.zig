// SPDX-License-Identifier: Apache-2.0

//! test_topic_subscriber_service — subscribes to a topic and reads messages.
//!
//! Standard args (injected by harness): --storage-path --group --service-name --broker-node-id
//! Extra args (positional): <topic_name> <expected_count> [start_pos]
//!   start_pos: 0 = earliest (default), 1 = latest

const std = @import("std");
const builtin = @import("builtin");
const ringloom_service = @import("ringloom_service");
const ringloom_common = @import("ringloom_common");
const rq = @import("ringloom_queue");

const RingLoomEngine = ringloom_service.RingLoomEngine;
const ServiceConfig = ringloom_service.ServiceConfig;
const TopicSubscription = ringloom_service.topics.TopicSubscription;
const TopicConfig = ringloom_common.topics.TopicConfig;

fn parseStringArg(args: []const [:0]const u8, name: []const u8, default: []const u8) []const u8 {
    for (args, 0..) |arg, i| {
        if (std.mem.eql(u8, arg, name) and i + 1 < args.len) return args[i + 1];
    }
    return default;
}

fn parseU64Arg(args: []const [:0]const u8, name: []const u8, default: u64) u64 {
    const val = parseStringArg(args, name, "");
    if (val.len == 0) return default;
    return std.fmt.parseUnsigned(u64, val, 10) catch default;
}

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

    // Parse standard harness args.
    const storage_path = parseStringArg(args, "--storage-path", "/dev/shm");
    const group = parseStringArg(args, "--group", "default");
    const service_name = parseStringArg(args, "--service-name", "topic-subscriber");
    const broker_node_id: i16 = @intCast(parseU64Arg(args, "--broker-node-id", 1));

    // Parse topic-specific positional args.
    var positional: [16][]const u8 = undefined;
    var pos_count: usize = 0;
    var ai: usize = 0;
    while (ai < args.len) : (ai += 1) {
        if (std.mem.startsWith(u8, args[ai], "--")) {
            ai += 1;
            continue;
        }
        if (pos_count < positional.len) {
            positional[pos_count] = args[ai];
            pos_count += 1;
        }
    }

    const topic_name: []const u8 = if (pos_count > 0) positional[0] else "test-topic";
    const expected_count: u64 = if (pos_count > 1) std.fmt.parseInt(u64, positional[1], 10) catch 10 else 10;
    const start_pos: u64 = if (pos_count > 2) std.fmt.parseInt(u64, positional[2], 10) catch 0 else 0;

    var stdout_buf: [4096]u8 = undefined;
    var stdout_w = std.Io.File.stdout().writer(io, &stdout_buf);
    const stdout = &stdout_w.interface;
    var stderr_buf: [4096]u8 = undefined;
    var stderr_w = std.Io.File.stderr().writer(io, &stderr_buf);
    const stderr = &stderr_w.interface;

    // Start the RingLoom service engine.
    var engine = try RingLoomEngine.start(allocator, ServiceConfig{
        .service_name = service_name,
        .storage_path = storage_path,
        .group = group,
        .broker_node_id = broker_node_id,
        .blocking_mode = false,
        .heartbeat_timeout_ms = 5000,
    });
    defer engine.stop();

    // Build the topic queue directory path.
    const topic_id = std.hash.Wyhash.hash(0, topic_name);
    const topic_config = TopicConfig{
        .roll_scheme_name = .{ 'F', 'A', 'S', 'T', '_', 'D', 'A', 'I', 'L', 'Y', 0, 0, 0, 0, 0, 0 },
        .retention_cycles = 4,
    };

    var dir_buf: [256]u8 = undefined;
    const queue_dir = std.fmt.bufPrint(&dir_buf, "{s}/topics/t_{x:0>16}", .{ storage_path, topic_id }) catch {
        stderr.print("invalid queue dir\n", .{}) catch {};
        std.process.exit(1);
    };

    // Open subscription (tailer) on the local queue directory.
    const start_index: u64 = if (start_pos == 0) 0 else 0;
    var subscription = TopicSubscription.open(allocator, queue_dir, start_index, topic_config) catch |err| {
        stderr.print("failed to open queue at {s}: {}\n", .{ queue_dir, err }) catch {};
        std.process.exit(1);
    };
    defer subscription.deinit();

    // Signal readiness.
    stdout.print("service ready\n", .{}) catch {};
    stdout.flush() catch {};

    // Poll loop.
    var received: u64 = 0;
    while (received < expected_count) {
        const msg = subscription.poll() catch continue;
        if (msg) |payload| {
            var expected_buf: [64]u8 = undefined;
            const expected = std.fmt.bufPrint(&expected_buf, "msg-{d}", .{received}) catch continue;
            if (!std.mem.eql(u8, payload, expected)) {
                stderr.print("order violation at {d}: expected '{s}', got '{s}'\n", .{ received, expected, payload }) catch {};
                std.process.exit(2);
            }
            received += 1;
        } else {
            _ = std.Io.sleep(io, .fromMilliseconds(100), .awake) catch {};
        }
    }

    stdout.print("received={d}\n", .{received}) catch {};
    stdout.print("done\n", .{}) catch {};
}
