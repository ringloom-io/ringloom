// SPDX-License-Identifier: Apache-2.0
//! test_topic_publisher_service — topic publisher for e2e tests.
//! Accepts standard harness flags plus positional: <topic_name> <count> [ack_mode]

const std = @import("std");
const builtin = @import("builtin");
const ringloom_service = @import("ringloom_service");
const ringloom_common = @import("ringloom_common");

fn parseStr(args: []const [:0]const u8, name: []const u8, default: []const u8) []const u8 {
    for (args, 0..) |a, i| {
        if (std.mem.eql(u8, a, name) and i + 1 < args.len) return args[i + 1];
    }
    return default;
}
fn parseU64(args: []const [:0]const u8, name: []const u8, default: u64) u64 {
    const v = parseStr(args, name, "");
    if (v.len == 0) return default;
    return std.fmt.parseUnsigned(u64, v, 10) catch default;
}

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    const alloc = switch (builtin.mode) {
        .Debug, .ReleaseSafe => gpa.allocator(),
        else => std.heap.smp_allocator,
    };
    defer if (builtin.mode == .Debug or builtin.mode == .ReleaseSafe) {
        _ = gpa.deinit();
    };

    const args = try init.minimal.args.toSlice(init.arena.allocator());
    const sp = parseStr(args, "--storage-path", "/dev/shm");
    const gr = parseStr(args, "--group", "default");
    const sn = parseStr(args, "--service-name", "pub");
    const bn: i16 = @intCast(parseU64(args, "--broker-node-id", 1));

    var eng = try ringloom_service.RingLoomEngine.start(alloc, ringloom_service.ServiceConfig{ .service_name = sn, .storage_path = sp, .group = gr, .broker_node_id = bn, .heartbeat_timeout_ms = 5000 });
    defer eng.stop();

    var pos: [16][]const u8 = undefined;
    var pc: usize = 0;
    var ai: usize = 0;
    while (ai < args.len) : (ai += 1) {
        if (std.mem.startsWith(u8, args[ai], "--")) {
            ai += 1;
            continue;
        }
        if (pc < pos.len) {
            pos[pc] = args[ai];
            pc += 1;
        }
    }

    const tn: []const u8 = if (pc > 0) pos[0] else "bench";
    const mc: u64 = if (pc > 1) std.fmt.parseInt(u64, pos[1], 10) catch 10 else 10;
    const av: u8 = if (pc > 2) std.fmt.parseInt(u8, pos[2], 10) catch 0 else 0;
    const am: ringloom_common.topics.AckMode = if (av == 1) .replicate_once else .fire_and_forget;

    var pub_ = ringloom_service.topics.TopicPublisher{ .topic_id = std.hash.Wyhash.hash(0, tn), .leader_node_id = 1, .leader_epoch = 1 };
    defer pub_.deinit();

    try std.Io.File.stdout().writeStreamingAll(io, "service ready\n");

    const payload = try alloc.alloc(u8, 32);
    defer alloc.free(payload);
    @memset(payload, 'X');

    var n: u64 = 0;
    while (n < mc) : (n += 1) {
        _ = pub_.publish(payload, @intCast(n), am);
    }

    if (am == .replicate_once) {
        var p: u64 = 0;
        while (p < mc) {
            if (pub_.isAcked(p)) {
                p += 1;
            } else {
                _ = std.Io.sleep(io, .fromMilliseconds(1), .awake) catch {};
            }
        }
    }
}
