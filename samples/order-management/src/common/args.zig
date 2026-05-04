// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const ringloom_common = @import("ringloom_common");

pub const CommonArgs = struct {
    storage_path: []const u8 = "/dev/shm",
    group: []const u8 = "order-management",
    service_name: []const u8,
    broker_node_id: i16 = 1,
    result_file: ?[]const u8 = null,
    idle_strategy: ringloom_common.platform.IdleStrategy = .{ .backoff = .{} },
};

pub fn parseCommon(args: []const [:0]const u8, default_service_name: []const u8) CommonArgs {
    return .{
        .storage_path = parseString(args, "--storage-path", "/dev/shm"),
        .group = parseString(args, "--group", "order-management"),
        .service_name = parseString(args, "--service-name", default_service_name),
        .broker_node_id = @intCast(parseInt(u64, args, "--broker-node-id", 1)),
        .result_file = parseOptionalString(args, "--result-file"),
        .idle_strategy = parseIdleStrategy(parseString(args, "--idle-strategy", "backoff")),
    };
}

pub fn parseString(args: []const [:0]const u8, flag: []const u8, default: []const u8) []const u8 {
    var i: usize = 0;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], flag) and i + 1 < args.len) {
            return args[i + 1];
        }
    }
    return default;
}

pub fn parseOptionalString(args: []const [:0]const u8, flag: []const u8) ?[]const u8 {
    var i: usize = 0;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], flag) and i + 1 < args.len) {
            return args[i + 1];
        }
    }
    return null;
}

pub fn parseInt(comptime T: type, args: []const [:0]const u8, flag: []const u8, default: T) T {
    var i: usize = 0;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], flag) and i + 1 < args.len) {
            return std.fmt.parseInt(T, args[i + 1], 10) catch default;
        }
    }
    return default;
}

pub fn parseBool(args: []const [:0]const u8, flag: []const u8) bool {
    for (args) |arg| {
        if (std.mem.eql(u8, arg, flag)) return true;
    }
    return false;
}

fn parseIdleStrategy(value: []const u8) ringloom_common.platform.IdleStrategy {
    if (std.mem.eql(u8, value, "busy_spin")) return .busy_spin;
    if (std.mem.eql(u8, value, "yielding")) return .yielding;
    if (std.mem.eql(u8, value, "sleeping")) return .sleeping;
    return .{ .backoff = .{} };
}

test "parseInt returns provided default when absent" {
    const argv = [_][:0]const u8{ "svc", "--orders", "42" };
    try std.testing.expectEqual(@as(u64, 42), parseInt(u64, &argv, "--orders", 1));
    try std.testing.expectEqual(@as(u64, 7), parseInt(u64, &argv, "--missing", 7));
}
