// SPDX-License-Identifier: Apache-2.0
//! Local broker KV operator. Success without --wait means requested only.
const std = @import("std");
const common = @import("ringloom_common");
const operator = @import("kv_operator_messages");
const Args = struct {
    storage: []const u8 = common.memory.constants.default_storage_path,
    group: []const u8 = "default",
    node: u8 = 1,
    kv_path: ?[]const u8 = null,
    store: []const u8 = "",
    wait: bool = false,
    timeout_ms: u32 = 30000,
};
fn parse(argv: []const []const u8) !Args {
    var args: Args = .{};
    var positional: usize = 0;
    var i: usize = 1;
    while (i < argv.len) : (i += 1) {
        const arg = argv[i];
        if (std.mem.eql(u8, arg, "--wait")) {
            args.wait = true;
            continue;
        }
        if (std.mem.startsWith(u8, arg, "--")) {
            i += 1;
            if (i == argv.len) return error.MissingOptionValue;
            const value = argv[i];
            if (std.mem.eql(u8, arg, "--storage-path")) args.storage = value else if (std.mem.eql(u8, arg, "--group")) args.group = value else if (std.mem.eql(u8, arg, "--kv-path")) args.kv_path = value else if (std.mem.eql(u8, arg, "--broker-node-id") or std.mem.eql(u8, arg, "--node-id")) args.node = std.fmt.parseUnsigned(u8, value, 10) catch return error.InvalidNode else if (std.mem.eql(u8, arg, "--timeout-ms")) args.timeout_ms = std.fmt.parseUnsigned(u32, value, 10) catch return error.InvalidTimeout else return error.UnknownOption;
            continue;
        }
        if (positional == 0) {
            if (!std.mem.eql(u8, arg, "rebuild")) return error.UnknownCommand;
        } else if (positional == 1) args.store = arg else return error.ExtraArgument;
        positional += 1;
    }
    if (positional != 2 or !common.kv.format.isValidStoreName(args.store)) return error.InvalidStoreName;
    if (args.group.len == 0 or std.mem.indexOfAny(u8, args.group, "/\\") != null or std.mem.eql(u8, args.group, ".") or std.mem.eql(u8, args.group, "..")) return error.InvalidGroup;
    if (args.node == 0) return error.InvalidNode;
    if (args.timeout_ms == 0) return error.InvalidTimeout;
    return args;
}
pub fn main(init: std.process.Init) !void {
    var buffer: [1024]u8 = undefined;
    var output = std.Io.File.stdout().writer(init.io, &buffer);
    defer output.interface.flush() catch {};
    const argv = try init.minimal.args.toSlice(init.arena.allocator());
    if (argv.len == 2 and (std.mem.eql(u8, argv[1], "--help") or std.mem.eql(u8, argv[1], "-h"))) {
        try output.interface.writeAll("Usage: ringloom-kv rebuild STORE [--storage-path PATH] [--group GROUP] [--broker-node-id N] [--kv-path PATH] [--wait] [--timeout-ms N]\nTargets this local broker only. Without --wait, success means requested.\n");
        return;
    }
    const args = try parse(argv);
    var broker = try common.memory.BrokerMetadataFile.open(args.storage, args.group, args.node);
    defer broker.close();
    if (!common.platform.isProcessAlive(@intCast(broker.header.pid)) or common.platform.Clock.epochMillis() - broker.loadHeartbeat() > 10000) return error.BrokerUnavailable;
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const root = args.kv_path orelse try std.fmt.bufPrint(&path_buf, "{s}/kv", .{args.storage});
    var control_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&control_buf, "{s}/{s}/node-{d}/{s}/control.shm", .{ root, args.group, args.node, args.store });
    var control = try common.kv.ControlFile.openReadOnly(path);
    defer control.close();
    const id = try common.kv.format.storeIdOf(args.store);
    if (control.storeId() != id) return error.StoreIdMismatch;
    const before = control.loadGeneration();
    var ring = try common.concurrent.ring_buffer.RingBuffer.init(@alignCast(broker.getControlBuffer()), false, null, null);
    var request: [operator.rebuild_size]u8 = undefined;
    operator.encodeRebuild(&request, id);
    try ring.write(1, &request);
    if (!args.wait) {
        try output.interface.print("Rebuild requested for {s} on local broker {d}; completion not observed.\n", .{ args.store, args.node });
        return;
    }
    const deadline = common.platform.Clock.monotonicNanos() + @as(i64, args.timeout_ms) * std.time.ns_per_ms;
    while (true) {
        if (control.getState() == .failed) return error.RebuildFailed;
        const generation = control.loadGeneration();
        if (generation > before) {
            try output.interface.print("Rebuild observed for {s}: generation {d} -> {d}.\n", .{ args.store, before, generation });
            return;
        }
        if (common.platform.Clock.monotonicNanos() >= deadline) return error.RebuildTimeout;
        common.platform.sleepNanos(10 * std.time.ns_per_ms);
    }
}
test "operator command validation" {
    try std.testing.expectError(error.InvalidStoreName, parse(&.{ "kv", "rebuild", "../x" }));
    try std.testing.expectError(error.UnknownCommand, parse(&.{ "kv", "delete", "x" }));
    try std.testing.expectError(error.ExtraArgument, parse(&.{ "kv", "rebuild", "x", "y" }));
    try std.testing.expectError(error.InvalidTimeout, parse(&.{ "kv", "rebuild", "x", "--timeout-ms", "0" }));
    const a = try parse(&.{ "kv", "rebuild", "prices", "--wait" });
    try std.testing.expect(a.wait);
}
