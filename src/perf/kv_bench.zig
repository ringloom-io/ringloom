// SPDX-License-Identifier: Apache-2.0
//! KV tests use real broker/service processes. Loopback nodes are not physical hosts.
const std = @import("std");
const testing = @import("ringloom_testing");
const TestHarness = testing.TestHarness;

// Keep KV-only broker configuration here rather than changing shared harness defaults.
pub fn startKvBroker(h: *TestHarness, node: u8, peers: []const testing.PeerSpec) !*testing.ProcessHandle {
    const a = h.allocator;
    const aeron = try h.env.aeronDirectoryPath(node);
    defer a.free(aeron);
    var mapped: std.ArrayList(testing.PeerSpec) = .empty;
    defer mapped.deinit(a);
    for (peers) |peer| try mapped.append(a, .{ .node_id = peer.node_id, .host = peer.host, .port = h.env.mapUdpPort(peer.port) });
    const path = try h.config_gen_inst.writeBrokerConfig(h.env.config_path, .{
        .node_id = node,
        .port = h.env.mapUdpPort(19000 + @as(u16, node)),
        .peers = mapped.items,
        .topics_enabled = true,
        .aeron_directory = aeron,
    }, h.env.storage_path);
    defer a.free(path);
    const old = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, path, a, .limited(1024 * 1024));
    defer a.free(old);
    const config = try a.print("{s}\nbroker.kv.enabled=true\nbroker.kv.generation.grace.ms=100\nbroker.topics.path={s}/topics-node-{d}\n", .{ old, h.env.storage_path, node });
    defer a.free(config);
    try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = path, .data = config });
    const process = try a.create(testing.ProcessHandle);
    errdefer a.destroy(process);
    var name_buf: [32]u8 = undefined;
    process.* = try testing.ProcessHandle.spawn(a, try std.fmt.bufPrint(&name_buf, "kv-broker-{d}", .{node}), "zig-out/bin/ringloom-broker", &.{ "--config", path }, h.env.logs_path);
    try process.setConfigPath(path);
    try process.setAeronDirectory(aeron);
    try h.processes.append(a, process);
    try h.waitForBrokerReady(process, 10000);
    return process;
}
test "kv bounded local serial apply get and iteration" {
    var h = try TestHarness.init(std.testing.allocator, "perf-kv-local");
    defer h.cleanup();
    errdefer h.markFailed();
    _ = try startKvBroker(&h, 1, &.{});
    const path = try @import("persistent_results.zig").scenarioPath(std.testing.allocator, "kv", "kv-local-serial.json");
    defer std.testing.allocator.free(path);
    const process = try h.startService(.{ .executable_name = "ringloom-test-kv-service", .service_name = "kv-perf", .extra_args = &.{ "--mode", "bench", "--count", "1000", "--result-file", path } });
    try std.testing.expectEqual(@as(u32, 0), try process.waitForExit(60000));
}

// Measure LMDB's steady-state allocations independently from the broker
// process. These are engine transactions, not a measurement of the applier.
test "kv engine steady state transaction allocation measurement" {
    const engine = @import("ringloom_common").kv.engine;
    var h = try TestHarness.init(std.testing.allocator, "perf-kv-engine-alloc");
    defer h.cleanup();
    errdefer h.markFailed();
    const dir = try std.testing.allocator.print("{s}/engine", .{h.env.storage_path});
    defer std.testing.allocator.free(dir);
    var env = try engine.WriteEnv.create(dir, 16 << 20, 16);
    defer env.close();
    // Warm LMDB's reusable write transaction and page state.
    var warm = try env.begin();
    try warm.put("key", .{ .write_index = 1, .expire_at_ns = 0 }, "value");
    try warm.commit();
    const before = engine.alloc_stats.allocs();
    var i: u64 = 0;
    while (i < 1000) : (i += 1) {
        var txn = try env.begin();
        try txn.put("key", .{ .write_index = i + 2, .expire_at_ns = 0 }, "value");
        try txn.commit();
    }
    const allocs = engine.alloc_stats.allocs() - before;
    const path = try @import("persistent_results.zig").scenarioPath(std.testing.allocator, "kv", "kv-engine-alloc.json");
    defer std.testing.allocator.free(path);
    var buf: [512]u8 = undefined;
    const json = try std.fmt.bufPrint(&buf, "{{\"suite\":\"kv\",\"scenario\":\"kv-engine-alloc\",\"transactions\":1000,\"engine_allocs\":{d},\"engine_allocs_per_txn\":{d}}}\n", .{ allocs, @as(f64, @floatFromInt(allocs)) / 1000 });
    try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = path, .data = json });
}
