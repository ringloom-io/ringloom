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
fn runClient(h: *TestHarness, node: u8, name: []const u8, mode: []const u8) !void {
    const process = try h.startService(.{ .executable_name = "ringloom-test-kv-service", .service_name = name, .broker_node_id = node, .extra_args = &.{ "--mode", mode } });
    try std.testing.expectEqual(@as(u32, 0), try process.waitForExit(30000));
}
test "kv single node CRUD TTL iteration view and writer contention" {
    var h = try TestHarness.init(std.testing.allocator, "kv-crud");
    defer h.cleanup();
    errdefer h.markFailed();
    _ = try startKvBroker(&h, 1, &.{});
    try runClient(&h, 1, "kv-crud", "crud");
}
test "kv three nodes remote writer leader and reader converge" {
    var h = try TestHarness.init(std.testing.allocator, "kv-remote");
    defer h.cleanup();
    errdefer h.markFailed();
    _ = try startKvBroker(&h, 1, &.{ .{ .node_id = 2, .host = "127.0.0.1", .port = 19002 }, .{ .node_id = 3, .host = "127.0.0.1", .port = 19003 } });
    _ = try startKvBroker(&h, 2, &.{ .{ .node_id = 1, .host = "127.0.0.1", .port = 19001 }, .{ .node_id = 3, .host = "127.0.0.1", .port = 19003 } });
    _ = try startKvBroker(&h, 3, &.{ .{ .node_id = 1, .host = "127.0.0.1", .port = 19001 }, .{ .node_id = 2, .host = "127.0.0.1", .port = 19002 } });
    try std.Io.sleep(std.testing.io, .fromNanoseconds(4 * std.time.ns_per_s), .awake);
    // Registration waits for the topic leader; data crosses UDP from node 2.
    try runClient(&h, 2, "kv-writer", "write");
    try runClient(&h, 3, "kv-reader", "read");
}

test "kv broker restart keeps existing reader serving then switches generation" {
    var h = try TestHarness.init(std.testing.allocator, "kv-continuity");
    defer h.cleanup();
    errdefer h.markFailed();
    const broker = try startKvBroker(&h, 1, &.{});
    try runClient(&h, 1, "seed", "write");
    const reader = try h.startService(.{ .executable_name = "ringloom-test-kv-service", .service_name = "continuity", .extra_args = &.{ "--mode", "continuity" } });
    try h.waitForServiceReady(reader, 10000);
    try h.stopProcess(broker);
    _ = try startKvBroker(&h, 1, &.{});
    try std.testing.expectEqual(@as(u32, 0), try reader.waitForExit(30000));
}

test "kv failed writer grants queued standby and crashed reader slot can reopen" {
    var h = try TestHarness.init(std.testing.allocator, "kv-writer-failure");
    defer h.cleanup();
    errdefer h.markFailed();
    _ = try startKvBroker(&h, 1, &.{});
    const holder = try h.startService(.{ .executable_name = "ringloom-test-kv-service", .service_name = "holder", .extra_args = &.{ "--mode", "hold" } });
    try h.waitForLogLine(holder, "writer holding", 10000);
    const standby = try h.startService(.{ .executable_name = "ringloom-test-kv-service", .service_name = "standby", .extra_args = &.{ "--mode", "standby" } });
    try h.waitForServiceReady(standby, 10000);
    h.killProcess(holder);
    try std.testing.expectEqual(@as(u32, 0), try standby.waitForExit(30000));
    const reader = try h.startService(.{ .executable_name = "ringloom-test-kv-service", .service_name = "doomed-reader", .extra_args = &.{ "--mode", "reader-crash" } });
    try h.waitForServiceReady(reader, 10000);
    h.killProcess(reader);
    try runClient(&h, 1, "replacement-reader", "read");
}

test "kv topic leader failover confirms reassert and writer resumes" {
    var h = try TestHarness.init(std.testing.allocator, "kv-leader-failure");
    defer h.cleanup();
    errdefer h.markFailed();
    const leader = try startKvBroker(&h, 1, &.{.{ .node_id = 2, .host = "127.0.0.1", .port = 19002 }});
    _ = try startKvBroker(&h, 2, &.{.{ .node_id = 1, .host = "127.0.0.1", .port = 19001 }});
    try std.Io.sleep(std.testing.io, .fromNanoseconds(4 * std.time.ns_per_s), .awake);
    const writer = try h.startService(.{ .executable_name = "ringloom-test-kv-service", .service_name = "surviving-writer", .broker_node_id = 2, .extra_args = &.{ "--mode", "failover" } });
    try h.waitForLogLine(writer, "writer before failover", 25000);
    h.killProcess(leader);
    try std.testing.expectEqual(@as(u32, 0), try writer.waitForExit(35000));
    try runClient(&h, 2, "surviving-reader", "read");
}

test "kv full map grows generation and preserves all entries" {
    var h = try TestHarness.init(std.testing.allocator, "kv-growth");
    defer h.cleanup();
    errdefer h.markFailed();
    _ = try startKvBroker(&h, 1, &.{});
    const writer = try h.startService(.{ .executable_name = "ringloom-test-kv-service", .service_name = "growth", .extra_args = &.{ "--mode", "growth", "--count", "12000" } });
    try std.testing.expectEqual(@as(u32, 0), try writer.waitForExit(90000));
}
