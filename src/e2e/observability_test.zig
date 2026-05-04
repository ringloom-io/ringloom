const std = @import("std");
const net = @import("ringloom_tcp").socket;
const testing_mod = @import("ringloom_testing");
const platform = @import("ringloom_common").platform;
const Clock = platform.Clock;

const TestHarness = testing_mod.TestHarness;
const ProcessHandle = testing_mod.ProcessHandle;

test "observability exporter exposes broker and service metadata metrics" {
    const allocator = std.testing.allocator;
    var harness = try TestHarness.init(allocator, "observability-scrape");
    errdefer harness.markFailed();
    defer harness.deinit();

    const broker = try harness.startBroker(.{});
    try harness.waitForBrokerReady(broker, 5000);

    const echo = try harness.startService(.{
        .executable_name = "ringloom-test-echo-service",
        .service_name = "echo",
    });
    try harness.waitForServiceReady(echo, 5000);

    const exporter_path = try std.fmt.allocPrint(allocator, "{s}/ringloom-observability", .{harness.bin_dir});
    defer allocator.free(exporter_path);

    const exporter = try allocator.create(ProcessHandle);
    var exporter_spawned = false;
    defer {
        if (exporter_spawned) exporter.deinit();
        allocator.destroy(exporter);
    }
    exporter.* = try ProcessHandle.spawn(
        allocator,
        "ringloom-observability",
        exporter_path,
        &.{
            "--storage-path", harness.env.storage_path,
            "--group",        "ringloom-test",
            "--listen",       "127.0.0.1:19464",
        },
        harness.env.logs_path,
    );
    exporter_spawned = true;

    const metrics = try scrapeMetrics(allocator, 19464, 5000);
    defer allocator.free(metrics);

    try std.testing.expect(std.mem.indexOf(u8, metrics, "ringloom_metadata_file_up") != null);
    try std.testing.expect(std.mem.indexOf(u8, metrics, "owner_type=\"broker\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, metrics, "service_name=\"echo\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, metrics, "ringloom_counter") != null);
    try std.testing.expect(std.mem.indexOf(u8, metrics, "ringloom_broker_bytes_sent_total") != null);
    try std.testing.expect(std.mem.indexOf(u8, metrics, "ringloom_service_messages_sent_total") != null);
    try std.testing.expect(std.mem.indexOf(u8, metrics, "ringloom_ring_capacity_bytes") != null);
    try std.testing.expect(std.mem.indexOf(u8, metrics, "ringloom_ring_free_bytes") != null);

    try exporter.stop();
    _ = exporter.waitForExit(5000) catch blk: {
        exporter.kill();
        break :blk 0;
    };
    try harness.stopProcess(echo);
    try harness.stopProcess(broker);
}

fn scrapeMetrics(allocator: std.mem.Allocator, port: u16, timeout_ms: u64) ![]u8 {
    const deadline = Clock.epochMillis() + @as(i64, @intCast(timeout_ms));
    while (Clock.epochMillis() < deadline) {
        if (tryScrapeMetrics(allocator, port)) |metrics| return metrics;
        platform.sleepNanos(50 * std.time.ns_per_ms);
    }
    return error.Timeout;
}

fn tryScrapeMetrics(allocator: std.mem.Allocator, port: u16) ?[]u8 {
    const addr = net.Address.parseIp4("127.0.0.1", port) catch return null;
    const fd = net.socket(
        addr.any.family,
        std.posix.SOCK.STREAM | std.posix.SOCK.CLOEXEC,
        0,
    ) catch return null;
    defer platform.closeFd(fd);

    net.connect(fd, &addr.any, addr.getOsSockLen()) catch return null;
    _ = net.write(fd, "GET /metrics HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n") catch return null;

    var response: std.ArrayList(u8) = .empty;
    errdefer response.deinit(allocator);
    var buf: [4096]u8 = undefined;
    while (true) {
        const n = std.posix.read(fd, &buf) catch break;
        if (n == 0) break;
        response.appendSlice(allocator, buf[0..n]) catch return null;
    }
    if (response.items.len == 0) {
        response.deinit(allocator);
        return null;
    }
    return response.toOwnedSlice(allocator) catch null;
}
