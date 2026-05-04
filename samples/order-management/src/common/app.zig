// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const ringloom_common = @import("ringloom_common");
const ringloom_service = @import("ringloom_service");

const Clock = ringloom_common.platform.Clock;
const platform = ringloom_common.platform;
const ServiceClient = ringloom_service.ServiceClient;

var signal_target: ?*std.atomic.Value(bool) = null;

pub fn installSignalHandler(shutdown_requested: *std.atomic.Value(bool)) void {
    signal_target = shutdown_requested;
    const handler: std.posix.Sigaction = .{
        .handler = .{ .handler = signalHandler },
        .mask = std.posix.sigemptyset(),
        .flags = 0,
    };
    std.posix.sigaction(std.posix.SIG.TERM, &handler, null);
    std.posix.sigaction(std.posix.SIG.INT, &handler, null);
}

fn signalHandler(_: std.posix.SIG) callconv(.c) void {
    if (signal_target) |target| {
        target.store(true, .release);
    }
}

pub fn printReady(io: std.Io, service_name: []const u8) !void {
    var buf: [512]u8 = undefined;
    var stdout_state = std.Io.File.stdout().writer(io, &buf);
    const stdout = &stdout_state.interface;
    try stdout.print("service ready: name={s}\n", .{service_name});
    try stdout.flush();
}

pub fn waitForClient(client: *ServiceClient, timeout_ms: u64) bool {
    const deadline = @as(i128, Clock.monotonicNanosStable()) +
        @as(i128, timeout_ms) * std.time.ns_per_ms;
    while (Clock.monotonicNanosStable() < deadline) {
        if (client.instanceCount() > 0) return true;
        platform.sleepNanos(50 * std.time.ns_per_ms);
    }
    return client.instanceCount() > 0;
}

pub fn logLifecycle(io: std.Io, owner_name: []const u8, event: ServiceClient.LifecycleEvent) void {
    _ = io;
    std.debug.print(
        "{s}: lifecycle target={s} event={s} service_id={d} node_id={d} leader={}\n",
        .{
            owner_name,
            event.service_name,
            @tagName(event.event_type),
            event.service_id,
            event.node_id,
            event.is_leader,
        },
    );
}
