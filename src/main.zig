const std = @import("std");
const brz_broker = @import("brz_broker");

pub fn main() !void {
    std.debug.print("BRZ Broker v0.0.0\n", .{});
    std.debug.print("Platform layer initialized.\n", .{});

    // Verify platform clock works.
    const now = brz_broker.Clock.epochMillis();
    std.debug.print("Current epoch millis: {d}\n", .{now});
}

test "simple test" {
    _ = @import("brz_broker");
}
