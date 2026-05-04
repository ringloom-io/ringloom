// SPDX-License-Identifier: Apache-2.0

pub const app = @import("app.zig");
pub const args = @import("args.zig");
pub const counters = @import("counters.zig");
pub const protocol = @import("protocol.zig");
pub const service_names = @import("service_names.zig");
pub const static_tables = @import("static_tables.zig");

comptime {
    _ = @import("app.zig");
    _ = @import("args.zig");
    _ = @import("counters.zig");
    _ = @import("protocol.zig");
    _ = @import("service_names.zig");
    _ = @import("static_tables.zig");
}

test "order-management common module compiles" {
    _ = app;
    _ = args;
    _ = counters;
    _ = protocol;
    _ = service_names;
    _ = static_tables;
}
