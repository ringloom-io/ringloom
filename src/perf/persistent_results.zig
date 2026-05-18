// SPDX-License-Identifier: Apache-2.0

const std = @import("std");

pub const default_results_root = "/tmp/ringloom-perf-results";

pub fn scenarioPath(
    allocator: std.mem.Allocator,
    suite: []const u8,
    file_name: []const u8,
) ![]const u8 {
    const dir_path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ default_results_root, suite });
    defer allocator.free(dir_path);

    try std.Io.Dir.cwd().createDirPath(std.testing.io, dir_path);
    return std.fmt.allocPrint(allocator, "{s}/{s}", .{ dir_path, file_name });
}
