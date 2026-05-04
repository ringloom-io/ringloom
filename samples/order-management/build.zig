// SPDX-License-Identifier: Apache-2.0

const std = @import("std");

pub fn build(b: *std.Build) void {
    const run = b.addSystemCommand(&.{
        "zig",
        "build",
        "sample-order-management",
    });
    run.setCwd(b.path("../.."));

    const step = b.step("sample-order-management", "Delegate to the repository root sample build");
    step.dependOn(&run.step);
}
