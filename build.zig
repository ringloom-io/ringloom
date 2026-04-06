const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // ── Library modules ──────────────────────────────────────────────

    const brz_common = b.addModule("brz_common", .{
        .root_source_file = b.path("src/common/root.zig"),
        .target = target,
    });

    const brz_service = b.addModule("brz_service", .{
        .root_source_file = b.path("src/service/root.zig"),
        .target = target,
        .imports = &.{
            .{ .name = "brz_common", .module = brz_common },
        },
    });

    const brz_broker = b.addModule("brz_broker", .{
        .root_source_file = b.path("src/broker/root.zig"),
        .target = target,
        .imports = &.{
            .{ .name = "brz_common", .module = brz_common },
        },
    });

    // ── Broker executable ────────────────────────────────────────────

    const broker_exe = b.addExecutable(.{
        .name = "brz-broker",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/bin/brz_broker_main.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "brz_common", .module = brz_common },
                .{ .name = "brz_broker", .module = brz_broker },
                .{ .name = "brz_service", .module = brz_service },
            },
        }),
    });
    b.installArtifact(broker_exe);

    const run_step = b.step("run", "Run the broker");
    const run_cmd = b.addRunArtifact(broker_exe);
    run_step.dependOn(&run_cmd.step);
    run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_cmd.addArgs(args);
    }

    // ── Tests ────────────────────────────────────────────────────────

    const common_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/common/root.zig"),
            .target = target,
        }),
    });
    const run_common_tests = b.addRunArtifact(common_tests);

    const broker_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/broker/root.zig"),
            .target = target,
            .imports = &.{
                .{ .name = "brz_common", .module = brz_common },
            },
        }),
    });
    const run_broker_tests = b.addRunArtifact(broker_tests);

    const service_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/service/root.zig"),
            .target = target,
            .imports = &.{
                .{ .name = "brz_common", .module = brz_common },
            },
        }),
    });
    const run_service_tests = b.addRunArtifact(service_tests);

    const exe_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/bin/brz_broker_main.zig"),
            .target = target,
            .imports = &.{
                .{ .name = "brz_common", .module = brz_common },
                .{ .name = "brz_broker", .module = brz_broker },
                .{ .name = "brz_service", .module = brz_service },
            },
        }),
    });
    const run_exe_tests = b.addRunArtifact(exe_tests);

    const test_step = b.step("test", "Run all tests");
    test_step.dependOn(&run_common_tests.step);
    test_step.dependOn(&run_broker_tests.step);
    test_step.dependOn(&run_service_tests.step);
    test_step.dependOn(&run_exe_tests.step);

    // ── brz-stat utility ─────────────────────────────────────────────

    const stat_exe = b.addExecutable(.{
        .name = "brz-stat",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tools/brz_stat.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    b.installArtifact(stat_exe);

    const stat_run_cmd = b.addRunArtifact(stat_exe);
    stat_run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        stat_run_cmd.addArgs(args);
    }

    const stat_step = b.step("stat", "Run the brz-stat monitoring tool");
    stat_step.dependOn(&stat_run_cmd.step);
}
