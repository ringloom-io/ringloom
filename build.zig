const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // ── Library modules ──────────────────────────────────────────────

    const brz_common = b.addModule("brz_common", .{
        .root_source_file = b.path("src/common/root.zig"),
        .target = target,
    });

    const brz_tcp = b.addModule("brz_tcp", .{
        .root_source_file = b.path("src/tcp/tcp.zig"),
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
            .{ .name = "brz_tcp", .module = brz_tcp },
        },
    });

    const brz_testing = b.addModule("brz_testing", .{
        .root_source_file = b.path("src/testing/root.zig"),
        .target = target,
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
                .{ .name = "brz_tcp", .module = brz_tcp },
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

    // ── Test service binaries (for end-to-end IPC tests) ─────────────
    //
    // Each entry maps a Zig source file name (under src/bin/) to the
    // installed executable name that the e2e/perf harness expects.

    const TestBinEntry = struct {
        source_name: []const u8,
        exe_name: []const u8,
    };

    const test_bin_entries = [_]TestBinEntry{
        .{ .source_name = "test_echo_service", .exe_name = "brz-test-echo-service" },
        .{ .source_name = "test_ping_service", .exe_name = "brz-test-ping-service" },
        .{ .source_name = "test_forwarder_service", .exe_name = "brz-test-forwarder-service" },
        .{ .source_name = "test_leader_service", .exe_name = "brz-test-leader-service" },
        .{ .source_name = "test_slow_consumer_service", .exe_name = "brz-test-slow-consumer-service" },
        .{ .source_name = "test_crashy_service", .exe_name = "brz-test-crashy-service" },
    };

    const test_bins_step = b.step("test-bins", "Build all test service binaries");

    for (test_bin_entries) |entry| {
        const test_exe = b.addExecutable(.{
            .name = entry.exe_name,
            .root_module = b.createModule(.{
                .root_source_file = b.path(
                    b.fmt("src/bin/{s}.zig", .{entry.source_name}),
                ),
                .target = target,
                .optimize = optimize,
                .imports = &.{
                    .{ .name = "brz_common", .module = brz_common },
                    .{ .name = "brz_service", .module = brz_service },
                    .{ .name = "brz_testing", .module = brz_testing },
                },
            }),
        });
        b.installArtifact(test_exe);
        test_bins_step.dependOn(&test_exe.step);
    }

    // ── Unit & integration tests ─────────────────────────────────────

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
                .{ .name = "brz_tcp", .module = brz_tcp },
            },
        }),
    });
    const run_broker_tests = b.addRunArtifact(broker_tests);

    const tcp_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/tcp/tcp.zig"),
            .target = target,
        }),
    });
    const run_tcp_tests = b.addRunArtifact(tcp_tests);

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

    const testing_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/testing/root.zig"),
            .target = target,
        }),
    });
    const run_testing_tests = b.addRunArtifact(testing_tests);

    const test_step = b.step("test", "Run all unit and integration tests");
    test_step.dependOn(&run_common_tests.step);
    test_step.dependOn(&run_broker_tests.step);
    test_step.dependOn(&run_tcp_tests.step);
    test_step.dependOn(&run_service_tests.step);
    test_step.dependOn(&run_exe_tests.step);
    test_step.dependOn(&run_testing_tests.step);

    const test_testing_step = b.step("test-testing", "Run testing harness tests only");
    test_testing_step.dependOn(&run_testing_tests.step);

    // ── End-to-end correctness tests ─────────────────────────────────
    //
    // These tests spawn real broker and service processes. The build
    // step depends on the broker executable and all test service
    // binaries being installed first.

    const e2e_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/e2e/root.zig"),
            .target = target,
            .imports = &.{
                .{ .name = "brz_testing", .module = brz_testing },
            },
        }),
    });
    const run_e2e_tests = b.addRunArtifact(e2e_tests);

    // e2e tests need the broker and all test services built first.
    run_e2e_tests.step.dependOn(b.getInstallStep());
    run_e2e_tests.step.dependOn(test_bins_step);

    const e2e_step = b.step("e2e", "Run end-to-end correctness tests");
    e2e_step.dependOn(&run_e2e_tests.step);

    // ── Performance benchmarks ───────────────────────────────────────
    //
    // Separate from correctness tests. Longer-running and
    // environment-sensitive. NOT included in the default `test` step.

    const perf_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/perf/root.zig"),
            .target = target,
            .optimize = .ReleaseFast,
            .imports = &.{
                .{ .name = "brz_testing", .module = brz_testing },
            },
        }),
    });
    const run_perf_tests = b.addRunArtifact(perf_tests);

    // perf tests also need broker + test services installed.
    run_perf_tests.step.dependOn(b.getInstallStep());
    run_perf_tests.step.dependOn(test_bins_step);

    const perf_step = b.step("perf", "Run performance benchmarks (ReleaseFast)");
    perf_step.dependOn(&run_perf_tests.step);

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
