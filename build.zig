const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // ── Library modules ──────────────────────────────────────────────

    const ringloom_common = b.addModule("ringloom_common", .{
        .root_source_file = b.path("src/common/root.zig"),
        .target = target,
    });

    const ringloom_tcp = b.addModule("ringloom_tcp", .{
        .root_source_file = b.path("src/tcp/tcp.zig"),
        .target = target,
        .imports = &.{
            .{ .name = "ringloom_common", .module = ringloom_common },
        },
    });

    const ringloom_service = b.addModule("ringloom_service", .{
        .root_source_file = b.path("src/service/root.zig"),
        .target = target,
        .imports = &.{
            .{ .name = "ringloom_common", .module = ringloom_common },
        },
    });

    const service_c_abi_shared_mod = b.createModule(.{
        .root_source_file = b.path("src/service/c_abi.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "ringloom_common", .module = ringloom_common },
        },
    });

    const service_c_abi_static_mod = b.createModule(.{
        .root_source_file = b.path("src/service/c_abi.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "ringloom_common", .module = ringloom_common },
        },
    });

    const service_shared_lib = b.addLibrary(.{
        .name = "ringloom_service",
        .linkage = .dynamic,
        .root_module = service_c_abi_shared_mod,
    });
    b.installArtifact(service_shared_lib);

    const service_static_lib = b.addLibrary(.{
        .name = "ringloom_service",
        .linkage = .static,
        .root_module = service_c_abi_static_mod,
    });
    b.installArtifact(service_static_lib);

    const install_service_header = b.addInstallHeaderFile(
        b.path("include/ringloom_service.h"),
        "ringloom_service.h",
    );
    b.getInstallStep().dependOn(&install_service_header.step);

    const ringloom_broker = b.addModule("ringloom_broker", .{
        .root_source_file = b.path("src/broker/root.zig"),
        .target = target,
        .imports = &.{
            .{ .name = "ringloom_common", .module = ringloom_common },
            .{ .name = "ringloom_tcp", .module = ringloom_tcp },
        },
    });

    const ringloom_testing = b.addModule("ringloom_testing", .{
        .root_source_file = b.path("src/testing/root.zig"),
        .target = target,
        .imports = &.{
            .{ .name = "ringloom_common", .module = ringloom_common },
        },
    });

    // ── Broker executable ────────────────────────────────────────────

    const broker_exe = b.addExecutable(.{
        .name = "ringloom-broker",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/bin/ringloom_broker_main.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "ringloom_common", .module = ringloom_common },
                .{ .name = "ringloom_broker", .module = ringloom_broker },
                .{ .name = "ringloom_tcp", .module = ringloom_tcp },
                .{ .name = "ringloom_service", .module = ringloom_service },
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
        .{ .source_name = "test_echo_service", .exe_name = "ringloom-test-echo-service" },
        .{ .source_name = "test_ping_service", .exe_name = "ringloom-test-ping-service" },
        .{ .source_name = "test_forwarder_service", .exe_name = "ringloom-test-forwarder-service" },
        .{ .source_name = "test_leader_service", .exe_name = "ringloom-test-leader-service" },
        .{ .source_name = "test_slow_consumer_service", .exe_name = "ringloom-test-slow-consumer-service" },
        .{ .source_name = "test_crashy_service", .exe_name = "ringloom-test-crashy-service" },
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
                    .{ .name = "ringloom_common", .module = ringloom_common },
                    .{ .name = "ringloom_service", .module = ringloom_service },
                    .{ .name = "ringloom_testing", .module = ringloom_testing },
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
                .{ .name = "ringloom_common", .module = ringloom_common },
                .{ .name = "ringloom_tcp", .module = ringloom_tcp },
            },
        }),
    });
    const run_broker_tests = b.addRunArtifact(broker_tests);

    const tcp_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/tcp/tcp.zig"),
            .target = target,
            .imports = &.{
                .{ .name = "ringloom_common", .module = ringloom_common },
            },
        }),
    });
    const run_tcp_tests = b.addRunArtifact(tcp_tests);

    const service_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/service/root.zig"),
            .target = target,
            .imports = &.{
                .{ .name = "ringloom_common", .module = ringloom_common },
            },
        }),
    });
    const run_service_tests = b.addRunArtifact(service_tests);

    const exe_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/bin/ringloom_broker_main.zig"),
            .target = target,
            .imports = &.{
                .{ .name = "ringloom_common", .module = ringloom_common },
                .{ .name = "ringloom_broker", .module = ringloom_broker },
                .{ .name = "ringloom_service", .module = ringloom_service },
            },
        }),
    });
    const run_exe_tests = b.addRunArtifact(exe_tests);

    const testing_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/testing/root.zig"),
            .target = target,
            .imports = &.{
                .{ .name = "ringloom_common", .module = ringloom_common },
            },
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

    const service_c_step = b.step("service-c", "Build the service C ABI library and header");
    service_c_step.dependOn(b.getInstallStep());
    service_c_step.dependOn(&run_service_tests.step);

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
                .{ .name = "ringloom_common", .module = ringloom_common },
                .{ .name = "ringloom_testing", .module = ringloom_testing },
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
                .{ .name = "ringloom_common", .module = ringloom_common },
                .{ .name = "ringloom_testing", .module = ringloom_testing },
            },
        }),
    });
    const run_perf_tests = b.addRunArtifact(perf_tests);

    // perf tests also need broker + test services installed.
    run_perf_tests.step.dependOn(b.getInstallStep());
    run_perf_tests.step.dependOn(test_bins_step);

    const perf_step = b.step("perf", "Run performance benchmarks (ReleaseFast)");
    perf_step.dependOn(&run_perf_tests.step);

    const java_bindings_cmd = b.addSystemCommand(&.{
        "sh",
        "-c",
        "cd bindings/java && gradle --no-daemon classes",
    });
    java_bindings_cmd.step.dependOn(b.getInstallStep());

    const java_bindings_step = b.step("java-bindings", "Compile the Java FFM bindings");
    java_bindings_step.dependOn(&java_bindings_cmd.step);

    const java_test_cmd = b.addSystemCommand(&.{
        "sh",
        "-c",
        "ROOT=$(pwd) && cd bindings/java && gradle --no-daemon test -Dringloom.projectRoot=\"$ROOT\" -Dringloom.nativeLibDir=\"$ROOT/zig-out/lib\" -Dringloom.brokerBin=\"$ROOT/zig-out/bin/ringloom-broker\"",
    });
    java_test_cmd.step.dependOn(b.getInstallStep());

    const test_java_step = b.step("test-java", "Run Java integration tests");
    test_java_step.dependOn(&java_test_cmd.step);

    // ── ringloom-stat utility ─────────────────────────────────────────────

    const stat_exe = b.addExecutable(.{
        .name = "ringloom-stat",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tools/ringloom_stat.zig"),
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

    const stat_step = b.step("stat", "Run the ringloom-stat monitoring tool");
    stat_step.dependOn(&stat_run_cmd.step);
}
