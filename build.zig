const std = @import("std");
const aeron_build = @import("build_support/aeron.zig");

pub fn build(b: *std.Build) void {
    const requested_target = b.standardTargetOptions(.{});
    const target = requested_target;
    const optimize = b.standardOptimizeOption(.{});
    const use_llvm = b.option(bool, "use-llvm", "Build Zig artifacts with the LLVM backend") orelse true;

    // ── Library modules ──────────────────────────────────────────────

    const ringloom_common = b.addModule("ringloom_common", .{
        .root_source_file = b.path("src/common/root.zig"),
        .target = target,
    });

    const ringloom_aeron = aeron_build.buildRingLoomAeron(b, target, optimize, .{
        .module_name = "ringloom_aeron",
        .library_name = "aeron_driver",
        .use_llvm = use_llvm,
    });

    const ringloom_aeron_perf = aeron_build.buildRingLoomAeron(b, target, .ReleaseFast, .{
        .module_name = null,
        .library_name = "aeron_driver_perf",
        .use_llvm = use_llvm,
    });

    const ringloom_service = b.addModule("ringloom_service", .{
        .root_source_file = b.path("src/service/root.zig"),
        .target = target,
        .imports = &.{
            .{ .name = "ringloom_common", .module = ringloom_common },
            .{ .name = "ringloom_aeron", .module = ringloom_aeron.module },
        },
    });

    const service_c_abi_shared_mod = b.createModule(.{
        .root_source_file = b.path("src/service/c_abi.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "ringloom_common", .module = ringloom_common },
            .{ .name = "ringloom_aeron", .module = ringloom_aeron.module },
        },
    });

    const service_c_abi_static_mod = b.createModule(.{
        .root_source_file = b.path("src/service/c_abi.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "ringloom_common", .module = ringloom_common },
            .{ .name = "ringloom_aeron", .module = ringloom_aeron.module },
        },
    });

    const service_shared_lib = b.addLibrary(.{
        .name = "ringloom_service",
        .linkage = .dynamic,
        .root_module = service_c_abi_shared_mod,
        .use_llvm = use_llvm,
    });
    const install_service_shared_lib = b.addInstallArtifact(service_shared_lib, .{});
    b.getInstallStep().dependOn(&install_service_shared_lib.step);

    const service_static_lib = b.addLibrary(.{
        .name = "ringloom_service",
        .linkage = .static,
        .root_module = service_c_abi_static_mod,
        .use_llvm = use_llvm,
    });
    const install_service_static_lib = b.addInstallArtifact(service_static_lib, .{});
    b.getInstallStep().dependOn(&install_service_static_lib.step);

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
            .{ .name = "ringloom_aeron", .module = ringloom_aeron.module },
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
                .{ .name = "ringloom_service", .module = ringloom_service },
            },
        }),
        .use_llvm = use_llvm,
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
            .use_llvm = use_llvm,
        });
        b.installArtifact(test_exe);
        test_bins_step.dependOn(&test_exe.step);
    }

    // ── Unit & integration tests ─────────────────────────────────────

    const c_link_test_target = target;

    const common_for_c_link_tests = b.createModule(.{
        .root_source_file = b.path("src/common/root.zig"),
        .target = c_link_test_target,
    });
    const aeron_for_c_link_tests = aeron_build.buildRingLoomAeron(b, c_link_test_target, optimize, .{
        .module_name = null,
        .library_name = "aeron_driver_c_link_test",
        .use_llvm = use_llvm,
    });
    const broker_for_c_link_tests = b.createModule(.{
        .root_source_file = b.path("src/broker/root.zig"),
        .target = c_link_test_target,
        .imports = &.{
            .{ .name = "ringloom_common", .module = common_for_c_link_tests },
            .{ .name = "ringloom_aeron", .module = aeron_for_c_link_tests.module },
        },
    });
    const service_for_c_link_tests = b.createModule(.{
        .root_source_file = b.path("src/service/root.zig"),
        .target = c_link_test_target,
        .imports = &.{
            .{ .name = "ringloom_common", .module = common_for_c_link_tests },
            .{ .name = "ringloom_aeron", .module = aeron_for_c_link_tests.module },
        },
    });

    const common_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/common/root.zig"),
            .target = target,
        }),
        .use_llvm = use_llvm,
    });
    const run_common_tests = b.addRunArtifact(common_tests);

    const broker_tests = b.addTest(.{
        .root_module = broker_for_c_link_tests,
        .use_llvm = use_llvm,
    });
    const run_broker_tests = b.addRunArtifact(broker_tests);

    var run_aeron_tests: ?*std.Build.Step.Run = null;
    if (target.result.os.tag == .linux) {
        const aeron_test_target = target;
        const ringloom_aeron_for_tests = aeron_build.buildRingLoomAeron(b, aeron_test_target, optimize, .{
            .module_name = null,
            .library_name = "aeron_driver_test",
            .use_llvm = use_llvm,
        });
        const aeron_tests = b.addTest(.{
            .root_module = ringloom_aeron_for_tests.module,
            .use_llvm = use_llvm,
        });
        run_aeron_tests = b.addRunArtifact(aeron_tests);
    }

    const service_tests = b.addTest(.{
        .root_module = service_for_c_link_tests,
        .use_llvm = use_llvm,
    });
    const run_service_tests = b.addRunArtifact(service_tests);

    const exe_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/bin/ringloom_broker_main.zig"),
            .target = c_link_test_target,
            .imports = &.{
                .{ .name = "ringloom_common", .module = common_for_c_link_tests },
                .{ .name = "ringloom_broker", .module = broker_for_c_link_tests },
                .{ .name = "ringloom_service", .module = service_for_c_link_tests },
            },
        }),
        .use_llvm = use_llvm,
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
        .use_llvm = use_llvm,
    });
    const run_testing_tests = b.addRunArtifact(testing_tests);

    const test_step = b.step("test", "Run all unit and integration tests");
    test_step.dependOn(&run_common_tests.step);
    test_step.dependOn(&run_broker_tests.step);
    if (run_aeron_tests) |run| {
        test_step.dependOn(&run.step);
    }
    test_step.dependOn(&run_service_tests.step);
    test_step.dependOn(&run_exe_tests.step);
    test_step.dependOn(&run_testing_tests.step);

    const test_testing_step = b.step("test-testing", "Run testing harness tests only");
    test_testing_step.dependOn(&run_testing_tests.step);

    const test_aeron_step = b.step("test-aeron", "Run Aeron wrapper tests only");
    if (run_aeron_tests) |run| {
        test_aeron_step.dependOn(&run.step);
    } else {
        const fail = b.addFail("ringloom_aeron tests are currently supported only on Linux");
        test_aeron_step.dependOn(&fail.step);
    }

    const service_c_step = b.step("service-c", "Build the service C ABI library and header");
    service_c_step.dependOn(&install_service_shared_lib.step);
    service_c_step.dependOn(&install_service_static_lib.step);
    service_c_step.dependOn(&install_service_header.step);
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
        .use_llvm = use_llvm,
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
        .use_llvm = use_llvm,
    });
    const run_perf_tests = b.addRunArtifact(perf_tests);

    // perf tests also need broker + test services installed.
    run_perf_tests.step.dependOn(b.getInstallStep());
    run_perf_tests.step.dependOn(test_bins_step);

    const perf_step = b.step("perf", "Run performance benchmarks (ReleaseFast)");
    perf_step.dependOn(&run_perf_tests.step);

    const ring_buffer_perf_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/perf/ring_buffer_bench.zig"),
            .target = target,
            .optimize = .ReleaseFast,
            .imports = &.{
                .{ .name = "ringloom_common", .module = ringloom_common },
            },
        }),
        .use_llvm = use_llvm,
    });
    const run_ring_buffer_perf_tests = b.addRunArtifact(ring_buffer_perf_tests);

    const perf_ring_buffer_step = b.step("perf-ring-buffer", "Run raw ring-buffer microbenchmarks (ReleaseFast)");
    perf_ring_buffer_step.dependOn(&run_ring_buffer_perf_tests.step);

    const plain_aeron_perf_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/perf/plain_aeron_remote_bench.zig"),
            .target = target,
            .optimize = .ReleaseFast,
            .imports = &.{
                .{ .name = "ringloom_common", .module = ringloom_common },
                .{ .name = "ringloom_aeron", .module = ringloom_aeron_perf.module },
                .{ .name = "ringloom_testing", .module = ringloom_testing },
            },
        }),
        .use_llvm = use_llvm,
    });
    const run_plain_aeron_perf_tests = b.addRunArtifact(plain_aeron_perf_tests);

    const perf_aeron_step = b.step("perf-aeron", "Run plain Aeron remote transit benchmarks (ReleaseFast)");
    perf_aeron_step.dependOn(&run_plain_aeron_perf_tests.step);

    const java_bindings_cmd = b.addSystemCommand(&.{
        "sh",
        "-c",
        "ROOT=$(pwd) && cd bindings/java && gradle --no-daemon classes -Pringloom.embeddedNativeLibDir=\"$ROOT/zig-out/lib\"",
    });
    java_bindings_cmd.step.dependOn(b.getInstallStep());

    const java_bindings_step = b.step("java-bindings", "Compile the Java FFM bindings");
    java_bindings_step.dependOn(&java_bindings_cmd.step);

    const java_test_cmd = b.addSystemCommand(&.{
        "sh",
        "-c",
        "ROOT=$(pwd) && cd bindings/java && gradle --no-daemon test -Pringloom.embeddedNativeLibDir=\"$ROOT/zig-out/lib\" -Dringloom.projectRoot=\"$ROOT\" -Dringloom.brokerBin=\"$ROOT/zig-out/bin/ringloom-broker\"",
    });
    java_test_cmd.step.dependOn(b.getInstallStep());

    const test_java_step = b.step("test-java", "Run Java integration tests");
    test_java_step.dependOn(&java_test_cmd.step);

    b.installDirectory(.{
        .source_dir = b.path("bindings/cpp/include"),
        .install_dir = .header,
        .install_subdir = "",
    });

    const cpp_bindings_cmd = b.addSystemCommand(&.{
        "sh",
        "-c",
        \\ROOT=$(pwd) &&
        \\mkdir -p "$ROOT/zig-out/bin" &&
        \\zig c++ -std=c++17 -Wall -Wextra -pedantic \
        \\  -I"$ROOT/include" \
        \\  -I"$ROOT/bindings/cpp/include" \
        \\  "$ROOT/bindings/cpp/test/cpp_bindings_test.cpp" \
        \\  -L"$ROOT/zig-out/lib" \
        \\  -lringloom_service \
        \\  -Wl,-rpath,"$ROOT/zig-out/lib" \
        \\  -o "$ROOT/zig-out/bin/ringloom-cpp-bindings-test"
    });
    cpp_bindings_cmd.step.dependOn(b.getInstallStep());

    const cpp_bindings_step = b.step("cpp-bindings", "Compile the C++ service bindings");
    cpp_bindings_step.dependOn(&cpp_bindings_cmd.step);

    const cpp_test_cmd = b.addSystemCommand(&.{
        "sh",
        "-c",
        \\ROOT=$(pwd) &&
        \\RINGLOOM_PROJECT_ROOT="$ROOT" \
        \\RINGLOOM_BROKER_BIN="$ROOT/zig-out/bin/ringloom-broker" \
        \\"$ROOT/zig-out/bin/ringloom-cpp-bindings-test"
    });
    cpp_test_cmd.step.dependOn(&cpp_bindings_cmd.step);

    const test_cpp_step = b.step("test-cpp", "Run C++ binding integration tests");
    test_cpp_step.dependOn(&cpp_test_cmd.step);

    const node_bindings_cmd = b.addSystemCommand(&.{
        "sh",
        "-c",
        "ROOT=$(pwd) && cd bindings/node && npm ci --ignore-scripts && npm run build",
    });
    node_bindings_cmd.step.dependOn(b.getInstallStep());

    const node_bindings_step = b.step("node-bindings", "Compile the Node.js Node-API bindings");
    node_bindings_step.dependOn(&node_bindings_cmd.step);

    const node_test_cmd = b.addSystemCommand(&.{
        "sh",
        "-c",
        "ROOT=$(pwd) && cd bindings/node && npm ci --ignore-scripts && npm run build && RINGLOOM_PROJECT_ROOT=\"$ROOT\" RINGLOOM_BROKER_BIN=\"$ROOT/zig-out/bin/ringloom-broker\" npm test",
    });
    node_test_cmd.step.dependOn(b.getInstallStep());

    const test_node_step = b.step("test-node", "Run Node.js binding integration tests");
    test_node_step.dependOn(&node_test_cmd.step);

    // ── ringloom-stat utility ─────────────────────────────────────────────

    const stat_exe = b.addExecutable(.{
        .name = "ringloom-stat",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tools/ringloom_stat.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "ringloom_common", .module = ringloom_common },
            },
        }),
        .use_llvm = use_llvm,
    });
    b.installArtifact(stat_exe);

    const stat_run_cmd = b.addRunArtifact(stat_exe);
    stat_run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        stat_run_cmd.addArgs(args);
    }

    const stat_step = b.step("stat", "Run the ringloom-stat monitoring tool");
    stat_step.dependOn(&stat_run_cmd.step);

    // ── Prometheus observability utility ───────────────────────────────

    const observability_exe = b.addExecutable(.{
        .name = "ringloom-observability",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tools/ringloom_observability.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "ringloom_common", .module = ringloom_common },
            },
        }),
        .use_llvm = use_llvm,
    });
    b.installArtifact(observability_exe);

    const observability_build_step = b.step("observability", "Build the ringloom-observability Prometheus exporter");
    observability_build_step.dependOn(&observability_exe.step);

    const observability_run_cmd = b.addRunArtifact(observability_exe);
    observability_run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        observability_run_cmd.addArgs(args);
    }

    const observability_run_step = b.step("run-observability", "Run the ringloom-observability Prometheus exporter");
    observability_run_step.dependOn(&observability_run_cmd.step);

    // ── Order-management sample ──────────────────────────────────────

    const order_sample_common = b.addModule("order_management_sample_common", .{
        .root_source_file = b.path("samples/order-management/src/common/root.zig"),
        .target = target,
        .imports = &.{
            .{ .name = "ringloom_common", .module = ringloom_common },
            .{ .name = "ringloom_service", .module = ringloom_service },
        },
    });

    const SampleBinEntry = struct {
        source_name: []const u8,
        exe_name: []const u8,
    };

    const sample_bin_entries = [_]SampleBinEntry{
        .{ .source_name = "order_simulator", .exe_name = "ringloom-sample-order-simulator" },
        .{ .source_name = "order_gateway", .exe_name = "ringloom-sample-order-gateway" },
        .{ .source_name = "risk_service", .exe_name = "ringloom-sample-risk-service" },
        .{ .source_name = "matching_engine", .exe_name = "ringloom-sample-matching-engine" },
        .{ .source_name = "execution_service", .exe_name = "ringloom-sample-execution-service" },
        .{ .source_name = "portfolio_service", .exe_name = "ringloom-sample-portfolio-service" },
    };

    const sample_build_step = b.step("sample-order-management", "Build the order-management sample binaries");

    for (sample_bin_entries) |entry| {
        const sample_exe = b.addExecutable(.{
            .name = entry.exe_name,
            .root_module = b.createModule(.{
                .root_source_file = b.path(
                    b.fmt("samples/order-management/src/services/{s}.zig", .{entry.source_name}),
                ),
                .target = target,
                .optimize = optimize,
                .imports = &.{
                    .{ .name = "ringloom_common", .module = ringloom_common },
                    .{ .name = "ringloom_service", .module = ringloom_service },
                    .{ .name = "order_management_sample_common", .module = order_sample_common },
                },
            }),
            .use_llvm = use_llvm,
        });
        b.installArtifact(sample_exe);
        sample_build_step.dependOn(&sample_exe.step);
    }

    const sample_common_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("samples/order-management/src/common/root.zig"),
            .target = target,
            .imports = &.{
                .{ .name = "ringloom_common", .module = ringloom_common },
                .{ .name = "ringloom_service", .module = ringloom_service },
            },
        }),
        .use_llvm = use_llvm,
    });
    const run_sample_common_tests = b.addRunArtifact(sample_common_tests);
    sample_build_step.dependOn(&run_sample_common_tests.step);
    sample_build_step.dependOn(&broker_exe.step);
    sample_build_step.dependOn(&stat_exe.step);
    sample_build_step.dependOn(b.getInstallStep());

    const run_sample_cmd = b.addSystemCommand(&.{
        "samples/order-management/scripts/run.sh",
        "--no-build",
        "--bin-dir",
        "zig-out/bin",
    });
    run_sample_cmd.step.dependOn(sample_build_step);
    if (b.args) |args| {
        run_sample_cmd.addArgs(args);
    }

    const run_sample_step = b.step("run-sample-order-management", "Run the order-management sample");
    run_sample_step.dependOn(&run_sample_cmd.step);

    const sample_smoke_cmd = b.addSystemCommand(&.{
        "samples/order-management/scripts/run.sh",
        "--no-build",
        "--bin-dir",
        "zig-out/bin",
        "--orders",
        "32",
        "--rate-per-sec",
        "1000",
    });
    sample_smoke_cmd.step.dependOn(sample_build_step);

    const sample_smoke_step = b.step("sample-order-management-smoke", "Run a small order-management sample smoke test");
    sample_smoke_step.dependOn(&sample_smoke_cmd.step);
}
