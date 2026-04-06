//! BRZ Testing Harness — orchestration library for end-to-end tests and
//! performance benchmarks.
//!
//! This is the root source file for the `brz_testing` library module.
//! It re-exports all public APIs: the test harness, temporary environment
//! manager, configuration generator, process runner, readiness detection,
//! log capture, result writers, and benchmark histogram.
//!
//! ## Quick start
//!
//! ```zig
//! const testing_harness = @import("brz_testing");
//! const TestHarness = testing_harness.TestHarness;
//!
//! var h = try TestHarness.init(allocator, "my_scenario");
//! defer h.cleanup();
//!
//! const broker = try h.startBroker(.{});
//! try h.waitForBrokerReady(broker, 10_000);
//! ```

// ── Harness (top-level orchestrator) ─────────────────────────────────

pub const harness = @import("harness.zig");
pub const TestHarness = harness.TestHarness;
pub const BrokerSpec = harness.BrokerSpec;
pub const ServiceSpec = harness.ServiceSpec;
pub const PeerSpec = harness.PeerSpec;

// ── Temporary environment ────────────────────────────────────────────

pub const temp_env = @import("temp_env.zig");
pub const TempEnv = temp_env.TempEnv;

// ── Configuration generation ─────────────────────────────────────────

pub const config_gen = @import("config_gen.zig");
pub const ConfigGen = config_gen.ConfigGen;

// ── Process runner ───────────────────────────────────────────────────

pub const process_runner = @import("process_runner.zig");
pub const ProcessHandle = process_runner.ProcessHandle;
pub const ProcessState = process_runner.ProcessState;

// ── Readiness detection ──────────────────────────────────────────────

pub const readiness = @import("readiness.zig");

// ── Log capture & failure diagnostics ────────────────────────────────

pub const log_capture = @import("log_capture.zig");
pub const LogCapture = log_capture.LogCapture;

// ── Result writers (JSON output) ─────────────────────────────────────

pub const result_writer = @import("result_writer.zig");
pub const PerfResult = result_writer.PerfResult;
pub const CorrectnessResult = result_writer.CorrectnessResult;

// ── Benchmark histogram ──────────────────────────────────────────────

pub const benchmark_histogram = @import("benchmark_histogram.zig");
pub const Histogram = benchmark_histogram.Histogram;

// ── Test discovery ───────────────────────────────────────────────────
// Ensure all tests in testing submodules are discovered by `zig build test`.

comptime {
    _ = @import("harness.zig");
    _ = @import("temp_env.zig");
    _ = @import("config_gen.zig");
    _ = @import("process_runner.zig");
    _ = @import("readiness.zig");
    _ = @import("log_capture.zig");
    _ = @import("result_writer.zig");
    _ = @import("benchmark_histogram.zig");
}

test "testing module compiles and exports expected symbols" {
    // Given / When — simply reference each re-exported type to verify
    // that the module graph resolves without errors.

    // Then
    _ = TestHarness;
    _ = BrokerSpec;
    _ = ServiceSpec;
    _ = PeerSpec;
    _ = TempEnv;
    _ = ConfigGen;
    _ = ProcessHandle;
    _ = ProcessState;
    _ = LogCapture;
    _ = PerfResult;
    _ = CorrectnessResult;
    _ = Histogram;
    _ = readiness;
}
