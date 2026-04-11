# Testing Guide — BRZ Broker

Instructions for running end-to-end correctness tests and performance benchmarks.
Intended for developers working on the library and for automated agents.

> **Design reference:** See `docs/impl/15-end-to-end-and-performance-testing.md`
> for the full rationale, test harness architecture, and scenario specifications.

---

## Prerequisites

| Tool | Version |
|------|---------|
| Zig  | 0.15.x  |
| OS   | Linux (shared-memory IPC requires `/dev/shm`) |

No external dependencies are required — everything is built from source via the
Zig build system.

---

## Build Steps Overview

```text
zig build test          # Unit & integration tests (fast, every change)
zig build e2e           # End-to-end correctness tests (multi-process)
zig build perf          # Performance benchmarks (ReleaseFast, longer-running)
zig build test-bins     # Build test service binaries only (no test run)
```

### Build step dependency graph

```text
test          →  unit & integration tests (all library modules)
e2e           →  broker exe + all test service binaries → e2e test runner
perf          →  broker exe + all test service binaries → perf benchmark runner (ReleaseFast)
test-bins     →  all test service binaries
```

---

## 1. Unit & Integration Tests

**What they cover:** Individual modules — ring buffers, encoders, parsers, config
parsing, harness utilities, IPC primitives, TCP framing, and broker internals.

**When to run:** On every code change. These are fast and deterministic.

```bash
zig build test
```

With full build summary:

```bash
zig build test --summary all
```

**Expected result:** All tests pass. Current baseline: **480 tests**.

---

## 2. End-to-End Correctness Tests

**What they cover:** Real multi-process scenarios — the broker is launched as a
separate process, test services are spawned, and observable behavior (readiness
markers, log output, metadata files, exit codes) is asserted.

**When to run:** On every PR and merge to main. These are longer-running but
provide the highest confidence for runtime correctness.

```bash
zig build e2e
```

### Test scenarios (31 tests in 11 files)

| File | Scenarios |
|------|-----------|
| `broker_startup_test.zig` | Startup, shutdown, invalid config, metadata creation, SIGTERM handling |
| `registration_test.zig` | Single service registration, multiple services, unique service IDs |
| `local_ipc_test.zig` | Two-service direct IPC, concurrent multi-service IPC, forwarder chain |
| `discovery_test.zig` | Discovery updates, late registration, removal notification |
| `cross_broker_test.zig` | Two-broker routing, three-broker routing, late broker join |
| `fragmentation_test.zig` | Large message fragmentation/reassembly, mixed sizes, very large messages |
| `heartbeat_timeout_test.zig` | Dead service cleanup, healthy service unaffected |
| `restart_test.zig` | Metadata reuse after cleanup, restart without prior cleanup |
| `leader_election_test.zig` | Leader election (2 instances), failover, three-instance survival |
| `graceful_unregister_test.zig` | Unregister timing, sequential unregister, discovery removal |
| `backpressure_test.zig` | System stability under slow consumer, multiple producers |

### How the harness works

Each test:
1. Creates an isolated temp workspace under `/tmp/brz-e2e-*`
2. Generates broker and service config files
3. Spawns processes from `zig-out/bin/`
4. Waits for readiness using log markers and file existence checks
5. Asserts on observable behavior
6. Tears down in reverse order
7. On failure: preserves the temp directory for inspection

### Debugging failed tests

If a test fails, the harness preserves the temp directory. Look for:

```text
/tmp/brz-e2e-<scenario>-<seq>/
├── config/     # Generated broker/service properties files
├── logs/       # stdout/stderr capture per process
├── results/    # JSON result files (if produced)
└── storage/    # Shared-memory root (instead of /dev/shm)
```

Review `logs/` for the broker and service stdout/stderr to identify the failure
point.

---

## 3. Performance Benchmarks

**What they cover:** Latency, throughput, backpressure onset, and recovery time
under realistic topologies (single-broker and two-broker clusters).

**When to run:** Manually before/after performance-sensitive changes, or on a
scheduled nightly basis. These are the slowest tests.

```bash
zig build perf
```

> Performance benchmarks are compiled with `ReleaseFast` optimization. They are
> **never** included in the default `test` step.

### Benchmark categories (30 tests in 6 files)

#### Latency benchmarks

| Benchmark | Topology | Message sizes |
|-----------|----------|---------------|
| Local round-trip latency | 1 broker + ping + echo (same host) | 32, 128, 512, 1024, 4096 B |
| Cross-broker round-trip latency | 2 brokers + ping on A + echo on B | 32, 128, 512, 1024, 4096 B |

#### Throughput benchmarks

| Benchmark | Topology | Message sizes |
|-----------|----------|---------------|
| Local throughput | 1 broker + ping + echo (same host) | 32, 256, 1024, 4096 B |
| Cross-broker throughput | 2 brokers + ping on A + echo on B | 32, 256, 1024, 4096, 16384 B |

#### Backpressure benchmarks

| Benchmark | Description |
|-----------|-------------|
| Onset detection (128B) | Escalating load: 1k → 100k messages |
| Onset detection (1024B) | Escalating load: 1k → 50k messages |
| Varying consumer delay | Sweep delay 0–10ms per message |
| Large payload (4096B) | Escalating load with 4KB messages |
| Sustained overload | 50k messages × 256B with 5ms consumer delay |

#### Recovery benchmarks

| Benchmark | Description |
|-----------|-------------|
| Service crash recovery | Crash → heartbeat timeout → replacement |
| Service kill recovery | SIGKILL → heartbeat timeout → replacement |
| Broker restart recovery | Stop broker B → restart → cross-broker messaging resumes |
| Local survives remote kill | Kill remote broker → local messaging intact |
| Rapid restart (5 iterations) | Repeated kill/restart cycles |

### Benchmark output format

Each benchmark writes a JSON result file:

```json
{
    "suite": "brz-perf",
    "scenario": "local-roundtrip-latency",
    "build_mode": "ReleaseFast",
    "message_size_bytes": 128,
    "warmup_messages": 10000,
    "measured_messages": 100000,
    "throughput_msgs_per_sec": 850000,
    "throughput_bytes_per_sec": 108800000,
    "latency_p50_ns": 4200,
    "latency_p95_ns": 7100,
    "latency_p99_ns": 9800,
    "latency_max_ns": 45100,
    "messages_sent": 100000,
    "messages_received": 100000,
    "send_failures": 0
}
```

---

## 4. Running Individual Tests

Zig's test runner does not support name-based filtering out of the box, but you
can rebuild individual test files for focused debugging:

```bash
# Run only the testing harness module tests
zig build test-testing

# Build e2e tests (compiles all scenarios; runner executes all)
zig build e2e

# Build perf benchmarks (compiles all benchmarks; runner executes all)
zig build perf
```

---

## 5. Comparing Performance Results

### Workflow for performance comparison

1. **Establish a baseline** — run `zig build perf` on the current main branch
   and record the results in `docs/benchmark-results.md`.

2. **Make changes** — implement the performance improvement.

3. **Re-run benchmarks** — run `zig build perf` again on the same machine under
   similar conditions (same load, no other heavy processes).

4. **Compare** — diff the new JSON results against the baseline. Update
   `docs/benchmark-results.md` with the new numbers and a note describing the
   change.

### Tips for reliable comparisons

- Always use the same machine and OS for before/after measurements.
- Close other CPU-intensive processes during benchmark runs.
- Run benchmarks multiple times and look for consistency.
- Compare percentiles (p50, p95, p99), not just averages.
- The `build_mode` field in results must always be `ReleaseFast`.

---

## 6. CI Integration

### Recommended tiers

| Tier | Trigger | Steps | Target runtime |
|------|---------|-------|----------------|
| 1 — Fast | Every PR | `zig build test` | < 1 min |
| 2 — E2E  | Merge to main / nightly | `zig build e2e` | minutes |
| 3 — Perf | Nightly / manual | `zig build perf` | minutes to tens of minutes |

### CI failure artifacts

On failure, upload:
- `logs/` — process stdout/stderr captures
- `config/` — generated config files
- `results/` — any partial JSON results
- Full test output

---

## 7. Architecture Reference

### Source layout

```text
src/
├── bin/                    # Executable entry points
│   ├── brz_broker_main.zig
│   ├── test_echo_service.zig
│   ├── test_ping_service.zig
│   ├── test_forwarder_service.zig
│   ├── test_leader_service.zig
│   ├── test_slow_consumer_service.zig
│   └── test_crashy_service.zig
├── e2e/                    # End-to-end correctness tests
│   ├── root.zig            # Test discovery entry point
│   ├── broker_startup_test.zig
│   ├── registration_test.zig
│   ├── local_ipc_test.zig
│   ├── discovery_test.zig
│   ├── cross_broker_test.zig
│   ├── fragmentation_test.zig
│   ├── heartbeat_timeout_test.zig
│   ├── restart_test.zig
│   ├── leader_election_test.zig
│   ├── graceful_unregister_test.zig
│   └── backpressure_test.zig
├── perf/                   # Performance benchmarks
│   ├── root.zig            # Benchmark discovery entry point
│   ├── local_latency_bench.zig
│   ├── local_throughput_bench.zig
│   ├── remote_latency_bench.zig
│   ├── remote_throughput_bench.zig
│   ├── backpressure_bench.zig
│   └── recovery_bench.zig
└── testing/                # Test harness library (brz_testing module)
    ├── root.zig
    ├── harness.zig         # TestHarness orchestrator
    ├── temp_env.zig        # Isolated temp workspace
    ├── config_gen.zig      # Broker/service config generation
    ├── process_runner.zig  # Process spawning and lifecycle
    ├── readiness.zig       # Readiness detection (log markers, file checks)
    ├── log_capture.zig     # Log capture and failure diagnostics
    ├── result_writer.zig   # JSON result file output
    └── benchmark_histogram.zig  # Latency histogram support
```

### Key build steps in `build.zig`

| Step | Description |
|------|-------------|
| `test` | All unit and integration tests (6 test suites) |
| `test-testing` | Testing harness module tests only |
| `test-bins` | Build all test service binaries |
| `e2e` | End-to-end correctness tests (depends on `test-bins` + broker exe) |
| `perf` | Performance benchmarks with `ReleaseFast` (depends on `test-bins` + broker exe) |
| `run` | Run the broker executable |
| `stat` | Run the `brz-stat` monitoring tool |

---

## 8. Current Status

As of the initial baseline, the broker runtime is not yet fully wired — the
broker process exits immediately on startup. This means:

- **Unit tests:** All **480 tests pass** ✅
- **E2E tests:** Compile and run, but **30/31 fail** at runtime (broker process
  exits before readiness). 1 test passes (compilation check). ⚠️
- **Perf benchmarks:** Compile and run, but **29/30 fail** at runtime (same
  root cause). 1 test passes (compilation check). ⚠️

Once the broker runtime startup is completed (see
`docs/impl/14-broker-binary-and-runtime-wiring.md`), these tests will begin
passing and producing real benchmark results.

See `docs/benchmark-results.md` for the current benchmark results baseline.
