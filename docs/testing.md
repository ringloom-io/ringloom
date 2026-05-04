# Testing Guide — RingLoom Broker

Instructions for running end-to-end correctness tests and performance benchmarks.
Intended for developers working on the library and for automated agents.

> **Design reference:** See `docs/impl/15-end-to-end-and-performance-testing.md`
> for the full rationale, test harness architecture, and scenario specifications.

---

## Prerequisites

| Tool | Version |
|------|---------|
| Zig  | 0.16.x  |
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
1. Creates an isolated temp workspace under `/tmp/ringloom-e2e-*`
2. Generates broker and service config files
3. Spawns processes from `zig-out/bin/`
4. Waits for readiness using log markers and file existence checks
5. Asserts on observable behavior
6. Tears down in reverse order
7. On failure: preserves the temp directory for inspection

### Debugging failed tests

If a test fails, the harness preserves the temp directory. Look for:

```text
/tmp/ringloom-e2e-<scenario>-<seq>/
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

### Option A: Automated harness (zig build)

```bash
zig build perf
```

> Performance benchmarks are compiled with `ReleaseFast` optimization. They are
> **never** included in the default `test` step. Note: the zig test runner
> suppresses stdout for passing tests, so result JSON files are not easily
> inspectable. Use the manual script (Option B) to capture results.

### Option B: Manual benchmark script (all sizes)

The `scripts/run-benchmarks.sh` script orchestrates brokers and services
directly, writing JSON result files to an output directory. It now captures
both:

- **paced transit latency** runs (`send_interval_ns=10000`) for meaningful
  end-to-end latency baselines
- **saturated benchmark** runs (`send_interval_ns=0`) for throughput and
  queueing behavior under load

```bash
# Build ReleaseFast binaries first
zig build install -Doptimize=ReleaseFast && zig build test-bins -Doptimize=ReleaseFast

# Run all benchmarks (local + cross-broker)
./scripts/run-benchmarks.sh

# Run only local (single-broker) benchmarks
./scripts/run-benchmarks.sh --local-only

# Run only cross-broker benchmarks
./scripts/run-benchmarks.sh --remote-only

# Custom output directory
./scripts/run-benchmarks.sh --output-dir ./my-results
```

Results are written to `/tmp/ringloom-bench-results/` by default.

- **Local** runs publish both ping and echo JSON, because the local throughput
  table intentionally uses the ping-side send rate as a same-host IPC metric.
- **Remote** runs publish only the echo-side JSON:
  - `remote-transit-latency-echo-<size>.json` — paced end-to-end one-way latency
  - `remote-saturated-benchmark-echo-<size>.json` — saturated queueing latency under load

Remote ping JSON is kept internal to the harness because those send-side values
only measure enqueue into broker A's local ring buffer, not actual cross-broker
transport behavior.

### Option C: Single-size benchmark (best-of-N)

The `scripts/bench-single-size.sh` script runs a single message size repeatedly
and keeps the best result. This is the recommended approach for getting clean
numbers, since you can close CPU-intensive applications (IDEs, agents, browsers)
and run each size in isolation.

By default, the single-size script now measures **transit latency**: the ping
service paces sends (`--send-interval-ns`, default 10,000 ns) to avoid building
an artificial steady-state queue. Use `--latency-mode saturated` to reproduce
the older **queueing latency** behavior where the sender runs flat out.

```bash
# Build ReleaseFast binaries first
zig build install -Doptimize=ReleaseFast && zig build test-bins -Doptimize=ReleaseFast

# Best-of-10 for 128 B unloaded cross-broker latency
./scripts/bench-single-size.sh 128 10

# Best-of-5 for 512 B saturated queueing latency
./scripts/bench-single-size.sh 512 5 --latency-mode saturated

# Best-of-5 for 32 B local IPC
./scripts/bench-single-size.sh 32 5 --local

# Custom output directory
./scripts/bench-single-size.sh 1024 5 --output-dir ./my-results
```

Best results are saved to `/tmp/ringloom-bench-best/` by default. The script prints
a summary with throughput, end-to-end latency percentiles, and—when the payload
is large enough for tracing (32 B+)—a stage breakdown for broker A queueing,
transport, and broker B local delivery.

**Tip:** For the most reliable results, run each size independently with all
other CPU-intensive processes stopped. Benchmark variance on a busy system can
be extreme — throughput can fluctuate 2–3× between runs due to background load.

The script tests message sizes: 32, 128, 512, 1024, and 4096 bytes.

### Interpreting results

- **End-to-end latency** (echo JSON): One-way latency from ping to echo,
  measured using monotonic timestamps embedded in the message payload.
  This is the primary latency metric.
- **Transit latency**: The paced single-size benchmark mode. This is the best
  approximation of unloaded broker-to-broker transit time.
- **Queueing latency**: The saturated benchmark mode. The sender intentionally
  outruns the end-to-end path, so the measured value includes backlog depth
  through broker A, TCP, broker B, and broker B → service handoff.
- **Send latency** (ping JSON): Time to write a message into the local
  broker's ring buffer. Lower bound on end-to-end latency.
- **Spinning backpressure**: With `--spin-timeout-ms`, the sender retries
  on `BufferFull` instead of counting a failure. The timestamp is
  re-embedded on each retry so latency excludes spin-wait time.
- **Stage breakdown** (echo JSON, 32 B+ payloads): Additional p50/p95/p99
  counters for broker A queueing, transport, and broker B delivery. Use these
  to tell whether a latency spike came from queue buildup before TCP, the
  cross-broker hop itself, or delivery after ingress.

### Benchmark categories (30 tests in 6 files)

#### Latency benchmarks

| Benchmark | Topology | Message sizes |
|-----------|----------|---------------|
| Local one-way latency | 1 broker + ping + echo (same host) | 32, 128, 512, 1024, 4096 B |
| Cross-broker send latency | 2 brokers + ping on A + echo on B | 32, 128, 512, 1024, 4096 B |

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
    "service_name": "echo",
    "total_received": 110000,
    "total_measured": 100000,
    "latency_p50_ns": 351,
    "latency_p95_ns": 481,
    "latency_p99_ns": 651,
    "latency_p99_9_ns": 24766,
    "latency_max_ns": 68046
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

## 5. Low-Latency System Tuning

To get reproducible, low-latency benchmark results the test machine must be
tuned at three levels: BIOS/firmware, kernel boot parameters, and runtime
settings.  The runtime layer is automated by `scripts/tune-system.sh`; the
other two require one-time manual configuration.

**Test machine reference:** GMKTec K8 Plus, 64 GB DDR5, AMD Ryzen 7 8845HS
(8 physical cores / 16 threads), Omarchy Linux, kernel 6.19, Limine bootloader.

> After all tuning is applied, run `sudo ./scripts/tune-system.sh --verify` to
> confirm the system state before benchmarking.

---

### 5.1 BIOS / Firmware Configuration

These settings must be changed in the BIOS/UEFI setup (typically press `DEL`
or `F2` during POST).  Exact menu locations vary by firmware version.

| Setting | Target | Why |
|---------|--------|-----|
| **SMT (Simultaneous Multi-Threading)** | **Disabled** | Eliminates logical-core contention.  With SMT off, cores 0–7 map 1:1 to physical cores. The CPU pinning layout below assumes SMT is disabled. |
| **C-States (C1E, C6, etc.)** | **Disabled** | Prevents the CPU from entering low-power idle states that add wake-up latency (10–100+ µs). |
| **Power Profile / Platform Power Management** | **Maximum Performance** | Locks all cores at maximum frequency. |
| **NUMA Interleaving** | **Enabled** (multi-socket only) | Not applicable to the single-socket 8845HS, but enable on multi-socket test machines to distribute memory accesses evenly. |

> **Note on P-States / CPPC:** Keep AMD CPPC **enabled** in BIOS.  The Linux
> `amd-pstate` driver requires CPPC to function.  Frequency is controlled at
> the OS level via the `performance` governor instead (applied by the tuning
> script).  Do **not** disable CPPC — that removes the kernel's ability to
> lock the frequency.

---

### 5.2 Kernel Boot Parameters (Limine)

These parameters are set once in the Limine bootloader configuration and take
effect on every boot.

Edit `/boot/limine/limine.conf` and append these to the `cmdline:` of your
Linux entry:

```
isolcpus=2-5 nohz_full=2-5 rcu_nocbs=2-5 processor.max_cstate=0
```

**Example Limine entry:**

```
:Omarchy Linux
    protocol: linux
    kernel_path: boot():/vmlinuz-linux
    kernel_cmdline: root=<your-root> rw isolcpus=2-5 nohz_full=2-5 rcu_nocbs=2-5 processor.max_cstate=0
    module_path: boot():/initramfs-linux.img
```

| Parameter | Effect |
|-----------|--------|
| `isolcpus=2-5` | Removes cores 2–5 from the general scheduler. Only explicitly pinned tasks run on them. Covers both local (2 cores) and remote (4 cores) benchmarks. |
| `nohz_full=2-5` | Disables the periodic timer tick on isolated cores (adaptive-tick mode), reducing jitter. |
| `rcu_nocbs=2-5` | Offloads RCU callback processing from isolated cores to housekeeping cores 0–1. |
| `processor.max_cstate=0` | Kernel-level C-state disable (belt-and-suspenders with BIOS setting). |

After editing, reboot and verify:

```bash
# Should show "2-5":
cat /sys/devices/system/cpu/isolated

# Should show "0":
cat /sys/devices/system/cpu/smt/active
```

> **Optional — extreme mode:** Adding `idle=poll` prevents the CPU from ever
> entering idle states, giving the absolute lowest wake-up latency.  However
> this causes very high power consumption and thermals on the mobile 8845HS,
> which can trigger thermal throttling under sustained load.  Only use if you
> measure an improvement for your specific workload.

---

### 5.3 Runtime Tuning Script

The `scripts/tune-system.sh` script applies all OS-level runtime settings that
do not require a reboot.

```bash
# Apply all runtime tuning (requires root):
sudo ./scripts/tune-system.sh

# Verify current settings (no root needed):
./scripts/tune-system.sh --verify

# Revert to original settings:
sudo ./scripts/tune-system.sh --revert
```

**What the script does:**

| Step | Detail |
|------|--------|
| Stop `irqbalance` | Prevents the irqbalance daemon from redistributing IRQs onto isolated cores. |
| CPU governor → `performance` | Locks all cores at maximum frequency (requires `amd-pstate` or `acpi-cpufreq`). |
| Disable turbo boost | Writes `0` to `/sys/devices/system/cpu/cpufreq/boost` for deterministic clock speeds. |
| Disable THP | Sets transparent huge pages to `never` — THP compaction causes unpredictable latency spikes. |
| `vm.swappiness=0` | Strongly discourages swapping, keeping shared-memory buffers resident. |
| `kernel.timer_migration=0` | Prevents timer callbacks from migrating to isolated cores. |
| Migrate IRQs | Moves IRQ affinity to housekeeping cores 0–1 (best-effort; some IRQs are non-writable). |

The script saves original settings to `/tmp/ringloom-tune-state.env` so that
`--revert` restores the exact prior state rather than guessing defaults.

---

### 5.4 CPU Core Assignment

With SMT disabled and `isolcpus=2-5`, the benchmark-critical cores are
partitioned.  Only the sender and receiver event loops are pinned — they
are the hot-path threads where cache locality and isolation matter most.
The control loop, test services (ping/echo), and the OS share the remaining
cores.

```
Core 0  ─── OS / interrupts / kernel work (housekeeping)
Core 1  ─── OS / interrupts / kernel work (housekeeping)
Core 2  ─── Broker: sender event loop   (pinned)
Core 3  ─── Broker: receiver event loop (pinned)
Core 4–7 ── Control loop, ping, echo, spare (unpinned)
```

> **Prerequisite:** This layout assumes SMT is disabled in BIOS.  With SMT
> enabled, cores 0–7 are logical cores sharing physical resources with siblings
> 8–15.  Pinning to logical cores on the same physical core defeats the purpose
> of isolation.

**Broker configuration** — set CPU affinity in `broker.properties`:

```properties
broker.sender.cpu.affinity=2
broker.receiver.cpu.affinity=3
```

The `run-benchmarks.sh` script already includes these settings in the generated
broker config files.  The control loop and test services are left unpinned.

**Two-broker layout** (cross-broker benchmarks, 4 pinned cores):

```
Core 0–1 ── OS / housekeeping
Core 2   ── Broker A: sender    (pinned)
Core 3   ── Broker A: receiver  (pinned)
Core 4   ── Broker B: sender    (pinned)
Core 5   ── Broker B: receiver  (pinned)
Core 6–7 ── Control loops, ping, echo (unpinned)
```

Broker A config:
```properties
broker.sender.cpu.affinity=2
broker.receiver.cpu.affinity=3
```

Broker B config:
```properties
broker.sender.cpu.affinity=4
broker.receiver.cpu.affinity=5
```

For the remote layout, update `isolcpus` accordingly:

```
isolcpus=2-5 nohz_full=2-5 rcu_nocbs=2-5 processor.max_cstate=0
```

---

### 5.5 Future Improvements

- **`mlock` / `mlockall`** — Pre-fault and lock shared-memory mapped regions
  to prevent page faults on the hot path.  The broker currently uses plain
  `mmap` without pre-touching; adding `mlockall(MCL_CURRENT | MCL_FUTURE)` at
  startup and raising the `memlock` ulimit would eliminate minor faults.
- **SCHED_FIFO** — Run broker threads under real-time scheduling for
  guaranteed preemption of other tasks.  Requires `kernel.sched_rt_runtime_us`
  tuning and careful priority assignment.
- **IRQ affinity at boot** — Use `irqaffinity=0-1` kernel parameter or a
  systemd unit to set IRQ affinity before any benchmark runs.

---

## 6. Comparing Performance Results

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

- Apply the full low-latency tuning stack (§5) before benchmarking.
- Always use the same machine and OS for before/after measurements.
- Close other CPU-intensive processes during benchmark runs.
- Run benchmarks multiple times and look for consistency.
- Compare percentiles (p50, p95, p99), not just averages.
- The `build_mode` field in results must always be `ReleaseFast`.

---

## 7. CI Integration

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

## 8. Architecture Reference

### Source layout

```text
scripts/
├── run-benchmarks.sh          # Manual benchmark orchestration script (all sizes)
└── bench-single-size.sh       # Single-size best-of-N benchmark script
src/
├── bin/                    # Executable entry points
│   ├── ringloom_broker_main.zig
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
│   ├── backpressure_bench.zig
│   └── recovery_bench.zig
└── testing/                # Test harness library (ringloom_testing module)
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
| `stat` | Run the `ringloom-stat` monitoring tool |

---

## 9. Current Status

- **Unit tests:** All **482 tests pass** ✅
- **E2E tests:** All pass ✅
- **Perf benchmarks:** All pass (exit code 0) ✅
- **Manual benchmarks:** `scripts/run-benchmarks.sh` captures JSON results
  for local and cross-broker scenarios.

See `docs/benchmark-results.md` for the current benchmark results baseline.
