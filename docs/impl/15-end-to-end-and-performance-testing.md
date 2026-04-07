# 15 — End-to-End & Performance Testing

> **Prerequisites:** This document builds on the implementation documents in
> `docs/impl`, especially:
> - `08-service-ipc.md`
> - `09-control-plane.md`
> - `10-threading-model.md`
> - `11-cluster-management.md`
> - `12-configuration-and-monitoring.md`
>
> It also assumes the packaging split described in the companion restructuring
> documents:
> - broker runtime as its own library and executable
> - service runtime as its own library
> - shared/common code extracted into a separate library
>
> This document defines the **test architecture**, **test harness**, **end-to-end
> scenarios**, **performance benchmarks**, and **CI execution model** needed to
> validate the BRZ Zig implementation as a real multi-process system rather than
> as a single in-process library.

---

## Table of Contents

1. [Overview](#1-overview)
2. [Goals and Non-Goals](#2-goals-and-non-goals)
3. [Testing Philosophy](#3-testing-philosophy)
4. [Test Pyramid for BRZ](#4-test-pyramid-for-brz)
5. [Required Build Outputs](#5-required-build-outputs)
6. [Test Workspace Layout](#6-test-workspace-layout)
7. [Test Harness Architecture](#7-test-harness-architecture)
   1. [Harness Responsibilities](#71-harness-responsibilities)
   2. [Process Model](#72-process-model)
   3. [Temporary Test Environment](#73-temporary-test-environment)
   4. [Readiness and Synchronization](#74-readiness-and-synchronization)
   5. [Log Capture and Failure Diagnostics](#75-log-capture-and-failure-diagnostics)
8. [Configuration Strategy for Tests](#8-configuration-strategy-for-tests)
9. [Test Service Binaries](#9-test-service-binaries)
   1. [Echo Service](#91-echo-service)
   2. [Ping Client Service](#92-ping-client-service)
   3. [Forwarder Service](#93-forwarder-service)
   4. [Leader-Aware Service](#94-leader-aware-service)
   5. [Slow Consumer Service](#95-slow-consumer-service)
   6. [Crashy Service](#96-crashy-service)
10. [End-to-End Test Scenarios](#10-end-to-end-test-scenarios)
    1. [Broker Startup and Shutdown](#101-broker-startup-and-shutdown)
    2. [Single Service Registration](#102-single-service-registration)
    3. [Two Local Services Direct IPC](#103-two-local-services-direct-ipc)
    4. [Service Discovery Updates](#104-service-discovery-updates)
    5. [Cross-Host Routing with Two Brokers](#105-cross-host-routing-with-two-brokers)
    6. [Fragmentation and Reassembly](#106-fragmentation-and-reassembly)
    7. [Heartbeat Timeout and Cleanup](#107-heartbeat-timeout-and-cleanup)
    8. [Service Restart and Metadata Reuse](#108-service-restart-and-metadata-reuse)
    9. [Leader Election for Services](#109-leader-election-for-services)
    10. [Broker Cluster Membership](#1010-broker-cluster-membership)
    11. [Backpressure Behavior](#1011-backpressure-behavior)
    12. [Graceful Unregister](#1012-graceful-unregister)
11. [Performance Test Suite](#11-performance-test-suite)
    1. [Performance Goals](#111-performance-goals)
    2. [Benchmark Categories](#112-benchmark-categories)
    3. [Measurement Rules](#113-measurement-rules)
    4. [Latency Benchmarks](#114-latency-benchmarks)
    5. [Throughput Benchmarks](#115-throughput-benchmarks)
    6. [Backpressure Benchmarks](#116-backpressure-benchmarks)
    7. [Recovery Benchmarks](#117-recovery-benchmarks)
12. [Metrics and Result Format](#12-metrics-and-result-format)
13. [Determinism, Timeouts, and Flake Prevention](#13-determinism-timeouts-and-flake-prevention)
14. [CI Execution Plan](#14-ci-execution-plan)
15. [Implementation Plan](#15-implementation-plan)
16. [Suggested File Structure](#16-suggested-file-structure)
17. [Appendix A: Example Harness APIs](#17-appendix-a-example-harness-apis)
18. [Appendix B: Example Zig Build Integration](#18-appendix-b-example-zig-build-integration)

---

## 1. Overview

The current implementation already contains unit and integration-style tests inside
individual documents and modules. Those are necessary, but they are not sufficient for
the system you actually want to ship.

BRZ is fundamentally a **multi-process distributed IPC system**:

- the broker is a standalone process
- each service is a standalone process
- same-host communication uses shared memory
- cross-host communication uses TCP between brokers
- lifecycle correctness depends on process startup, shutdown, crashes, heartbeats,
  metadata files, and inter-process timing

Because of that, the test strategy must validate the system at the same level it runs in
production: **separate OS processes with real shared-memory files and real TCP connections**.

This document introduces a dedicated testing layer with two major purposes:

1. **End-to-end correctness**
   - prove that broker and services work together as separate processes
   - validate registration, discovery, routing, heartbeats, leader election, and cleanup
   - verify behavior across both same-host and cross-host paths

2. **Performance characterization**
   - measure latency and throughput for the main message paths
   - quantify backpressure and recovery behavior
   - provide repeatable benchmark outputs for regression tracking

The result should be a testing system that answers questions like:

- Does `brz-broker` actually start the broker event loops?
- Can two services on the same host communicate without broker hot-path involvement?
- Can two brokers route messages across TCP correctly?
- Does a dead service get removed after heartbeat timeout?
- What is the p50/p99 latency for local IPC and cross-host routing?
- What throughput can the broker sustain before backpressure appears?

---

## 2. Goals and Non-Goals

### Goals

1. Validate the real runtime topology:
   - one broker process per host
   - one process per service instance

2. Exercise the real transport mechanisms:
   - shared memory
   - TCP connections
   - metadata files
   - heartbeats and timeouts

3. Provide a reusable harness for spawning and controlling:
   - brokers
   - services
   - synthetic test clients

4. Make failures diagnosable:
   - capture stdout/stderr
   - preserve temp directories on failure
   - dump counters and metadata state

5. Separate correctness tests from performance tests:
   - correctness tests must be stable and CI-friendly
   - performance tests may be longer-running and environment-sensitive

6. Support incremental growth:
   - start with a small set of deterministic scenarios
   - add more complex cluster and failure tests later

### Non-Goals

1. This document does **not** replace unit tests.
   Unit tests remain the fastest way to validate ring buffers, encoders, parsers, and
   small algorithms.

2. This document does **not** require a full external orchestration framework like
   Kubernetes or Docker.
   The first implementation should run entirely on one machine using multiple local
   processes.

3. This document does **not** attempt to simulate arbitrary packet loss at the OS level.
   If packet-loss simulation is needed later, it should be added as an optional advanced
   layer.

4. This document does **not** define production observability dashboards.
   It only defines the metrics and outputs needed for tests and benchmarks.

---

## 3. Testing Philosophy

The most important change is this:

> **Tests must validate BRZ as a system of binaries, not just as a collection of modules.**

That means:

- the broker executable must be launched by tests
- service executables must be launched by tests
- tests must wait for readiness using observable state
- tests must assert on externally visible behavior, not internal implementation details

The preferred order of confidence is:

1. **Unit tests**
   - fast
   - deterministic
   - narrow scope

2. **In-process integration tests**
   - validate module wiring
   - still useful for quick feedback

3. **Multi-process end-to-end tests**
   - validate the real runtime model
   - highest confidence for correctness

4. **Performance tests**
   - validate non-functional requirements
   - detect regressions over time

A bug that only appears when separate processes interact is exactly the kind of bug this
new layer is meant to catch.

---

## 4. Test Pyramid for BRZ

The recommended test pyramid is:

| Layer | Scope | Runtime | Frequency | Purpose |
|---|---|---:|---|---|
| Unit | single module/function | milliseconds | every change | correctness of small components |
| Integration | multiple modules in one process | milliseconds to seconds | every change | wiring and API behavior |
| End-to-end | multiple real processes | seconds | every PR / merge | runtime correctness |
| Performance | multiple real processes under load | seconds to minutes | scheduled / manual / release | regression tracking and capacity |

### Expected distribution

- **70%** unit tests
- **20%** integration tests
- **10%** end-to-end and performance tests

The exact percentages are not strict, but the principle matters:
end-to-end tests are expensive and should cover the most important workflows, not every
small branch.

---

## 5. Required Build Outputs

To support this test strategy, the build must produce distinct artifacts.

### Required executables

1. `brz-broker`
   - the real broker process
   - must start broker lifecycle, event loops, sockets, and metadata

2. `brz-test-echo-service`
   - registers with broker
   - receives messages
   - optionally replies

3. `brz-test-ping-service`
   - sends requests to another service
   - records latency and success/failure counts

4. `brz-test-forwarder-service`
   - receives a message and forwards it to another service

5. `brz-test-leader-service`
   - participates in leader election
   - reports leader changes

6. `brz-test-slow-consumer-service`
   - intentionally consumes slowly to trigger backpressure

7. `brz-test-crashy-service`
   - registers, optionally sends/receives some messages, then exits abruptly

8. `brz-test-harness`
   - optional standalone executable for orchestrating scenarios
   - alternatively, scenarios can be implemented as Zig tests that spawn processes

### Required libraries

1. `brz-common`
   - shared protocol, memory layout, ring buffer, config parsing, platform abstractions

2. `brz-broker-lib`
   - broker runtime and lifecycle API

3. `brz-service-lib`
   - service runtime and client API

This split matters because the tests should link only what they need:
test services should depend on `brz-service-lib` and `brz-common`, not on broker-only
implementation internals.

---

## 6. Test Workspace Layout

All end-to-end tests must run in an isolated temporary workspace.

### Per-test workspace

Each scenario gets its own root directory, for example:

`zig-out/tmp/e2e/<test-name>-<timestamp-or-seq>/`

Inside it:

- `storage/`
  - fake shared-memory root used instead of `/dev/shm`
- `logs/`
  - stdout/stderr capture for each process
- `config/`
  - generated broker and service properties files
- `results/`
  - structured outputs such as JSON summaries
- `artifacts/`
  - optional copied metadata snapshots on failure

### Why not use `/dev/shm` directly?

Using a test-local storage root is better because it:

- avoids collisions between concurrent tests
- avoids polluting the real machine-wide shared-memory namespace
- makes cleanup deterministic
- allows preserving artifacts for debugging

The implementation should still use memory-mapped files; only the base path changes.

---

## 7. Test Harness Architecture

The harness is the core of this document.

It is responsible for creating a realistic but controlled environment for multi-process
tests.

## 7.1 Harness Responsibilities

The harness must:

1. create a temporary workspace
2. generate config files for brokers and services
3. spawn broker and service processes
4. capture stdout/stderr for each process
5. wait for readiness conditions
6. drive scenario actions
7. collect results and counters
8. enforce timeouts
9. terminate processes cleanly or forcibly
10. preserve diagnostics on failure

## 7.2 Process Model

Each spawned process should be represented by a small control object:

- process name
- executable path
- pid / child handle
- config path
- stdout log path
- stderr log path
- readiness state
- exit status

Suggested process categories:

- `BrokerProcess`
- `ServiceProcess`
- `ScenarioClientProcess` or in-harness client actor

### Process lifecycle states

Each process should move through:

1. `created`
2. `spawned`
3. `ready`
4. `stopping`
5. `exited`

The harness should never assume a process is ready immediately after spawn.

## 7.3 Temporary Test Environment

For each scenario, the harness should generate:

- broker config file(s)
- service config file(s)
- unique ports
- unique storage path
- unique group name

### Port allocation

Use deterministic local ports where possible, for example:

- broker 1 TCP port: `19001`
- broker 2 TCP port: `19002`
- broker 3 TCP port: `19003`

For parallel test execution, either:

1. allocate ports dynamically from a reserved range, or
2. serialize the end-to-end suite

The simpler first version is to run end-to-end tests serially.

## 7.4 Readiness and Synchronization

Readiness must be based on observable conditions, not sleeps alone.

### Broker readiness

A broker is considered ready when all of the following are true:

1. process is alive
2. broker metadata file exists
3. control buffer is initialized
4. TCP socket listen completed
5. startup log contains a readiness marker such as:
   - `broker started`
   - `control loop running`
   - `routing loop running`

The implementation should add an explicit readiness log line to make this reliable.

### Service readiness

A service is considered ready when:

1. process is alive
2. service metadata file exists
3. registration response has been received
4. service logs a readiness marker such as:
   - `service registered`
   - `service ready`

### Scenario synchronization

The harness should provide helpers like:

- `waitForBrokerReady(timeout_ms)`
- `waitForServiceReady(timeout_ms)`
- `waitForLogLine(process, needle, timeout_ms)`
- `waitForMetadataFile(path, timeout_ms)`
- `waitForCondition(fn, timeout_ms, poll_interval_ms)`

## 7.5 Log Capture and Failure Diagnostics

Every process must have separate stdout and stderr capture files.

On failure, the harness should print a concise summary:

- scenario name
- timed-out step
- process statuses
- last N lines of each log
- relevant metadata file paths
- counters snapshot if available

The harness should preserve the temp directory on failure and delete it on success by
default.

Recommended failure dump:

- last 200 lines of broker logs
- last 200 lines of service logs
- broker counters snapshot
- directory listing of storage root
- process exit codes
- elapsed time per scenario step

---

## 8. Configuration Strategy for Tests

Tests should generate config files rather than relying on checked-in static files.

### Broker config template

Each broker config should include at least:

- `broker.node.id`
- `broker.local.host.port`
- `broker.member.host.ports`
- `broker.group.name`
- `broker.storage.path`
- `broker.control.buffer.size`
- `broker.messages.buffer.size`
- `broker.tcp.send.buffer.size`
- `broker.tcp.recv.buffer.size`
- `broker.threading.mode`
- `broker.idle.strategy`

### Service config template

Each service config should include at least:

- `service.name`
- `service.group.name`
- `service.storage.path`
- `service.control.buffer.size`
- `service.messages.buffer.size`
- `service.blocking.mode`
- `service.leader_election.enabled`

### Test-specific overrides

Scenarios should be able to override:

- heartbeat timeout
- buffer sizes
- max frame length
- threading mode
- idle strategy
- leader election enabled
- artificial delays for test services

### Recommended defaults for correctness tests

Use small but safe values:

- control buffer: `64 KB`
- messages buffer: `1 MB`
- TCP send buffer: `256 KB`
- TCP recv buffer: `256 KB`
- max frame length: `65536`
- threading mode: `DEDICATED`
- idle strategy: `backoff`

### Recommended defaults for performance tests

Use production-like values and pin them in the benchmark output.

---

## 9. Test Service Binaries

The end-to-end suite should use a small set of purpose-built service binaries.

These binaries should be simple, deterministic, and configurable via command-line flags
or properties files.

## 9.1 Echo Service

### Purpose

Validate registration, discovery, local IPC, remote routing, and request/response flow.

### Behavior

- registers as service name `echo`
- waits for messages
- replies with the same payload or a small transformed payload
- logs:
  - registration success
  - message count
  - reply count

### Configurable options

- max messages before exit
- reply delay
- payload validation mode

## 9.2 Ping Client Service

### Purpose

Drive traffic and record latency.

### Behavior

- registers as service name `ping`
- creates a client for `echo`
- sends N messages
- waits for N replies
- records:
  - sent count
  - received count
  - timeout count
  - latency histogram

### Output

At the end, writes a JSON result file with:

- scenario name
- message size
- message count
- p50/p95/p99/max latency
- throughput
- failures

## 9.3 Forwarder Service

### Purpose

Validate multi-hop service interactions and service discovery updates.

### Behavior

- registers as `forwarder`
- on message, forwards to `echo`
- optionally returns the downstream response to original sender

This is useful for proving that service-side client creation and discovery updates work
while the process is already running.

## 9.4 Leader-Aware Service

### Purpose

Validate service leader election and leader change notifications.

### Behavior

- registers with leader election enabled
- logs whether it is leader
- logs every leader change event
- optionally writes a small status file for the harness to inspect

## 9.5 Slow Consumer Service

### Purpose

Trigger backpressure and buffer saturation.

### Behavior

- registers normally
- intentionally sleeps or rate-limits message handling
- exposes counters:
  - messages received
  - messages delayed
  - queue full observations if visible

## 9.6 Crashy Service

### Purpose

Validate heartbeat timeout, stale metadata handling, and restart behavior.

### Behavior

- registers successfully
- optionally sends or receives a few messages
- exits abruptly without graceful unregister

The harness then verifies broker cleanup behavior.

---

## 10. End-to-End Test Scenarios

Each scenario below should be implemented as a separate test case with its own workspace.

## 10.1 Broker Startup and Shutdown

### Purpose

Verify that the `brz-broker` binary actually starts the broker runtime.

### Steps

1. Generate broker config
2. Spawn `brz-broker`
3. Wait for broker readiness marker
4. Verify broker metadata file exists
5. Verify process remains alive for a short steady-state window
6. Request shutdown or terminate gracefully
7. Verify clean exit

### Assertions

- broker process starts successfully
- readiness marker appears
- metadata file exists
- no immediate crash
- exit code is zero on graceful shutdown

### This scenario directly addresses

> “The main binary, `brz-broker`, does not start the broker.”

If this test fails, the binary wiring is still wrong.

## 10.2 Single Service Registration

### Purpose

Verify that one service can register with a running broker.

### Steps

1. Start broker
2. Start echo service
3. Wait for service readiness
4. Inspect broker logs or counters
5. Optionally inspect service metadata

### Assertions

- service receives registration response
- broker registry contains one local service
- service remains alive
- no error logs indicating registration failure

## 10.3 Two Local Services Direct IPC

### Purpose

Validate same-host direct service-to-service communication.

### Steps

1. Start broker
2. Start echo service
3. Start ping service
4. Wait for both services to be ready
5. Ping sends N messages to echo
6. Echo replies
7. Ping writes result file

### Assertions

- all N messages succeed
- no broker-routed remote path is used
- latency remains within expected local IPC bounds
- no dropped messages

### Notes

This is the most important same-host happy-path scenario.

## 10.4 Service Discovery Updates

### Purpose

Verify subscription and instance update flow.

### Steps

1. Start broker
2. Start ping service
3. Ping subscribes to `echo`
4. Start first echo service
5. Verify ping sees one instance
6. Start second echo service
7. Verify ping sees two instances
8. Stop one echo service
9. Verify ping sees one remaining instance

### Assertions

- instance list updates are delivered
- removals are reflected
- no stale instance remains after graceful stop

## 10.5 Cross-Host Routing with Two Brokers

### Purpose

Validate real TCP routing between brokers.

### Topology

- broker A on `127.0.0.1:19001`
- broker B on `127.0.0.1:19002`
- ping service attached to broker A
- echo service attached to broker B

### Steps

1. Start broker A
2. Start broker B
3. Wait for cluster readiness
4. Start echo on broker B
5. Start ping on broker A
6. Ping sends N messages to echo
7. Echo replies through broker B back to broker A
8. Collect results

### Assertions

- all N messages succeed
- remote routing headers are correct
- both brokers remain healthy
- no unexpected connection drops or timeouts in the happy path

## 10.6 Large Frame Handling

### Purpose

Validate messages up to `max_frame_length`.

### Steps

1. Start two brokers or one broker depending on path under test
2. Start sender and receiver services
3. Send payloads of various sizes up to `max_frame_length`
4. Receiver validates payload checksum

### Assertions

- full payload arrives intact
- no corruption
- frames exceeding `max_frame_length` are rejected by sender

### Suggested payload sizes

- `2 * mtu`
- `8 KB`
- `64 KB`

## 10.7 Heartbeat Timeout and Cleanup

### Purpose

Verify dead service detection.

### Steps

1. Start broker
2. Start crashy service
3. Wait for registration success
4. Kill service abruptly
5. Wait longer than heartbeat timeout
6. Observe broker cleanup

### Assertions

- broker removes dead service
- subscribers receive updated instance list
- stale service is not routed to after timeout

## 10.8 Service Restart and Metadata Reuse

### Purpose

Verify restart behavior after crash.

### Steps

1. Start broker
2. Start crashy service
3. Wait for registration
4. Kill service abruptly
5. Wait for cleanup
6. Restart same service name
7. Verify new registration succeeds

### Assertions

- stale metadata does not block restart
- broker sees new live instance
- service can send/receive again

## 10.9 Leader Election for Services

### Purpose

Validate per-service leader election.

### Steps

1. Start broker
2. Start leader-aware service instance A
3. Start leader-aware service instance B
4. Verify one leader is chosen
5. Stop current leader
6. Verify follower becomes leader

### Assertions

- exactly one leader at a time
- leader change notification is delivered
- leader state converges after removal

## 10.10 Broker Cluster Membership

### Purpose

Validate broker-to-broker membership and cluster state propagation.

### Steps

1. Start broker A
2. Start broker B
3. Wait for membership convergence
4. Optionally start broker C
5. Verify all brokers observe the same cluster state

### Assertions

- peers are discovered
- leader election converges
- membership changes propagate

## 10.11 Backpressure Behavior

### Purpose

Validate behavior when a consumer is slower than producers.

### Steps

1. Start broker
2. Start slow consumer service
3. Start ping/load generator service
4. Send messages faster than consumer can process
5. Observe counters and outcomes

### Assertions

- backpressure is observable
- producer sees expected failure or retry behavior
- system remains stable
- no corruption or deadlock

## 10.12 Graceful Unregister

### Purpose

Verify clean service shutdown path.

### Steps

1. Start broker
2. Start echo service
3. Wait for registration
4. Stop echo service gracefully
5. Observe broker state

### Assertions

- unregister message is processed
- service removed promptly
- subscribers updated without waiting for heartbeat timeout

---

## 11. Performance Test Suite

Performance tests should be separate from correctness tests in both code organization and
build steps.

## 11.1 Performance Goals

The suite should answer:

- What is local IPC round-trip latency?
- What is cross-broker round-trip latency?
- What is max sustainable throughput for fixed message sizes?
- At what point does backpressure begin?
- How long does recovery take after service or broker failure?

The suite should detect regressions, not just produce one-off numbers.

## 11.2 Benchmark Categories

1. **Local IPC latency**
2. **Local IPC throughput**
3. **Cross-broker latency**
4. **Cross-broker throughput**
5. **Fragmentation overhead**
6. **Backpressure onset**
7. **Recovery time**

## 11.3 Measurement Rules

To keep results meaningful:

1. Warm up before measuring
2. Use monotonic clock only
3. Pin message size and count in output
4. Report percentiles, not just averages
5. Separate setup time from measurement time
6. Run multiple iterations and report min/median/max across runs
7. Record machine and build mode metadata

### Warm-up

Each benchmark should have:

- warm-up phase: e.g. 10,000 messages
- measurement phase: e.g. 100,000 or 1,000,000 messages

### Build mode

Benchmarks should run in optimized mode only, typically `ReleaseFast`.

## 11.4 Latency Benchmarks

### Local round-trip latency

Topology:

- one broker
- ping service
- echo service
- same host

Measure:

- p50
- p95
- p99
- p99.9 if sample size is large enough
- max

Message sizes:

- 32 B
- 128 B
- 512 B
- 1 KB
- 4 KB

### Cross-broker round-trip latency

Topology:

- two brokers on loopback
- ping on broker A
- echo on broker B

Same metrics and message sizes.

## 11.5 Throughput Benchmarks

### Local throughput

Measure messages/sec and bytes/sec for:

- one producer, one consumer
- multiple producers to one service if supported by scenario design

### Cross-broker throughput

Measure:

- end-to-end messages/sec
- broker send/receive counters
- no connection drops or queue overflow in happy path

Suggested message sizes:

- 32 B
- 256 B
- 1 KB
- 4 KB
- 16 KB

## 11.6 Backpressure Benchmarks

### Purpose

Find the point where the system transitions from healthy flow to sustained pressure.

### Method

1. Fix consumer speed
2. Increase producer rate
3. Record:
   - send failures
   - retries
   - queue occupancy if available
   - latency inflation
   - throughput plateau

### Output

A table like:

| offered rate | achieved rate | p99 latency | send failures | notes |
|---:|---:|---:|---:|---|
| 100k msg/s | 100k msg/s | 80 us | 0 | healthy |
| 200k msg/s | 198k msg/s | 140 us | 12 | mild pressure |
| 300k msg/s | 220k msg/s | 2.1 ms | 4,200 | saturated |

## 11.7 Recovery Benchmarks

### Service recovery time

Measure time from:

- service crash
- broker detection
- registry update
- successful replacement service registration

### Broker reconnection time

Measure time from:

- broker B restart
- broker A detecting peer return
- successful cross-broker message flow resuming

---

## 12. Metrics and Result Format

All performance tests should emit structured result files.

### Recommended format

JSON per benchmark run:

```json
{
  "suite": "brz-perf",
  "scenario": "local-roundtrip-latency",
  "timestamp_utc": "2026-01-01T12:00:00Z",
  "build_mode": "ReleaseFast",
  "machine": {
    "os": "linux",
    "arch": "x86_64"
  },
  "topology": {
    "brokers": 1,
    "services": 2
  },
  "message_size_bytes": 128,
  "warmup_messages": 10000,
  "measured_messages": 100000,
  "results": {
    "throughput_msgs_per_sec": 850000,
    "throughput_bytes_per_sec": 108800000,
    "latency_ns": {
      "p50": 4200,
      "p95": 7100,
      "p99": 9800,
      "max": 45100
    }
  },
  "counters": {
    "messages_sent": 100000,
    "messages_received": 100000,
    "send_failures": 0,
    "tcp_reconnects": 0
  }
}
```

### Correctness result format

Correctness scenarios can emit a smaller summary:

- scenario name
- pass/fail
- elapsed time
- process exit codes
- key counters
- failure reason if any

---

## 13. Determinism, Timeouts, and Flake Prevention

End-to-end tests are vulnerable to flakes unless designed carefully.

### Rules

1. Prefer readiness markers over fixed sleeps
2. Use generous but bounded timeouts
3. Run scenarios serially at first
4. Use loopback networking only
5. Use isolated storage roots
6. Keep correctness workloads small
7. Preserve artifacts on failure

### Suggested default timeouts

- broker ready: `5 s`
- service ready: `5 s`
- cluster convergence: `10 s`
- heartbeat cleanup: `heartbeat_timeout + 5 s`
- scenario hard timeout: `30 s`

### Avoid

- asserting on exact timing in correctness tests
- large message counts in correctness tests
- depending on log ordering across processes
- sharing ports or storage paths between tests

---

## 14. CI Execution Plan

The suite should be split into tiers.

### Tier 1: fast tests

Run on every PR:

- unit tests
- in-process integration tests
- minimal end-to-end smoke tests:
  - broker startup/shutdown
  - single service registration
  - two local services direct IPC

Target runtime: under a few minutes.

### Tier 2: full end-to-end suite

Run on merge to main and nightly:

- all correctness scenarios
- two-broker routing
- heartbeat timeout
- leader election
- restart scenarios

### Tier 3: performance suite

Run nightly or manually:

- latency benchmarks
- throughput benchmarks
- backpressure benchmarks
- recovery benchmarks

Performance results should be archived for trend comparison.

### CI failure artifacts

Always upload on failure:

- logs
- generated configs
- result JSON
- preserved temp workspace

---

## 15. Implementation Plan

Implement this in phases.

### Phase 1 — smoke coverage

1. Make `brz-broker` start the real broker runtime
2. Add readiness log markers
3. Build one test service: echo
4. Build one harness
5. Implement:
   - broker startup/shutdown
   - single service registration
   - two local services direct IPC

### Phase 2 — discovery and failure handling

1. Add ping service
2. Add crashy service
3. Implement:
   - service discovery updates
   - graceful unregister
   - heartbeat timeout and cleanup
   - restart after crash

### Phase 3 — cross-broker routing

1. Add two-broker harness support
2. Implement:
   - cross-host routing
   - fragmentation/reassembly
   - broker cluster membership

### Phase 4 — leader election

1. Add leader-aware service
2. Implement:
   - service leader election
   - leader failover

### Phase 5 — performance suite

1. Add benchmark result JSON
2. Add latency histogram support
3. Implement:
   - local latency
   - remote latency
   - throughput
   - backpressure
   - recovery timing

---

## 16. Suggested File Structure

Suggested additions under `src/` and `test/`:

```text
src/
  apps/
    brz_broker_main.zig
    test_echo_service_main.zig
    test_ping_service_main.zig
    test_forwarder_service_main.zig
    test_leader_service_main.zig
    test_slow_consumer_service_main.zig
    test_crashy_service_main.zig

  testing/
    harness.zig
    process_runner.zig
    temp_env.zig
    config_gen.zig
    readiness.zig
    log_capture.zig
    result_writer.zig
    benchmark_histogram.zig

test/
  e2e/
    broker_startup_test.zig
    registration_test.zig
    local_ipc_test.zig
    discovery_test.zig
    cross_broker_test.zig
    fragmentation_test.zig
    heartbeat_timeout_test.zig
    restart_test.zig
    leader_election_test.zig
    graceful_unregister_test.zig

  perf/
    local_latency_bench.zig
    local_throughput_bench.zig
    remote_latency_bench.zig
    remote_throughput_bench.zig
    backpressure_bench.zig
    recovery_bench.zig
```

If you prefer to keep everything under `src/`, the same logical split still applies.

---

## 17. Appendix A: Example Harness APIs

The exact API shape can vary, but the harness should expose something close to this:

```zig
pub const TestHarness = struct {
    allocator: std.mem.Allocator,
    workspace: Workspace,

    pub fn init(allocator: std.mem.Allocator) !TestHarness;
    pub fn deinit(self: *TestHarness) void;

    pub fn createBrokerConfig(self: *TestHarness, spec: BrokerSpec) ![]const u8;
    pub fn createServiceConfig(self: *TestHarness, spec: ServiceSpec) ![]const u8;

    pub fn startBroker(self: *TestHarness, spec: BrokerSpec) !BrokerHandle;
    pub fn startService(self: *TestHarness, spec: ServiceSpec) !ServiceHandle;

    pub fn waitForBrokerReady(self: *TestHarness, broker: *BrokerHandle, timeout_ms: u64) !void;
    pub fn waitForServiceReady(self: *TestHarness, service: *ServiceHandle, timeout_ms: u64) !void;

    pub fn stopProcess(self: *TestHarness, proc: *ProcessHandle) !void;
    pub fn killProcess(self: *TestHarness, proc: *ProcessHandle) !void;

    pub fn preserveArtifactsOnFailure(self: *TestHarness) void;
};
```

### Broker spec

```zig
pub const BrokerSpec = struct {
    node_id: u8,
    host: []const u8 = "127.0.0.1",
    port: u16,
    peers: []const PeerSpec = &.{},
    threading_mode: ThreadingMode = .dedicated,
    idle_strategy: IdleStrategyName = .backoff,
    storage_path_override: ?[]const u8 = null,
};
```

### Service spec

```zig
pub const ServiceSpec = struct {
    executable_name: []const u8,
    service_name: []const u8,
    broker_node_id: u8,
    group_name: []const u8 = "brz-test",
    leader_election_enabled: bool = false,
    extra_args: []const []const u8 = &.{},
};
```

---

## 18. Appendix B: Example Zig Build Integration

The build should expose separate steps for correctness and performance.

### Recommended build steps

- `zig build test`
  - unit and integration tests

- `zig build e2e`
  - end-to-end correctness suite

- `zig build perf`
  - performance benchmarks

- `zig build test-services`
  - build all helper service binaries

### Build graph expectations

1. `e2e` depends on:
   - broker executable
   - required test service executables
   - harness code

2. `perf` depends on:
   - broker executable
   - ping/echo services
   - benchmark support code

3. correctness and performance outputs should be separated:
   - `zig-out/e2e-results/`
   - `zig-out/perf-results/`

### Important rule

Performance benchmarks must not run as part of the default `test` step.

They are too environment-sensitive and too slow for normal edit-compile-test loops.

---

## Summary

This document adds the missing testing layer needed for BRZ to behave like a real
multi-process IPC system.

The key decisions are:

1. test the broker and services as **separate processes**
2. use a dedicated **test harness** with isolated temp workspaces
3. add a small set of **purpose-built test service binaries**
4. implement a focused **end-to-end correctness suite**
5. implement a separate **performance benchmark suite**
6. integrate both into the build and CI as distinct tiers

Most importantly, this document gives you a direct path to fixing the current
shortcomings:

- the broker binary will be validated by a startup smoke test
- the split between broker and service runtimes will be exercised by real process
  boundaries
- end-to-end and performance coverage will become first-class parts of the project

*Next recommended companion documents:*
- `13-library-and-package-split.md`
- `14-broker-binary-and-runtime-wiring.md`
