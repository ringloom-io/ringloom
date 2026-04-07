# BRZ Follow-Up Implementation Guide — Modularization, Executables, and End-to-End Validation

This document series extends the existing BRZ Zig implementation plan with a second phase
focused on three shortcomings in the current codebase:

1. **Everything is currently bundled into one library**
   - broker runtime
   - service runtime
   - shared/common code
   - CLI entrypoints
   - test support
2. **The main binary does not actually start the broker**
   - `brz-broker` should boot the broker runtime and block until shutdown
3. **There is no proper end-to-end and performance validation plan**
   - multi-process broker/service tests
   - same-host and cross-host scenarios
   - failure and recovery scenarios
   - repeatable performance benchmarks

This follow-up guide defines the target package structure, executable model, test strategy,
and migration plan needed to address those issues without discarding the architecture or
the implementation work already captured in `docs/architecture.md` and `docs/impl`.

---

## Relationship to Existing Documents

The original implementation documents in `docs/impl` describe how to build the BRZ system
functionally. They are still valid for:

- shared-memory layout
- ring buffers
- TCP transport
- control plane
- threading model
- cluster management
- configuration and monitoring

However, those documents mostly describe the system from a **feature and subsystem**
perspective. They do not fully specify the **package boundaries**, **binary ownership**,
or **multi-process test harness** needed for a production-quality Zig project layout.

This new document set fills that gap.

Think of the original `docs/impl` set as:

- **how BRZ works**

And this new `docs/impl-next` set as:

- **how BRZ should be packaged, launched, tested, and evolved**

---

## Goals of This Follow-Up Phase

### 1. Split the monolith into clear Zig packages/modules

The implementation should be reorganized into separate libraries with explicit ownership:

- **common/core library**
  - shared memory primitives
  - protocol definitions
  - ring buffers
  - platform abstractions
  - configuration primitives shared by broker and services
- **broker library**
  - broker runtime
  - control loop
  - routing
  - cluster management
  - broker-specific config
- **service library**
  - service runtime / engine
  - service registration client logic
  - service-side control agent
  - service client registry
- **test support library**
  - process spawning helpers
  - temp directory / shm isolation
  - polling/assertion helpers
  - fixture builders
- **executables**
  - `brz-broker`
  - optional sample/demo services
  - optional admin/stat tools

The key requirement is that **broker and services are separate processes**, even though
they share common libraries.

### 2. Make `brz-broker` actually run the broker

The `brz-broker` executable must:

- load broker configuration
- initialize broker runtime
- start broker threads/event loops
- install signal handling
- block until shutdown
- perform graceful cleanup

It must stop being a placeholder binary that only prints diagnostics.

### 3. Add real end-to-end and performance testing

The project needs a dedicated test layer that validates:

- broker startup/shutdown
- service registration against a live broker
- same-host service-to-service messaging
- cross-host routing via multiple broker processes
- heartbeat timeout and cleanup
- leader election behavior
- restart/recovery behavior
- throughput and latency baselines

These tests must be designed as **multi-process tests**, not just in-memory integration
tests.

---

## Non-Goals

This document set does **not** redefine the BRZ architecture itself. It does not replace:

- the shared-memory file layouts
- the TCP wire protocol
- the ring buffer algorithm
- the control-plane message formats
- the threading model semantics

Instead, it constrains how those pieces are assembled into a maintainable Zig project.

---

## Design Principles for the New Document Set

The same hot-path invariants from the original implementation still apply:

- no allocations on hot path
- no locks on hot path
- no exceptions/panics for expected back-pressure conditions
- single-writer ownership of mutable counters
- zero-copy shared-memory IPC where applicable

In addition, this follow-up phase introduces several **packaging and operability**
principles:

### 1. Clear ownership boundaries

Every source file should belong conceptually to one of:

- common/core
- broker
- service
- tooling
- tests

If a file is imported by both broker and service, it belongs in common/core.

### 2. Executables are thin wrappers over runtimes

Binaries should contain only:

- argument/config parsing
- runtime construction
- startup/shutdown orchestration
- process lifecycle handling

All real behavior belongs in libraries.

### 3. Multi-process behavior must be testable

Anything that depends on process boundaries must be validated with actual spawned
processes, not only mocked or in-process simulations.

### 4. Test support is first-class code

The harness for end-to-end tests should be treated as a maintained subsystem, not as
throwaway scripts.

### 5. Migration should be incremental

The codebase should be able to move from the current single-library layout to the target
layout in stages, while preserving buildability and existing tests as much as possible.

---

## New Document Set

This follow-up series is organized around the missing concerns.

| Phase | Document | Description |
|:-----:|----------|-------------|
| 0 | `00-overview.md` | Overview, goals, target package map, migration order |
| 1 | `01-package-and-module-split.md` | How to split common, broker, service, tools, and test support into separate Zig modules/libraries |
| 2 | `02-broker-runtime-and-main-binary.md` | How the broker runtime should be exposed and how `brz-broker` should start and manage it |
| 3 | `03-service-runtime-and-process-model.md` | Service-side runtime boundaries, service executable patterns, and process lifecycle |
| 4 | `04-build-graph-and-artifact-layout.md` | `build.zig` restructuring for multiple libraries, executables, tests, and installable artifacts |
| 5 | `05-end-to-end-test-harness.md` | Multi-process test harness design, fixtures, temp storage isolation, and orchestration |
| 6 | `06-end-to-end-test-scenarios.md` | Concrete E2E scenarios: registration, local IPC, remote routing, heartbeats, leader election, restart |
| 7 | `07-performance-and-benchmarking.md` | Throughput/latency benchmarks, methodology, output format, and regression thresholds |
| 8 | `08-migration-plan.md` | Step-by-step refactor plan from current layout to target layout |

---

## Target Project Shape

The exact final directory names can vary, but the architecture should converge on a shape
like this:

```text
brz-broker/
├── build.zig
├── build.zig.zon
├── src/
│   ├── core/                 # shared/common runtime code
│   ├── broker/               # broker-only runtime code
│   ├── service/              # service-only runtime code
│   ├── tools/                # CLI tools and support binaries
│   ├── apps/                 # executable entrypoints
│   │   ├── brz_broker_main.zig
│   │   ├── sample_service_a.zig
│   │   └── sample_service_b.zig
│   ├── testing/              # reusable test harness support
│   └── root.zig              # optional umbrella exports, but not the only API surface
├── tests/
│   ├── e2e/
│   ├── perf/
│   └── fixtures/
└── docs/
    ├── impl/
    └── impl-next/
```

### Logical libraries

At the build graph level, the project should expose at least these logical modules:

- `brz_core`
- `brz_broker`
- `brz_service`
- `brz_test_support`

Optional:

- `brz_tools`
- `brz_samples`

### Why this split matters

This separation solves the current ambiguity where:

- broker code can accidentally depend on service-only code
- service code can accidentally import broker internals
- the main binary is not clearly tied to a broker runtime API
- tests have no dedicated place for process orchestration utilities

---

## Recommended Ownership Boundaries

### `brz_core`

Contains code shared by broker and services:

- platform abstraction
- memory mapping and metadata descriptors
- ring buffers and counters
- protocol headers and message encoding primitives
- shared configuration parsing helpers
- common IPC abstractions
- common monitoring primitives

This library must not depend on broker-only or service-only runtime code.

### `brz_broker`

Contains broker-only behavior:

- broker runtime/container
- control loop
- sender/receiver loops
- routing engine
- cluster management
- broker-specific config loading
- broker lifecycle API

This library may depend on `brz_core`, but not on `brz_service`.

### `brz_service`

Contains service-only behavior:

- service engine/runtime
- service registration flow
- service-side control agent
- message consumer runtime
- service client registry
- service lifecycle API

This library may depend on `brz_core`, but not on `brz_broker`.

### `brz_test_support`

Contains reusable testing helpers:

- process spawning
- temp directory allocation
- unique group/storage naming
- log capture
- polling/retry assertions
- fixture config generation
- benchmark result formatting helpers

This library may depend on `brz_core`, and optionally on public APIs of
`brz_broker`/`brz_service`, but should avoid depending on private internals.

---

## Executable Model

### `brz-broker`

This is the production broker process.

Responsibilities:

1. parse CLI args / environment / config path
2. load broker config
3. create broker runtime
4. start broker runtime
5. install signal handlers
6. wait for shutdown
7. stop broker runtime cleanly
8. return meaningful exit code

It should not contain broker business logic directly.

### Service executables

The framework should support service processes as separate binaries. There are two common
patterns:

1. **Application-owned service binaries**
   - each service has its own `main`
   - imports `brz_service`
   - registers handlers
   - starts service runtime
2. **Sample/demo binaries**
   - used for examples and E2E fixtures
   - live in `src/apps` or `examples`

The framework should not assume services run in-process with the broker.

---

## Testing Strategy Overview

The project should have three distinct test layers.

### 1. Unit tests

Purpose:

- validate isolated algorithms and data structures

Examples:

- ring buffer correctness
- protocol encode/decode
- config parsing
- leader election logic
- TCP message framing

These mostly already exist or are already planned.

### 2. In-process integration tests

Purpose:

- validate subsystem wiring without full process orchestration

Examples:

- broker runtime object startup/shutdown in a single process
- service engine startup against synthetic metadata
- command queue interactions
- control loop dispatch behavior

These are useful but insufficient for process-boundary correctness.

### 3. End-to-end multi-process tests

Purpose:

- validate the real deployment model

Examples:

- start broker process
- start one or more service processes
- exchange messages through shared memory
- verify registration/discovery
- verify remote routing with two brokers
- kill a service and verify heartbeat cleanup

This is the missing layer that this document set prioritizes.

---

## Performance Validation Overview

Performance tests should be separate from correctness tests.

### Correctness tests answer:

- does it work?

### Performance tests answer:

- how fast is it?
- how much jitter does it have?
- did a change regress throughput or latency?

The benchmark suite should include at least:

- same-host one-way latency
- same-host request/response latency
- same-host throughput
- cross-broker one-way latency
- cross-broker throughput
- registration storm / startup scaling
- back-pressure behavior under slow consumer conditions

Performance tests should produce machine-readable output so regressions can be tracked over
time.

---

## Key Problems in the Current State

### Problem 1: Single-library packaging

Current issue:

- all code is effectively treated as one package
- boundaries are implicit
- executable ownership is unclear
- testability of process roles is weaker

Impact:

- accidental coupling
- harder refactoring
- harder reuse
- harder to reason about public API vs internal implementation

### Problem 2: `main` is a placeholder

Current issue:

- the main binary prints startup text and exits
- it does not construct or run the broker runtime

Impact:

- no real broker process artifact
- no realistic E2E testing target
- no deployable binary behavior

### Problem 3: No dedicated E2E/perf document set

Current issue:

- some integration tests are described inside subsystem docs
- there is no unified multi-process harness design
- there is no benchmark methodology

Impact:

- process-boundary bugs can slip through
- restart/timeout behavior is under-specified
- performance claims are hard to validate

---

## What Success Looks Like

After implementing the `impl-next` series, the project should support the following
workflow:

### Broker

```text
zig build brz-broker
zig-out/bin/brz-broker --config path/to/broker.properties
```

Expected behavior:

- broker starts
- metadata files are created/opened
- event loops run
- process blocks until signal/shutdown

### Service

```text
zig build sample-service-a
zig-out/bin/sample-service-a --config path/to/service-a.properties
```

Expected behavior:

- service starts as its own process
- registers with broker
- begins heartbeat/control/message loops
- can send/receive messages

### End-to-end tests

```text
zig build test-e2e
```

Expected behavior:

- test harness creates isolated temp storage
- spawns broker and service processes
- waits for readiness
- runs assertions
- tears everything down cleanly

### Performance tests

```text
zig build perf
```

Expected behavior:

- benchmark scenarios run in controlled environments
- results are emitted in structured form
- optional thresholds can fail CI on regression

---

## Implementation Order

The recommended order for this follow-up phase is:

1. **Define package boundaries**
   - move shared code into core
   - isolate broker runtime API
   - isolate service runtime API
2. **Fix broker executable**
   - create a real broker lifecycle API
   - wire `brz-broker` to it
3. **Restructure build graph**
   - multiple modules
   - multiple executables
   - dedicated test steps
4. **Create test support library**
   - process harness
   - temp config generation
   - readiness polling
5. **Add E2E scenarios**
   - start with broker + one service
   - then local two-service path
   - then two-broker remote path
6. **Add performance suite**
   - baseline same-host
   - baseline cross-host
   - regression reporting
7. **Execute migration cleanup**
   - remove obsolete imports
   - tighten public APIs
   - document stable module boundaries

This order minimizes disruption because it establishes boundaries before adding more
behavior on top.

---

## Compatibility and Migration Guidance

This refactor should be done incrementally.

### Recommended migration rule

For a period of time, it is acceptable to have:

- old top-level re-export modules
- new structured modules underneath

But the direction should be one-way:

- new code imports the new module boundaries
- old compatibility exports exist only to ease transition

Eventually, the compatibility layer can be removed once the build graph and tests are
fully migrated.

---

## Deliverables of the `impl-next` Series

By the end of this document set, the implementation should have:

- a documented module split
- a documented broker runtime API
- a documented service runtime/process model
- a documented multi-artifact build graph
- a documented E2E harness
- a documented E2E scenario suite
- a documented performance benchmark suite
- a documented migration plan

---

## Audience

These documents are written for the engineer implementing or refactoring the Zig codebase.

They assume familiarity with:

- `docs/architecture.md`
- the existing `docs/impl` series
- Zig build graph concepts
- BRZ shared-memory and TCP architecture

They are intentionally implementation-oriented and should be used as a direct guide for
code and build-system changes.

---

## Reading Order

Read these documents in order:

1. `00-overview.md`
2. `01-package-and-module-split.md`
3. `02-broker-runtime-and-main-binary.md`
4. `03-service-runtime-and-process-model.md`
5. `04-build-graph-and-artifact-layout.md`
6. `05-end-to-end-test-harness.md`
7. `06-end-to-end-test-scenarios.md`
8. `07-performance-and-benchmarking.md`
9. `08-migration-plan.md`

---

## Summary

The original BRZ implementation documents explain the internals of the system well, but
the project now needs a second layer of specification focused on:

- **modular packaging**
- **real executable behavior**
- **multi-process validation**
- **performance measurement**
- **incremental migration**

That is the purpose of `docs/impl-next`.

The rest of this series turns those goals into concrete implementation guidance.

---

*Next: `01-package-and-module-split.md`*