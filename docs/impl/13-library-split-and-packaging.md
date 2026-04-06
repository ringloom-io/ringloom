# 13 — Library Split & Packaging

> **Prerequisites:** [00 — Overview](../impl/00-overview.md), [08 — Service ↔ Broker IPC](../impl/08-service-ipc.md), [09 — Control Plane](../impl/09-control-plane.md), [10 — Threading Model](../impl/10-threading-model.md), [12 — Configuration & Monitoring](../impl/12-configuration-and-monitoring.md)
>
> This document defines how to split the current single-library Zig implementation into separate libraries, executables, and test targets so that:
>
> 1. broker code is packaged independently from service code,
> 2. shared/common code is reusable without dragging in the whole broker,
> 3. the `brz-broker` binary actually starts the broker process,
> 4. end-to-end and performance testing can be built on top of the packaged artifacts.

This document is intentionally packaging-focused. It does **not** redefine the runtime architecture from `architecture.md`; instead, it maps that architecture onto a cleaner Zig project structure.

---

## Table of Contents

1. [Goals](#1-goals)
2. [Current Problems](#2-current-problems)
3. [Target Package Model](#3-target-package-model)
   1. [Package Boundaries](#31-package-boundaries)
   2. [Dependency Rules](#32-dependency-rules)
   3. [Process Model](#33-process-model)
4. [Target Repository Layout](#4-target-repository-layout)
5. [Library Specifications](#5-library-specifications)
   1. [`brz-common`](#51-brz-common)
   2. [`brz-service`](#52-brz-service)
   3. [`brz-broker`](#53-brz-broker)
6. [Executable Specifications](#6-executable-specifications)
   1. [`brz-broker`](#61-brz-broker-executable)
   2. [Test Service Executables](#62-test-service-executables)
   3. [Utility Executables](#63-utility-executables)
7. [Public API Boundaries](#7-public-api-boundaries)
8. [Build System Specification](#8-build-system-specification)
   1. [Modules and Artifacts](#81-modules-and-artifacts)
   2. [Build Steps](#82-build-steps)
   3. [Install Layout](#83-install-layout)
9. [Broker Startup Specification](#9-broker-startup-specification)
10. [Migration Plan](#10-migration-plan)
11. [Acceptance Criteria](#11-acceptance-criteria)
12. [Testing Impact](#12-testing-impact)
13. [Implementation Notes](#13-implementation-notes)

---

## 1. Goals

The packaging split must satisfy these goals:

- **Separate broker runtime from service runtime.**
  The broker is a standalone process. Services are separate processes that communicate with the broker through shared memory and, for cross-host traffic, through broker-to-broker UDP transport.

- **Keep shared code reusable.**
  Ring buffers, metadata files, message headers, control message codecs, platform abstractions, and common configuration helpers should live in a shared library that both broker and services can import.

- **Avoid accidental broker dependencies in services.**
  A service should not need sender, receiver, cluster, or broker control-loop code linked into its binary.

- **Make the main broker binary real.**
  Running `zig build run` or invoking the installed `brz-broker` executable must create the broker runtime, start its event loops, and block until shutdown.

- **Enable realistic end-to-end tests.**
  Tests must be able to launch a broker process and one or more service processes as separate OS processes.

- **Enable performance testing.**
  Benchmarks should be able to measure same-host IPC, broker-routed local traffic, and cross-host broker traffic with minimal harness overhead.

---

## 2. Current Problems

The current implementation has three structural shortcomings:

### 2.1 Single-library packaging

The codebase currently exposes one top-level module and one main executable, while the source tree already contains broker-oriented and service-oriented code side by side.

This causes several issues:

- service binaries can accidentally import broker-only internals,
- broker binaries can depend on service-only convenience APIs,
- compile times and binary size grow unnecessarily,
- public API boundaries are unclear,
- end-to-end process composition is harder to reason about.

### 2.2 Main binary is not a broker launcher

The current `src/main.zig` behaves like a placeholder and only prints startup text plus a clock check. It does not:

- load broker configuration,
- create broker metadata,
- initialize transport,
- create control/sender/receiver loops,
- start broker threads,
- wait for shutdown,
- perform graceful cleanup.

That means the installed `brz-broker` executable does not match its name.

### 2.3 No dedicated end-to-end or performance test packaging

There are integration-style tests embedded in implementation docs and some unit tests in source files, but there is no packaging plan for:

- process-based end-to-end tests,
- reusable test service binaries,
- benchmark executables,
- test-specific configuration and temp storage layout.

---

## 3. Target Package Model

## 3.1 Package Boundaries

The project must be split into **three Zig libraries/modules** and **multiple executables**:

| Package | Type | Purpose |
|---|---|---|
| `brz-common` | library/module | Shared low-level runtime used by both broker and services |
| `brz-service` | library/module | Service-side engine, clients, control agent, IPC helpers |
| `brz-broker` | library/module | Broker-side runtime, routing, transport, cluster, control loop |
| `brz-broker` | executable | Standalone broker process launcher |
| `brz-stat` | executable | Monitoring/inspection tool |
| `e2e-*` | executables | Test-only broker/service fixtures |
| `bench-*` | executables | Performance harnesses |

### 3.1.1 Why three libraries?

A two-way split (`common` + `broker`) is insufficient because service code would still either:
- live in `common`, polluting the shared API with service runtime concerns, or
- live in `broker`, forcing services to depend on broker packaging.

A three-way split gives the correct layering:

- `common` = reusable substrate,
- `service` = client/service runtime,
- `broker` = broker process runtime.

## 3.2 Dependency Rules

The dependency graph must be strictly acyclic:

| Package | May depend on |
|---|---|
| `brz-common` | nothing internal |
| `brz-service` | `brz-common` |
| `brz-broker` | `brz-common` |
| broker executable | `brz-broker` |
| service executable | `brz-service`, optionally app code |
| e2e harnesses | `brz-broker`, `brz-service`, `brz-common` |
| benchmarks | `brz-broker`, `brz-service`, `brz-common` |

### 3.2.1 Forbidden dependencies

These imports must be forbidden by convention and code review:

- `brz-common` importing `brz-service`
- `brz-common` importing `brz-broker`
- `brz-service` importing `brz-broker`
- broker runtime importing service application helpers

### 3.2.2 Allowed duplication

If a tiny helper is only meaningful on one side, keep it there. Do **not** move code into `common` just to avoid a few duplicated lines. `common` should contain only genuinely shared concepts.

## 3.3 Process Model

The packaging split must reflect the actual runtime process model:

### 3.3.1 Broker process

A broker process owns:

- broker metadata file,
- broker control ring buffer,
- broker send/messages ring buffer,
- UDP transport sockets,
- sender/receiver/control event loops,
- cluster state,
- service registry,
- monitoring counters and error log.

### 3.3.2 Service process

A service process owns:

- service metadata file,
- service control ring buffer,
- service messages ring buffer,
- service-side control agent thread,
- service-side message consumer thread,
- service client registry,
- application handlers.

### 3.3.3 Shared memory relationship

Broker and services are separate processes that map the same metadata-backed shared memory files. The packaging split must make this explicit:

- broker code manages broker-owned shared memory regions,
- service code manages service-owned shared memory regions,
- shared file layouts and ring buffer semantics live in `brz-common`.

---

## 4. Target Repository Layout

The repository should be reorganized conceptually as follows:

```text
brz-broker/
├── build.zig
├── build.zig.zon
├── src/
│   ├── common/
│   │   ├── root.zig
│   │   ├── platform/
│   │   ├── concurrent/
│   │   ├── memory/
│   │   ├── message/
│   │   ├── protocol/
│   │   ├── config/
│   │   └── monitoring/
│   ├── service/
│   │   ├── root.zig
│   │   ├── engine/
│   │   ├── ipc/
│   │   ├── control/
│   │   ├── client/
│   │   └── config/
│   ├── broker/
│   │   ├── root.zig
│   │   ├── runtime/
│   │   ├── control/
│   │   ├── sender/
│   │   ├── receiver/
│   │   ├── cluster/
│   │   ├── transport/
│   │   └── config/
│   ├── bin/
│   │   ├── brz_broker_main.zig
│   │   ├── brz_stat_main.zig
│   │   └── test_services/
│   └── testing/
│       ├── e2e/
│       ├── fixtures/
│       ├── perf/
│       └── support/
├── docs/
│   ├── architecture.md
│   ├── impl/
│   └── impl-next/
└── tools/
```

### 4.1 Mapping from current layout

The current top-level folders under `src/` already suggest the right split. The migration is mostly about **re-rooting** modules, not redesigning the whole codebase.

#### Move to `src/common/`

- `platform/`
- `concurrent/`
- `memory/`
- `message/` for shared headers/codecs only
- `protocol/`
- `monitoring/`
- shared config parsing helpers
- shared top-level reexports

#### Move to `src/service/`

- service engine
- service control agent
- service client registry
- service-side IPC producer/consumer wrappers
- service config
- service application-facing API

#### Move to `src/broker/`

- broker runtime
- broker control loop
- sender
- receiver
- cluster
- transport
- broker config
- broker application wiring

### 4.2 Transitional compatibility

During migration, compatibility shims may remain at old import paths, but only temporarily. Example:

- old `src/platform.zig` may re-export `src/common/root.zig` platform symbols,
- old `src/service.zig` may re-export `src/service/root.zig`,
- old `src/control.zig` may re-export broker control symbols.

These shims must be removed once all imports are updated.

---

## 5. Library Specifications

## 5.1 `brz-common`

`brz-common` is the shared substrate. It must contain only code that is meaningful to both broker and service runtimes.

### 5.1.1 Responsibilities

- platform abstraction
- atomics and synchronization primitives
- ring buffer implementation
- counters and error log primitives
- metadata file layouts
- buffers provider abstractions that are shared
- common message header definitions
- control message wire structs and codecs shared by both sides
- UDP protocol frame definitions
- shared config parsing utilities
- common constants

### 5.1.2 Non-responsibilities

`brz-common` must **not** contain:

- broker control loop logic
- broker sender/receiver loops
- cluster management
- service engine lifecycle
- service client registry behavior
- application-facing service APIs

### 5.1.3 Public API

The public API of `brz-common` should be intentionally small and stable:

- `platform`
- `concurrent`
- `memory`
- `message`
- `protocol`
- `monitoring`
- `config`
- `constants`

### 5.1.4 Suggested root exports

`src/common/root.zig` should re-export only stable namespaces, not every internal file.

Example export groups:

- `pub const platform = @import("platform/root.zig");`
- `pub const concurrent = @import("concurrent/root.zig");`
- `pub const memory = @import("memory/root.zig");`
- `pub const message = @import("message/root.zig");`
- `pub const protocol = @import("protocol/root.zig");`

## 5.2 `brz-service`

`brz-service` is the service-side runtime library.

### 5.2.1 Responsibilities

- service startup and shutdown
- service metadata creation/opening
- registration with broker
- waiting for registration response
- service heartbeat writing
- service control agent
- service message consumer
- service client and client registry
- same-host direct IPC path
- broker-routed remote send path
- service-facing configuration
- application-facing engine API

### 5.2.2 Public API

The service library should expose a clean application-facing API centered around:

- `BrzEngine`
- `ServiceConfig`
- `ServiceClient`
- `MessageHandler`
- optional typed client abstractions later

### 5.2.3 Internal-only service modules

These should remain internal to `brz-service`:

- control message polling internals
- registration wait loops
- service-side routing helpers
- internal client registry mutation logic

### 5.2.4 Dependency rule

`brz-service` may import only `brz-common` plus Zig stdlib.

It must not import broker runtime code.

## 5.3 `brz-broker`

`brz-broker` is the broker-side runtime library.

### 5.3.1 Responsibilities

- broker creation and shutdown
- broker metadata creation/opening
- control loop
- service registry
- service heartbeat checking
- service leader election
- sender event loop
- receiver event loop
- routing
- cluster management
- transport backends
- broker configuration
- monitoring wiring
- signal-aware lifecycle support

### 5.3.2 Public API

The broker library should expose:

- `Broker`
- `BrokerConfig`
- `BrokerApplication`
- `ThreadingMode`
- broker lifecycle helpers

### 5.3.3 Internal-only broker modules

These should remain internal to `brz-broker`:

- low-level routing tables
- retransmit internals
- peer connection state machines
- cluster admin dispatch internals
- transport completion plumbing

---

## 6. Executable Specifications

## 6.1 `brz-broker` executable

The installed `brz-broker` executable must be the real broker launcher.

### 6.1.1 Required behavior

On startup it must:

1. parse CLI arguments,
2. resolve config file path,
3. load and validate `BrokerConfig`,
4. initialize logging/monitoring,
5. create broker runtime,
6. start broker threads/event loops,
7. install signal handlers,
8. block until shutdown,
9. perform graceful cleanup,
10. return non-zero exit code on startup failure.

### 6.1.2 CLI contract

Minimum CLI:

- `brz-broker`
- `brz-broker --config path/to/broker.properties`
- `brz-broker --help`
- `brz-broker --version`

Optional later:

- `--dump-config`
- `--validate-config`
- `--foreground`
- `--threading-mode shared`

### 6.1.3 Main function shape

The executable main should be thin:

- parse args,
- call broker application bootstrap,
- map errors to exit codes.

All real runtime logic belongs in the `brz-broker` library, not in the executable.

## 6.2 Test Service Executables

End-to-end tests need small standalone service binaries.

Required test fixtures:

| Executable | Purpose |
|---|---|
| `e2e-echo-service` | receives a message and echoes/acks |
| `e2e-ping-service` | sends requests and records latency |
| `e2e-heartbeat-service` | stays registered and emits heartbeats |
| `e2e-crash-service` | registers, then exits abruptly for failure tests |

These binaries should be tiny wrappers around `brz-service`.

## 6.3 Utility Executables

### 6.3.1 `brz-stat`

`brz-stat` remains a separate executable and should depend primarily on `brz-common` monitoring and metadata readers.

### 6.3.2 Benchmark executables

At minimum:

- `bench-ipc-local`
- `bench-broker-local`
- `bench-broker-remote-sim`
- `bench-ring-buffer`

These should be separate executables rather than unit tests so they can:
- run longer,
- print structured metrics,
- be invoked in CI or manually,
- avoid test harness overhead.

---

## 7. Public API Boundaries

The split only works if public APIs are explicit.

### 7.1 Stable public surfaces

Each library must have a single root file:

- `src/common/root.zig`
- `src/service/root.zig`
- `src/broker/root.zig`

Consumers should import those roots, not deep internal files.

### 7.2 Internal namespace convention

Internal files should be organized under subdirectories and treated as private unless re-exported from the root.

### 7.3 No deep imports from executables

Executables should not import internal files like:

- `src/broker/control/control_loop.zig`
- `src/service/control/control_agent.zig`

Instead they should import the package root and use public constructors.

### 7.4 API design rule

If an executable needs a deep internal import, that is a signal that the library root API is incomplete.

---

## 8. Build System Specification

## 8.1 Modules and Artifacts

The build graph must define at least these modules:

- `brz_common`
- `brz_service`
- `brz_broker`

And these artifacts:

- static or object library for `brz_common`
- static or object library for `brz_service`
- static or object library for `brz_broker`
- executable `brz-broker`
- executable `brz-stat`
- test executables
- benchmark executables

### 8.1.1 Linking model

Preferred model:

- libraries are Zig modules first,
- executables link only what they import,
- optional static libraries may be installed for packaging clarity.

### 8.1.2 Import wiring

`brz_service` imports `brz_common`.

`brz_broker` imports `brz_common`.

The broker executable imports `brz_broker`.

Test fixtures import either `brz_service` or `brz_broker` as needed.

## 8.2 Build Steps

The build should expose these top-level steps:

| Step | Purpose |
|---|---|
| `zig build` | build and install default artifacts |
| `zig build run` | run `brz-broker` |
| `zig build test` | run unit and integration tests |
| `zig build e2e` | run process-based end-to-end tests |
| `zig build bench` | build benchmark executables |
| `zig build perf` | run selected performance tests |
| `zig build stat` | run `brz-stat` |

### 8.2.1 `run` step contract

`zig build run` must run the broker executable, not a placeholder app.

### 8.2.2 `test` step contract

`zig build test` should include:
- unit tests for all three libraries,
- in-process integration tests where appropriate.

### 8.2.3 `e2e` step contract

`zig build e2e` should:
- build broker and service fixture executables,
- create temp storage/config,
- launch processes,
- wait for readiness,
- run assertions,
- tear everything down.

## 8.3 Install Layout

Installed artifacts should look like:

```text
zig-out/
├── bin/
│   ├── brz-broker
│   ├── brz-stat
│   ├── e2e-echo-service
│   ├── e2e-ping-service
│   └── bench-ipc-local
└── lib/
    ├── libbrz_common.a
    ├── libbrz_service.a
    └── libbrz_broker.a
```

Static library installation is optional if the project is only consumed as a Zig package, but the logical split must still exist in the build graph.

---

## 9. Broker Startup Specification

This section fixes the specific issue that the main binary does not start the broker.

### 9.1 Required runtime flow

The broker executable must perform this sequence:

1. `parseArgs()`
2. `loadBrokerConfig()`
3. `validateBrokerConfig()`
4. `Broker.create(allocator, config)`
5. `broker.start()`
6. `installSignalHandlers(broker)`
7. `broker.awaitShutdown()`
8. `broker.shutdown()`

### 9.2 `BrokerApplication`

Introduce a thin broker application facade in the broker library:

| Type | Responsibility |
|---|---|
| `BrokerApplication` | top-level bootstrap and shutdown orchestration |
| `Broker` | owns runtime state and threads |
| `BrokerConfig` | immutable validated config |

### 9.3 Error handling

Startup failures must:
- print a clear error message,
- return non-zero exit code,
- clean up partially initialized resources.

### 9.4 Signal handling

The executable or broker application layer must handle:
- `SIGINT`
- `SIGTERM`

Behavior:
- first signal initiates graceful shutdown,
- repeated signal may force faster termination later if desired.

### 9.5 Logging

At minimum, startup should log:
- broker version,
- node id,
- bind address,
- storage path,
- threading mode,
- peer count.

---

## 10. Migration Plan

The split should be implemented incrementally.

### Phase 1 — Define roots and reexports

- create `src/common/root.zig`
- create `src/service/root.zig`
- create `src/broker/root.zig`
- re-export existing code without moving much yet

Goal: establish package boundaries first.

### Phase 2 — Move shared code into `common`

Move:
- `platform`
- `concurrent`
- `memory`
- shared `message`
- `protocol`
- `monitoring`

Update imports.

### Phase 3 — Move service runtime into `service`

Move:
- `BrzEngine`
- service control agent
- service client registry
- service-side IPC wrappers
- service config

Update imports so service code depends only on `common`.

### Phase 4 — Move broker runtime into `broker`

Move:
- broker lifecycle
- control loop
- sender
- receiver
- cluster
- transport
- broker config

Update imports so broker code depends only on `common`.

### Phase 5 — Replace placeholder main

- create `src/bin/brz_broker_main.zig`
- wire it to `brz-broker` library
- make `zig build run` launch the real broker

### Phase 6 — Add e2e and benchmark packaging

- add fixture executables
- add e2e runner
- add benchmark executables
- add build steps

### Phase 7 — Remove compatibility shims

Once all imports are updated:
- delete old top-level forwarding files,
- enforce root-only imports in docs and examples.

---

## 11. Acceptance Criteria

This document is considered implemented when all of the following are true:

### 11.1 Library split

- there are distinct `brz_common`, `brz_service`, and `brz_broker` modules,
- `brz_service` does not import broker internals,
- `brz_broker` does not depend on service runtime code.

### 11.2 Broker executable

- `zig build run` starts a real broker,
- installed `brz-broker` starts a real broker,
- broker remains running until signaled or fatal error occurs.

### 11.3 Service executable compatibility

- a standalone service binary can link only `brz-service` and `brz-common`,
- service registration and messaging still work.

### 11.4 Testability

- process-based e2e tests can launch broker and services separately,
- benchmark executables can be built independently.

### 11.5 Documentation consistency

- docs refer to the new package boundaries,
- examples import package roots rather than internal files.

---

## 12. Testing Impact

This packaging split is a prerequisite for the next two documents:
- end-to-end testing specification,
- performance testing specification.

### 12.1 Unit tests

Unit tests should move with their owning package:
- ring buffer tests under `common`,
- service engine tests under `service`,
- broker control/routing tests under `broker`.

### 12.2 Integration tests

In-process integration tests remain useful for:
- ring buffer + metadata interactions,
- control message encode/decode,
- service client registry behavior.

### 12.3 End-to-end tests

Process-based tests become much cleaner because:
- broker is a real executable,
- services are real executables,
- test harness can treat them as black-box processes.

### 12.4 Performance tests

Benchmarks can now measure:
- service-only local IPC path,
- service-to-broker local path,
- broker-to-broker remote path,
- broker startup and shutdown overhead.

---

## 13. Implementation Notes

### 13.1 Naming

Use hyphenated names for executables and underscored names for Zig module identifiers:

| Concern | Name |
|---|---|
| executable | `brz-broker` |
| executable | `brz-stat` |
| module | `brz_broker` |
| module | `brz_service` |
| module | `brz_common` |

### 13.2 Keep roots small

Each root file should mostly:
- re-export stable APIs,
- define package version if needed,
- avoid embedding implementation logic.

### 13.3 Avoid over-centralizing config

Keep:
- shared parsing helpers in `common`,
- `BrokerConfig` in `broker`,
- `ServiceConfig` in `service`.

### 13.4 Avoid over-centralizing message code

Only shared wire formats belong in `common`.
Behavioral dispatch belongs in the owning package.

### 13.5 Preserve architecture invariants

This packaging split must not change the core invariants from `architecture.md`:

- no allocations on hot path,
- no locks on hot path,
- single-writer ownership,
- shared-memory IPC for same-host communication,
- broker-per-host process model,
- duty-cycle event loops.

### 13.6 Recommended next documents

After this document, add:

1. `14-end-to-end-testing.md`
2. `15-performance-and-benchmarking.md`

Those documents should build directly on the package boundaries defined here.

---

## Summary

The current codebase already contains the right conceptual pieces, but they are packaged too loosely. The correct fix is to split the implementation into:

- `brz-common` for shared substrate,
- `brz-service` for service runtime,
- `brz-broker` for broker runtime,

and then make the `brz-broker` executable a real broker launcher.

This split aligns the Zig packaging model with the actual runtime architecture: one broker process per host, many service processes per host, shared memory between them, and cleanly separated responsibilities. It also creates the foundation needed for robust end-to-end and performance testing.