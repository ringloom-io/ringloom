# 14 — Broker Executable & Startup Wiring

> **Prerequisites:**  
> [00 — Overview](../impl/00-overview.md),  
> [08 — Service ↔ Broker IPC](../impl/08-service-ipc.md),  
> [09 — Control Plane](../impl/09-control-plane.md),  
> [10 — Threading Model](../impl/10-threading-model.md),  
> [11 — Cluster Management](../impl/11-cluster-management.md),  
> [12 — Configuration & Monitoring](../impl/12-configuration-and-monitoring.md)

This document defines the implementation changes required to make the project start a
real broker process, rather than just compiling a library and printing a placeholder
message. It also defines the executable boundary, startup sequence, shutdown sequence,
build targets, and the separation between reusable libraries and the `brz-broker`
binary.

The goal is to address two concrete shortcomings:

1. The current main binary does **not** start the broker.
2. The project structure does not clearly separate:
   - broker-only code,
   - service/client runtime code,
   - shared/common code.

This document establishes the executable and library architecture that the rest of the
implementation should follow.

---

## Table of Contents

1. [Overview](#1-overview)
2. [Goals](#2-goals)
3. [Target Package Layout](#3-target-package-layout)
   1. [Library Boundaries](#31-library-boundaries)
   2. [Executable Targets](#32-executable-targets)
4. [Build Graph Changes](#4-build-graph-changes)
   1. [Required Build Outputs](#41-required-build-outputs)
   2. [Naming Rules](#42-naming-rules)
5. [Broker Runtime API](#5-broker-runtime-api)
   1. [Broker Struct Responsibilities](#51-broker-struct-responsibilities)
   2. [Broker Lifecycle API](#52-broker-lifecycle-api)
6. [Broker Application Layer](#6-broker-application-layer)
   1. [Why an Application Layer Exists](#61-why-an-application-layer-exists)
   2. [BrokerApplication Struct](#62-brokerapplication-struct)
   3. [BrokerApplicationFactory](#63-brokerapplicationfactory)
7. [Executable Entry Point](#7-executable-entry-point)
   1. [CLI Behavior](#71-cli-behavior)
   2. [Main Function Contract](#72-main-function-contract)
   3. [Signal Handling](#73-signal-handling)
8. [Startup Sequence](#8-startup-sequence)
9. [Shutdown Sequence](#9-shutdown-sequence)
10. [Configuration Resolution](#10-configuration-resolution)
11. [Logging & Exit Codes](#11-logging--exit-codes)
12. [File Structure](#12-file-structure)
13. [Implementation Sketches](#13-implementation-sketches)
   1. [Broker Runtime](#131-broker-runtime)
   2. [Broker Application](#132-broker-application)
   3. [Main Executable](#133-main-executable)
   4. [Build Script](#134-build-script)
14. [Testing](#14-testing)
15. [Migration Plan](#15-migration-plan)
16. [Acceptance Criteria](#16-acceptance-criteria)

---

## 1. Overview

The current executable entry point is only a placeholder. It prints a version banner and
verifies that the platform clock works, but it does not:

- load broker configuration,
- create broker metadata,
- initialize control/sender/receiver loops,
- start broker threads,
- install signal handlers,
- block until shutdown,
- perform graceful cleanup.

That means the project currently behaves like a library demo, not like a deployable
broker process.

This document introduces a proper executable architecture with three layers:

1. **Shared runtime library** — common code used by broker and services.
2. **Broker runtime library** — broker-specific logic and lifecycle.
3. **Broker executable** — thin process entry point that loads config, starts the broker,
   waits for termination, and exits with a meaningful status code.

This mirrors the architecture described in `architecture.md`: the broker is a standalone
process, while services are separate processes that communicate with it through shared
memory and TCP.

---

## 2. Goals

### Primary goals

- Make `brz-broker` start the actual broker.
- Separate reusable code from process-specific code.
- Keep the executable thin and operationally focused.
- Make startup and shutdown deterministic.
- Provide a stable lifecycle API for future end-to-end tests.

### Secondary goals

- Make it easy to add:
  - a service executable,
  - test harness executables,
  - benchmark executables,
  - admin/monitoring tools.
- Avoid circular dependencies between broker and service code.
- Preserve hot-path invariants by keeping orchestration logic out of the hot path.

### Non-goals

This document does **not** define:
- the full service executable design,
- the end-to-end test harness in detail,
- benchmark methodology in detail.

Those belong in separate follow-up documents, but this document provides the lifecycle
surface they depend on.

---

## 3. Target Package Layout

## 3.1 Library Boundaries

The project should be split into three logical modules.

### A. `brz_core`

Shared code used by both broker and services.

This includes:

- `platform`
- `concurrent`
- `memory`
- `message`
- `ipc`
- `protocol`
- selected `config` pieces shared by both sides
- selected `monitoring` pieces shared by both sides

This module must **not** depend on broker-only code or service-only code.

### B. `brz_broker`

Broker-specific runtime.

This includes:

- `control`
- `sender`
- `receiver`
- `transport`
- `cluster`
- `flow_control`
- `threading`
- broker-specific config/application wiring

This module may depend on `brz_core`.

### C. `brz_service`

Service/client runtime.

This includes:

- service engine
- service control agent
- service message consumer
- service client registry
- service-side config
- service-side lifecycle helpers

This module may depend on `brz_core`.

### Dependency rule

The dependency graph must be:

- `brz_core` ← `brz_broker`
- `brz_core` ← `brz_service`

And never:

- `brz_broker` ← `brz_service`
- `brz_service` ← `brz_broker`

Broker and service communicate through shared memory files, ring buffers, and protocol
definitions, not through direct code dependencies.

---

## 3.2 Executable Targets

At minimum, the build must produce these executables:

- `brz-broker` — standalone broker process
- `brz-stat` — monitoring/statistics tool

Future executables should fit naturally into the same structure:

- `brz-service-*` sample services
- `brz-e2e-*` test harness binaries
- `brz-bench-*` benchmark binaries

The executable target name for the broker should be `brz-broker` with a hyphen, matching
the process name and user-facing command.

If the package/module name remains `brz_broker` internally, that is acceptable. The
artifact name exposed to users should still be `brz-broker`.

---

## 4. Build Graph Changes

## 4.1 Required Build Outputs

The build should expose:

- one shared core module,
- one broker module,
- one service module,
- one broker executable,
- one monitoring executable,
- test steps for each module,
- integration/e2e test steps later.

The build graph should support these use cases:

- import `brz_core` from broker code,
- import `brz_core` from service code,
- import `brz_broker` from the broker executable,
- import `brz_service` from service executables and tests.

### Required conceptual outputs

| Output | Type | Purpose |
|---|---|---|
| `brz_core` | module/library | shared runtime |
| `brz_broker` | module/library | broker runtime |
| `brz_service` | module/library | service runtime |
| `brz-broker` | executable | actual broker process |
| `brz-stat` | executable | monitoring tool |

---

## 4.2 Naming Rules

Use these naming conventions consistently:

| Kind | Name style | Example |
|---|---|---|
| Zig module | snake_case | `brz_core` |
| Executable artifact | kebab-case | `brz-broker` |
| Source root file | snake_case | `broker_application.zig` |
| Public type | PascalCase | `BrokerApplication` |

This avoids confusion between:
- import names,
- artifact names,
- process names.

---

## 5. Broker Runtime API

## 5.1 Broker Struct Responsibilities

The broker runtime type should represent the running broker system, not the CLI process.

`Broker` is responsible for:

- owning broker metadata mappings,
- owning counters and monitoring state,
- owning control/sender/receiver loops,
- owning command queues,
- owning thread runners or composite runners,
- starting runtime threads,
- stopping runtime threads,
- releasing runtime resources.

`Broker` is **not** responsible for:

- parsing CLI arguments,
- printing usage text,
- deciding exit codes,
- installing OS signal handlers,
- formatting startup banners.

Those belong to the application/executable layer.

---

## 5.2 Broker Lifecycle API

The broker runtime should expose a small, explicit lifecycle API.

### Required API

```zig
pub const Broker = struct {
    pub fn init(allocator: std.mem.Allocator, config: BrokerConfig) !Broker;
    pub fn start(self: *Broker) !void;
    pub fn shutdown(self: *Broker) void;
    pub fn deinit(self: *Broker) void;
};
```

### Semantics

- `init(...)`
  - allocates and initializes runtime state,
  - opens/creates metadata files,
  - creates event loops and command queues,
  - does **not** start threads yet.

- `start()`
  - starts the configured threading mode,
  - transitions the broker into running state,
  - is idempotent only if explicitly implemented; otherwise calling twice is a bug.

- `shutdown()`
  - requests stop and joins all threads,
  - safe to call during normal shutdown and signal-triggered shutdown,
  - should tolerate partial startup.

- `deinit()`
  - frees memory and closes mappings,
  - must be called exactly once after `shutdown()` or after failed `init()` cleanup paths.

### Optional API

If useful, the runtime may also expose:

```zig
pub fn isRunning(self: *const Broker) bool;
pub fn awaitShutdown(self: *Broker) void;
```

But `awaitShutdown()` should not be the only way to manage lifecycle. The application
layer should own the shutdown barrier.

---

## 6. Broker Application Layer

## 6.1 Why an Application Layer Exists

A broker process needs orchestration logic that should not pollute the runtime:

- config loading,
- logger initialization,
- signal registration,
- startup banner,
- shutdown barrier,
- exit code mapping.

If this logic is placed directly in `main.zig`, the executable becomes hard to test.
If it is placed directly in `Broker`, the runtime becomes coupled to process concerns.

The solution is a small application layer.

---

## 6.2 BrokerApplication Struct

`BrokerApplication` is the process-level wrapper around `Broker`.

Responsibilities:

- own the allocator used by the process,
- own the loaded `BrokerConfig`,
- own the `Broker` instance,
- install signal handlers,
- start the broker,
- wait for shutdown,
- coordinate graceful stop,
- map failures to exit codes.

### Required API

```zig
pub const BrokerApplication = struct {
    pub fn init(allocator: std.mem.Allocator, config: BrokerConfig) !BrokerApplication;
    pub fn run(self: *BrokerApplication) !u8;
    pub fn shutdown(self: *BrokerApplication) void;
    pub fn deinit(self: *BrokerApplication) void;
};
```

### `run()` contract

`run()` should:

1. initialize runtime resources if not already initialized,
2. start the broker,
3. install signal handling,
4. block until shutdown is requested,
5. stop the broker,
6. return an exit code.

This makes `run()` easy to use from:
- `main.zig`,
- integration harnesses,
- smoke tests.

---

## 6.3 BrokerApplicationFactory

A factory type or factory function is recommended to keep construction readable.

Responsibilities:

- load config from file/environment,
- validate config,
- create the broker runtime,
- wire monitoring and optional diagnostics,
- return a ready-to-run `BrokerApplication`.

### Suggested API

```zig
pub const BrokerApplicationFactory = struct {
    pub fn create(
        allocator: std.mem.Allocator,
        config_path: ?[]const u8,
    ) !BrokerApplication;
};
```

Alternative:

```zig
pub fn createBrokerApplication(
    allocator: std.mem.Allocator,
    config_path: ?[]const u8,
) !BrokerApplication;
```

Either is acceptable. Prefer the style already used elsewhere in the codebase.

---

## 7. Executable Entry Point

## 7.1 CLI Behavior

The broker executable should support a minimal operational CLI.

### Required behavior

- `brz-broker`
  - load config from default resolution rules,
  - start broker,
  - block until shutdown.

- `brz-broker --config path/to/file.properties`
  - load config from explicit path.

- `brz-broker --help`
  - print usage and exit `0`.

- `brz-broker --version`
  - print version and exit `0`.

### Optional future flags

- `--validate-config`
- `--dump-config`
- `--foreground`
- `--log-level`
- `--threading-mode`

These are not required for the first implementation.

---

## 7.2 Main Function Contract

The `main()` function must be thin.

It should do only this:

1. create top-level allocator,
2. parse CLI args,
3. create `BrokerApplication`,
4. call `run()`,
5. exit with returned code.

It must not contain broker wiring logic directly.

### Required behavior on failure

- startup/configuration failures:
  - print a clear error message to stderr,
  - exit non-zero.

- runtime failures during startup:
  - print a clear error message,
  - ensure partial resources are cleaned up,
  - exit non-zero.

---

## 7.3 Signal Handling

The broker process must shut down cleanly on termination signals.

### Required signals

On POSIX:
- `SIGINT`
- `SIGTERM`

### Required behavior

When a signal is received:

- set a shutdown flag or notify a shutdown barrier,
- do not perform heavy cleanup directly in the signal handler,
- let the main/application thread perform orderly shutdown.

### Rationale

Signal handlers must remain minimal and async-signal-safe. They should not:
- allocate,
- log through complex logging frameworks,
- lock mutexes,
- call arbitrary cleanup code.

---

## 8. Startup Sequence

The broker startup sequence must be explicit and deterministic.

### Required order

1. Parse CLI arguments.
2. Resolve config path.
3. Load and validate `BrokerConfig`.
4. Initialize process-level logging.
5. Create `BrokerApplication`.
6. Create `Broker` runtime.
7. Open/create broker metadata files.
8. Initialize counters and monitoring state.
9. Initialize command queues.
10. Initialize control loop.
11. Initialize sender loop.
12. Initialize receiver loop.
13. Wire inter-loop command queues.
14. Install signal handlers.
15. Start broker threads according to configured threading mode.
16. Mark application as running.
17. Block on shutdown barrier.

### Important invariant

No worker thread should start before all shared state and inter-loop references are fully
wired.

That means:
- create first,
- wire second,
- start last.

---

## 9. Shutdown Sequence

Shutdown must be the reverse of startup, with thread stop before resource teardown.

### Required order

1. Shutdown requested by signal or internal failure.
2. Mark application as stopping.
3. Stop/join broker threads.
4. Stop network I/O.
5. Flush or finalize monitoring state if needed.
6. Close metadata mappings.
7. Release command queue buffers.
8. Release runtime-owned allocations.
9. Release application-owned allocations.
10. Exit process.

### Important invariants

- No mapped memory may be unmapped while worker threads can still access it.
- No command queue buffer may be freed before all threads are joined.
- `shutdown()` should be safe if startup failed halfway through.

### Best-effort cleanup

If one cleanup step fails, continue cleanup of remaining resources where possible.

---

## 10. Configuration Resolution

The executable should use the same configuration model already described in the config
document, but the startup layer must define the precedence clearly.

### Precedence

1. Explicit CLI `--config <path>`
2. Environment variable override
3. Default config path

### Recommended defaults

- broker config default file: `broker.properties`
- service config default file: `service.properties` later

### Validation failures

Configuration validation must fail fast before any worker threads are started.

Examples:
- invalid node ID,
- duplicate peer node IDs,
- non-power-of-two buffer sizes,
- invalid host/port syntax,
- unsupported threading mode.

---

## 11. Logging & Exit Codes

## Logging

The executable should log these lifecycle events at minimum:

- broker starting
- config loaded
- threading mode selected
- broker started
- shutdown requested
- broker stopped

Hot-path logging rules remain unchanged: no logging on the hot path.

## Exit codes

Use a small, stable exit code set.

| Code | Meaning |
|---|---|
| `0` | clean shutdown |
| `1` | CLI usage error |
| `2` | configuration error |
| `3` | startup/runtime initialization error |
| `4` | unexpected fatal runtime error |

If the implementation prefers an enum internally, map it to these numeric values at the
edge.

---

## 12. File Structure

The following structure is recommended.

```text
src/
├── core.zig                         # root of shared runtime exports
├── broker.zig                       # root of broker runtime exports
├── service.zig                      # root of service runtime exports
├── main.zig                         # broker executable entry point
├── app/
│   ├── broker_application.zig
│   ├── broker_application_factory.zig
│   └── shutdown_barrier.zig
├── broker_runtime/
│   ├── broker_runtime.zig
│   ├── broker_context.zig
│   └── broker_lifecycle.zig
├── platform/
├── concurrent/
├── memory/
├── message/
├── ipc/
├── protocol/
├── control/
├── sender/
├── receiver/
├── transport/
├── cluster/
├── flow_control/
├── threading/
├── config/
└── monitoring/
```

### Notes

- The exact directory names may vary.
- The important part is the separation of:
  - shared runtime,
  - broker runtime,
  - application/executable layer.

If renaming directories is too disruptive immediately, the same separation can first be
introduced through root modules and wrapper files, then followed by physical moves later.

---

## 13. Implementation Sketches

## 13.1 Broker Runtime

```zig
const std = @import("std");
const BrokerConfig = @import("../config/broker_config.zig").BrokerConfig;

pub const Broker = struct {
    allocator: std.mem.Allocator,
    config: BrokerConfig,
    started: bool = false,

    pub fn init(allocator: std.mem.Allocator, config: BrokerConfig) !Broker {
        var self = Broker{
            .allocator = allocator,
            .config = config,
        };

        // Create metadata mappings, counters, command queues, event loops,
        // and wire all dependencies here.

        return self;
    }

    pub fn start(self: *Broker) !void {
        if (self.started) return error.AlreadyStarted;

        // Start control/sender/receiver according to threading mode.
        self.started = true;
    }

    pub fn shutdown(self: *Broker) void {
        if (!self.started) return;

        // Stop and join threads in safe order.
        self.started = false;
    }

    pub fn deinit(self: *Broker) void {
        // Free buffers, close mappings, release owned resources.
        _ = self;
    }
};
```

### Runtime rule

`Broker` should be usable from tests without going through the CLI.

That means tests should be able to do:

```zig
var broker = try Broker.init(allocator, config);
defer broker.deinit();

try broker.start();
defer broker.shutdown();
```

---

## 13.2 Broker Application

```zig
const std = @import("std");
const Broker = @import("../broker_runtime/broker_runtime.zig").Broker;
const BrokerConfig = @import("../config/broker_config.zig").BrokerConfig;

pub const BrokerApplication = struct {
    allocator: std.mem.Allocator,
    config: BrokerConfig,
    broker: Broker,
    shutdown_requested: std.atomic.Value(bool),

    pub fn init(allocator: std.mem.Allocator, config: BrokerConfig) !BrokerApplication {
        return .{
            .allocator = allocator,
            .config = config,
            .broker = try Broker.init(allocator, config),
            .shutdown_requested = std.atomic.Value(bool).init(false),
        };
    }

    pub fn run(self: *BrokerApplication) !u8 {
        try self.installSignalHandlers();
        try self.broker.start();

        while (!self.shutdown_requested.load(.acquire)) {
            std.time.sleep(100 * std.time.ns_per_ms);
        }

        self.broker.shutdown();
        return 0;
    }

    pub fn shutdown(self: *BrokerApplication) void {
        self.shutdown_requested.store(true, .release);
    }

    pub fn deinit(self: *BrokerApplication) void {
        self.broker.deinit();
    }

    fn installSignalHandlers(self: *BrokerApplication) !void {
        _ = self;
        // Register SIGINT/SIGTERM handlers that only request shutdown.
    }
};
```

### Application rule

The application layer owns waiting and signal coordination. The runtime owns broker
resources and threads.

---

## 13.3 Main Executable

```zig
const std = @import("std");
const BrokerApplicationFactory = @import("app/broker_application_factory.zig").BrokerApplicationFactory;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    if (hasHelpFlag(args)) {
        try printHelp();
        return;
    }

    if (hasVersionFlag(args)) {
        try printVersion();
        return;
    }

    const config_path = try parseConfigPath(args);

    var app = try BrokerApplicationFactory.create(allocator, config_path);
    defer app.deinit();

    const exit_code = try app.run();
    std.process.exit(exit_code);
}
```

### Main rule

No broker internals should be wired directly in `main()`.

---

## 13.4 Build Script

The build script should expose separate modules and a real broker executable.

```zig
const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const brz_core = b.addModule("brz_core", .{
        .root_source_file = b.path("src/core.zig"),
        .target = target,
    });

    const brz_broker = b.addModule("brz_broker", .{
        .root_source_file = b.path("src/broker.zig"),
        .target = target,
        .imports = &.{
            .{ .name = "brz_core", .module = brz_core },
        },
    });

    const brz_service = b.addModule("brz_service", .{
        .root_source_file = b.path("src/service.zig"),
        .target = target,
        .imports = &.{
            .{ .name = "brz_core", .module = brz_core },
        },
    });

    const broker_exe = b.addExecutable(.{
        .name = "brz-broker",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "brz_broker", .module = brz_broker },
                .{ .name = "brz_core", .module = brz_core },
            },
        }),
    });

    b.installArtifact(broker_exe);
}
```

### Build rule

The executable must import the broker runtime module and start the real broker.

---

## 14. Testing

This document requires a new class of tests.

### Unit tests

- CLI arg parsing
- config path resolution
- exit code mapping
- startup failure cleanup
- signal/shutdown barrier behavior

### Integration tests

- `Broker.init()` creates runtime successfully with test config
- `Broker.start()` starts all configured loops
- `Broker.shutdown()` stops cleanly
- `BrokerApplication.run()` starts and stops on shutdown request

### Smoke test

A minimal smoke test should verify:

1. create temporary config,
2. start `brz-broker`,
3. wait until broker metadata file exists,
4. request shutdown,
5. assert clean exit.

This is the first executable-level test that proves the binary actually starts the broker.

---

## 15. Migration Plan

Implement this in small steps.

### Step 1 — Introduce runtime/application split

- create `BrokerApplication`
- create `BrokerApplicationFactory`
- move startup orchestration out of `main.zig`

### Step 2 — Make `main.zig` start the real broker

- replace placeholder banner-only logic
- load config
- create app
- run app

### Step 3 — Split root modules

- introduce `brz_core`
- introduce `brz_broker`
- introduce `brz_service`
- re-export existing code through those roots

### Step 4 — Rename executable artifact

- install `brz-broker` executable
- keep internal module names stable if needed

### Step 5 — Add smoke/integration tests

- broker startup/shutdown test
- application lifecycle test

This order minimizes disruption while immediately fixing the broken executable behavior.

---

## 16. Acceptance Criteria

This document is satisfied when all of the following are true:

1. Running `zig build run` starts a real broker, not a placeholder program.
2. Running the installed `brz-broker` binary starts the broker event loops.
3. The executable can be stopped cleanly with `SIGINT` or `SIGTERM`.
4. Broker startup logic is not embedded directly in `main.zig`.
5. Shared code is separated from broker-only code through explicit module boundaries.
6. Broker runtime can be instantiated directly from tests without going through CLI code.
7. A smoke test proves that the broker process starts, creates its runtime state, and
   shuts down cleanly.

---

## Summary

To fix the current shortcomings, the implementation must treat the broker as a real
standalone process with a thin executable layer on top of a reusable broker runtime.

The key architectural decisions are:

- split code into `brz_core`, `brz_broker`, and `brz_service`,
- introduce a `BrokerApplication` process wrapper,
- keep `Broker` focused on runtime lifecycle,
- make `main.zig` a thin CLI/bootstrap layer,
- ensure `brz-broker` actually starts the broker threads and waits for shutdown.

This document is the executable/startup counterpart to the existing runtime documents and
is the foundation for the next two missing pieces:
- service executable/process documents,
- end-to-end and performance test documents.