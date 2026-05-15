# RingLoom

![RingLoom logo](assets/RingLoom.png)

**RingLoom** is a high-performance message broker and service runtime for low-latency systems.
It combines zero-copy same-host IPC over shared-memory ring buffers with UDP-based cross-host
routing, giving services one messaging model for both local and remote communication.

RingLoom is aimed at workloads where predictable latency, explicit memory layout, and
allocation-free hot paths matter more than general-purpose middleware features.

## Why RingLoom

- **Fast local communication** with memory-mapped metadata files and lock-free ring buffers.
- **Cross-host routing** through broker-to-broker reliable UDP transport with platform-optimized I/O.
- **Predictable execution** from pre-allocated buffers, explicit wire formats, and tight event loops.
- **Operational visibility** through test harnesses, `ringloom-stat`, and a Prometheus exporter.
- **Service integration options** through Zig modules plus C/C++, Java, and Node.js bindings.

## Highlights

- Shared-memory IPC between services and the local broker
- Broker-to-broker reliable UDP transport with a POSIX engine and optional AF_XDP acceleration
- Dedicated control, sender, and receiver event loops
- Service registration, discovery, heartbeat tracking, and leader election
- Cluster membership and state synchronization
- End-to-end and performance test harnesses
- Order-management sample application spanning multiple services and broker nodes

## Architecture at a glance

```text
same host                                              remote host
┌──────────────┐   shared memory   ┌────────────────┐   UDP   ┌────────────────┐   shared memory   ┌──────────────┐
│ Service      │ ────────────────▶│ RingLoom       │ ──────▶│ RingLoom       │ ────────────────▶│ Service      │
│ producer     │                   │ broker node A  │         │ broker node B  │                   │ consumer     │
└──────────────┘                   └────────────────┘         └────────────────┘                   └──────────────┘
```

Core modules:

| Module | Purpose |
|---|---|
| `ringloom_common` | Shared foundations: ring buffers, message codecs, config, monitoring, platform helpers |
| `ringloom_udp` | Reliable UDP transport primitives, POSIX endpoint, and optional AF_XDP support |
| `ringloom_service` | Service-side runtime and client APIs |
| `ringloom_broker` | Broker runtime, routing, cluster management, and event loops |
| `ringloom_testing` | Multi-process test harness for end-to-end and perf scenarios |

## Getting started

### Requirements

- Zig **0.16.x**
- Linux
- Optional tooling for bindings:
  - Gradle for Java bindings
  - Node.js and npm for Node.js bindings
  - A C++17 toolchain for C++ bindings

### Common commands

| Command | What it does |
|---|---|
| `zig build test` | Run unit and integration tests |
| `zig build test-testing` | Run testing harness tests only |
| `zig build e2e` | Run end-to-end correctness tests |
| `zig build perf` | Run performance benchmarks |
| `zig build run -- --config <path>` | Run the broker |
| `zig build stat` | Run the `ringloom-stat` monitoring CLI |
| `zig build observability` | Build the Prometheus exporter |
| `zig build run-observability -- --storage-path /dev/shm --group default` | Run the Prometheus exporter |
| `zig build sample-order-management` | Build the sample application |
| `zig build run-sample-order-management` | Run the sample application |

### Bindings and integration

| Area | Command |
|---|---|
| C service ABI | `zig build service-c` |
| C++ bindings | `zig build test-cpp` |
| Java bindings | `zig build test-java` |
| Java framework | `zig build test-java-framework` |
| Node.js bindings | `zig build test-node` |

## Repository layout

| Path | Contents |
|---|---|
| `src/` | Core broker, service runtime, transport, tests, and binaries |
| `tools/` | Operational tools such as `ringloom-stat` and the observability exporter |
| `bindings/` | C++, Java, and Node.js integration layers |
| `samples/order-management/` | Runnable multi-service sample application |
| `docs/` | Architecture, testing, observability, and implementation notes |

## Documentation

- [`docs/architecture.md`](docs/architecture.md) - broker architecture and data flow
- [`docs/testing.md`](docs/testing.md) - testing strategy and harness details
- [`docs/observability.md`](docs/observability.md) - Prometheus exporter design
- [`docs/samples_order_management.md`](docs/samples_order_management.md) - sample topology and behavior

## Project status

RingLoom already covers the core runtime pieces: local IPC, broker routing, cluster control
flows, service discovery, testing infrastructure, and multiple language bindings. The project
is still evolving around operational tooling, broader recovery coverage, and production
hardening.

## License

[Apache-2.0](LICENSE)
