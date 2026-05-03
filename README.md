# RingLoom

![RingLoom logo](assets/RingLoom.png)

RingLoom is a Zig message broker and runtime for low-latency service-to-service
communication. It uses shared-memory ring buffers for same-host IPC and TCP between
brokers for cross-host routing.

## Purpose

RingLoom is built for predictable messaging in clustered systems where hot-path
efficiency, explicit memory control, and simple transport semantics matter more than
general-purpose middleware features.

## Architecture

- Same-host communication uses memory-mapped metadata files and lock-free ring buffers.
- Cross-host communication uses framed TCP with `io_uring` on Linux and `kqueue` on macOS.
- Brokers handle registration, discovery, heartbeats, routing, cluster membership, and leader election.

## Implemented

- Shared-memory IPC between services and the local broker
- Broker-to-broker TCP transport and frame protocol
- Dedicated control, sender, and receiver event loops
- Service registration, discovery, and heartbeat tracking
- Cluster membership, leader election, and state synchronization
- End-to-end and performance test harnesses

## Planned

- Additional kernel-bypass transport backends
- More operational tooling and monitoring views
- Broader failure-recovery and long-running soak coverage
- Further hardening of back-pressure and flow-control behavior
- Packaging and deployment refinements for production use

## Build and test

Requires Zig 0.16.x.

```bash
zig build test
zig build e2e
zig build perf
```

## License

[Apache-2.0](LICENSE)
