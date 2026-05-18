# Testing Architecture

RingLoom tests are layered so contributors can validate pure logic, process
integration, cross-node behavior, bindings, and performance separately.

## Test layers

| Layer | Command | Coverage |
|---|---|---|
| Unit/integration | `zig build test` | Ring buffers, metadata, codecs, service runtime, broker internals, Aeron wrapper. |
| Testing harness | `zig build test-testing` | Process runner, temp environments, config generation, readiness checks. |
| End-to-end | `zig build e2e` | Multi-process brokers/services, discovery, local IPC, remote Aeron UDP, restart, back-pressure, observability. |
| Performance | `zig build perf` | Local throughput/latency, remote latency, back-pressure, recovery. |
| Plain Aeron reference | `zig build perf-aeron` | Raw Aeron UDP comparison without RingLoom routing/final delivery. |
| Bindings | `zig build test-cpp`, `zig build test-java`, `zig build test-node` | C++/Java/Node API integration over the C ABI. |
| Samples | `zig build sample-order-management`, `zig build run-sample-order-management` | Runnable application topology. |

## Harness model

The e2e harness starts real broker and service processes with generated configs,
unique metadata directories, and unique Aeron directories/ports. It waits for
readiness markers, asserts behavior through logs/metadata/messages, and preserves
failure directories for debugging.

Failure-preserved directories include broker configs, process logs, metadata files,
Aeron directories/CnC files, and result artifacts when applicable.

## Core scenarios

E2E coverage should include:

1. Broker startup and metadata creation.
2. Service registration and graceful unregister.
3. Service discovery updates.
4. Local service-to-service ring-buffer IPC.
5. Cross-broker Aeron UDP delivery.
6. Leader election and leader-routed sends.
7. Heartbeat timeout and crash recovery.
8. Back-pressure and slow consumers.
9. Fragmentation/reassembly.
10. Observability scrape/stat output.

## Performance interpretation

Use paced transit latency to understand unloaded route cost and saturated benchmarks
to understand queueing behavior. Remote RingLoom latency includes:

1. Source service data-header encode.
2. Direct Aeron UDP publication.
3. Target broker receiver polling/routing.
4. Final service messages ring write.
5. Target service consume timing.

The plain Aeron benchmark is useful as a floor for transport-only remote latency.

See [`../testing.md`](../testing.md) for developer-facing commands and current
scenario lists.
