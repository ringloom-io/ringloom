# Benchmark Results — BRZ Broker

Tracked performance benchmark results for regression detection and improvement
comparison. Update this file after each significant performance-related change.

> **How to run:** `zig build perf` (see `docs/testing.md` for full instructions)

---

## Current Baseline

**Date:** 2026-04-11
**Zig version:** 0.15.2
**Build mode:** ReleaseFast
**OS:** Linux x86_64
**Status:** Broker runtime not yet fully wired — benchmarks compile but do not
produce results yet. All latency/throughput values below are **pending**.

---

## Local IPC Latency (single broker, same host)

Topology: 1 broker + ping service + echo service

| Message size | Warmup | Measured | p50 (ns) | p95 (ns) | p99 (ns) | max (ns) |
|-------------:|-------:|---------:|---------:|---------:|---------:|---------:|
| 32 B         | 10,000 | 100,000  | —        | —        | —        | —        |
| 128 B        | 10,000 | 100,000  | —        | —        | —        | —        |
| 512 B        | 10,000 | 100,000  | —        | —        | —        | —        |
| 1,024 B      | 10,000 | 100,000  | —        | —        | —        | —        |
| 4,096 B      | 5,000  | 50,000   | —        | —        | —        | —        |

## Local IPC Throughput (single broker, same host)

Topology: 1 broker + ping service + echo service

| Message size | Warmup | Measured | msgs/sec | MB/sec |
|-------------:|-------:|---------:|---------:|-------:|
| 32 B         | 50,000 | 500,000  | —        | —      |
| 256 B        | 50,000 | 500,000  | —        | —      |
| 1,024 B      | 50,000 | 500,000  | —        | —      |
| 4,096 B      | 50,000 | 500,000  | —        | —      |

## Cross-Broker Latency (two brokers on loopback)

Topology: broker A (node 1) ↔ broker B (node 2), ping on A, echo on B

| Message size | Warmup | Measured | p50 (ns) | p95 (ns) | p99 (ns) | max (ns) |
|-------------:|-------:|---------:|---------:|---------:|---------:|---------:|
| 32 B         | 10,000 | 100,000  | —        | —        | —        | —        |
| 128 B        | 10,000 | 100,000  | —        | —        | —        | —        |
| 512 B        | 10,000 | 100,000  | —        | —        | —        | —        |
| 1,024 B      | 10,000 | 100,000  | —        | —        | —        | —        |
| 4,096 B      | 10,000 | 100,000  | —        | —        | —        | —        |

## Cross-Broker Throughput (two brokers on loopback)

Topology: broker A (node 1) ↔ broker B (node 2), ping on A, echo on B

| Message size | Warmup | Measured | msgs/sec | MB/sec |
|-------------:|-------:|---------:|---------:|-------:|
| 32 B         | 20,000 | 200,000  | —        | —      |
| 256 B        | 20,000 | 200,000  | —        | —      |
| 1,024 B      | 20,000 | 200,000  | —        | —      |
| 4,096 B      | 20,000 | 200,000  | —        | —      |
| 16,384 B     | 20,000 | 200,000  | —        | —      |

## Backpressure Onset (128B, escalating load)

Topology: 1 broker + slow consumer (1ms delay) + ping producer

| Message count | msg size | achieved msgs/sec | p99 (ns) | send failures | notes |
|--------------:|---------:|------------------:|---------:|--------------:|-------|
| 1,000         | 128 B    | —                 | —        | —             | —     |
| 5,000         | 128 B    | —                 | —        | —             | —     |
| 10,000        | 128 B    | —                 | —        | —             | —     |
| 25,000        | 128 B    | —                 | —        | —             | —     |
| 50,000        | 128 B    | —                 | —        | —             | —     |
| 100,000       | 128 B    | —                 | —        | —             | —     |

## Recovery Time

| Scenario | Recovery time (ms) | Notes |
|----------|-------------------:|-------|
| Service crash → replacement ready | — | Includes heartbeat timeout (~10s) |
| Service kill → replacement ready  | — | Includes heartbeat timeout (~10s) |
| Broker restart → messaging resumes | — | Includes cluster re-formation |
| Rapid restart (5 iterations avg)   | — | Average per-iteration recovery |

---

## Change Log

| Date | Change | Impact | Commit |
|------|--------|--------|--------|
| 2026-04-11 | Initial baseline created | No results yet — broker runtime pending | — |

<!-- Template for adding new entries:
| YYYY-MM-DD | Description of change | e.g. "p99 latency -15%" | abc1234 |
-->
