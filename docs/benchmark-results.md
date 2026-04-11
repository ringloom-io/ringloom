# Benchmark Results — BRZ Broker

Tracked performance benchmark results for regression detection and improvement
comparison. Update this file after each significant performance-related change.

> **How to run:** `zig build perf` (see `docs/testing.md` for full instructions)

---

## Current Baseline

**Date:** 2025-07-10
**Zig version:** 0.15.2
**Build mode:** ReleaseFast
**OS:** Linux x86_64
**Status:** Local IPC and cross-broker TCP transport fully operational.
Per-message send-latency histograms instrumented in the ping service.

---

## Local IPC Latency (single broker, same host)

Topology: 1 broker + ping service + echo service

| Message size | Warmup |  Measured |   msgs/sec |    MB/sec | p50 (ns) | p95 (ns) | p99 (ns) | max (ns) |
|-------------:|-------:|----------:|-----------:|----------:|---------:|---------:|---------:|---------:|
| 32 B         | 50,000 |  500,000  | 11,180,679 |    341.1  |       70 |       81 |      100 |    6,532 |
| 128 B        | 50,000 |  500,000  | 10,669,853 |  1,302.5  |       70 |       90 |      140 |    5,620 |
| 512 B        | 50,000 |  500,000  |  9,570,660 |  4,673.2  |       80 |      121 |      170 |   10,459 |
| 1,024 B      | 50,000 |  500,000  |  6,764,160 |  6,605.6  |      120 |      210 |      250 |   20,228 |
| 4,096 B      | 50,000 |  499,911  |  6,931,078 | 27,074.5  |      100 |      321 |      370 |    9,497 |

> 4,096 B: 89 send failures (ring buffer full at 4 MB capacity).

## Local IPC Throughput (single broker, same host)

Topology: 1 broker + ping service + echo service

| Message size | Warmup |  Measured |   msgs/sec |    MB/sec |
|-------------:|-------:|----------:|-----------:|----------:|
| 32 B         | 50,000 |  500,000  | 11,180,679 |    341.1  |
| 128 B        | 50,000 |  500,000  | 10,669,853 |  1,302.5  |
| 512 B        | 50,000 |  500,000  |  9,570,660 |  4,673.2  |
| 1,024 B      | 50,000 |  500,000  |  6,764,160 |  6,605.6  |
| 4,096 B      | 50,000 |  499,911  |  6,931,078 | 27,074.5  |

> Throughput and latency runs are unified — the same run captures both.

## Cross-Broker Latency (two brokers on loopback)

Topology: broker A (node 1) ↔ broker B (node 2), ping on A, echo on B

| Message size | Warmup |    Sent |  Failed | p50 (ns) | p95 (ns) | p99 (ns) |  max (ns) |
|-------------:|-------:|--------:|--------:|---------:|---------:|---------:|----------:|
| 32 B         | 20,000 |  72,831 | 127,169 |       30 |       40 |       50 |    14,807 |
| 128 B        | 20,000 |  32,037 | 167,963 |       30 |       41 |       60 |    29,474 |
| 512 B        | 20,000 |  12,062 | 187,938 |       31 |       41 |       60 |       290 |
| 1,024 B      | 20,000 |   7,939 | 192,061 |       31 |       50 |       91 |       811 |
| 4,096 B      | 20,000 |   3,889 | 196,111 |       70 |      200 |      320 |     5,821 |

> Latency measures time to write into the send ring buffer (not end-to-end TCP
> round-trip). High failure counts reflect ring-buffer backpressure: the sender
> event loop flushes to TCP slower than the producer writes, so the buffer fills
> and subsequent sends fail with `BufferFull`.

## Cross-Broker Throughput (two brokers on loopback)

Topology: broker A (node 1) ↔ broker B (node 2), ping on A, echo on B

| Message size | Warmup |    Sent |  Failed |   msgs/sec |    MB/sec |
|-------------:|-------:|--------:|--------:|-----------:|----------:|
| 32 B         | 20,000 |  72,831 | 127,169 | 10,891,431 |    332.3  |
| 128 B        | 20,000 |  32,037 | 167,963 |  5,652,258 |    690.0  |
| 512 B        | 20,000 |  12,062 | 187,938 |  2,734,527 |  1,335.2  |
| 1,024 B      | 20,000 |   7,939 | 192,061 |  1,841,995 |  1,798.8  |
| 4,096 B      | 20,000 |   3,889 | 196,111 |    858,688 |  3,354.3  |

> Throughput is the peak burst rate of successful sends. The high failure
> counts are expected: the producer saturates the ring buffer far faster than
> TCP can drain it. In a real deployment, application-level flow control
> (retry loops, rate limiting) would smooth the effective throughput.

## Backpressure Onset (escalating load)

Topology: 1 broker + slow consumer (configurable delay) + ping producer

### 128 B messages — onset by count

| Message count | msg size | sent    | send failures | achieved msgs/sec |
|--------------:|---------:|--------:|--------------:|------------------:|
| 1,000         | 128 B    |   1,000 |             0 |         5,524,861 |
| 5,000         | 128 B    |   5,000 |             0 |         7,812,500 |
| 10,000        | 128 B    |   2,833 |         7,167 |         1,812,539 |
| 25,000        | 128 B    |     256 |        24,744 |           102,154 |
| 50,000        | 128 B    |       0 |        50,000 |                 0 |
| 100,000       | 128 B    |       0 |       100,000 |                 0 |

> Ring buffer (1 MB) saturates between 5K–10K messages at 128 B. Beyond that,
> the slow consumer cannot drain fast enough and sends fail with BufferFull.

### 1,024 B messages — onset by count

| Message count | msg size | sent  | send failures | achieved msgs/sec |
|--------------:|---------:|------:|--------------:|------------------:|
| 1,000         | 1,024 B  | 1,000 |             0 |         2,849,002 |
| 5,000         | 1,024 B  |   583 |         4,417 |           686,690 |
| 10,000        | 1,024 B  |   512 |         9,488 |           333,550 |
| 25,000        | 1,024 B  |   256 |        24,744 |            95,952 |
| 50,000        | 1,024 B  |     0 |        50,000 |                 0 |

### 4,096 B messages — onset by count

| Message count | msg size | sent  | send failures | achieved msgs/sec |
|--------------:|---------:|------:|--------------:|------------------:|
| 1,000         | 4,096 B  |   255 |           745 |           894,736 |
| 5,000         | 4,096 B  |   255 |         4,745 |           261,538 |
| 10,000        | 4,096 B  |   255 |         9,745 |           211,442 |
| 25,000        | 4,096 B  |   255 |        24,745 |            81,417 |

> At 4,096 B, the 1 MB ring buffer holds ~255 messages. Once full, all
> additional sends fail until the consumer drains entries.

### Consumer delay sweep (10,000 msgs × 128 B)

| Delay | sent  | send failures | achieved msgs/sec |
|------:|------:|--------------:|------------------:|
|  0 ms | 8,279 |         1,721 |         6,836,498 |
|  1 ms | 7,710 |         2,290 |         7,079,889 |
|  2 ms | 7,710 |         2,290 |         5,191,919 |
|  5 ms | 7,710 |         2,290 |         5,648,351 |
| 10 ms | 7,710 |         2,290 |         6,727,748 |

### Sustained backpressure (50,000 msgs × 128 B, 1 ms consumer delay)

| Metric         | Value      |
|----------------|------------|
| Sent           |      3,971 |
| Send failures  |     46,029 |
| Achieved rate  | 653,770 msgs/sec |

## Recovery Time

| Scenario | Recovery time (ms) | Notes |
|----------|-------------------:|-------|
| Service crash → replacement ready | 12,051 | Includes heartbeat timeout (~10 s) |
| Service kill → replacement ready  | 12,051 | Includes heartbeat timeout (~10 s) |
| Broker restart → messaging resumes | 5,102 | Includes cluster re-formation |
| Rapid restart (5 iterations avg)   | 12,051 | Average per-iteration recovery |
| Local survive remote kill          | n/a | 5,000 msgs sent at 4.1M msgs/s — local path unaffected |

---

## Change Log

| Date | Change | Impact | Commit |
|------|--------|--------|--------|
| 2026-04-11 | Initial baseline created | No results yet — broker runtime pending | — |
| 2025-07-08 | Local IPC path fixed; first real benchmark results | Local: 5.8M msgs/s (32B), 9.3 GB/s (4096B) | — |
| 2025-07-10 | Cross-broker TCP transport + service discovery + per-message latency | Local: 11.2M msgs/s (32B); Remote: 10.9M burst msgs/s (32B); latency p50=70ns local, 30ns remote | — |

<!-- Template for adding new entries:
| YYYY-MM-DD | Description of change | e.g. "p99 latency -15%" | abc1234 |
-->
