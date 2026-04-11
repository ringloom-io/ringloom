# Benchmark Results — BRZ Broker

Tracked performance benchmark results for regression detection and improvement
comparison. Update this file after each significant performance-related change.

> **How to run:** `scripts/run-benchmarks.sh` or `zig build perf`
> (see `docs/testing.md` for full instructions)

---

## Current Baseline

**Date:** 2025-07-10
**Zig version:** 0.15.2
**Build mode:** ReleaseFast
**OS:** Linux x86_64
**Status:** Local IPC fully operational with end-to-end latency measurement.
Cross-broker TCP control plane operational; data-plane forwarding pending.
Spinning backpressure eliminates send failures on the local path.

---

## Local IPC Latency (single broker, same host)

Topology: 1 broker + ping service + echo service

End-to-end one-way latency measured at the echo (receiver) service. The ping
service embeds a monotonic timestamp in each message payload; the echo service
reads it on receipt and records the difference. This measures the full path:
`ping → ring buffer → broker → ring buffer → echo`.

| Message size | Warmup |  Measured | Sent | Failed | p50 (ns) | p95 (ns) | p99 (ns) | p99.9 (ns) | max (ns) |
|-------------:|-------:|----------:|-----:|-------:|---------:|---------:|---------:|-----------:|---------:|
| 32 B         | 10,000 |  100,000  | 100K |      0 |      340 |      421 |      581 |     59,651 |   85,920 |
| 128 B        | 10,000 |  100,000  | 100K |      0 |      351 |      481 |      651 |     24,766 |   68,046 |
| 512 B        | 10,000 |  100,000  | 100K |      0 |      401 |      451 |      520 |      3,377 |   35,316 |
| 1,024 B      | 10,000 |  100,000  | 100K |      0 |      451 |      541 |    1,022 |     23,213 |   41,076 |
| 4,096 B      |  5,000 |   50,000  |  50K |      0 |      671 |    1,243 |    1,353 |      9,428 |   19,697 |

> Spinning backpressure (100 ms timeout) ensures 0 send failures. The sender
> re-embeds the timestamp on each retry so latency excludes spin-wait time.
> p99.9 tail latency reflects occasional kernel scheduling jitter.

## Local IPC Throughput (single broker, same host)

Topology: 1 broker + ping service + echo service

| Message size | Warmup |  Measured |   msgs/sec |    MB/sec |
|-------------:|-------:|----------:|-----------:|----------:|
| 32 B         | 10,000 |  100,000  |  2,308,935 |     70.5  |
| 128 B        | 10,000 |  100,000  |  2,157,776 |    263.4  |
| 512 B        | 10,000 |  100,000  |  2,152,111 |  1,050.8  |
| 1,024 B      | 10,000 |  100,000  |  1,898,073 |  1,853.6  |
| 4,096 B      |  5,000 |   50,000  |  1,385,195 |  5,411.7  |

> Throughput and latency runs are unified — the same run captures both.
> Throughput is lower than prior baselines because spinning backpressure
> paces the sender to match the broker's consumer rate (0 failures).

## Cross-Broker Send Latency (two brokers on loopback)

Topology: broker A (node 1) ↔ broker B (node 2), ping on A, echo on B

Send-side latency: time for ping to write a message into the local broker's
ring buffer. With spinning backpressure, the sender retries on `BufferFull`
for up to 100 ms, eliminating send failures.

| Message size | Warmup |    Sent | Failed |   msgs/sec | p50 (ns) | p95 (ns) | p99 (ns) | p99.9 (ns) |  max (ns) |
|-------------:|-------:|--------:|-------:|-----------:|---------:|---------:|---------:|-----------:|----------:|
| 32 B         | 10,000 | 100,000 |      0 |  1,021,586 |      280 |      340 |   41,978 |     48,220 |   125,402 |
| 128 B        | 10,000 | 100,000 |      0 |    857,059 |      310 |      401 |   46,326 |     50,984 |    63,568 |
| 512 B        | 10,000 | 100,000 |      0 |    664,526 |      281 |      380 |   63,458 |     79,237 |   110,535 |
| 1,024 B      | 10,000 | 100,000 |      0 |    707,779 |      300 |      400 |   59,030 |     66,654 |    81,000 |
| 4,096 B      | 10,000 | 100,000 |      0 |    543,779 |      340 |      410 |   82,743 |     91,830 |   106,037 |

> **Known limitation:** The cross-broker data-plane (TCP message forwarding)
> does not yet deliver messages to the echo service on the remote broker.
> The control plane (peer discovery, service advertisement) works correctly.
> The latency numbers above reflect send-side ring buffer writes only. The
> high p99 (40–80 µs) is caused by spinning retries when the ring buffer is
> full and the broker's TCP sender is draining it.

## Cross-Broker Throughput (two brokers on loopback)

Topology: broker A (node 1) ↔ broker B (node 2), ping on A, echo on B

| Message size | Warmup |    Sent | Failed |   msgs/sec |    MB/sec |
|-------------:|-------:|--------:|-------:|-----------:|----------:|
| 32 B         | 10,000 | 100,000 |      0 |  1,021,586 |     31.2  |
| 128 B        | 10,000 | 100,000 |      0 |    857,059 |    104.6  |
| 512 B        | 10,000 | 100,000 |      0 |    664,526 |    324.5  |
| 1,024 B      | 10,000 | 100,000 |      0 |    707,779 |    691.2  |
| 4,096 B      | 10,000 | 100,000 |      0 |    543,779 |  2,124.1  |

> Throughput reflects the sustained send rate with spinning backpressure.
> All 100K messages are accepted into the local ring buffer (0 failures),
> but end-to-end delivery to the remote echo service is pending data-plane
> implementation.

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
| 2025-07-10 | End-to-end latency at echo side, spinning backpressure, p99.9 | Local e2e p50=340ns (32B); 0 send failures; cross-broker data-plane pending | — |

<!-- Template for adding new entries:
| YYYY-MM-DD | Description of change | e.g. "p99 latency -15%" | abc1234 |
-->
