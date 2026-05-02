# Benchmark Results — BRZ Broker

Tracked performance benchmark results for regression detection and improvement
comparison. Update this file after each significant performance-related change.

> **How to run:** `scripts/run-benchmarks.sh` or `zig build perf`
> (see `docs/testing.md` for full instructions)

---

## Current Baseline

**Date:** 2026-05-02
**Zig version:** 0.16.0
**Build mode:** ReleaseFast
**OS:** Linux x86_64 (shared development workstation, untuned)
**Status:** Benchmark harness refreshed after switching benchmark tracing to a
stable `CLOCK_MONOTONIC`-backed clock and gating broker-side tracing behind an
explicit benchmark-only config flag.

These results were collected on an **untuned shared dev machine** (SMT enabled,
no isolated CPUs, turbo boost enabled), so they are useful for tracking current
behavior and queueing breakdowns but are **not** directly comparable to the
older baseline.

`scripts/run-benchmarks.sh` still measures **saturated queueing latency**. For
unloaded cross-broker transit latency, use `scripts/bench-single-size.sh`,
which now defaults to paced transit mode.

---

## Local IPC Latency (single broker, same host)

Topology: 1 broker + ping service + echo service

End-to-end one-way latency measured at the echo (receiver) service. The ping
service embeds a monotonic timestamp in each message payload; the echo service
reads it on receipt and records the difference. This measures the full path:
`ping → ring buffer → broker → ring buffer → echo`.

| Message size | Warmup |  Measured | Sent | Failed | p50 (ns) | p95 (ns) | p99 (ns) | p99.9 (ns) | max (ns) |
|-------------:|-------:|----------:|-----:|-------:|---------:|---------:|---------:|-----------:|---------:|
| 32 B         | 10,000 |  100,000  | 100K |      0 |      310 |      631 |   69,378 |    110,906 |  121,425 |
| 128 B        | 10,000 |  100,000  | 100K |      0 |      310 |    1,052 |  108,101 |    133,507 |  137,044 |
| 512 B        | 10,000 |  100,000  | 100K |      0 |  142,234 |  145,981 |  147,453 |    148,615 |  149,507 |
| 1,024 B      | 10,000 |  100,000  | 100K |      0 |  101,558 |  105,475 |  107,159 |    109,082 |  110,094 |
| 4,096 B      |  5,000 |   50,000  |  50K |      0 |   44,743 |   73,706 |   76,011 |    102,771 |  119,982 |

> On this untuned machine, 32–128 B still show sub-microsecond local p50, but
> 512–4096 B reflect steady-state queueing because the saturated sender can
> outrun the consumer path. Use `bench-single-size.sh --local` for paced local
> transit measurements when you want unloaded latency instead of queueing.

## Local IPC Throughput (single broker, same host)

Topology: 1 broker + ping service + echo service

| Message size | Warmup |  Measured |   msgs/sec |    MB/sec |
|-------------:|-------:|----------:|-----------:|----------:|
| 32 B         | 10,000 |  100,000  | 13,762,730 |    420.0  |
| 128 B        | 10,000 |  100,000  | 12,459,506 |  1,520.9  |
| 512 B        | 10,000 |  100,000  | 12,218,963 |  5,966.3  |
| 1,024 B      | 10,000 |  100,000  |  7,231,180 |  7,061.7  |
| 4,096 B      |  5,000 |   50,000  |  2,093,890 |  8,179.3  |

> Throughput and latency runs are unified — the same saturated run captures
> both. The dev-machine refresh reached much higher burst rates than the older
> tuned baseline, but the corresponding local echo latency for 512–4096 B also
> shows queue buildup rather than unloaded transit time.

## Cross-Broker Send Latency (two brokers on loopback)

Topology: broker A (node 1) ↔ broker B (node 2), ping on A, echo on B

Send-side latency: time for ping to write a message into the local broker's
ring buffer. With spinning backpressure, the sender retries on `BufferFull`
for up to 100 ms, eliminating send failures.

| Message size | Warmup |    Sent | Failed |   msgs/sec | p50 (ns) | p95 (ns) | p99 (ns) | p99.9 (ns) |    max (ns) |
|-------------:|-------:|--------:|-------:|-----------:|---------:|---------:|---------:|-----------:|------------:|
| 32 B         | 10,000 | 100,000 |      0 |  5,319,714 |       20 |       31 |       41 |     23,544 |   1,160,909 |
| 128 B        | 10,000 | 100,000 |      0 |  4,045,798 |       30 |       40 |       41 |     68,087 |     517,585 |
| 512 B        | 10,000 | 100,000 |      0 |  2,490,039 |       30 |       40 |       51 |     85,208 |     183,611 |
| 1,024 B      | 10,000 | 100,000 |      0 |  1,268,327 |       39 |       50 |       70 |    189,682 |     326,347 |
| 4,096 B      |  5,000 |  50,000 |      0 |    434,722 |       80 |      111 |      210 |    561,431 |     885,341 |

> Send-side p50 remains sub-100 ns across all sizes (ring buffer write only).
> The lower sustained rates vs. the tuned baseline reflect the
> untuned machine and shared environment, not a regression in the ring-buffer
> write path itself.

## Cross-Broker Throughput (two brokers on loopback)

Topology: broker A (node 1) ↔ broker B (node 2), ping on A, echo on B

| Message size | Warmup |    Sent | Failed |   msgs/sec |    MB/sec |
|-------------:|-------:|--------:|-------:|-----------:|----------:|
| 32 B         | 10,000 | 100,000 |      0 |  5,319,714 |    162.3  |
| 128 B        | 10,000 | 100,000 |      0 |  4,045,798 |    493.9  |
| 512 B        | 10,000 | 100,000 |      0 |  2,490,039 |  1,215.8  |
| 1,024 B      | 10,000 | 100,000 |      0 |  1,268,327 |  1,238.6  |
| 4,096 B      |  5,000 |  50,000 |      0 |    434,722 |  1,698.1  |

> Throughput reflects the sustained send rate with spinning backpressure.
> All messages are accepted into the local ring buffer (0 failures).

## Cross-Broker Echo Queueing Latency (two brokers on loopback)

Topology: broker A (node 1) ↔ broker B (node 2), ping on A, echo on B

End-to-end latency measured at the echo (receiver) service on broker B while
the sender runs at saturation. The ping service on broker A embeds a monotonic
timestamp in each message. Full path: `ping → ring buffer → broker A sender →
TCP → broker B receiver → ring buffer → echo`.

| Message size | Warmup |    Sent | Received | Measured | p50 (ns) | p95 (ns) | p99 (ns) | p99.9 (ns) |    max (ns) |
|-------------:|-------:|--------:|---------:|---------:|---------:|---------:|---------:|-----------:|------------:|
| 32 B         | 10,000 | 100,000 |  110,000 |  100,000 | 10,102,654 | 12,230,970 | 12,763,017 | 12,878,201 | 12,880,455 |
| 128 B        | 10,000 | 100,000 |  109,639 |  100,000 |  4,501,408 |  5,137,727 |  5,275,923 |  5,352,315 |  5,375,117 |
| 512 B        | 10,000 | 100,000 |  103,139 |  100,000 |    809,641 |  1,165,711 |  1,371,543 |  1,757,599 |  1,774,440 |
| 1,024 B      | 10,000 | 100,000 |  101,643 |  100,000 |    871,836 |  1,096,203 |  1,284,301 |  1,455,940 |  1,597,963 |
| 4,096 B      |  5,000 |  50,000 |   50,458 |   50,000 |    766,542 |  1,063,262 |  1,280,485 |  1,453,295 |  1,495,784 |

> The stable-clock refresh makes the saturated queueing story much clearer:
> broker **B local delivery remains sub-5 µs p50** across all sizes, while the
> measured p50 is dominated by **broker A queueing + transport backlog**.
>
> On this untuned machine, 32 B is worst (~10.1 ms p50) because the sender can
> inject it fastest, creating the deepest standing queue. By 512 B–4096 B the
> sender slows down enough that queueing collapses into the sub-millisecond
> range even though send-side latency stays tiny.
>
> Delivery ratio is now effectively 100% in the refreshed runs because the
> receiver and echo service kept up well enough on this machine for the tested
> message counts.
>
> **Single-size script note:** `scripts/bench-single-size.sh` now defaults to a
> paced **transit latency** mode for unloaded measurements. Use
> `--latency-mode saturated` to reproduce the queueing-oriented numbers in this
> section.
>
> Representative p50 breakdowns from the refreshed harness:
> - **32 B:** broker A queue 3.26 ms, transport 5.95 ms, broker B delivery 0.35 µs
> - **128 B:** broker A queue 1.82 ms, transport 2.63 ms, broker B delivery 0.40 µs
> - **512 B:** broker A queue 736 µs, transport 73 µs, broker B delivery 0.67 µs

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
| 2025-07-11 | TCP pipeline optimizations: writev batching, buffered reads, rdtsc clock, SmpAllocator, io_uring SQPOLL, idle strategy tuning, flush-before-drain, backpressure | Cross-broker echo queueing p50 **2.9–3.4 ms** (first working end-to-end); send throughput 8× (1M → 8M msgs/s at 32B); send p99 1000× (42 µs → 41 ns) | — |
| 2026-05-02 | Single-size benchmark defaults to paced transit latency and records sender/transport/delivery stage breakdown | Remote single-size runs now distinguish unloaded transit from saturated queueing; stage timing points show where backlog accumulates | — |
| 2026-05-02 | Benchmark trace clock switched to stable `CLOCK_MONOTONIC`; broker-side tracing gated behind benchmark-only config; full harness rerun on untuned dev machine | Remote saturated queueing refresh: 32 B p50 **10.1 ms**, 128 B **4.5 ms**, 512 B **810 µs**. Local refresh shows burst throughput up to **13.8M msgs/s** (32 B) but saturated local latency is environment-sensitive on the untuned machine | — |

<!-- Template for adding new entries:
| YYYY-MM-DD | Description of change | e.g. "p99 latency -15%" | abc1234 |
-->
