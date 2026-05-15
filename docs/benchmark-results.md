# Benchmark Results — RingLoom Broker

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
**Status:** `scripts/run-benchmarks.sh` now captures paced **transit
latency** and separate saturated benchmark runs. Benchmark tracing uses a
stable `CLOCK_MONOTONIC`-backed clock and remains gated behind an explicit
benchmark-only config flag.

These results were collected on an **untuned shared dev machine** (SMT enabled,
no isolated CPUs, turbo boost enabled), so they are useful for tracking current
behavior and queueing breakdowns but are **not** directly comparable to the
older baseline.

`scripts/run-benchmarks.sh` now captures both **transit latency** and
**saturated queueing** data in one pass. Remote ping-side send metrics are
intentionally excluded from this baseline because they only measure enqueue
into broker A's local ring buffer. `scripts/bench-single-size.sh` remains the
better tool for best-of-N per-size runs on a quieter machine.

### Reliable UDP comparison (2026-05-15)

Command:

```bash
zig build install -Doptimize=ReleaseFast && zig build test-bins -Doptimize=ReleaseFast
./scripts/run-benchmarks.sh --output-dir /tmp/ringloom-bench-results-new
```

This rerun was collected after the TCP → reliable-UDP cutover on the same kind
of **untuned shared dev workstation**. The new run produced **materially better
cross-broker transit latency** for 32 B–1024 B messages, but local throughput,
cross-broker saturated queueing latency, and 4,096 B remote transit were not
better in the same run. The full 2026-05-02 baseline tables therefore remain
the published all-suite reference, with the improved remote-transit comparison
captured here.

| Message size | Old p50 (ns) | New p50 (ns) | Δ p50 | Old p95 (ns) | New p95 (ns) | Δ p95 | Old p99 (ns) | New p99 (ns) | Δ p99 |
|-------------:|-------------:|-------------:|------:|-------------:|-------------:|------:|-------------:|-------------:|------:|
| 32 B         |        8,946 |        5,400 | -39.6% |       12,864 |        6,532 | -49.2% |      224,326 |        7,453 | -96.7% |
| 128 B        |        8,786 |        5,470 | -37.7% |       12,974 |        6,833 | -47.3% |      246,978 |        8,105 | -96.7% |
| 512 B        |        8,426 |        5,530 | -34.4% |       14,888 |        6,943 | -53.4% |      315,475 |        8,405 | -97.3% |
| 1,024 B      |       10,440 |        6,742 | -35.4% |       19,526 |        8,145 | -58.3% |      316,437 |       10,370 | -96.7% |

---

## Local IPC Transit Latency (single broker, same host)

Topology: 1 broker + ping service + echo service

End-to-end one-way latency measured at the echo (receiver) service during the
**paced transit** run (`send_interval_ns=10000`). The ping service embeds a
monotonic timestamp in each message payload; the echo service reads it on
receipt and records the difference. This measures the full path:
`ping → ring buffer → echo`.

| Message size | Warmup |  Measured | Sent | Failed | p50 (ns) | p95 (ns) | p99 (ns) | p99.9 (ns) | max (ns) |
|-------------:|-------:|----------:|-----:|-------:|---------:|---------:|---------:|-----------:|---------:|
| 32 B         | 10,000 |  100,000  | 100K |      0 |      391 |      632 |      741 |      3,296 |   64,309 |
| 128 B        | 10,000 |  100,000  | 100K |      0 |      410 |      651 |      752 |      1,042 |   67,765 |
| 512 B        | 10,000 |  100,000  | 100K |      0 |      441 |      712 |      861 |      1,052 |   67,124 |
| 1,024 B      | 10,000 |  100,000  | 100K |      0 |      491 |      761 |      891 |      1,182 |   61,814 |
| 4,096 B      |  5,000 |   50,000  |  50K |      0 |      591 |      912 |    1,062 |      1,563 |   70,801 |

> With pacing enabled, local single-broker transit stays comfortably
> sub-microsecond at p50 across all tested sizes. The older large-message local
> spikes were measurement artifacts from using saturated runs as a latency
> baseline rather than unloaded transit.

## Local IPC Throughput (single broker, same host)

Topology: 1 broker + ping service + echo service

| Message size | Warmup |  Measured |   msgs/sec |    MB/sec |
|-------------:|-------:|----------:|-----------:|----------:|
| 32 B         | 10,000 |  100,000  | 13,825,521 |    442.4  |
| 128 B        | 10,000 |  100,000  | 14,144,271 |  1,810.5  |
| 512 B        | 10,000 |  100,000  | 11,971,746 |  6,129.5  |
| 1,024 B      | 10,000 |  100,000  |  7,179,781 |  7,352.1  |
| 4,096 B      |  5,000 |   50,000  |  2,112,646 |  8,653.4  |

> Throughput now comes from a separate **saturated benchmark** run, so the local
> throughput table and local transit-latency table no longer describe the same
> execution mode. That split makes each metric much easier to interpret.

## Cross-Broker Transit Latency (two brokers on loopback)

Topology: broker A (node 1) ↔ broker B (node 2), ping on A, echo on B

End-to-end latency measured at the echo (receiver) service on broker B during
the **paced transit** run (`send_interval_ns=10000`). The ping service on
broker A embeds a monotonic timestamp in each message. Full path:
`ping → ring buffer → broker A sender → reliable UDP → broker B receiver → ring buffer → echo`.

| Message size | Warmup |    Sent | Received | Measured | p50 (ns) | p95 (ns) | p99 (ns) | p99.9 (ns) |   max (ns) |
|-------------:|-------:|--------:|---------:|---------:|---------:|---------:|---------:|-----------:|-----------:|
| 32 B         | 10,000 | 100,000 |  110,000 |  100,000 |    8,946 |   12,864 |  224,326 |    418,376 |    738,619 |
| 128 B        | 10,000 | 100,000 |  110,000 |  100,000 |    8,786 |   12,974 |  246,978 |    467,666 |    634,185 |
| 512 B        | 10,000 | 100,000 |  110,000 |  100,000 |    8,426 |   14,888 |  315,475 |  1,463,623 |  2,448,518 |
| 1,024 B      | 10,000 | 100,000 |  110,000 |  100,000 |   10,440 |   19,526 |  316,437 |    562,983 |    880,943 |
| 4,096 B      |  5,000 |  50,000 |   55,000 |   50,000 |   12,193 |   18,655 |  246,587 |    493,735 |    880,632 |

> This is the automated harness's new **meaningful cross-broker latency**
> baseline: p50 is now in the **8.4–12.2 µs** range instead of the previous
> millisecond-scale saturated queue depth.
>
> The stage breakdown shows that unloaded transit is dominated by the transport
> hop, not broker-local queueing:
> - **32 B:** broker A queue 0.54 µs, transport 7.91 µs, broker B delivery 0.49 µs
> - **128 B:** broker A queue 0.54 µs, transport 7.75 µs, broker B delivery 0.48 µs
> - **512 B:** broker A queue 0.59 µs, transport 7.27 µs, broker B delivery 0.52 µs
> - **1,024 B:** broker A queue 0.75 µs, transport 9.08 µs, broker B delivery 0.60 µs
> - **4,096 B:** broker A queue 1.08 µs, transport 10.32 µs, broker B delivery 0.63 µs

## Cross-Broker Saturated Queueing Latency (two brokers on loopback)

Topology: broker A (node 1) ↔ broker B (node 2), ping on A, echo on B

End-to-end latency measured at the echo (receiver) service on broker B during
the separate **saturated benchmark** run (`send_interval_ns=0`). The ping
service on broker A embeds a monotonic timestamp in each message. Full path:
`ping → ring buffer → broker A sender → reliable UDP → broker B receiver → ring buffer → echo`.

| Message size | Warmup |    Sent | Received | Measured | p50 (ns) | p95 (ns) | p99 (ns) | p99.9 (ns) |    max (ns) |
|-------------:|-------:|--------:|---------:|---------:|---------:|---------:|---------:|-----------:|------------:|
| 32 B         | 10,000 | 100,000 |  110,000 |  100,000 |  7,709,750 | 11,769,826 | 12,113,453 | 12,214,110 | 12,241,902 |
| 128 B        | 10,000 | 100,000 |  109,117 |  100,000 |  3,470,603 |  5,322,356 |  5,473,135 |  5,744,157 |  5,768,382 |
| 512 B        | 10,000 | 100,000 |  102,889 |  100,000 |  1,244,096 |  1,604,544 |  1,926,731 |  2,065,619 |  2,101,695 |
| 1,024 B      | 10,000 | 100,000 |  101,787 |  100,000 |    636,379 |    791,617 |    874,892 |  1,004,452 |  1,013,178 |
| 4,096 B      |  5,000 |  50,000 |   50,448 |   50,000 |    756,843 |  1,002,799 |  1,228,968 |  1,381,941 |  1,432,074 |

> The stable-clock refresh still makes the saturated queueing story clear:
> broker **B local delivery remains microsecond-scale**, while the measured p50
> is dominated by **broker A queueing + transport backlog**.
>
> On this untuned machine, 32 B is worst (~7.7 ms p50) because the sender can
> inject it fastest, creating the deepest standing queue. By 512 B–4096 B the
> sender slows down enough that queueing collapses into the sub-millisecond
> range even though send-side latency stays tiny.
>
> All measured samples were collected for every size. `total_received` can land
> slightly below warmup+measured on some saturated runs because shutdown races
> with trailing warmup echoes, but the measured benchmark window completed.
>
> Representative p50 breakdowns from the saturated harness:
> - **32 B:** broker A queue 2.18 ms, transport 5.27 ms, broker B delivery 0.37 µs
> - **128 B:** broker A queue 1.43 ms, transport 2.54 ms, broker B delivery 0.29 µs
> - **512 B:** broker A queue 1.10 ms, transport 112.27 µs, broker B delivery 1.08 µs
> - **1,024 B:** broker A queue 526.92 µs, transport 105.85 µs, broker B delivery 2.64 µs
> - **4,096 B:** broker A queue 332.36 µs, transport 422.40 µs, broker B delivery 1.10 µs

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
| 2026-05-02 | `run-benchmarks.sh` split into paced transit and saturated benchmark passes | Automated baseline now reports remote transit p50 **8.4–12.2 µs** while still keeping saturated queueing p50 **0.64–7.71 ms** and throughput data in the same harness output | 
| 2026-05-15 | Reliable UDP cutover comparison rerun | Cross-broker transit latency improved for 32 B–1024 B by ~34–40% at p50 and ~47–58% at p95; saturated queueing and 4,096 B transit were not better in the same untuned run, so the older full-suite baseline stays published | — |

<!-- Template for adding new entries:
| YYYY-MM-DD | Description of change | e.g. "p99 latency -15%" | abc1234 |
-->
