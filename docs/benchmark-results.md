# Benchmark Results — RingLoom Broker

Tracked performance benchmark results for regression detection and improvement
comparison. Update this file after each significant performance-related change.

> **How to run:** `scripts/run-benchmarks.sh` or `zig build perf`
> (see `docs/testing.md` for full instructions)

---

## Current Baseline

**Date:** 2026-05-18
**Zig version:** 0.16.0
**Build mode:** ReleaseFast
**OS:** Linux x86_64 (shared development workstation, untuned)
**Status:** Direct-UDP-only remote sends are enabled. `scripts/run-benchmarks.sh`
captures paced **transit latency** and separate saturated benchmark runs, and
`zig build perf-aeron` captures the raw Aeron UDP reference. The documented
validation suite (`zig build test`, `zig build e2e`, `zig build perf`, and
`zig build perf-aeron`) passed on this run.

These results were collected on an **untuned shared dev machine** (SMT enabled,
no isolated CPUs, turbo boost enabled), so they are useful for tracking current
behavior and queueing breakdowns but are **not** directly comparable to the
older baseline.

`scripts/run-benchmarks.sh` now captures both **transit latency** and
**saturated queueing** data in one pass. Remote ping-side send metrics are
intentionally excluded from this baseline because they only measure enqueue
into broker A's local ring buffer. `scripts/bench-single-size.sh` remains the
better tool for best-of-N per-size runs on a quieter machine.

On the new direct-UDP remote path, the old broker-ingress stage tracing no
longer maps cleanly onto the data plane. The current automated harness therefore
records only end-to-end remote latency (`stage_breakdown_measured = 0`) for the
cross-broker sections below.

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
| 32 B         | 10,000 |  100,000  | 100K |      0 |      721 |    1,022 |    1,222 |      3,316 |   70,280 |
| 128 B        | 10,000 |  100,000  | 100K |      0 |      731 |    1,002 |    1,112 |      2,865 |   60,222 |
| 512 B        | 10,000 |  100,000  | 100K |      0 |      762 |    1,032 |    1,172 |      1,984 |   75,229 |
| 1,024 B      | 10,000 |  100,000  | 100K |      0 |      811 |    1,082 |    1,232 |      2,515 |   62,476 |
| 4,096 B      |  5,000 |   50,000  |  50K |      0 |      902 |    1,212 |    1,372 |      3,246 |   61,083 |

> With pacing enabled, local single-broker transit stays comfortably
> sub-microsecond at p50 across all tested sizes. The older large-message local
> spikes were measurement artifacts from using saturated runs as a latency
> baseline rather than unloaded transit.

## Local IPC Throughput (single broker, same host)

Topology: 1 broker + ping service + echo service

| Message size | Warmup |  Measured |   msgs/sec |    MB/sec |
|-------------:|-------:|----------:|-----------:|----------:|
| 32 B         | 10,000 |  100,000  |  2,341,975 |     74.9  |
| 128 B        | 10,000 |  100,000  |  1,722,623 |    220.5  |
| 512 B        | 10,000 |  100,000  |  2,520,478 |  1,290.5  |
| 1,024 B      | 10,000 |  100,000  |  2,022,571 |  2,071.1  |
| 4,096 B      |  5,000 |   50,000  |  1,271,682 |  5,208.8  |

> Throughput now comes from a separate **saturated benchmark** run, so the local
> throughput table and local transit-latency table no longer describe the same
> execution mode. That split makes each metric much easier to interpret.

## Cross-Broker Transit Latency (two brokers on loopback)

Topology: broker A (node 1) ↔ broker B (node 2), ping on A, echo on B

End-to-end latency measured at the echo (receiver) service on broker B during
the **paced transit** run (`send_interval_ns=10000`). The ping service on
broker A embeds a monotonic timestamp in each message. Full path:
`ping → direct Aeron UDP publication → broker B receiver → ring buffer → echo`.

| Message size | Warmup |    Sent | Received | Measured | p50 (ns) | p95 (ns) | p99 (ns) | p99.9 (ns) |   max (ns) |
|-------------:|-------:|--------:|---------:|---------:|---------:|---------:|---------:|-----------:|-----------:|
| 32 B         | 10,000 | 100,000 |  110,000 |  100,000 |    6,502 |    8,025 |    9,237 |     33,252 |    356,813 |
| 128 B        | 10,000 | 100,000 |  110,000 |  100,000 |    6,593 |    8,205 |    9,548 |     58,178 |    269,581 |
| 512 B        | 10,000 | 100,000 |  110,000 |  100,000 |    6,031 |    7,785 |    9,608 |     80,950 |    392,298 |
| 1,024 B      | 10,000 | 100,000 |  110,000 |  100,000 |    6,011 |    8,146 |    9,738 |     75,550 |    308,693 |
| 4,096 B      |  5,000 |  50,000 |   55,000 |   50,000 |    8,115 |   10,991 |   14,527 |    108,682 |    400,283 |

> This is the first direct-UDP-only cross-broker baseline after removing broker
> ingress fallback from the data path: p50 is now in the **6.0–8.1 µs** range.
>
> The remote stage-breakdown fields are currently all zero
> (`stage_breakdown_measured = 0`) on this path, so this section records only
> end-to-end latency.

## Plain Aeron Remote Transit Reference (two media drivers on loopback)

Topology: Aeron media driver A ↔ Aeron media driver B over UDP loopback, ping
client on A, echo subscription on B. Both embedded media drivers use
`DEDICATED` threading with separate conductor, sender, and receiver agent
threads.

Command: `zig build perf-aeron`

| Message size | Warmup |    Sent | Received | Measured | p50 (ns) | p95 (ns) | p99 (ns) | p99.9 (ns) | max (ns) |
|-------------:|-------:|--------:|---------:|---------:|---------:|---------:|---------:|-----------:|---------:|
| 32 B         | 10,000 | 110,000 |  110,000 |  100,000 |    5,400 |    6,823 |    7,865 |     11,371 |   37,339 |
| 128 B        | 10,000 | 110,000 |  110,000 |  100,000 |    5,470 |    6,522 |    7,354 |     10,129 |   30,427 |
| 512 B        | 10,000 | 110,000 |  110,000 |  100,000 |    5,581 |    6,752 |    8,245 |     12,704 |   36,317 |
| 1,024 B      | 10,000 | 110,000 |  110,000 |  100,000 |    5,641 |    6,582 |    7,294 |     11,592 |  173,251 |
| 4,096 B      |  5,000 |  55,000 |   55,000 |   50,000 |    6,623 |    8,115 |    9,527 |     12,654 |   30,536 |

> Compared with RingLoom remote transit from the same run, plain Aeron is about
> **0.4–1.5 µs lower at p50**. That remaining gap is the current rough cost of
> broker-B routing, service delivery handoff, and the extra RingLoom framing
> around the raw Aeron UDP path.

## Cross-Broker Saturated Queueing Latency (two brokers on loopback)

Topology: broker A (node 1) ↔ broker B (node 2), ping on A, echo on B

End-to-end latency measured at the echo (receiver) service on broker B during
the separate **saturated benchmark** run (`send_interval_ns=0`). The ping
service on broker A embeds a monotonic timestamp in each message. Full path:
`ping → direct Aeron UDP publication → broker B receiver → ring buffer → echo`.

| Message size | Warmup |    Sent | Received | Measured | p50 (ns) | p95 (ns) | p99 (ns) | p99.9 (ns) |    max (ns) |
|-------------:|-------:|--------:|---------:|---------:|---------:|---------:|---------:|-----------:|------------:|
| 32 B         | 10,000 | 100,000 |  110,000 |  100,000 |    481,664 |    692,596 |    759,130 |    854,155 |    868,422 |
| 128 B        | 10,000 | 100,000 |  110,000 |  100,000 |    262,978 |    386,938 |    410,252 |    489,819 |    503,184 |
| 512 B        | 10,000 | 100,000 |  110,000 |  100,000 |    134,500 |    211,953 |    244,133 |    251,066 |    308,623 |
| 1,024 B      | 10,000 | 100,000 |  110,000 |  100,000 |  8,827,466 |  9,722,739 |  9,860,925 |  9,882,645 | 10,008,479 |
| 4,096 B      |  5,000 |  50,000 |   55,000 |   50,000 |  7,038,175 |  7,554,593 |  7,621,107 |  7,641,244 |  7,695,104 |

> The direct-UDP path materially changes the small/medium saturated story on this
> untuned machine: **32 B–512 B now stay in the ~0.13–0.48 ms p50 range**, while
> **1,024 B–4,096 B** still accumulate multi-millisecond backlog.
>
> All measured samples were collected for every size. `total_received` can land
> slightly below warmup+measured on some saturated runs because shutdown races
> with trailing warmup echoes, but the measured benchmark window completed.
>
> As with the paced remote runs, the cross-broker direct-UDP benchmark currently
> reports only end-to-end latency and leaves stage-breakdown fields at zero.

> **Note:** `zig build perf` now persists reusable backpressure and recovery
> artifacts under `/tmp/ringloom-perf-results/{backpressure,recovery}`, so the
> tables below are refreshed from the same 2026-05-18 run as the other sections.

## Backpressure Onset (escalating load)

Topology: 1 broker + slow consumer (configurable delay) + ping producer

### 128 B messages — onset by count

| Message count | msg size | sent    | send failures | achieved msgs/sec |
|--------------:|---------:|--------:|--------------:|------------------:|
| 1,000         | 128 B    |   1,000 |             0 |           576,701 |
| 5,000         | 128 B    |   5,000 |             0 |           730,673 |
| 10,000        | 128 B    |   2,736 |         7,264 |           267,135 |
| 25,000        | 128 B    |     512 |        24,488 |            26,589 |
| 50,000        | 128 B    |       0 |        50,000 |                 0 |
| 100,000       | 128 B    |       0 |       100,000 |                 0 |

> Ring buffer (1 MB) saturates between 5K–10K messages at 128 B. Beyond that,
> the slow consumer cannot drain fast enough and sends fail with BufferFull.

### 1,024 B messages — onset by count

| Message count | msg size | sent  | send failures | achieved msgs/sec |
|--------------:|---------:|------:|--------------:|------------------:|
| 1,000         | 1,024 B  | 1,000 |             0 |           584,112 |
| 5,000         | 1,024 B  |   537 |         4,463 |           120,837 |
| 10,000        | 1,024 B  |   512 |         9,488 |            61,895 |
| 25,000        | 1,024 B  |   256 |        24,744 |            12,450 |
| 50,000        | 1,024 B  |     0 |        50,000 |                 0 |

### 4,096 B messages — onset by count

| Message count | msg size | sent  | send failures | achieved msgs/sec |
|--------------:|---------:|------:|--------------:|------------------:|
| 1,000         | 4,096 B  |   255 |           745 |           191,154 |
| 5,000         | 4,096 B  |   255 |         4,745 |            51,703 |
| 10,000        | 4,096 B  |   255 |         9,745 |            28,970 |
| 25,000        | 4,096 B  |   255 |        24,745 |            12,709 |

> At 4,096 B, the 1 MB ring buffer holds ~255 messages. Once full, all
> additional sends fail until the consumer drains entries.

### Consumer delay sweep (10,000 msgs × 128 B)

| Delay | sent  | send failures | achieved msgs/sec |
|------:|------:|--------------:|------------------:|
|  0 ms | 9,219 |           781 |           754,666 |
|  1 ms | 7,713 |         2,287 |           668,139 |
|  2 ms | 7,710 |         2,290 |           697,927 |
|  5 ms | 7,710 |         2,290 |           605,703 |
| 10 ms | 7,710 |         2,290 |           625,608 |

### Sustained backpressure (50,000 msgs × 256 B, 5 ms consumer delay)

| Metric         | Value      |
|----------------|------------|
| Sent           |      3,972 |
| Send failures  |     46,028 |
| Achieved rate  | 97,895 msgs/sec |

## Recovery Time

| Scenario | Recovery time (ms) | Notes |
|----------|-------------------:|-------|
| Service crash → replacement ready | 12,050 | Includes heartbeat timeout (~10 s) |
| Service kill → replacement ready  | 12,050 | Includes heartbeat timeout (~10 s) |
| Broker restart → messaging resumes | 12,263 | Measured until the first successful 100-message post-restart probe |
| Rapid restart (5 iterations avg)   | 12,050 | Average per-iteration recovery |
| Local survive remote kill          | n/a | 5,000 msgs sent at 732,600 msgs/s — local path unaffected |

> The broker-restart recovery benchmark now waits for a real successful
> cross-broker probe instead of stopping at echo re-registration. On this
> direct-UDP baseline, that puts restart recovery in the same ~12 s envelope as
> the heartbeat-driven service recovery cases.

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
| 2026-05-02 | `run-benchmarks.sh` split into paced transit and saturated benchmark passes | Automated baseline now reports remote transit p50 **8.4–12.2 µs** while still keeping saturated queueing p50 **0.64–7.71 ms** and throughput data in the same harness output | — |
| 2026-05-17 | Aeron v2 cleanup rerun after shared threading helper move and hot-path scratch-buffer cleanup | Remote transit p50 improved to **7.0–9.0 µs**; 4 KiB transit p50 is **9.0 µs**. Saturated 512 B–4 KiB remains transport-backlog dominated at **8.8–12.3 ms** p50 on the untuned workstation | — |
| 2026-05-17 | Added plain Aeron two-driver remote transit comparison benchmark | Plain Aeron p50 is **5.4–6.6 µs**, roughly **1.5–2.4 µs** below RingLoom remote transit on the same workstation | — |
| 2026-05-18 | Removed broker-ingress fallback and switched remote sends to direct UDP only; reran documented tests plus manual harness | Remote transit p50 is now **6.0–8.1 µs** and the plain-Aeron gap shrank to **0.4–1.5 µs**. Saturated 32 B–512 B queueing dropped to **0.13–0.48 ms** p50, while 1 KiB–4 KiB remains backlog-dominated at **7.0–8.8 ms** p50 on the untuned workstation | — |
| 2026-05-18 | Persisted backpressure/recovery perf JSON and tightened broker-restart recovery measurement to require a successful post-restart probe | Backpressure tables below are refreshed from reusable artifacts, and broker restart recovery now measures **12.3 s** rather than the older under-measured **5.1 s** | — |

<!-- Template for adding new entries:
| YYYY-MM-DD | Description of change | e.g. "p99 latency -15%" | abc1234 |
-->
