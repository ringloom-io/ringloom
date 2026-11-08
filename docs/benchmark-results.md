# Benchmark Results — RingLoom Broker

Tracked performance benchmark results for regression detection and improvement
comparison. Update this file after each significant performance-related change.

> **Repeatable workflow:** [Benchmark guide](benchmarks.md), including agent reruns and report generation.
>
> **How to run:** `scripts/run-benchmarks.sh` or `zig build perf`
> (see `docs/testing.md` for full instructions)

---

## Historical baseline (2026-05-18)

This snapshot predates the topic and recovery measurement corrections. See the
new focused snapshot below; unchanged sections have not been refreshed.

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


## Focused snapshot — 2026-11-07

Ring and topic measurements after the stale-record zeroing and topic ack fixes,
on the dirty working tree based on `4f44189bfd0090411e8a5969900278e9e7d34c3a`.
These are exploratory shared-workstation results, not a claimed improvement over
the historical snapshot. Other suites above remain historical.

Command:

```sh
scripts/bench-all.sh --output-dir /tmp/ringloom-bench-review-002 --repeat 3 --suites 'ring topic'
```

Raw artifacts, logs and tracked-file diff are retained locally at
`/tmp/ringloom-bench-review-002/` (temporary storage; archive before cleanup).
The run uses single-broker topics, so replicate_once confirms local append,
not remote replication or fsync. Fire-and-forget includes a final acked sentinel.
1024-byte runs use 64,026 messages to stay below the queue cycle-file limit;
smaller sizes use 100,000. Each has 1,000 warmup messages. Values below are field
medians; columns can come from different runs. Some short-run metrics have large
spread, so use these for reproducing behavior, not small regression claims.

## Run metadata

- **Date:** 2026-11-07T18:28:11.691608+00:00
- **Commit:** 4f44189bfd0090411e8a5969900278e9e7d34c3a
- **Working tree dirty:** True
- **Repeats:** 3
- **Environment overrides:** {}
- **Zig version:** 0.16.0
- **Build mode:** ReleaseFast
- **Kernel:** 7.2.15-1-omarchy
- **CPU:** AMD Ryzen 7 8845HS w/ Radeon 780M Graphics
- **Online CPUs:** 16
- **SMT active:** 1
- **Isolated CPUs:** none
- **CPU governor:** performance
- **Load average (start):** 2.56 2.10 2.46 1/1871 130417
- **Suites:** ring, topic

Median of up to 3 runs per row. Percentiles are medians of per-run percentiles, not pooled samples.

## Repeat variability

| File | Runs | Largest metric spread |
|------|------|-----------------------|
| ring-buffer/single-threaded-read-64B.json | 3 | 71.4% |
| ring-buffer/single-threaded-write-64B.json | 3 | 60.0% |
| ring-buffer/spsc-handoff-64B.json | 3 | 24.6% |
| ring-buffer/spsc-write-64B.json | 3 | 4.1% |
| topic/topic-publish-fire_and_forget-1024B.json | 3 | 87.2% |
| topic/topic-publish-fire_and_forget-128B.json | 3 | 21.5% |
| topic/topic-publish-fire_and_forget-32B.json | 3 | 50.4% |
| topic/topic-publish-replicate_once-1024B.json | 3 | 108.2% |
| topic/topic-publish-replicate_once-128B.json | 3 | 119.0% |
| topic/topic-publish-replicate_once-32B.json | 3 | 34.1% |

## Ring Buffer Microbenchmarks (64 B, no broker)

| Scenario | msgs/sec | p50 (ns) | p99 (ns) | max (ns) |
|----------|----------|----------|----------|----------|
| single-threaded-read-64B | 96,307,922 | 9 | 14 | 39 |
| single-threaded-write-64B | 257,530,596 | 3 | 5 | 6 |
| spsc-handoff-64B | 4,267,136 | 150 | 180 | 23,283 |
| spsc-write-64B | 179,075,276 | 5 | 6 | 6 |

## Topic Publish Throughput (single broker, Aeron IPC)

| Message size | Ack mode | Published | Appended msgs/sec | Offer msgs/sec | Final ack wait (us) |
|--------------|----------|-----------|-------------------|----------------|---------------------|
| 32 B | fire_and_forget | 100,000 | 16,033,657 | 16,674,698 | 240 |
| 32 B | replicate_once | 100,000 | 12,867,454 | 13,218,529 | 264 |
| 128 B | fire_and_forget | 100,000 | 14,445,890 | 15,007,407 | 259 |
| 128 B | replicate_once | 100,000 | 11,958,190 | 12,417,888 | 185 |
| 1,024 B | fire_and_forget | 64,026 | 4,023,181 | 4,074,714 | 142 |
| 1,024 B | replicate_once | 64,026 | 4,105,438 | 4,142,781 | 141 |

### Recovery validation (one run)

Command: `zig build perf -Dperf-filter=recovery -Doptimize=ReleaseFast --summary all`.
All five cases passed. Logs: `/tmp/ringloom-review-recovery.log`; raw summaries:
`/tmp/ringloom-perf-results/recovery/`. Same workstation and dirty revision as
above; run immediately beforehand. This is validation of the corrected timer,
not a repeated performance baseline. Service readiness is the endpoint for
service cases. Broker recovery includes planned waits and successful send probes.

#### Measured recovery

| Scenario | Failure detected (ms) | Recovery complete (ms) | Iterations | Max (ms) |
|----------|-----------------------|------------------------|------------|----------|
| broker-restart-recovery | 0 | 11,367 | 1 | 11,367 |
| rapid-service-restart | 11,824 | 11,875 | 5 | 11,980 |
| service-crash-recovery | 12,920 | 12,974 | 1 | 12,974 |
| service-kill-recovery | 11,929 | 11,980 | 1 | 11,980 |

### Transport validation (one run)

The service script completed all 20 paced/saturated local/remote cases; all ten
paced receiver files report their full expected measured count (100,000, or
50,000 for 4096-byte messages). The five plain-Aeron sizes also completed.
Artifacts: `/tmp/ringloom-bench-review-transport/`, including its own metadata
and generated tables. Command: `scripts/bench-all.sh --output-dir
/tmp/ringloom-bench-review-transport --repeat 1 --suites 'ipc aeron'`.
The wrapper was edited during this run, interrupting final collection; fresh
Aeron JSON was copied and the report regenerated manually, as recorded in
`collection-note.txt`. These single-run transport values are retained for
validation and do not replace the repeated baseline.


The final unchanged wrapper also completed a `ring backpressure` validation run:
`scripts/bench-all.sh --output-dir /tmp/ringloom-bench-wrapper-final --repeat 1
--suites 'ring backpressure'`. Its report and raw JSON are retained there. This
verifies collection after the wrapper edits; it is separate from the three-run
ring/topic snapshot above.

## Partial-suite snapshot (kv only) — 2026-11-08

First KV measurements. **Only the `kv` suite was run**; every other section
above is historical and was not refreshed. No earlier KV baseline exists, so
`bench-report.py` was run without `--baseline` and nothing here is a regression
or improvement claim. These are exploratory shared-workstation results.

Command (serial, nothing else benchmarked concurrently):

```sh
scripts/bench-all.sh --output-dir /tmp/ringloom-kv-bench-20261108a --repeat 3 --suites kv
python3 scripts/bench-report.py /tmp/ringloom-kv-bench-20261108a
```

Raw JSON (`run-{1,2,3}/kv/*.json`), per-run `kv.log`, `build.log`,
`metadata.json`, `working-tree.patch` and the wrapper's `report.md` are retained
locally at `/tmp/ringloom-kv-bench-20261108a/` (temporary storage; archive before
cleanup). The wrapper's `report.md` was rendered before the KV report tables were
extended; `report-regenerated.md` in the same directory is the output of the current
`bench-report.py` on the same raw JSON, and that is what's pasted below.

- **Commit:** `d2bbbac4beafc0fbbf1bcbde46825571dd8df73a`, **working tree dirty**
  (tracked diff in `working-tree.patch`; untracked KV sources such as
  `src/perf/kv_bench.zig`, `src/perf/kv_scale_bench.zig` and
  `docs/kv-store-spec.md` are not in that patch).
- **Host:** AMD Ryzen 7 8845HS, 16 online CPUs, SMT on, no isolated CPUs,
  governor `performance`, kernel 7.2.15-1-omarchy, Zig 0.17.1, ReleaseFast.
  Load average at start 0.65 0.85 0.81. No host tuning was changed.
- **Integrity:** 3/3 runs completed with wrapper exit 0. `kv-local-serial`:
  1,000 published / 1,000 visible / 0 failed in every run. Scale runs: `failed` 0,
  every entry iterated (1k / 1M / 10M), 80,000 of 80,000 parallel gets completed.
  Read-path Zig and LMDB allocations were 0 in every run (the harness fails on any).
- **Spread:** `(max-min)/median` across runs is largest for p99 tails on short runs:
  1M get p99 39% (5,060 / 3,717 / 3,598 ns), 1k get/miss p99 up to 28%,
  `kv-local-serial` writer/get p99 up to 19%. p50s, setup, iteration and
  parallel timings are within ~9%. Treat p99s as indicative only.

### Measurement definitions

- **kv-local-serial** (`src/perf/kv_bench.zig` → `ringloom-test-kv-service --mode bench`):
  one broker with KV enabled, same host. 1,000 sequential puts (100 B value,
  `replicate_once`, single broker so no remote replica), each followed by waiting
  for the write to be acknowledged. *Writer p50/p99* times only the `put` call;
  *Serial write+ack total ns* is wall time for all 1,000 put+ack cycles
  (median 237.0 ms, i.e. ~237 µs per serial write, not a throughput figure).
  Gets then repeatedly read **one hot key** (`visible`) and one absent key,
  100,000 times each, on a 1,001-entry store.
- **native-reader-scale** (`src/perf/kv_scale_bench.zig`): the LMDB store is
  populated directly by the engine in 1,000-put transactions (*Setup ns*; this is
  **not** applier or topic throughput), then opened by the native reader. Gets read
  20,000 keys with a strided pattern (`i*7919 mod N`), so at 1M/10M they are
  spread across the store, not a warm hot key. Views re-read the same key set inside one
  session after the get pass. *Parallel* = 4 threads, each its own session,
  20,000 gets each, wall time from start gate to join. No broker, topic, or
  visibility path is involved.
- **kv-engine-alloc**: 1,000 single-put LMDB write transactions after warmup,
  counting LMDB allocations (engine only, not the applier).

### Results (field medians of 3 runs)

## KV bounded same-host serial workload

| Scenario | Entries | Published | Visible | Failed | Get p50 ns | Get p99 ns | Miss p99 ns | Writer p99 ns | Serial write+ack total ns | Iteration ns | Read Zig allocs | Read LMDB allocs |
|----------|---------|-----------|---------|--------|------------|------------|-------------|---------------|---------------------------|--------------|-----------------|------------------|
| kv-local-serial | 1,001 | 1,000 | 1,000 | 0 | 120 | 160 | 150 | 4,889 | 237,005,179 | 30,478 | 0 | 0 |

Writer p50 (raw JSON, not in table): 421 / 391 / 411 ns.

## KV native reader scale

| Entries | Failed | Get p50 ns | Get p99 ns | Miss p50 ns | Miss p99 ns | View p50 ns | View p99 ns | Iteration ns | Iterated | Reader threads | Parallel gets | Parallel ns | Setup ns | Read Zig allocs | Read LMDB allocs |
|---------|--------|------------|------------|-------------|-------------|-------------|-------------|--------------|----------|----------------|---------------|-------------|----------|-----------------|------------------|
| 1,000 | 0 | 140 | 191 | 120 | 180 | 100 | 121 | 6,423 | 1,000 | 4 | 80,000 | 4,106,587 | 335,769 | 0 | 0 |
| 1,000,000 | 0 | 781 | 3,717 | 251 | 430 | 642 | 1,373 | 14,822,321 | 1,000,000 | 4 | 80,000 | 17,548,218 | 451,243,227 | 0 | 0 |
| 10,000,000 | 0 | 3,406 | 7,795 | 341 | 511 | 1,053 | 2,395 | 153,906,634 | 10,000,000 | 4 | 80,000 | 26,871,757 | 4,868,571,913 | 0 | 0 |

## KV engine write-transaction allocations

| Transactions | LMDB allocs | LMDB allocs per txn |
|--------------|-------------|---------------------|
| 1,000 | 999 | 1.0 |

### Against the initial targets in `docs/kv-store-spec.md` §14.2

These numbers are not directly comparable to the targets, so this is not a verdict:

- *Get hit at 1M, warm, p50 ≤ 300 ns / p99 ≤ 1 µs*: measured 781 / 3,717 ns, but
  with a strided spread-key pattern rather than a warm working set. A warm-key 1M
  run has not been measured.
- *View p50 ≤ 150 ns*: 100 ns at 1k; 642 ns at 1M and 1,053 ns at 10M (spread keys).
- *Writer `put` p99 ≤ 1 µs (`fire_and_forget`)*: not measured. The measured
  writer p99 of 4,889 ns uses `replicate_once` on a 1,001-entry store.

### Not yet measured (spec §14.3 items without numbers)

- End-to-end writer → reader **visibility latency**, on one host and **across
  two hosts** (no cross-host KV run exists; loopback nodes are not physical hosts).
- **Applier throughput** (batched, 100 B values, target ≥ 500k puts/s). Scale
  setup time measures direct engine population only, and kv-local-serial is
  ack-serialized.
- **Rebuild time** from a 1M-entry log (§8.5), generation switch, and map growth cost.
- Writer `put` latency in `fire_and_forget` mode, and writer latency under
  concurrent load or with multiple writers.
- Warm-key get latency at 1M/10M entries, and multi-reader scaling beyond one fixed
  4-thread point (per-thread latency histograms, varying thread counts,
  multi-process readers, readers running concurrently with an active applier).
- Per-operation allocation counts on the applier/write path through the broker
  (only the engine-only transaction count above was measured).
