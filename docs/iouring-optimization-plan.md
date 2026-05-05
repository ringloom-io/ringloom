# io_uring Optimization Plan for Broker-to-Broker TCP

## Summary

Yes, broker-to-broker communication can likely improve with additional
io_uring work, but the biggest wins are narrower than the idealized
`docs/iouring.md` architecture suggests.

The current benchmarked hot path is mostly synchronous nonblocking TCP:

- The sender uses per-peer `writev` batching by default. The optional
  sender io_uring path exists in `src/broker/sender/sender_event_loop.zig`,
  but `io_ring` is initialized to `null` because synchronous `writev` has
  been faster for the common single-peer case.
- The receiver uses `accept4`, nonblocking `read`, a 128 KiB read-ahead
  buffer, and in-process frame parsing in `src/broker/receiver/*`.
- The generic `ringloom_tcp` io_uring backend is minimal: single-shot
  accept/connect/recv/send, no setup flags, no provided buffer rings, no
  multishot operations, no fixed files, and no zero-copy send.

Because the current cross-broker transit baseline is already 8.4-12.2 us p50
on loopback, the realistic first target is not a 5-10x reduction. A measured,
incremental plan should aim for lower syscall overhead, shallower transport
backlog under saturation, and tighter p99/p99.9 tails.

## Current Baseline and Bottleneck Read

From `docs/benchmark-results.md`:

| Benchmark | Current p50 range | Relevant breakdown |
|---|---:|---|
| Cross-broker paced transit | 8.4-12.2 us | Broker A queue ~0.5-1.1 us, transport ~7.3-10.3 us, Broker B delivery ~0.5-0.6 us |
| Cross-broker saturated queueing | 0.64-7.71 ms | Broker A queue + transport backlog dominates; Broker B local delivery remains microsecond-scale |

The paced transit bottleneck is the transport hop. The saturated benchmark is
mostly standing queue/backlog, especially for 32 B and 128 B messages.

## Optimization Hypotheses

### High-confidence opportunities

1. **Receiver-side multishot recv with provided buffer rings**
   - Replaces repeated nonblocking `read()` attempts with one armed recv per
     connection.
   - Removes a syscall from the steady-state receive path.
   - Avoids per-cycle empty `read()` polling when the event loop is spinning.
   - Best first io_uring target because the current receiver is synchronous and
     every cross-broker message must pass through it.

2. **io_uring setup flags for owned event-loop rings**
   - Use `IORING_SETUP_SINGLE_ISSUER`, `IORING_SETUP_COOP_TASKRUN`, oversized
     CQ, and optional `IORING_SETUP_SQPOLL`.
   - The architecture already maps sender and receiver to dedicated loops, so
     `SINGLE_ISSUER` is a natural fit.
   - `SQPOLL` should be configurable and benchmark-gated because it consumes a
     core and may not beat synchronous `writev` for one loopback peer.

3. **Batched completion processing and larger CQ headroom**
   - Current batch buffers are 64 CQEs. Use larger completion batches where
     io_uring becomes a real transport path.
   - Oversize CQ entries to prevent overflow under multishot recv bursts.

4. **Capability detection and runtime fallback**
   - Probe for multishot recv, provided buffer rings, SQPOLL, fixed files, and
     zero-copy send.
   - Fall back to the existing synchronous path per feature, not globally.
   - This keeps Linux kernel differences from changing broker correctness.

### Conditional opportunities

1. **Sender io_uring with writev and SQPOLL**
   - Likely useful for many peers or high fan-out, where one batched
     `io_uring_enter` can replace many write syscalls.
   - For the current two-broker benchmark, keep synchronous `writev` as the
     default until measurements prove otherwise.

2. **Fixed files / direct descriptors**
   - Useful once accept/connect/read/write all live in the same io_uring
     lifecycle.
   - Lower priority for the current benchmark because connection setup is not
     in the steady-state measurement window.

3. **Recv bundles**
   - Potentially useful for saturated runs on kernels 6.10+.
   - Adds parser complexity because a completion can cover multiple contiguous
     buffers and arbitrary TCP frame boundaries.

4. **Zero-copy send**
   - Do not enable by default for 32 B, 128 B, or 512 B messages. The spec also
     notes regular send can be faster below roughly 1 KiB.
   - Consider only for larger frames, probably 4 KiB+ or a future larger-message
     benchmark.
   - Requires strict buffer lifetime tracking until notification CQEs arrive.

## Plan of Action

### Phase 0: Measurement and guardrails

1. Add or extend benchmark diagnostics for:
   - sender write calls/SQEs per message,
   - receiver read completions per message,
   - bytes per read/write batch,
   - CQE batch sizes,
   - io_uring fallback/error counters,
   - optional syscall counts using `perf stat`.
2. Preserve the current synchronous TCP path as the default baseline.
3. Add benchmark variants that explicitly select:
   - current sync receiver + sync sender,
   - io_uring receiver + sync sender,
   - io_uring receiver + io_uring sender,
   - SQPOLL on/off.

### Phase 1: Shared io_uring capability layer

1. Reconcile the minimal `src/tcp/io_uring_engine.zig` and
   `src/broker/transport/io_uring.zig` wrappers with the needs of the actual
   broker sender/receiver hot path.
2. Add setup support for:
   - `IORING_SETUP_CQSIZE`,
   - `IORING_SETUP_SINGLE_ISSUER` when supported,
   - `IORING_SETUP_COOP_TASKRUN` when supported,
   - optional `IORING_SETUP_SQPOLL`,
   - configurable queue depth and CQ depth.
3. Add runtime probing for required operations and feature flags.
4. Expose clear counters for feature activation and fallback.

### Phase 2: Receiver multishot path

1. Add an optional receiver io_uring ring owned only by `ReceiverEventLoop`.
2. Convert listener accept to multishot accept when supported.
3. For each connected peer, arm one multishot recv using a provided buffer
   group.
4. Feed received chunks into the existing stream parser semantics:
   - handle headers split across chunks,
   - handle payloads split across chunks,
   - preserve existing validation, heartbeat, admin, and service routing
     behavior,
   - return buffers to the provided buffer ring immediately after copying into
     parser/service-owned buffers.
5. Keep the current 128 KiB read-ahead receiver as fallback.
6. Benchmark buffer sizes of 4 KiB, 8 KiB, 16 KiB, and 64 KiB. For the current
    benchmark payloads, 8-16 KiB should avoid most frame splits without excessive
    memory.

## Status After Implementing Phases 0-2

Phases 0-2 have now been implemented and benchmarked in `ReleaseFast` on the
same untuned shared workstation used for the current baseline. The new path is
functionally correct, but it is **not yet a consistent performance win**.

The current receiver-only io_uring path improves one paced small-message case
and one saturated 512 B case, but regresses other sizes:

| Benchmark | Sync baseline | io_uring receiver | Result |
|---|---:|---:|---|
| Paced transit p50, 32 B | 10.03 us | 8.12 us | 19.1% faster |
| Paced transit p50, 128 B | 9.14 us | 9.90 us | 8.3% slower |
| Paced transit p50, 512 B | 8.59 us | 9.35 us | 8.9% slower |
| Saturated throughput, 32 B | 6.78M msg/s | 5.59M msg/s | 17.5% slower |
| Saturated throughput, 128 B | 4.01M msg/s | 4.09M msg/s | 1.9% faster |
| Saturated throughput, 512 B | 2.12M msg/s | 2.44M msg/s | 14.9% faster |

The transport hop is still the dominant paced-latency component, and saturated
latency is still driven mostly by sender-side queueing and transport backlog.
That means Phase 2 by itself was not enough to make io_uring the best default
path.

### Phase 3: Sender path experiments

1. Keep synchronous `writev` as the default for one peer until a sender
   experiment produces a repeatable win across paced and saturated runs.
2. Increase and benchmark sync `writev` batch sizes before changing the sender
   transport model:
   - current max batch is 64 frames,
   - test 128, 256, and kernel `IOV_MAX`-bounded batches,
   - record bytes per flush and frames per syscall so backlog reduction can be
     attributed to batching rather than noise.
3. Re-enable the existing sender io_uring path behind config and initialize it
   with the Phase 1 setup flags.
4. For the io_uring sender, test:
   - one outstanding `writev` per peer as today,
   - larger single-SQE writev batches,
   - SQPOLL on/off,
   - CQ batch size 64 vs 256,
   - whether completion handling overhead exceeds syscall savings in the
     single-peer loopback case.
5. Avoid multiple independent writes in flight on the same TCP stream unless
   ordering is proven safe or enforced with linking.

### Phase 3A: Receiver-path tuning before any default switch

1. Benchmark receiver CQE copy/processing batch sizes above the current 64-entry
   batch buffer, especially 128 and 256.
2. Sweep provided-buffer settings with the current implementation:
   - `recv.buffer.size` at 4 KiB, 8 KiB, 16 KiB, and 64 KiB,
   - `recv.buffer.count` at 128, 256, and 512,
   - CQ depth sized to avoid burst overflow.
3. Add explicit counters for:
   - multishot accept re-arms,
   - multishot recv re-arms,
   - provided-buffer starvation,
   - fallback-to-sync reasons,
   - bytes copied from provided buffers into the parser buffer.
4. Only reconsider enabling the receiver path by default if tuned settings
   improve best and median paced latency without materially hurting saturated
   throughput.

## Status After Implementing Phases 3/3A

Phases 3 and 3A have now been implemented and benchmarked in `ReleaseFast` on
the same untuned shared workstation used for the prior io_uring measurements.
The synchronous sender remains the safest default, but the sender io_uring path
is now functional enough for continued experiments instead of silently degrading
to synchronous `writev` on retryable TCP backpressure.

Implemented Phase 3 sender work:

- Added config-backed sender knobs for synchronous `writev` batch size,
  per-peer write budget, optional sender io_uring enablement, sender CQE batch
  size, and the shared io_uring setup flags.
- Raised the per-peer sender `writev` batch ceiling to 1024 frames so 128, 256,
  and larger batch experiments are possible.
- Added sender counters for synchronous `writev` calls/frames/bytes and sender
  io_uring activation, fallbacks, SQEs, CQEs, writev frames/bytes, retryable
  write completions, and io_uring-driven peer disconnects.
- Re-enabled the sender io_uring `writev` path behind config while preserving
  one outstanding write per peer.
- Hardened sender io_uring completion handling: retryable write CQE errors such
  as `EAGAIN` now clear the in-flight marker and retry the queued batch instead
  of disabling io_uring globally. Runtime write CQE failures no longer disable
  the whole sender ring while other peers may still have unprocessed write
  completions; non-retryable peer/socket failures disconnect only that peer.

Implemented Phase 3A receiver work:

- Added a configurable receiver CQE batch size, bounded by the receiver's
  internal CQE buffer.
- Added counters for multishot accept/recv re-arms, provided-buffer starvation,
  fallback reasons, and bytes copied from provided buffers into the parser path.
- Added explicit `NOBUFS` handling so provided-buffer starvation is counted and
  recv is re-armed instead of being treated like a peer disconnect.

Focused 128 B best-of-3 rerun after the sender io_uring fallback hardening:

| Variant | Paced p50 | Saturated throughput | Saturated p50 | Saturated p95 | Result |
|---|---:|---:|---:|---:|---|
| Sync `writev` batch 256, budget 256 | 8.03 us | 4.80M msg/s | 4.19 ms | 4.99 ms | Strongest overall Phase 3 default candidate, but still benchmark-gated |
| Sender io_uring, batch 256, SQPOLL off | 8.19 us | 4.59M msg/s | 4.19 ms | 4.54 ms | No longer needs retryable-error fallback; competitive, but not clearly better than sync batching |
| Sender io_uring, batch 256, SQPOLL on | 7.89 us | 3.56M msg/s | 2.17 ms | 2.50 ms | SQPOLL now completes and lowers latency, but costs saturated throughput on this host |
| Receiver io_uring, CQE batch 128, 16 KiB x 256 buffers | 8.24 us | 4.40M msg/s | 4.18 ms | 4.62 ms | Works with the new CQE batch knob, but not enough evidence for a default switch |

Validation run after the Phase 3/3A implementation:

- `zig build test -Doptimize=Debug`
- `zig build install -Doptimize=ReleaseFast`
- `zig build test-bins -Doptimize=ReleaseFast`
- `zig build e2e -Doptimize=Debug`

The current recommendation is to keep the default sender on synchronous
`writev`, with batch/budget tuning available for benchmarks and deployments
that can validate the workload. Sender io_uring should remain opt-in:
SQPOLL-on is now stable in the focused benchmark and can improve latency, but
its lower saturated throughput on this untuned host is not acceptable as a
default. Receiver io_uring should also remain opt-in until a wider sweep across
message sizes, receiver buffer sizes/counts, and CQ depth confirms no tail or
throughput regressions.

### Phase 4: Advanced receive/send features

1. Add recv bundles only after the plain multishot receiver is stable and
   measured.
2. Add fixed files/direct descriptors after the receiver and sender can fully
   own socket lifecycle through io_uring.
3. Prototype zero-copy send for large frames only:
   - pre-register a bounded large-frame buffer pool,
   - track notification CQEs before reuse,
   - record copy-vs-zero-copy fallback usage,
   - keep regular send for sub-1 KiB messages.
4. Consider NAPI busy polling only for physical NIC benchmarks, not loopback
   first.

### Phase 5: Benchmark acceptance criteria

1. Run `scripts/run-benchmarks.sh` before and after each phase on the same host.
2. For targeted single-size work, use `scripts/bench-single-size.sh` with
   repeated runs and compare best, median, and tail results.
3. Accept an optimization only if it improves at least one target metric without
   regressing correctness or materially worsening another benchmark mode.
4. Record results in `docs/benchmark-results.md` after each accepted change.

## Estimated Benchmark Impact

These estimates assume the same loopback benchmark topology and a kernel new
enough for multishot recv and provided buffer rings. They are intentionally
conservative because current results were captured on an untuned shared
workstation.

| Change | Expected impact on paced cross-broker p50 | Expected impact on saturated queueing |
|---|---:|---:|
| Receiver multishot recv + provided buffers | Mixed in current measurements; possible 0-20% lower after tuning, but can regress | Mixed in current measurements; may help tails or medium-size saturation, but not yet reliable |
| io_uring setup flags + larger CQ batches | 5-15% lower when io_uring path is active | 5-20% lower under bursts; more visible in p99/p99.9 |
| Sender io_uring + SQPOLL for one peer | -5% to +10%; may regress if CQE overhead beats syscall savings | 0-20% lower if it reduces transport backlog; must be benchmark-gated |
| Larger `writev` batches without io_uring | 0-10% lower p50 | 10-30% lower for 32 B/128 B saturated runs if sender syscall pressure is limiting |
| Recv bundles on kernel 6.10+ | 0-10% lower p50 | 10-25% lower on saturated runs with many small frames |
| Zero-copy send for 4 KiB+ frames | Little/no gain for small messages; possible 5-15% at 4 KiB+ | 5-15% for large-frame throughput/backlog if true zero-copy is achieved |

Projected ranges for the current published tables:

| Benchmark | Current | Plausible near-term target | Notes |
|---|---:|---:|---|
| Cross-broker paced p50, 32-512 B | 8.4-8.9 us | 6.5-8.0 us | Receiver multishot is the main lever; sender sync path may remain best |
| Cross-broker paced p50, 1-4 KiB | 10.4-12.2 us | 8.5-10.5 us | Larger payloads may benefit slightly from reduced recv/copy overhead |
| Saturated p50, 32 B | 7.71 ms | 4.5-6.5 ms | Most improvement must come from reducing standing transport backlog |
| Saturated p50, 128 B | 3.47 ms | 2.3-3.0 ms | Batch size and sender backlog behavior likely matter as much as io_uring |
| Saturated p50, 512 B | 1.24 ms | 0.9-1.15 ms | Already less dominated by transport backlog |
| Saturated p50, 1 KiB | 636 us | 500-600 us | Moderate room |
| Saturated p50, 4 KiB | 757 us | 600-720 us | Zero-copy may help only if payload size and kernel path cooperate |

## Recommendation

The receiver multishot path was still the right first implementation step, but
the measured result is that it should remain **experimental and disabled by
default** for now.

The next work should focus on **sender-side backlog reduction and batching**:

1. Tune and benchmark larger synchronous `writev` batches first.
2. Re-run sender io_uring experiments with the new setup flags and larger CQ
   batches.
3. Tune receiver CQE batch sizes and provided-buffer settings only as a
   follow-up, not as the primary next lever.

Treat sender io_uring, SQPOLL, recv bundles, fixed files, and zero-copy send as
benchmark-gated follow-ups rather than default behavior. The existing
synchronous `writev` sender is deliberately selected for the common one-peer
case, and the new measurements confirm that replacing the current hot path
blindly can reduce performance instead of improving it.
