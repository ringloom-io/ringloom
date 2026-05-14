# RingLoom v2 Implementation Plan

This folder tracks the implementation plan for `docs/architecture-v2.md`. The work is intentionally split so each step can land with focused tests. The v2 cutover is allowed to break v1 TCP contracts, but each intermediate commit should keep the repository buildable and the relevant tests passing.

## Progress tracker

| Status | Step | Plan file | Integration role |
|---|---:|---|---|
| [ ] | 1 | [Analysis and cutover boundaries](01-analysis-and-cutover.md) | Establish source inventory, deletion plan, and CI strategy |
| [ ] | 2 | [Per-destination send buffers](02-send-buffer-metadata.md) | Replace the broker-wide send ring with destination buffers |
| [ ] | 3 | [UDP protocol and term logs](03-udp-protocol-codecs.md) | Define frame codecs, stream identity, and retained send logs |
| [ ] | 4 | [POSIX UDP endpoint](04-posix-udp-engine.md) | Provide portable UDP send/receive engine |
| [ ] | 5 | [AF_XDP and eBPF endpoint](05-af-xdp-ebpf-engine.md) | Add optional kernel bypass and safe fallback |
| [ ] | 6 | [Sender reliability and scheduler](06-sender-reliability-scheduler.md) | Drain destination buffers fairly and retransmit reliably |
| [ ] | 7 | [Receiver reassembly and routing](07-receiver-reassembly-routing.md) | Detect loss, rebuild messages, route to local services |
| [ ] | 8 | [Flow control, congestion, observability](08-flow-congestion-observability.md) | Replace TCP backpressure with per-stream control and counters |
| [ ] | 9 | [Config, tests, and cleanup](09-config-tests-cleanup.md) | Wire broker config, update tests, and remove obsolete TCP paths |

## Dependency graph

```
01-analysis
  |
  +--> 02-send-buffer-metadata
  |       |
  |       +--> 06-sender-reliability-scheduler
  |
  +--> 03-udp-protocol-codecs
          |
          +--> 04-posix-udp-engine
          |       |
          |       +--> 06-sender-reliability-scheduler
          |       +--> 07-receiver-reassembly-routing
          |
          +--> 05-af-xdp-ebpf-engine
                  |
                  +--> 09-config-tests-cleanup

06-sender-reliability-scheduler + 07-receiver-reassembly-routing
  |
  +--> 08-flow-congestion-observability
          |
          +--> 09-config-tests-cleanup
```

## Integration sequence

1. **Inventory and freeze contracts.** Document all TCP and single-send-buffer assumptions before editing code. Decide which files are deleted, replaced, or temporarily adapted.
2. **Refactor send buffers first.** Implement per-destination send buffers while the old TCP sender can still be used as a temporary backend. This isolates shared-memory and ServiceClient risk from UDP reliability work.
3. **Introduce protocol primitives.** Add UDP frame codecs, term log structures, stream identity, position arithmetic, and loss/retransmit state with unit tests.
4. **Add POSIX UDP transport.** Build a fully tested portable UDP endpoint before AF_XDP.
5. **Wire sender and receiver loops.** Replace TCP stream parsing with reliable UDP stream scheduling and reassembly.
6. **Add AF_XDP as an optional engine.** AF_XDP must never be required for normal CI; it is capability-gated and must fall back deterministically unless `require_af_xdp` is configured.
7. **Replace config and tests.** Update e2e harness and test services to exercise the v2 path. Remove TCP config and obsolete TCP-specific tests after equivalent UDP coverage exists.

## Current implementation surfaces

| Area | Current file(s) | Why it matters |
|---|---|---|
| Broker metadata | `src/common/memory/broker_metadata.zig` | Creates one `send_buffer` and exposes `getSendBuffer()` |
| Service send path | `src/service/service_client.zig` | Caches one `broker_send_ring_buffer`; writes `TcpFrameHeader` into it |
| Sender loop | `src/broker/sender/sender_event_loop.zig` | Single consumer of one send ring; routes to per-peer TCP write queues |
| Receiver loop | `src/broker/receiver/receiver_event_loop.zig` | TCP accept, handshake, stream parsing, complete-frame routing |
| TCP protocol | `src/tcp/frame.zig`, `src/tcp/handshake.zig` | V1 wire protocol to replace |
| Config | `src/common/config/broker_config.zig`, `src/common/config/config_loader.zig`, `src/testing/config_gen.zig` | TCP/io_uring knobs become UDP/AF_XDP knobs |
| Tests | `src/e2e/cross_broker_test.zig`, `src/e2e/fragmentation_test.zig`, `src/e2e/backpressure_test.zig` | Existing e2e intent remains, transport assumptions change |

## Cross-step test matrix

| Behavior | First tested in | E2E coverage |
|---|---|---|
| Destination send-buffer directory creation/open | Step 2 | Broker startup metadata validation |
| Service send routes to destination buffer | Step 2 | Local producer -> broker sender smoke |
| DATA/STATUS/NAK/SETUP codecs | Step 3 | Protocol malformed-frame e2e after Step 7 |
| Term log append/rotate/retransmit | Step 3 | Loss-injection e2e after Step 7 |
| POSIX UDP packet send/receive | Step 4 | Cross-broker POSIX UDP e2e |
| AF_XDP filter redirects only configured ports | Step 5 | Capability-gated AF_XDP e2e |
| Sender skips only blocked destination | Step 6 | Per-destination backpressure isolation e2e |
| Receiver reassembles fragmented remote messages | Step 7 | Fragmentation e2e |
| Slow target service reduces only its stream window | Step 8 | Slow-consumer cross-broker e2e |
| Config fallback from AF_XDP to POSIX | Step 9 | Startup fallback e2e/unit |

## Completion rule

A step is complete only when:

1. Its code changes are wired into all relevant call sites.
2. The tests listed in that step exist and pass.
3. Counters or error paths added by the step are observable in unit or e2e tests where practical.
4. No obsolete v1 behavior remains reachable unless a later step explicitly owns its removal.

