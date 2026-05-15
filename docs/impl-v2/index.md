# RingLoom v2 Implementation Plan

This folder tracks the implementation plan for `docs/architecture-v2.md`. The work is intentionally split so each step can land with focused tests. The v2 cutover is allowed to break v1 TCP contracts, but each intermediate commit should keep the repository buildable and the relevant tests passing.

## Progress tracker

| Status | Step | Plan file | Integration role |
|---|---:|---|---|
| [x] | 1 | [Analysis and cutover boundaries](01-analysis-and-cutover.md) | Establish source inventory, deletion plan, and CI strategy |
| [x] | 2 | [Per-destination send buffers](02-send-buffer-metadata.md) | Replace the broker-wide send ring with destination buffers |
| [x] | 3 | [UDP protocol and term logs](03-udp-protocol-codecs.md) | Define frame codecs, stream identity, and retained send logs |
| [x] | 4 | [POSIX UDP endpoint](04-posix-udp-engine.md) | Provide portable UDP send/receive engine |
| [x] | 5 | [AF_XDP and eBPF endpoint](05-af-xdp-ebpf-engine.md) | Add optional kernel bypass and safe fallback |
| [x] | 6 | [Sender reliability and scheduler](06-sender-reliability-scheduler.md) | Production sender loop drains destination buffers over reliable UDP |
| [x] | 7 | [Receiver reassembly and routing](07-receiver-reassembly-routing.md) | Production receiver loop polls UDP packets, reassembles DATA, and routes completed messages |
| [x] | 8 | [Flow control, congestion, observability](08-flow-congestion-observability.md) | UDP pressure/counter integration is wired through metadata and Prometheus output |
| [x] | 9 | [Config, tests, and cleanup](09-config-tests-cleanup.md) | v2 config/harness/test surfaces are active; obsolete TCP code has been removed |

All v2 implementation steps are complete under the completion rule. The old broker-to-broker TCP module and TCP-only compatibility helpers have been removed.

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
| Broker metadata | `src/common/memory/broker_metadata.zig` | Owns destination send-buffer directory, flow-control, and peer counter regions |
| Service send path | `src/service/service_client.zig` | Routes service messages into destination send buffers with transport-neutral `MessageHeader` envelopes |
| Sender loop | `src/broker/sender/sender_event_loop.zig` | Drains per-destination buffers and sends SETUP/DATA/HEARTBEAT over reliable UDP |
| Receiver loop | `src/broker/receiver/receiver_event_loop.zig` | Polls UDP packets, emits SETUP_RESPONSE/STATUS/NAK, and routes reassembled messages |
| UDP protocol | `src/udp/*` | V2 frame codecs, term logs, receive windows, POSIX endpoint, and optional AF_XDP primitives |
| Config | `src/common/config/broker_config.zig`, `src/common/config/config_loader.zig`, `src/testing/config_gen.zig` | Emits and validates UDP/send-buffer/AF_XDP transport keys |
| Tests | `src/e2e/cross_broker_test.zig`, `src/e2e/fragmentation_test.zig`, `src/e2e/backpressure_test.zig`, `src/e2e/observability_test.zig` | E2E coverage exercises the v2 UDP transport and observability path |

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
