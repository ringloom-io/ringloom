# Step 1: Analysis and Cutover Boundaries

## Objective

Create an implementation inventory that makes the v2 cutover safe to execute. This step does not change runtime behavior. It records which v1 TCP and single-send-buffer contracts will be deleted, replaced, or temporarily adapted.

## Current assumptions to verify

1. `src/common/memory/broker_metadata.zig` stores one `messages_buffer_length`, maps one `send_buffer`, and exposes `getSendBuffer()`.
2. `src/service/service_client.zig` stores `broker_send_ring_buffer` and writes `TcpFrameHeader + payload` into that single ring.
3. `src/broker/sender/sender_event_loop.zig` owns one `send_ring_buffer` and applies global drain limiting through peer write-queue space.
4. `src/broker/receiver/receiver_event_loop.zig` is TCP-stream oriented: accept, handshake, partial frame parsing, route complete frames.
5. `src/tcp/*` contains the old TCP transport and protocol contracts.
6. `src/common/config/*` and `src/testing/config_gen.zig` expose TCP and io_uring keys that should not survive the v2 cutover.

## Implementation tasks

1. Produce a source inventory issue or checklist covering:
   - files to delete after v2 cutover,
   - files to rename or replace,
   - files requiring compatibility shims only during intermediate steps,
   - tests that should be ported versus replaced.
2. Decide final module names:
   - recommended: `src/udp/root.zig` imported as `ringloom_udp`,
   - broker transport adapters remain under `src/broker/transport/`.
3. Define cutover policy:
   - no runtime TCP/UDP dual-stack compatibility,
   - intermediate branch may adapt TCP sender to per-destination buffers for Step 2 only,
   - final broker config requires `broker.transport=udp`.
4. Define code deletion boundaries:
   - `src/tcp/*` becomes removable after Step 9,
   - TCP frame/handshake types must not be referenced by service APIs after Step 3,
   - TCP counters are replaced by UDP counters in Step 8.

## Tests for this step

This is a documentation/inventory step, but it should still include validation commands once code work begins:

1. `zig build test` before modifications to establish baseline.
2. `zig build e2e` before modifications if the environment can run multi-process tests.
3. A lightweight grep check in later steps that no new code imports `ringloom_tcp` outside temporary compatibility files.

## Done criteria

- The inventory exists and is referenced by later PRs or commits.
- The final module names and deletion boundaries are accepted.
- Baseline test commands and any known environment constraints are recorded.

