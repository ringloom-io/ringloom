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

## Implementation inventory

### Files to delete after the v2 cutover

- `src/tcp/*`: old TCP socket, frame, handshake, and connection-manager library. It remains as a temporary backend until Step 9.
- TCP-specific broker transport adapters under `src/broker/transport/` that only support TCP stream I/O once POSIX UDP and AF_XDP replacements are fully wired.
- TCP-focused e2e assertions in `src/e2e/cross_broker_test.zig`, `src/e2e/fragmentation_test.zig`, and `src/e2e/backpressure_test.zig` after equivalent UDP coverage exists.

### Files to replace or rename

- `src/common/memory/broker_metadata.zig`: replace the single send-buffer layout with v2 metadata version, send-buffer directory, and fixed per-destination send region.
- `src/service/service_client.zig`: replace the cached broker-wide send ring with destination-buffer handle lookup and transport-neutral route envelopes.
- `src/broker/sender/sender_event_loop.zig`: replace global send-ring draining with fair scanning over destination buffers.
- `src/broker/receiver/receiver_event_loop.zig`: replace TCP accept/stream parsing with UDP/AF_XDP packet validation and receive-window processing in later steps.
- `src/common/config/broker_config.zig`, `src/common/config/config_loader.zig`, and `src/testing/config_gen.zig`: replace TCP/io_uring config keys with UDP/send-buffer/AF_XDP keys during Step 9.

### Temporary compatibility shims

- The sender may synthesize TCP frames from transport-neutral route envelopes while Step 2 validates per-destination shared-memory isolation.
- `src/broker/receiver/*` may continue parsing TCP frames until Step 7 owns receiver reassembly and routing.
- `src/broker/control/control_loop.zig` may broadcast admin messages through destination buffers, but must not emit TCP frame headers after Step 3.

### Tests to port versus replace

- Port intent from `src/e2e/cross_broker_test.zig`: cross-broker delivery remains required, but transport setup becomes UDP.
- Port intent from `src/e2e/fragmentation_test.zig`: message fragmentation remains required, but coverage moves to UDP DATA fragment reassembly.
- Port intent from `src/e2e/backpressure_test.zig`: slow-destination isolation becomes per-stream/per-destination flow control instead of TCP write-queue pressure.
- Keep local IPC, registration, discovery, leader election, heartbeat, and restart tests; update only transport-specific assumptions as the later steps replace TCP.

### Final module names

- Add `src/udp/root.zig` imported as `ringloom_udp`.
- Keep broker-facing endpoint adapters under `src/broker/transport/`.
- Keep shared application route envelopes under `ringloom_common.protocol` so services do not import transport modules.

### Cutover policy

- No runtime TCP/UDP dual-stack compatibility is required.
- The intermediate branch may use a TCP bridge inside the sender only; service and control producers should write transport-neutral envelopes.
- The final broker config requires `broker.transport=udp`.

### Deletion boundaries

- `src/tcp/*` is removable after Step 9.
- Service APIs and control broadcasts must not reference TCP frame headers after Step 3.
- Receiver TCP parsing remains reachable only until Step 7.
- TCP counters are replaced by UDP counters in Step 8.

### Validation notes

- Baseline `zig build test` passed before implementation changes.
- `zig build e2e` is reserved for the later transport-wiring steps because Step 2 keeps the temporary TCP backend and Step 3 adds unit-tested UDP primitives.

## Tests for this step

This is a documentation/inventory step, but it should still include validation commands once code work begins:

1. `zig build test` before modifications to establish baseline.
2. `zig build e2e` before modifications if the environment can run multi-process tests.
3. A lightweight grep check in later steps that no new code imports `ringloom_tcp` outside temporary compatibility files.

## Done criteria

- The inventory exists and is referenced by later PRs or commits.
- The final module names and deletion boundaries are accepted.
- Baseline test commands and any known environment constraints are recorded.
