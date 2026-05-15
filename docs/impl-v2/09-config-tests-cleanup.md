# Step 9: Config, Tests, and Cleanup

## Objective

Finish the v2 cutover by replacing TCP config and tests, updating the e2e harness, documenting operational configuration, and removing obsolete TCP paths.

## Source touchpoints

| File/dir | Change |
|---|---|
| `src/common/config/broker_config.zig` | Replace TCP fields with UDP/send-buffer/AF_XDP fields |
| `src/common/config/config_loader.zig` | Parse and validate v2 keys |
| `src/testing/config_gen.zig` | Emit UDP and optional AF_XDP test config |
| `src/testing/harness.zig` | Add UDP/transport options to `BrokerSpec` |
| `src/e2e/*` | Port or replace TCP-era tests |
| `src/tcp/*` | Removed after v2 cutover |
| `docs/architecture.md` | Leave v1 doc intact unless a later docs task asks to retire it |
| `README` or user docs | Update run/config examples if present |

## Config changes

Remove or deprecate:

- `broker.tcp.send.buffer.size`
- `broker.tcp.recv.buffer.size`
- `broker.tcp.nodelay`
- `broker.peer.write.queue.capacity`
- TCP io_uring sender/receiver config that no longer applies.

Add:

- `broker.transport=udp`
- `broker.udp.local.host.port`
- `broker.udp.member.host.ports`
- `broker.udp.mtu`
- `broker.udp.term.length`
- `broker.udp.receiver.window.length`
- `broker.udp.heartbeat.interval.ms`
- `broker.udp.session.timeout.ms`
- `broker.udp.nak.initial.delay.us`
- `broker.udp.nak.retry.delay.us`
- `broker.send.buffers.max.entries`
- `broker.send.buffers.default.size`
- `broker.send.buffers.max.total.bytes`
- `broker.transport.engine`
- `broker.af_xdp.*`

Validation:

1. MTU must be large enough for the largest protocol header and below configured max.
2. Term length must be a power of two and at least the minimum.
3. Receiver window must not exceed half the term length by default.
4. Send-buffer total budget must fit metadata file limits.
5. AF_XDP ports must include the broker UDP local port when AF_XDP is enabled.
6. `require_af_xdp` fails startup if prerequisites are not met.

## Test migration

### Existing e2e tests to port

| Current test | V2 action |
|---|---|
| `broker_startup_test.zig` | Port to UDP config and v2 metadata layout |
| `registration_test.zig` | Keep; service control IPC remains local |
| `discovery_test.zig` | Keep; remote discovery now rides UDP admin frames |
| `cross_broker_test.zig` | Port to POSIX UDP and add loss/reorder variants |
| `fragmentation_test.zig` | Port to UDP MTU fragmentation/reassembly |
| `heartbeat_timeout_test.zig` | Port to UDP session heartbeat |
| `restart_test.zig` | Add epoch rejection of stale packets |
| `leader_election_test.zig` | Port admin messages to UDP transport |
| `backpressure_test.zig` | Add per-destination remote backpressure isolation |
| `observability_test.zig` | Assert v2 counters |

### New e2e tests

1. `udp_loss_recovery_test.zig`
   - drop packets deterministically,
   - verify NAK and retransmit,
   - verify one application delivery.
2. `udp_reorder_duplicate_test.zig`
   - reorder and duplicate packets,
   - verify in-order delivery and duplicate counters.
3. `remote_destination_backpressure_test.zig`
   - slow service A and normal service B on same peer,
   - verify B continues while A is pressured.
4. `af_xdp_fallback_test.zig`
   - `prefer_af_xdp` falls back to POSIX when unavailable,
   - `require_af_xdp` fails cleanly when unavailable.
5. `af_xdp_capability_test.zig`
   - skipped unless environment indicates AF_XDP support.

## Test harness support

Add harness controls for:

- UDP local/member endpoints,
- MTU,
- term length,
- send-buffer capacity,
- transport engine,
- AF_XDP interface/queue/port,
- packet loss/reorder/duplicate injection for POSIX UDP tests.

Loss injection can be implemented as a test-only endpoint wrapper around the UDP endpoint interface. It should not require kernel packet filters for normal CI.

## Cleanup checklist

1. Remove production imports of `ringloom_tcp`.
2. Delete TCP module from build graph.
3. Remove TCP counters from monitoring output.
4. Remove TCP config keys from generated e2e config.
5. Update architecture references in docs that point to TCP as current design.
6. Run full relevant test suite:
   - `zig build test`
   - `zig build test-testing`
   - `zig build e2e`
   - capability-gated AF_XDP tests where available
   - `zig build perf` for performance comparison when environment permits

## Done criteria

- Default broker-to-broker transport is reliable UDP.
- POSIX UDP e2e tests pass without AF_XDP support.
- AF_XDP tests pass or skip deterministically based on capability.
- No production code path uses the old TCP transport.
- Documentation and generated config examples use v2 keys.
