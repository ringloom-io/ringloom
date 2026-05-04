# Order Management Sample Architecture

This document defines the architecture for a Zig sample application under
`samples/order-management`. The sample should demonstrate RingLoom as a reference
runtime for low-latency microservices: same-host shared-memory IPC, cross-broker
TCP routing, service discovery, leader-aware routing, flow control/back-pressure,
monitoring, graceful startup/shutdown, and allocation-free hot paths.

The sample is intentionally small enough to read in one sitting, but it should be
structured like a production application rather than a toy benchmark.

---

## Goals

1. Provide a complete, runnable two-node RingLoom application.
2. Show a clean service architecture that new users can copy.
3. Exercise both local and cross-broker message paths in normal operation.
4. Keep all per-message code allocation-free.
5. Make operational behavior visible through logs, counters, result files, and
   `ringloom-stat`.
6. Keep domain logic independent from process wiring so services are easy to test.

## Non-goals

- Building a full exchange or order-management system.
- Persistence, recovery journals, external databases, or external queues.
- Dynamic schemas, JSON payloads, or text protocols on the hot path.
- Hiding RingLoom concepts behind a large framework.

---

## Topology

The default run starts two brokers and six service types. Services are spread
across the two broker nodes so every scenario includes local IPC and remote
broker-to-broker routing.

```text
samples/order-management

Host / node 1                                      Host / node 2
broker node_id=1                                  broker node_id=2
127.0.0.1:19101                                   127.0.0.1:19102
┌──────────────────────────────┐                  ┌──────────────────────────────┐
│ order-simulator              │                  │ matching-engine              │
│ order-gateway                │                  │ execution-service            │
│ risk-service                 │                  │                              │
│ portfolio-service            │                  │                              │
└──────────────────────────────┘                  └──────────────────────────────┘
```

### Default message flow

```text
order-simulator
  └─ local IPC ─► order-gateway
       └─ local IPC ─► risk-service
            └─ cross-broker TCP ─► matching-engine
                 └─ local IPC ─► execution-service
                      └─ cross-broker TCP ─► portfolio-service
```

This path gives a clear per-order latency chain:

1. **Simulator → Gateway:** local direct shared-memory path.
2. **Gateway → Risk:** local direct shared-memory path.
3. **Risk → Matching:** remote broker-routed path.
4. **Matching → Execution:** local direct shared-memory path on node 2.
5. **Execution → Portfolio:** remote broker-routed path back to node 1.

### Optional full-feature profile

The startup script should also support a `--profile full` mode that starts an
extra `risk-service` instance on node 2 and an extra `matching-engine` instance
on node 1. This profile demonstrates:

- load balancing to multiple service instances with the same service name,
- remote and local instances behind one `ServiceClient`,
- leader election for `matching-engine`,
- lifecycle callbacks when an optional process is stopped and restarted.

The default profile remains the simplest six-service topology.

---

## Services

| Service | Node | Role | RingLoom features demonstrated |
|---|---:|---|---|
| `order-simulator` | 1 | Generates deterministic order flow and drives the scenario. | Producer pacing, fixed payload construction, back-pressure response. |
| `order-gateway` | 1 | Validates inbound orders, assigns gateway sequence numbers, forwards risk checks. | Service discovery, local IPC, zero-copy claim/send pattern. |
| `risk-service` | 1 | Applies simple fixed-table credit and symbol limits. | Local receive, remote send, fixed-size state tables. |
| `matching-engine` | 2 | Maintains a tiny in-memory book and emits fills/rejects. | Cross-broker receive, leader-aware routing in full profile. |
| `execution-service` | 2 | Converts fills into execution reports and acknowledgements. | Local IPC receive/send, batching-friendly handler design. |
| `portfolio-service` | 1 | Tracks positions and P&L from execution reports. | Cross-broker receive, lifecycle visibility, read-only snapshots. |

### `order-simulator`

The simulator is the only service that creates synthetic domain traffic. It
should:

- generate `NewOrder`, `CancelOrder`, and occasional `BulkOrderBatch` messages;
- use a deterministic PRNG seed so runs are reproducible;
- support `--orders`, `--rate-per-sec`, `--burst-size`, `--message-size`, and
  `--duration-sec`;
- stop after the requested count or duration;
- write a compact summary JSON outside the hot path at shutdown.

The simulator should create its `ServiceClient` once at startup and wait until
`order-gateway` is discovered before sending the first order.

### `order-gateway`

The gateway should validate order shape, stamp gateway sequence numbers, and
forward `RiskCheckRequest` messages to `risk-service`. Validation must be simple:

- reject unknown symbols,
- reject zero quantity,
- reject prices outside a fixed range,
- reject malformed envelopes or unsupported versions.

Rejected orders are returned to the simulator as `OrderRejected` messages. The
rejection path is still allocation-free: write a fixed-size reject payload into a
claimed RingLoom buffer.

### `risk-service`

Risk keeps fixed arrays for per-account credit and per-symbol notional limits.
It should receive `RiskCheckRequest`, update in-memory counters, and send either:

- `RiskAccepted` to `matching-engine`, or
- `OrderRejected` back to `order-gateway`.

The normal accepted path crosses from node 1 to node 2. That makes the risk
service the first clear example of a service that uses the same `ServiceClient`
API regardless of whether the target is local or remote.

### `matching-engine`

The matching engine should keep a deliberately tiny fixed-size order book:

- a static symbol table, for example `AAPL`, `MSFT`, `NVDA`, `ZIG`;
- bounded price levels per symbol;
- bounded resting orders per price level;
- no heap growth after startup.

It receives accepted orders, matches against resting liquidity, and emits
`Fill` or `OrderRested` events to `execution-service`.

In `--profile full`, matching should run with leader election enabled. Upstream
services should use `sendToLeader` for order entry to show how users can route to
the elected active instance without building their own coordination layer.

### `execution-service`

Execution receives fills from matching and emits `ExecutionReport` messages. It
should also maintain simple per-run counters:

- fills received,
- reports emitted,
- send failures by RingLoom error kind,
- maximum observed handler time in nanoseconds.

The service must not print per fill. It should log only readiness, lifecycle
events, and final summaries.

### `portfolio-service`

Portfolio receives execution reports from node 2 and updates fixed-size position
state indexed by account and symbol. It should periodically publish a
`PortfolioSnapshot` only when explicitly requested by the simulator or by a cold
control path; it must not allocate or format text from the message handler.

---

## RingLoom feature coverage

| Feature | How the sample demonstrates it |
|---|---|
| Shared-memory local IPC | Simulator → gateway, gateway → risk, matching → execution. |
| Cross-broker TCP routing | Risk → matching and execution → portfolio. |
| Service registration | Every service starts through `RingLoomEngine.start`. |
| Service discovery | Each service creates clients by service name and waits for target instances. |
| Load balancing | `--profile full` starts multiple `risk-service` instances behind one client. |
| Leader election | `--profile full` starts multiple `matching-engine` instances and routes with `sendToLeader`. |
| Back-pressure | Simulator and gateway count `SendBufferFull`, `BackPressure`, and remote peer congestion errors. |
| Flow-control counters | Broker configs enable flow-control and peer send counters in the full profile. |
| Fragmentation / large payloads | Simulator can send `BulkOrderBatch` messages that exceed a single logical order payload. |
| Lifecycle callbacks | Services register lifecycle handlers and log instance availability changes outside the hot path. |
| Heartbeats and cleanup | Startup script can stop one optional full-profile service and observe discovery removal. |
| Monitoring | `ringloom-stat` can attach to the sample storage path and group. |
| Graceful shutdown | Startup script sends SIGTERM in reverse dependency order and waits for child exits. |

---

## Payload protocol

RingLoom keeps transport framing broker-internal. The destination broker strips
the TCP frame before writing to the target service ring buffer, but preserves the
logical `template_id` as the service-visible ring-buffer `msg_type_id`. This
means handlers can dispatch the same way for local and remote `tryClaim` sends:
switch on `msg_type_id`, then decode the application payload.

The sample should still put a small domain envelope at the beginning of every
payload for correlation and latency timing, but it should not duplicate routing
or template metadata that RingLoom already provides.

```zig
pub const ProtocolVersion: u8 = 1;

pub const TemplateId = enum(u16) {
    new_order = 1001,
    cancel_order = 1002,
    risk_check_request = 1101,
    risk_accepted = 1102,
    order_rejected = 1103,
    fill = 1201,
    execution_report = 1301,
    portfolio_snapshot_request = 1401,
    portfolio_snapshot = 1402,
    bulk_order_batch = 1501,
};

pub const Envelope = extern struct {
    correlation_id: i64,
    created_ns: u64,
    stage_ns: u64,
    payload_len: u16,
    version: u8,
    flags: u8,
    source_stage: u8,
    reserved: [3]u8,
};
```

Each message body should be an `extern struct` immediately after `Envelope`.
Variable text should be avoided on the hot path. Use fixed numeric identifiers
or fixed byte arrays:

```zig
pub const Symbol = enum(u16) {
    aapl = 1,
    msft = 2,
    nvda = 3,
    zig = 4,
};

pub const NewOrder = extern struct {
    account_id: u32,
    order_id: u64,
    symbol: Symbol,
    side: Side,
    quantity: u32,
    price_nanos: i64,
    tif: TimeInForce,
};
```

The sample should include compile-time assertions for every wire type:

```zig
comptime {
    std.debug.assert(@sizeOf(Envelope) == 32);
    std.debug.assert(@alignOf(NewOrder) <= 8);
}
```

### Encoding and decoding

Protocol helpers should be flyweight-style overlays over caller-provided memory:

- `encodeEnvelope(dest: []u8, fields: EnvelopeFields) ![]u8`
- `decodeEnvelope(src: []const u8) !*const Envelope`
- `payloadAs(comptime T: type, src: []const u8) !*const T`
- `templateFromMsgType(msg_type_id: i32) !TemplateId`
- `claimMessage(client: *ServiceClient, template_id: TemplateId, body_size: usize) !ClaimedMessage`

No helper should allocate. Encoding into a RingLoom claim should look like:

```zig
var msg = try protocol.claimMessage(client, .risk_check_request, @sizeOf(RiskCheckRequest));
defer msg.abortUnlessCommitted();

protocol.writeEnvelope(msg.payload, .{
    .payload_len = @sizeOf(RiskCheckRequest),
    .correlation_id = order_id,
});

protocol.writeBody(RiskCheckRequest, msg.body(), .{ ... });
msg.commit();
```

Handlers should dispatch on the RingLoom message type:

```zig
fn messageHandler(msg_type_id: i32, payload: []const u8) void {
    const template = protocol.templateFromMsgType(msg_type_id) catch {
        counters.unknown_template += 1;
        return;
    };

    switch (template) {
        .risk_check_request => handleRiskCheck(payload),
        .execution_report => handleExecutionReport(payload),
        else => counters.unknown_template += 1,
    }
}
```

`client.send(payload)` is acceptable for cold paths and small startup probes, but
the reference hot path should prefer `tryClaim` so the payload is constructed
directly in the target ring buffer or broker send ring buffer.

---

## Source layout

```text
samples/order-management/
├── README.md
├── build.zig                     # optional standalone package entry
├── config/
│   ├── broker_1.properties
│   └── broker_2.properties
├── scripts/
│   └── run.sh                    # builds, generates workspace, starts/stops all processes
└── src/
    ├── common/
    │   ├── app.zig               # common process runner and signal handling
    │   ├── args.zig              # startup-only CLI parsing
    │   ├── counters.zig          # fixed counters structs
    │   ├── protocol.zig          # envelope, template IDs, encode/decode helpers
    │   ├── service_names.zig     # comptime service-name constants
    │   └── static_tables.zig     # symbols, accounts, starting limits
    └── services/
        ├── order_simulator.zig
        ├── order_gateway.zig
        ├── risk_service.zig
        ├── matching_engine.zig
        ├── execution_service.zig
        └── portfolio_service.zig
```

If the sample is integrated into the repository root build, the root `build.zig`
should expose:

```text
zig build sample-order-management
zig build run-sample-order-management -- --profile default
zig build run-sample-order-management -- --profile full --orders 100000
```

The sample can also keep a small standalone `samples/order-management/build.zig`
for readers who copy it into their own project, but the repository build should
remain the authoritative path used by CI.

---

## Service structure

Each service binary should follow the same shape.

```text
service file
├── imports and constants
├── AppState                 # preallocated state; one pointer passed to handlers
├── init()                   # startup allocations and client discovery
├── messageHandler()         # no allocation, no formatting, no blocking I/O
├── doColdMaintenance()      # periodic summaries, result-file writes, optional snapshots
└── main()                   # CLI, allocator, RingLoomEngine lifecycle, signal handling
```

Domain logic should be separated from RingLoom wiring. For example,
`matching_engine.zig` should have small pure helpers:

- `validateOrder`
- `matchOrder`
- `applyRestingOrder`
- `emitFill`

Only `emitFill` should know about RingLoom clients. The matching algorithm itself
should operate on fixed arrays and return a small result enum/struct.

### Common runner

`common/app.zig` should provide reusable startup/shutdown utilities:

- install SIGTERM/SIGINT handlers,
- parse common flags,
- choose allocator,
- start `RingLoomEngine`,
- create clients,
- wait for service discovery,
- print a `service ready: name=...` marker,
- run a cold maintenance loop until shutdown.

The runner is not a framework. It should be thin enough that readers can see the
actual RingLoom API calls.

---

## Startup script

`samples/order-management/scripts/run.sh` should be the primary entry point.

Recommended usage:

```bash
samples/order-management/scripts/run.sh
samples/order-management/scripts/run.sh --profile full --orders 100000 --rate-per-sec 50000
samples/order-management/scripts/run.sh --optimize ReleaseFast --workspace /tmp/ringloom-orders
samples/order-management/scripts/run.sh --no-build --bin-dir zig-out/bin
```

Responsibilities:

1. Build sample binaries unless `--no-build` is set.
2. Create a workspace:
   `WORKSPACE=${WORKSPACE:-/tmp/ringloom-order-management-XXXXXX}`.
3. Create `config/`, `logs/`, `results/`, and `storage/` directories.
4. Write broker configs with matching peers:
   - node 1: `broker.member.host.ports=2@127.0.0.1:19102`
   - node 2: `broker.member.host.ports=1@127.0.0.1:19101`
5. Start both brokers and wait for readiness markers.
6. Start consumers before producers:
   `portfolio-service`, `execution-service`, `matching-engine`,
   `risk-service`, `order-gateway`, then `order-simulator`.
7. Wait for each `service ready: name=...` marker.
8. Stream a concise status line and write full logs to files.
9. On SIGINT/SIGTERM, stop child processes in reverse order using recorded PIDs.
10. Print the workspace path, logs path, storage path, and `ringloom-stat`
    command before exit.

The script should never use broad process-name killing. It should record PIDs
for every child and signal those exact PIDs.

### Generated broker properties

Default profile:

```properties
broker.node.id=1
broker.local.host.port=127.0.0.1:19101
broker.member.host.ports=2@127.0.0.1:19102
broker.group.name=order-management
broker.storage.path=${WORKSPACE}/storage
broker.control.buffer.size=65536
broker.messages.buffer.size=1048576
broker.threading.mode=dedicated
broker.idle.strategy=backoff
broker.flow.control.enabled=true
broker.flow.control.peer.send.counters.enabled=true
broker.benchmark.latency.tracing.enabled=true
```

Node 2 uses `broker.node.id=2`, `broker.local.host.port=127.0.0.1:19102`, and
`broker.member.host.ports=1@127.0.0.1:19101`.

---

## Allocation and latency rules

The sample should make the allocation boundary explicit: allocations are allowed
before the `service ready` marker and after shutdown begins; they are not allowed
while processing messages.

### Allowed at startup

- CLI argument parsing.
- Loading broker/service configuration.
- Creating `RingLoomEngine`.
- Creating `ServiceClient` instances.
- Waiting for service discovery.
- Allocating result histograms or summary buffers.
- Initializing fixed state arrays.

### Forbidden on the hot path

- `allocator.alloc`, `allocator.create`, or `ArrayList.append` that can grow.
- `std.fmt.allocPrint` or JSON formatting.
- Per-message logging.
- Opening files.
- String maps keyed by runtime strings.
- Capturing closures or heap-owned callback state.
- Retrying forever inside a message handler.

### Required hot-path patterns

- Use `ServiceClient.tryClaim` for normal sends.
- Fill RingLoom claim memory directly, then commit.
- Use `extern struct` payloads and pointer overlays.
- Use fixed arrays indexed by dense IDs.
- Use integer IDs for symbols, accounts, orders, and rejection reasons.
- Record counters with atomics or plain fields owned by one service thread.
- Return explicit error counters for back-pressure, unknown template, bad payload,
  and no available instance.
- Keep handler work bounded; defer cold reporting to the maintenance loop.

### Back-pressure behavior

Each producer should have a clear policy:

| Producer | Policy |
|---|---|
| Simulator | Pace down and count failed sends; optionally stop when failure rate exceeds a flag-defined threshold. |
| Gateway | Reject the order with a fixed `system_busy` reason when risk is unavailable or full. |
| Risk | Count remote send congestion and return a `system_busy` reject to gateway when possible. |
| Matching | Count execution-service congestion; do not block matching indefinitely. |
| Execution | Count portfolio congestion and keep processing fills; final summaries expose dropped reports. |

The policy should be visible in code comments near the send sites because it is a
domain decision, not a RingLoom default.

---

## Observability

Each service should write:

- one readiness line,
- lifecycle changes for target service availability,
- final counters on shutdown,
- optional JSON summary into `${WORKSPACE}/results`.

Recommended final counters:

```text
orders_generated
orders_validated
orders_rejected
risk_accepted
risk_rejected
fills_emitted
execution_reports_emitted
portfolio_updates_applied
send_buffer_full
back_pressure
no_available_instance
bad_payload
unknown_template
max_handler_ns
```

The startup script should print:

```bash
zig build stat -- --storage-path "$WORKSPACE/storage" --group order-management
```

or the equivalent `zig-out/bin/ringloom-stat` command if the stat utility is
already built.

---

## Testing strategy

The sample should have tests at three levels.

1. **Protocol unit tests**
   - envelope size and alignment,
   - template decoding,
   - malformed payload rejection,
   - round-trip encode/decode for every message type.

2. **Domain unit tests**
   - gateway validation,
   - risk limit accept/reject,
   - matching behavior with fixed books,
   - portfolio position updates.

3. **Smoke run**
   - build sample binaries,
   - start both brokers and six services,
   - run a small deterministic order count,
   - assert all result files exist,
   - assert generated count equals terminal portfolio/reject counts.

The smoke run can become a build step later:

```text
zig build sample-order-management-smoke
```

---

## Implementation sequence

1. Add sample build targets and empty service binaries.
2. Add `common/protocol.zig` with envelope, payload structs, and tests.
3. Add `common/app.zig` runner with signal handling and discovery helpers.
4. Implement services in downstream order: portfolio, execution, matching, risk,
   gateway, simulator.
5. Add `scripts/run.sh` with workspace generation, broker config generation, PID
   tracking, readiness waits, and reverse-order shutdown.
6. Add default-profile smoke run.
7. Add `--profile full` with extra risk/matching instances, leader routing, and
   lifecycle demonstration.
8. Tune buffer sizes, idle strategies, and result summaries after the smoke run
   is stable.

---

## Acceptance criteria

- `samples/order-management` contains all source, config templates, and scripts
  needed to run the sample.
- One command starts two brokers and all default services.
- The default run processes a deterministic number of orders and exits cleanly.
- Logs show local and cross-broker message paths being exercised.
- No service allocates after its readiness marker during normal message handling.
- The full profile demonstrates load balancing, leader-aware send, lifecycle
  callbacks, flow-control counters, and back-pressure accounting.
- The document and sample code are concise enough to serve as a reference for
  new RingLoom service authors.
