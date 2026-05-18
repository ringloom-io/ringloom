# Order Management Sample

The order-management sample under `samples/order-management/` is a runnable
multi-service RingLoom application. It demonstrates local shared-memory IPC,
cross-node Aeron UDP routing, service discovery, load balancing, leader routing,
flow-control handling, observability, and allocation-free payload handling.

For the sample's place in the overall architecture, see
[`components/samples.md`](components/samples.md).

## Goals

1. Provide a complete two-node RingLoom application that new users can read and run.
2. Exercise local and remote message paths in normal operation.
3. Keep per-message code allocation-free.
4. Keep domain logic independent from process wiring.
5. Expose useful logs, counters, result files, and `ringloom-stat` state.

Non-goals include durable persistence, external databases, JSON hot-path payloads,
and hiding RingLoom concepts behind a large framework.

## Topology

The default run starts two brokers and six service types:

```text
Node 1                                      Node 2
broker node_id=1                           broker node_id=2

order-simulator                            matching-engine
order-gateway                              execution-service
risk-service
portfolio-service
```

Default message flow:

```text
order-simulator
  -> order-gateway       local messages ring buffer
  -> risk-service        local messages ring buffer
  -> matching-engine     direct Aeron UDP to broker node 2
  -> execution-service   local messages ring buffer on node 2
  -> portfolio-service   direct Aeron UDP to broker node 1
```

Remote messages are delivered by the target broker into the destination service's
messages ring buffer, so handlers use the same dispatch path for local and remote
traffic.

## Optional full profile

The full profile adds extra `risk-service` and `matching-engine` instances. It
demonstrates:

1. Load balancing across instances with the same service name.
2. Local and remote instances behind one `ServiceClient`.
3. `sendToLeader` for the active matching engine.
4. Lifecycle callbacks when optional services stop and restart.

## Services

| Service | Default node | Role | RingLoom features demonstrated |
|---|---:|---|---|
| `order-simulator` | 1 | Generates deterministic order flow. | Producer pacing, fixed payload construction, back-pressure response. |
| `order-gateway` | 1 | Validates inbound orders and forwards risk checks. | Service discovery, local IPC, zero-copy claim/send. |
| `risk-service` | 1 | Applies account and symbol limits. | Local receive, remote send, fixed-size state tables. |
| `matching-engine` | 2 | Maintains a small in-memory book and emits fills/rejects. | Remote receive, leader-aware routing in full profile. |
| `execution-service` | 2 | Converts fills into execution reports. | Local receive/send, handler counters. |
| `portfolio-service` | 1 | Tracks positions and P&L. | Remote receive, snapshots, lifecycle visibility. |

## Payload protocol

The sample uses fixed-layout Zig `extern struct` payloads. RingLoom carries routing
metadata in its message envelopes and remote data header, so the domain payload only
needs application fields such as correlation ID, timestamps, account, symbol, side,
quantity, and price.

Example template IDs:

```zig
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
```

Every wire type should have compile-time size and alignment assertions. Variable text
is avoided on the hot path; fixed numeric identifiers or fixed byte arrays are used
instead.

## Feature coverage

| Feature | Demonstration |
|---|---|
| Shared-memory local IPC | Simulator -> gateway, gateway -> risk, matching -> execution. |
| Cross-node Aeron UDP | Risk -> matching and execution -> portfolio. |
| Service registration/discovery | Every service starts through the RingLoom engine and discovers named targets. |
| Load balancing | Full profile starts multiple instances behind one client. |
| Service leader routing | Full profile routes matching work to the elected leader. |
| Back-pressure | Producers count and respond to buffer-full/back-pressure errors. |
| Fragmentation/large payloads | Bulk-order messages exercise larger payload handling. |
| Observability | `ringloom-stat` and exporter can inspect the sample group. |
| Graceful shutdown | Startup script stops children in dependency-aware order. |

## Running

```bash
zig build sample-order-management
zig build run-sample-order-management
```

The sample script writes configs, starts brokers and services, waits for readiness,
runs the scenario, and prints a final summary. Use `ringloom-stat` or the Prometheus
exporter against the sample storage path/group to inspect runtime state while it runs.
