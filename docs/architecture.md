# RingLoom Architecture

RingLoom is a low-latency service messaging runtime built around two data-plane paths:

1. **Same-host delivery** uses memory-mapped service metadata files and lock-free
   message ring buffers. Services write directly to the target service's messages
   ring buffer; the broker is not on the local hot path.
2. **Cross-host delivery** uses Aeron UDP. A service connects to the broker-owned
   embedded Aeron media driver on its host and publishes remote-target frames
   directly to the target node's broker. The target broker validates the frame and
   writes the payload into the destination service's local messages ring buffer.

The broker still owns registration, service discovery, heartbeats, cluster membership,
leader election, observability metadata, and final delivery for remote messages.
Control-plane traffic stays on metadata control ring buffers locally and Aeron UDP
admin streams between brokers.

## Documentation map

| Document | Audience | Contents |
|---|---|---|
| [`components/aeron-runtime.md`](components/aeron-runtime.md) | Runtime implementers, operators | Embedded Aeron driver, Zig wrapper, channels, streams, and threading modes. |
| [`components/broker.md`](components/broker.md) | Broker contributors | Broker startup, control/sender/receiver loops, Aeron UDP data/admin routing. |
| [`components/service-client.md`](components/service-client.md) | Service authors, binding authors | Service runtime, `ServiceClient`, local ring buffers, direct remote UDP, load balancing. |
| [`components/metadata-and-protocol.md`](components/metadata-and-protocol.md) | LLM agents, contributors | Metadata file layout, ring-buffer regions, data headers, admin/control envelopes. |
| [`components/clustering.md`](components/clustering.md) | Runtime contributors, operators | Peer membership, broker leader election, service leadership, discovery propagation. |
| [`components/flow-control.md`](components/flow-control.md) | Runtime contributors, operators | Local ring capacity, Aeron back-pressure, send errors, counters, recovery semantics. |
| [`components/configuration-and-startup.md`](components/configuration-and-startup.md) | Operators, test authors | Broker/service config, metadata paths, Aeron directories, startup/shutdown lifecycle. |
| [`components/bindings.md`](components/bindings.md) | Binding maintainers | C ABI and C++, Java, and Node.js bindings. |
| [`components/observability.md`](components/observability.md) | Operators, tooling authors | `ringloom-stat`, Prometheus exporter, RingLoom counters, Aeron CnC metrics. |
| [`components/testing.md`](components/testing.md) | Contributors, agents | Unit, e2e, perf, binding, and sample test coverage. |
| [`components/samples.md`](components/samples.md) | New users, demo authors | Order-management sample topology and feature coverage. |

## Design goals

1. Keep local messaging allocation-free and broker-free after route discovery.
2. Use Aeron for cross-host loss detection, retransmission, publisher limits, and
   receiver-driven flow control instead of maintaining a custom TCP stack.
3. Preserve the RingLoom addressing model: `(node_id, service_id)`, `template_id`,
   `correlation_id`, and payload bytes.
4. Normalize all inbound messages to the same service-visible receive API, whether
   the message originated locally or on another node.
5. Keep control traffic explicit and inspectable through shared metadata and admin
   streams.
6. Make runtime state visible without adding allocations or syscalls to hot paths.

## Non-goals and design decisions

RingLoom does **not** use Aeron Cluster. RingLoom's cluster is a routing and discovery
mesh, not a replicated state machine. Broker leadership is lightweight and heartbeat
based; application services can independently elect service leaders.

RingLoom does **not** use Aeron Archive for message durability. Delivery is
best-effort across service or broker crashes. Aeron can retransmit UDP loss while
images remain live, but RingLoom does not promise durable replay.

## Core topology

```text
Host A / node 1                                           Host B / node 2

+---------------------+                                  +---------------------+
| Service producer    |                                  | Service consumer    |
| - ServiceClient     |                                  | - MessageConsumer   |
| - Aeron client      |                                  | - messages ring RB  |
+----------+----------+                                  +----------^----------+
           | local target: direct ring-buffer write                  |
           | remote target: direct Aeron UDP publication             |
           v                                                         |
+----------+---------------------------------------------------------+----------+
| Broker node 1                                          Broker node 2         |
| - embedded Aeron media driver                          - embedded Aeron      |
| - control loop                                         - receiver loop       |
| - sender agent loop                                    - routing registry    |
| - receiver/admin loop                                  - final ring delivery |
+--------------------------------------------------------------------------------+
```

The service API hides the route choice. Service code sends by service name, explicit
instance, or elected leader. The client runtime chooses the current target instance,
then either writes into a local messages ring buffer or publishes an Aeron frame to
the peer broker's data stream.

## Control plane and data plane

| Plane | Local mechanism | Cross-node mechanism | Main owners |
|---|---|---|---|
| Service registration | Service -> broker control ring buffer | Service announcements on Aeron admin UDP | `ControlLoop`, `ControlAgent` |
| Discovery updates | Broker -> service control ring buffer | Service-added/removed admin messages | `ServiceRegistry`, `ClusterManager` |
| Service heartbeats | Metadata heartbeat fields/control messages | Admin service lifecycle messages | Control loop and service control agent |
| Broker heartbeats | N/A | Aeron admin UDP heartbeat messages | Cluster manager/control loop |
| Local application data | Target service messages ring buffer | N/A | `ServiceClient`, `IpcProducer`, `MessageConsumer` |
| Remote application data | Final target messages ring buffer | Aeron UDP data stream | Service Aeron runtime, broker receiver |

## End-to-end message flows

### Local service to local service

1. The target service registers and creates a service metadata file with a messages
   ring buffer.
2. The broker publishes the service instance through discovery.
3. The source service opens an `IpcProducer` over the target service's messages
   ring buffer.
4. `ServiceClient` claims or writes a record directly into that buffer.
5. The target `MessageConsumer` polls the buffer and dispatches the payload handler.

### Service to remote service

1. The source service receives discovery data for a target on a different `node_id`.
2. The service-side Aeron runtime lazily opens an exclusive publication for that
   peer broker's data channel and stream.
3. `ServiceClient` writes a `RingLoomDataHeader` followed by the application payload
   into the Aeron claim or offer buffer.
4. Aeron sends the UDP data frame to the peer broker's embedded media driver.
5. The peer broker receiver loop polls the data subscription, validates the header,
   checks target identity, and writes the payload into the target service messages
   ring buffer.
6. The target service consumes the message through the same handler path used for
   local traffic.

### Broker admin and cluster traffic

Cluster membership, broker heartbeats, service-added/removal announcements, service
leader changes, and capacity advisories use reserved Aeron admin UDP streams. Admin
payloads are dispatched to broker control-plane handlers; they are not delivered to
application service handlers.

## Module dependency graph

```text
ringloom_aeron      Aeron C shim and Zig wrappers

ringloom_common     metadata files, ring buffers, headers/codecs, monitoring,
                    config loading, platform helpers

ringloom_service    service runtime, ServiceClient, control agent, local IPC,
                    direct remote Aeron publications, C ABI

ringloom_broker     broker runtime, control/sender/receiver loops, routing,
                    clustering, leader election, embedded Aeron driver

ringloom_testing    multi-process harness, config generation, process control,
                    readiness checks, benchmark helpers
```

The broker depends on `ringloom_common` and `ringloom_aeron`. The service runtime
depends on `ringloom_common` and uses `ringloom_aeron` for remote outbound routes.
Bindings are layered on the C ABI exported by `ringloom_service`.

## Threading model

RingLoom preserves broker duty-cycle modes while embedding Aeron's media driver:

| Mode | Broker loops | Aeron agent assignment |
|---|---|---|
| `dedicated` | control, sender, receiver | conductor on control; sender on sender; receiver on receiver |
| `shared_network` | control, network | conductor on control; sender and receiver on network |
| `shared` | one composite loop | conductor, sender, receiver, control, and routing in one loop |

Services keep their control agent and message consumer modes. Service Aeron clients
use the broker-owned Aeron directory and invoke client housekeeping from the service
runtime.

## Addressing and stream namespace

RingLoom identity remains independent from Aeron identity.

| Concept | Owner | Purpose |
|---|---|---|
| `node_id` | Broker config | Identifies the broker node and is copied into RingLoom headers. |
| `service_id` | Broker metadata allocator | Identifies a registered service instance on a node. |
| `service_name` | Service config | Groups instances for discovery and load balancing. |
| `template_id` | Application protocol | Identifies the application payload shape. |
| `correlation_id` | Sender | Request/response and latency correlation. |
| Aeron `stream_id` | Broker config/stream base | Data and admin stream selection. |
| Aeron `session_id` | Aeron driver | Diagnostic identity; not used for RingLoom routing. |

By convention, broker configs derive stream IDs from base values plus node ID:
broker ingress, admin, and data ranges are configured separately so tooling can
classify streams.

## Metadata and observability

Every broker and service writes a metadata file under the configured storage path
and group. Metadata files contain identity, heartbeat timestamps, discovery data,
ring-buffer regions, counters, and error logs. Brokers also advertise Aeron
directory and peer channel information so services can create direct remote
publications.

Operators can inspect metadata through `ringloom-stat` or scrape it through the
Prometheus exporter. Aeron driver state is read from the broker-owned CnC file and
reported alongside RingLoom counters.

## Delivery, ordering, and failure semantics

RingLoom is best-effort across process and broker failure. Messages already visible
in a service messages ring buffer are available to that service until overwritten by
normal ring-buffer progress; messages in Aeron or in producer-local scratch buffers
can be lost if a process dies.

Ordering is per producer-to-target route. RingLoom does not define a total order
between local ring-buffer writes and remote Aeron-originated writes arriving at the
same target service.

Back-pressure is explicit:

1. Local sends fail or wait based on the target messages ring buffer capacity.
2. Remote sends surface Aeron publication statuses such as back-pressured,
   not-connected, closed, or max-position-exceeded.
3. Broker final delivery to a full target messages ring buffer is counted and
   handled by the configured local delivery policy.

## Running and validating the system

Common commands:

```bash
zig build test
zig build e2e
zig build perf
zig build run -- --config <path>
zig build stat
zig build observability
```

See [`docs/testing.md`](testing.md) for developer-oriented commands and
[`components/testing.md`](components/testing.md) for how the test layers map to the
architecture.
