# Service Runtime and Client API

Services use one messaging API for local and remote targets. The service runtime
handles registration, discovery, local ring-buffer producers, remote Aeron
publications, heartbeats, message consumption, counters, and shutdown.

## Main modules

| Module | Role |
|---|---|
| `src/service/ringloom_engine.zig` | Service lifecycle and runtime assembly. |
| `src/service/service_client.zig` | Public send/request/claim API and route selection. |
| `src/service/aeron_runtime.zig` | Service-side Aeron client and direct peer publications. |
| `src/service/service_client_registry.zig` | Client lookup and lifecycle management. |
| `src/service/service_instance.zig` | Discovered target instance metadata. |
| `src/service/load_balancer.zig` | Target selection across instances. |
| `src/service/control_agent.zig` | Registration, discovery updates, heartbeats, leader updates. |
| `src/service/message_consumer.zig` | Polls service messages ring buffer and dispatches handlers. |
| `src/service/ipc/*` | Local ring-buffer producers and consumers. |
| `src/service/c_abi.zig` | Stable ABI used by language bindings. |

## Service startup

1. The service opens local broker metadata and reads broker Aeron discovery data.
2. It creates its service metadata file with a control ring buffer and messages ring
   buffer.
3. It registers through the broker control ring buffer.
4. The broker replies with assigned `service_id`, heartbeat/control parameters, and
   Aeron discovery details.
5. The service starts its control agent and message consumer.
6. The service Aeron runtime connects to the broker-owned Aeron directory for
   remote outbound routes.

## Route selection

`ServiceClient` chooses a target instance by API call and load-balancer state:

| API shape | Target selection |
|---|---|
| Send by service name | Load-balanced across discovered healthy instances. |
| Send to explicit instance | Uses the requested service ID/node ID. |
| Send to leader | Uses the discovered elected service leader. |
| Request/response | Sends with a correlation ID and caller-managed response route. |
| `tryClaim` | Returns a payload slice for zero-copy fill, then commits or aborts. |

After target selection:

1. If `target_node_id == local_node_id`, the client uses an `IpcProducer` for the
   target service messages ring buffer.
2. If `target_node_id != local_node_id`, the client encodes `RingLoomDataHeader`
   and sends through the service Aeron runtime's direct peer UDP publication.

## Local ring-buffer fast path

The local path is:

```text
source ServiceClient -> target service messages ring buffer -> target MessageConsumer
```

The broker is involved only before the send, through registration and discovery.
The source service caches producers for target instances and writes records with the
service-visible message envelope. Local capacity checks are exact because the target
ring buffer is mapped in the source process.

## Remote direct UDP path

The remote path is:

```text
source ServiceClient
  -> ServiceAeronRuntime direct peer publication
  -> Aeron UDP
  -> target broker receiver loop
  -> target service messages ring buffer
  -> target MessageConsumer
```

The service runtime lazily creates one exclusive Aeron publication per peer node
from broker discovery metadata. Remote sends encode:

```text
RingLoomDataHeader + application payload
```

For small frames, `tryClaim` writes directly into Aeron's publication buffer. Larger
frames use an owned scratch buffer and `offer`.

## Load balancing and leaders

The service client tracks discovered instances for a service name. Load balancing is
kept separate from routing:

1. The load balancer selects a service instance.
2. The route layer decides local ring buffer vs remote Aeron UDP.
3. Flow-control and send errors are recorded against the chosen route.

Service leadership is propagated by control/admin messages. `sendToLeader` fails
with a leader-specific error when no leader is known rather than silently choosing
an arbitrary instance.

## Consumer modes

Services can run a threaded consumer or externally poll. Both modes read only the
service messages ring buffer, so application handlers do not need to care whether a
message originated from a local service or from a remote Aeron route.

## Error model

Common send errors include:

| Error | Typical source |
|---|---|
| `NoAvailableInstance` | No discovered instance for the service name. |
| `NoLeaderAvailable` | Leader-routed send before a leader is known. |
| `SendBufferFull` / `BackPressure` | Local ring buffer capacity or remote Aeron publisher limit. |
| `RemoteTransportUnavailable` | Missing Aeron discovery, no peer publication, or failed remote commit. |
| `PeerDisconnected` | Aeron publication is not connected. |
| `MessageTooLong` | Payload exceeds the local ring or Aeron route limit. |

The API does not hide remote transport failures behind success-shaped fallbacks.
