# Broker Runtime

The RingLoom broker owns local service coordination and cross-node routing. It is not
in the hot path for local service-to-service data messages, but it is the final
delivery point for remote Aeron UDP data and the authority for service discovery.

## Main modules

| Module | Role |
|---|---|
| `src/broker/app/broker_runtime.zig` | Runtime assembly and lifecycle. |
| `src/broker/app/broker_application.zig` | Application-level broker object. |
| `src/broker/control/control_loop.zig` | Service registration, discovery, heartbeats, flow-control refresh, admin commands. |
| `src/broker/sender.zig` | Aeron sender-agent duty cycle. No broker send ring data path remains. |
| `src/broker/receiver/receiver_event_loop.zig` | Aeron UDP data/admin polling and final delivery. |
| `src/broker/receiver/message_router.zig` | Routing registry and writes into service messages ring buffers. |
| `src/broker/aeron.zig` | Broker Aeron client, UDP transport, stream/channel helpers. |
| `src/broker/cluster/*` | Peer membership, broker heartbeat, admin dispatch, broker leader election. |

## Event loops

### Control loop

The control loop is the broker's local authority. It:

1. Drains the service -> broker control ring buffer.
2. Registers and unregisters services.
3. Allocates service IDs and publishes registration responses.
4. Maintains local service discovery snapshots.
5. Processes admin commands received by the receiver loop.
6. Runs service heartbeat checks.
7. Updates flow-control regions and monitoring counters.
8. Broadcasts service-added, service-removed, leader-changed, capacity, and broker
   heartbeat messages over Aeron admin UDP.
9. Invokes the Aeron conductor agent when assigned by the threading mode.

### Sender loop

The sender loop exists to invoke Aeron's sender agent in modes where RingLoom owns that duty cycle.

### Receiver loop

The receiver loop:

1. Invokes the Aeron receiver agent when assigned.
2. Polls the broker data UDP subscription.
3. Decodes and validates `RingLoomDataHeader`.
4. Drops misdirected or malformed frames with counters.
5. Routes valid remote data to local service messages ring buffers.
6. Polls the broker admin UDP subscription.
7. Validates admin envelopes and posts admin commands to the control loop.
8. Tracks peer liveness from admin/data activity.

## Remote final delivery

Remote messages arrive as:

```text
RingLoomDataHeader (32 bytes) + application payload
```

The receiver validates:

1. Header magic/version/length.
2. Payload length.
3. Source and target node/service IDs.
4. Target node equals the local broker node.
5. Target service exists in the routing registry.

Then it converts the data header to the service-visible message envelope and writes
the payload into the target service messages ring buffer. The target service sees the
same consumer API that it uses for local traffic.

## Routing registry

The control loop updates the receiver routing registry when local services register
or unregister. Registry entries hold the target service ID, service name, node ID,
and a mapping of the target messages ring buffer for final delivery.

Unregister/removal avoids freeing mappings while the receiver might still read them;
cleanup happens after the receiver loop has stopped.

## Broker UDP transport

`BrokerUdpTransport` owns:

1. Data inbound subscription.
2. Admin inbound subscription.
3. Peer admin publications.
4. Peer metadata for data stream/channel discovery.

Broker-to-broker admin traffic uses broker publications. Service-to-broker remote
data traffic uses service-side direct publications to the target broker's data stream.

## Counters and errors

Broker counters distinguish:

1. Received bytes and routed frames.
2. Unknown target service drops.
3. Target service ring-buffer full drops.
4. Invalid frame drops.
5. Unknown peer or misdirected UDP drops.
6. Admin message receive/dispatch errors.
7. Aeron forwarding statuses for admin publications.
8. Service heartbeat and broker heartbeat timeouts.

Errors that affect startup are surfaced through broker startup/config/runtime exit
codes rather than being hidden behind fallback transport behavior.
