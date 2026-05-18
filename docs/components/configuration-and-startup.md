# Configuration and Startup

RingLoom configuration controls broker identity, shared-memory paths, Aeron
transport, threading, buffer sizes, monitoring, and flow control.

## Broker configuration

Core broker fields:

| Field | Purpose |
|---|---|
| `node_id` | Stable broker node identity. |
| `local_host`, `local_port` | UDP endpoint used by the broker Aeron data/admin subscriptions. |
| `peer_endpoints` | Peer node IDs and UDP endpoints. |
| `group_name` | Metadata namespace under the storage path. |
| `storage_path` | Base path for metadata and default Aeron directory derivation. |
| `control_buffer_size` | Broker control ring data capacity. |
| `messages_buffer_size` | Service messages ring default capacity. |
| `max_frame_length` | Application frame validation limit. |
| `threading_mode` | RingLoom loop layout. |
| `aeron_threading_mode` | Aeron agent assignment. |
| `fc_*` | Flow-control region and refresh settings. |

Legacy TCP-named fields may remain in config structs for compatibility or tests, but
the active remote data path is Aeron UDP.

## Aeron configuration

See [`aeron-runtime.md`](aeron-runtime.md) for full field descriptions. Operators
should tune term length, MTU, sparse files, liveness timeout, and directory cleanup
based on deployment constraints.

For local development, keep Aeron directories under `/dev/shm` so cleanup is simple
and term buffers do not hit slower persistent disks.

## Service configuration

Service config controls:

1. Service name.
2. Local broker node/group/storage path.
3. Control and messages ring capacities.
4. Heartbeat timeout.
5. Blocking vs non-blocking local producer mode.
6. Consumer mode: threaded or external polling.
7. Flow-control policy.

The service discovers Aeron details from broker metadata and registration responses;
application code should not construct Aeron publications manually.

## Startup sequence

Broker:

1. Load and validate config.
2. Start embedded Aeron driver and broker Aeron client.
3. Open broker UDP transport.
4. Create broker metadata with control ring and Aeron discovery.
5. Initialize control, sender, receiver, cluster, and monitoring state.
6. Start event-loop threads according to threading mode.

Service:

1. Open broker metadata.
2. Create service metadata.
3. Register with the broker over the broker control ring.
4. Receive registration response and discovery state.
5. Start control agent and message consumer.
6. Connect service Aeron runtime for remote outbound routes.

## Shutdown sequence

Services should unregister gracefully, stop producing, stop the consumer/control
agent, close cached producers/publications, and unmap metadata.

Brokers should stop receiver-facing work before freeing routing mappings, stop
control/sender loops, close Aeron publications/subscriptions/clients, stop the
embedded driver, and preserve or delete Aeron directories according to config.

## Operational commands

```bash
zig build run -- --config <path>
zig build stat
zig build observability
zig build run-observability -- --storage-path /dev/shm --group <group>
```
