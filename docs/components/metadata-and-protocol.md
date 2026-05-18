# Metadata and Protocol

RingLoom's stable local coordination surface is the metadata file. Metadata files are
memory-mapped under the configured storage path and group and are shared by brokers,
services, tools, and tests.

## Metadata file roles

| File kind | Owner | Readers | Main contents |
|---|---|---|---|
| Broker metadata | Broker process | Local services, tools | Identity, control ring, Aeron discovery, flow-control region, counters, error log. |
| Service metadata | Service process | Broker, local services, tools | Identity, control ring, messages ring, service Aeron discovery, counters, error log. |

Metadata v2 removes the old broker send ring as an active data-plane region. Service
messages ring buffers remain central because they are the local fast path and the
final delivery path for remote messages.

## Broker metadata

Broker metadata contains:

1. Header with metadata version, control buffer length, broker service ID `0`, node
   ID, PID, start timestamp, heartbeat, and next service ID allocator state.
2. Service -> broker control ring buffer.
3. Aeron discovery region:
   - Aeron directory.
   - Broker ingress stream ID.
   - Admin/data stream bases.
   - Local data/admin channels.
   - Peer data channel and stream metadata.
4. Optional flow-control region.
5. Monitoring tail with counter values, counter metadata, and error log.

The `messages_buffer_length` field is retained for compatibility with callers and
validation paths, but the broker metadata file does not rely on a broker send ring
for v2 remote data.

## Service metadata

Service metadata contains:

1. Header with metadata version, control/messages buffer lengths, service ID, node
   ID, blocking mode, PID, start timestamp, heartbeat timeout, and heartbeat.
2. Optional blocking trailer.
3. Service Aeron discovery region containing broker Aeron directory and broker
   ingress metadata.
4. Broker -> service control ring buffer.
5. Producers -> service messages ring buffer.
6. Monitoring tail with counter values, counter metadata, and error log.

Every service receives application messages from the messages ring buffer only.

## Ring buffers

Control and message buffers use the common MPSC ring-buffer implementation with a
fixed trailer for producer/consumer positions and heartbeat state. Capacities are
power-of-two byte sizes. Producers claim or write records; consumers poll a bounded
number of records per duty cycle.

Control rings carry RingLoom control messages: registration responses, discovery
snapshots, leader changes, lifecycle events, and heartbeat/control-plane updates.

Message rings carry application payloads plus service-visible metadata such as
template ID, correlation ID, source/target identity, flags, and payload length.

## RingLoom data header

Remote Aeron data frames carry a 32-byte header before the application payload:

```text
RingLoomDataHeader (32 bytes)
  magic[4]          "RLM2"
  version           2
  flags
  header_length     32
  correlation_id
  source_node_id
  source_service_id
  target_node_id
  target_service_id
  template_id
  reserved
  payload_length
```

The header is validated by the receiving broker before final delivery. The broker
then converts the data header to the service-visible message envelope for the target
messages ring buffer.

## Flags and fragmentation

Flags identify unfragmented messages and begin/end fragments. Local ring-buffer
messages use the same logical envelope fields; Aeron frames use the data header.

Large application payloads are handled by the message fragmentation and assembly
helpers in `src/common/message/`. Fragmentation preserves `template_id` and
`correlation_id`; consumers receive the reassembled payload through the normal
handler path.

## Admin messages

Broker-to-broker control traffic uses Aeron UDP admin streams. Admin envelopes carry
source node, target node, template ID, epoch, and payload length. Payloads represent
broker heartbeats, service-added/removal announcements, service leader changes,
capacity updates, and other cluster events.

Admin traffic is consumed by broker receivers and dispatched to control-plane
handlers. It is never delivered to application service handlers.

## Protocol invariants

1. RingLoom routes by `(target_node_id, target_service_id)`.
2. Application dispatch is by `template_id`.
3. `correlation_id` is copied through local and remote routes.
4. Aeron session IDs are diagnostics, not routing keys.
5. Metadata offsets and region lengths must be validated before external tools read
   optional regions.
6. Extern structs define wire/shared-memory layouts and must keep compile-time size
   assertions.
