# Step 2: Per-Destination Send Buffers

## Objective

Replace the single broker-wide cross-host send ring buffer with a send-buffer directory and independent ring buffers keyed by remote destination service:

```
(target_node_id, target_service_id) -> MPSC destination send ring
```

This step should be implemented before the UDP transport cutover if possible. It can temporarily feed the existing TCP sender so shared-memory changes are tested in isolation.

## Source touchpoints

| File | Change |
|---|---|
| `src/common/memory/broker_metadata.zig` | Replace singular `send_buffer` field/accessor with v2 directory and send region |
| `src/common/memory/constants.zig` | Add v2 metadata version, directory entry sizes, default per-destination capacity |
| `src/common/memory/root.zig` | Re-export new directory types |
| `src/service/service_client.zig` | Replace `broker_send_ring_buffer` with destination buffer handles |
| `src/broker/control/*` | Provision destination buffers during discovery or first send request |
| `src/broker/sender/sender_event_loop.zig` | Read from multiple destination buffers instead of one global ring |
| `src/common/monitoring/*` | Add directory and per-destination counters to metadata reader/exporter |

## Data structures

Add:

```
SendBufferDirectoryHeader
  version: u32
  entry_count: u32
  entry_size: u32
  send_region_offset: u64
  send_region_length: u64

SendBufferEntry
  state
  pressure_state
  generation
  target_node_id
  target_service_id
  stream_id
  ring_offset
  ring_capacity
  max_message_length
  producer_count
  bytes_pending
  messages_pending
  counters...
```

Add helpers:

- `findByDestination(target_node_id, target_service_id)`,
- `findOrAllocateDestination(...)`,
- `validateHandle(index, generation)`,
- `ringSliceForEntry(entry)`,
- `markDraining(entry)`,
- `reclaimClosed(entry)`.

## ServiceClient behavior

1. Discovery attaches or allows lookup of a `SendBufferHandle`.
2. Sending to a remote service validates handle generation.
3. If handle is stale, refresh once through control/discovery state.
4. If no handle exists:
   - non-blocking send returns an explicit error,
   - spin strategy waits until provisioning deadline,
   - no hidden global fallback buffer is used.
5. Write payload format should be transport-neutral after Step 3. During a temporary TCP bridge, a compatibility encoder may still write TCP frames, but it must be isolated.

## Sender behavior

1. Maintain an active destination list derived from directory entries.
2. Round-robin or deficit-round-robin through destination buffers.
3. Consume only from buffers whose pressure state allows progress.
4. A full or blocked destination does not reduce the drain limit for other destinations.
5. Publish `bytes_pending`, `messages_pending`, and pressure state per entry.

## Tests

### Unit tests

1. Directory initializes with correct version, entry count, offsets, and zeroed entries.
2. `findOrAllocateDestination` returns stable entry for repeated destination lookup.
3. Generation mismatch invalidates stale handles.
4. Ring slices are aligned and have `capacity + trailer` bytes.
5. Directory full returns a deterministic error.
6. Draining entry rejects new ServiceClient sends but allows sender drain.
7. Reclaim bumps generation and clears counters.

### Service tests

1. ServiceClient sending to two remote services writes to two different ring buffers.
2. Filling destination A's buffer returns backpressure for A but does not affect sends to B.
3. Stale handle refresh succeeds after reclaim/reallocate.

### Broker tests

1. Sender scan consumes from multiple destination buffers fairly.
2. A blocked destination buffer is skipped without consuming its head record.
3. Destination counters update after writes, drains, drops, and reclaim.

### E2E tests

1. Two remote echo services: force one destination buffer small/full and verify the other destination still receives messages.
2. Multi-producer same destination: verify shared remote-destination buffer semantics are documented and counters reflect contention.

## Done criteria

- No production code depends on a broker-wide cross-host send ring.
- ServiceClient remote send uses destination handles.
- Sender progress is independent per destination buffer.
- Tests demonstrate that one full destination buffer does not block another destination.

