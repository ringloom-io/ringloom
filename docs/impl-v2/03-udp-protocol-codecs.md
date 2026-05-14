# Step 3: UDP Protocol and Term Logs

## Objective

Introduce transport-neutral reliable UDP protocol primitives: frame codecs, stream identity, position arithmetic, term logs, retransmit addressing, and malformed-frame validation.

## Source touchpoints

| File/dir | Change |
|---|---|
| `src/udp/` | New module for protocol, term log, loss/retransmit state |
| `src/common/protocol/` | Move shared RingLoom route-envelope definitions here if needed |
| `src/tcp/frame.zig` | Stop using as remote send payload contract |
| `src/service/service_client.zig` | Stop writing TCP frame headers after sender has a UDP envelope format |
| `build.zig` | Add `ringloom_udp` module and tests |

## Types to add

1. `CommonHeader`
2. `SetupHeader`
3. `SetupResponseHeader`
4. `DataHeader`
5. `StatusHeader`
6. `NakHeader`
7. `RttmHeader`
8. `HeartbeatHeader`
9. `ErrorHeader`
10. `StreamId`
11. `StreamKey`
12. `TermLog`
13. `Position`
14. `RetransmitAction`
15. `LossRange`

## Stream identity

Recommended stream key:

```
StreamKey = {
    source_node_id,
    target_node_id,
    target_service_id,
}
```

`source_service_id` remains in DATA route metadata. This keeps the transport stream aligned with the per-destination send-buffer key while preserving source service identity for applications.

`stream_id` should be a stable hash of `StreamKey` plus generation guard in setup metadata. Collisions must be detected by comparing full stream keys during SETUP.

## Codec requirements

1. No heap allocation while encoding or decoding headers.
2. All encoded sizes are comptime asserted.
3. All multi-byte fields are little-endian.
4. Decoders validate:
   - magic,
   - version,
   - frame type,
   - `header_length`,
   - `frame_length`,
   - reserved fields,
   - route fields for DATA,
   - MTU limits.
5. Invalid frames return typed errors for counters and tests.

## Term log requirements

1. Three term partitions per stream.
2. Power-of-two term length.
3. 32-byte frame alignment.
4. Position arithmetic:
   - pack/unpack `(term_id, term_offset)`,
   - compute absolute position,
   - compute partition index.
5. Append committed DATA frames with release store on `frame_length`.
6. Scan committed frames for normal send and retransmit ranges.
7. Rotate terms only when safe relative to acknowledged receiver position.

## Tests

### Codec unit tests

1. Header sizes and field offsets match the architecture.
2. Encode/decode round trip for every frame type.
3. Invalid magic/version/type is rejected.
4. Frame length below header length is rejected.
5. Frame length above MTU is rejected.
6. DATA route fields round trip:
   - node IDs,
   - service IDs,
   - template ID,
   - correlation ID,
   - flags.
7. SETUP validates group hash and target node.
8. NAK range validation rejects zero length and out-of-term requests.

### Term log unit tests

1. Append single frame and scan it back.
2. Append fragments of one message and preserve message ID/offsets.
3. Rotate terms at boundary and write padding.
4. Reject append when sender position reaches sender limit.
5. Retransmit scan returns only committed frames.
6. Retransmit scan clamps to term boundary and configured max length.
7. Duplicate or stale session ID is detected by stream/session validation helpers.

## Done criteria

- `ringloom_udp` protocol tests pass independently of broker runtime.
- No new UDP protocol code imports `ringloom_tcp`.
- Service/broker code can depend on route envelopes without depending on TCP frame headers.

