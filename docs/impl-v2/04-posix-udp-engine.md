# Step 4: POSIX UDP Endpoint

## Objective

Implement the portable UDP transport endpoint used by default and used as fallback when AF_XDP is unavailable.

## Source touchpoints

| File/dir | Change |
|---|---|
| `src/udp/posix_endpoint.zig` | New POSIX UDP endpoint |
| `src/udp/endpoint.zig` | Shared endpoint interface |
| `src/broker/transport/` | Broker adapter for UDP endpoint polling/sending |
| `src/common/config/*` | Add UDP socket config keys in Step 9; use defaults during this step |
| `src/testing/config_gen.zig` | Later emits UDP config |

## Endpoint behavior

1. Bind one non-blocking UDP socket per broker local endpoint.
2. Use `recvmmsg`/`sendmmsg` on Linux when available.
3. Fall back to `recvmsg`/`sendmsg` loops.
4. Set socket send/receive buffer sizes from config.
5. Set DF / PMTUD where supported.
6. Return explicit packet metadata:
   - source address,
   - destination/local address if available,
   - ECN/TOS if later needed,
   - received byte length.
7. Never allocate while polling packets.

## Packet batching

The endpoint should support:

```
poll(packets: []PacketView, scratch: []u8) !usize
send(packet: []const u8, destination: Address) !usize
sendBatch(batch: []const OutboundPacket) !usize
```

`PacketView` references slices inside preallocated receive buffers. The receiver event loop must finish processing or copy required bytes before the next poll overwrites those buffers.

## Error handling

| Error | Behavior |
|---|---|
| `WouldBlock` | no work |
| send buffer full | report retryable send pressure |
| EMSGSIZE / MTU error | increment MTU counter and notify stream |
| connection refused ICMP on connected UDP | mark peer/session suspect, do not crash |
| malformed address config | startup config error |

## Tests

### Unit tests

1. Address parse and formatting for IPv4 endpoints.
2. Socket config validation rejects invalid MTU and buffer sizes.
3. Endpoint init/deinit closes file descriptors.
4. `WouldBlock` maps to no-work result.

### Integration tests

1. Two endpoints on loopback exchange a DATA-sized datagram.
2. Batch send/receive preserves datagram boundaries.
3. Oversized datagram returns MTU/size error.
4. Endpoint receives only on bound port.
5. Non-blocking poll returns promptly when idle.

### Broker smoke test

After Step 6/7 wiring, run a two-broker POSIX UDP cross-routing smoke test before AF_XDP work starts.

## Done criteria

- POSIX UDP endpoint passes standalone tests.
- Endpoint interface is sufficient for sender and receiver loops.
- No AF_XDP code is needed for normal test execution.

