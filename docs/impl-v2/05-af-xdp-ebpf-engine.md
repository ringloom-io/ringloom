# Step 5: AF_XDP and eBPF Endpoint

## Objective

Add optional AF_XDP kernel-bypass transport for configured RingLoom UDP ports while preserving POSIX UDP fallback.

## Source touchpoints

| File/dir | Change |
|---|---|
| `src/udp/af_xdp_endpoint.zig` | AF_XDP endpoint and UMEM management |
| `src/udp/xdp_filter.*` | eBPF/XDP filter source or embedded bytecode |
| `src/udp/endpoint.zig` | Engine selection and common endpoint interface |
| `src/common/config/*` | AF_XDP mode, interface, port, queue, UMEM options |
| `build.zig` | Optional build/link steps for eBPF artifact if needed |
| `src/common/monitoring/*` | AF_XDP counters |

## XDP filter requirements

The XDP program must:

1. Parse Ethernet.
2. Parse IPv4 and IPv6 enough to locate UDP headers.
3. Return `XDP_PASS` for non-UDP traffic.
4. Return `XDP_PASS` for UDP destination ports not configured for RingLoom.
5. Return `XDP_PASS` for fragmented packets by default.
6. Redirect matching UDP packets to XSKMAP for the configured RX queue.
7. Return `XDP_PASS` if redirect target is missing.

This port guard is mandatory. Kernel bypass must not steal unrelated traffic.

## AF_XDP endpoint requirements

1. Probe kernel/NIC capabilities.
2. Support config modes:
   - `posix`,
   - `prefer_af_xdp`,
   - `require_af_xdp`.
3. Allocate UMEM, fill rings, RX rings, TX rings, completion rings.
4. Bind XSK to interface and queue.
5. Attach XDP program.
6. Populate XSK map.
7. Poll RX without allocation.
8. Send TX frames with user-space Ethernet/IP/UDP headers and checksums.
9. Resolve destination MAC addresses through a bounded neighbor resolver.
10. Detach XDP program and close XSK on shutdown if this broker attached it.

## Fallback policy

| Mode | AF_XDP setup failure |
|---|---|
| `posix` | Do not attempt AF_XDP |
| `prefer_af_xdp` | Log fallback, increment counter, start POSIX UDP |
| `require_af_xdp` | Fail broker startup |

Generic XDP is not considered kernel bypass for production. If config allows generic mode, counters must identify it clearly.

## Operational notes to document in user-facing config

1. Required capabilities and sysctls.
2. NIC/driver requirements.
3. RX queue steering requirement.
4. Container/Kubernetes constraints.
5. No simultaneous POSIX receive on redirected port.
6. PMTUD and fragmented IP policy.

## Tests

### Unit tests

1. XDP filter decision table:
   - TCP packet -> pass,
   - UDP wrong port -> pass,
   - UDP configured port -> redirect when XSK exists,
   - fragmented IP -> pass,
   - malformed packet -> pass or abort safely.
2. AF_XDP config validation:
   - missing interface,
   - invalid queue,
   - no configured ports,
   - `require_af_xdp` failure mapping.
3. UMEM frame allocator:
   - allocate/free,
   - exhaustion,
   - descriptor bounds.
4. UDP/IP checksum helpers for IPv4 and IPv6.

### Capability-gated integration tests

These tests skip when AF_XDP is unavailable:

1. Attach filter to test interface and verify unrelated UDP port still reaches kernel.
2. Verify configured RingLoom UDP port reaches AF_XDP socket.
3. AF_XDP endpoint loopback or veth pair sends and receives one frame.
4. POSIX peer can communicate with AF_XDP peer.

### Fallback tests

1. `prefer_af_xdp` with unavailable AF_XDP starts POSIX UDP and increments fallback counter.
2. `require_af_xdp` with unavailable AF_XDP fails startup with a clear config/startup error.

## Done criteria

- AF_XDP is optional and does not affect normal CI.
- XDP program redirects only configured RingLoom UDP ports.
- Fallback behavior is deterministic and tested.
- POSIX UDP remains the default safe engine.

