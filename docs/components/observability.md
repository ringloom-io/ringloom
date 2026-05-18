# Observability

RingLoom exposes runtime state through shared metadata files, Aeron CnC files,
`ringloom-stat`, and a Prometheus exporter.

## Design principles

1. Hot paths update atomics and ring-buffer positions only.
2. Expensive formatting and aggregation happen out of process.
3. Tools map metadata read-only and validate offsets before reading.
4. RingLoom counters and Aeron counters are shown together.
5. Stale metadata files are reported as stale rather than crashing tools.

## Metadata sources

| Source | Data |
|---|---|
| Broker metadata | Broker identity, heartbeat, control ring stats, flow-control region, counters, error log, Aeron discovery. |
| Service metadata | Service identity, heartbeat, control/messages ring stats, counters, error log. |
| Aeron CnC | Driver status, client liveness, publication/subscription positions, NAKs, retransmits, flow-control counters. |

## Tools

| Tool | Purpose |
|---|---|
| `ringloom-stat` | One-shot human CLI for local inspection. |
| `ringloom-observability` | Long-running HTTP exporter for Prometheus. |
| E2E/perf result writers | Persist benchmark and recovery artifacts for regression analysis. |

## Prometheus metric groups

Metrics use the `ringloom_` prefix for RingLoom state and `ringloom_aeron_` for
Aeron-derived state.

Important groups:

1. Metadata file liveness and heartbeat age.
2. Broker and service generic counters.
3. Direct stable counters for common runtime totals.
4. Ring capacity, used bytes, free bytes, and usage ratio.
5. Flow-control pressure state and update age.
6. Aeron driver, publication, subscription, and system counters.
7. Exporter self-metrics for scrape count, scan errors, invalid metadata, and
   truncation.

Avoid high-cardinality labels such as full file paths, arbitrary payload data, or
error descriptions.

## Ring and transport diagnostics

Local pressure is diagnosed from service messages ring usage. Remote pressure is
diagnosed from Aeron publication statuses and CnC counters. Broker final-delivery
drops are diagnosed from receiver counters such as target-service-full and invalid
frame drops.

The active architecture has no TCP write queue or broker send ring metrics.

## Operator commands

```bash
zig build stat
zig build observability
zig build run-observability -- --storage-path /dev/shm --group ringloom --listen 127.0.0.1:9464
```

See [`../observability.md`](../observability.md) for the exporter operator guide.
