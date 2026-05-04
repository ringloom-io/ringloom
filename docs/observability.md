# RingLoom Observability

This document designs the standalone observability process that exposes RingLoom broker
and service shared-memory metrics as Prometheus text format.

The exporter is intentionally out-of-process. Brokers and services keep the hot path to
atomic counter updates and ring-buffer position writes; the exporter discovers metadata
files, maps them read-only, samples counters, derives gauges, and serves `/metrics`.

---

## Goals

1. Expose all broker and service metadata counters through Prometheus.
2. Export derived ring-buffer occupancy for broker and service rings without adding hot-path
   counter writes.
3. Export flow-control and per-peer send state already present in broker metadata.
4. Continue serving partial metrics if a broker or service exits, restarts, or leaves a
   stale metadata file behind.
5. Share metadata parsing logic with `ringloom-stat` where practical, but keep
   `ringloom-stat` as a one-shot human CLI and `ringloom-observability` as the long-running
   scrape endpoint.

Non-goals:

1. No metrics aggregation inside broker or service processes.
2. No Prometheus remote-write client.
3. No dashboard generation.
4. No application-specific metric schema beyond reading application counters allocated in
   service metadata.

---

## Executable

Name: `ringloom-observability`

Suggested build steps:

```bash
zig build observability
zig build run-observability -- --storage-path /dev/shm --group ringloom --listen 127.0.0.1:9464
```

Suggested CLI:

| Flag | Default | Description |
|---|---|---|
| `--storage-path PATH` | `/dev/shm` | Base metadata path. |
| `--group GROUP` | `default` | RingLoom group directory under the storage path. |
| `--listen HOST:PORT` | `127.0.0.1:9464` | HTTP listen address. |
| `--refresh-ms N` | `1000` | Metadata directory rescan interval. |
| `--broker-node-id N` | unset | Optional single broker filter. |
| `--service-name NAME` | unset | Optional repeatable service-name filter. |
| `--include-stale` | `true` | Include dead/stale metadata files with liveness gauges. |
| `--max-files N` | `4096` | Safety cap for metadata files scanned per refresh. |
| `--max-response-bytes N` | `8388608` | Safety cap for generated `/metrics` response body. |

Endpoints:

| Path | Response |
|---|---|
| `/metrics` | Prometheus text exposition. |
| `/healthz` | `200 OK` when the HTTP server is running. |
| `/readyz` | `200 OK` after the first successful metadata scan, otherwise `503`. |

---

## Architecture

```
┌──────────────────────────────┐
│ ringloom-observability       │
│                              │
│  MetadataScanner             │
│    scans services/           │
│    opens broker/service dat  │
│                              │
│  MetadataCatalog             │
│    owns read-only mmaps      │
│    refreshes stale entries   │
│                              │
│  SampleRenderer              │
│    atomic-load counters      │
│    derive ring gauges        │
│    format Prometheus text    │
│                              │
│  std.http Server             │
│    /metrics /healthz /readyz │
└──────────────────────────────┘
```

A single-threaded implementation is sufficient initially:

1. The accept loop handles one scrape at a time.
2. Before rendering, it refreshes the metadata catalog if `refresh-ms` has elapsed.
3. Rendering reads directly from mapped memory using acquire loads.
4. The response is written using a bounded `std.Io.Writer` buffer or a streaming response.

If high scrape concurrency is needed later, use one accept thread plus a read-mostly catalog
protected by a `std.Thread.RwLock`. Do not move aggregation into broker or service processes.

---

## Metadata discovery

The scanner reads:

```text
<storage_path>/<group>/services/
```

Broker files:

```text
broker_<node_id>.dat
```

Service files:

```text
<service_name>_node<node_id>_<service_id>.dat
```

For every file:

1. Open read-only.
2. `stat` file size.
3. `mmap` read-only with `MAP_SHARED`.
4. Parse the fixed 512-byte metadata header.
5. Validate all fixed regions.
6. Read monitoring-tail offsets and lengths if present.
7. Keep the mapping in the catalog until the next rescan removes or replaces it.

Catalog entries should include:

| Field | Meaning |
|---|---|
| `path` | Absolute file path. |
| `kind` | broker or service. |
| `node_id` | Owner node ID. |
| `service_id` | Broker is 0; service ID for services. |
| `service_name` | `broker` for brokers; parsed name for services. |
| `pid` | Owner PID from metadata header. |
| `start_timestamp_ms` | Owner start timestamp. |
| `heartbeat_time_ms` | Latest heartbeat timestamp. |
| `mapped` | Read-only mapping. |
| `regions` | Validated region descriptors. |
| `last_seen_scan` | Scan generation. |

Stale files are not fatal. The exporter should emit liveness and heartbeat-age gauges for
them when `--include-stale=true`, and it should skip invalid regions while incrementing
exporter self-counters.

---

## Metadata validation

The exporter must validate before every read that depends on offsets:

1. File length is at least 512 bytes.
2. Header buffer lengths are positive powers of two.
3. Ring regions include the 768-byte ring trailer.
4. Every computed or header-provided offset/length fits within file length.
5. Counter values length is a multiple of 128.
6. Counter metadata length is a multiple of 256 and has at least as many slots as values.
7. Error-log length is zero or large enough for at least one entry header.
8. Flow-control and per-peer region versions match expected versions before fields are read.
9. Labels read from counter metadata are valid bounded byte slices; invalid labels are
   sanitized at render time rather than trusted.

Invalid files or regions should be skipped with exporter self-metrics, not logged on every
scrape. Rate-limit warnings to avoid log spam from stale files.

---

## Metrics model

All metric names emitted by the exporter use the `ringloom_` prefix.

Common labels:

| Label | Applies to | Description |
|---|---|---|
| `group` | all metrics | RingLoom group. |
| `owner_type` | metadata metrics | `broker` or `service`. |
| `node_id` | all owner metrics | Owning node. |
| `service_id` | service metrics, broker as `0` | Service ID. |
| `service_name` | service metrics | Service name. |
| `ring` | ring gauges | `control`, `send`, or `messages`. |
| `peer_node_id` | per-peer metrics | Peer broker node. |
| `slot` | flow-control metrics | Flow-control slot index. |

Do not add high-cardinality labels such as file path, PID, error description, or arbitrary
payload/template data to hot metrics. PID can be exported as a gauge value if needed.

### Generic metadata counters

Every allocated counter from the metadata `CountersManager` is exported as:

```text
ringloom_counter{group="...",owner_type="broker",node_id="1",service_id="0",service_name="broker",counter="broker_bytes_sent_total"} 123
```

For stable runtime counters listed in `docs/impl/12-configuration-and-monitoring.md`, the
exporter should also emit a direct metric name:

```text
ringloom_broker_bytes_sent_total{group="...",node_id="1"} 123
ringloom_service_messages_sent_total{group="...",node_id="1",service_id="42",service_name="orders"} 456
```

The direct names make dashboards and alerts easy to write. The generic `ringloom_counter`
metric preserves visibility for application-defined service counters without hard-coding
their names in the exporter.

### Process and metadata gauges

| Metric | Type | Labels | Description |
|---|---|---|---|
| `ringloom_metadata_file_up` | gauge | common owner labels | 1 when the file could be opened and parsed. |
| `ringloom_process_alive` | gauge | common owner labels | 1 when the PID appears alive, else 0. |
| `ringloom_heartbeat_age_seconds` | gauge | common owner labels | Current wall-clock age of owner heartbeat. |
| `ringloom_process_start_time_seconds` | gauge | common owner labels | Start timestamp from metadata header. |
| `ringloom_metadata_version` | gauge | common owner labels | Metadata version, 0 for legacy files. |

### Ring gauges

Derived from ring trailer positions:

| Metric | Type | Description |
|---|---|---|
| `ringloom_ring_capacity_bytes` | gauge | Ring data capacity excluding trailer. |
| `ringloom_ring_used_bytes` | gauge | `producer_position - consumer_position`, clamped to `[0, capacity]` for rendering. |
| `ringloom_ring_free_bytes` | gauge | `capacity - used_bytes`. |
| `ringloom_ring_usage_ratio` | gauge | `used_bytes / capacity`. |
| `ringloom_ring_producer_position` | gauge | Raw producer position. |
| `ringloom_ring_consumer_position` | gauge | Raw consumer position. |
| `ringloom_ring_consumer_heartbeat_age_seconds` | gauge | Age of ring consumer heartbeat if present. |

Rings:

| Owner | Rings |
|---|---|
| Broker | `control`, `send` |
| Service | `control`, `messages` |

### Flow-control metrics

For every allocated flow-control slot in a broker metadata file:

| Metric | Type | Description |
|---|---|---|
| `ringloom_flow_control_remaining_bytes` | gauge | Last known target service remaining capacity. |
| `ringloom_flow_control_capacity_bytes` | gauge | Target service message ring capacity. |
| `ringloom_flow_control_usage_ratio` | gauge | `1 - remaining_bytes / capacity` when capacity is non-zero. |
| `ringloom_flow_control_pressure_state` | gauge | 0 unknown, 1 normal, 2 pressured. |
| `ringloom_flow_control_update_age_seconds` | gauge | Age of last update. |

Labels include `source_node_id` for the broker owning the file plus target `node_id`,
`service_id`, and `slot`.

### Per-peer send metrics

For every active per-peer send-counter entry:

| Metric | Type | Description |
|---|---|---|
| `ringloom_broker_peer_connected` | gauge | 1 connected, 0 disconnected. |
| `ringloom_broker_peer_ring_pending_bytes` | gauge | Bytes in broker send ring destined for the peer. |
| `ringloom_broker_peer_queue_pending_bytes` | gauge | Bytes in the peer write queue. |
| `ringloom_broker_peer_queue_capacity_bytes` | gauge | Write queue byte capacity. |
| `ringloom_broker_peer_bytes_sent_total` | counter | Lifetime bytes sent to peer. |
| `ringloom_broker_peer_bytes_dropped_total` | counter | Lifetime bytes dropped for peer. |
| `ringloom_broker_peer_counter_update_age_seconds` | gauge | Age of last counter update. |

Labels include `node_id` for the local broker and `peer_node_id` for the peer.

### Exporter self-metrics

The exporter should expose its own health:

| Metric | Type | Description |
|---|---|---|
| `ringloom_observability_scrapes_total` | counter | `/metrics` requests served. |
| `ringloom_observability_scrape_errors_total` | counter | Scrapes that failed before response generation. |
| `ringloom_observability_metadata_files` | gauge | Number of files in the current catalog. |
| `ringloom_observability_metadata_scan_errors_total` | counter | Directory scan/open/stat/mmap errors. |
| `ringloom_observability_invalid_metadata_files_total` | counter | Files rejected by validation. |
| `ringloom_observability_invalid_regions_total` | counter | Individual optional regions rejected by validation. |
| `ringloom_observability_last_scan_timestamp_seconds` | gauge | Unix time of last completed scan. |
| `ringloom_observability_render_truncated_total` | counter | Responses truncated or rejected by `--max-response-bytes`. |

---

## HTTP server design

Use Zig's standard-library HTTP server. The implementation should follow the project's
active Zig version APIs; for Zig 0.16.x this means:

1. Listen with `std.net.Address.listen`.
2. Accept TCP connections.
3. Create explicit read and write buffers for the stream.
4. Initialize `std.http.Server` from the stream reader/writer interfaces.
5. Call `receiveHead()`.
6. Respond with text using `request.respond` or stream with `request.respondStreaming`.

Response headers for `/metrics`:

```text
Content-Type: text/plain; version=0.0.4; charset=utf-8
Cache-Control: no-cache
```

For large deployments, prefer streaming response generation so a single scrape does not
require one large allocation. If an allocating buffer is used initially, enforce
`--max-response-bytes`.

---

## Prometheus text formatting

Renderer rules:

1. Emit `# HELP` and `# TYPE` once per direct metric name.
2. Sanitize counter labels into Prometheus label values by escaping `\`, `"`, and newlines.
3. Sanitize dynamic counter names:
   - lowercase ASCII letters and digits are preserved,
   - `-`, `.`, and space become `_`,
   - other bytes become `_`,
   - if the name starts with a digit, prefix `_`.
4. Treat metadata counter labels ending in `_total` as counters; otherwise use gauge for
   direct metrics unless the label is in the known counter list.
5. Do not emit negative values for counters. If a metadata counter is negative, emit it
   only through `ringloom_counter` and increment an exporter invalid-region/self counter.
6. Emit timestamps only if Prometheus scrape-time timestamps become necessary; default to
   no explicit sample timestamp.

Example output:

```text
# HELP ringloom_broker_bytes_sent_total Total bytes sent by a broker over TCP.
# TYPE ringloom_broker_bytes_sent_total counter
ringloom_broker_bytes_sent_total{group="default",node_id="1"} 1048576

# HELP ringloom_ring_usage_ratio Ring occupancy ratio.
# TYPE ringloom_ring_usage_ratio gauge
ringloom_ring_usage_ratio{group="default",owner_type="service",node_id="1",service_id="42",service_name="orders",ring="messages"} 0.37

# HELP ringloom_counter Generic metadata counter by stored counter label.
# TYPE ringloom_counter untyped
ringloom_counter{group="default",owner_type="service",node_id="1",service_id="42",service_name="orders",counter="orders_generated_total"} 10000
```

---

## Implementation modules

Suggested files:

```text
tools/ringloom_observability.zig
src/common/monitoring/metadata_reader.zig
src/common/monitoring/prometheus.zig
src/common/monitoring/metric_name.zig
```

`metadata_reader.zig` should be reusable by `ringloom-stat`:

| Type/function | Purpose |
|---|---|
| `MetadataKind` | `broker` or `service`. |
| `MetadataIdentity` | Parsed owner labels. |
| `RegionDescriptor` | Validated offset/length pair. |
| `MappedMetadata` | Owns file descriptor and read-only mmap. |
| `MetadataCatalog` | Current set of mapped files. |
| `openMetadataFile` | Open, mmap, parse, and validate one file. |
| `deriveRingStats` | Read ring trailer positions and capacity. |
| `readCounters` | Iterate allocated counter metadata/value slots. |
| `readErrorLog` | Iterate error log entries for `ringloom-stat`; exporter should not emit descriptions as labels. |

`prometheus.zig` should own text formatting and escaping. It should not know how to open
metadata files.

---

## Testing

Unit tests:

1. Metric-name sanitization.
2. Label-value escaping.
3. Ring-stat derivation from synthetic ring trailer positions.
4. Counter-region validation rejects out-of-bounds offsets and mismatched lengths.
5. Prometheus renderer emits valid HELP/TYPE/sample lines.
6. Legacy metadata without monitoring tail still emits liveness and ring gauges.

Integration tests:

1. Start a broker and one test service, send messages, scrape `/metrics`, and assert broker
   and service counters are present.
2. Fill a service messages ring or broker send ring enough to trigger a `BufferFull`
   counter, scrape, and assert the corresponding `_total` increased.
3. Enable flow control, scrape, and assert flow-control slot and per-peer metrics are
   present.
4. Delete or replace a metadata file between scans and assert the exporter keeps serving
   and updates `ringloom_observability_metadata_files`.
5. Run `zig build stat` and the exporter metadata reader against the same fixture to ensure
   shared parsing stays compatible.

---

## Rollout order

1. Move broker generic counters and error log into broker metadata while preserving current
   event-loop counter updates.
2. Add service metadata counter/error-log regions and service runtime counter updates.
3. Update `ringloom-stat` to prove metadata counters are externally readable.
4. Add shared metadata reader helpers.
5. Implement `ringloom-observability` and `/metrics`.
6. Add e2e scrape coverage.
