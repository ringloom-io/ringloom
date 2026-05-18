# RingLoom Observability

RingLoom observability is out-of-process. Brokers and services update shared-memory
metadata counters, ring-buffer positions, heartbeats, and error logs; tools map those
files read-only and render human or Prometheus output.

For the architecture behind these tools, see
[`components/observability.md`](components/observability.md).

## Tools

| Tool | Command | Purpose |
|---|---|---|
| `ringloom-stat` | `zig build stat` | One-shot CLI for broker/service metadata and Aeron state. |
| Prometheus exporter | `zig build observability` | Builds `ringloom-observability`. |
| Exporter runner | `zig build run-observability -- --storage-path /dev/shm --group ringloom --listen 127.0.0.1:9464` | Serves `/metrics`, `/healthz`, and `/readyz`. |

## Metadata discovery

The exporter scans:

```text
<storage_path>/<group>/services/
```

Expected files:

```text
broker_<node_id>.dat
<service_name>_node<node_id>_<service_id>.dat
```

For each file, tooling opens read-only, validates the fixed header, validates every
region before reading offsets, maps the file with `MAP_SHARED`, and samples atomics
with acquire semantics where required.

Stale files are not fatal. Tools report liveness and heartbeat age so operators can
distinguish a dead owner from an unreadable metadata file.

## Prometheus endpoints

| Path | Response |
|---|---|
| `/metrics` | Prometheus text exposition. |
| `/healthz` | `200 OK` when the HTTP server is running. |
| `/readyz` | `200 OK` after the first successful metadata scan, otherwise `503`. |

Suggested flags:

| Flag | Default | Description |
|---|---|---|
| `--storage-path PATH` | `/dev/shm` | Base metadata path. |
| `--group GROUP` | `default` | RingLoom group directory. |
| `--listen HOST:PORT` | `127.0.0.1:9464` | HTTP listen address. |
| `--refresh-ms N` | `1000` | Metadata directory rescan interval. |
| `--broker-node-id N` | unset | Optional broker filter. |
| `--service-name NAME` | unset | Optional repeatable service-name filter. |
| `--include-stale` | `true` | Include dead or stale metadata files. |
| `--max-files N` | `4096` | Safety cap for metadata files scanned per refresh. |
| `--max-response-bytes N` | `8388608` | Safety cap for `/metrics` response body. |

## Metric labels

Common labels:

| Label | Applies to | Description |
|---|---|---|
| `group` | all RingLoom metrics | Metadata group. |
| `owner_type` | metadata metrics | `broker` or `service`. |
| `node_id` | owner metrics | Owning broker node. |
| `service_id` | service metrics | Service instance ID; broker uses `0`. |
| `service_name` | service metrics | Service name. |
| `ring` | ring gauges | `control` or `messages`. |
| `peer_node_id` | peer metrics | Remote broker node. |
| `stream_id` | Aeron metrics | Aeron stream ID. |
| `session_id` | Aeron metrics | Aeron session ID. |
| `channel_kind` | Aeron metrics | `ipc`, `udp_data`, `udp_admin`, or `unknown`. |
| `counter_id` | Aeron metrics | Aeron CnC counter slot ID. |

Do not add high-cardinality labels such as full file paths, arbitrary payload data,
or free-form error descriptions.

## Metric groups

### Metadata owner gauges

| Metric | Type | Description |
|---|---|---|
| `ringloom_metadata_file_up` | gauge | 1 when the file can be opened and parsed. |
| `ringloom_process_alive` | gauge | 1 when the PID appears alive. |
| `ringloom_heartbeat_age_seconds` | gauge | Age of latest owner heartbeat. |
| `ringloom_process_start_time_seconds` | gauge | Owner start timestamp. |
| `ringloom_metadata_version` | gauge | Metadata format version. |

### Ring gauges

| Metric | Type | Description |
|---|---|---|
| `ringloom_ring_capacity_bytes` | gauge | Ring data capacity excluding trailer. |
| `ringloom_ring_used_bytes` | gauge | Producer position minus consumer position, clamped for rendering. |
| `ringloom_ring_free_bytes` | gauge | Remaining capacity. |
| `ringloom_ring_usage_ratio` | gauge | Used/capacity ratio. |
| `ringloom_ring_producer_position` | gauge | Raw producer position. |
| `ringloom_ring_consumer_position` | gauge | Raw consumer position. |

Broker metadata exposes the control ring. Service metadata exposes control and
messages rings. The active architecture has no broker send ring metric.

### Runtime counters

Every allocated metadata counter can be exported generically:

```text
ringloom_counter{group="ringloom",owner_type="service",node_id="1",service_id="42",service_name="orders",counter="messages_received_total"} 123
```

Stable runtime counters may also be exported as direct names such as:

```text
ringloom_service_messages_received_total{group="ringloom",node_id="1",service_id="42",service_name="orders"} 123
ringloom_broker_receiver_invalid_frame_drops_total{group="ringloom",node_id="2"} 4
```

### Flow-control metrics

Flow-control metrics describe advisory target capacity and pressure state:

| Metric | Type | Description |
|---|---|---|
| `ringloom_flow_control_remaining_bytes` | gauge | Last known target remaining capacity. |
| `ringloom_flow_control_capacity_bytes` | gauge | Target service messages ring capacity. |
| `ringloom_flow_control_usage_ratio` | gauge | Derived usage ratio. |
| `ringloom_flow_control_pressure_state` | gauge | 0 unknown, 1 normal, 2 pressured. |
| `ringloom_flow_control_update_age_seconds` | gauge | Age of last update. |

### Aeron metrics

For broker metadata that advertises an Aeron directory, the exporter opens
`<aeron_directory>/cnc.dat` and emits:

| Metric | Type | Description |
|---|---|---|
| `ringloom_aeron_driver_up` | gauge | 1 when CnC is readable and driver appears alive. |
| `ringloom_aeron_driver_start_time_seconds` | gauge | Driver start timestamp. |
| `ringloom_aeron_cnc_version` | gauge | CnC semantic version integer. |
| `ringloom_aeron_client_liveness_timeout_seconds` | gauge | Driver client liveness timeout. |
| `ringloom_aeron_counter` | untyped | Generic Aeron CnC counter sample. |

Direct Aeron metric names cover important system and stream counters such as bytes
sent/received, NAKs, retransmits, publication positions/limits, subscription
positions, sender flow-control events, and Aeron error counts.

## Renderer rules

1. Emit `# HELP` and `# TYPE` once per direct metric name.
2. Escape `\`, `"`, and newlines in label values.
3. Sanitize dynamic counter names before using them in labels.
4. Treat known `_total` direct metrics as counters and other direct metrics as
   gauges unless explicitly classified.
5. Do not emit negative values for counters.
6. Enforce `--max-response-bytes` or stream the response.

## Testing

Observability coverage belongs in unit and e2e tests:

1. Metric-name and label escaping.
2. Metadata validation against synthetic files.
3. Ring-stat derivation.
4. Prometheus rendering.
5. Running broker/service scrape with RingLoom and Aeron metrics present.
6. Stale metadata files and missing CnC files produce clear status metrics.
