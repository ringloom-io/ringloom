# Aeron Runtime

RingLoom uses Aeron as the active cross-host transport. Each broker owns one
embedded Aeron media driver, and services connect to that driver as Aeron clients
for remote outbound publications.

## Responsibilities

| Layer | Responsibility |
|---|---|
| `src/aeron/` | Zig wrapper and C shim around Aeron client/media-driver APIs. |
| Broker runtime | Starts the embedded driver, owns the Aeron directory, and invokes driver agents. |
| Service runtime | Connects an Aeron client to the local broker-owned directory and creates peer publications. |
| Broker UDP transport | Owns broker data/admin subscriptions and broker-to-peer admin publications. |
| Observability tools | Read Aeron CnC counters and expose them with RingLoom metadata counters. |

## Broker-owned media driver

The broker derives or reads an Aeron directory during startup and writes it to broker
metadata so local services can discover it. The default derived shape is based on the
storage path, group, and node ID.

Relevant config fields include:

| Field | Purpose |
|---|---|
| `aeron_directory` | Explicit Aeron directory; empty means derive from storage/group/node. |
| `aeron_ipc_term_length` | Term length for IPC publications. |
| `aeron_udp_term_length` | Term length for UDP publications. |
| `aeron_ipc_mtu_length` | IPC MTU length. |
| `aeron_mtu_length` | UDP MTU length. |
| `aeron_sparse_files` | Whether term buffers are sparse files. |
| `aeron_delete_directory_on_start` | Remove stale directory before startup. |
| `aeron_delete_directory_on_shutdown` | Remove directory on clean shutdown. |
| `aeron_publication_linger_timeout_ns` | Publication linger timeout. |
| `aeron_client_liveness_timeout_ns` | Client liveness timeout. |
| `aeron_threading_mode` | Aeron agent assignment mode. |

## Threading modes

The wrapper exposes driver-agent invokers so RingLoom can drive Aeron from its own
event loops.

| Mode | RingLoom threads | Agent mapping |
|---|---|---|
| `dedicated` | control, sender, receiver | control invokes conductor; sender invokes sender; receiver invokes receiver. |
| `shared_network` | control, network | control invokes conductor; network invokes sender and receiver. |
| `shared` | one composite loop | one loop invokes all Aeron agents and RingLoom work. |

If a requested mode cannot be represented by the Aeron embedding API, broker startup
must fail clearly instead of silently changing threading semantics.

## Data and admin channels

Broker config defines local UDP endpoint information and peer endpoints. The broker
creates:

1. An inbound data subscription for frames targeted at its node.
2. An inbound admin subscription for broker heartbeats and cluster/discovery admin
   messages.
3. Peer admin publications for broker-originated admin traffic.
4. Peer data stream metadata that services use for direct remote publications.

Services lazily create exclusive publications for peer data channels when a remote
send first targets that node.

## Stream namespace

RingLoom keeps separate stream bases for categories:

| Stream category | Config base | Typical derivation |
|---|---|---|
| Broker ingress | `aeron_ingress_stream_base` | Reserved for broker-local ingress compatibility and diagnostics. |
| Admin UDP | `aeron_admin_stream_base` | `base + node_id`. |
| Data UDP | `aeron_data_stream_base` | `base + node_id`. |

Aeron session IDs are diagnostic only. RingLoom routes by the `RingLoomDataHeader`
inside the Aeron payload.

## Lifecycle

Broker startup order:

1. Parse and validate config.
2. Build or clean the Aeron directory.
3. Start the embedded driver.
4. Open a broker Aeron client.
5. Create broker UDP data/admin subscriptions and admin publications.
6. Create broker metadata with Aeron discovery fields.
7. Start control, sender, and receiver duty cycles.

Shutdown stops service-facing loops before closing publications, subscriptions,
clients, driver agents, and finally the embedded driver.

## Non-goals

RingLoom does not use Aeron Cluster or Aeron Archive in the active architecture.
Aeron provides transport mechanics; RingLoom owns routing, discovery, leadership,
metadata, and application semantics.
