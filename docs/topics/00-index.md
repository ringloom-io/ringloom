# Persistent Topics — Implementation Specs

Task-level contracts for implementing [persistent topics](../topics-architecture.md). Each spec is
self-contained enough to implement and test in isolation, with explicit dependencies.

## Locked design decisions

per-broker `topics.enabled` opt-in · **separate single topic-leader** elected among topics-enabled
brokers (decoupled from cluster master) sequences all topics · AP + epoch fencing + failover
**catch-up barrier** · **full-mesh replication** (every topics-enabled broker replicates every topic) ·
ack modes **`fire_and_forget` (default)** / **`replicate_once`** (≥1 replica; single-node → leader
append) with **throttled HWM ack feedback** · broadcast consumption · `topic_id = hash(name)` ·
subscriber-chosen start position · first-creation-wins immutable config.

## Task map

| # | Spec | Depends on | Module(s) |
|---|---|---|---|
| 01 | [Topic identity & config](01-topic-identity-and-config.md) | — | `topics/topic_id.zig`, `topic_config.zig` |
| 02 | [Registry & metadata propagation](02-registry-and-metadata-propagation.md) | 01 | `topics/topic_registry.zig`, `topic_admin.zig` |
| 03 | [Control-plane protocol (service↔broker)](03-control-plane-protocol.md) | 01,02 | `topics/topic_messages.zig`, control_loop |
| 04 | [Wire protocol & routing (Aeron)](04-wire-protocol-and-routing.md) | 01 | `common/message/topic_data_header.zig` |
| 05 | [Leader sequencing & append](05-leader-sequencing-and-append.md) | 02,04 | `topics/topic_engine.zig`, `topic_store.zig`, `topic_prefetcher.zig` |
| 06 | [Replication over Aeron](06-replication-over-aeron.md) | 04,05 | `topics/repl_aeron_transport.zig`, `repl_session.zig` |
| 07 | [Replica lifecycle & subscription](07-replica-lifecycle-and-subscription.md) | 03,06 | control_loop, topic_engine |
| 08 | [Topic leadership, failover & epoch fencing](08-failover-and-epoch-fencing.md) | 05,06 | `topics/topic_leader_election.zig`, topic_engine |
| 09 | [Service client API & tailer](09-service-client-api-and-tailer.md) | 03 | `service/topics/*`, `c_abi.zig` |
| 10 | [Observability & metrics](10-observability-and-metrics.md) | 05,06 | counters, ringloom-stat |
| 11 | [Testing plan](11-testing-plan.md) | all | unit, e2e, perf |
| 12 | [Java topic bindings](12-java-topic-bindings.md) | 09 | `bindings/java` (FFM wrappers of the C ABI) |

## Conventions reminder (see repo Copilot instructions)

- Zig 0.16.x; extern structs for all wire formats; cache-line padding on shared atomics; power-of-two
  ring capacities; no hot-path allocation; SPDX header on every new file (`// SPDX-License-Identifier: Apache-2.0`).
- ringloom-queue is consumed via `@import("ringloom_queue")` (`repl`, `Queue`, `Appender`, `Tailer`,
  `RollScheme`); its replication contract is in that repo's `docs/12-replication.md`.
