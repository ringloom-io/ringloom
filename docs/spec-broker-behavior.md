# RingLoom Broker Behavioral Specification

**Version:** 1.0
**Date:** 2026-03-14
**Status:** Draft
**Applies to:** RingLoom v0.1.0

---

## Table of Contents

1. [Broker Identity & Bootstrap](#1-broker-identity--bootstrap)
2. [Aeron Channel Topology](#2-aeron-channel-topology)
3. [Service Registration Protocol](#3-service-registration-protocol)
4. [Service Discovery Protocol](#4-service-discovery-protocol)
5. [Heartbeat Protocol](#5-heartbeat-protocol)
6. [Message Routing](#6-message-routing)
7. [Broker Leader Election (Bully Algorithm)](#7-broker-leader-election-bully-algorithm)
8. [Service Leader Election](#8-service-leader-election)
9. [Cluster State Synchronization](#9-cluster-state-synchronization)

---

## 1. Broker Identity & Bootstrap

### 1.1 Fixed Identity

- **Service ID:** Always `0`.
- **Service Name:** Always `"broker"`.
- These values are hardcoded and must not be configurable.

### 1.2 Bootstrap Sequence

1. **Load configuration:** Read `broker.properties` (or the file specified by `broker.properties.file` system property). Parse `broker.node.id`, `broker.local.host.port`, `broker.member.host.ports`, stream IDs, and encryption settings.

2. **Create metadata file:** Write the broker's metadata file at:
   ```
   <storage_path>/<broker_group_name>/services/broker_0.dat
   ```
   Or when at-rest encryption is enabled:
   ```
   <storage_path>/<broker_group_name>/services/0.dat
   ```
   Populate the metadata header with:
   - `controlBufferLength`: Computed from configured control buffer size + blocking trailer.
   - `messagesBufferLength`: Computed from configured messages buffer size + blocking trailer.
   - `serviceId`: `0`
   - `nodeId`: From `broker.node.id`. This value is stored as `int16` in the metadata file (2 bytes, little-endian) but transmitted as `uint8` in `ringloomMessageHeader` fields. Values fit within 0–255; cast to `uint8` when comparing with RingLoom message headers.
   - `pid`: Current OS process ID.
   - `startTimestampMs`: Current epoch milliseconds.

3. **Initialize ring buffers:** Create the control and messages ring buffers from the metadata file's mapped memory regions. Control buffer receives service registration and heartbeat messages. Messages buffer receives cross-host messages to route.

4. **Start Aeron transport** (if multi-node cluster):
   - Start embedded media driver (if `broker.media.driver.enabled = true`).
   - Create Aeron subscriptions and publications (see Section 2).

5. **Start agent threads:**
   - **Broker agent thread** (`BrokerAgent`): Runs scheduler tasks (heartbeat checking, admin subscriber polling) and control message processing.
   - **Routing agent thread** (`MessageRoutingAgent`): Handles Aeron message routing subscriber (inbound from remote brokers) and message routing consumer (outbound to remote brokers).

6. **Initiate election** (if multi-node cluster): Begin broker leader election protocol (see Section 7).

7. **Single-node cluster:** If `broker.member.host.ports` is empty, auto-elect self as leader. Skip Aeron transport initialization.

### 1.3 Shutdown Sequence

1. Close agent runners (stops both broker-agent and routing-agent threads).
2. Close Aeron transport (publications, subscriptions, media driver).
3. Unmap and clean up the metadata file.

---

## 2. Aeron Channel Topology

Aeron provides reliable UDP transport for cross-host communication between brokers. Each broker uses two Aeron stream types.

### 2.1 Stream Types

| Stream | Default Stream ID | Header Type | Purpose |
|--------|-------------------|-------------|---------|
| Admin | `100` (`broker.admin.stream.id`) | `brokerMessageHeader` (8 bytes) | Broker-to-broker cluster messages: election, heartbeat, state sync. |
| Message | `101` (`broker.message.stream.id`) | `ringloomMessageHeader` (19 bytes) | Cross-host service message routing. |

### 2.2 Channel Format

All Aeron channels use unicast UDP:

```
aeron:udp?endpoint=<host>:<port>
```

Example: `aeron:udp?endpoint=192.168.1.10:40456`

### 2.3 Per-Broker Resources

Each broker creates the following Aeron resources:

| Resource | Count | Description |
|----------|-------|-------------|
| **Subscription** (admin) | 1 | Listens on own `host:port`, admin stream ID. Receives cluster messages from all peers. |
| **Subscription** (message) | 1 | Listens on own `host:port`, message stream ID. Receives routed service messages from all peers. |
| **ExclusivePublication** (admin) | 1 per peer | One publication per peer broker for sending cluster messages. Channel = peer's `host:port`, admin stream ID. |
| **ExclusivePublication** (message) | 1 per peer | One publication per peer broker for sending routed service messages. Channel = peer's `host:port`, message stream ID. |

**ExclusivePublication** is used (not `Publication`) because each publication has a single writer thread, avoiding CAS overhead.

### 2.4 Media Driver

- **Directory:** `<ringloom_directory>/aeron/` (where `ringloom_directory = <storage_path>/<broker_group_name>`).
- **Embedded mode** (`broker.media.driver.enabled = true`): The broker starts its own `MediaDriver` instance.
- **External mode** (`broker.media.driver.enabled = false`): The broker connects to a pre-existing media driver at the configured directory.

### 2.5 Connection Detection

Aeron provides image lifecycle callbacks:
- **Image available:** New peer broker connected. Triggers election (see Section 7).
- **Image unavailable:** Peer broker disconnected. Triggers election (see Section 7).

---

## 3. Service Registration Protocol

### 3.1 Overview

Registration establishes the communication channel between a service and the broker. The service creates its own metadata file, then sends a registration request to the broker via the broker's control ring buffer.

### 3.2 Service-Side Steps

1. **Scan for existing metadata files:** Check `<storage_path>/<broker_group_name>/services/` for a file matching the service name with a dead process PID. If found, reuse that file (inheriting its service ID).

2. **Create metadata file** (if no reusable file found):
   - Allocate the next service ID from the broker's metadata file (`NEXT_SERVICE_ID_OFFSET`, atomic increment).
   - Create the file at `<storage_path>/<broker_group_name>/services/<name>_<id>.dat`.
   - Populate the metadata header (buffer sizes, blocking flags, timeouts, service ID, PID, timestamp).
   - If at-rest encryption is enabled, write the encrypted service-name marker.

3. **Open broker's metadata file:** Map the broker's metadata file (`broker_0.dat` or `0.dat`) to access the broker's control ring buffer.

4. **Send `RegisterService` message:** Write to the broker's control ring buffer:
   - `templateId = 1`
   - `serviceId` = assigned service ID
   - `serviceName` = service name (32-byte ASCII, zero-padded)
   - `leaderElectionEnabled` = from service configuration

5. **Wait for `RegistrationResponse`:** Poll the service's own control ring buffer for the broker's response.

6. **Process response:**
   - Store the confirmed `serviceId` and `nodeId`.
   - If `isLeader = T`, mark self as leader.

7. **Start heartbeat:** Begin updating the heartbeat timestamp in the metadata file every ~1 second.

### 3.3 Broker-Side Steps

1. **Poll control ring buffer:** `ControlMessageProcessor` reads the broker's control ring buffer and dispatches messages by `templateId`.

2. **Handle `RegisterService` (templateId = 1):**
   - Parse `serviceId`, `serviceName`, `leaderElectionEnabled`.
   - Register the service in the broker's `ServiceRegistry` (keyed by `serviceId` + `nodeId`).
   - Open the service's metadata file and create a `BuffersProvider` for the service's message ring buffer (used for sending messages to the service).

3. **Send `RegistrationResponse` (templateId = 2):**
   - Write to the service's control ring buffer.
   - `serviceId` = confirmed service ID.
   - `nodeId` = this broker's node ID.
   - `isLeader` = result of leader election (if applicable).

4. **Evaluate leader** (if `leaderElectionEnabled`):
   - If this broker is the cluster leader, invoke `ServiceLeaderElectionManager` (see Section 8).
   - If leader changed, broadcast `LeaderChanged` to all local instances.

5. **Notify peer brokers:**
   - Broadcast `ServiceAdded` (templateId = 6) via the admin stream to all peer brokers.
   - This message includes `nodeId`, `serviceId`, `serviceName`, and `leaderElectionEnabled`.

6. **Notify local subscribers:**
   - For all services that have subscribed to this service name (see Section 4), send an updated `ServiceInstances` list to their control ring buffers.

---

## 4. Service Discovery Protocol

### 4.1 Subscription

1. **Service sends `SubscribeToServiceUpdates` (templateId = 3):**
   - Written to the broker's control ring buffer.
   - Contains `localServiceId` (subscriber) and `remoteServiceName` (the service to watch).

2. **Broker registers subscription:**
   - The `ServiceSubscriptionRegistry` maps `serviceName → Set<subscriberServiceId>`.

3. **Immediate response:**
   - If any instances of `remoteServiceName` are already registered (local or remote), the broker immediately sends a `ServiceInstances` (templateId = 4) message to the subscriber's control ring buffer.

### 4.2 Instance Updates

On any change to the instance set for a service name (registration, removal, cluster state update):

1. Broker sends `ServiceInstances` (templateId = 4) to **all** locally subscribed services for that service name.
2. The `ServiceInstances` message contains the **complete** current set of instances (not a delta).
3. Each instance entry includes `serviceId` and `nodeId`.

### 4.3 Service-Side Instance Processing

When a service receives `ServiceInstances`:

1. For each instance where `nodeId == localNodeId`:
   - Create an `IpcProducer` that writes directly to the target service's messages ring buffer (same-host, zero-copy path).

2. For each instance where `nodeId != localNodeId`:
   - Create a producer that writes to the local broker's messages ring buffer with the remote `targetNodeId` and `targetServiceId` set in the RingLoom header.
   - The broker handles forwarding to the remote node.

3. Remove any previously tracked instances that are not in the new list.

---

## 5. Heartbeat Protocol

### 5.1 Service-Side Heartbeat

- **Mechanism:** The service writes the current epoch milliseconds to the `HEARTBEAT_TIME_OFFSET` (byte 256) in its own metadata file.
- **Frequency:** Every ~1 second.
- **Access pattern:** Volatile store of an int64 value.
- **No ring buffer involved:** This is a direct memory-mapped file write, not a ring buffer message.

### 5.2 Broker-Side Health Checking

- **Component:** `ServiceHeartbeatChecker`
- **Frequency:** Every 3 seconds.
- **Algorithm:**
  ```
  for each locally registered service:
      lastHeartbeat = volatile_read(service.metadataFile[HEARTBEAT_TIME_OFFSET])
      elapsed = currentTimeMillis() - lastHeartbeat
      if elapsed > 10,000 ms:
          handleServiceRemoved(service)
  ```

### 5.3 Unhealthy Service Handling

When `handleServiceRemoved(service)` is triggered:

1. Unregister the service from `ServiceRegistry`.
2. Close the `BuffersProvider` for the service's metadata file.
3. Broadcast `ServiceRemoved` (templateId = 7) to all peer brokers via the admin stream.
4. Send updated `ServiceInstances` (empty or reduced list) to all local subscribers of this service name.
5. Re-evaluate service leader election if the removed service was a leader or leader election was enabled (see Section 8).

### 5.4 Broker-to-Broker Heartbeat

Brokers send `BrokerHeartbeat` messages to peer brokers via the admin Aeron stream (stream ID `100`) to detect cluster-member failures.

- **Message:** `BrokerHeartbeat` (broker `templateId = 4`, see spec-wire-protocol.md Section 3 for broker message schemas).
- **Frequency:** Sent periodically by the broker admin scheduler, piggy-backed onto the scheduler's normal duty cycle.
- **Detection:** Failure to receive Aeron images (i.e., Aeron "image unavailable" event) is the primary partition-detection mechanism. The explicit `BrokerHeartbeat` message serves as a liveness signal used in leader election logic (see Section 7).
- **Handling:** Received `BrokerHeartbeat` messages update the per-peer liveness state in `NodeMembership`.

### 5.5 Timeout Configuration

| Parameter | Value | Description |
|-----------|-------|-------------|
| Heartbeat write interval | ~1 second | Service writes current time to metadata file. |
| Health check interval | 3 seconds | Broker reads heartbeat timestamps. |
| Heartbeat timeout | 10 seconds | If `elapsed > 10s`, the service is considered dead. |

These values are currently hardcoded in the reference implementation. Implementors should use the same defaults for compatibility, though future RingLoom versions may make these configurable.

---

## 6. Message Routing

### 6.1 Same-Host (IPC) Path

When `targetNodeId == sourceNodeId` (both services on the same broker):

```
┌──────────┐                    ┌──────────┐
│ Service A │───IpcProducer────►│ Service B │
│           │  (direct ring     │           │
│           │   buffer write)   │           │
└──────────┘                    └──────────┘
```

1. Service A's `ServiceClient` picks Service B as the target (via load balancer).
2. Service A writes **directly** to Service B's messages ring buffer via `IpcProducer`.
3. The broker is NOT involved in same-host message delivery.
4. Service B's `MessageConsumerAgent` picks up the message on its next poll.

### 6.2 Cross-Host Path

When `targetNodeId != sourceNodeId` (services on different brokers):

```
┌──────────┐         ┌──────────┐         ┌──────────┐         ┌──────────┐
│ Service A │──IPC──►│ Broker 1  │──Aeron──►│ Broker 2  │──IPC──►│ Service B │
│ (Node 1)  │        │ (Node 1)  │  (UDP)   │ (Node 2)  │        │ (Node 2)  │
└──────────┘         └──────────┘         └──────────┘         └──────────┘
```

**Sending side (Broker 1):**

1. Service A writes to Broker 1's **messages ring buffer** with `targetNodeId = 2` and `targetServiceId = B's ID`.
2. `MessageRoutingConsumer` polls Broker 1's messages ring buffer.
3. For each message, parse the `targetNodeId` from the RingLoom header.
4. Look up the `AeronProducer` (ExclusivePublication) for the target node.
5. `MessageRoutingPublisher` forwards the message via the Aeron message stream.
6. If transport encryption is enabled, encrypt the payload before sending (RingLoom header remains plaintext).

**Receiving side (Broker 2):**

1. `MessageRoutingSubscriber` polls the Aeron message stream subscription.
2. For each received message, parse the `targetServiceId` from the RingLoom header.
3. If transport encryption is enabled, decrypt the payload.
4. Look up the target service's `BuffersProvider` (messages ring buffer) via `targetServiceId`.
5. Write the decrypted message to the target service's messages ring buffer.
6. Service B's `MessageConsumerAgent` picks up the message on its next poll.

### 6.3 Routing Decision

The routing decision is made solely by the `targetNodeId` field in the RingLoom header:

| Condition | Action |
|-----------|--------|
| `targetNodeId == localNodeId` | Should not reach broker's message ring buffer (direct IPC). However, if it does, the broker delivers locally. |
| `targetNodeId != localNodeId` | Forward via Aeron to the target broker. |

### 6.4 Message Ordering

- **Same-host (IPC):** Messages from a single producer to a single consumer are delivered in FIFO order (ring buffer guarantees).
- **Cross-host (Aeron):** Messages are delivered in the order they are offered to the Aeron publication. Aeron provides reliable, ordered delivery per stream.
- **Multi-producer:** When multiple services send to the same target, ordering between different senders is determined by the ring buffer CAS ordering (not guaranteed to match wall-clock time).

---

## 7. Broker Leader Election (Bully Algorithm)

The broker cluster uses a Bully algorithm variant to elect a cluster leader. The broker with the **lowest `nodeId`** wins.

### 7.1 Election Triggers

An election is initiated when:
- An Aeron **image becomes available** (new peer broker connected).
- An Aeron **image becomes unavailable** (existing peer broker disconnected).

### 7.2 Protocol

```
┌─────────────┐         ┌─────────────┐         ┌─────────────┐
│   Broker A   │         │   Broker B   │         │   Broker C   │
│   nodeId=1   │         │   nodeId=2   │         │   nodeId=3   │
└──────┬───────┘         └──────┬───────┘         └──────┬───────┘
       │                        │                        │
       │  InitiateElection(1)   │                        │
       ├───────────────────────►├───────────────────────►│
       │                        │                        │
       │                        │  InitiateElection(2)   │
       │◄───────────────────────┼───────────────────────►│
       │                        │                        │
       │                        │                        │  InitiateElection(3)
       │◄───────────────────────┼◄───────────────────────┤
       │                        │                        │
       │  NodeAcknowledgment(1) │                        │
       ├───────────────────────►│                        │
       │  NodeAcknowledgment(1) │                        │
       ├────────────────────────┼───────────────────────►│
       │  (I have lower ID)     │                        │
       │                        │  NodeAcknowledgment(2) │
       │                        ├───────────────────────►│
       │                        │                        │
       │  LeaderAnnouncement(1) │                        │
       ├───────────────────────►├───────────────────────►│
       │                        │                        │
```

**Step-by-step:**

1. **Trigger detected:** Any broker can initiate.
2. **Broadcast `InitiateElection(myNodeId, myHostPort)`** (templateId = 1) to all peers via the admin stream.
3. **On receiving `InitiateElection(senderNodeId)`:**
   - Register the sender as a known cluster member.
   - If `myNodeId < senderNodeId`: Send `NodeAcknowledgment(myNodeId)` (templateId = 2) — "I have priority."
   - If `myNodeId > senderNodeId`: Do not send acknowledgment (the sender has priority).
4. **Election window:** Wait for a brief period for acknowledgments.
5. **Decide winner:**
   - If no `NodeAcknowledgment` received with a lower `nodeId`, and no `InitiateElection` received from a lower `nodeId`: I am the leader.
   - Broadcast `LeaderAnnouncement(myNodeId)` (templateId = 3) to all peers.
6. **On receiving `LeaderAnnouncement(leaderNodeId)`:** Accept the sender as the cluster leader.
7. **Post-election:** Leader sends `ClusterStateSnapshot` (templateId = 5) to all peers.

### 7.3 Single-Node Cluster

If `broker.member.host.ports` is empty (no peers), the broker automatically elects itself as leader. No election messages are sent.

### 7.4 Election Invariants

- The broker with the **lowest `nodeId`** always wins.
- Election is triggered by Aeron connection lifecycle events, not by timers.
- Each broker maintains a `NodeMembership` map (`nodeId → Node`) tracking all known peers and their leader status.
- The leader status is used to gate certain operations (e.g., service leader designation, state broadcasts).

---

## 8. Service Leader Election

Service leader election is an optional per-service feature that designates one instance of a service as the "leader" across the entire cluster.

### 8.1 Opt-In

Leader election is enabled per-service via:
- Service configuration: `ringloom.service.leader_election.enabled = true`
- Registration message: `leaderElectionEnabled = T` (BooleanType)

If not enabled, all instances are equal (no leader concept).

### 8.2 Election Rule

**Lowest `serviceId` wins.** Among all registered instances of a service name across the entire cluster, the instance with the lowest `serviceId` is designated as leader.

This is effectively a "first-registered-wins" policy because service IDs are monotonically incremented.

### 8.3 Managed by Cluster Leader Only

Service leader designation is exclusively performed by the **broker cluster leader**. Non-leader brokers track service state but do not make leader designation decisions.

This prevents split-brain scenarios where multiple brokers might designate different leaders.

### 8.4 Leader Evaluation Triggers

| Event | Action |
|-------|--------|
| **Service registered** with `leaderElectionEnabled = T` | Re-evaluate leader for that service name. |
| **Service removed** that was leader (or had leader election enabled) | Re-evaluate leader for that service name. |
| **Broker leadership change** | New broker leader calls `reEvaluateAllLeaders()` for all `leaderElectionEnabled` services. |
| **Cluster state received** from peer | Update service registry, then re-evaluate affected services. |

### 8.5 Leader Designation Flow

When the broker cluster leader evaluates a service leader:

1. Collect all registered instances of the service name (local and remote tracked via cluster state).
2. Find the instance with the lowest `serviceId`.
3. If the leader changed (or no leader was previously set):
   a. **Broadcast `LeaderChanged` (templateId = 6)** to all **local** instances of that service name via their control ring buffers.
   b. **Broadcast `ServiceLeaderDesignated` (templateId = 8)** to all **peer brokers** via the admin stream. Peers then forward `LeaderChanged` to their local instances.

### 8.6 Service-Side Leader Handling

When a service receives `LeaderChanged` (templateId = 6):

1. Parse `leaderServiceId`, `serviceName`, and `leaderNodeId`.
2. Update the local `ServiceClient` for that service name:
   - If `leaderServiceId == myServiceId && leaderNodeId == myNodeId`: Mark self as leader.
   - Otherwise: Mark the designated instance as leader in the local service registry.
3. Applications can register event handlers to react to leader changes.

### 8.7 Split-Brain Recovery

When the cluster leader changes (e.g., old leader went down, new leader elected):

1. The new broker leader calls `reEvaluateAllLeaders()`.
2. This iterates all services with `leaderElectionEnabled = T`.
3. For each, re-evaluates based on the currently known global state.
4. If a leader was previously designated but the old leader service is now gone, a new leader is designated.
5. Full re-evaluation ensures consistency across the cluster.

---

## 9. Cluster State Synchronization

Broker cluster state synchronization ensures that all brokers have a consistent view of which services exist across the cluster.

### 9.1 State Propagation Events

| Event | Message | Direction |
|-------|---------|-----------|
| New broker joined | `ClusterStateSnapshot` (templateId = 5) | Leader → new peer, and new peer → leader |
| Service registered locally | `ServiceAdded` (templateId = 6) | Local broker → all peers |
| Service removed locally | `ServiceRemoved` (templateId = 7) | Local broker → all peers |
| New service leader designated | `ServiceLeaderDesignated` (templateId = 8) | Cluster leader → all peers |

### 9.2 ClusterStateSnapshot

Sent after an election settles or when a new broker joins:

1. Each broker sends its full list of locally registered services.
2. The snapshot includes `nodeId`, and for each service: `serviceId`, `serviceName`, `leaderElectionEnabled`.
3. Receiving brokers merge the snapshot into their `ServiceRegistry`.
4. Notify local subscribers of any updated service instance lists.

### 9.3 Incremental Updates

After the initial snapshot, updates are sent incrementally:

- `ServiceAdded`: When a service registers on any broker, that broker broadcasts to peers. Receiving brokers add the instance to their registry and notify affected subscribers.
- `ServiceRemoved`: When a service is removed (heartbeat timeout, graceful shutdown), the broker broadcasts to peers. Receiving brokers remove the instance and notify subscribers.

### 9.4 ServiceLeaderDesignated

When the cluster leader designates a new service leader:

1. Cluster leader broadcasts `ServiceLeaderDesignated` to all peers.
2. Receiving broker:
   a. Parses `nodeId`, `serviceId`, `serviceName`.
   b. Updates the leader record in its service registry.
   c. Sends `LeaderChanged` to all local instances of that service name.

### 9.5 Consistency Model

- **Eventual consistency:** Cluster state is eventually consistent. Small windows of inconsistency exist during propagation.
- **No total ordering:** Messages from different brokers may arrive in different orders at different peers. The state converges because updates are idempotent (add/remove by unique key).
- **Clean recovery:** After any disruption (broker crash, network partition healing), the election + state snapshot mechanism restores consistency.

### 9.6 State Tracking Data Structures

| Broker Structure | Type | Key | Value |
|-----------------|------|-----|-------|
| `ServiceRegistry` | `BiInt2ObjectMap` | `(serviceId, nodeId)` | `ServiceInstance` |
| `ServiceSubscriptionRegistry` | Map | `serviceName` | `Set<subscriberServiceId>` |
| `NodeMembership` | `Int2ObjectHashMap` | `nodeId` | `Node (id, hostAndPort, local, leader)` |
| `MessageRoutingProducerRegistry` | Map | `nodeId` | `AeronProducer` |
