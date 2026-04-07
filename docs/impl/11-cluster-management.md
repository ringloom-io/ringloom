# 11 — Cluster Management

> **Prerequisites:** [09 — Control Plane](09-control-plane.md) (service registration,
> heartbeats, control ring buffer protocol), [04 — TCP Transport Library](04-tcp-transport-library.md)
> (`brz_tcp` I/O engine, connection management, message framing), [10 — Threading Model](10-threading-model.md)
> (event loops, duty cycles, inter-loop command queues).
>
> **Depended on by:** [12 — Configuration & Monitoring](12-configuration-and-monitoring.md)
> (monitoring counters for cluster state).

This document describes the cluster management subsystem: how brokers discover each
other, establish peer connections, elect a cluster leader, synchronize service state,
and manage per-service leader designation. Every operation in this subsystem runs on one
of the two existing event loop threads (broker-agent or routing-agent) — no additional
threads are introduced.

The design simplifies the Java reference implementation's `cluster/` and `admin/`
packages (`ClusterManager`, `LeaderElection`, `NodeMembership`, `ClusterEventHandler`,
`ClusterStateManager`, `BrokerAdminPublisher`, `BrokerAdminSubscriber`,
`ServiceLeaderElectionManager`). Key changes from the Java reference:

1. **VRRP-style leader election** replaces the Bully algorithm — heartbeats and
   elections are unified into a single mechanism, eliminating three message types and
   the 5-second election window.
2. **Merged `Node` struct** — peer connection state is folded into `Node`, removing
   the parallel `PeerConnection` tracking structure.
3. **Return values instead of callbacks** — `LeaderElection` methods return results
   that the caller acts on, instead of invoking function-pointer callbacks.
4. **Snapshot on peer join only** — `ClusterStateSnapshot` is sent when a new peer
   connects, not on every leader change. Incremental updates handle the steady state.

As with the rest of the Zig rewrite, Aeron is replaced with the custom TCP transport
from doc 04 and SBE flyweights with `packed struct` overlays.

---

## Table of Contents

1. [Overview](#1-overview)
2. [Peer Connection Lifecycle](#2-peer-connection-lifecycle)
   1. [Connection Establishment](#21-connection-establishment)
   2. [Disconnection Detection](#22-disconnection-detection)
3. [Broker Leader Election (VRRP-style)](#3-broker-leader-election-vrrp-style)
   1. [Algorithm](#31-algorithm)
   2. [Why VRRP over Bully](#32-why-vrrp-over-bully)
   3. [LeaderElection Implementation](#33-leaderelection-implementation)
   4. [Election Timing](#34-election-timing)
4. [Admin Message Protocol](#4-admin-message-protocol)
   1. [Message Table](#41-message-table)
   2. [Admin Message Wire Format](#42-admin-message-wire-format)
5. [Cluster State Synchronization](#5-cluster-state-synchronization)
   1. [State Model](#51-state-model)
   2. [ClusterStateSnapshot](#52-clusterstatesnapshot)
   3. [Handling State Snapshot](#53-handling-state-snapshot)
   4. [Incremental Updates](#54-incremental-updates)
6. [Service Leader Election (Cluster-Wide)](#6-service-leader-election-cluster-wide)
   1. [Policy](#61-policy)
   2. [Triggers](#62-triggers)
   3. [ServiceLeaderElectionManager](#63-serviceleaderelectionmanager)
   4. [Handling ServiceLeaderDesignated](#64-handling-serviceleaderdesignated)
   5. [Split-Brain Recovery](#65-split-brain-recovery)
7. [Node Membership](#7-node-membership)
8. [Broker-to-Broker Heartbeat](#8-broker-to-broker-heartbeat)
9. [Admin Message Dispatch](#9-admin-message-dispatch)
10. [ClusterManager Facade](#10-clustermanager-facade)
11. [ClusterEventHandler](#11-clustereventhandler)
12. [Integration with Existing Event Loops](#12-integration-with-existing-event-loops)
13. [Testing](#13-testing)
14. [File Structure](#14-file-structure)

---

## 1. Overview

A BRZ cluster consists of N broker processes, each running on a separate host, each
identified by a unique `nodeId` (a `u8`, range 0–255). Brokers communicate over
TCP using the wire protocol from doc 04. Every broker maintains a full replica of the
cluster's service registry — which services are running on which nodes — and converges
to a consistent view through an eventually-consistent state synchronization protocol.

Cluster management handles five responsibilities:

| Responsibility | Mechanism | Owner |
|---|---|---|
| **Peer connection lifecycle** | SETUP → SM handshake, heartbeat liveness | Sender + receiver event loops |
| **Broker leader election** | VRRP-style master advertisement (lowest `nodeId` wins) | Broker-agent event loop |
| **Cluster state synchronization** | Snapshot on peer join + incremental updates | Broker-agent event loop |
| **Service leader election** | Lowest `serviceId` wins, managed by broker leader | Broker-agent event loop |
| **Admin message routing** | DATA frames with `ADMIN` flag on admin channel | Both event loops |

**Key invariant:** All cluster state mutations happen on the **broker-agent thread**.
The receiver event loop deserializes inbound admin messages and posts them to the
broker-agent via the inter-loop command queue (see doc 10). This eliminates the need
for any synchronization on cluster data structures.

**Single-node cluster shortcut:** If `broker.member.host.ports` is empty (no peers
configured), the broker auto-elects itself as leader on startup. No TCP handshakes,
no election messages, no admin heartbeats. All code paths in this document are
effectively no-ops in the single-node case.

---

## 2. Peer Connection Lifecycle

### 2.1 Connection Establishment

When a broker starts with peers configured, it initiates a connection to each peer
using the SETUP → SM handshake from doc 04:

```
Broker A (nodeId=1)                        Broker B (nodeId=2)
     │                                          │
     │  TCP connect + Handshake {                │
     │    magic=0x42525A00, version=1,           │
     │    source=1, target=2, dir=SEND,          │
     │    epoch=1, group_hash=0xABCD}            │
     ├──────────────────────────────────────────►│
     │                                           │  Validate handshake
     │                                           │  Register incoming connection
     │                                           │
     │  TCP accept + Handshake {                 │
     │    source=2, target=1, dir=SEND, ...}     │
     │◄──────────────────────────────────────────┤
     │                                           │
     │  Both directions established               │
     │                                           │
     │  ─── admin + message traffic flows ───     │
```

**Steps (executed on the broker-agent thread at startup):**

1. Read peer endpoints from config (`broker.member.host.ports`), which contains a
   comma-separated list of `id@host:port` pairs.
2. For each peer, resolve the `std.net.Address` and register the node in
   `NodeMembership` with `connection_state = .disconnected`.
3. Enqueue a `connect_peer` command to the sender event loop (via the sender command
   queue from doc 10).
4. The sender event loop initiates a TCP connection via `brz_tcp` and sends the
   24-byte handshake frame upon connection.
5. When the receiver event loop accepts a TCP connection from a peer, it validates the
   handshake (magic, protocol version, group hash, target node ID, session epoch).
6. The receiver event loop posts a `peer_connected` command to the broker-agent, which
   updates `Node.connection_state = .connected` and triggers `ClusterStateSnapshot`
   exchange (§5.2).

Both sides independently initiate SETUP to each other — the protocol is symmetric.
A broker accepts a SETUP from a peer even if it has already sent its own SETUP to
that peer. The result is two unidirectional channels (one send log per direction),
matching the Java reference's one Aeron `Publication` + one `Subscription` per peer.

**Connection state** is tracked directly on the `Node` struct in `NodeMembership`
(see §7). There is no separate `PeerConnection` struct — all peer metadata lives in
one place. Since `nodeId` is `u8`, the `NodeMembership` array is at most 256
entries — allocated once at startup from the page allocator.

### 2.2 Disconnection Detection

A peer is considered disconnected when:

1. **No admin heartbeat received** for `MASTER_DOWN_INTERVAL` (default 3 seconds, see
   §3.4). The broker-agent thread checks `Node.last_heartbeat_ns` during its duty
   cycle. This is the same timer used for leader election — liveness detection and
   election are unified.
2. **Receiver event loop reports a fatal error** for the peer's receive log (CRC
   failures beyond threshold, or the peer sends a TEARDOWN frame).

Both paths funnel through the same handler on the broker-agent thread:

```zig
// src/cluster/cluster_event_handler.zig (partial)

/// Called on the broker-agent thread when a peer is determined to be dead.
/// This is the single point of disconnection handling — all downstream
/// effects flow from here.
pub fn handlePeerDisconnected(self: *ClusterEventHandler, node_id: u8) void {
    const node = &(self.node_membership.nodes[node_id] orelse return);
    if (node.connection_state == .disconnected) return; // already handled

    // 1. Mark disconnected
    node.connection_state = .disconnected;
    node.setup_attempt_count = 0;

    // 2. Remove the node from membership
    self.node_membership.removeNode(node_id);

    // 3. Remove all service instances registered on that node
    const removed_count = self.service_registry.removeByNodeId(node_id);
    if (removed_count > 0) {
        // Notify local subscribers of removed services
        self.notifyAffectedSubscribers(node_id);
    }

    // 4. Command receiver event loop to tear down receive log buffer
    self.receiver_cmd_queue.enqueue(.{ .close_peer = node_id }) catch |err| {
        self.error_log.record("Failed to enqueue close_peer command: {}", .{err});
    };

    // 5. Command sender event loop to remove peer from send list
    self.sender_cmd_queue.enqueue(.{ .close_peer = node_id }) catch |err| {
        self.error_log.record("Failed to enqueue close_peer to sender: {}", .{err});
    };

    // 6. Leader election is handled automatically — if the departed node
    //    was the master, the master-down timer will fire and a new leader
    //    will be elected (§3).

    // 7. If we are the broker leader, re-evaluate service leaders
    //    for any services that had instances on the departed node
    if (self.leader_election.isLocalNodeLeader()) {
        self.reEvaluateAndBroadcast();
    }
}
```

**Liveness check (runs as a duty-cycle function on the broker-agent thread):**

The liveness check uses the same `MASTER_DOWN_INTERVAL_NS` (3 seconds) as the
leader election timer — a single timeout governs both peer liveness and leadership.
The `ClusterEventHandler.checkPeerLiveness()` method (§11) iterates all connected
nodes and calls `handlePeerDisconnected()` for any whose `last_heartbeat_ns`
exceeds the interval. This feeds into the leader election naturally: if the departed
node was the leader, `checkMasterDown()` will fire on the next duty cycle and
elect a new leader.

---

## 3. Broker Leader Election (VRRP-style)

### 3.1 Algorithm

The cluster uses a VRRP-style (Virtual Router Redundancy Protocol, RFC 5798) master
advertisement protocol where the **lowest `nodeId` always wins**. Unlike the Bully
algorithm used in the Java reference, this approach unifies heartbeats and leader
election into a single mechanism: **a heartbeat from a node is simultaneously its
claim to leadership priority**.

```
Trigger: periodic heartbeat (1 second interval)

┌──────────────┐        ┌──────────────┐        ┌──────────────┐
│   Broker A    │        │   Broker B    │        │   Broker C    │
│   nodeId=1    │        │   nodeId=2    │        │   nodeId=3    │
└──────┬────────┘        └──────┬────────┘        └──────┬────────┘
       │                        │                        │
       │  BrokerHeartbeat(1)    │                        │
       ├───────────────────────►├───────────────────────►│
       │                        │                        │
       │  BrokerHeartbeat(2)    │                        │
       │◄───────────────────────┼───────────────────────►│
       │                        │                        │
       │                        │   BrokerHeartbeat(3)   │
       │◄───────────────────────┼◄───────────────────────┤
       │                        │                        │
       │  All nodes see nodeId=1│has the lowest ID       │
       │  → A is accepted as    │leader by all nodes     │
       │  (master-down timers   │reset on each heartbeat)│
       │                        │                        │
```

**Step-by-step:**

1. **Every broker broadcasts `BrokerHeartbeat(myNodeId)` every 1 second** to all
   connected peers. This is both a liveness signal and a leadership assertion.
2. **On receiving `BrokerHeartbeat(senderNodeId)`:**
   - If `senderNodeId` is unknown, register the sender as a new cluster member
     (equivalent to Java's `memberJoined()`).
   - Update `Node.last_heartbeat_ns` for liveness tracking.
   - If `senderNodeId` ≤ current leader's `nodeId` (or no leader is known), accept
     the sender as the new leader and reset the master-down timer.
   - If `senderNodeId` > current leader's `nodeId`, ignore the leadership claim
     (a better-priority node is already leading).
3. **Master-down timer:** Each broker maintains a timer initialized to
   `MASTER_DOWN_INTERVAL` (3 seconds). The timer is reset every time a heartbeat is
   received from the current leader. If the timer expires without a heartbeat from
   the leader, the broker assumes the leader is dead.
4. **On master-down timer expiry:**
   - Clear the current leader.
   - Check if the local node has the lowest `nodeId` among all known alive nodes.
   - If yes, declare self as leader: call `onLeaderElected(self.local_node_id)`.
   - If no, wait for the node with the lowest `nodeId` to assert itself via its
     next heartbeat (which will arrive within 1 second).
5. **Post-election:** The new leader sends `ClusterStateSnapshot` only if this is a
   new peer connection (not on every leadership change). Service leader re-evaluation
   still runs on every leadership change.
6. **Preemption:** If a node with a lower `nodeId` comes online (e.g. a crashed broker
   restarts), it starts sending heartbeats. All other nodes automatically defer to it
   as the new leader — no explicit election round is needed.

**Key insight:** There is no election phase, no election window, no acknowledgment
round. The heartbeat *is* the election. Leadership is an emergent property of "which
node with the lowest ID am I hearing from?"

### 3.2 Why VRRP over Bully

The Java reference implementation uses a Bully algorithm with three dedicated election
message types (`InitiateElection`, `NodeAcknowledgment`, `LeaderAnnouncement`) and a
5-second election window. The VRRP-style approach is simpler and faster:

| Aspect | Bully (Java reference) | VRRP-style (this design) |
|---|---|---|
| **Election message types** | 3 (`InitiateElection`, `NodeAcknowledgment`, `LeaderAnnouncement`) | 0 — reuses `BrokerHeartbeat` |
| **Total admin message types** | 8 | 5 |
| **Election state** | `election_in_progress`, `election_start_ns`, `lowest_seen`, callbacks | `master_down_deadline_ns` — one field |
| **Time to elect new leader** | 5 seconds (election window) | 3 seconds (master-down interval), tunable |
| **Code complexity** | ~150 lines for `LeaderElection` | ~60 lines for `LeaderElection` |
| **Correctness edge cases** | Stale elections, overlapping windows, late acknowledgments | Essentially none — stateless |
| **Preemption** | Requires explicit re-election round | Automatic — lower-priority node's heartbeat preempts |
| **Cold start convergence** | 5 seconds after first peer connects | 3 seconds (master-down timer) |

The VRRP approach is well-proven in production — keepalived has used it for 20+ years
for Linux high-availability clusters.

### 3.3 LeaderElection Implementation

The `LeaderElection` struct is owned by the broker-agent thread. It tracks leadership
state and uses return values (not callbacks) so the caller can act on state changes
directly.

```zig
// src/cluster/leader_election.zig

const std = @import("std");
const clock = @import("../platform/clock.zig");

pub const LeaderElection = struct {
    /// Result returned by methods that may change leadership state.
    pub const Result = struct {
        /// The leader nodeId after this operation, or null if no leader.
        leader: ?u8 = null,
        /// True if the leader changed as a result of this operation.
        changed: bool = false,
    };

    /// This broker's node ID (immutable after init).
    local_node_id: u8,

    /// The currently accepted cluster leader. `null` = no leader known.
    current_leader: ?u8 = null,

    /// Monotonic deadline: if `clock.monotonicNanos()` exceeds this value
    /// without a heartbeat from the current leader, the leader is presumed dead.
    master_down_deadline_ns: i64 = 0,

    /// 3 × heartbeat interval. The leader must send at least one heartbeat
    /// within this window or it is considered dead.
    const MASTER_DOWN_INTERVAL_NS: i64 = 3 * std.time.ns_per_s;

    pub fn init(local_node_id: u8) LeaderElection {
        return .{
            .local_node_id = local_node_id,
            // Set initial deadline so the first check after startup triggers
            // self-election if no peers respond within the interval.
            .master_down_deadline_ns = clock.monotonicNanos() + MASTER_DOWN_INTERVAL_NS,
        };
    }

    // ── Heartbeat handling (the core of VRRP-style election) ─────────

    /// Called when a BrokerHeartbeat is received from a peer.
    /// Returns a Result indicating whether the leader changed.
    ///
    /// This is the primary election mechanism: if the sender has equal or
    /// better priority (lower nodeId) than the current leader, it becomes
    /// the new leader. The master-down timer is reset.
    pub fn onBrokerHeartbeat(self: *LeaderElection, sender_id: u8, now_ns: i64) Result {
        const current = self.current_leader orelse std.math.maxInt(u8);

        if (sender_id <= current) {
            // Sender has equal or better priority — accept as leader
            const changed = self.current_leader == null or self.current_leader.? != sender_id;
            self.current_leader = sender_id;
            self.master_down_deadline_ns = now_ns + MASTER_DOWN_INTERVAL_NS;
            return .{ .leader = sender_id, .changed = changed };
        }

        // Sender has worse priority — not a leadership change, but still
        // a valid heartbeat for liveness purposes (handled by caller).
        return .{ .leader = self.current_leader, .changed = false };
    }

    // ── Master-down timer check ──────────────────────────────────────

    /// Called once per broker-agent duty cycle. Checks if the master-down
    /// timer has expired. If so, determines the new leader.
    ///
    /// Returns a Result. If `changed == true`, the caller must invoke
    /// post-election logic (service leader re-evaluation, etc.).
    pub fn checkMasterDown(self: *LeaderElection, now_ns: i64) Result {
        // No timeout if we are the leader (we don't need our own heartbeats)
        if (self.current_leader != null and self.current_leader.? == self.local_node_id) {
            return .{ .leader = self.current_leader, .changed = false };
        }

        // Timer hasn't expired yet
        if (now_ns < self.master_down_deadline_ns) {
            return .{ .leader = self.current_leader, .changed = false };
        }

        // Master-down timer expired — the current leader is presumed dead.
        // Become leader if we have the lowest nodeId among known-alive nodes.
        // (The caller provides alive-node information via NodeMembership;
        //  here we optimistically self-elect. If a better node is alive,
        //  its next heartbeat will preempt us within 1 second.)
        const previous = self.current_leader;
        self.current_leader = self.local_node_id;
        self.master_down_deadline_ns = now_ns + MASTER_DOWN_INTERVAL_NS;

        const changed = previous == null or previous.? != self.local_node_id;
        return .{ .leader = self.local_node_id, .changed = changed };
    }

    // ── Peer departure ───────────────────────────────────────────────

    /// Called when a peer is known to have disconnected (e.g. TEARDOWN
    /// frame received). If the departed peer was the leader, resets the
    /// master-down timer to trigger immediate re-election on the next
    /// duty cycle.
    pub fn onPeerDisconnected(self: *LeaderElection, departed_id: u8, now_ns: i64) Result {
        if (self.current_leader != null and self.current_leader.? == departed_id) {
            // Leader is gone — expire the timer immediately
            self.current_leader = null;
            self.master_down_deadline_ns = now_ns; // triggers on next checkMasterDown()
            return .{ .leader = null, .changed = true };
        }
        return .{ .leader = self.current_leader, .changed = false };
    }

    // ── Queries ──────────────────────────────────────────────────────

    pub fn isLocalNodeLeader(self: *const LeaderElection) bool {
        return self.current_leader != null and self.current_leader.? == self.local_node_id;
    }

    pub fn getLeader(self: *const LeaderElection) ?u8 {
        return self.current_leader;
    }
};
```

**Design notes:**

- **No callbacks.** Every method returns a `Result` struct. The caller
  (`ClusterEventHandler`) inspects `result.changed` and invokes downstream logic.
  This eliminates re-entrant call chains and makes testing trivial — assert on
  return values, no mock callbacks needed.
- **No election state machine.** There is no `election_in_progress` flag, no
  `lowest_seen` accumulator, no election window timer. The single
  `master_down_deadline_ns` field replaces all of it.
- **Self-election on master-down.** When the timer fires, the local node declares
  itself leader. If a node with a lower `nodeId` is still alive, its heartbeat
  will arrive within 1 second and preempt. This is correct because: (a) the
  preempting node has better priority, and (b) all other nodes will also accept
  the preemption when they receive the same heartbeat.

### 3.4 Election Timing

The master-down interval is 3 seconds (`MASTER_DOWN_INTERVAL_NS = 3 × 1s`), derived
from the heartbeat interval of 1 second. This means a broker must miss 3 consecutive
heartbeats before being declared dead. The interval provides tolerance for temporary
packet loss and scheduling jitter while being 40% faster than the old 5-second Bully
election window.

**Timeline for a 3-broker cluster startup:**

```
t=0.0s   Broker A starts (nodeId=1), sends SETUP to B and C
t=0.1s   Broker B starts (nodeId=2), sends SETUP to A and C
t=0.2s   Broker C starts (nodeId=3), sends SETUP to A and B
t=0.3s   A↔B connected (SM received)
t=0.4s   A↔C connected
t=0.5s   B↔C connected
         — all brokers start exchanging BrokerHeartbeat —
t=0.5s   B and C receive BrokerHeartbeat(1) from A
         → both accept A as leader (nodeId=1 is lowest)
         → A's master-down timer is checked: A sees only its own heartbeat,
           self-elects immediately (no one with lower nodeId exists)
t=0.5s   Leader elected: A (nodeId=1). Total time: ~0.2s after connections.
t=0.5s   A sends ClusterStateSnapshot to B and C (triggered by peer join)
```

**Timeline for leader failure:**

```
t=0.0s   A is the leader (nodeId=1). Heartbeats flowing normally.
t=1.0s   A crashes. No more heartbeats from A.
t=1.0s   B and C still see their master-down timers counting down.
t=3.0s   B's master-down timer expires. B self-elects (nodeId=2).
         C's master-down timer expires. C self-elects (nodeId=3).
t=3.0s   B sends BrokerHeartbeat(2). C receives it.
         C sees nodeId=2 < nodeId=3 → accepts B as leader.
t=3.0s   Leader elected: B (nodeId=2). Total time: ~3s after crash.
```

**Preemption timeline (crashed leader restarts):**

```
t=0.0s   B is the leader (nodeId=2). A was previously crashed.
t=5.0s   A restarts, re-establishes connections.
t=5.5s   A sends BrokerHeartbeat(1). B and C receive it.
         Both see nodeId=1 < nodeId=2 → accept A as new leader.
t=5.5s   A's master-down timer fires, A self-elects (lowest known nodeId).
t=5.5s   Leader preempted: A (nodeId=1). No election round needed.
```

---

## 4. Admin Message Protocol

### 4.1 Message Table

Admin messages use a distinct **admin channel** — a separate logical stream from the
service message channel. In the Java reference, this is an Aeron stream with a
different `streamId` (`broker.admin.stream.id = 100`). In our TCP transport, admin
messages are DATA frames with the `ADMIN` flag (`0x20`) set in the frame header's
flags byte. The receiver event loop checks this flag and dispatches to the admin
handler instead of the message router.

| templateId | Message | Direction | Description |
|:----------:|---------|-----------|-------------|
| 1 | `BrokerHeartbeat` | Any → all peers | Broker liveness + leadership assertion |
| 2 | `ClusterStateSnapshot` | Any → new peer | Full state sync on peer connection |
| 3 | `ServiceAdded` | Any → all peers | A service registered locally |
| 4 | `ServiceRemoved` | Any → all peers | A service deregistered locally |
| 5 | `ServiceLeaderDesignated` | Leader → all peers | Leader designated for a service |

Compared to the Java reference which has 8 message types (including `InitiateElection`,
`NodeAcknowledgment`, `LeaderAnnouncement`), the VRRP-style protocol needs only 5.
The three election-specific message types are eliminated — `BrokerHeartbeat` subsumes
their function.

**templateId renumbering note:** The Java SBE schema (`broker/src/main/resources/messages.xml`)
uses templateIds 1–8. This design renumbers to 1–5 for simplicity. If wire
compatibility with the Java broker is needed during a migration period, the original
IDs (4–8) can be preserved and IDs 1–3 simply left unused.

### 4.2 Admin Message Wire Format

Admin messages are framed inside DATA frames (doc 04) with the `ADMIN` flag. The
payload starts with an 8-byte `AdminMessageHeader` followed by the message-specific
body.

```
Admin message layout (inside DATA frame payload):

Offset   Size   Type     Field
───────────────────────────────────────────────
0        2      u16      block_length     — size of the message body (excluding header)
2        2      u16      template_id      — message type (1–5)
4        2      u16      schema_id        — always 688 (matches Java SBE schema)
6        2      u16      version          — schema version (1)
8        N      bytes    message body     — template-specific fields
```

This mirrors the Java `brokerMessageHeader` composite from the SBE schema. In Zig:

```zig
// src/cluster/admin_messages.zig

pub const SCHEMA_ID: u16 = 688;
pub const SCHEMA_VERSION: u16 = 1;

/// Wire header for all admin messages. Matches the Java `brokerMessageHeader`.
/// 8 bytes, little-endian, overlaid directly on the DATA frame payload.
pub const AdminMessageHeader = packed struct(u64) {
    block_length: u16,
    template_id: u16,
    schema_id: u16,
    version: u16,
};

comptime {
    std.debug.assert(@sizeOf(AdminMessageHeader) == 8);
}
```

**Message body layouts (all fields little-endian):**

```zig
// src/cluster/admin_messages.zig (continued)

/// templateId = 1: BrokerHeartbeat
/// Serves as both liveness keepalive and leadership priority assertion.
/// Java SBE: nodeId (uint8) + hostAndPort (char[22])
pub const BrokerHeartbeatBody = packed struct {
    node_id: u8,
    host_and_port: [22]u8,
};

/// templateId = 3: ServiceAdded
/// Java SBE: nodeId (uint8) + serviceId (uint16) + serviceName (char[32])
///           + leaderElectionEnabled (uint8 BooleanType)
pub const ServiceAddedBody = packed struct {
    node_id: u8,
    service_id: u16,
    service_name: [32]u8,
    leader_election_enabled: u8, // 0 = false, 1 = true
};

/// templateId = 4: ServiceRemoved
/// Java SBE: nodeId (uint8) + serviceId (uint16) + serviceName (char[32])
pub const ServiceRemovedBody = packed struct {
    node_id: u8,
    service_id: u16,
    service_name: [32]u8,
};

/// templateId = 5: ServiceLeaderDesignated
/// Java SBE: nodeId (uint8) + serviceId (uint16) + serviceName (char[32])
pub const ServiceLeaderDesignatedBody = packed struct {
    node_id: u8,
    service_id: u16,
    service_name: [32]u8,
};
```

`ClusterStateSnapshot` (templateId = 2) contains a repeating group and is handled
separately (§5.2).

**Encoding helper (zero-copy into send buffer):**

```zig
// src/cluster/admin_messages.zig (continued)

/// Encode an admin message header + body into the provided buffer.
/// Returns the total encoded length (header + body).
pub fn encodeAdminMessage(
    buf: []u8,
    comptime BodyType: type,
    template_id: u16,
    body: BodyType,
) usize {
    const header_len = @sizeOf(AdminMessageHeader);
    const body_len = @sizeOf(BodyType);
    std.debug.assert(buf.len >= header_len + body_len);

    // Write header
    const header_ptr: *AdminMessageHeader = @ptrCast(@alignCast(buf.ptr));
    header_ptr.* = .{
        .block_length = @intCast(body_len),
        .template_id = template_id,
        .schema_id = SCHEMA_ID,
        .version = SCHEMA_VERSION,
    };

    // Write body
    const body_ptr: *BodyType = @ptrCast(@alignCast(buf[header_len..].ptr));
    body_ptr.* = body;

    return header_len + body_len;
}
```

---

## 5. Cluster State Synchronization

### 5.1 State Model

The cluster maintains an eventually-consistent replica of the service registry on every
broker. The source of truth for a service instance is the broker where that service is
locally registered. Updates are idempotent — keyed by `(serviceId, nodeId)` — so
duplicate or reordered messages converge to the same state.

This matches the Java `ClusterStateManager` + `ServiceRegistry` with its
`BiInt2ObjectMap<ServiceInstance>` keyed by `(serviceId, nodeId)`.

**State tracked per remote service instance:**

```zig
// src/cluster/cluster_state.zig

pub const RemoteServiceInstance = struct {
    service_id: u16,
    node_id: u8,
    service_name: [32]u8,
    leader_election_enabled: bool,
};
```

The broker-agent thread stores remote instances in a flat array (pre-allocated for
`MAX_REMOTE_INSTANCES`, e.g. 4096). Lookups by `(serviceId, nodeId)` use a hash map;
lookups by `serviceName` scan the array (rare, cold-path only).

### 5.2 ClusterStateSnapshot

Sent by each broker **when a new peer connects** (SETUP handshake completes). Unlike
the Java reference which sends snapshots on every leader election, this design sends
them only on peer join. Rationale:

- **Peer join** is the only event where a node might have missed incremental updates
  (it was offline or newly joining).
- **Leader change** does not cause data loss — all nodes already received incremental
  `ServiceAdded`/`ServiceRemoved` updates while the previous leader was alive.
- **Split-brain recovery** is handled by service leader re-evaluation (§6.5), which
  does not require a full state snapshot.

This reduces network traffic during leader flapping scenarios.

The Java `BrokerAdminPublisher.sendClusterStateSnapshot()` encodes an SBE repeating
group of services. In Zig, the snapshot uses a variable-length encoding:

```
ClusterStateSnapshot wire layout (templateId = 2):

AdminMessageHeader (8 bytes)
├── block_length: 5    (fixed fields below)
├── template_id:  2
├── schema_id:    688
└── version:      1

Fixed fields (1 byte):
├── node_id:       u8     (the sender's nodeId)

Group header (4 bytes):
├── block_length:  u16    (size of each group entry)
└── num_in_group:  u16    (number of service entries)

Repeated group entries (N × entry_size):
├── service_id:              u16
├── service_name:            [32]u8
└── leader_election_enabled: u8
```

**Encoding:**

```zig
// src/cluster/cluster_state.zig

const admin = @import("admin_messages.zig");

const GroupHeader = packed struct(u32) {
    block_length: u16,
    num_in_group: u16,
};

const SnapshotEntry = packed struct {
    service_id: u16,
    service_name: [32]u8,
    leader_election_enabled: u8,
};

comptime {
    std.debug.assert(@sizeOf(SnapshotEntry) == 35);
}

/// Encode a ClusterStateSnapshot into the provided buffer.
/// Returns the total encoded length.
pub fn encodeClusterStateSnapshot(
    buf: []u8,
    local_node_id: u8,
    instances: []const RemoteServiceInstance,
) usize {
    var offset: usize = 0;

    // Admin message header
    const hdr_ptr: *admin.AdminMessageHeader = @ptrCast(@alignCast(buf[offset..].ptr));
    hdr_ptr.* = .{
        .block_length = 1, // node_id only (fixed part before group)
        .template_id = 2,
        .schema_id = admin.SCHEMA_ID,
        .version = admin.SCHEMA_VERSION,
    };
    offset += @sizeOf(admin.AdminMessageHeader);

    // Fixed field: node_id
    buf[offset] = local_node_id;
    offset += 1;

    // Group header
    const group_hdr: *GroupHeader = @ptrCast(@alignCast(buf[offset..].ptr));
    group_hdr.* = .{
        .block_length = @intCast(@sizeOf(SnapshotEntry)),
        .num_in_group = @intCast(instances.len),
    };
    offset += @sizeOf(GroupHeader);

    // Group entries
    for (instances) |inst| {
        const entry: *SnapshotEntry = @ptrCast(@alignCast(buf[offset..].ptr));
        entry.* = .{
            .service_id = inst.service_id,
            .service_name = inst.service_name,
            .leader_election_enabled = if (inst.leader_election_enabled) 1 else 0,
        };
        offset += @sizeOf(SnapshotEntry);
    }

    return offset;
}
```

**Sending (called when a new peer connects):**

```zig
// src/cluster/cluster_state.zig (continued)

/// Send our local service instances as a snapshot to a specific peer or all peers.
/// Called on the broker-agent thread when a new peer connection is established.
pub fn sendClusterStateSnapshot(
    self: *ClusterState,
    local_instances: []const RemoteServiceInstance,
    local_node_id: u8,
    send_fn: *const fn (buf: []const u8) void,
) void {
    if (local_instances.len == 0) return;

    const len = encodeClusterStateSnapshot(
        &self.snapshot_buf,
        local_node_id,
        local_instances,
    );
    send_fn(self.snapshot_buf[0..len]);
}
```

### 5.3 Handling State Snapshot

When a broker receives a `ClusterStateSnapshot`, it merges the contents into its
local replica. Merge is additive for new entries and removes stale entries for the
sender's `nodeId` that are not in the snapshot.

```zig
// src/cluster/cluster_state.zig (continued)

/// Merge a received ClusterStateSnapshot into the local registry.
/// Called on the broker-agent thread.
pub fn onClusterStateSnapshot(
    self: *ClusterState,
    payload: []const u8,
    service_registry: *ServiceRegistry,
    service_leader_election: *ServiceLeaderElectionManager,
    notify_subscribers_fn: *const fn (service_name: []const u8) void,
) void {
    // Decode header
    const node_id = payload[0];
    const group_hdr: *const GroupHeader = @ptrCast(@alignCast(payload[1..].ptr));
    const num_entries = group_hdr.num_in_group;
    const entry_size = group_hdr.block_length;

    var offset: usize = 1 + @sizeOf(GroupHeader);
    var received_ids = std.BoundedArray(u32, 256){}; // serviceId << 16 | nodeId

    // Add new / update existing
    var i: u16 = 0;
    while (i < num_entries) : (i += 1) {
        const entry: *const SnapshotEntry = @ptrCast(@alignCast(payload[offset..].ptr));

        const instance = RemoteServiceInstance{
            .service_id = entry.service_id,
            .node_id = node_id,
            .service_name = entry.service_name,
            .leader_election_enabled = entry.leader_election_enabled != 0,
        };

        if (!service_registry.has(entry.service_id, node_id)) {
            service_registry.register(instance);
            notify_subscribers_fn(&entry.service_name);
        }

        if (instance.leader_election_enabled) {
            service_leader_election.registerInstance(
                trimServiceName(&entry.service_name),
                entry.service_id,
                node_id,
            );
        }

        received_ids.appendAssumeCapacity(
            @as(u32, entry.service_id) << 16 | @as(u32, node_id),
        );

        offset += entry_size;
    }

    // Remove stale entries for this nodeId that were NOT in the snapshot
    service_registry.removeByNodeIdExcluding(node_id, received_ids.constSlice());
}
```

### 5.4 Incremental Updates

After the initial snapshot exchange, service additions and removals are propagated
incrementally. Each broker broadcasts updates for its own local services only.

**ServiceAdded (templateId = 3):**

Sent when a service completes registration on the local broker (see doc 09 §3). The
broker-agent thread encodes the message and broadcasts it:

```zig
// src/cluster/cluster_state.zig (continued)

/// Broadcast a ServiceAdded event to all peer brokers.
/// Java equivalent: `ClusterStateManager.serviceAdded()`.
pub fn broadcastServiceAdded(
    self: *ClusterState,
    service_id: u16,
    service_name: [32]u8,
    leader_election_enabled: bool,
    local_node_id: u8,
    broadcast_fn: *const fn (buf: []const u8) void,
) void {
    const len = admin.encodeAdminMessage(
        &self.send_buf,
        admin.ServiceAddedBody,
        3, // templateId
        .{
            .node_id = local_node_id,
            .service_id = service_id,
            .service_name = service_name,
            .leader_election_enabled = if (leader_election_enabled) 1 else 0,
        },
    );
    broadcast_fn(self.send_buf[0..len]);
}
```

**Handling inbound `ServiceAdded`:**

```zig
/// Handle a ServiceAdded message from a peer broker.
pub fn onServiceAdded(
    self: *ClusterState,
    payload: []const u8,
    service_registry: *ServiceRegistry,
    service_leader_election: *ServiceLeaderElectionManager,
    notify_fn: *const fn ([]const u8) void,
) void {
    const body: *const admin.ServiceAddedBody = @ptrCast(@alignCast(payload.ptr));

    const instance = RemoteServiceInstance{
        .service_id = body.service_id,
        .node_id = body.node_id,
        .service_name = body.service_name,
        .leader_election_enabled = body.leader_election_enabled != 0,
    };

    service_registry.register(instance);
    notify_fn(&body.service_name);

    if (instance.leader_election_enabled) {
        service_leader_election.registerInstance(
            trimServiceName(&body.service_name),
            body.service_id,
            body.node_id,
        );
    }
}
```

**ServiceRemoved (templateId = 4):**

```zig
/// Handle a ServiceRemoved message from a peer broker.
pub fn onServiceRemoved(
    self: *ClusterState,
    payload: []const u8,
    service_registry: *ServiceRegistry,
    service_leader_election: *ServiceLeaderElectionManager,
    notify_fn: *const fn ([]const u8) void,
) void {
    const body: *const admin.ServiceRemovedBody = @ptrCast(@alignCast(payload.ptr));

    service_registry.remove(body.service_id, body.node_id);
    notify_fn(&body.service_name);

    service_leader_election.unregisterInstance(
        trimServiceName(&body.service_name),
        body.service_id,
    );
}
```

---

## 6. Service Leader Election (Cluster-Wide)

### 6.1 Policy

Service leader election is an optional, per-service feature that designates exactly one
instance of a named service as the "leader" across the entire cluster. The rule is
**lowest `serviceId` wins** — effectively a first-registered-wins policy, because
service IDs are monotonically incremented by the broker.

This maps directly to the Java `ServiceLeaderElectionManager`.

A service opts in by setting `leaderElectionEnabled = true` in its registration message
(`brz.service.leader_election.enabled = true` in config). If not opted in, no leader
is tracked for that service.

### 6.2 Triggers

Service leader evaluation is invoked on the broker-agent thread when:

| Event | Action |
|---|---|
| Service registered with `leaderElectionEnabled = true` | Evaluate leader for that service name |
| Service removed (was leader or had election enabled) | Re-evaluate leader for that service name |
| Broker leadership changes (new broker leader elected) | Re-evaluate all service leaders |
| Cluster state snapshot received from a peer | Register instances, then evaluate affected services |

### 6.3 ServiceLeaderElectionManager

The `ServiceLeaderElectionManager` is single-threaded (broker-agent thread only). It
maintains per-service-name leader state using a hash map keyed by service name.

```zig
// src/cluster/service_leader_election.zig

const std = @import("std");

pub const ServiceLeaderElectionManager = struct {
    /// Per-service leader state, keyed by service name (null-padded [32]u8).
    leader_states: std.AutoHashMap([32]u8, LeaderState),
    /// This broker's nodeId — used to check if we are the cluster leader.
    local_node_id: u8,

    /// Mutable state for one service name.
    const LeaderState = struct {
        /// Set of registered instances, encoded as (serviceId << 16 | nodeId).
        /// Equivalent to Java's `IntHashSet`.
        registered_instances: std.AutoHashMap(u32, void),
        /// Current leader serviceId, or null if no leader.
        leader_service_id: ?u16 = null,
        /// Current leader nodeId.
        leader_node_id: ?u8 = null,

        fn encodeKey(service_id: u16, node_id: u8) u32 {
            return (@as(u32, service_id) << 16) | @as(u32, node_id);
        }

        fn decodeServiceId(key: u32) u16 {
            return @intCast(key >> 16);
        }

        fn decodeNodeId(key: u32) u8 {
            return @intCast(key & 0xFF);
        }
    };

    pub const ElectionResult = struct {
        service_id: u16,
        node_id: u8,
        changed: bool,
    };

    pub const ServiceLeaderChange = struct {
        service_name: [32]u8,
        result: ElectionResult,
    };

    pub fn init(allocator: std.mem.Allocator, local_node_id: u8) ServiceLeaderElectionManager {
        return .{
            .leader_states = std.AutoHashMap([32]u8, LeaderState).init(allocator),
            .local_node_id = local_node_id,
        };
    }

    /// Register a service instance for leader election tracking.
    pub fn registerInstance(
        self: *ServiceLeaderElectionManager,
        service_name: [32]u8,
        service_id: u16,
        node_id: u8,
    ) void {
        const gop = self.leader_states.getOrPut(service_name) catch return;
        if (!gop.found_existing) {
            gop.value_ptr.* = .{
                .registered_instances = std.AutoHashMap(u32, void).init(
                    self.leader_states.allocator,
                ),
            };
        }
        gop.value_ptr.registered_instances.put(
            LeaderState.encodeKey(service_id, node_id),
            {},
        ) catch {};
    }

    /// Unregister a service instance. If it was the leader, clear the leader.
    pub fn unregisterInstance(
        self: *ServiceLeaderElectionManager,
        service_name: [32]u8,
        service_id: u16,
    ) void {
        const state = self.leader_states.getPtr(service_name) orelse return;

        // Remove all entries with this serviceId (across any nodeId)
        var iter = state.registered_instances.keyIterator();
        var to_remove: [64]u32 = undefined;
        var remove_count: usize = 0;
        while (iter.next()) |key_ptr| {
            if (LeaderState.decodeServiceId(key_ptr.*) == service_id) {
                if (remove_count < to_remove.len) {
                    to_remove[remove_count] = key_ptr.*;
                    remove_count += 1;
                }
            }
        }
        for (to_remove[0..remove_count]) |key| {
            _ = state.registered_instances.remove(key);
        }

        // Clear leader if it was the removed instance
        if (state.leader_service_id) |leader_id| {
            if (leader_id == service_id) {
                state.leader_service_id = null;
                state.leader_node_id = null;
            }
        }

        // Clean up empty entries
        if (state.registered_instances.count() == 0) {
            state.registered_instances.deinit();
            _ = self.leader_states.remove(service_name);
        }
    }

    /// Elect a leader for the given service. If a leader already exists,
    /// returns it. Otherwise picks the lowest serviceId.
    /// Java equivalent: `ServiceLeaderElectionManager.electLeader()`.
    pub fn electLeader(
        self: *ServiceLeaderElectionManager,
        service_name: [32]u8,
    ) ?ElectionResult {
        const state = self.leader_states.getPtr(service_name) orelse return null;
        if (state.registered_instances.count() == 0) return null;

        // If a leader is already set, return it (no change)
        if (state.leader_service_id) |leader_sid| {
            return .{
                .service_id = leader_sid,
                .node_id = state.leader_node_id.?,
                .changed = false,
            };
        }

        return self.pickLowestServiceId(state);
    }

    /// Force re-election: clear current leader and pick the lowest serviceId.
    pub fn reElectLeader(
        self: *ServiceLeaderElectionManager,
        service_name: [32]u8,
    ) ?ElectionResult {
        const state = self.leader_states.getPtr(service_name) orelse return null;
        if (state.registered_instances.count() == 0) return null;

        const previous = state.leader_service_id;
        state.leader_service_id = null;
        state.leader_node_id = null;

        const result = self.pickLowestServiceId(state) orelse return null;

        // If the same leader was re-elected, mark as not changed
        if (previous != null and previous.? == result.service_id) {
            return .{
                .service_id = result.service_id,
                .node_id = result.node_id,
                .changed = false,
            };
        }
        return result;
    }

    /// Re-evaluate all tracked services. Called after a broker leadership
    /// change (split-brain recovery). Returns a list of services where
    /// leadership changed.
    /// Java equivalent: `ServiceLeaderElectionManager.reEvaluateAllLeaders()`.
    pub fn reEvaluateAllLeaders(
        self: *ServiceLeaderElectionManager,
        changes_out: []ServiceLeaderChange,
    ) usize {
        var count: usize = 0;
        var iter = self.leader_states.iterator();
        while (iter.next()) |entry| {
            const state = entry.value_ptr;
            const previous = state.leader_service_id;
            state.leader_service_id = null;
            state.leader_node_id = null;

            if (self.pickLowestServiceId(state)) |result| {
                if (previous == null or previous.? != result.service_id) {
                    if (count < changes_out.len) {
                        changes_out[count] = .{
                            .service_name = entry.key_ptr.*,
                            .result = result,
                        };
                        count += 1;
                    }
                }
            }
        }
        return count;
    }

    /// Set the leader from a remote broker's designation (this broker is
    /// not the cluster leader).
    pub fn setLeaderFromRemote(
        self: *ServiceLeaderElectionManager,
        service_name: [32]u8,
        service_id: u16,
        node_id: u8,
    ) void {
        const gop = self.leader_states.getOrPut(service_name) catch return;
        if (!gop.found_existing) {
            gop.value_ptr.* = .{
                .registered_instances = std.AutoHashMap(u32, void).init(
                    self.leader_states.allocator,
                ),
            };
        }
        gop.value_ptr.leader_service_id = service_id;
        gop.value_ptr.leader_node_id = node_id;
    }

    /// Check if leader election is enabled for a service.
    pub fn isLeaderElectionEnabled(
        self: *const ServiceLeaderElectionManager,
        service_name: [32]u8,
    ) bool {
        return self.leader_states.contains(service_name);
    }

    // ── Internal ─────────────────────────────────────────────────────

    fn pickLowestServiceId(self: *ServiceLeaderElectionManager, state: *LeaderState) ?ElectionResult {
        _ = self;
        var lowest_sid: u16 = std.math.maxInt(u16);
        var lowest_nid: u8 = 0;

        var iter = state.registered_instances.keyIterator();
        while (iter.next()) |key_ptr| {
            const sid = LeaderState.decodeServiceId(key_ptr.*);
            if (sid < lowest_sid) {
                lowest_sid = sid;
                lowest_nid = LeaderState.decodeNodeId(key_ptr.*);
            }
        }

        if (lowest_sid == std.math.maxInt(u16)) return null;

        state.leader_service_id = lowest_sid;
        state.leader_node_id = lowest_nid;

        return .{
            .service_id = lowest_sid,
            .node_id = lowest_nid,
            .changed = true,
        };
    }
};
```

### 6.4 Handling ServiceLeaderDesignated

When a non-leader broker receives `ServiceLeaderDesignated` (templateId = 5) from the
cluster leader, it updates its local tracking and forwards the leader change to local
service instances via their control ring buffers:

```zig
// src/cluster/cluster_state.zig (continued)

/// Handle ServiceLeaderDesignated from the cluster leader.
/// Java equivalent: `ServiceLifecycleController.handleServiceLeaderDesignated()`.
pub fn onServiceLeaderDesignated(
    self: *ClusterState,
    payload: []const u8,
    service_leader_election: *ServiceLeaderElectionManager,
    control_processor: *ControlMessageProcessor,
) void {
    const body: *const admin.ServiceLeaderDesignatedBody = @ptrCast(
        @alignCast(payload.ptr),
    );

    const service_name = trimServiceName(&body.service_name);

    // Update local election state
    service_leader_election.setLeaderFromRemote(
        body.service_name,
        body.service_id,
        body.node_id,
    );

    // Forward LeaderChanged to local service instances via control ring buffers
    control_processor.broadcastLeaderChanged(
        service_name,
        body.service_id,
        body.node_id,
    );
}
```

**Broadcasting designation from the cluster leader:**

When the local broker is the cluster leader and evaluates a new service leader, it
must both notify local services and broadcast to peer brokers:

```zig
// src/cluster/cluster_state.zig (continued)

/// Broadcast ServiceLeaderDesignated to all peer brokers.
/// Called only when this broker is the cluster leader.
pub fn broadcastServiceLeaderDesignated(
    self: *ClusterState,
    service_id: u16,
    service_name: [32]u8,
    local_node_id: u8,
    broadcast_fn: *const fn (buf: []const u8) void,
) void {
    const len = admin.encodeAdminMessage(
        &self.send_buf,
        admin.ServiceLeaderDesignatedBody,
        5, // templateId
        .{
            .node_id = local_node_id,
            .service_id = service_id,
            .service_name = service_name,
        },
    );
    broadcast_fn(self.send_buf[0..len]);
}
```

### 6.5 Split-Brain Recovery

When the cluster leader changes (e.g. old leader crashed, new leader emerged via
VRRP-style heartbeat priority), the new leader calls `reEvaluateAllLeaders()`. This
iterates every service with leader election enabled, clears the current leader, and
re-picks the lowest `serviceId`. If the leader changed, it broadcasts
`ServiceLeaderDesignated` to all peers and `LeaderChanged` to all local instances.

This matches the Java `ClusterEventHandler.leaderElected()` → `reEvaluateAllLeaders()`
flow:

```zig
// Called in ClusterEventHandler when a leadership change is detected.

fn reEvaluateAndBroadcast(self: *ClusterEventHandler) void {
    var changes: [256]ServiceLeaderElectionManager.ServiceLeaderChange = undefined;
    const count = self.service_leader_election.reEvaluateAllLeaders(&changes);

    for (changes[0..count]) |change| {
        const result = change.result;

        // Broadcast to peer brokers
        self.cluster_state.broadcastServiceLeaderDesignated(
            result.service_id,
            change.service_name,
            self.local_node_id,
            self.broadcast_admin_fn,
        );

        // Notify local service instances via control ring buffers
        self.control_processor.broadcastLeaderChanged(
            trimServiceName(&change.service_name),
            result.service_id,
            result.node_id,
        );
    }
}
```

Note that **no `ClusterStateSnapshot` is sent on leadership change**. The incremental
`ServiceAdded`/`ServiceRemoved` updates already keep all nodes in sync. The snapshot
is only needed when a *new peer connects* (§5.2), because that peer may have missed
incremental updates while it was offline.

---

## 7. Node Membership

`NodeMembership` tracks all known brokers in the cluster. It is the Zig equivalent of
the Java `NodeMembership` class with its `Int2ObjectHashMap<Node>`.

**Key change from the Java reference:** The `Node` struct includes connection lifecycle
fields (`connection_state`, `last_setup_sent_ns`, `setup_attempt_count`) that were
previously in a separate `PeerConnection` struct. This eliminates a parallel tracking
data structure and ensures all per-node state lives in one place.

```zig
// src/cluster/node_membership.zig

const std = @import("std");

pub const ConnectionState = enum(u8) {
    /// No connection attempt in progress.
    disconnected,
    /// TCP handshake sent, waiting for peer acceptance.
    handshake_sent,
    /// Handshake accepted, traffic can flow.
    connected,
};

pub const Node = struct {
    id: u8,
    /// "host:port" string, null-padded to 22 bytes.
    host_and_port: [22]u8,
    /// Resolved TCP address for this peer. Null for the local node.
    address: ?std.net.Address = null,
    /// True if this is the local broker.
    is_local: bool,
    /// True if this node is the current cluster leader.
    is_leader: bool = false,
    /// Monotonic timestamp of last heartbeat from this node.
    last_heartbeat_ns: i64 = 0,

    // ── Connection lifecycle (merged from PeerConnection) ────────────

    /// Current connection state. Written only on broker-agent thread.
    connection_state: ConnectionState = .disconnected,
    /// Monotonic timestamp of last SETUP attempt (for retry backoff).
    last_setup_sent_ns: i64 = 0,
    /// Number of consecutive SETUP attempts without a successful SM response.
    setup_attempt_count: u32 = 0,

    const SETUP_RETRY_INTERVAL_NS: i64 = 1 * std.time.ns_per_s;

    /// Returns true if a SETUP retry is due.
    pub fn shouldRetrySetup(self: *const Node, now_ns: i64) bool {
        if (self.is_local) return false;
        if (self.connection_state != .setup_sent and
            self.connection_state != .disconnected) return false;
        return (now_ns - self.last_setup_sent_ns) >= SETUP_RETRY_INTERVAL_NS;
    }

    pub fn markSetupSent(self: *Node, now_ns: i64) void {
        self.connection_state = .setup_sent;
        self.last_setup_sent_ns = now_ns;
        self.setup_attempt_count += 1;
    }

    pub fn markConnected(self: *Node, now_ns: i64) void {
        self.connection_state = .connected;
        self.last_heartbeat_ns = now_ns;
        self.setup_attempt_count = 0;
    }

    pub fn markDisconnected(self: *Node) void {
        self.connection_state = .disconnected;
        self.setup_attempt_count = 0;
    }
};

pub const NodeMembership = struct {
    /// Node storage indexed by nodeId. Since nodeId is u8, a flat array
    /// of 256 optional slots is more efficient than a hash map.
    nodes: [256]?Node = [_]?Node{null} ** 256,
    /// nodeId of the local broker (set at init, never changes).
    local_node_id: u8,
    /// Number of active nodes.
    count: u16 = 0,

    pub fn init(local_node_id: u8, local_host_and_port: [22]u8) NodeMembership {
        var self = NodeMembership{
            .local_node_id = local_node_id,
        };
        self.nodes[local_node_id] = .{
            .id = local_node_id,
            .host_and_port = local_host_and_port,
            .is_local = true,
            .connection_state = .connected, // local node is always "connected"
        };
        self.count = 1;
        return self;
    }

    pub fn addNode(self: *NodeMembership, node_id: u8, host_and_port: [22]u8, address: ?std.net.Address) void {
        if (self.nodes[node_id] != null) return; // already known
        self.nodes[node_id] = .{
            .id = node_id,
            .host_and_port = host_and_port,
            .address = address,
            .is_local = false,
        };
        self.count += 1;
    }

    pub fn removeNode(self: *NodeMembership, node_id: u8) void {
        if (self.nodes[node_id]) |_| {
            self.nodes[node_id] = null;
            self.count -= 1;
        }
    }

    pub fn hasNode(self: *const NodeMembership, node_id: u8) bool {
        return self.nodes[node_id] != null;
    }

    pub fn getNode(self: *NodeMembership, node_id: u8) ?*Node {
        if (self.nodes[node_id]) |*node| return node;
        return null;
    }

    pub fn getLocalNode(self: *const NodeMembership) *const Node {
        return &(self.nodes[self.local_node_id].?);
    }

    /// Set the leader flag on the given node. Clears the flag on all others.
    pub fn electLeader(self: *NodeMembership, leader_node_id: u8) void {
        for (&self.nodes) |*slot| {
            if (slot.*) |*node| {
                node.is_leader = (node.id == leader_node_id);
            }
        }
    }

    /// Returns the nodeId of the current leader, or null if no leader is set.
    pub fn getLeader(self: *const NodeMembership) ?u8 {
        for (self.nodes) |slot| {
            if (slot) |node| {
                if (node.is_leader) return node.id;
            }
        }
        return null;
    }

    /// True if the local node is the current cluster leader.
    pub fn isLeader(self: *const NodeMembership) bool {
        if (self.nodes[self.local_node_id]) |node| {
            return node.is_leader;
        }
        return false;
    }

    /// Returns the lowest nodeId among all known nodes.
    pub fn findHighestPriorityNodeId(self: *const NodeMembership) u8 {
        for (self.nodes, 0..) |slot, i| {
            if (slot != null) return @intCast(i);
        }
        return self.local_node_id; // fallback
    }

    /// Iterate all active nodes. Calls `callback` for each.
    pub fn forEach(
        self: *const NodeMembership,
        callback: *const fn (node: *const Node) void,
    ) void {
        for (self.nodes) |slot| {
            if (slot) |*node| {
                callback(node);
            }
        }
    }
};
```

**Why a flat array instead of a hash map:** `nodeId` is `u8` (0–255). A 256-slot array
of `?Node` uses ~256 × `@sizeOf(?Node)` ≈ ~16 KB (slightly larger than before due to
the merged connection fields). It fits in L1/L2 cache, gives O(1) lookups with no
hashing, and is allocation-free after init. The Java reference uses
`Int2ObjectHashMap` because Java lacks value-type arrays with nullable elements — in
Zig the flat array is strictly better.

**Why merge `PeerConnection` into `Node`:** The previous design had two parallel
arrays — `NodeMembership.nodes[256]` and `ClusterEventHandler.peers[256]` — both
indexed by `nodeId`, both tracking the same peer. This meant connection state was in
`PeerConnection` while membership state was in `Node`, requiring coordinated updates
across both. The merged design puts all per-node state in one struct, eliminating an
entire file (`peer_connection.zig`) and the `peers` array from `ClusterEventHandler`.

---

## 8. Broker-to-Broker Heartbeat

Each broker broadcasts an admin `BrokerHeartbeat` (templateId = 1) to all peers at a
regular interval. This serves a dual purpose:

1. **Liveness signal:** The broker-agent thread monitors `Node.last_heartbeat_ns` to
   detect dead peers.
2. **Leadership assertion:** The heartbeat carries the sender's `nodeId`, which is its
   priority for leader election (§3). Receiving a heartbeat from a lower `nodeId`
   triggers leader acceptance.

The heartbeat body includes `host_and_port` so that a broker receiving a heartbeat
from an unknown `nodeId` can register it as a new cluster member without requiring a
prior SETUP handshake for the admin channel.

```zig
// src/cluster/broker_heartbeat.zig

const std = @import("std");
const clock = @import("../platform/clock.zig");
const admin = @import("admin_messages.zig");

pub const BrokerHeartbeatSender = struct {
    local_node_id: u8,
    local_host_and_port: [22]u8,
    next_heartbeat_ns: i64 = 0,
    send_buf: [64]u8 = undefined,
    broadcast_fn: *const fn (buf: []const u8) void,

    /// Interval between broker heartbeats.
    const HEARTBEAT_INTERVAL_NS: i64 = 1 * std.time.ns_per_s;

    pub fn init(
        local_node_id: u8,
        local_host_and_port: [22]u8,
        broadcast_fn: *const fn (buf: []const u8) void,
    ) BrokerHeartbeatSender {
        return .{
            .local_node_id = local_node_id,
            .local_host_and_port = local_host_and_port,
            .broadcast_fn = broadcast_fn,
        };
    }

    /// Duty-cycle function: send heartbeat if interval has elapsed.
    /// Returns 1 if a heartbeat was sent, 0 otherwise.
    pub fn sendIfDue(self: *BrokerHeartbeatSender, now_ns: i64) u32 {
        if (now_ns < self.next_heartbeat_ns) return 0;

        const len = admin.encodeAdminMessage(
            &self.send_buf,
            admin.BrokerHeartbeatBody,
            1, // templateId
            .{
                .node_id = self.local_node_id,
                .host_and_port = self.local_host_and_port,
            },
        );
        self.broadcast_fn(self.send_buf[0..len]);
        self.next_heartbeat_ns = now_ns + HEARTBEAT_INTERVAL_NS;
        return 1;
    }
};
```

**Receiving heartbeats:**

When the receiver event loop gets a `BrokerHeartbeat` admin message, it posts the
source `nodeId`, `host_and_port`, and the current monotonic timestamp to the
broker-agent command queue. The broker-agent thread processes it:

```zig
// Broker-agent command handler (in cluster_event_handler.zig)

/// Handle an inbound BrokerHeartbeat. This is the unified handler for:
/// 1. Liveness tracking (update last_heartbeat_ns)
/// 2. Peer discovery (register unknown nodes)
/// 3. Leader election (VRRP-style priority comparison)
pub fn onBrokerHeartbeat(
    self: *ClusterEventHandler,
    node_id: u8,
    host_and_port: [22]u8,
    now_ns: i64,
) void {
    // 1. Register unknown peers (discovery via heartbeat)
    if (!self.node_membership.hasNode(node_id)) {
        self.node_membership.addNode(node_id, host_and_port, null);
        // Send our state to the new peer
        self.cluster_state.sendClusterStateSnapshot(
            self.service_registry.getLocalInstances(),
            self.local_node_id,
            self.broadcast_admin_fn,
        );
    }

    // 2. Update liveness timestamp
    if (self.node_membership.getNode(node_id)) |node| {
        node.last_heartbeat_ns = now_ns;
    }

    // 3. Leader election via VRRP-style priority comparison
    const result = self.leader_election.onBrokerHeartbeat(node_id, now_ns);
    if (result.changed) {
        self.handleLeaderChange(result.leader.?);
    }
}
```

**Timing relationship:**

```
Heartbeat interval:    1 second
Master-down interval:  3 seconds (3 × heartbeat interval)

A peer must miss 3 consecutive heartbeats before being declared dead
and a new leader potentially elected. This provides tolerance for
temporary packet loss and scheduling jitter.
```

---

## 9. Admin Message Dispatch

Admin messages arrive as DATA frames with the `ADMIN` flag (`0x20`) set. The receiver
event loop detects this flag, decodes the `AdminMessageHeader`, and dispatches by
`templateId`.

Because cluster state mutations must happen on the broker-agent thread, the receiver
event loop does not call `ClusterEventHandler` methods directly. Instead, it
enqueues typed commands on the inter-loop command queue (see doc 10):

```zig
// src/cluster/admin_dispatch.zig

const admin = @import("admin_messages.zig");
const CmdQueue = @import("../threading/cmd_queue.zig").CmdQueue;

/// Admin command variants posted from receiver → broker-agent.
pub const AdminCommand = union(enum) {
    broker_heartbeat: struct { node_id: u8, host_and_port: [22]u8, received_ns: i64 },
    cluster_state_snapshot: struct { data: []const u8 },
    service_added: struct { data: []const u8 },
    service_removed: struct { data: []const u8 },
    service_leader_designated: struct { data: []const u8 },
};

/// Called by the receiver event loop when a DATA frame has the ADMIN flag.
/// Decodes the header, copies the payload, and enqueues a typed command.
pub fn dispatchAdminMessage(
    payload: []const u8,
    cmd_queue: *CmdQueue(AdminCommand),
    now_ns: i64,
) void {
    if (payload.len < @sizeOf(admin.AdminMessageHeader)) return;

    const header: *const admin.AdminMessageHeader = @ptrCast(
        @alignCast(payload.ptr),
    );
    const body = payload[@sizeOf(admin.AdminMessageHeader)..];

    switch (header.template_id) {
        1 => { // BrokerHeartbeat
            const msg: *const admin.BrokerHeartbeatBody = @ptrCast(
                @alignCast(body.ptr),
            );
            cmd_queue.enqueue(.{
                .broker_heartbeat = .{
                    .node_id = msg.node_id,
                    .host_and_port = msg.host_and_port,
                    .received_ns = now_ns,
                },
            }) catch {};
        },
        2 => { // ClusterStateSnapshot (variable-length — copy payload)
            cmd_queue.enqueue(.{
                .cluster_state_snapshot = .{ .data = body },
            }) catch {};
        },
        3 => { // ServiceAdded
            cmd_queue.enqueue(.{
                .service_added = .{ .data = body },
            }) catch {};
        },
        4 => { // ServiceRemoved
            cmd_queue.enqueue(.{
                .service_removed = .{ .data = body },
            }) catch {};
        },
        5 => { // ServiceLeaderDesignated
            cmd_queue.enqueue(.{
                .service_leader_designated = .{ .data = body },
            }) catch {};
        },
        else => {}, // unknown templateId — silently drop
    }
}
```

**Broker-agent side — draining the admin command queue:**

```zig
// Broker-agent duty cycle (in cluster_event_handler.zig)

/// Drain all pending admin commands from the receiver event loop.
/// Returns total work count.
pub fn processAdminCommands(self: *ClusterEventHandler, now_ns: i64) u32 {
    var work_count: u32 = 0;
    while (self.admin_cmd_queue.dequeue()) |cmd| {
        switch (cmd) {
            .broker_heartbeat => |e| self.onBrokerHeartbeat(
                e.node_id,
                e.host_and_port,
                e.received_ns,
            ),
            .cluster_state_snapshot => |e| self.cluster_state.onClusterStateSnapshot(
                e.data,
                self.service_registry,
                self.service_leader_election,
                self.notifySubscribersFn,
            ),
            .service_added => |e| self.cluster_state.onServiceAdded(
                e.data,
                self.service_registry,
                self.service_leader_election,
                self.notifySubscribersFn,
            ),
            .service_removed => |e| self.cluster_state.onServiceRemoved(
                e.data,
                self.service_registry,
                self.service_leader_election,
                self.notifySubscribersFn,
            ),
            .service_leader_designated => |e| self.cluster_state.onServiceLeaderDesignated(
                e.data,
                self.service_leader_election,
                self.control_processor,
            ),
        }
        work_count += 1;
    }

    // Also check master-down timer (VRRP-style election)
    const election_result = self.leader_election.checkMasterDown(now_ns);
    if (election_result.changed) {
        self.handleLeaderChange(election_result.leader.?);
        work_count += 1;
    }

    return work_count;
}
```

---

## 10. ClusterManager Facade

`ClusterManager` is a read-only facade that other subsystems (message routing, control
plane) use to query cluster state. It holds references to `NodeMembership` and
`LeaderElection` but exposes no mutation methods. This matches the Java
`ClusterManager` class.

```zig
// src/cluster/cluster_manager.zig

const NodeMembership = @import("node_membership.zig").NodeMembership;
const Node = @import("node_membership.zig").Node;

pub const ClusterManager = struct {
    cluster_name: []const u8,
    node_membership: *const NodeMembership,
    single_node_cluster: bool,

    pub fn init(
        cluster_name: []const u8,
        node_membership: *const NodeMembership,
        single_node_cluster: bool,
    ) ClusterManager {
        return .{
            .cluster_name = cluster_name,
            .node_membership = node_membership,
            .single_node_cluster = single_node_cluster,
        };
    }

    pub fn hasNode(self: *const ClusterManager, node_id: u8) bool {
        return self.node_membership.hasNode(node_id);
    }

    pub fn hasLeader(self: *const ClusterManager) bool {
        return self.node_membership.getLeader() != null;
    }

    pub fn getLocalNode(self: *const ClusterManager) *const Node {
        return self.node_membership.getLocalNode();
    }

    pub fn getLeader(self: *const ClusterManager) ?u8 {
        return self.node_membership.getLeader();
    }

    pub fn isLeader(self: *const ClusterManager) bool {
        return self.node_membership.isLeader();
    }

    /// True if the cluster has enough members to make progress.
    /// Single-node clusters always have consensus.
    pub fn hasConsensus(self: *const ClusterManager) bool {
        return self.single_node_cluster or self.node_membership.count > 1;
    }
};
```

---

## 11. ClusterEventHandler

`ClusterEventHandler` is the central coordinator that wires together all cluster
subsystems. It is the Zig equivalent of the Java `ClusterEventHandler` class. It
lives on the broker-agent thread and orchestrates reactions to cluster events.

**Key changes from the Java reference:**

1. **No `peers` array.** Connection state is merged into `NodeMembership.Node` (§7).
2. **No function pointer callbacks on `LeaderElection`.** The handler inspects return
   values from `LeaderElection` methods and dispatches downstream logic directly.
3. **Unified heartbeat + election handling** in `onBrokerHeartbeat()` — one method
   handles liveness, peer discovery, and leader election.

```zig
// src/cluster/cluster_event_handler.zig

const std = @import("std");
const LeaderElection = @import("leader_election.zig").LeaderElection;
const NodeMembership = @import("node_membership.zig").NodeMembership;
const ClusterState = @import("cluster_state.zig").ClusterState;
const ServiceLeaderElectionManager = @import("service_leader_election.zig").ServiceLeaderElectionManager;
const BrokerHeartbeatSender = @import("broker_heartbeat.zig").BrokerHeartbeatSender;
const AdminCommand = @import("admin_dispatch.zig").AdminCommand;
const CmdQueue = @import("../threading/cmd_queue.zig").CmdQueue;

pub const ClusterEventHandler = struct {
    // ── Subsystem references (all owned by BrokerContext) ────────────
    leader_election: *LeaderElection,
    node_membership: *NodeMembership,
    cluster_state: *ClusterState,
    service_leader_election: *ServiceLeaderElectionManager,
    service_registry: *ServiceRegistry,
    control_processor: *ControlMessageProcessor,
    heartbeat_sender: *BrokerHeartbeatSender,

    // ── Inter-loop communication ─────────────────────────────────────
    admin_cmd_queue: *CmdQueue(AdminCommand),
    sender_cmd_queue: *CmdQueue(SenderCommand),
    receiver_cmd_queue: *CmdQueue(ReceiverCommand),

    // ── Config ───────────────────────────────────────────────────────
    local_node_id: u8,

    // ── Callbacks ────────────────────────────────────────────────────
    broadcast_admin_fn: *const fn (buf: []const u8) void,
    notifySubscribersFn: *const fn (service_name: []const u8) void,

    // ── Constants ────────────────────────────────────────────────────
    const MASTER_DOWN_INTERVAL_NS: i64 = 3 * std.time.ns_per_s;

    /// Called when a leadership change is detected.
    /// Handles both the NodeMembership update and downstream effects.
    fn handleLeaderChange(self: *ClusterEventHandler, leader_node_id: u8) void {
        self.node_membership.electLeader(leader_node_id);

        if (self.node_membership.isLeader()) {
            // This broker is the new leader — re-evaluate all service leaders
            // to ensure convergence after potential split-brain.
            self.reEvaluateAndBroadcast();
        }
    }

    /// Handle an inbound BrokerHeartbeat. Unified handler for liveness,
    /// peer discovery, and leader election.
    pub fn onBrokerHeartbeat(
        self: *ClusterEventHandler,
        node_id: u8,
        host_and_port: [22]u8,
        now_ns: i64,
    ) void {
        // 1. Register unknown peers (discovery via heartbeat)
        if (!self.node_membership.hasNode(node_id)) {
            self.node_membership.addNode(node_id, host_and_port, null);
            // Send our state to the new peer
            self.cluster_state.sendClusterStateSnapshot(
                self.service_registry.getLocalInstances(),
                self.local_node_id,
                self.broadcast_admin_fn,
            );
        }

        // 2. Update liveness timestamp
        if (self.node_membership.getNode(node_id)) |node| {
            node.last_heartbeat_ns = now_ns;
        }

        // 3. Leader election via VRRP-style priority comparison
        const result = self.leader_election.onBrokerHeartbeat(node_id, now_ns);
        if (result.changed) {
            self.handleLeaderChange(result.leader.?);
        }
    }

    /// Called when a new peer connection is established (SETUP handshake
    /// complete). Sends ClusterStateSnapshot to the new peer.
    pub fn onPeerConnected(self: *ClusterEventHandler, node_id: u8, now_ns: i64) void {
        if (self.node_membership.getNode(node_id)) |node| {
            node.markConnected(now_ns);
        }

        // Send our local state to the new peer
        self.cluster_state.sendClusterStateSnapshot(
            self.service_registry.getLocalInstances(),
            self.local_node_id,
            self.broadcast_admin_fn,
        );
    }

    /// Called on the broker-agent thread when a peer is determined to be dead.
    /// This is the single point of disconnection handling — all downstream
    /// effects flow from here.
    pub fn handlePeerDisconnected(self: *ClusterEventHandler, node_id: u8) void {
        const node = self.node_membership.getNode(node_id) orelse return;
        if (node.connection_state == .disconnected) return;

        // 1. Mark disconnected
        node.markDisconnected();

        // 2. Remove the node from membership
        self.node_membership.removeNode(node_id);

        // 3. Remove all service instances registered on that node
        const removed_count = self.service_registry.removeByNodeId(node_id);
        if (removed_count > 0) {
            self.notifyAffectedSubscribers(node_id);
        }

        // 4. Command receiver event loop to tear down receive log buffer
        self.receiver_cmd_queue.enqueue(.{ .close_peer = node_id }) catch |err| {
            self.error_log.record("Failed to enqueue close_peer command: {}", .{err});
        };

        // 5. Command sender event loop to remove peer from send list
        self.sender_cmd_queue.enqueue(.{ .close_peer = node_id }) catch |err| {
            self.error_log.record("Failed to enqueue close_peer to sender: {}", .{err});
        };

        // 6. Notify leader election of the departure
        const election_result = self.leader_election.onPeerDisconnected(
            node_id,
            @import("../platform/clock.zig").monotonicNanos(),
        );
        if (election_result.changed) {
            // Leader will be re-elected on next checkMasterDown()
        }

        // 7. If we are the broker leader, re-evaluate service leaders
        if (self.leader_election.isLocalNodeLeader()) {
            self.reEvaluateAndBroadcast();
        }
    }

    /// Check all connected peers for heartbeat timeout.
    /// Uses the same interval as the VRRP master-down timer.
    fn checkPeerLiveness(self: *ClusterEventHandler, now_ns: i64) u32 {
        var work_count: u32 = 0;
        for (&self.node_membership.nodes) |*slot| {
            if (slot.*) |*node| {
                if (node.is_local) continue;
                if (node.connection_state != .connected) continue;
                if (now_ns - node.last_heartbeat_ns > MASTER_DOWN_INTERVAL_NS) {
                    self.handlePeerDisconnected(node.id);
                    work_count += 1;
                }
            }
        }
        return work_count;
    }

    // ── Duty cycle ───────────────────────────────────────────────────

    /// Top-level duty-cycle function for cluster management.
    /// Called once per broker-agent iteration.
    pub fn doWork(self: *ClusterEventHandler, now_ns: i64) u32 {
        var work_count: u32 = 0;

        // 1. Drain admin commands from receiver event loop
        //    (includes VRRP-style election check via checkMasterDown)
        work_count += self.processAdminCommands(now_ns);

        // 2. Check peer liveness (heartbeat timeout)
        work_count += self.checkPeerLiveness(now_ns);

        // 3. Send broker heartbeats
        work_count += self.heartbeat_sender.sendIfDue(now_ns);

        // 4. Retry SETUP for disconnected peers
        work_count += self.retrySetups(now_ns);

        return work_count;
    }

    // ── Private ──────────────────────────────────────────────────────

    fn reEvaluateAndBroadcast(self: *ClusterEventHandler) void {
        var changes: [256]ServiceLeaderElectionManager.ServiceLeaderChange = undefined;
        const count = self.service_leader_election.reEvaluateAllLeaders(&changes);

        for (changes[0..count]) |change| {
            const result = change.result;

            self.cluster_state.broadcastServiceLeaderDesignated(
                result.service_id,
                change.service_name,
                self.local_node_id,
                self.broadcast_admin_fn,
            );

            self.control_processor.broadcastLeaderChanged(
                trimServiceName(&change.service_name),
                result.service_id,
                result.node_id,
            );
        }
    }

    fn retrySetups(self: *ClusterEventHandler, now_ns: i64) u32 {
        var work_count: u32 = 0;
        for (&self.node_membership.nodes) |*slot| {
            if (slot.*) |*node| {
                if (node.is_local) continue;
                if (node.shouldRetrySetup(now_ns)) {
                    self.sender_cmd_queue.enqueue(.{
                        .send_setup = .{ .node_id = node.id },
                    }) catch {};
                    node.markSetupSent(now_ns);
                    work_count += 1;
                }
            }
        }
        return work_count;
    }

    // (processAdminCommands implementation shown in §9)
};
```

---

## 12. Integration with Existing Event Loops

The cluster management subsystem does **not** introduce new threads. It plugs into the
two event loops from doc 10:

### 12.1 Broker-Agent Thread

The `ClusterEventHandler.doWork()` function is registered as a duty-cycle function in
the broker-agent event loop. It runs alongside the existing control message processing
and scheduler tasks:

```
Broker-agent duty cycle:
┌──────────────────────────────────────────────────────────────┐
│  1. ControlMessageProcessor.processControlMessages()          │
│  2. Scheduler.runDueTasks()                                   │
│  3. ClusterEventHandler.doWork(now_ns)     ◄── NEW           │
│                                                               │
│  → total_work_count = sum of all                              │
│  → if total_work_count == 0: idle strategy engages            │
└──────────────────────────────────────────────────────────────┘
```

### 12.2 Receiver Event Loop (Routing-Agent Thread)

The receiver event loop is extended to detect the `ADMIN` flag on DATA frames and
route them to `dispatchAdminMessage()` instead of the message router:

```zig
// In receiver event loop — DATA frame handler (doc 06)

fn handleDataFrame(self: *ReceiverEventLoop, frame: *const DataFrameHeader, payload: []const u8) void {
    if (frame.flags & ADMIN_FLAG != 0) {
        // Admin message — dispatch to broker-agent via command queue
        admin_dispatch.dispatchAdminMessage(
            payload,
            &self.admin_cmd_queue,
            clock.monotonicNanos(),
        );
    } else {
        // Application message — route to target service
        self.message_router.route(frame, payload);
    }
}
```

### 12.3 Sender Event Loop

The sender event loop handles outbound admin messages by encoding them as frames
with the `ADMIN` flag set. Admin messages share the same TCP connections as application
messages — no separate connection is needed (unlike the Java reference which uses a
separate Aeron stream ID). The `ADMIN` flag in the frame header is sufficient to
distinguish the two types at the receiver.

### 12.4 Command Flow Diagram

```
                       ┌─────────────────────────┐
                       │    Receiver Event Loop    │
                       │  (routing-agent thread)   │
                       ├───────────────────────────┤
                       │                           │
                       │  TCP read → frame         │
                       │     │                     │
                       │     ├─ ADMIN flag?        │
                       │     │   YES → decode hdr  │
                       │     │   → enqueue command ─┼──── admin_cmd_queue ────┐
                       │     │   NO  → route msg   │                         │
                       │     │                     │                         ▼
                       └─────────────────────────┘               ┌──────────────────┐
                                                                 │  Broker-Agent     │
                       ┌─────────────────────────┐               │  Event Loop       │
                       │    Sender Event Loop      │               ├──────────────────┤
                       │  (routing-agent thread)   │               │                  │
                       ├───────────────────────────┤  sender_cmd   │ drain admin cmds │
                       │                           │◄──── queue ──┤ check master-down│
                       │  dequeue send commands    │               │ check liveness   │
                       │  encode + frame messages  │               │ send heartbeats  │
                       │  submit TCP writes        │               │ reconnect peers  │
                       │                           │               │                  │
                       └───────────────────────────┘               └──────────────────┘
```

---

## 13. Testing

### 13.1 Unit Tests

All unit tests run in a single thread with mock/stub callbacks replacing the network
layer. Pre-allocate small buffers (256–1024 bytes) to exercise edge cases.

**Leader election tests (VRRP-style):**

```zig
// src/cluster/leader_election.zig — test block

test "lowest nodeId wins via heartbeat — 3 nodes" {
    // Given: node 2 is the local node
    var election = LeaderElection.init(2);

    // When: receive heartbeats from nodes 1 and 3
    const now_ns: i64 = 1_000_000_000;
    const result1 = election.onBrokerHeartbeat(3, now_ns);
    const result2 = election.onBrokerHeartbeat(1, now_ns);

    // Then: node 1 should be the leader (lowest nodeId)
    try std.testing.expectEqual(@as(?u8, 1), election.current_leader);
    try std.testing.expect(!election.isLocalNodeLeader());
    try std.testing.expect(result2.changed); // changed when 1 beat 3
}

test "heartbeat from higher nodeId does not change leader" {
    // Given: node 1 is already the leader
    var election = LeaderElection.init(2);
    _ = election.onBrokerHeartbeat(1, 1_000_000_000);

    // When: receive heartbeat from node 3 (worse priority)
    const result = election.onBrokerHeartbeat(3, 2_000_000_000);

    // Then: leader unchanged
    try std.testing.expectEqual(@as(?u8, 1), election.current_leader);
    try std.testing.expect(!result.changed);
}

test "master-down timer triggers self-election" {
    // Given: node 2 is the local node, node 1 was the leader
    var election = LeaderElection.init(2);
    _ = election.onBrokerHeartbeat(1, 1_000_000_000);
    try std.testing.expectEqual(@as(?u8, 1), election.current_leader);

    // When: master-down timer expires (3 seconds, no heartbeat from node 1)
    const result = election.checkMasterDown(5_000_000_000);

    // Then: node 2 self-elects
    try std.testing.expectEqual(@as(?u8, 2), election.current_leader);
    try std.testing.expect(result.changed);
    try std.testing.expect(election.isLocalNodeLeader());
}

test "preemption — lower nodeId heartbeat overrides current leader" {
    // Given: node 3 is the local node, self-elected as leader
    var election = LeaderElection.init(3);
    _ = election.checkMasterDown(5_000_000_000); // self-elects
    try std.testing.expect(election.isLocalNodeLeader());

    // When: node 1 comes back and sends a heartbeat
    const result = election.onBrokerHeartbeat(1, 6_000_000_000);

    // Then: node 1 preempts node 3
    try std.testing.expectEqual(@as(?u8, 1), election.current_leader);
    try std.testing.expect(!election.isLocalNodeLeader());
    try std.testing.expect(result.changed);
}

test "single-node cluster self-elects on master-down" {
    // Given: only node 1, no peers
    var election = LeaderElection.init(1);

    // When: master-down timer expires (no heartbeats from anyone)
    const result = election.checkMasterDown(5_000_000_000);

    // Then: node 1 is the leader
    try std.testing.expectEqual(@as(?u8, 1), election.current_leader);
    try std.testing.expect(election.isLocalNodeLeader());
    try std.testing.expect(result.changed);
}

test "peer disconnection clears leader and triggers re-election" {
    // Given: node 1 was the leader
    var election = LeaderElection.init(2);
    _ = election.onBrokerHeartbeat(1, 1_000_000_000);

    // When: node 1 disconnects
    const result = election.onPeerDisconnected(1, 2_000_000_000);

    // Then: leader is cleared, change flagged
    try std.testing.expectEqual(@as(?u8, null), election.current_leader);
    try std.testing.expect(result.changed);

    // And: next checkMasterDown causes self-election
    const result2 = election.checkMasterDown(2_000_000_000);
    try std.testing.expectEqual(@as(?u8, 2), result2.leader);
    try std.testing.expect(result2.changed);
}

test "heartbeat resets master-down timer" {
    // Given: node 2 is the local node, node 1 is the leader
    var election = LeaderElection.init(2);
    _ = election.onBrokerHeartbeat(1, 1_000_000_000);

    // When: heartbeat received just before timeout
    _ = election.onBrokerHeartbeat(1, 3_500_000_000);

    // Then: master-down timer reset — no election at t=4s
    const result = election.checkMasterDown(4_000_000_000);
    try std.testing.expect(!result.changed);
    try std.testing.expectEqual(@as(?u8, 1), election.current_leader);
}
```

**Service leader election tests:**

```zig
// src/cluster/service_leader_election.zig — test block

test "lowest serviceId wins service leader election" {
    // Given: three instances of "pricing" across two nodes
    var mgr = ServiceLeaderElectionManager.init(std.testing.allocator, 1);
    defer mgr.leader_states.deinit();

    const name = padServiceName("pricing");
    mgr.registerInstance(name, 5, 1); // serviceId=5 on node 1
    mgr.registerInstance(name, 3, 2); // serviceId=3 on node 2
    mgr.registerInstance(name, 7, 1); // serviceId=7 on node 1

    // When: elect leader
    const result = mgr.electLeader(name);

    // Then: serviceId 3 wins (lowest)
    try std.testing.expect(result != null);
    try std.testing.expectEqual(@as(u16, 3), result.?.service_id);
    try std.testing.expectEqual(@as(u8, 2), result.?.node_id);
    try std.testing.expect(result.?.changed);
}

test "re-evaluate after leader removed" {
    // Given: serviceId 3 was the leader
    var mgr = ServiceLeaderElectionManager.init(std.testing.allocator, 1);
    defer mgr.leader_states.deinit();

    const name = padServiceName("pricing");
    mgr.registerInstance(name, 3, 2);
    mgr.registerInstance(name, 5, 1);
    _ = mgr.electLeader(name); // 3 wins

    // When: serviceId 3 is removed
    mgr.unregisterInstance(name, 3);
    const result = mgr.reElectLeader(name);

    // Then: serviceId 5 is the new leader
    try std.testing.expect(result != null);
    try std.testing.expectEqual(@as(u16, 5), result.?.service_id);
    try std.testing.expect(result.?.changed);
}

test "reEvaluateAllLeaders after broker leadership change" {
    // Given: two services with leaders
    var mgr = ServiceLeaderElectionManager.init(std.testing.allocator, 1);
    defer mgr.leader_states.deinit();

    const pricing = padServiceName("pricing");
    const orders = padServiceName("orders");
    mgr.registerInstance(pricing, 3, 2);
    mgr.registerInstance(pricing, 5, 1);
    mgr.registerInstance(orders, 1, 1);
    mgr.registerInstance(orders, 2, 2);
    _ = mgr.electLeader(pricing);
    _ = mgr.electLeader(orders);

    // When: re-evaluate all (simulating new broker leader)
    var changes: [16]ServiceLeaderElectionManager.ServiceLeaderChange = undefined;
    const count = mgr.reEvaluateAllLeaders(&changes);

    // Then: leaders are unchanged (same lowest IDs), so no changes reported
    try std.testing.expectEqual(@as(usize, 0), count);
}
```

**Cluster state tests:**

```zig
test "cluster state snapshot encode and decode roundtrip" {
    // Given: two local service instances
    const instances = [_]RemoteServiceInstance{
        .{ .service_id = 1, .node_id = 1, .service_name = padServiceName("pricing"), .leader_election_enabled = true },
        .{ .service_id = 2, .node_id = 1, .service_name = padServiceName("orders"), .leader_election_enabled = false },
    };

    var buf: [4096]u8 = undefined;
    const len = encodeClusterStateSnapshot(&buf, 1, &instances);

    // When: decode
    // (skip AdminMessageHeader)
    const payload = buf[@sizeOf(admin.AdminMessageHeader)..len];
    const decoded_node_id = payload[0];
    const group_hdr: *const GroupHeader = @ptrCast(@alignCast(payload[1..].ptr));

    // Then: matches input
    try std.testing.expectEqual(@as(u8, 1), decoded_node_id);
    try std.testing.expectEqual(@as(u16, 2), group_hdr.num_in_group);
}

test "snapshot merge adds missing and removes stale instances" {
    // Given: local registry has serviceId=5 from node 2 (stale)
    // Snapshot from node 2 contains only serviceId=3

    // When: merge snapshot

    // Then: serviceId=5 removed, serviceId=3 added
}
```

**Node membership tests:**

```zig
test "addNode and removeNode" {
    var nm = NodeMembership.init(1, padHostPort("localhost:40456"));

    nm.addNode(2, padHostPort("host2:40456"), null);
    try std.testing.expect(nm.hasNode(2));
    try std.testing.expectEqual(@as(u16, 2), nm.count);

    nm.removeNode(2);
    try std.testing.expect(!nm.hasNode(2));
    try std.testing.expectEqual(@as(u16, 1), nm.count);
}

test "electLeader sets leader flag" {
    var nm = NodeMembership.init(1, padHostPort("localhost:40456"));
    nm.addNode(2, padHostPort("host2:40456"), null);

    nm.electLeader(2);
    try std.testing.expectEqual(@as(?u8, 2), nm.getLeader());
    try std.testing.expect(!nm.isLeader()); // local node 1 is not the leader
}

test "findHighestPriorityNodeId returns lowest" {
    var nm = NodeMembership.init(3, padHostPort("localhost:40456"));
    nm.addNode(1, padHostPort("host1:40456"), null);
    nm.addNode(5, padHostPort("host5:40456"), null);

    try std.testing.expectEqual(@as(u8, 1), nm.findHighestPriorityNodeId());
}

test "merged connection state on Node" {
    var nm = NodeMembership.init(1, padHostPort("localhost:40456"));
    nm.addNode(2, padHostPort("host2:40456"), null);

    // Initially disconnected
    const node = nm.getNode(2).?;
    try std.testing.expectEqual(ConnectionState.disconnected, node.connection_state);

    // Mark setup sent
    node.markSetupSent(1_000_000_000);
    try std.testing.expectEqual(ConnectionState.setup_sent, node.connection_state);
    try std.testing.expectEqual(@as(u32, 1), node.setup_attempt_count);

    // Mark connected
    node.markConnected(2_000_000_000);
    try std.testing.expectEqual(ConnectionState.connected, node.connection_state);
    try std.testing.expectEqual(@as(u32, 0), node.setup_attempt_count);
}
```

### 13.2 Integration Tests

Integration tests run two broker processes (or two broker instances in separate
threads with real TCP connections on `127.0.0.1`):

| Test | Scenario | Assertion |
|---|---|---|
| 2-broker connect + election | Start A (nodeId=1), then B (nodeId=2). Wait for heartbeat exchange. | Both brokers have `current_leader == 1` within 3s. |
| Service visibility across brokers | Register "pricing" on broker A. Wait for `ServiceAdded` propagation. | Broker B's registry contains "pricing" with A's nodeId. |
| Broker disconnect re-election | Start A+B. Kill A. Wait for master-down timeout. | B detects A gone, self-elects as leader within 3s. |
| Preemption on rejoin | A+B running, A is leader. Kill A, B self-elects. Restart A. | A preempts B within 1s of first heartbeat. |
| Service leader re-evaluation | Register "pricing" on A and B. A is leader (lower serviceId). Kill A. | B's "pricing" instance becomes the new service leader. |
| State snapshot on peer join | A+B running with services. Disconnect B, then reconnect. | B receives snapshot from A, state converges. |

### 13.3 Testing Tips

1. **Use short timeouts in tests.** Override `MASTER_DOWN_INTERVAL_NS` and
   `HEARTBEAT_INTERVAL_NS` to ~100ms for integration tests. This keeps test suites
   fast.

2. **Test VRRP-style election with all orderings.** Send heartbeats from multiple
   nodes in different orders. Assert that the lowest nodeId always wins regardless
   of arrival order.

3. **Test preemption.** Start with a higher-nodeId leader, then introduce heartbeats
   from a lower nodeId. Assert the leader changes immediately on the next heartbeat.

4. **Mock the broadcast function.** In unit tests, replace `broadcast_admin_fn` with a
   function that records all outbound messages into an `ArrayList`. Assert on the
   sequence and content of messages sent.

5. **Test return values, not callbacks.** Since `LeaderElection` uses return values
   instead of callbacks, unit tests are simpler: call a method, inspect the
   `Result` struct. No need for mock callback setup or `expectCall` assertions.

6. **Test idempotent merges.** Send the same `ServiceAdded` twice. Assert the registry
   contains exactly one entry (not two). Send a `ClusterStateSnapshot` that matches
   the current state. Assert nothing changed.

7. **Single-node cluster is a degenerate case.** Verify that `ClusterEventHandler.doWork()`
   returns 0 immediately when no peers are configured. No heartbeats sent, no
   elections triggered, no admin commands processed.

---

## 14. File Structure

```
src/
  cluster/
    cluster_manager.zig           # Read-only facade for cluster queries
    cluster_event_handler.zig     # Central coordinator (broker-agent thread)
    leader_election.zig           # VRRP-style leader election via heartbeat priority
    node_membership.zig           # Node tracking — u8-indexed flat array (includes connection state)
    cluster_state.zig             # State sync — snapshot + incremental updates
    service_leader_election.zig   # Per-service leader election manager
    admin_messages.zig            # Wire format: AdminMessageHeader + body structs
    admin_dispatch.zig            # Receiver-side dispatch → command queue
    broker_heartbeat.zig          # Periodic admin heartbeat sender (also drives election)
```

**Removed files (compared to old Bully-based design):**
- `peer_connection.zig` — connection state merged into `Node` in `node_membership.zig`.

**Dependency graph (within the cluster subsystem):**

```
admin_messages.zig           ◄── used by all (wire format definitions)
       │
       ▼
admin_dispatch.zig           ◄── receiver event loop
       │
       ▼ (command queue)
cluster_event_handler.zig    ◄── broker-agent event loop (top-level coordinator)
       │
       ├── leader_election.zig         (returns Result values, no callbacks)
       ├── node_membership.zig         (includes Node with connection state)
       ├── cluster_state.zig
       ├── service_leader_election.zig
       └── broker_heartbeat.zig

cluster_manager.zig          ◄── read-only facade (used by message routing, control plane)
       │
       └── node_membership.zig (const pointer)
```

**Cross-module dependencies:**

| This module | Depends on |
|---|---|
| `cluster_event_handler.zig` | `threading/cmd_queue.zig` (doc 10), `control/control_message_processor.zig` (doc 09), `registry/service_registry.zig` (doc 09) |
| `admin_dispatch.zig` | `threading/cmd_queue.zig` (doc 10) |
| `node_membership.zig` | `std` only — no internal dependencies |
| `leader_election.zig` | `platform/clock.zig` (doc 01) — only for `init()` default deadline |
| `admin_messages.zig` | `std` only — no internal dependencies |
| `broker_heartbeat.zig` | `platform/clock.zig` (doc 01) |

---

*Previous: [10 — Threading Model](10-threading-model.md)*
·
*Next: [12 — Configuration & Monitoring](12-configuration-and-monitoring.md)*