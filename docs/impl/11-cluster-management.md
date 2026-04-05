# 11 — Cluster Management

> **Prerequisites:** [09 — Control Plane](09-control-plane.md) (service registration,
> heartbeats, control ring buffer protocol), [04 — UDP Transport & io_uring](04-udp-transport-and-io-uring.md)
> (DATA/SM/SETUP frames, `io_uring` send/recv, wire protocol), [10 — Threading Model](10-threading-model.md)
> (event loops, duty cycles, inter-loop command queues).
>
> **Depended on by:** [12 — Configuration & Monitoring](12-configuration-and-monitoring.md)
> (monitoring counters for cluster state).

This document describes the cluster management subsystem: how brokers discover each
other, establish peer connections, elect a cluster leader, synchronize service state,
and manage per-service leader designation. Every operation in this subsystem runs on one
of the two existing event loop threads (broker-agent or routing-agent) — no additional
threads are introduced.

The design maps directly to the Java reference implementation's `cluster/` and
`admin/` packages (`ClusterManager`, `LeaderElection`, `NodeMembership`,
`ClusterEventHandler`, `ClusterStateManager`, `BrokerAdminPublisher`,
`BrokerAdminSubscriber`, `ServiceLeaderElectionManager`) but replaces Aeron with the
custom UDP transport from doc 04 and SBE flyweights with `packed struct` overlays.

---

## Table of Contents

1. [Overview](#1-overview)
2. [Peer Connection Lifecycle](#2-peer-connection-lifecycle)
   1. [Connection Establishment](#21-connection-establishment)
   2. [PeerConnection](#22-peerconnection)
   3. [Disconnection Detection](#23-disconnection-detection)
3. [Broker Leader Election (Bully Algorithm)](#3-broker-leader-election-bully-algorithm)
   1. [Algorithm](#31-algorithm)
   2. [Admin Message Protocol](#32-admin-message-protocol)
   3. [Admin Message Wire Format](#33-admin-message-wire-format)
   4. [LeaderElection Implementation](#34-leaderelection-implementation)
   5. [Election Timing](#35-election-timing)
4. [Cluster State Synchronization](#4-cluster-state-synchronization)
   1. [State Model](#41-state-model)
   2. [ClusterStateSnapshot](#42-clusterstatesnapshot)
   3. [Handling State Snapshot](#43-handling-state-snapshot)
   4. [Incremental Updates](#44-incremental-updates)
5. [Service Leader Election (Cluster-Wide)](#5-service-leader-election-cluster-wide)
   1. [Policy](#51-policy)
   2. [Triggers](#52-triggers)
   3. [ServiceLeaderElectionManager](#53-serviceleaderelectionmanager)
   4. [Handling ServiceLeaderDesignated](#54-handling-serviceleaderdesignated)
   5. [Split-Brain Recovery](#55-split-brain-recovery)
6. [Node Membership](#6-node-membership)
7. [Broker-to-Broker Heartbeat](#7-broker-to-broker-heartbeat)
8. [Admin Message Dispatch](#8-admin-message-dispatch)
9. [ClusterManager Facade](#9-clustermanager-facade)
10. [ClusterEventHandler](#10-clustereventhandler)
11. [Integration with Existing Event Loops](#11-integration-with-existing-event-loops)
12. [Testing](#12-testing)
13. [File Structure](#13-file-structure)

---

## 1. Overview

A BRZ cluster consists of N broker processes, each running on a separate host, each
identified by a unique `nodeId` (a `u8`, range 0–255). Brokers communicate over
UDP using the wire protocol from doc 04. Every broker maintains a full replica of the
cluster's service registry — which services are running on which nodes — and converges
to a consistent view through an eventually-consistent state synchronization protocol.

Cluster management handles five responsibilities:

| Responsibility | Mechanism | Owner |
|---|---|---|
| **Peer connection lifecycle** | SETUP → SM handshake, heartbeat liveness | Sender + receiver event loops |
| **Broker leader election** | Bully algorithm (lowest `nodeId` wins) | Broker-agent event loop |
| **Cluster state synchronization** | Snapshot on election + incremental updates | Broker-agent event loop |
| **Service leader election** | Lowest `serviceId` wins, managed by broker leader | Broker-agent event loop |
| **Admin message routing** | DATA frames with `ADMIN` flag on admin channel | Both event loops |

**Key invariant:** All cluster state mutations happen on the **broker-agent thread**.
The receiver event loop deserializes inbound admin messages and posts them to the
broker-agent via the inter-loop command queue (see doc 10). This eliminates the need
for any synchronization on cluster data structures.

**Single-node cluster shortcut:** If `broker.member.host.ports` is empty (no peers
configured), the broker auto-elects itself as leader on startup. No SETUP frames,
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
     │  SETUP {nodeId=1, logBufLen=2MB,          │
     │         mtu=1408, initialSeq=0}           │
     ├──────────────────────────────────────────►│
     │                                           │  Allocate receive log buffer
     │                                           │  for nodeId=1
     │                                           │
     │         SM {nodeId=2,                      │
     │             consumption_pos=0,             │
     │             window=recv_log_len/2}         │
     │◄──────────────────────────────────────────┤
     │                                           │
     │  Connection established                    │
     │  send_limit = 0 + window                   │
     │                                           │
     │  ─── admin + message traffic flows ───     │
```

**Steps (executed on the broker-agent thread at startup):**

1. Read peer endpoints from config (`broker.member.host.ports`), which contains a
   comma-separated list of `host:port` pairs.
2. For each peer, allocate a `PeerConnection` struct and resolve the `std.net.Address`.
3. Enqueue a `send_setup` command to the sender event loop (via the sender command
   queue from doc 10).
4. The sender event loop encodes a SETUP frame (see doc 04 §2.3) and submits it as an
   `io_uring` `sendmsg` SQE.
5. When the receiver event loop gets a SETUP from a peer, it allocates a receive log
   buffer, records the peer's `nodeId`, and sends an initial SM back.
6. When the sender event loop gets the SM completion, it sets `send_limit = window`,
   marking the connection as established.
7. The receiver event loop posts a `peer_connected` command to the broker-agent, which
   triggers leader election (§3).

Both sides independently initiate SETUP to each other — the protocol is symmetric.
A broker accepts a SETUP from a peer even if it has already sent its own SETUP to
that peer. The result is two unidirectional channels (one send log per direction),
matching the Java reference's one Aeron `Publication` + one `Subscription` per peer.

### 2.2 PeerConnection

`PeerConnection` is the broker-agent thread's view of a peer broker. It does not
directly touch network I/O — it holds metadata and connection state. The sender and
receiver event loops maintain their own per-peer state (`PeerSender`, `PeerReceiver`
from docs 05/06) keyed by `nodeId`.

```zig
// src/cluster/peer_connection.zig

const std = @import("std");
const clock = @import("../platform/clock.zig");

pub const ConnectionState = enum(u8) {
    /// No connection attempt in progress.
    disconnected,
    /// SETUP frame sent, waiting for SM.
    setup_sent,
    /// SM received, traffic can flow.
    connected,
};

pub const PeerConnection = struct {
    /// Unique broker identifier (from config).
    node_id: u8,
    /// Resolved UDP address for this peer.
    address: std.net.Address,
    /// Current connection state. Written only on broker-agent thread.
    state: ConnectionState = .disconnected,
    /// Monotonic timestamp of last admin heartbeat received from this peer.
    /// Used for liveness detection (§2.3).
    last_heartbeat_received_ns: i64 = 0,
    /// Monotonic timestamp of last SETUP attempt (for retry backoff).
    last_setup_sent_ns: i64 = 0,
    /// Number of consecutive SETUP attempts without a successful SM response.
    setup_attempt_count: u32 = 0,

    const SETUP_RETRY_INTERVAL_NS: i64 = 1 * std.time.ns_per_s;
    const MAX_SETUP_ATTEMPTS: u32 = 0; // 0 = unlimited retries

    pub fn init(node_id: u8, address: std.net.Address) PeerConnection {
        return .{
            .node_id = node_id,
            .address = address,
        };
    }

    /// Returns true if a SETUP retry is due.
    pub fn shouldRetrySend(self: *const PeerConnection, now_ns: i64) bool {
        if (self.state != .setup_sent and self.state != .disconnected) return false;
        return (now_ns - self.last_setup_sent_ns) >= SETUP_RETRY_INTERVAL_NS;
    }

    pub fn markSetupSent(self: *PeerConnection, now_ns: i64) void {
        self.state = .setup_sent;
        self.last_setup_sent_ns = now_ns;
        self.setup_attempt_count += 1;
    }

    pub fn markConnected(self: *PeerConnection, now_ns: i64) void {
        self.state = .connected;
        self.last_heartbeat_received_ns = now_ns;
        self.setup_attempt_count = 0;
    }

    pub fn markDisconnected(self: *PeerConnection) void {
        self.state = .disconnected;
        self.setup_attempt_count = 0;
    }
};
```

The broker-agent thread maintains a fixed-size array of `PeerConnection` pointers,
indexed by `nodeId`. Since `nodeId` is `u8`, the array is at most 256 entries —
allocated once at startup from the page allocator.

### 2.3 Disconnection Detection

A peer is considered disconnected when:

1. **No admin heartbeat received** for `IMAGE_LIVENESS_TIMEOUT` (default 10 seconds).
   The broker-agent thread checks `last_heartbeat_received_ns` during its duty cycle.
2. **Receiver event loop reports a fatal error** for the peer's receive log (CRC
   failures beyond threshold, or the peer sends a TEARDOWN frame).

Both paths funnel through the same handler on the broker-agent thread:

```zig
// src/cluster/cluster_event_handler.zig (partial)

/// Called on the broker-agent thread when a peer is determined to be dead.
/// This is the single point of disconnection handling — all downstream
/// effects flow from here.
pub fn handlePeerDisconnected(self: *ClusterEventHandler, node_id: u8) void {
    const peer = self.peers[node_id] orelse return;
    if (peer.state == .disconnected) return; // already handled

    // 1. Mark disconnected
    peer.markDisconnected();

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

    // 6. Trigger broker leader election
    self.leader_election.triggerElection();

    // 7. If we are the broker leader, re-evaluate service leaders
    //    for any services that had instances on the departed node
    if (self.leader_election.isLocalNodeLeader()) {
        self.service_leader_election.reEvaluateAllLeaders(self);
    }
}
```

**Liveness check (runs as a duty-cycle function on the broker-agent thread):**

```zig
// src/cluster/cluster_event_handler.zig (partial)

const IMAGE_LIVENESS_TIMEOUT_NS: i64 = 10 * std.time.ns_per_s;

/// Duty-cycle function: returns work count (0 or 1).
pub fn checkPeerLiveness(self: *ClusterEventHandler, now_ns: i64) u32 {
    var work_count: u32 = 0;
    for (self.peers) |maybe_peer| {
        const peer = maybe_peer orelse continue;
        if (peer.state != .connected) continue;
        if (now_ns - peer.last_heartbeat_received_ns > IMAGE_LIVENESS_TIMEOUT_NS) {
            self.handlePeerDisconnected(peer.node_id);
            work_count += 1;
        }
    }
    return work_count;
}
```

---

## 3. Broker Leader Election (Bully Algorithm)

### 3.1 Algorithm

The cluster uses a Bully algorithm variant where the **lowest `nodeId` always wins**.
This matches the Java reference's `LeaderElection` class.

```
Trigger: peer connection established or peer disconnected

┌──────────────┐        ┌──────────────┐        ┌──────────────┐
│   Broker A    │        │   Broker B    │        │   Broker C    │
│   nodeId=1    │        │   nodeId=2    │        │   nodeId=3    │
└──────┬────────┘        └──────┬────────┘        └──────┬────────┘
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
       │  ("I have priority")   │                        │
       ├───────────────────────►│                        │
       │  NodeAcknowledgment(1) │                        │
       ├────────────────────────┼───────────────────────►│
       │                        │                        │
       │  (election window expires — 5 seconds)          │
       │                        │                        │
       │  LeaderAnnouncement(1) │                        │
       ├───────────────────────►├───────────────────────►│
       │                        │                        │
       │  ClusterStateSnapshot  │                        │
       ├───────────────────────►├───────────────────────►│
```

**Step-by-step:**

1. **Trigger detected:** A peer connects (SM received) or disconnects (liveness
   timeout). Any broker can initiate.
2. **Broadcast `InitiateElection(myNodeId, myHostPort)`** to all peers via the admin
   channel.
3. **On receiving `InitiateElection(senderNodeId, hostPort)`:**
   - Register the sender as a known cluster member.
   - If `myNodeId < senderNodeId`: send `NodeAcknowledgment(myNodeId)` to that peer —
     this means "I have priority over you."
   - If `myNodeId > senderNodeId`: do nothing (the sender has priority).
   - If no local election is in progress, start one.
4. **Election window:** Wait `ELECTION_WINDOW_NS` (5 seconds) for all acknowledgments
   to arrive.
5. **Decide winner:** When the window expires, each broker independently determines the
   lowest `nodeId` it has seen (including itself). If `lowestSeen == myNodeId`, this
   broker is the leader.
6. **Leader announces:** Broadcast `LeaderAnnouncement(myNodeId)` to all peers.
7. **Non-leaders accept:** On receiving `LeaderAnnouncement`, stop any in-progress
   election and accept the announced leader.
8. **Post-election:** The leader sends `ClusterStateSnapshot` to synchronize state.

### 3.2 Admin Message Protocol

Admin messages use a distinct **admin channel** — a separate logical stream from the
service message channel. In the Java reference, this is an Aeron stream with a
different `streamId` (`broker.admin.stream.id = 100`). In our UDP transport, admin
messages are DATA frames with the `ADMIN` flag (`0x20`) set in the frame header's
flags byte. The receiver event loop checks this flag and dispatches to the admin
handler instead of the message router.

| templateId | Message | Direction | Description |
|:----------:|---------|-----------|-------------|
| 1 | `InitiateElection` | Any → all peers | Start an election round |
| 2 | `NodeAcknowledgment` | Responder → initiator | "I have priority (lower nodeId)" |
| 3 | `LeaderAnnouncement` | Winner → all peers | Declare the election winner |
| 4 | `BrokerHeartbeat` | Any → all peers | Broker liveness keepalive |
| 5 | `ClusterStateSnapshot` | Leader → all peers | Full state sync after election |
| 6 | `ServiceAdded` | Any → all peers | A service registered locally |
| 7 | `ServiceRemoved` | Any → all peers | A service deregistered locally |
| 8 | `ServiceLeaderDesignated` | Leader → all peers | Leader designated for a service |

These template IDs match the Java SBE schema (`broker/src/main/resources/messages.xml`)
exactly.

### 3.3 Admin Message Wire Format

Admin messages are framed inside DATA frames (doc 04) with the `ADMIN` flag. The
payload starts with an 8-byte `AdminMessageHeader` followed by the message-specific
body.

```
Admin message layout (inside DATA frame payload):

Offset   Size   Type     Field
───────────────────────────────────────────────
0        2      u16      block_length     — size of the message body (excluding header)
2        2      u16      template_id      — message type (1–8)
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

/// templateId = 1: InitiateElection
/// Java SBE: nodeId (uint8) + hostAndPort (char[22])
pub const InitiateElectionBody = packed struct {
    node_id: u8,
    host_and_port: [22]u8,
};

/// templateId = 2: NodeAcknowledgment
/// Java SBE: nodeId (uint8)
pub const NodeAcknowledgmentBody = packed struct {
    node_id: u8,
};

/// templateId = 3: LeaderAnnouncement
/// Java SBE: nodeId (uint8)
pub const LeaderAnnouncementBody = packed struct {
    node_id: u8,
};

/// templateId = 4: BrokerHeartbeat
/// Java SBE: nodeId (uint8)
pub const BrokerHeartbeatBody = packed struct {
    node_id: u8,
};

/// templateId = 6: ServiceAdded
/// Java SBE: nodeId (uint8) + serviceId (uint16) + serviceName (char[32])
///           + leaderElectionEnabled (uint8 BooleanType)
pub const ServiceAddedBody = packed struct {
    node_id: u8,
    service_id: u16,
    service_name: [32]u8,
    leader_election_enabled: u8, // 0 = false, 1 = true
};

/// templateId = 7: ServiceRemoved
/// Java SBE: nodeId (uint8) + serviceId (uint16) + serviceName (char[32])
pub const ServiceRemovedBody = packed struct {
    node_id: u8,
    service_id: u16,
    service_name: [32]u8,
};

/// templateId = 8: ServiceLeaderDesignated
/// Java SBE: nodeId (uint8) + serviceId (uint16) + serviceName (char[32])
pub const ServiceLeaderDesignatedBody = packed struct {
    node_id: u8,
    service_id: u16,
    service_name: [32]u8,
};
```

`ClusterStateSnapshot` (templateId = 5) contains a repeating group and is handled
separately (§4.2).

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

### 3.4 LeaderElection Implementation

The `LeaderElection` struct is owned by the broker-agent thread. It tracks election
state and handles inbound election messages posted from the receiver event loop via the
command queue. This maps to the Java `LeaderElection` class and its inner
`ElectionStatus`.

```zig
// src/cluster/leader_election.zig

const std = @import("std");
const clock = @import("../platform/clock.zig");
const admin = @import("admin_messages.zig");

pub const LeaderElection = struct {
    /// This broker's node ID (immutable after init).
    local_node_id: u8,

    /// The currently accepted cluster leader. `null` = no leader known.
    current_leader: ?u8 = null,

    /// Whether an election round is in progress.
    election_in_progress: bool = false,

    /// Monotonic timestamp when the current election round started.
    election_start_ns: i64 = 0,

    /// The lowest nodeId seen so far in this election round (including self).
    /// This is equivalent to `ElectionStatus.currentLeaderId` in the Java code.
    lowest_seen: u8 = std.math.maxInt(u8),

    /// Callback: send an admin message to a specific peer.
    send_admin_fn: *const fn (node_id: u8, buf: []const u8) void,

    /// Callback: broadcast an admin message to all connected peers.
    broadcast_admin_fn: *const fn (buf: []const u8) void,

    /// Callback: invoked when a leader is elected (equivalent to
    /// `ClusterEventHandler.leaderElected()`).
    on_leader_elected_fn: *const fn (leader_node_id: u8) void,

    /// Callback: invoked when a new member joins (equivalent to
    /// `ClusterEventHandler.memberJoined()`).
    on_member_joined_fn: *const fn (node_id: u8, host_and_port: []const u8) void,

    /// Pre-allocated buffer for encoding outbound admin messages.
    /// Large enough for any single admin message.
    send_buf: [256]u8 = undefined,

    const ELECTION_WINDOW_NS: i64 = 5 * std.time.ns_per_s;

    pub fn init(
        local_node_id: u8,
        send_admin_fn: *const fn (u8, []const u8) void,
        broadcast_admin_fn: *const fn ([]const u8) void,
        on_leader_elected_fn: *const fn (u8) void,
        on_member_joined_fn: *const fn (u8, []const u8) void,
    ) LeaderElection {
        return .{
            .local_node_id = local_node_id,
            .send_admin_fn = send_admin_fn,
            .broadcast_admin_fn = broadcast_admin_fn,
            .on_leader_elected_fn = on_leader_elected_fn,
            .on_member_joined_fn = on_member_joined_fn,
        };
    }

    // ── Election initiation ──────────────────────────────────────────

    /// Called when a peer connects or disconnects. Starts a new election
    /// round if one is not already in progress.
    pub fn triggerElection(self: *LeaderElection) void {
        if (!self.election_in_progress) {
            self.lowest_seen = self.local_node_id;
        }
        self.election_in_progress = true;
        self.election_start_ns = clock.monotonicNanos();

        // Broadcast InitiateElection to all peers
        const len = admin.encodeAdminMessage(
            &self.send_buf,
            admin.InitiateElectionBody,
            1, // templateId
            .{
                .node_id = self.local_node_id,
                .host_and_port = encodeHostPort(), // filled from config
            },
        );
        self.broadcast_admin_fn(self.send_buf[0..len]);
    }

    // ── Inbound message handlers ─────────────────────────────────────
    // Called on the broker-agent thread after the receiver event loop
    // posts deserialized admin commands via the inter-loop command queue.

    /// Handle InitiateElection from a peer.
    /// Java equivalent: `LeaderElection.handleInitElection()`.
    pub fn onInitiateElection(self: *LeaderElection, source_node_id: u8, host_and_port: []const u8) void {
        // Register the sender as a cluster member
        self.on_member_joined_fn(source_node_id, host_and_port);

        // Track lowest nodeId seen
        if (source_node_id < self.lowest_seen) {
            self.lowest_seen = source_node_id;
        }

        // Send acknowledgment if we have priority (lower nodeId)
        if (self.local_node_id < source_node_id) {
            const len = admin.encodeAdminMessage(
                &self.send_buf,
                admin.NodeAcknowledgmentBody,
                2, // templateId
                .{ .node_id = self.local_node_id },
            );
            self.send_admin_fn(source_node_id, self.send_buf[0..len]);
        }

        // If we haven't started our own election, start one
        if (!self.election_in_progress) {
            self.triggerElection();
        }
    }

    /// Handle NodeAcknowledgment from a peer.
    /// Java equivalent: `LeaderElection.handleNodeAcknowledgment()`.
    pub fn onNodeAcknowledgment(self: *LeaderElection, node_id: u8) void {
        if (node_id < self.lowest_seen) {
            self.lowest_seen = node_id;
        }
    }

    /// Handle LeaderAnnouncement from a peer.
    /// Java equivalent: `LeaderElection.handleLeaderAnnouncement()`.
    pub fn onLeaderAnnouncement(self: *LeaderElection, leader_node_id: u8) void {
        self.current_leader = leader_node_id;
        self.election_in_progress = false;
        self.on_leader_elected_fn(leader_node_id);
    }

    // ── Duty-cycle check ─────────────────────────────────────────────

    /// Called once per broker-agent duty cycle. Checks if the election
    /// window has expired and, if so, resolves the election.
    /// Returns 1 if an election was resolved, 0 otherwise.
    pub fn checkElectionResult(self: *LeaderElection, now_ns: i64) u32 {
        if (!self.election_in_progress) return 0;

        // Wait for the full election window
        if (now_ns - self.election_start_ns < ELECTION_WINDOW_NS) return 0;

        // Election window expired — lowest nodeId wins
        const leader = self.lowest_seen;
        self.current_leader = leader;
        self.election_in_progress = false;

        if (leader == self.local_node_id) {
            // We are the leader — announce to all peers
            const len = admin.encodeAdminMessage(
                &self.send_buf,
                admin.LeaderAnnouncementBody,
                3, // templateId
                .{ .node_id = self.local_node_id },
            );
            self.broadcast_admin_fn(self.send_buf[0..len]);
        }

        self.on_leader_elected_fn(leader);
        return 1;
    }

    // ── Queries ──────────────────────────────────────────────────────

    pub fn isLocalNodeLeader(self: *const LeaderElection) bool {
        return self.current_leader != null and self.current_leader.? == self.local_node_id;
    }

    pub fn getLeader(self: *const LeaderElection) ?u8 {
        return self.current_leader;
    }

    pub fn isElectionInProgress(self: *const LeaderElection) bool {
        return self.election_in_progress;
    }

    // ── Peer departure ───────────────────────────────────────────────

    /// Called when a peer disconnects. If the departed peer was the
    /// current leader, triggers a new election. Matches the Java
    /// `LeaderElection.memberLeft()` method.
    pub fn memberLeft(self: *LeaderElection, departed_node_id: u8) void {
        const was_leader = self.current_leader != null and
            self.current_leader.? == departed_node_id;
        if (was_leader) {
            self.current_leader = null;
            self.triggerElection();
        }
    }

    // ── Internal ─────────────────────────────────────────────────────

    fn encodeHostPort() [22]u8 {
        // Filled from broker config at init time. Pad with zeros.
        // Implementation reads from BrokerConfig.localHostPort.
        return [_]u8{0} ** 22; // placeholder — real impl copies config string
    }
};
```

### 3.5 Election Timing

The election window is 5 seconds (`ELECTION_WINDOW_NS`), matching the Java
`ELECTION_WINDOW_MILLIS = 5_000`. This is deliberately long — election correctness
depends on all live peers having time to respond, and the election happens only on
topology changes (not on the hot path).

**Timeline for a 3-broker cluster startup:**

```
t=0.0s   Broker A starts, sends SETUP to B and C
t=0.1s   Broker B starts, sends SETUP to A and C
t=0.2s   Broker C starts, sends SETUP to A and B
t=0.3s   A↔B connected (SM received), both trigger election
t=0.4s   A↔C connected, C triggers election
t=0.5s   B↔C connected
         — all brokers exchange InitiateElection + NodeAcknowledgment —
t=5.3s   Election window expires on A (first to start)
         A sees lowest_seen=1 (itself) → announces leadership
t=5.4s   B and C receive LeaderAnnouncement(1), accept A as leader
t=5.4s   A sends ClusterStateSnapshot to B and C
```

---

## 4. Cluster State Synchronization

### 4.1 State Model

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

### 4.2 ClusterStateSnapshot

Sent by the leader (and by each newly joined broker) after an election settles or
when a new peer connects. The Java `BrokerAdminPublisher.sendClusterStateSnapshot()`
encodes an SBE repeating group of services.

In Zig, the snapshot uses a variable-length encoding:

```
ClusterStateSnapshot wire layout (templateId = 5):

AdminMessageHeader (8 bytes)
├── block_length: 5    (fixed fields below)
├── template_id:  5
├── schema_id:    688
└── version:      1

Fixed fields (5 bytes):
├── node_id:       u8     (the sender's nodeId)
├── group header:  4 bytes
│   ├── block_length: u16  (size of each group entry)
│   └── num_in_group:  u16  (number of service entries)

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
        .template_id = 5,
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

**Sending (called by the new leader after election or when a peer joins):**

```zig
// src/cluster/cluster_state.zig (continued)

/// Send our local service instances as a snapshot to all peers (or one peer).
/// Called on the broker-agent thread.
pub fn sendClusterStateSnapshot(
    self: *ClusterState,
    local_instances: []const RemoteServiceInstance,
    local_node_id: u8,
    broadcast_fn: *const fn (buf: []const u8) void,
) void {
    if (local_instances.len == 0) return;

    const len = encodeClusterStateSnapshot(
        &self.snapshot_buf,
        local_node_id,
        local_instances,
    );
    broadcast_fn(self.snapshot_buf[0..len]);
}
```

### 4.3 Handling State Snapshot

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

### 4.4 Incremental Updates

After the initial snapshot exchange, service additions and removals are propagated
incrementally. Each broker broadcasts updates for its own local services only.

**ServiceAdded (templateId = 6):**

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
        6, // templateId
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

**ServiceRemoved (templateId = 7):**

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

## 5. Service Leader Election (Cluster-Wide)

### 5.1 Policy

Service leader election is an optional, per-service feature that designates exactly one
instance of a named service as the "leader" across the entire cluster. The rule is
**lowest `serviceId` wins** — effectively a first-registered-wins policy, because
service IDs are monotonically incremented by the broker.

This maps directly to the Java `ServiceLeaderElectionManager`.

A service opts in by setting `leaderElectionEnabled = true` in its registration message
(`brz.service.leader_election.enabled = true` in config). If not opted in, no leader
is tracked for that service.

### 5.2 Triggers

Service leader evaluation is invoked on the broker-agent thread when:

| Event | Action |
|---|---|
| Service registered with `leaderElectionEnabled = true` | Evaluate leader for that service name |
| Service removed (was leader or had election enabled) | Re-evaluate leader for that service name |
| Broker leadership changes (new broker leader elected) | Re-evaluate all service leaders |
| Cluster state snapshot received from a peer | Register instances, then evaluate affected services |

### 5.3 ServiceLeaderElectionManager

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

### 5.4 Handling ServiceLeaderDesignated

When a non-leader broker receives `ServiceLeaderDesignated` (templateId = 8) from the
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
        8, // templateId
        .{
            .node_id = local_node_id,
            .service_id = service_id,
            .service_name = service_name,
        },
    );
    broadcast_fn(self.send_buf[0..len]);
}
```

### 5.5 Split-Brain Recovery

When the cluster leader changes (e.g. old leader crashed, new leader elected via
Bully algorithm), the new leader calls `reEvaluateAllLeaders()`. This iterates every
service with leader election enabled, clears the current leader, and re-picks the
lowest `serviceId`. If the leader changed, it broadcasts `ServiceLeaderDesignated`
to all peers and `LeaderChanged` to all local instances.

This matches the Java `ClusterEventHandler.leaderElected()` → `reEvaluateAllLeaders()`
flow:

```zig
// Called in ClusterEventHandler.onLeaderElected() when this broker is the new leader.

fn reEvaluateAndBroadcast(
    self: *ClusterEventHandler,
) void {
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

---

## 6. Node Membership

`NodeMembership` tracks all known brokers in the cluster. It is the Zig equivalent of
the Java `NodeMembership` class with its `Int2ObjectHashMap<Node>`.

```zig
// src/cluster/node_membership.zig

const std = @import("std");

pub const Node = struct {
    id: u8,
    /// "host:port" string, null-padded to 22 bytes.
    host_and_port: [22]u8,
    /// True if this is the local broker.
    is_local: bool,
    /// True if this node is the current cluster leader.
    is_leader: bool = false,
    /// Monotonic timestamp of last heartbeat from this node.
    last_heartbeat_ns: i64 = 0,
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
        };
        self.count = 1;
        return self;
    }

    pub fn addNode(self: *NodeMembership, node_id: u8, host_and_port: [22]u8) void {
        if (self.nodes[node_id] != null) return; // already known
        self.nodes[node_id] = .{
            .id = node_id,
            .host_and_port = host_and_port,
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

    /// Returns the lowest nodeId among all known nodes (for election).
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
of `?Node` uses ~256 × `@sizeOf(?Node)` ≈ ~10 KB. It fits in L1 cache, gives O(1)
lookups with no hashing, and is allocation-free after init. The Java reference uses
`Int2ObjectHashMap` because Java lacks value-type arrays with nullable elements — in
Zig the flat array is strictly better.

---

## 7. Broker-to-Broker Heartbeat

Each broker broadcasts an admin `BrokerHeartbeat` (templateId = 4) to all peers at a
regular interval. This serves as the liveness signal that `checkPeerLiveness()` (§2.3)
monitors.

```zig
// src/cluster/broker_heartbeat.zig

const std = @import("std");
const clock = @import("../platform/clock.zig");
const admin = @import("admin_messages.zig");

pub const BrokerHeartbeatSender = struct {
    local_node_id: u8,
    next_heartbeat_ns: i64 = 0,
    send_buf: [64]u8 = undefined,
    broadcast_fn: *const fn (buf: []const u8) void,

    /// Interval between broker heartbeats. Matches the Java reference's
    /// 1-second scheduler interval for admin subscriber polling.
    const HEARTBEAT_INTERVAL_NS: i64 = 1 * std.time.ns_per_s;

    pub fn init(
        local_node_id: u8,
        broadcast_fn: *const fn (buf: []const u8) void,
    ) BrokerHeartbeatSender {
        return .{
            .local_node_id = local_node_id,
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
            4, // templateId
            .{ .node_id = self.local_node_id },
        );
        self.broadcast_fn(self.send_buf[0..len]);
        self.next_heartbeat_ns = now_ns + HEARTBEAT_INTERVAL_NS;
        return 1;
    }
};
```

**Receiving heartbeats:**

When the receiver event loop gets a `BrokerHeartbeat` admin message, it posts the
source `nodeId` and the current monotonic timestamp to the broker-agent command queue.
The broker-agent thread updates `PeerConnection.last_heartbeat_received_ns`:

```zig
// Broker-agent command handler (in cluster_event_handler.zig)

pub fn onBrokerHeartbeat(self: *ClusterEventHandler, node_id: u8, now_ns: i64) void {
    if (self.peers[node_id]) |peer| {
        peer.last_heartbeat_received_ns = now_ns;
    }
}
```

**Timing relationship:**

```
Heartbeat interval:  1 second
Liveness timeout:   10 seconds

A peer must miss 10 consecutive heartbeats before being declared dead.
This provides tolerance for temporary packet loss and scheduling jitter.
```

---

## 8. Admin Message Dispatch

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
    initiate_election: struct { node_id: u8, host_and_port: [22]u8 },
    node_acknowledgment: struct { node_id: u8 },
    leader_announcement: struct { leader_node_id: u8 },
    broker_heartbeat: struct { node_id: u8, received_ns: i64 },
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
        1 => { // InitiateElection
            const msg: *const admin.InitiateElectionBody = @ptrCast(
                @alignCast(body.ptr),
            );
            cmd_queue.enqueue(.{
                .initiate_election = .{
                    .node_id = msg.node_id,
                    .host_and_port = msg.host_and_port,
                },
            }) catch {};
        },
        2 => { // NodeAcknowledgment
            const msg: *const admin.NodeAcknowledgmentBody = @ptrCast(
                @alignCast(body.ptr),
            );
            cmd_queue.enqueue(.{
                .node_acknowledgment = .{ .node_id = msg.node_id },
            }) catch {};
        },
        3 => { // LeaderAnnouncement
            const msg: *const admin.LeaderAnnouncementBody = @ptrCast(
                @alignCast(body.ptr),
            );
            cmd_queue.enqueue(.{
                .leader_announcement = .{ .leader_node_id = msg.node_id },
            }) catch {};
        },
        4 => { // BrokerHeartbeat
            const msg: *const admin.BrokerHeartbeatBody = @ptrCast(
                @alignCast(body.ptr),
            );
            cmd_queue.enqueue(.{
                .broker_heartbeat = .{
                    .node_id = msg.node_id,
                    .received_ns = now_ns,
                },
            }) catch {};
        },
        5 => { // ClusterStateSnapshot (variable-length — copy payload)
            cmd_queue.enqueue(.{
                .cluster_state_snapshot = .{ .data = body },
            }) catch {};
        },
        6 => { // ServiceAdded
            cmd_queue.enqueue(.{
                .service_added = .{ .data = body },
            }) catch {};
        },
        7 => { // ServiceRemoved
            cmd_queue.enqueue(.{
                .service_removed = .{ .data = body },
            }) catch {};
        },
        8 => { // ServiceLeaderDesignated
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
            .initiate_election => |e| self.leader_election.onInitiateElection(
                e.node_id,
                &e.host_and_port,
            ),
            .node_acknowledgment => |e| self.leader_election.onNodeAcknowledgment(e.node_id),
            .leader_announcement => |e| self.leader_election.onLeaderAnnouncement(e.leader_node_id),
            .broker_heartbeat => |e| self.onBrokerHeartbeat(e.node_id, e.received_ns),
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

    // Also check election timeout
    work_count += self.leader_election.checkElectionResult(now_ns);

    return work_count;
}
```

---

## 9. ClusterManager Facade

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

## 10. ClusterEventHandler

`ClusterEventHandler` is the central coordinator that wires together all cluster
subsystems. It is the Zig equivalent of the Java `ClusterEventHandler` class. It
lives on the broker-agent thread and orchestrates reactions to cluster events.

```zig
// src/cluster/cluster_event_handler.zig

const std = @import("std");
const LeaderElection = @import("leader_election.zig").LeaderElection;
const NodeMembership = @import("node_membership.zig").NodeMembership;
const ClusterState = @import("cluster_state.zig").ClusterState;
const ServiceLeaderElectionManager = @import("service_leader_election.zig").ServiceLeaderElectionManager;
const PeerConnection = @import("peer_connection.zig").PeerConnection;
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

    // ── Peer tracking ────────────────────────────────────────────────
    peers: [256]?*PeerConnection = [_]?*PeerConnection{null} ** 256,

    // ── Inter-loop communication ─────────────────────────────────────
    admin_cmd_queue: *CmdQueue(AdminCommand),
    sender_cmd_queue: *CmdQueue(SenderCommand),
    receiver_cmd_queue: *CmdQueue(ReceiverCommand),

    // ── Config ───────────────────────────────────────────────────────
    local_node_id: u8,

    // ── Callbacks ────────────────────────────────────────────────────
    broadcast_admin_fn: *const fn (buf: []const u8) void,
    notifySubscribersFn: *const fn (service_name: []const u8) void,

    /// Called when the broker leader election settles.
    /// Equivalent to Java `ClusterEventHandler.leaderElected()`.
    pub fn onLeaderElected(self: *ClusterEventHandler, leader_node_id: u8) void {
        self.node_membership.electLeader(leader_node_id);

        if (self.node_membership.isLeader()) {
            // This broker is the new leader — re-evaluate all service leaders
            // to ensure convergence after potential split-brain.
            self.reEvaluateAndBroadcast();
        }
    }

    /// Called when a new peer broker connects.
    /// Equivalent to Java `ClusterEventHandler.memberJoined()`.
    pub fn onMemberJoined(self: *ClusterEventHandler, node_id: u8, host_and_port: []const u8) void {
        var hp: [22]u8 = [_]u8{0} ** 22;
        const copy_len = @min(host_and_port.len, 22);
        @memcpy(hp[0..copy_len], host_and_port[0..copy_len]);

        self.node_membership.addNode(node_id, hp);

        // Send our local state to the new peer
        self.cluster_state.sendClusterStateSnapshot(
            self.service_registry.getLocalInstances(),
            self.local_node_id,
            self.broadcast_admin_fn,
        );
    }

    /// Called when a peer broker disconnects (heartbeat timeout).
    /// Equivalent to Java `ClusterEventHandler.memberLeft()`.
    pub fn onMemberLeft(self: *ClusterEventHandler, node_id: u8) void {
        self.handlePeerDisconnected(node_id);
    }

    // ── Duty cycle ───────────────────────────────────────────────────

    /// Top-level duty-cycle function for cluster management.
    /// Called once per broker-agent iteration.
    pub fn doWork(self: *ClusterEventHandler, now_ns: i64) u32 {
        var work_count: u32 = 0;

        // 1. Drain admin commands from receiver event loop
        work_count += self.processAdminCommands(now_ns);

        // 2. Check election timeout
        work_count += self.leader_election.checkElectionResult(now_ns);

        // 3. Check peer liveness
        work_count += self.checkPeerLiveness(now_ns);

        // 4. Send broker heartbeats
        work_count += self.heartbeat_sender.sendIfDue(now_ns);

        // 5. Retry SETUP for disconnected peers
        work_count += self.retrySetups(now_ns);

        return work_count;
    }

    // (private methods: processAdminCommands, checkPeerLiveness,
    //  handlePeerDisconnected, reEvaluateAndBroadcast, retrySetups
    //  — implementations shown in earlier sections)
};
```

---

## 11. Integration with Existing Event Loops

The cluster management subsystem does **not** introduce new threads. It plugs into the
two event loops from doc 10:

### 11.1 Broker-Agent Thread

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

### 11.2 Receiver Event Loop (Routing-Agent Thread)

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

### 11.3 Sender Event Loop

The sender event loop handles outbound admin messages by encoding them as DATA frames
with the `ADMIN` flag set. Admin messages share the same UDP socket as application
messages — no separate socket is needed (unlike the Java reference which uses a
separate Aeron stream ID). The `ADMIN` flag in the frame header is sufficient to
distinguish the two types at the receiver.

### 11.4 Command Flow Diagram

```
                       ┌─────────────────────────┐
                       │    Receiver Event Loop    │
                       │  (routing-agent thread)   │
                       ├───────────────────────────┤
                       │                           │
                       │  UDP recv → DATA frame    │
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
                       │                           │◄──── queue ──┤ check election   │
                       │  dequeue send commands    │               │ check liveness   │
                       │  encode DATA+ADMIN flag   │               │ send heartbeats  │
                       │  submit io_uring sendmsg  │               │ retry SETUPs     │
                       │                           │               │                  │
                       └───────────────────────────┘               └──────────────────┘
```

---

## 12. Testing

### 12.1 Unit Tests

All unit tests run in a single thread with mock/stub callbacks replacing the network
layer. Pre-allocate small buffers (256–1024 bytes) to exercise edge cases.

**Leader election tests:**

```zig
// src/cluster/leader_election.zig — test block

test "lowest nodeId wins election — 3 nodes" {
    // Given: node 2 is the local node
    var sent_messages = std.ArrayList(SentMessage).init(std.testing.allocator);
    defer sent_messages.deinit();
    var elected_leader: ?u8 = null;

    var election = LeaderElection.init(
        2, // local_node_id
        makeSendFn(&sent_messages),
        makeBroadcastFn(&sent_messages),
        makeElectedFn(&elected_leader),
        makeJoinedFn(),
    );

    // When: election triggered, then receive InitiateElection from nodes 1 and 3
    election.triggerElection();
    election.onInitiateElection(1, "host1:40456\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00");
    election.onNodeAcknowledgment(1);
    election.onInitiateElection(3, "host3:40456\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00");

    // Simulate window expiry
    const future_ns = std.time.ns_per_s * 10;
    _ = election.checkElectionResult(future_ns);

    // Then: node 1 should be the leader (lowest nodeId)
    try std.testing.expectEqual(@as(?u8, 1), election.current_leader);
    try std.testing.expect(!election.isLocalNodeLeader());
}

test "election after node disconnect — leader re-election" {
    // Given: node 1 was the leader
    var election = LeaderElection.init(2, ...);
    election.current_leader = 1;

    // When: node 1 disconnects
    election.memberLeft(1);

    // Then: election is in progress
    try std.testing.expect(election.election_in_progress);
    try std.testing.expectEqual(@as(?u8, null), election.current_leader);
}

test "single-node cluster auto-elects self" {
    // Given: only node 1, no peers
    var elected_leader: ?u8 = null;
    var election = LeaderElection.init(1, ...);

    election.triggerElection();
    const future_ns = std.time.ns_per_s * 10;
    _ = election.checkElectionResult(future_ns);

    // Then: node 1 is the leader
    try std.testing.expectEqual(@as(?u8, 1), election.current_leader);
    try std.testing.expect(election.isLocalNodeLeader());
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

    nm.addNode(2, padHostPort("host2:40456"));
    try std.testing.expect(nm.hasNode(2));
    try std.testing.expectEqual(@as(u16, 2), nm.count);

    nm.removeNode(2);
    try std.testing.expect(!nm.hasNode(2));
    try std.testing.expectEqual(@as(u16, 1), nm.count);
}

test "electLeader sets leader flag" {
    var nm = NodeMembership.init(1, padHostPort("localhost:40456"));
    nm.addNode(2, padHostPort("host2:40456"));

    nm.electLeader(2);
    try std.testing.expectEqual(@as(?u8, 2), nm.getLeader());
    try std.testing.expect(!nm.isLeader()); // local node 1 is not the leader
}

test "findHighestPriorityNodeId returns lowest" {
    var nm = NodeMembership.init(3, padHostPort("localhost:40456"));
    nm.addNode(1, padHostPort("host1:40456"));
    nm.addNode(5, padHostPort("host5:40456"));

    try std.testing.expectEqual(@as(u8, 1), nm.findHighestPriorityNodeId());
}
```

### 12.2 Integration Tests

Integration tests run two broker processes (or two broker instances in separate
threads with real UDP sockets on `127.0.0.1`):

| Test | Scenario | Assertion |
|---|---|---|
| 2-broker connect + election | Start A (nodeId=1), then B (nodeId=2). Wait for SETUP handshake. | Both brokers have `current_leader == 1` within 10s. |
| Service visibility across brokers | Register "pricing" on broker A. Wait for `ServiceAdded` propagation. | Broker B's registry contains "pricing" with A's nodeId. |
| Broker disconnect re-election | Start A+B. Kill A. Wait for liveness timeout. | B detects A gone, re-elects self as leader. |
| Service leader re-evaluation | Register "pricing" on A and B. A is leader (lower serviceId). Kill A. | B's "pricing" instance becomes the new service leader. |
| State snapshot on rejoin | A+B running with services. Disconnect B, then reconnect. | B receives snapshot from A, state converges. |

### 12.3 Testing Tips

1. **Use short timeouts in tests.** Override `IMAGE_LIVENESS_TIMEOUT_NS` and
   `ELECTION_WINDOW_NS` to ~100ms for integration tests. This keeps test suites fast.

2. **Test election with all orderings.** The Bully algorithm's correctness depends on
   messages arriving within the election window regardless of order. Shuffle
   `InitiateElection` and `NodeAcknowledgment` arrival order in unit tests.

3. **Mock the broadcast function.** In unit tests, replace `broadcast_admin_fn` with a
   function that records all outbound messages into an `ArrayList`. Assert on the
   sequence and content of messages sent.

4. **Test idempotent merges.** Send the same `ServiceAdded` twice. Assert the registry
   contains exactly one entry (not two). Send a `ClusterStateSnapshot` that matches
   the current state. Assert nothing changed.

5. **Single-node cluster is a degenerate case.** Verify that `ClusterEventHandler.doWork()`
   returns 0 immediately when no peers are configured. No heartbeats sent, no elections
   triggered, no admin commands processed.

---

## 13. File Structure

```
src/
  cluster/
    cluster_manager.zig           # Read-only facade for cluster queries
    cluster_event_handler.zig     # Central coordinator (broker-agent thread)
    leader_election.zig           # Bully algorithm — broker leader election
    node_membership.zig           # Node tracking — u8-indexed flat array
    cluster_state.zig             # State sync — snapshot + incremental updates
    service_leader_election.zig   # Per-service leader election manager
    admin_messages.zig            # Wire format: AdminMessageHeader + body structs
    admin_dispatch.zig            # Receiver-side dispatch → command queue
    peer_connection.zig           # Per-peer connection state + lifecycle
    broker_heartbeat.zig          # Periodic admin heartbeat sender
```

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
       ├── leader_election.zig
       ├── node_membership.zig
       ├── cluster_state.zig
       ├── service_leader_election.zig
       ├── peer_connection.zig
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
| `peer_connection.zig` | `platform/clock.zig` (doc 01) |
| `admin_messages.zig` | `std` only — no internal dependencies |
| `broker_heartbeat.zig` | `platform/clock.zig` (doc 01) |

---

*Previous: [10 — Threading Model](10-threading-model.md)*
·
*Next: [12 — Configuration & Monitoring](12-configuration-and-monitoring.md)*