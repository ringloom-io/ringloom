# Clustering and Leadership

RingLoom clustering is a routing and discovery mesh. It synchronizes peer liveness,
service instance presence, service leadership, and broker leadership across nodes.
It is not a replicated log or consensus state machine.

## Cluster traffic

Broker-to-broker cluster traffic uses Aeron UDP admin streams. Admin messages cover:

1. Broker heartbeats.
2. Service-added announcements.
3. Service-removed announcements.
4. Service leader designation.
5. Capacity and flow-control advisories.
6. Peer liveness transitions.

The receiver loop polls admin UDP, validates admin envelopes, and posts commands to
the control loop. The control loop mutates registries and broadcasts derived state.

## Peer membership

Broker config lists peer endpoints. At runtime, each broker tracks peer liveness from
broker heartbeats and Aeron/admin receive activity. A peer can be alive, suspect, or
dead based on heartbeat age.

When a peer is considered unavailable, services hosted by that peer are removed or
marked unavailable in discovery snapshots so clients stop choosing them.

## Broker leader election

Broker leader election is VRRP-style:

1. The broker with the lowest alive `node_id` is leader.
2. Broker heartbeats are leadership claims.
3. A heartbeat from a lower node ID preempts the current leader.
4. If the current leader's master-down timer expires, the local broker can
   self-elect until a lower node heartbeat is observed.

There is no election term, vote round, quorum, or replicated log. This leadership is
for lightweight cluster coordination and observability.

## Service leadership

Service leadership is separate from broker leadership. Multiple instances of the
same service name can exist across nodes. The control plane designates one service
instance as leader and publishes updates to interested clients.

Service clients use `sendToLeader` or equivalent APIs when an application needs an
active/standby pattern. If no leader is known, leader-routed sends fail explicitly.

## Discovery propagation

Local service registration updates the local broker's service registry. The broker:

1. Sends registration response to the service over the service control ring.
2. Updates local discovery snapshots for local clients.
3. Broadcasts service-added admin messages to peer brokers.
4. Recomputes service leadership if needed.

Remote service-added messages update peer service entries and flow into local
service discovery snapshots. The reverse path applies to graceful unregister,
heartbeat timeout, or peer loss.

## Failure behavior

RingLoom remains best-effort. If a broker or service crashes, in-flight messages may
be lost. The cluster layer eventually removes stale services through heartbeat and
peer liveness timeouts, then publishes discovery updates so clients can route around
the failure.
