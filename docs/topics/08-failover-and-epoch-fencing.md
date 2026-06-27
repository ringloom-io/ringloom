# 08 — Topic Leadership, Failover & Epoch Fencing

**Goal:** Elect a topic leader **among topics-enabled brokers** (decoupled from the cluster master),
keep topics available across topic-leadership changes, and guarantee the single-writer invariant via a
monotonic `leader_epoch` plus a **failover catch-up barrier** that prevents truncating acked data.
**Modules:** `topics/topic_leader_election.zig`, `topic_engine.zig`, `topic_admin.zig`, `control_loop.zig`.
**Depends on:** 05, 06.

> Locked decisions: a **single topic leader** (lowest-nodeId topics-enabled broker) sequences ALL
> topics; AP (no quorum); ack modes `fire_and_forget` / `replicate_once`; **full-mesh replication**.
> This spec makes failover **safe** (no divergence) and **bounded** (only the un-acked tail beyond the
> most-advanced surviving replica can be lost; `replicate_once`-acked data is preserved).

---

## 1. Topic leader election (separate from the cluster master)

The cluster master (`cluster/leader_election.zig`) may have topics **disabled**, so topic leadership
runs as its own election restricted to the **topics-enabled** candidate set:

```zig
// topics/topic_leader_election.zig — reuses the lowest-nodeId-alive rule of leader_election.zig,
// but the candidate set is "brokers that advertise topics.enabled".
pub const TopicLeaderElection = struct {
    local_node_id: u8,
    topics_enabled_peers: NodeSet,   // learned from heartbeats carrying a topics_enabled bit
    current_topic_leader: ?u8 = null,
    leader_term: u64 = 0,            // monotonic; bumped on every accepted change
};
```

- Brokers advertise a **`topics_enabled` bit** on their existing broker heartbeat / membership frame.
  The candidate set for topic leadership is exactly the alive nodes with that bit set.
- Rule: **lowest-nodeId alive topics-enabled broker wins** — identical algorithm to
  `leader_election.zig`, just over the filtered set. We reuse its `onBrokerHeartbeat` /
  `checkMasterDown` / `onPeerDisconnected` logic (factor the comparison so the candidate set is a
  parameter) rather than duplicating it.
- **`leader_term` (epoch)** is monotonic: piggyback `leader_term` on heartbeats; a node adopts
  `max(seen_term)`; a node that *becomes* topic leader sets `leader_term = max(seen_term) + 1`. Stale
  leaders necessarily carry a lower term than the node that superseded them.
- A single topic leader ⇒ **one epoch value shared by all topics** it leads.

> Best-effort monotonicity, not consensus. During a partition two nodes can briefly cross terms;
> fencing (§3) + the catch-up barrier (§2) still prevent queue divergence and acked-data loss.

## 2. Failover sequence (topics-enabled node N becomes topic leader)

Because replication is **full mesh**, N already holds a replica of every topic. The critical addition
is the **catch-up barrier**: N must sync each topic to the most-advanced surviving replica **before**
accepting any write to it.

```
1. Topic election marks N the topic leader; N sets leader_term = max(seen)+1 = E.
2. N broadcasts admin TopicLeaderChanged{topic_id=0 (all), new_leader=N, leader_epoch=E}.
3. CATCH-UP BARRIER — for each topic T, BEFORE accepting any write to T:
   a. N marks store[T].accepting_writes = false.
   b. N broadcasts TopicAppliedQuery{T}; every surviving topics-enabled peer replies
      TopicAppliedReply{T, node, last_applied_index}. N includes its own local replica tip.
   c. N picks the peer P with the MAX last_applied_index (the most-advanced replica). If that is N
      itself, no pull is needed.
   d. If P != N and P's index > N's: N runs a sink against P (catchup) until N's replica reaches
      P's tip. (N already has a full-mesh replica, so this is a short catchup, not a full_replay.)
   e. N promotes its (now most-advanced) replica to master IN PLACE: opens an Appender and continues
      the index space from that tip. Sets store[T].epoch = E, accepting_writes = true.
4. Only now does N accept publishes for T and serve sources to the other replicas.
5. Producers receive TopicLeaderChanged (broker→service, spec 03 template 13), re-target N, and
   stamp subsequent publishes with epoch E.
6. Other replicas observe the higher epoch / leader-changed, tear down their old sink session, and
   re-HELLO against N. ringloom-queue chooses catchup/live from their last_applied_index. A replica
   that was AHEAD of N's chosen tip is reconciled by sink-ahead reset (§4) — but note N chose the
   global max, so this can only happen for writes that were never acked anywhere reachable.
```

The barrier guarantees N's accepted index space is a **superset of every `replicate_once`-acked
message** (an acked message is, by definition, on ≥1 replica, and N synced to the max replica). Only
the un-acked tip beyond the most-advanced reachable replica may be lost.

### 2.1 Index continuity on promotion

A promoted replica continues appending **after** the chosen (max) `last_applied_index`. Because the
replica is byte-exact with the old master up to that index, the new master's index space is a clean
prefix-extension. Replicas ahead of that point are handled by §4 (sink-ahead) — only possible for
never-acked writes.

## 3. Fencing rules (prevent divergence)

- **Publish append (leader, spec 05 §3):** drop any publish frame whose `leader_epoch != store[T].epoch`,
  and drop any frame while `accepting_writes == false` (barrier not yet cleared). A producer still
  stamped with the old epoch is fenced until it learns epoch E.
- **Replication envelopes (spec 06 §5):** a sink accepts frames only with epoch ≥ its highest seen;
  an epoch increase forces re-HELLO against the (new) source. A source ignores control frames not at
  its term.
- **Stale leader writes:** an old leader O that didn't notice it lost leadership keeps appending to
  *its* master at epoch E-1. No replica will accept E-1 frames once they've seen E, so O's writes go
  nowhere and are discarded when O rejoins (O becomes a replica, runs a sink, and re-syncs to N's
  queue — O's divergent tail is dropped, never merged).

## 4. Sink-ahead on rejoin

When old leader O (or any replica) rejoins and its `last_applied_index` exceeds N's master tip (it
had un-acked writes N never saw), ringloom-queue's source replies `HELLO_NACK(sink_ahead_of_source)`.
The broker **discards O's divergent suffix**: O truncates/recreates its replica to N's first-available
and `full_replay`s (or `catchup`s). Implement `resetReplicaToLeader(topic_id)` that closes the local
queue, removes the dir, recreates empty, and restarts the sink. This is the only safe resolution under
AP and is consistent with "the follower never silently diverges." Because N picked the global-max
replica at the barrier, this only ever discards writes that were never acked anywhere reachable.

## 5. Data-loss semantics (accepted, documented)

| Scenario | Outcome |
|---|---|
| `replicate_once`-acked message, single-node failure | **Preserved.** It is on ≥1 surviving replica; the catch-up barrier syncs N to the max replica before accepting writes. |
| Leader dies; `fire_and_forget` tail not yet on any reachable replica | The un-acked tail beyond the most-advanced replica is lost (bounded, fenced). |
| Multiple simultaneous failures losing every node holding the acked index | Acked data can be lost only if **all** replicas holding it are simultaneously lost (no quorum). |
| Old leader rejoins with divergent (never-acked) tail | Divergent suffix discarded (sink-ahead reset, §4); never merged. |

> Full mesh removes the old "no replica anywhere → total topic loss" window: every topics-enabled
> broker replicates every topic, so a topic has as many replicas as there are topics-enabled brokers.

Future hardening (out of scope, noted): `quorum_durable` acks (ack only after K replicas applied) and
per-topic leadership distribution (rendezvous hashing) are additive to this design.

## 6. Tests

- Topic election candidate set: only topics-enabled brokers stand; lowest-nodeId wins; a topics-
  disabled lower-nodeId broker is **not** elected topic leader.
- Term monotonicity: simulate heartbeats with terms; a node adopts max; a new topic leader sets max+1.
- **Catch-up barrier:** new leader with a lagging local replica pulls from the most-advanced peer to
  its tip BEFORE `accepting_writes`; a publish arriving during the barrier is dropped/counted.
- Barrier preserves acked data: a `replicate_once`-acked message on a peer-but-not-N survives failover.
- Promote-in-place: replica promoted at the max index K; new appends continue at K+1; followers read a
  continuous index space.
- Fencing: old-epoch publish dropped; old-epoch repl frame dropped; stale leader writes never accepted
  by a replica that has seen the new epoch.
- Sink-ahead: rejoining ex-leader with extra (never-acked) tail is reset to the new leader and converges.
