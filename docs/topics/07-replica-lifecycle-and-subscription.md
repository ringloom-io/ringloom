# 07 — Replica Lifecycle & Subscription

**Goal:** Eagerly create a local replica queue + sink on **every** topics-enabled broker as soon as a
topic exists (full mesh), and hand any local subscriber a tailer-ready queue directory. Replicas are
**not** torn down on unsubscribe — they persist for the life of the topic.
**Modules:** `control_loop.zig` (topic-created + subscribe handling), `topic_engine.zig` (receiver-loop, owns queues+sinks) / `topic_store.zig`.
**Depends on:** 03, 06.

---

## 1. Eager replica creation (full mesh)

Replicas are created when a topics-enabled broker **learns the topic exists**, independent of any
subscriber. Triggered by a `TopicCreated`/`TopicInfo` admin frame (spec 02 §2). On the control loop:

```
on TopicCreated/TopicInfo(id, name, config, leader_node, epoch):
    if not topics.enabled: ignore
    registry.upsert(id, name, config, leader_node, epoch)
    if self_is_topic_leader(id):
        rec.local_role = leader   # master opened from the create/publish path (spec 05)
    else if not rec.local_queue_open:
        receiver engine ← OpenReplica{id, config}                 # create dir with exact geometry
        receiver engine ← StartSink{id, leader_node=leader_node}  # HELLO → full_replay/catchup → live
        rec.local_role = replica; rec.local_queue_open = true
```

## 2. Subscribe → return the already-open queue

Triggered by `SubscribeTopicMsg` (spec 03 §4). Because of full mesh the local replica (or master)
normally already exists; subscribe is a metadata lookup, not a creation:

```
id = topicIdOf(name)
rec = registry.get(id) orelse leaderLookup(id)        // spec 02 §2
if rec == null: reply unknown_topic; return
if not rec.local_queue_open:                          // only if eager-open hasn't landed yet
    receiver engine ← OpenReplica{id, rec.config}
    receiver engine ← StartSink{id, leader_node=rec.leader_node_id}
    rec.local_role = (self_is_topic_leader(id)) ? leader : replica
    rec.local_queue_open = true
rec.local_subscriber_count += 1                       // informational only; does NOT gate lifetime
reply TopicSubscriptionResponse{ id, queue_dir(id), rec.config, start_index(start_position) }
```

Notes:

- **On the topic-leader node, subscribers read the master queue directly** — no replica/sink needed
  there (the master is already a valid, tailer-readable queue). `queue_dir` points at the master dir.
- `start_index`:
  - `earliest` → the queue's first available index (0 or first retained after truncation).
  - `latest` → the master HWM (queried via registry/topic-engine status). The local replica may not
    have reached it yet; the tailer blocks until it does. That is correct and expected.

## 3. OpenReplica (receiver engine)

```
OpenReplica{id, config}:
    dir = topics.path/<group>/node-<id>/<name-or-id>/
    q = rq.Queue.open(.{ .dir = dir, .create = true,
                         .roll_scheme = schemeByName(config.roll_scheme_name),
                         .retention_cycles = config.retention_cycles,
                         .spawn_helper_threads = false,    // prefetcher thread drives maintenancePoll
                         .enable_prefetcher = true })
    store.put(id, .{ role = replica, queue = q, epoch = rec.leader_epoch })
StartSink{id, leader_node}:
    bind repl.Sink to q over Aeron channels toward leader_node (spec 06)
    # sink derives last_applied_index from q on init → resumes/catches up automatically
```

The directory name should be stable and filesystem-safe. Prefer the **topic name** if it is a safe
slug; otherwise use the hex `topic_id`. Store the chosen mapping in the registry for observability.

## 3. Subscriber read path (service side)

The subscriber service opens its **own** ringloom-queue `Tailer` on `queue_dir` at `start_index` and
polls it directly (spec 09). The broker is **not** involved per message. Multiple subscriber services
co-located on the same node each open independent tailers on the same replica directory (standard
ringloom-queue multi-reader, cross-process mmap).

## 4. Unsubscribe / teardown

```
UnsubscribeTopicMsg(id, svc):
    rec.local_subscriber_count -= 1
    # FULL MESH: the replica is NOT closed on unsubscribe. Every topics-enabled broker keeps a
    # live replica + sink for every topic so it can take over leadership (spec 08) and so a later
    # subscribe is instantly serviceable. local_subscriber_count is informational only.
```

A replica queue + sink is closed only when the **topic itself is deleted** (a future admin
operation) or the broker shuts down / disables topics. A subscriber process crash is detected by the
existing service liveness path; its tailer simply stops, but the replica keeps following the leader.

## 5. Races & edge cases

- **Subscribe before TopicCreated propagates:** `leaderLookup` (admin `TopicLookup`/`TopicInfo`)
  resolves it; the `TopicInfo` reply also triggers the eager OpenReplica (§1). If the topic leader
  doesn't know it either → `unknown_topic` (the producer hasn't registered yet).
- **TopicCreated and a local subscribe race:** the control loop is single-threaded and owns the
  registry, so `local_queue_open` gates a single OpenReplica regardless of which arrives first.
- **Leader changes while subscribed:** the sink observes the epoch bump / leader-changed admin and
  re-targets the new topic leader (spec 08); the subscriber's tailer is unaffected (same local dir).

## 6. Tests

- `TopicCreated` on a topics-enabled non-leader opens a replica + sink exactly once, with **no**
  subscriber present (full mesh).
- Subscribe returns the already-open queue_dir; a subscribe that beats the eager-open still opens
  once; second local subscribe does not re-open; counts track (informational).
- Topic-leader-node subscribe points at the master dir, opens no sink.
- `earliest` vs `latest` start indices computed correctly; tailer on a lagging replica blocks then
  delivers.
- Unsubscribe does **not** close the replica/sink (full mesh); only topic-delete/shutdown does.
