# RingLoom Replicated KV Stores — Specification

**Status:** Design (pre-implementation).
**Audience:** RingLoom broker/service engineers and AI agents implementing the feature.
**Scope:** Named, replicated key-value stores. Each store has one writer client and many readers.
Readers on any host read a local copy through shared memory. Replication is asynchronous and built on
[persistent topics](topics-architecture.md). Storage uses LMDB behind a replaceable engine
interface.

> This document is both the narrative and the contract. Once implementation starts, it can be
> split into per-task specs under `docs/kv/`, following `docs/topics/`.

---

## 1. Goals and non-goals

### 1.1 Goals

- **Very fast local reads.** A reader process maps its host's store read-only and looks up keys
  without involving the broker. Reads do not allocate.
- **Automatic replication.** Every KV-enabled broker keeps a local copy of every store, so readers
  can attach on any such host.
- **One writer per store.** A lease, enforced by fencing in the replicated log, ensures only one
  writer client's mutations take effect. A standby writer can take over when the holder fails.
- **Failover.** When the topic leader fails, another broker takes over. The existing persistent
  topic failover (spec 08) handles leadership changes. Writers can run on any host.
- **Many named stores.** Each store has its own log, local files, and writer lease.
- **Operations:** `put` (optional TTL), `delete`, `clear`, `get`, and full iteration.
- **Replaceable storage engine.** LMDB is the first engine. Applications and bindings never see
  LMDB types, so the engine can change without changing the public API.
- **No allocation on the hot path** (§12).

### 1.2 Non-goals for v1

- No strong consistency. Readers may see slightly stale data, and different hosts may briefly
  disagree. There is no quorum or Raft.
- No read-your-writes guarantee across hosts, and no waiting for a specific write to become visible.
  The `write_index` stored with each value (§6.2) is available if an application needs to build that
  itself.
- No transactions, compare-and-swap, or atomic multi-key batches visible to readers. Batches are
  applied in one engine transaction today, but readers must not depend on that.
- No range or prefix scans. Only full iteration, in an order the engine defines.
- No capacity-based eviction such as LRU. Entries leave the store only through delete, clear, or TTL.
- No log compaction or snapshots. The log is kept indefinitely; see §13 and phase 2.
- No store deletion API. Operators remove a store's log and files manually.

---

## 2. Locked design decisions

| Area | Decision | Rationale / consequence |
|---|---|---|
| Source of truth | **Each store is a persistent topic named `__kv.<store>`.** The topic log holds every mutation. Each local store is rebuilt from that log. | Reuses topic sequencing, Aeron replication, catch-up, epoch fencing, and on-disk storage. The topic log also acts as the write-ahead log. |
| Consistency | **Availability over consistency** (AP), inherited from topics. | Readers may lag. During a network partition, the side that loses has its recent writes discarded (§10). |
| Leadership | **The topic leader orders mutations for every store.** Leadership is not tied to the writer's host. | Writes from a writer on another host cross one network hop (Aeron UDP). Writes from the same host use Aeron IPC. |
| Replication scope | **Full mesh.** Every topics-enabled broker stores every store's log. Every KV-enabled broker also builds every store locally. | Readers can attach on any KV-enabled host without warm-up. This is a superset of "replicate to hosts with readers"; on-demand building is phase 2. |
| Writer exclusivity | **Writer lease, recorded as grant records in the log.** Each broker discards mutations whose `writer_epoch` differs from the latest grant. | Fencing is decided by the log, not by timing, so every replica makes the same choice. A stale writer cannot take effect. |
| Storage engine | **LMDB, one environment per store per generation**, behind `KvEngine` (§7). | Mature, uses memory mapping, supports multiple reader processes without locks, and copies nothing on lookup. Gives iteration for free. |
| Local durability | **Local store files are disposable.** The broker always rebuilds a store from the local topic log at startup. They live under `<storage_path>/kv`, by default `/dev/shm`. | Simplest restart model. No LMDB fsync is needed (`MDB_NOSYNC`), and no crash-consistency logic. |
| Rebuilds and reader continuity | **Generations.** A rebuild writes a new store generation while readers keep using the previous one. Readers then switch atomically. | Readers never see a partly built store after the first build. |
| TTL | **Absolute expiry time, stamped by the writer, checked by readers on every read.** Each broker also deletes expired entries in the background. | No coordination is needed. Expiry can shift by the clock skew between hosts. |
| Map size | **Fixed for each generation.** When a store fills up, the broker rebuilds into a larger generation. | Readers never handle LMDB `MDB_MAP_RESIZED`. |
| Configuration | **Store configuration is the first frame in the store's log**, so it is immutable and the first creation wins. | Replicates without new registry plumbing. |

---

## 3. System model

### 3.1 Roles

```text
Writer client (any host)
   │ KvWriter.put/delete/clear  → topic publish (Aeron IPC if co-located with topic leader, else UDP)
   ▼
Topic leader broker ── appends frames to topic __kv.<store> (opaque payloads, epoch-fenced)
   │  ringloom-queue replication over Aeron (full mesh, async)
   ▼
Every topics-enabled broker: local copy of the __kv.<store> log
   │  local Tailer
   ▼
KV-enabled broker: KV applier (sole writer of local store) ──► <kv.path>/.../<store>/gen-N/ (LMDB)
                                                         └──► control.shm (generation, applied index, state)
   ▲
Reader clients on the same host: read-only mmap, lock-free, no allocation
```

- **Writer client:** a service holding the store's writer lease. It encodes mutation frames (§6.1)
  and publishes them to the store topic. There is at most one effective writer per store.
- **Topic leader:** the existing topic leader (spec 08). It orders and appends frames without
  inspecting their contents. It also runs lease arbitration (§9) for every store.
- **Grant tracker:** runs on every topics-enabled broker. It reads the local store logs, looks only
  at frame headers, and tracks the latest writer grant for each store. A broker that becomes leader
  therefore already knows the current lease holder.
- **KV applier:** runs on every KV-enabled broker. It reads the local store log and applies frames
  to the local store. It is the only process that writes to local store files.
- **Reader client:** a service on a KV-enabled host. It opens the local store read-only and serves
  `get` and iteration without contacting the broker.

### 3.2 Enablement

- `broker.kv.enabled=true` requires `broker.topics.enabled=true`. Config validation fails otherwise.
- The grant tracker runs on every topics-enabled broker, even if KV is disabled there. Any of them
  can become topic leader and must arbitrate leases. Tracking only reads frame headers, so it is
  cheap.
- Readers can only open stores on a KV-enabled broker. Elsewhere `openKvReader` returns
  `KV_NOT_AVAILABLE_LOCALLY`.

---

## 4. Store identity and configuration

### 4.1 Names

- Store names contain `[A-Za-z0-9._-]`, are 1–64 bytes long, and are used directly as directory
  names.
- Backing topic: `__kv.<name>`. `store_id = topic_id = hash("__kv.<name>")`, using the topics hash
  (spec 01).
- The `__kv.` topic prefix is **reserved**. Ordinary topic publish and subscribe APIs reject it with
  `TOPIC_NAME_RESERVED`.
- `TopicConfig.flags` bit 1, `flag_kv_store`, marks these topics so the registry, metrics, and tools
  can recognize them.

### 4.2 Replicated store configuration (immutable)

```zig
pub const KvStoreConfig = extern struct {
    size: u32 = @sizeOf(KvStoreConfig),     // ABI-evolution prefix, like other option structs
    format_version: u16 = 1,
    _pad0: u16 = 0,
    max_key_len: u16 = 511,                 // ≤ engine limit (LMDB default MDB_MAXKEYSIZE = 511)
    _pad1: u16 = 0,
    max_value_len: u32 = 64 * 1024,         // ≤ max_frame_length - frame/record headers (validated)
    default_ttl_ms: u64 = 0,                // 0 = no expiry unless the put specifies one
    map_size_hint: u64 = 1 << 30,           // initial engine capacity in bytes; local override allowed
    topic_roll_scheme: [16]u8 = "FAST_DAILY" ++ [_]u8{0} ** 6,
    flags: u32 = 0,                         // reserved
    _reserved: [28]u8 = [_]u8{0} ** 28,
    comptime { std.debug.assert(@sizeOf(KvStoreConfig) == 80); }
};
```

- A store is created when a writer first registers with a config (§9.2). The topic leader creates
  `__kv.<name>` with `retention_cycles = 0` (required in v1, §13) and `flag_kv_store`. It then
  appends a **`STORE_HEADER` frame** containing the `KvStoreConfig` as index 0 of the log.
- The first creation wins. A later writer registration with a different config, ignoring
  `map_size_hint`, fails with `CONFIG_MISMATCH`. Readers never supply a config; they use the one in
  the log.
- Each broker can override capacity with `broker.kv.map.size.max` (§7.3). That setting is local, not
  replicated.

---

## 5. Files and shared-memory layout

```text
<kv.path>/<group>/node-<id>/<store>/
    control.shm              # KvControlHeader (4 KiB, fixed), survives broker restart
    gen-<n:08>/data.mdb      # LMDB data file (map size fixed at generation creation)
    gen-<n:08>/lock.mdb      # LMDB reader table / writer mutex
```

`kv.path` defaults to `<storage_path>/kv`, and `storage_path` defaults to `/dev/shm`. Every local
store can be rebuilt from the topic log on disk, so tmpfs is the recommended location.

### 5.1 Control header

Each cache line is written by a single writer. All fields are little-endian. Reader processes map it
read-only.

```zig
pub const KvControlHeader = extern struct {
    // ---- line 0: immutable after creation ----
    magic: u64 = 0x564B_4D4F_4F4C_4752, // "RGLOOMKV"
    layout_version: u32 = 1,
    engine_kind: u32,                    // 1 = LMDB
    store_id: u64,
    max_key_len: u32,
    max_value_len: u32,
    _pad0: [32]u8,
    // ---- line 1: generation switch (written rarely, by the applier) ----
    generation: std.atomic.Value(u64),   // 0 = no generation ready yet
    state: std.atomic.Value(u32),        // KvStoreState
    _pad1: [52]u8,
    // ---- line 2: progress (written after every apply commit) ----
    applied_index: std.atomic.Value(u64),
    applied_leader_epoch: std.atomic.Value(u64),
    applied_at_ns: std.atomic.Value(u64),      // CLOCK_REALTIME of last commit
    _pad2: [40]u8,
    // ---- line 3: liveness / lag (written periodically, by the applier) ----
    applier_heartbeat_ns: std.atomic.Value(u64),
    local_log_tip_index: std.atomic.Value(u64),
    replication_live: std.atomic.Value(u32),   // 1 if the topic replica is in live mode (or local is leader)
    _pad3: [44]u8,
    // remainder of the 4 KiB page reserved
};

pub const KvStoreState = enum(u32) {
    initializing = 0, // no generation published yet; readers get NOT_READY
    catching_up = 1,  // serving data; applied_index behind local log tip or replica not live
    live = 2,         // applied to local tip and replication live
    rebuilding = 3,   // serving generation N while N+1 is built
    failed = 4,       // applier stopped (see metrics/logs); readers still serve the last generation
};
```

---

## 6. Wire and storage formats

### 6.1 Mutation frame: one topic entry

Each topic entry carries one frame. All fields are little-endian. Records start on 8-byte
boundaries.

```zig
pub const KvFrameHeader = extern struct {
    magic: u32 = 0x564B_4C52,    // "RLKV"
    version: u8 = 1,
    kind: KvFrameKind,           // u8
    flags: u16 = 0,
    record_count: u32,           // DATA frames only
    body_len: u32,               // bytes after this header
    leader_epoch: u64,           // copied from TopicPublishHeader by the writer (leader-fenced)
    writer_epoch: u64,           // lease epoch; 0 for leader-authored frames
    comptime { std.debug.assert(@sizeOf(KvFrameHeader) == 32); }
};

pub const KvFrameKind = enum(u8) {
    store_header = 1,   // body: KvStoreConfig (always topic index 0)
    data = 2,           // body: record_count × (KvRecordHeader ‖ key ‖ value ‖ pad8)
    writer_grant = 3,   // body: KvWriterGrant
    writer_release = 4, // body: KvWriterRelease
};

pub const KvRecordHeader = extern struct {
    op: KvOp,                    // u8
    flags: u8 = 0,
    key_len: u16,
    value_len: u32,              // 0 for delete/clear
    expire_at_ns: u64,           // CLOCK_REALTIME ns; 0 = never. put only.
    comptime { std.debug.assert(@sizeOf(KvRecordHeader) == 16); }
};

pub const KvOp = enum(u8) { put = 1, delete = 2, clear = 3 };

pub const KvWriterGrant = extern struct {
    writer_epoch: u64,
    holder_session_nonce: u64,   // random per writer registration; distinguishes restarts
    holder_service_id: u32,
    holder_node_id: u16,
    _pad: u16 = 0,
    granted_at_ns: u64,
};

pub const KvWriterRelease = extern struct { writer_epoch: u64, reason: u32, _pad: u32 = 0 };
```

The topic leader appends `store_header`, `writer_grant`, and `writer_release` frames through a
leader-local append command on the topic engine. Only writer clients produce `data` frames.

**Validation, performed by the applier and grant tracker:** check `magic`, `version`, `body_len`
against the entry length, `record_count` against the body, each `key_len` against `max_key_len`,
and each `value_len` against `max_value_len`. Malformed frames are skipped as a whole and counted in
`kv_frames_malformed`. Every replica receives the same bytes, so every replica skips the same frames.

### 6.2 Value layout in the engine

```text
data[key] = KvValueHeader ‖ value bytes
KvValueHeader = extern struct { write_index: u64, expire_at_ns: u64 }   // 16 bytes
```

`write_index` is the topic index of the frame that last wrote the key. It supports freshness checks
and debugging. Engines may store values unaligned, so readers must use unaligned loads for the
header.

---

## 7. Storage engine abstraction

### 7.1 Interface

The engine is chosen at build time with `-Dkv-engine=lmdb`, the default. Its kind is recorded in
`KvControlHeader.engine_kind`, and a reader fails cleanly with `ENGINE_MISMATCH` if its build
differs. Both the broker and the service runtime import `src/common/kv/engine.zig`, which re-exports
the selected implementation. Each implementation must provide:

```zig
// Writer side (broker applier only)
pub const WriteEnv = struct {
    pub fn create(dir: []const u8, map_size: u64, max_readers: u32) !WriteEnv;   // setup; may allocate
    pub fn close(self: *WriteEnv) void;
    pub fn begin(self: *WriteEnv) !WriteTxn;
};
pub const WriteTxn = struct {
    pub fn put(self: *WriteTxn, key: []const u8, hdr: KvValueHeader, value: []const u8) !void; // error.MapFull
    pub fn delete(self: *WriteTxn, key: []const u8) !void;          // not-found is not an error
    pub fn clear(self: *WriteTxn) !void;
    pub fn expiryInsert(self: *WriteTxn, expire_at_ns: u64, key: []const u8) !void;
    pub fn expiryRemove(self: *WriteTxn, expire_at_ns: u64, key: []const u8) !void;
    pub fn sweepExpired(self: *WriteTxn, now_ns: u64, max: u32) !u32; // deletes ≤ max expired keys
    pub fn setMeta(self: *WriteTxn, meta: KvEngineMeta) !void;
    pub fn commit(self: *WriteTxn) !void;
    pub fn abort(self: *WriteTxn) void;
};

// Reader side (service runtime; also used by broker diagnostics)
pub const ReadEnv = struct {
    pub fn open(dir: []const u8, max_readers: u32) !ReadEnv;          // setup; may allocate
    pub fn close(self: *ReadEnv) void;
    pub fn newSession(self: *ReadEnv) !ReadSession;                  // setup; claims a reader slot
};
pub const ReadSession = struct {
    pub fn begin(self: *ReadSession) !void;                          // hot path: no allocation
    pub fn get(self: *ReadSession, key: []const u8) !?[]const u8;    // view valid until end()
    pub fn cursorFirst(self: *ReadSession) !?Entry;                  // full iteration
    pub fn cursorNext(self: *ReadSession) !?Entry;
    pub fn end(self: *ReadSession) void;                             // hot path: no allocation
    pub fn deinit(self: *ReadSession) void;
};
```

An engine conformance test suite, `src/common/kv/engine_conformance_test.zig`, runs against every
engine. It covers behavior, multi-process reader visibility, view lifetimes, iteration, the expiry
index, and allocation counts on hot-path calls.

### 7.2 LMDB mapping

- **Dependency:** LMDB C sources are vendored through `build.zig.zon` and compiled with Zig. The
  OpenLDAP Public License is compatible with Apache-2.0 distribution; add it to `NOTICE`/licenses.
- **Environment per store generation.** Named databases:
  - `data`: key → `KvValueHeader ‖ value`.
  - `expiry`: `be64(expire_at_ns) ‖ key` → empty. Only keys with expiry are included. This orders
    entries by expiry time for the sweeper.
  - `meta`: fixed keys `format_version`, `store_id`, `applied_index`, `applied_leader_epoch`,
    `writer_epoch`, `engine_kind`. v1 writes these but never reads them back, because it always
    rebuilds. They exist so phase 2 can resume instead.
- **Writer flags:** `MDB_WRITEMAP | MDB_NOSYNC | MDB_NOMETASYNC | MDB_NOTLS | MDB_NORDAHEAD`.
  Durability does not matter because local files are disposable (§2).
- **Reader flags:** `MDB_RDONLY | MDB_NOTLS | MDB_NORDAHEAD`. With `MDB_NOTLS`, a reader slot
  belongs to a read session rather than an OS thread. This is required for guard objects and for
  Java and Node thread pools.
- **Read session:** `newSession` does `mdb_txn_begin(RDONLY)` then `mdb_txn_reset`. `begin` uses
  `mdb_txn_renew`, and `end` uses `mdb_txn_reset`. Neither allocates. `get` uses `mdb_get` and
  returns a pointer into the map.
- **Iteration:** a cursor is opened once per session and reused through `mdb_cursor_renew`. Keys come
  back in byte order, but the public API documents the order as engine-defined.
- **`max_readers`:** `broker.kv.max.readers`, default 1024. Each read session holds one slot.
  Exhaustion returns `READERS_FULL`.
- **Reader-slot cleanup:** the applier calls `mdb_reader_check` every `broker.kv.reader.check.ms`,
  default 1000, to clear slots left by dead reader processes.

### 7.3 Capacity

- New generation map size: `max(map_size_hint, 2 × size of the previous generation's data)`, capped
  by `broker.kv.map.size.max`, default 64 GiB. Pages are allocated only when touched, so on tmpfs
  only used pages consume memory.
- When a put returns `MapFull`, the applier aborts the transaction, sets state `rebuilding`, and
  builds generation N+1 with double the map size, capped. If the cap is already reached, the state
  becomes `failed`. Readers keep serving generation N; `kv_store_full` is raised; the operator raises
  the cap and the next broker restart rebuilds.

---

## 8. KV applier (broker)

### 8.1 Threading

- `broker.kv.apply.threads`, default 1, controls the number of applier threads. Stores are assigned
  by `store_id % threads`. Each thread owns its stores' writer environments, so every environment
  has exactly one writer.
- The idle strategy and CPU affinity follow existing threading settings:
  `broker.kv.apply.idle.strategy` (`busy_spin | yield | backoff`) and
  `broker.kv.apply.cpu.affinity`.
- Registration, lookups, and generation housekeeping travel between the control loop and appliers
  through preallocated single-producer, single-consumer command queues, as with topics.

### 8.2 Apply loop for each store

```text
loop:
  n = drain local Tailer up to apply.batch.max.entries (default 1024) or apply.batch.max.us (default 50)
  if n == 0: update heartbeat/lag fields periodically; maybe sweep (§11.3); idle; continue
  txn = env.begin()
  for each entry (index i):
     verify i == applied_index + 1, else trigger rebuild (§8.4) and stop
     frame = validate(entry)                         // malformed → skip, count
     switch frame.kind:
       store_header   → verify matches cached config
       writer_grant   → current_writer_epoch = grant.writer_epoch
       writer_release → if release.writer_epoch == current_writer_epoch: current_writer_epoch = 0
       data           → if frame.writer_epoch == 0 or != current_writer_epoch: skip, count kv_frames_fenced
                        else for each record:
                          put    → if expire_at_ns != 0 and expire_at_ns <= now: delete(key) (expired on arrival)
                                   else: old = existing expiry (if any) → expiryRemove
                                         put(key, {i, expire_at_ns}, value); expiryInsert if expire_at_ns != 0
                          delete → expiryRemove(old) if any; delete(key)
                          clear  → clear()   // data + expiry
     applied_index = i; applied_leader_epoch = frame.leader_epoch
  txn.setMeta(...); txn.commit()
  control.applied_index/epoch/at_ns.store(release)
  state = live if applied_index == local_log_tip and replication_live else catching_up
```

To remove an old expiry entry, the applier reads the existing value header in the same transaction.
That is one extra B-tree lookup per put or delete on stores that use TTL.

### 8.3 Generations and switching

1. **Initial build** happens when the control header is missing, for example on first start or after
   a host reboot clears tmpfs. The applier creates `control.shm` with `generation = 0` and state
   `initializing`. It builds `gen-00000001` from log index 0 to the local tip, then sets
   `generation = 1` with release ordering and state `live` or `catching_up`. Until then, readers get
   `NOT_READY`.
2. **Rebuild**, for any trigger in §8.4, happens while serving generation N. The applier sets state
   `rebuilding` and builds `gen-(N+1)` from index 0 in a separate environment. When it reaches the
   local tip, it publishes `generation = N+1`.
3. Read sessions check `generation` with one acquire load at each `begin()` (§11.1). When it changes,
   the client reopens on the new generation.
4. Old generations are deleted after `broker.kv.generation.grace.ms`, default 5000. On Linux, a
   reader that still has the old files mapped keeps a valid mapping after the files are unlinked, so
   deletion cannot crash it.

### 8.4 Rebuild triggers

- **Broker start, always (v1).** The control header and the old generation survive, so readers keep
  serving while generation N+1 is built. This is the deliberately simple restart model chosen for v1.
- **Local topic replica reset.** When `resetReplicaToLeader` runs (spec 08 §4, sink-ahead after a
  partition heals), the topic engine notifies the KV subsystem with `TopicReplicaReset{topic_id}`.
  The applier closes its tailer, waits until the replica has caught up again, and rebuilds.
- **Index discontinuity.** If the next tailer entry is not `applied_index + 1`, the applier rebuilds.
- **Map full** (§7.3).
- **Operator command:** `ringloom-kv rebuild <store>`.

### 8.5 Rebuild cost

Rebuild time is roughly `log_bytes / apply throughput`. The phase-1 benchmark (§14) must measure
this, both entries per second and MB per second, so operators can judge restart time. Logs that grow
too large are addressed by phase-2 snapshots.

---

## 9. Writer lease and fencing

### 9.1 Invariant

A data frame changes a store **only if its `writer_epoch` equals the epoch of the most recent
`writer_grant` before it in the log, with no matching `writer_release` in between.** Every applier
evaluates this against the same log, so the outcome is identical everywhere. Leader-side checks only
improve liveness; safety does not depend on them.

### 9.2 Acquire

```text
writer service ──RegisterKvWriter{name, KvStoreConfig, acquire_mode, session_nonce}──▶ local broker
   local broker ──(admin UDP, unless local is topic leader)──▶ topic leader
topic leader control loop:
   wait until: topic barrier cleared (spec 08) AND grant tracker caught up to local tip for the store
   if store missing → create topic + append STORE_HEADER (§4.2)
   else if config mismatch → CONFIG_MISMATCH
   holder = tracker.current(store)
   if holder.nonce == requester.nonce           → GRANTED (same epoch; reconnect is idempotent)
   else if holder exists and holder service alive:
        acquire_mode == fail_if_held → HELD_BY_OTHER
        acquire_mode == wait         → QUEUED (FIFO waiters per store)
   else:
        e = tracker.last_epoch(store) + 1
        append WRITER_GRANT{e, requester}; on append → GRANTED{e}
response → local broker → writer: KvWriterResponse{status, store_id, writer_epoch, topic_id,
                                   leader_node_id, leader_epoch, effective KvStoreConfig}
```

The writer receives `GRANTED` only after the grant frame is appended. Its data frames therefore
receive higher topic indices than the grant.

### 9.3 Release and takeover

- **Graceful:** `UnregisterKvWriter` or writer shutdown → the leader appends
  `WRITER_RELEASE{epoch}` and grants the next queued waiter, if any, with `KvWriterGranted` sent
  asynchronously.
- **Writer failure:** cluster discovery removes the holder's service through a heartbeat timeout or
  peer loss (`clustering.md`). The leader then appends `WRITER_RELEASE` and grants the next waiter.
- **Stale writer:** if a former holder is still running but partitioned, it may keep sending frames
  with its old epoch. Appliers skip them under §9.1. The leader sends it `KvWriterRevoked{epoch}`
  when reachable, and the client then returns `NOT_WRITER`.
- **Optional optimization:** the leader may drop data frames from non-holders before appending them.
  This requires the topic append path to read `KvFrameHeader.writer_epoch` for `flag_kv_store`
  topics. It is not required for correctness and is deferred unless benchmarks show that fenced
  frames waste meaningful log space.

### 9.4 Topic-leader failover

1. The new leader N runs the topic catch-up barrier (spec 08 §2). N's grant tracker has read N's
   local log, so it knows the latest grant.
2. Writers receive `TopicLeaderChanged` and redirect publishing (existing topic publisher behavior).
   They also send **`KvWriterReassert{store, writer_epoch, nonce}`** and return `LEADER_UNAVAILABLE`
   from `put` until the reassert is confirmed.
3. N handles the reassert as follows:
   - If its tracker's latest grant matches, it confirms.
   - If the grant was lost in failover because it was never replicated, and no other holder exists,
     N appends a new grant with epoch `last + 1` and replies with the new epoch.
   - If a different holder exists, N sends `KvWriterRevoked`.

   This prevents a writer from sending frames that every applier would silently discard.
4. Data frames in flight during the barrier are dropped by topic fencing (spec 08 §3). Writers that
   need to know whether a write survived use `replicate_once` (§11.2) and retry if it is not
   acknowledged.

---

## 10. Failure semantics

| Scenario | Outcome |
|---|---|
| Writer process or host dies | Its lease is released after the discovery timeout. A queued standby receives `KvWriterGranted`. `fire_and_forget` frames not yet appended are lost. |
| Topic leader dies | Barrier and new leader per spec 08. Writers reassert their leases (§9.4). Frames still in flight and never replicated are lost; `replicate_once`-acknowledged frames survive one node failure. Readers on other hosts keep serving. |
| A replica broker dies | Its readers keep serving the last generation, which becomes stale; `status()` shows a stale `applier_heartbeat_ns`. On restart the broker rebuilds while readers continue serving. |
| Reader process dies | Its LMDB reader slots are cleared by `mdb_reader_check`. |
| Network partition, two topic leaders | Both sides may accept writes, and each side's readers see that side's writes. When the partition heals, the side whose leadership survives under spec 08 wins. The other side's log tail is discarded, those replicas reset, and their appliers rebuild (§8.4). Data converges to the winning history. |
| Writer and topic leader on opposite sides of a partition | The writer's frames do not reach the leader, and the writer sees back-pressure or `LEADER_UNAVAILABLE`. If discovery on the leader's side declares the writer dead, a standby there may acquire the lease. The old writer's later frames are then fenced under §9.1. |
| Store full | Automatic rebuild into a larger generation; if capped, state `failed` and readers keep serving (§7.3). |
| Malformed frame | Skipped identically on every replica and counted. |
| Clock skew | A TTL takes effect earlier or later on each host by that host's skew relative to the writer. |

**Readers never block on cluster events.** They always serve the latest generation that has been
published locally.

---

## 11. Client APIs

### 11.1 Reader (native Zig)

```zig
pub const KvReader = struct {
    pub fn status(self: *const KvReader) KvStatus;   // state, generation, applied_index, lag, heartbeat age
    pub fn newSession(self: *KvReader) !KvReadSession; // setup: claims an engine reader slot
    pub fn close(self: *KvReader) void;
};

pub const KvReadSession = struct {   // one per thread; not thread-safe
    /// Copying get. Begin, look up, copy, end. Returns null if missing or expired.
    pub fn get(self: *KvReadSession, key: []const u8, out: []u8) KvGetResult; // .found(len) / .missing / .buffer_too_small(len) / .not_ready
    /// Zero-copy. The view stays valid until `release()`; it holds an engine read snapshot.
    pub fn view(self: *KvReadSession, key: []const u8) KvViewResult;
    pub fn release(self: *KvReadSession) void;
    /// Full iteration over one consistent snapshot; expired entries are skipped.
    /// Order is engine-defined. Key/value views are valid only inside the callback.
    pub fn forEach(self: *KvReadSession, ctx: anytype, comptime cb: fn (@TypeOf(ctx), []const u8, []const u8) bool) KvIterResult;
};
```

- **Opening:** `client.openKvReader(name, .{ .wait_ready_timeout_ms = 0, .prefault = false })`
  sends `OpenKvReader{name}`. The broker replies `KvReaderResponse{status, store_id, store_dir,
  engine_kind, config}`. The client maps `control.shm` and opens the current generation. With
  `prefault=true`, it calls `madvise(MADV_WILLNEED)` on the engine mapping to avoid page faults on
  first access.
- **Every `begin`** loads `control.generation` with acquire ordering. If the value changed since the
  session opened, the session moves to the new generation. Opening a generation allocates engine
  state; this is the **only allowed allocation on the read path**. It is rare, happening on rebuild
  or switch only, and is counted in `kv_reader_generation_switches`.
- **Expiry check:** a value is treated as absent when `expire_at_ns != 0 and now >= expire_at_ns`.
  The clock is set per reader with `ttl_clock = precise | coarse`. `precise`, the default, uses
  `CLOCK_REALTIME` through the vDSO. `coarse` uses `CLOCK_REALTIME_COARSE`, which is cheaper and has
  millisecond resolution.
- **Long-held views and long iterations** keep an old engine snapshot alive, which makes the store
  file grow during that time. The applier reports `kv_oldest_reader_age_ms` and logs a warning above
  `broker.kv.reader.warn.ms`, default 1000.
- **Brokerless reads:** after opening, a reader does not depend on the broker. If the broker dies,
  reads keep working on the last generation.

### 11.2 Writer (native Zig)

```zig
pub const KvWriter = struct {
    pub fn put(self: *KvWriter, key: []const u8, value: []const u8, opts: KvPutOpts) KvWriteResult;
    pub fn delete(self: *KvWriter, key: []const u8, opts: KvWriteOpts) KvWriteResult;
    pub fn clear(self: *KvWriter, opts: KvWriteOpts) KvWriteResult;
    /// Batching: records accumulate in a preallocated buffer of max_frame_length and are sent as
    /// one DATA frame on commitBatch() or when the buffer fills (auto-flush).
    pub fn beginBatch(self: *KvWriter) void;
    pub fn commitBatch(self: *KvWriter, opts: KvWriteOpts) KvWriteResult;
    pub fn isAcked(self: *const KvWriter, publish_seq: u64) bool;    // replicate_once, via topic HWM
    pub fn state(self: *const KvWriter) KvWriterState;               // granted | queued | revoked | reasserting
    pub fn close(self: *KvWriter) void;                              // releases the lease
};
pub const KvPutOpts = struct { ttl_ms: ?u64 = null, ack_mode: AckMode = .fire_and_forget }; // null → store default
```

- **Opening:** `client.openKvWriter(name, KvStoreConfig, .{ .acquire = .fail_if_held | .wait,
  .on_state = callback })`.
- **Expiry:** `expire_at_ns = CLOCK_REALTIME(now) + ttl`, computed on the writer host. A TTL of 0
  means the entry never expires.
- **Results:** `ok(publish_seq)`, `back_pressured`, `not_writer`, `leader_unavailable`,
  `key_too_large`, `value_too_large`, `batch_full`.
- **Encoding** writes directly into the Aeron claim buffer, or into the preallocated batch buffer,
  with no allocation.

### 11.3 TTL sweeper

Every `broker.kv.sweep.interval.ms` (default 100), each applier deletes up to
`broker.kv.sweep.batch` (default 1000) expired keys per store in its own transaction, using the
`expiry` index. Sweeping is local to each broker and does not write to the log, because the expiry
times are already replicated. Readers never depend on sweeping; expiry checks happen on every read.

### 11.4 C ABI and bindings

Add the following to `include/ringloom_service.h` with an ABI version bump. Option structs start
with a `size` field; handles are opaque; views are borrowed.

```c
typedef struct ringloom_kv_reader ringloom_kv_reader_t;
typedef struct ringloom_kv_session ringloom_kv_session_t;
typedef struct ringloom_kv_writer ringloom_kv_writer_t;

int  ringloom_kv_open_reader(ringloom_client_t*, const char* name,
        const ringloom_kv_reader_options_t* opts, ringloom_kv_reader_t** out);
int  ringloom_kv_reader_status(const ringloom_kv_reader_t*, ringloom_kv_status_t* out);
int  ringloom_kv_session_new(ringloom_kv_reader_t*, ringloom_kv_session_t** out);
int  ringloom_kv_get(ringloom_kv_session_t*, const uint8_t* key, size_t key_len,
        uint8_t* out, size_t out_cap, size_t* out_len);                     // copy
int  ringloom_kv_view(ringloom_kv_session_t*, const uint8_t* key, size_t key_len,
        const uint8_t** out_val, size_t* out_len);                          // borrowed until release
void ringloom_kv_release(ringloom_kv_session_t*);
int  ringloom_kv_for_each(ringloom_kv_session_t*, ringloom_kv_visit_fn fn, void* ctx);
void ringloom_kv_session_free(ringloom_kv_session_t*);
void ringloom_kv_close_reader(ringloom_kv_reader_t*);

int  ringloom_kv_open_writer(ringloom_client_t*, const char* name, const ringloom_kv_store_config_t* cfg,
        const ringloom_kv_writer_options_t* opts, ringloom_kv_writer_t** out);
int  ringloom_kv_put(ringloom_kv_writer_t*, const uint8_t* key, size_t key_len,
        const uint8_t* val, size_t val_len, int64_t ttl_ms /* -1 = store default */,
        uint8_t ack_mode, uint64_t* out_seq);
int  ringloom_kv_delete(ringloom_kv_writer_t*, const uint8_t* key, size_t key_len, uint8_t ack_mode, uint64_t* out_seq);
int  ringloom_kv_clear(ringloom_kv_writer_t*, uint8_t ack_mode, uint64_t* out_seq);
int  ringloom_kv_begin_batch(ringloom_kv_writer_t*);
int  ringloom_kv_commit_batch(ringloom_kv_writer_t*, uint8_t ack_mode, uint64_t* out_seq);
int  ringloom_kv_is_acked(ringloom_kv_writer_t*, uint64_t seq);
int  ringloom_kv_writer_state(const ringloom_kv_writer_t*);
void ringloom_kv_close_writer(ringloom_kv_writer_t*);
```

The C++, Node, and Java bindings wrap only this C ABI and never link LMDB directly, so replacing the
engine does not affect them. For Java, the copying `get` into a caller-provided `MemorySegment` is
the main API; `view` is exposed as a scoped segment.

**ABI 7 additions** (service ABI 6 → 7):

```c
int      ringloom_kv_reader_config(const ringloom_kv_reader_t*, ringloom_kv_store_config_t* out);
int      ringloom_kv_writer_config(const ringloom_kv_writer_t*, ringloom_kv_store_config_t* out);
uint64_t ringloom_kv_writer_acked_seq(const ringloom_kv_writer_t*);    // lock-free, monotonic
uint64_t ringloom_kv_writer_leader_epoch(const ringloom_kv_writer_t*); // 0 = none
uint64_t ringloom_kv_writer_epoch(const ringloom_kv_writer_t*);        // 0 while queued/revoked
int      ringloom_kv_abandon_batch(ringloom_kv_writer_t*);
```

- `*_config` require `out->size >= sizeof(ringloom_kv_store_config_t)`. The reader reports the limits
  stored in the store's control file; the writer reports the config the broker accepted.
- `acked_seq` uses the same sequence namespace as the `out_seq` of `put`/`delete`/`clear`/`commit_batch`.
- `abandon_batch` drops records buffered since `begin_batch` and ends the batch. Frames auto-flushed
  while the batch was open (buffer full) are already published and are not recalled.
- Threading: a read session may be used by any thread, one at a time (MDB_NOTLS). Writer mutations,
  the writer accessors and control polling are serialized by the native `KvContext` lock, so one
  thread may publish while another runs `ringloom_service_poll_control`. `close_writer` must not race
  with publishing on the same writer (it frees the writer), nor — conservatively — with control polling.

**Java binding (ABI 7):** `KvSession` defaults to `KvSessionMode.SERIAL` (any thread, one at a time,
pinned to the viewing thread while a `view` is held, closable from any idle thread);
`KvReader.newSession(KvSessionMode.CONFINED)` keeps the owner-thread check. `get`, `put` and `delete`
have offset/length overloads that pass raw addresses (allocation-free); `forEach` reuses one upcall stub
per session and `forEachRaw(KvRawVisitor)` iterates without allocation. `get` is linked with
`Linker.Option.critical(true)` and accepts heap segments. `put`/`delete` are not critical: they can wait
on the `KvContext` lock that the control thread holds while upcalling the Java state callback, and a
critical call blocks the safepoint that callback may need. Writes therefore require native segments.
`KvReader` adds `config()`, `status(KvStatusHolder)` and `openSessionCount()`; `KvWriter` adds
`ackedSequence()`, `leaderEpoch()`, `writerEpoch()` (single downcalls, no monitor), `config()` and
`abandonBatch()`.

---

## 12. Control-plane messages

These use the next free template IDs after the topic templates, assigned during implementation. All
are extern structs with size asserts and encode/decode round-trip tests, like spec 03.

| Message | Direction | Purpose |
|---|---|---|
| `RegisterKvWriter` / `KvWriterResponse` | service ↔ broker (+ admin forward to topic leader) | Acquire the lease and create the store if needed (§9.2) |
| `KvWriterGranted` | broker → service | Asynchronous grant for a queued writer |
| `KvWriterRevoked` | broker → service | Fenced or superseded |
| `KvWriterReassert` / response | service → broker → leader | After a topic-leader change (§9.4) |
| `UnregisterKvWriter` | service → broker → leader | Graceful release |
| `OpenKvReader` / `KvReaderResponse` | service ↔ local broker | Return the store directory and metadata; no forwarding |
| `TopicReplicaReset` (internal) | topic engine → KV subsystem | Rebuild trigger (§8.4) |
| `KvLeaderAppend` (internal) | control loop → topic engine | Append a leader-authored frame: header, grant, or release |

---

## 13. Log retention (v1 constraint)

- Store topics require `retention_cycles = 0`, enforced at creation. Every local store must be
  rebuildable from index 0, and new brokers join through ringloom-queue `full_replay`.
- Log size is the total bytes ever written. For mostly static data this is acceptable. Workloads
  that reload often, for example a daily `clear` followed by a full reload, grow the log without
  bound and need phase-2 snapshots.
- Expose `kv_log_bytes` and `kv_live_bytes` per store so operators can see how much log space is no
  longer reflected in the live store.

---

## 14. Performance, allocation, and validation

### 14.1 Allocation contract

| Path | Requirement |
|---|---|
| Reader `get` / `view` / `release` / `forEach` | **Zero allocation**; only generation switches are exempt (§11.1). |
| Writer `put` / `delete` / `clear` / batch | **Zero allocation**. |
| Topic append and replication | Unchanged; already free of hot-path allocation (topics spec). |
| Broker applier: RingLoom code | **Zero allocation** in steady state. |
| Broker applier: engine internals | LMDB with `MDB_WRITEMAP` is expected to allocate little once warm, but this is **unverified**. It is measured and reported per transaction. The phase-1 gate is that steady-state allocation must be measured. A non-zero result is reported, and is not automatically a failure, because the applier is off the read hot path. |
| Open, registration, rebuild, sweep setup | May allocate. |

**How allocations are measured:** Zig code uses a counting allocator in tests. LMDB is compiled with
a counting `malloc`/`calloc`/`realloc`/`free` shim, using `-D` defines when compiling `mdb.c` and
`midl.c`, so the conformance suite and perf harness can assert or report counts per operation. An
`LD_PRELOAD` counter is the fallback if the shim proves fragile.

### 14.2 Initial targets (confirmed or adjusted by phase-1 benchmarks)

| Metric | Initial target |
|---|---|
| `get` hit, copy mode, key ≤ 64 B, value ≤ 256 B, warm, 1M entries: p50 / p99 | ≤ 300 ns / ≤ 1 µs |
| `view` within an already-begun session (no renew/reset): p50 | ≤ 150 ns |
| Writer `put` call (`fire_and_forget`), p99 | ≤ 1 µs |
| End-to-end visibility, writer → reader on a remote host, light load, p99 | ≤ replication p99 + 100 µs |
| Applier throughput, 100 B values, batched | ≥ 500k puts/s per applier thread |
| Rebuild from 1M-entry log | Measured and reported (§8.5) |

### 14.3 Benchmarks

Add a `kv` suite to the perf harness (`-Dperf-filter=kv`) and to `scripts/bench-all.sh --suites`,
following `docs/benchmarks.md`. It covers: get hit and miss latency histograms at 1k, 1M, and 10M
entries; get across multiple reader threads; iteration rate; writer call latency; end-to-end
visibility on one host and across two hosts; applier throughput; rebuild time; and allocation counts
per operation. Results must be produced with `-Doptimize=ReleaseFast`, kept as raw JSON, and
compared with `scripts/bench-report.py`.

### 14.4 Tests

- **Unit:** frame and record encode/decode with size asserts; validation of every malformed-frame
  case; fencing state machine (§9.1) including grant, release, stale epoch, and epoch 0; TTL decisions
  including expiry on arrival; control-header generation protocol.
- **Engine conformance** (§7.1), run under the LMDB engine, including reading from a second
  process while the writer commits.
- **Applier:** applying a log produces the expected store; rebuild publishes N+1 while N keeps
  serving; index discontinuity triggers a rebuild; a full map grows the generation; the sweeper
  removes expired keys.
- **e2e (`zig build e2e`):**
  - Single node: put, get, delete, clear, TTL, and iteration.
  - Three nodes: writer on host A, leader on host B, reader on host C converges.
  - Writer failover to a standby.
  - Topic-leader failover with reassert, including a grant lost in failover.
  - Stale-writer fencing.
  - Partition heal: the losing side's replica is reset and its store rebuilds to the winning
    history.
  - Broker restart: readers keep serving and switch generations.
  - Reader process crash: its slot is reclaimed.
- **Bindings:** `zig build test-cpp|test-node|test-java` cover reader and writer smoke paths and
  copy and view semantics.

---

## 15. Observability

Per store, using the existing counter infrastructure, `ringloom-stat`, and topic metrics:
`kv_applied_index`, `kv_apply_lag_entries`, `kv_apply_batches`, `kv_apply_txn_ns` (histogram),
`kv_frames_applied`, `kv_frames_fenced`, `kv_frames_malformed`, `kv_records_put/delete/clear`,
`kv_expired_swept`, `kv_entries`, `kv_live_bytes`, `kv_log_bytes`, `kv_map_used_pct`,
`kv_generation`, `kv_rebuilds{reason}`, `kv_store_state`, `kv_readers_active`,
`kv_oldest_reader_age_ms`, `kv_reader_generation_switches`, `kv_engine_allocs`, `kv_writer_epoch`,
`kv_writer_holder`, and `kv_writer_waiters`.

---

## 16. Broker configuration

| Key | Default | Meaning |
|---|---|---|
| `broker.kv.enabled` | `false` | Build stores locally and serve readers. Requires topics. |
| `broker.kv.path` | `<storage_path>/kv` | Location of local store files. tmpfs is recommended. |
| `broker.kv.max.stores` | `64` | Preallocated store slots. |
| `broker.kv.apply.threads` | `1` | Number of applier threads; stores are assigned by `store_id`. |
| `broker.kv.apply.batch.max.entries` | `1024` | Maximum log entries per apply transaction. |
| `broker.kv.apply.batch.max.us` | `50` | Maximum time to gather a batch before committing. |
| `broker.kv.apply.idle.strategy` | `backoff` | `busy_spin`, `yield`, or `backoff`. |
| `broker.kv.apply.cpu.affinity` | unset | Pin applier threads to CPUs. |
| `broker.kv.map.size.max` | `64 GiB` | Cap on automatic map-size growth. |
| `broker.kv.max.readers` | `1024` | Engine reader slots per store. |
| `broker.kv.reader.check.ms` | `1000` | Interval for cleaning up dead reader slots. |
| `broker.kv.reader.warn.ms` | `1000` | Warn when a read snapshot is older than this. |
| `broker.kv.sweep.interval.ms` / `.batch` | `100` / `1000` | TTL sweeper interval and batch size. |
| `broker.kv.generation.grace.ms` | `5000` | How long to keep an old generation after switching. |

---

## 17. Implementation plan

| # | Task | Depends on | Module(s) |
|---|---|---|---|
| K01 | Formats: frame, record, config, value header | — | `src/common/kv/kv_format.zig` |
| K02 | Engine interface, LMDB vendoring and engine, conformance suite, allocation shim | K01 | `build.zig`, `src/common/kv/engine.zig`, `lmdb_engine.zig` |
| K03 | Control header and generation directories | K01 | `src/common/kv/kv_control.zig` |
| K04 | Topic hooks: `flag_kv_store`, reserved prefix, `KvLeaderAppend`, `TopicReplicaReset` | — | `src/broker/topics/*` |
| K05 | Applier: apply loop, generations, rebuild triggers, sweeper, reader-slot check | K02–K04 | `src/broker/kv/kv_applier.zig`, `kv_subsystem.zig` |
| K06 | Lease: grant tracker, leader arbitration, reassert, control and admin messages | K04 | `src/broker/kv/kv_writer_lease.zig`, `kv_messages.zig` |
| K07 | Service API: `KvWriter`, `KvReader`, `KvReadSession`, C ABI | K02, K03, K06 | `src/service/kv/*`, `c_abi.zig`, header |
| K08 | Bindings: C++, Node, Java | K07 | `bindings/*` |
| K09 | Metrics and `ringloom-stat` integration | K05, K06 | monitoring |
| K10 | Test service binaries, e2e scenarios, `kv` perf suite | all | `src/bin`, `src/e2e`, `src/perf`, scripts |

### Phase 2 (designed later; v1 must not prevent these)

- **Snapshots and retention.** Store-independent snapshot files containing `put` records as of
  applied index S. Topic retention can then delete log cycles before the oldest kept snapshot. New or
  lagging brokers that receive `HELLO_NACK(index_unavailable)` fetch a snapshot over a new Aeron
  transfer stream, load it as a new generation, and start the topic replica at S+1. **First verify
  that ringloom-queue can start a replica at a non-zero index.**
- **Resume on restart.** Use the `meta` keys to resume instead of rebuilding, after checking that
  the log entry at `applied_index` has the expected epoch.
- **On-demand building:** per-store `materialize = eager | on_demand`, building a store only on hosts
  with registered readers.
- **Alternative engine,** for example a custom shared-memory hash table, behind §7.1, if benchmarks
  show the B-tree lookup is the bottleneck.
- **Read-your-writes helper:** wait until local `applied_index ≥ i`.

---

## 18. Open items to confirm during implementation

1. **Topic payload limit:** confirm the maximum topic entry size (`max_frame_length` and
   ringloom-queue entry limits). This determines the largest valid `max_value_len` and batch size.
2. **Leader-local append:** confirm the topic engine can accept a leader-originated append command
   and report the assigned index back to the control loop.
3. **Replica reset notification:** confirm that `resetReplicaToLeader` has a clear place to emit
   `TopicReplicaReset` and that the applier's tailer can be safely closed before the directory is
   recreated.
4. **LMDB allocation behavior** with `MDB_WRITEMAP` in steady state. Measure it in K02 and record the
   result here.
   **Measured (K02, LMDB 0.9.33, `MDB_WRITEMAP|NOSYNC|NOMETASYNC|NOTLS|NORDAHEAD`, counting shim from
   `alloc_shim.c`, test `engine: writer transaction allocation counts`, 1000 txns after a 200-txn warm-up):**

   | Writer txn shape | LMDB heap allocs per txn |
   |---|---|
   | 256 new puts (100 B values) | 1.71 (0.0067 per put; 1.5 KB/txn) |
   | 1 put | 1.00 |
   | 256 overwrites of existing keys | 2.00 |

   So allocation is **not zero but is O(1) per transaction, independent of the number of puts**: one
   `malloc` per transaction for the free-page list (`me_pghead` is rebuilt from the freeDB and freed at
   txn end), a second when page recycling needs it. The reader path (`begin`/`get`/cursor/`end`) is
   **exactly zero** allocations/frees (asserted over 2000 iterations). Reporting rather than failing,
   per §14.1; batching (§8.2 `apply.batch.max.entries`) amortises it to well under 0.01 allocs/put.
5. **Map size on tmpfs:** confirm `MDB_WRITEMAP` sizing stays sparse on tmpfs and ext4, and that
   only touched pages are charged to memory.
   **Measured (K02, test `engine: map stays sparse on ...`):** an 8 GiB map (`st_size` = 8589934592 B)
   with ~5.9 MB of data (40k puts of 100 B) has `st_blocks*512` = 5,947,392 B, equal to the engine's
   `used_bytes` (`(last_pgno+1)*page_size`), on `/dev/shm` (tmpfs), `/tmp` (tmpfs) and the project
   directory (btrfs). `MDB_WRITEMAP` therefore stays sparse on all three and only touched pages are
   charged. `WriteEnv.usedBytes()` is a high-water mark (recycled pages are reused first), which is the
   right input for `kv_map_used_pct`. ext4 was not available on the measurement host.
