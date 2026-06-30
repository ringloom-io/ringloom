# 01 — Topic Identity & Config

**Goal:** Deterministic topic IDs and an immutable per-topic configuration record.
**Modules:** `src/broker/topics/topic_id.zig`, `src/broker/topics/topic_config.zig`.
**Depends on:** nothing.

---

## 1. Topic ID

```zig
// topic_id.zig
pub const TopicId = u64;

/// Deterministic, coordination-free. Every broker computes the same id from the name.
pub fn topicIdOf(name: []const u8) TopicId {
    // 64-bit hash. Use std.hash.Wyhash with a FIXED seed (0) so all nodes/binaries agree.
    return std.hash.Wyhash.hash(0, name);
}
```

Rules:

- `topic_id` is a **distinct ID space** from `service_id` (`u16`). Never route topic frames by
  `target_service_id`.
- The hash is computed identically on every broker and every service client → no allocation of IDs,
  no propagation of an ID→name table required for *agreement* (the table is still kept for
  collision detection and observability, see §3 and spec 02).
- `topic_id == 0` is reserved/invalid (treat a name hashing to 0 by rejecting; probability ~2⁻⁶⁴,
  but assert and fail closed).

## 2. Topic config

Ack mode is selected **per publish** (see spec 04 `TopicPublishHeader`), not stored per topic.
`TopicConfig` therefore carries only the immutable ringloom-queue geometry.

```zig
// topic_config.zig

/// Per-PUBLISH acknowledgement mode (carried in TopicPublishHeader, spec 04 — NOT in TopicConfig).
pub const AckMode = enum(u8) {
    fire_and_forget = 0,  // default: no ack, never waits
    replicate_once = 1,   // ack once ≥1 replica applies (single-node broker → ack on leader append)
    // reserved: quorum_durable = 2
};

/// Immutable after first creation. Defines the ringloom-queue geometry; MUST be
/// identical on leader and every replica or index-exact replication NACKs.
pub const TopicConfig = extern struct {
    roll_scheme_name: [16]u8,     // e.g. "FAST_DAILY" right-padded with 0
    retention_cycles: u32,        // 0 = keep indefinitely
    flags: u32,                   // bit0 = use_huge_pages
    _reserved: [4]u8 = .{0,0,0,0},

    pub fn rollSchemeName(self: *const TopicConfig) []const u8 { ... } // trim trailing 0
    pub fn eqlForReplication(self: *const TopicConfig, other: *const TopicConfig) bool {
        // Compare roll_scheme_name + retention + huge-pages flag (the full geometry).
    }
};
```

## 3. First-creation-wins validation

When a registration arrives for a `name`:

1. `id = topicIdOf(name)`.
2. If the registry already has `id`:
   - If the stored **name != name** → **collision** → reject with `error.TopicIdCollision`
     (extremely unlikely; fail closed so two different names never share a queue).
   - Else if stored `TopicConfig` is not equal to the requested config → reject with
     `error.TopicConfigMismatch` (first-creation-wins; config is immutable).
   - Else → idempotent success, return the existing record.
3. Else → create (spec 02).

Because ack mode is per publish, it plays no part in the first_wins decision; geometry equality
(`eqlForReplication`) IS the full config identity.

## 4. Tests

- `topicIdOf` is stable across calls and equals a hand-computed Wyhash for a known string.
- Distinct names → distinct ids (sample set); a name hashing to 0 is rejected.
- `TopicConfig` is exactly 32 bytes (`comptime` assert); `rollSchemeName` round-trips.
- `eqlForReplication` compares full geometry; differing geometry is rejected by first_wins.
