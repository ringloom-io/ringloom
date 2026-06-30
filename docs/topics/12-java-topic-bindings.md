# 12 — Java topic bindings

**Goal:** Java FFM bindings for the persistent-topic service ABI defined in
[spec 09](09-service-client-api-and-tailer.md): registration, publishing (with
per-publish ack modes), subscription, borrowed-payload polling, ack querying, and
per-subscription maintenance. This is the Java side of §4 "Bindings" in spec 09.
**Modules:** `bindings/java/src/main/java/io/ringloom/service/*`; C ABI surface
in `include/ringloom_service.h` and `src/service/c_abi.zig`.
**Depends on:** 09.

The framework layer (`ringloom-framework-java`) consumes topics **only** through
these bindings. Runtime, polling, dispatch, annotations, and ack completion live
in the framework repo, not here. This spec covers the low-level binding only.

---

## 1. Native ABI recap (spec 09 §3)

The symbols the Java binding wraps (already specced in 09; restated here so this
doc is self-contained for binding work):

```c
typedef struct ringloom_topic_config {
    uint32_t size;                 // leading size, per ABI convention
    char     roll_scheme[16];      // ringloom-queue RollScheme name, e.g. "FAST_DAILY"
    uint32_t retention_cycles;     // 0 = keep all
    uint32_t flags;                // reserved, 0 for now
} ringloom_topic_config_t;

typedef enum ringloom_topic_start {
    RINGLOOM_TOPIC_START_EARLIEST = 0,
    RINGLOOM_TOPIC_START_LATEST   = 1,
} ringloom_topic_start_t;

// ack_mode is per publish, NOT part of topic config:
//   0 = fire_and_forget, 1 = replicate_once

// Producer
int  ringloom_register_topic_publication(
        ringloom_client_t* client,
        const ringloom_topic_config_t* cfg,
        const char* name,
        ringloom_topic_publisher_t** out);

int  ringloom_publish_to_topic(
        ringloom_topic_publisher_t* pub,
        const uint8_t* payload,
        size_t len,
        int64_t correlation_id,
        uint8_t ack_mode,           // 0 = fire_and_forget, 1 = replicate_once
        uint64_t* out_index);       // assigned publish index (replicate_once); ignored for f&f

int  ringloom_topic_is_acked(ringloom_topic_publisher_t* pub, uint64_t publish_index); // 1=acked,0=pending
void ringloom_unregister_topic_publication(ringloom_topic_publisher_t* pub);

// Subscriber
int  ringloom_subscribe_topic(
        ringloom_client_t* client,
        const char* name,
        ringloom_topic_start_t start,
        ringloom_topic_subscription_t** out);

int  ringloom_topic_poll(
        ringloom_topic_subscription_t* sub,
        const uint8_t** out_payload,   // borrowed, valid until next poll
        size_t* out_len,
        int64_t* out_index);           // ringloom-queue index of this entry

void ringloom_unsubscribe_topic(ringloom_topic_subscription_t* sub);
```

### 1.1 Maintenance symbol (new, for the framework prefetcher)

Spec 09 does not name a per-subscription maintenance entry point, but the
framework's prefetcher thread needs one to drive ringloom-queue `maintenancePoll`
off the poll hot path (see `topics-architecture.md` §6). Add:

```c
// Advances the subscription's ringloom-queue maintenance/cleaner work and
// pre-touches read pages ahead of the tailer. Safe to call from any thread that
// does not poll the same subscription concurrently. Returns native status; the
// number of work units actually performed is not surfaced (best-effort).
int ringloom_topic_subscription_maintenance_poll(
        ringloom_topic_subscription_t* sub,
        int max_work_units);
```

Semantics: this symbol maps directly onto the subscription's embedded
ringloom-queue tailer maintenance. It must **not** advance the read cursor the
poll path consumes — only page residency and the cleaner. The Zig side should
guard `max_work_units <= 0` as a no-op returning `RINGLOOM_OK`.

**ABi versioning:** these symbols are **additive**. Follow the repo's existing
policy: if additive symbols are allowed under the current ABI version, keep the
version and resolve the topic symbols lazily/optionally (see §5); otherwise bump
`ringloom_service_abi_version` and the Java `RingloomNative.ABI_VERSION`
together. **Recommended: keep the version; make topic symbol resolution optional
(§5).** This avoids forcing every consumer onto a new native build just for
topics, which are an opt-in feature.

---

## 2. New Java value types

All in package `io.ringloom.service`, mirroring the existing style (immutable
records where there is no native handle, plain `final class implements
AutoCloseable` for handle-backed types).

### 2.1 `TopicConfig`

Immutable record mirroring `ringloom_topic_config_t`:

```java
public record TopicConfig(String rollScheme, int retentionCycles, int flags) {
    public static final TopicConfig DEFAULT =
        new TopicConfig("FAST_DAILY", 0, 0);
    public TopicConfig {
        Objects.requireNonNull(rollScheme, "rollScheme");
        byte[] bytes = rollScheme.getBytes(StandardCharsets.UTF_8);
        if (bytes.length == 0 || bytes.length > 16) {
            throw new IllegalArgumentException("rollScheme must be 1..16 UTF-8 bytes");
        }
        if (retentionCycles < 0) {
            throw new IllegalArgumentException("retentionCycles must be non-negative");
        }
    }
}
```

### 2.2 `TopicStart` / `TopicAckMode`

Enums mirroring the native constants, with `nativeValue()` for the downcall:

```java
public enum TopicStart {
    EARLIEST(0),
    LATEST(1);
    // ... nativeValue()
}

public enum TopicAckMode {
    FIRE_AND_FORGET(0),
    REPLICATE_ONCE(1);
    // ... nativeValue()
}
```

### 2.3 `TopicPublishResult`

Small immutable record returned by the publish binding, carrying the native
status and the assigned publish index (meaningful only for `REPLICATE_ONCE`):

```java
public record TopicPublishResult(int status, long publishIndex) {
    public boolean ok() { return status == RingloomStatus.OK; }
}
```

The hot-path publish method should avoid allocating a result object; see §3.2.
`TopicPublishResult` is the ergonomic/profile path.

---

## 3. `TopicPublisher`

Handle-backed wrapper around `ringloom_topic_publisher_t`. Created from a
`RingloomClient`; closed via `unregister`.

```java
public final class TopicPublisher implements AutoCloseable {
    private final MemorySegment nativeHandle;
    private final AtomicBoolean closed;

    TopicPublisher(MemorySegment nativeHandle) { ... }

    /**
     * Hot-path fire-and-forget publish. Returns a {@link RingloomStatus} int.
     * No ack tracking; the index assigned by the leader is not surfaced.
     */
    public int publish(MemorySegment payload);

    /**
     * Hot-path publish with explicit ack mode. For REPLICATE_ONCE the assigned
     * publish index is written into {@code outIndexHolder[0]} so the caller can
     * poll {@link #isAcked(long)}; ignored for FIRE_AND_FORGET. Returns status.
     * Must not allocate on the hot path.
     */
    public int publish(MemorySegment payload, TopicAckMode ackMode, long correlationId, long[] outIndexHolder);

    /**
     * Ergonomic fire-and-forget copy publish (allocates native staging).
     */
    public TopicPublishResult publishOrThrow(byte[] payload);

    /**
     * Non-blocking ack check for REPLICATE_ONCE. True once the publish index has
     * been applied by >=1 replica (or appended, on a single-node broker). Mirrors
     * the native HWM feedback; never blocks.
     */
    public boolean isAcked(long publishIndex);

    /** The topic name this publisher was registered for. */
    public String topic();

    @Override public void close();   // ringloom_unregister_topic_publication
}
```

Rules (consistent with `RingloomClient`):

1. `publish(MemorySegment)` and `publish(..., ackMode, ..., outIndexHolder)` are
   the hot-path entry points and must return status ints without allocating.
2. `outIndexHolder` is a caller-owned `long[1]` reused across calls — the binding
   must not allocate it. `null` is rejected.
3. `payload == null` is treated as an empty payload (length 0), mirroring
   `RingloomClient.send`.
4. `publishOrThrow(byte[])` copies into a confined arena and throws on non-OK;
   it is documented as allocating and is not a hot-path method.
5. `close()` is idempotent and calls `ringloom_unregister_topic_publication`.

### 3.1 Registration entry point

`TopicPublisher` is created via `RingloomClient`, adding a method symmetric to
`createClient`:

```java
public TopicPublisher registerTopicPublication(String topicName, TopicConfig config);
```

This sends `RegisterTopicPublicationMsg` over the control ring buffer and awaits
`TopicPublicationResponseMsg` (spec 09 §1). It allocates native staging for the
topic name and config and is a startup-only path. It must throw a clear
`RingloomException` with the broker's status for `config_mismatch`, `collision`,
and `disabled` outcomes (spec 03 §2 `TopicPublicationResponseMsg.status`).

### 3.2 Hot-path publish binding (status-only)

```java
public int publish(MemorySegment payload, TopicAckMode ackMode, long correlationId, long[] outIndexHolder) {
    ensureOpen();
    Objects.requireNonNull(ackMode, "ackMode");
    Objects.requireNonNull(outIndexHolder, "outIndexHolder");
    MemorySegment segment = payload == null ? MemorySegment.NULL : payload;
    try (Arena arena = Arena.ofConfined()) {
        MemorySegment outIndex = arena.allocate(ValueLayout.JAVA_LONG);
        int status = RingloomNative.topicPublish(
            nativeHandle,
            RingloomNative.payloadPointer(segment), segment.byteSize(),
            correlationId,
            ackMode.nativeValue(),
            outIndex);
        outIndexHolder[0] = outIndex.get(ValueLayout.JAVA_LONG, 0);
        return status;
    }
}
```

`publish(MemorySegment)` delegates with `FIRE_AND_FORGET`, correlation id `0`,
and a discarded index holder (a class-owned `long[1]`, allocated once at
construction — not per call).

---

## 4. `TopicSubscription`

Handle-backed wrapper around `ringloom_topic_subscription_t`. Created from a
`RingloomClient`; closed via `unsubscribe`.

```java
public final class TopicSubscription implements AutoCloseable {
    private final MemorySegment nativeHandle;
    private final AtomicBoolean closed;
    private final String topic;

    TopicSubscription(MemorySegment nativeHandle, String topic) { ... }

    /**
     * Polls one message. On success writes the borrowed payload address/length
     * into {@code out} and returns {@link RingloomStatus#OK}. When no message is
     * available returns a distinct status (see §4.1). The payload is valid only
     * until the next {@link #poll(TopicPollResult)} on this subscription.
     */
    public int poll(TopicPollResult out);

    /**
     * Drives ringloom-queue maintenance/cleaner work and read-page pre-touch for
     * this subscription's tailer. Called by the framework prefetcher thread.
     * Returns native status. Best-effort; never advances the read cursor.
     */
    public int maintenancePoll(int maxWorkUnits);

    public String topic();
    public boolean closed();

    @Override public void close();   // ringloom_unsubscribe_topic
}
```

### 4.1 `TopicPollResult` and the empty-poll status

`ringloom_topic_poll` returns borrowed pointers into native memory. The Java
binding must not copy the payload on the hot path. Use a reusable result holder:

```java
public final class TopicPollResult {
    private long payloadAddress;
    private long payloadLength;
    private long index;

    void refreshFromNative(MemorySegment outPayload, long outLen, long outIndex) { ... }

    /** Borrowed, valid only until the next poll of the owning subscription. */
    public MemorySegment payloadSegment() {
        return MemorySegment.ofAddress(payloadAddress).reinterpret(payloadLength);
    }
    public long index() { return index; }
}
```

The empty-poll case: spec 09's `ringloom_topic_poll` needs a return value that
means "no message available (tailer at tip)". Extend `RingloomStatus` (or reuse
an existing value) with a documented `NOT_READY`/`END_OF_HISTORY` code used
exclusively by topic poll. **Recommended: add `RingloomStatus.NOT_READY = <next>`
on the native side and surface it as-is; do not throw.** The framework poll loop
treats it as "subscription idle, move on" (see framework dispatch spec).

If the native ABI reuses `0` for "no message" and a non-zero for success, the
binding must document the mapping explicitly and never confuse it with an error.

### 4.2 Subscription entry point

```java
public TopicSubscription subscribeTopic(String topicName, TopicStart start);
```

Sends `SubscribeTopicMsg` and awaits `TopicSubscriptionResponseMsg` (spec 09 §2,
spec 03 §4). The response carries `queue_dir` and `start_index`, which the
native runtime uses internally to open the ringloom-queue tailer; the Java handle
hides this. Throws for `unknown_topic` and `disabled`.

### 4.3 Poll binding (zero-copy borrow)

```java
public int poll(TopicPollResult out) {
    ensureOpen();
    Objects.requireNonNull(out, "out");
    try (Arena arena = Arena.ofConfined()) {
        MemorySegment outPayload = arena.allocate(RingloomNative.ADDRESS);
        MemorySegment outLen = arena.allocate(ValueLayout.JAVA_LONG);
        MemorySegment outIndex = arena.allocate(ValueLayout.JAVA_LONG);
        int status = RingloomNative.topicPoll(nativeHandle, outPayload, outLen, outIndex);
        if (status == RingloomStatus.OK) {
            out.refreshFromNative(
                outPayload.get(RingloomNative.ADDRESS, 0),
                outLen.get(ValueLayout.JAVA_LONG, 0),
                outIndex.get(ValueLayout.JAVA_LONG, 0));
        }
        return status;
    }
}
```

The confined arena allocations are scratch for out-parameters; they are not
per-message payload copies. (If a later optimization removes them — e.g. a
class-owned out-struct — the binding signature stays the same.)

### 4.4 Maintenance binding

```java
public int maintenancePoll(int maxWorkUnits) {
    ensureOpen();
    if (maxWorkUnits < 0) {
        throw new IllegalArgumentException("maxWorkUnits must be non-negative");
    }
    return RingloomNative.topicSubscriptionMaintenancePoll(nativeHandle, maxWorkUnits);
}
```

---

## 5. `RingloomNative` additions

Add method handles, function descriptors, and layout constants for the topic
symbols. Follow the existing static-initialization pattern exactly. Layout
constants:

```java
static final long TOPIC_CONFIG_SIZE = 28L;   // u32 size + 16 roll + u32 retention + u32 flags
static final long TOPIC_CONFIG_SIZE_OFFSET = 0L;
static final long TOPIC_CONFIG_ROLL_SCHEME_OFFSET = 4L;
static final long TOPIC_CONFIG_RETENTION_OFFSET = 20L;
static final long TOPIC_CONFIG_FLAGS_OFFSET = 24L;
```

(`TOPIC_CONFIG_SIZE` must match the Zig `@sizeOf(ringloom_topic_config_t)`; add a
comptime assert on the Zig side and a Java-side test that encodes a known value
and reads it back.)

Downcall descriptors:

```java
// int ringloom_register_topic_publication(client, cfg*, name, name_len, out**)
FunctionDescriptor.of(ValueLayout.JAVA_INT, ADDRESS, ADDRESS, ADDRESS, ValueLayout.JAVA_LONG, ADDRESS)

// int ringloom_publish_to_topic(pub*, payload, len, correlation_id, ack_mode, out_index*)
FunctionDescriptor.of(ValueLayout.JAVA_INT, ADDRESS, ADDRESS, ValueLayout.JAVA_LONG,
                      ValueLayout.JAVA_LONG, ValueLayout.JAVA_BYTE, ADDRESS)

// int ringloom_topic_is_acked(pub*, publish_index)
FunctionDescriptor.of(ValueLayout.JAVA_INT, ADDRESS, ValueLayout.JAVA_LONG)

// void ringloom_unregister_topic_publication(pub*)
FunctionDescriptor.ofVoid(ADDRESS)

// int ringloom_subscribe_topic(client*, name, name_len, start, out**)
FunctionDescriptor.of(ValueLayout.JAVA_INT, ADDRESS, ADDRESS, ValueLayout.JAVA_LONG,
                      ValueLayout.JAVA_BYTE, ADDRESS)

// int ringloom_topic_poll(sub*, out_payload*, out_len*, out_index*)
FunctionDescriptor.of(ValueLayout.JAVA_INT, ADDRESS, ADDRESS, ADDRESS, ADDRESS)

// void ringloom_unsubscribe_topic(sub*)
FunctionDescriptor.ofVoid(ADDRESS)

// int ringloom_topic_subscription_maintenance_poll(sub*, max_work_units)
FunctionDescriptor.of(ValueLayout.JAVA_INT, ADDRESS, ValueLayout.JAVA_INT)
```

### 5.1 Optional symbol resolution

Because topic symbols are additive, resolve them with a tolerance flag so a
native build predating topics does not fail `RingloomNative` class init. Add a
helper alongside the existing `downcall`:

```java
private static MethodHandle optionalDowncall(String symbol, FunctionDescriptor descriptor) {
    return SYMBOLS.find(symbol).map(address -> LINKER.downcallHandle(address, descriptor)).orElse(null);
}
```

The eight topic handles are resolved this way; a package-private static boolean
`TOPIC_SYMBOLS_PRESENT` records whether all eight resolved. Public topic methods
on `RingloomClient` (`registerTopicPublication`, `subscribeTopic`) must throw a
clear `IllegalStateException` ("native library does not support topics; rebuild
libringloom_service with topics support") when `!TOPIC_SYMBOLS_PRESENT`. The
maintenance symbol in particular may be absent on older builds; the framework
disables the prefetcher thread with a warning in that case (spec 13, framework).

`RingloomNative` must expose these handles via package-private static methods
(`topicRegisterPublication`, `topicPublish`, `topicIsAcked`,
`topicUnregisterPublication`, `topicSubscribe`, `topicPoll`, `topicUnsubscribe`,
`topicSubscriptionMaintenancePoll`) following the `invokeExact` + `propagate`
pattern used by every other downcall.

---

## 6. Naming / length conventions

These mirror the existing `createClient`/`send` conventions and must be applied
consistently:

1. Topic names are UTF-8. Pass the byte length explicitly alongside the pointer
   (the ABI is not C-string based; see `ringloom_service_create_client`).
2. `payload == null` ⇒ `MemorySegment.NULL` + length `0`.
3. All out-parameters use confined arenas; nothing persists beyond the call.
4. Status ints are returned raw; ergonomic `*OrThrow` variants call
   `RingloomNative.throwForStatus`.

---

## 7. Tests

Mirror the existing binding integration tests (`RingloomLocalIpcIT`,
`RingloomClientTargetsIT`) and the allocation test
(`RingloomHotPathAllocationIT`). A topics-enabled test broker is required; extend
`TestBroker` / `scripts/start-test-broker.sh` to set `broker.topics.enabled =
true` and `broker.topics.path`.

### 7.1 Functional tests

1. `registerTopicPublication(name, cfg)` against a topics-enabled broker returns a
   `TopicPublisher`; a second registration with a **mismatched** config throws
   with `config_mismatch`; a duplicate with the **same** config is idempotent.
2. `subscribeTopic(name, EARLIEST)` returns a `TopicSubscription`; subscribe +
   publish N from a second service ⇒ `poll` returns the exact payloads in order,
   with monotonically increasing `index`.
3. `LATEST` subscriber misses pre-existing messages and sees only messages
   published after subscribe.
4. `publish(payload)` (FIRE_AND_FORGET) returns `OK`; subscriber still sees it.
5. `replicate_once`: `publish(..., REPLICATE_ONCE, ..., outIndex)` returns `OK`
   and `outIndex[0]` is set; `isAcked(outIndex[0])` flips from `false` to `true`
   on a single-node topics-enabled broker (acked on leader append) and after a
   replica applies on a multi-node broker.
6. Borrowed-view lifetime: `TopicPollResult.payloadSegment()` is valid until the
   next `poll`; copying out of it works; a second `poll` invalidates the first.
7. `maintenancePoll(n)` returns `OK` and does not corrupt concurrent poll; with
   no data it is a no-op.
8. `close()` on publisher/subscription is idempotent and releases the handle
   (subsequent operations throw `IllegalStateException`).
9. Against a broker with topics **disabled**: `registerTopicPublication` throws
   with `disabled`; `subscribeTopic` throws with `disabled`.
10. With topic symbols absent (`!TOPIC_SYMBOLS_PRESENT`, simulated by an older
    native build): `registerTopicPublication`/`subscribeTopic` throw the clear
    "native library does not support topics" error.

### 7.2 Allocation tests

1. Steady-state `publish(MemorySegment)` and `publish(..., REPLICATE_ONCE, ...,
   outIndexHolder)` with a reused `long[1]` allocate nothing (extend
   `RingloomHotPathAllocationIT`).
2. Steady-state `poll(result)` with a reused `TopicPollResult` allocates nothing
   on the OK path.
3. `maintenancePoll` allocates nothing.
4. `registerTopicPublication` / `subscribeTopic` / `publishOrThrow(byte[])` are
   explicitly excluded from hot-path allocation guarantees (startup/ergonomic).

### 7.3 Wire/layout tests

1. `TOPIC_CONFIG_SIZE` matches the native `ringloom_topic_config_t` size; encode
   `TopicConfig("FAST_DAILY", 7, 0)` and read back all fields through the layout
   constants.
2. `TopicStart` / `TopicAckMode` `nativeValue()` match the C enum values.

---

## 8. Acceptance criteria

1. `TopicPublisher` and `TopicSubscription` expose the full spec-09 surface plus
   `maintenancePoll`, with hot-path methods returning status ints and allocating
   nothing.
2. Topic symbols resolve optionally; missing topic support fails topic calls with
   a clear error rather than failing `RingloomNative` class init.
3. Subscribers read the local replica/master directly via `poll` (zero broker
   round trip per message) and observe broadcast order.
4. `replicate_once` publishes return an index and complete `isAcked` after the
   documented replication/append rule.
5. All functional, allocation, and layout tests pass against a topics-enabled
   test broker.
6. Existing non-topic binding tests and the `ABI_VERSION` contract are unchanged
   (additive symbols, no version bump unless the repo's policy requires one).

---

## 9. Cross-repo handoff

This binding is consumed by the framework repo
(`ringloom-framework-java`) under `docs/impl/12-topic-runtime-and-polling.md`
onward. The framework:

- Calls `RingloomClient.registerTopicPublication` / `subscribeTopic` at startup.
- Drives `TopicSubscription.poll` from `RingloomRuntime.pollTopics()`.
- Drives `TopicSubscription.maintenancePoll` from its prefetcher thread.
- Drives `TopicPublisher.publish(..., REPLICATE_ONCE, ...)` and
  `isAcked`/control-plane HWM feedback to complete `AckCallback`s (spec 13/15,
  framework).

Any change to the symbol names, the `ringloom_topic_config_t` layout, or the
empty-poll status code in this spec must be reflected in the framework specs in
lockstep.
