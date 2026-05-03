# 16 — Service C ABI & Java FFM Bindings

> **Prerequisites:** [08 — Service ↔ Broker IPC](08-service-ipc.md),
> [13 — Library Split & Packaging](13-library-split-and-packaging.md),
> [14 — Broker Executable & Startup](14-broker-executable-and-startup.md),
> [15 — End-to-End & Performance Testing](15-end-to-end-and-performance-testing.md)
>
> This document specifies how to expose `ringloom_service` as a shared native
> library with a stable C ABI, then build Java bindings on top of that ABI using
> the Java Foreign Function & Memory API.

The service runtime is currently a Zig module imported by Zig service
executables. The next packaging step is to make the service runtime usable from
other languages without coupling those languages to Zig source layout, Zig
calling conventions, or Zig error unions.

The first non-Zig consumer is Java. Java must call the native library through
the FFM API and exercise the same service lifecycle as a Zig service: connect to
a locally running broker, register, discover peers, send messages, receive
messages, and shut down cleanly.

---

## Table of Contents

1. [Goals](#1-goals)
2. [Non-Goals](#2-non-goals)
3. [Target Repository Layout](#3-target-repository-layout)
4. [Native Package Outputs](#4-native-package-outputs)
5. [C ABI Design](#5-c-abi-design)
   1. [ABI Principles](#51-abi-principles)
   2. [Header File](#52-header-file)
   3. [Types](#53-types)
   4. [Status Codes](#54-status-codes)
   5. [Lifecycle API](#55-lifecycle-api)
   6. [Client and Send API](#56-client-and-send-api)
   7. [Zero-Copy Claim/Commit API](#57-zero-copy-claimcommit-api)
   8. [Polling Receive API](#58-polling-receive-api)
   9. [Diagnostics API](#59-diagnostics-api)
   10. [Threading Contract](#510-threading-contract)
   11. [Ownership and Lifetime](#511-ownership-and-lifetime)
6. [Zig Implementation Plan](#6-zig-implementation-plan)
7. [Build System Specification](#7-build-system-specification)
8. [Java Binding Specification](#8-java-binding-specification)
9. [Java Integration Tests](#9-java-integration-tests)
10. [Broker Startup Helper Script](#10-broker-startup-helper-script)
11. [Validation and Acceptance Criteria](#11-validation-and-acceptance-criteria)
12. [Implementation Phases](#12-implementation-phases)
13. [Open Decisions](#13-open-decisions)

---

## 1. Goals

1. Produce an installed shared native library for the service runtime:
   - canonical library target name: `ringloom_service`
   - Linux installed file: `zig-out/lib/libringloom_service.so`
   - macOS installed file: `zig-out/lib/libringloom_service.dylib`
   - optional static archive for C embedders: `zig-out/lib/libringloom_service.a`

2. Produce an installed C header:
   - `zig-out/include/ringloom_service.h`
   - checked-in source: `include/ringloom_service.h`

3. Expose a stable C ABI that can be consumed from C, Java FFM, and future
   bindings without depending on Zig-specific types.

4. Keep native handles opaque:
   - `ringloom_service_t`
   - `ringloom_client_t`

5. Provide C ABI coverage for the initial service use cases:
   - start a service and register with a broker
   - stop and destroy the service
   - inspect assigned service/node identity
   - create a client for a named target service
   - send payload bytes to the target service with copy-based convenience APIs
   - claim writable ring-buffer memory, write directly into it, and commit or
     abort the claim
   - create a service-side message consumer
   - poll inbound messages from the caller's thread
   - expose inbound payload memory as borrowed raw bytes without copying
   - retrieve native error details

6. Implement Java bindings under `bindings/java` using the Java FFM API.

7. Add Java integration tests that run against a real local broker process.

8. Add a reusable broker startup helper script under `scripts/` for non-Zig
   binding tests.

9. Keep the Java hot path allocation-free:
   - no per-message Java object allocation while polling
   - no per-send allocation for zero-copy claim/commit
   - no implicit payload copies
   - no exception allocation for expected send/poll failures

10. Prefer integer status/error codes on hot-path APIs. Exceptions are acceptable
    for startup, configuration, programmer errors, and convenience wrappers, but
    not for expected runtime outcomes such as buffer full, no instance,
    backpressure, or peer disconnected.

---

## 2. Non-Goals

1. This work does **not** expose the broker runtime through C ABI.
   Only the service-side runtime is in scope.

2. This work does **not** require a static library for Java. Java FFM loads
   symbols from a process image or shared library; it cannot directly load a
   static archive. A static archive may be produced as an optional packaging
   artifact for C/C++ embedders, but the Java binding requires the shared
   library.

3. This work does **not** expose internal ring-buffer or metadata-file structs
   directly. Those remain implementation details in `ringloom_common` and
   `ringloom_service`.

4. This work does **not** introduce Java serialization, schemas, or generated
   message codecs. Payloads are byte arrays or memory segments.

5. This work does **not** make Java part of the default `zig build test` path.
   Java tests should have an explicit build/test command because they require a
   JDK and a local broker process.

6. This work does **not** provide lifetime safety for borrowed payload memory
   after a receive poll handler returns. Java users who need to retain payload
   bytes must copy them before returning from the handler.

7. This work does **not** make exception-driven Java APIs the primary hot-path
   surface. Exception-throwing methods may exist as convenience wrappers, but
   the performance API must return integer status codes.

---

## 3. Target Repository Layout

```text
ringloom/
├── build.zig
├── include/
│   └── ringloom_service.h
├── src/
│   └── service/
│       ├── root.zig
│       ├── ringloom_engine.zig
│       ├── service_client.zig
│       └── c_abi.zig
├── bindings/
│   └── java/
│       ├── README.md
│       ├── build.gradle.kts
│       ├── settings.gradle.kts
│       └── src/
│           ├── main/java/io/ringloom/service/
│           │   ├── RingloomService.java
│           │   ├── RingloomClient.java
│           │   ├── BufferClaim.java
│           │   ├── MessageConsumer.java
│           │   ├── RingloomMessage.java
│           │   ├── RingloomException.java
│           │   ├── RingloomNative.java
│           │   ├── RingloomStatus.java
│           │   ├── ServiceConfig.java
│           │   └── MessageHandler.java
│           └── test/java/io/ringloom/service/
│               ├── RingloomServiceLifecycleIT.java
│               ├── RingloomLocalIpcIT.java
│               └── RingloomNativeSmokeIT.java
└── scripts/
    └── start-test-broker.sh
```

`src/service/c_abi.zig` is the only Zig source file that should export C ABI
symbols. It acts as an adapter between stable C types and the internal Zig
service runtime.

---

## 4. Native Package Outputs

The native packaging step must install:

| Output | Path | Purpose |
|---|---|---|
| Shared library | `zig-out/lib/libringloom_service.so` or `zig-out/lib/libringloom_service.dylib` | Primary FFM runtime artifact loaded by Java and other dynamic FFI consumers |
| Static library | `zig-out/lib/libringloom_service.a` | Optional archive for C/C++ embedders or statically linked downstream launchers |
| Header | `zig-out/include/ringloom_service.h` | Stable C ABI contract |
| Broker executable | `zig-out/bin/ringloom-broker` | Required by integration tests |
| Test services | `zig-out/bin/ringloom-test-*` | Optional fixtures for Java integration scenarios |

The shared library must include the code needed by the service runtime and its
dependency on `ringloom_common`. It must not link broker runtime or TCP runtime
code. Java FFM tests load this shared library directly; no JNI shim is required.

---

## 5. C ABI Design

### 5.1 ABI Principles

The ABI must follow these rules:

1. Use only C-compatible scalar types, pointers, and `extern struct` layouts.
2. Do not expose Zig slices, error unions, optionals, allocators, tagged unions,
   or function pointers with Zig calling convention.
3. Do not expose internal struct definitions.
4. Return status codes from all fallible functions.
5. Return allocated/native objects through out-parameters.
6. Accept all strings as UTF-8 pointer + byte length.
7. Accept copy-based payloads as byte pointer + byte length.
8. Expose zero-copy sends through an explicit claim/commit/abort API.
9. Expose receives through an explicit polling API driven by the caller's
   thread.
10. Keep polled message payload memory valid only for the duration of the poll
    handler call.
11. Make repeated destroy/free calls safe only when passed `NULL`.
12. Never let Zig panics cross the ABI boundary.

The C ABI should be boring, explicit, and versioned. All richer language
bindings should adapt their language-native ergonomics to this low-level ABI
rather than expanding the native surface prematurely.

### 5.2 Header File

Source path:

```text
include/ringloom_service.h
```

Installed path:

```text
zig-out/include/ringloom_service.h
```

The header must:

- include only standard C headers (`stdint.h`, `stddef.h`, `stdbool.h`)
- support C and C++ consumers via `extern "C"`
- define an ABI version macro
- declare opaque handles
- declare status codes and exported functions
- document ownership for every pointer argument

Header skeleton:

```c
#ifndef RINGLOOM_SERVICE_H
#define RINGLOOM_SERVICE_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define RINGLOOM_SERVICE_ABI_VERSION 1u

typedef struct ringloom_service ringloom_service_t;
typedef struct ringloom_client ringloom_client_t;
typedef struct ringloom_message_consumer ringloom_message_consumer_t;

typedef enum ringloom_status {
    RINGLOOM_OK = 0,
    RINGLOOM_ERR_INVALID_ARGUMENT = 1,
    RINGLOOM_ERR_OUT_OF_MEMORY = 2,
    RINGLOOM_ERR_BROKER_NOT_FOUND = 3,
    RINGLOOM_ERR_REGISTRATION_TIMEOUT = 4,
    RINGLOOM_ERR_BUFFER_FULL = 5,
    RINGLOOM_ERR_NO_AVAILABLE_INSTANCE = 6,
    RINGLOOM_ERR_BACKPRESSURE = 7,
    RINGLOOM_ERR_PEER_DISCONNECTED = 8,
    RINGLOOM_ERR_CLAIM_NOT_ACTIVE = 9,
    RINGLOOM_ERR_MESSAGE_TOO_LONG = 10,
    RINGLOOM_ERR_INTERNAL = 255
} ringloom_status_t;

typedef struct ringloom_service_config {
    const char *storage_path;
    size_t storage_path_len;
    const char *group;
    size_t group_len;
    const char *service_name;
    size_t service_name_len;
    int16_t broker_node_id;
    bool blocking_mode;
    int32_t heartbeat_timeout_ms;
    size_t control_buffer_length;
    size_t messages_buffer_length;
    bool leader_election_enabled;
} ringloom_service_config_t;

typedef struct ringloom_message {
    int64_t correlation_id;
    int16_t source_node_id;
    int16_t source_service_id;
    int16_t target_node_id;
    int16_t target_service_id;
    uint16_t template_id;
    uint8_t flags;
    const uint8_t *payload;
    size_t payload_len;
} ringloom_message_t;

typedef struct ringloom_buffer_claim {
    uint8_t *payload;
    size_t payload_len;
    uintptr_t _ring_buffer;
    size_t _header_index;
    int32_t _record_length;
    uint8_t _active;
} ringloom_buffer_claim_t;

typedef void (*ringloom_message_handler_t)(
    void *user_data,
    const ringloom_message_t *message
);

uint32_t ringloom_service_abi_version(void);

ringloom_status_t ringloom_service_start(
    const ringloom_service_config_t *config,
    ringloom_service_t **out_service
);

void ringloom_service_stop(ringloom_service_t *service);
void ringloom_service_destroy(ringloom_service_t *service);

ringloom_status_t ringloom_service_id(
    const ringloom_service_t *service,
    int32_t *out_service_id
);

ringloom_status_t ringloom_service_node_id(
    const ringloom_service_t *service,
    int16_t *out_node_id
);

ringloom_status_t ringloom_service_create_message_consumer(
    ringloom_service_t *service,
    ringloom_message_consumer_t **out_consumer
);

void ringloom_message_consumer_destroy(ringloom_message_consumer_t *consumer);

ringloom_status_t ringloom_message_consumer_poll(
    ringloom_message_consumer_t *consumer,
    ringloom_message_handler_t handler,
    void *user_data,
    uint32_t limit,
    uint32_t *out_count
);

ringloom_status_t ringloom_service_create_client(
    ringloom_service_t *service,
    const char *target_service_name,
    size_t target_service_name_len,
    ringloom_client_t **out_client
);

void ringloom_client_destroy(ringloom_client_t *client);

ringloom_status_t ringloom_client_send(
    ringloom_client_t *client,
    const uint8_t *payload,
    size_t payload_len
);

ringloom_status_t ringloom_client_try_claim(
    ringloom_client_t *client,
    uint16_t template_id,
    size_t payload_len,
    ringloom_buffer_claim_t *out_claim
);

ringloom_status_t ringloom_buffer_claim_commit(
    ringloom_buffer_claim_t *claim
);

ringloom_status_t ringloom_buffer_claim_abort(
    ringloom_buffer_claim_t *claim
);

ringloom_status_t ringloom_client_send_to(
    ringloom_client_t *client,
    int32_t target_service_id,
    const uint8_t *payload,
    size_t payload_len
);

ringloom_status_t ringloom_client_send_to_leader(
    ringloom_client_t *client,
    const uint8_t *payload,
    size_t payload_len
);

const char *ringloom_status_string(ringloom_status_t status);
const char *ringloom_last_error_message(void);

#ifdef __cplusplus
}
#endif

#endif
```

### 5.3 Types

#### `ringloom_service_t`

Opaque native handle that owns:

- `RingLoomEngine`
- allocator used for the engine and ABI wrapper allocations
- duplicated copies of config strings needed after `ringloom_service_start`
- service metadata and broker metadata mappings

#### `ringloom_client_t`

Opaque native handle that references:

- parent `ringloom_service_t`
- internal `ServiceClient`

The parent service must outlive all clients created from it. Destroying the
service while clients are still alive is invalid and should return a diagnostic
in debug builds if detected.

#### `ringloom_message_consumer_t`

Opaque native handle that references the service's inbound messages ring buffer.
It does not own a native thread. The caller drives progress by invoking
`ringloom_message_consumer_poll(...)`.

Only one consumer may poll a given service's messages ring buffer at a time.
This preserves the MPSC ring buffer's single-consumer invariant.

#### `ringloom_service_config_t`

Value struct passed by pointer into `ringloom_service_start`.

Required fields:

- `service_name`
- `service_name_len`

Optional fields:

- `storage_path`; default is RingLoom's default storage path
- `group`; default is `"default"`
- numeric fields may be `0` to request defaults, except `broker_node_id`, where
  `0` should also map to default broker node id `1`

All string data is copied during `ringloom_service_start`; the caller may free
or move its input memory after the function returns.

#### `ringloom_message_t`

Polling view of an inbound application message.

The payload pointer is borrowed directly from the service messages ring buffer.
It is valid only until the poll handler returns. Language bindings that need to
retain data must copy it during the handler call.

#### `ringloom_buffer_claim_t`

Stack-allocatable claim descriptor for zero-copy sends.

The public fields are:

- `payload`: writable memory in the selected target ring buffer
- `payload_len`: number of bytes the caller may write

The `_ring_buffer`, `_header_index`, `_record_length`, and `_active` fields are
ABI-private bookkeeping used by `ringloom_buffer_claim_commit` and
`ringloom_buffer_claim_abort`. Callers and bindings must treat them as opaque
and must not modify them.

### 5.4 Status Codes

The ABI must map Zig errors to stable C status values:

| Status | Typical Zig source |
|---|---|
| `RINGLOOM_OK` | success |
| `RINGLOOM_ERR_INVALID_ARGUMENT` | null out-param, missing service name, invalid lengths |
| `RINGLOOM_ERR_OUT_OF_MEMORY` | `error.OutOfMemory` |
| `RINGLOOM_ERR_BROKER_NOT_FOUND` | broker metadata open failure |
| `RINGLOOM_ERR_REGISTRATION_TIMEOUT` | registration response wait timeout |
| `RINGLOOM_ERR_BUFFER_FULL` | ring buffer write capacity failure |
| `RINGLOOM_ERR_NO_AVAILABLE_INSTANCE` | no discovered target service instance |
| `RINGLOOM_ERR_BACKPRESSURE` | flow-control rejection or timeout |
| `RINGLOOM_ERR_PEER_DISCONNECTED` | remote peer disconnected |
| `RINGLOOM_ERR_CLAIM_NOT_ACTIVE` | commit/abort called on an inactive or already completed claim |
| `RINGLOOM_ERR_MESSAGE_TOO_LONG` | payload length exceeds ring buffer maximum message size |
| `RINGLOOM_ERR_INTERNAL` | unexpected native runtime error |

`ringloom_status_string` returns static constant strings and never fails.

`ringloom_last_error_message` returns a thread-local string populated by the
last ABI call on the calling thread. It must never return `NULL`; if no error is
available it returns an empty string.

### 5.5 Lifecycle API

#### `ringloom_service_abi_version`

Returns `RINGLOOM_SERVICE_ABI_VERSION`.

Bindings must check this at load time and fail fast if the native ABI version is
unsupported.

#### `ringloom_service_start`

Responsibilities:

1. Validate pointers and required lengths.
2. Copy config strings into native-owned memory.
3. Convert C config to `ringloom_service.RingLoomEngine.ServiceConfig`.
4. Start the engine using the service runtime allocator in external-consumer
   mode.
5. Store the resulting native wrapper in `out_service`.

External-consumer mode is important for Java. It starts service registration,
heartbeat/control-plane handling, and metadata ownership, but it does not start
the native `MessageConsumer` thread. Inbound message progress is driven by
`ringloom_message_consumer_poll(...)` from Java or another host-language thread.

Failure behavior:

- `out_service` must be set to `NULL` before any fallible work.
- partially initialized state must be cleaned up.
- error details must be recorded in thread-local native error state.

#### `ringloom_service_stop`

Gracefully stops the service runtime:

1. stop control agent thread
2. unregister from broker
3. close metadata mappings

Calling stop multiple times should be safe. The first call performs the work;
subsequent calls are no-ops.

#### `ringloom_service_destroy`

Destroys the native wrapper. If the service has not already been stopped,
destroy must stop it first.

After destroy returns, all clients created from this service are invalid.

### 5.6 Client and Send API

#### `ringloom_service_create_client`

Creates or retrieves a target service client by name. This maps to
`RingLoomEngine.createClient`.

The native wrapper must keep enough state to destroy the returned client handle
without double-freeing internal clients owned by the engine registry.

Recommended first implementation:

- `ringloom_client_t` is a lightweight wrapper around a borrowed internal
  `*ServiceClient`
- `ringloom_client_destroy` frees only the wrapper
- the internal client remains owned by `ServiceClientRegistry`

#### `ringloom_client_send`

Sends bytes to one discovered instance using the service runtime load balancer.

This maps to `ServiceClient.send` and is a copy-based convenience API. It is
not the preferred hot-path API for Java.

#### `ringloom_client_send_to`

Sends bytes to a specific service id.

This maps to `ServiceClient.sendTo`.

#### `ringloom_client_send_to_leader`

Sends bytes to the current leader instance.

This maps to `ServiceClient.sendToLeader`.

### 5.7 Zero-Copy Claim/Commit API

The hot-path send API must expose the same memory claim pattern used by the
underlying ring buffer:

1. choose a target instance using the same load-balancing or target-specific
   rules as copy-based send
2. claim a contiguous payload region in the target ring buffer
3. return a writable pointer to the caller
4. let the caller write the payload directly into ring-buffer memory
5. publish the record with `commit`, or convert it to padding with `abort`

Primary function:

```c
ringloom_status_t ringloom_client_try_claim(
    ringloom_client_t *client,
    uint16_t template_id,
    size_t payload_len,
    ringloom_buffer_claim_t *out_claim
);
```

Commit/abort functions:

```c
ringloom_status_t ringloom_buffer_claim_commit(
    ringloom_buffer_claim_t *claim
);

ringloom_status_t ringloom_buffer_claim_abort(
    ringloom_buffer_claim_t *claim
);
```

Claim contract:

1. `out_claim` is caller-allocated and must be zeroed by the native function on
   failure.
2. On success, `out_claim->payload` points directly into the selected ring
   buffer.
3. Caller may write at most `out_claim->payload_len` bytes.
4. Exactly one of `commit` or `abort` must be called for every successful claim.
5. `commit` publishes the bytes already written by the caller.
6. `abort` marks the claimed record as padding so the consumer can skip it.
7. `commit` and `abort` make the claim inactive; repeated calls return
   `RINGLOOM_ERR_CLAIM_NOT_ACTIVE`.
8. Claim memory is not stable after commit/abort and must not be retained.

The first claim API may support only the load-balanced send path. Follow-up APIs
can add target-specific and leader-specific claims:

```c
ringloom_status_t ringloom_client_try_claim_to(...);
ringloom_status_t ringloom_client_try_claim_to_leader(...);
```

For a same-host target, the payload pointer refers to the target service's
messages ring buffer. For a remote target, it refers to the local broker send
ring buffer. In both cases, the caller writes directly into the ring buffer that
the next runtime component will consume.

### 5.8 Polling Receive API

The C ABI must not rely on a native message-consumer thread for Java services.
Instead, it exposes a polling consumer:

```c
ringloom_status_t ringloom_service_create_message_consumer(
    ringloom_service_t *service,
    ringloom_message_consumer_t **out_consumer
);

ringloom_status_t ringloom_message_consumer_poll(
    ringloom_message_consumer_t *consumer,
    ringloom_message_handler_t handler,
    void *user_data,
    uint32_t limit,
    uint32_t *out_count
);
```

Polling contract:

1. The caller owns the polling thread.
2. `poll` reads at most `limit` messages.
3. `poll` invokes `handler` synchronously on the caller's thread.
4. `out_count` receives the number of application messages delivered.
5. Payload memory in `ringloom_message_t` points directly into the service's
   messages ring buffer.
6. Payload memory is valid only until the handler returns.
7. After the handler returns, the native ring buffer reader may zero the record
   and advance the head position.
8. A handler that wants to retain bytes must copy them before returning.
9. There must be only one active polling consumer per service.

The current internal `RingBuffer.MessageHandler` exposes message type and
payload. The C ABI wants richer `ringloom_message_t` metadata. If the internal
consumer cannot provide full metadata yet, phase 1 may populate:

- `template_id` from the ring-buffer message type
- `payload` and `payload_len`
- all unavailable routing fields as `0`

The follow-up work should align `MessageConsumer` with
`MessageHeader.decode(...)` so polled messages receive source/target identity,
correlation id, flags, and template id consistently.

### 5.9 Diagnostics API

The ABI must expose two diagnostic layers:

1. Stable status codes for programmatic control flow.
2. Thread-local human-readable details for logs/tests.

Examples:

```c
ringloom_service_t *svc = NULL;
ringloom_status_t rc = ringloom_service_start(&cfg, &svc);
if (rc != RINGLOOM_OK) {
    fprintf(stderr, "start failed: %s: %s\n",
        ringloom_status_string(rc),
        ringloom_last_error_message());
}
```

Native error messages should include contextual information such as:

- missing or invalid field name
- broker metadata path that could not be opened
- registration timeout duration
- target service name for discovery/send failures

### 5.10 Threading Contract

In C ABI external-consumer mode, the service runtime starts native threads for:

- control agent

It does not start a native message-consumer thread. Java owns the message
polling thread and controls receive progress by calling
`MessageConsumer.poll(...)`.

C ABI thread-safety rules:

| Operation | Thread-safe? | Notes |
|---|---:|---|
| `ringloom_service_start` | no | caller owns initialization |
| `ringloom_service_stop` | yes | must be idempotent |
| `ringloom_service_destroy` | no | caller must coordinate with client users |
| `ringloom_service_create_client` | externally synchronized | first implementation may rely on engine registry assumptions |
| `ringloom_client_send*` | yes after creation | underlying ring-buffer writes are MPSC |
| `ringloom_client_try_claim*` | yes after creation | every successful claim must be committed or aborted |
| `ringloom_message_consumer_poll` | single consumer only | one polling thread per service messages ring buffer |

### 5.11 Ownership and Lifetime

Ownership rules must be simple enough for all bindings:

1. Caller owns input strings and payloads.
2. Native library copies config strings during service start.
3. Native library does not retain send payload pointers after send returns.
4. Native library owns service and client handles.
5. Caller must destroy every client handle.
6. Caller must destroy every service handle.
7. Service handle must outlive client handles.
8. Every successful buffer claim must be committed or aborted.
9. Claimed send memory is borrowed until commit/abort and must not be retained.
10. Polled receive payload is borrowed and must be copied by the binding if
    retained after the handler returns.

---

## 6. Zig Implementation Plan

Add:

```text
src/service/c_abi.zig
```

The file should:

1. import `ringloom_service` internals through local imports and
   `ringloom_common`
2. define C ABI handle wrappers
3. define `export fn` symbols matching `include/ringloom_service.h`
4. translate C config to `RingLoomEngine.ServiceConfig`
5. translate Zig errors to `ringloom_status_t`
6. populate thread-local error details
7. expose claim/commit/abort by adapting `RingBuffer.tryClaim`
8. expose polling receive by adapting `RingBuffer.read`
9. include compile-time layout checks for all exported structs mirrored from the
   C header

Recommended internal wrapper:

```zig
const ServiceHandle = struct {
    allocator: std.mem.Allocator,
    engine: ?*RingLoomEngine,
    storage_path: []u8,
    group: []u8,
    service_name: []u8,
    stopped: std.atomic.Value(bool),
};

const ClientHandle = struct {
    service: *ServiceHandle,
    client: *ServiceClient,
};

const MessageConsumerHandle = struct {
    service: *ServiceHandle,
    ring_buffer: RingBuffer,
};
```

The ABI layer should use a native allocator owned by the library. For debug
builds use `std.heap.DebugAllocator(.{}) = .init`; for release builds use an
allocator strategy already accepted in the project.

Exported functions should use C-compatible signatures. In Zig 0.15.x, prefer:

```zig
export fn ringloom_service_abi_version() u32 {
    return abi_version;
}
```

For polling, define a C-callable handler function pointer type:

```zig
const MessageHandler = ?*const fn (
    user_data: ?*anyopaque,
    message: *const RingloomMessage,
) callconv(.c) void;
```

The handler is invoked synchronously by `ringloom_message_consumer_poll` on the
calling thread. The ABI layer must not spawn a native consumer thread for Java
services.

Error handling requirements:

- no Zig error union crosses the ABI boundary
- no panic crosses the ABI boundary
- all invalid pointer arguments return `RINGLOOM_ERR_INVALID_ARGUMENT`
- all unexpected errors return `RINGLOOM_ERR_INTERNAL` with details

Tests to add in Zig:

1. `src/service/c_abi_test.zig` or tests inside `c_abi.zig`
2. ABI version returns expected value
3. invalid arguments return `RINGLOOM_ERR_INVALID_ARGUMENT`
4. status strings are non-null
5. config conversion applies defaults
6. C struct sizes match header assumptions

The service root should import `c_abi.zig` in its `comptime` test-discovery
block if the ABI tests live there.

---

## 7. Build System Specification

Update `build.zig` to add a shared library artifact using Zig 0.15.x build APIs.

Recommended structure:

```zig
const service_c_abi_mod = b.createModule(.{
    .root_source_file = b.path("src/service/c_abi.zig"),
    .target = target,
    .optimize = optimize,
    .imports = &.{
        .{ .name = "ringloom_common", .module = ringloom_common },
        .{ .name = "ringloom_service", .module = ringloom_service },
    },
});

const service_shared_lib = b.addLibrary(.{
    .name = "ringloom_service",
    .linkage = .dynamic,
    .root_module = service_c_abi_mod,
});

b.installArtifact(service_shared_lib);
```

Header installation:

```zig
const install_header = b.addInstallHeaderFile(
    b.path("include/ringloom_service.h"),
    "ringloom_service.h",
);
b.getInstallStep().dependOn(&install_header.step);
```

Add a dedicated build step:

```text
zig build service-c
```

`service-c` should depend on:

- shared library build
- header install
- C ABI tests

Add Java binding steps:

```text
zig build java-bindings
zig build test-java
```

`java-bindings` should:

- build/install native service library and header
- run the Java binding compile task

`test-java` should:

- build/install native service library
- build/install broker executable
- run Java integration tests

The Java build may be invoked through Gradle from `bindings/java`:

```zig
const java_test = b.addSystemCommand(&.{
    "sh",
    "-c",
    "cd bindings/java && ./gradlew test",
});
java_test.step.dependOn(b.getInstallStep());
```

If the repository does not want a Gradle wrapper checked in initially, use the
system `gradle` command and document the JDK/Gradle prerequisite. The preferred
long-term developer experience is a checked-in Gradle wrapper scoped to
`bindings/java`.

Optional static archive:

- If C/C++ embedders need static linking, add a second `addLibrary` target with
  `.linkage = .static`.
- Java FFM does not use the static archive. It loads the shared library from
  `zig-out/lib`.

Validation commands:

```bash
zig build service-c
zig build test
zig build test-java
```

---

## 8. Java Binding Specification

Java package:

```text
io.ringloom.service
```

Minimum Java version:

- Java 22 or newer for the finalized FFM API
- if Java 21 must be supported, tests must pass
  `--enable-preview --enable-native-access=ALL-UNNAMED`

Preferred baseline:

```text
Java 22+
```

### Main Java Types

### Java API Layers

The Java binding must expose two layers:

| Layer | Purpose | Allocation/error policy |
|---|---|---|
| Performance API | Hot path send/poll loops | no Java allocation, no payload copy, integer return codes |
| Convenience API | Setup, tests, simple examples | may allocate, may throw exceptions, may copy if method name makes it explicit |

The performance API is the primary API for production services. Convenience
methods must delegate to the performance API and must be documented as
unsuitable for latency-sensitive loops if they allocate, copy, or throw for
expected runtime outcomes.

Status code constants in Java must mirror `ringloom_status_t`:

```java
public final class RingloomStatus {
    public static final int OK = 0;
    public static final int INVALID_ARGUMENT = 1;
    public static final int OUT_OF_MEMORY = 2;
    public static final int BROKER_NOT_FOUND = 3;
    public static final int REGISTRATION_TIMEOUT = 4;
    public static final int BUFFER_FULL = 5;
    public static final int NO_AVAILABLE_INSTANCE = 6;
    public static final int BACKPRESSURE = 7;
    public static final int PEER_DISCONNECTED = 8;
    public static final int CLAIM_NOT_ACTIVE = 9;
    public static final int MESSAGE_TOO_LONG = 10;
    public static final int INTERNAL = 255;

    public static boolean isOk(int status) {
        return status == OK;
    }
}
```

Expected hot-path failures must be returned as these integer codes. They must
not allocate exceptions unless the caller chooses an explicit convenience
wrapper.

#### `ServiceConfig`

Immutable Java record or builder-backed class:

```java
public record ServiceConfig(
    String serviceName,
    String storagePath,
    String group,
    short brokerNodeId,
    boolean blockingMode,
    int heartbeatTimeoutMillis,
    long controlBufferLength,
    long messagesBufferLength,
    boolean leaderElectionEnabled
) {}
```

Provide defaults matching Zig:

- `storagePath`: RingLoom default storage path unless explicitly set
- `group`: `"default"`
- `brokerNodeId`: `1`
- `heartbeatTimeoutMillis`: `10_000`
- `controlBufferLength`: `65_536`
- `messagesBufferLength`: `1_048_576`

#### `RingloomService`

High-level AutoCloseable wrapper:

```java
public final class RingloomService implements AutoCloseable {
    public static RingloomService start(ServiceConfig config);
    public int serviceId();
    public short nodeId();
    public RingloomClient createClient(String targetServiceName);
    public MessageConsumer messageConsumer();
    public void stop();
    @Override public void close();
}
```

`close()` must be idempotent and call native destroy exactly once.

#### `RingloomClient`

High-level AutoCloseable wrapper:

```java
public final class RingloomClient implements AutoCloseable {
    public BufferClaim newClaim();
    public int tryClaim(int templateId, long payloadLength, BufferClaim claim);
    public int send(MemorySegment payload);
    public int sendTo(int targetServiceId, MemorySegment payload);
    public int sendToLeader(MemorySegment payload);

    // Convenience wrappers; not hot-path APIs.
    public void sendOrThrow(byte[] payload);
    public void sendOrThrow(MemorySegment payload);
    @Override public void close();
}
```

`newClaim()` allocates once and is intended for setup. The returned claim object
is reusable across sends. The hot path calls `tryClaim(..., claim)` with a
pre-created claim object and receives a `RingloomStatus` integer.

`tryClaim(...)` is the preferred zero-copy send API. It must not allocate or
copy payload bytes. It fills the caller-provided `BufferClaim` with a borrowed
payload view that points directly into the selected RingLoom ring buffer.

`send(MemorySegment)` is a copy-based convenience for callers that already own
native memory. `sendOrThrow(...)` wrappers may allocate/copy and may throw; they
must not be used in latency-sensitive loops.

#### `BufferClaim`

Hot-path zero-copy send wrapper:

```java
public final class BufferClaim implements AutoCloseable {
    public long payloadAddress();
    public long payloadLength();
    public MemorySegment payloadSegment();
    public int commit();
    public int abort();
    @Override public void close();
}
```

Usage:

```java
BufferClaim claim = client.newClaim();

int status = client.tryClaim(templateId, payloadLength, claim);
if (status == RingloomStatus.OK) {
    long address = claim.payloadAddress();
    long length = claim.payloadLength();
    // Write directly into RingLoom ring-buffer memory.
    status = claim.commit();
}
```

`payloadAddress()` and `payloadLength()` are the allocation-free hot-path
accessors. `payloadSegment()` is a convenience FFM view; if creating or resizing
that view allocates on the target Java version, it must be documented as not
hot-path-safe.

`commit()` and `abort()` return integer status codes. They must not throw for
expected claim lifecycle errors. `close()` must abort an active claim, making
try-with-resources safe for setup/tests, but production hot paths should usually
call `commit()`/`abort()` explicitly and reuse the claim.

After `commit`, `abort`, or `close`, the payload address/segment must be treated
as invalid by user code.

#### `MessageConsumer`

Java-owned receive loop driver:

```java
public final class MessageConsumer implements AutoCloseable {
    public int poll(MessageHandler handler, int limit);
    public int lastStatus();
    @Override public void close();
}
```

The application decides where this polling loop lives. For example:

```java
while (running) {
    int work = consumer.poll(handler, 256);
    if (work < 0) {
        int status = consumer.lastStatus();
        // Handle status without allocating an exception.
        continue;
    }
    idleStrategy.idle(work);
}
```

`poll(...)` returns a non-negative message count on success. On failure it
returns a negative value and stores the native `RingloomStatus` in
`lastStatus()`. This keeps the hot path primitive-only and avoids allocating a
result object or exception. There must be only one polling thread per service
consumer.

#### `MessageHandler`

```java
@FunctionalInterface
public interface MessageHandler {
    void onMessage(RingloomMessage message);
}
```

`MessageHandler` implementations used on the hot path must be pre-created and
must not capture per-message state that allocates.

`RingloomMessage` should expose:

- correlation id
- source node id
- source service id
- target node id
- target service id
- template id
- flags
- payload address as `long`
- payload length as `long`
- optional payload as borrowed `MemorySegment`

`RingloomMessage` must be a mutable reusable view owned by the binding, not a
new object allocated for each message. The polling implementation updates this
view before invoking the handler. Its payload address points directly at the
service's messages ring buffer and is valid only during the
`MessageHandler.onMessage(...)` call. Users who need to retain data must copy it
before returning from the handler.

Allocation-free hot-path accessors:

```java
public long payloadAddress();
public long payloadLength();
```

It may offer an explicit convenience method:

```java
public byte[] copyPayload();
public MemorySegment payloadSegment();
```

`copyPayload()` is intentionally explicit. `payloadSegment()` is allowed only as
a borrowed view; if it allocates a fresh Java object per message on the chosen
JDK, users should prefer the primitive address/length hot-path accessors.

#### `RingloomNative`

Internal low-level FFM binding:

- loads symbols from the native library
- declares `FunctionDescriptor`s
- declares `MemoryLayout`s for C structs
- performs ABI version check
- exposes native status codes directly to the performance API
- converts native status codes to Java exceptions only in convenience wrappers

It should not be public API.

### Native Library Loading

Java FFM loads symbols from a process image or shared library. It cannot
directly `dlopen` a static archive. Therefore the required native artifact for
Java is:

```text
zig-out/lib/libringloom_service.so    # Linux
zig-out/lib/libringloom_service.dylib # macOS
```

The Java binding should load the library from `ringloom.nativeLibDir` in tests
and allow production users to configure the path explicitly. It should not rely
on a static archive or JNI shim.

### Java Error Handling

The performance API must use integer status codes directly. It must not throw
exceptions for expected hot-path outcomes:

- buffer full
- no available service instance
- backpressure
- peer disconnected
- inactive claim
- message too long
- transient native/internal poll failure

Exception-throwing wrappers may exist for setup code, tests, and simple example
programs. Those wrappers map native status codes to Java exceptions:

| Native status | Java exception |
|---|---|
| `RINGLOOM_OK` | none |
| `RINGLOOM_ERR_INVALID_ARGUMENT` | `IllegalArgumentException` |
| `RINGLOOM_ERR_OUT_OF_MEMORY` | `OutOfMemoryError` |
| other non-zero status | `RingloomException` |

`RingloomException` should include:

- status code
- status name
- `ringloom_last_error_message()`

Rules:

1. Methods named `*OrThrow` may throw.
2. Startup/configuration APIs may throw because they are not hot path.
3. Hot-path methods (`tryClaim`, `commit`, `abort`, `send(MemorySegment)`,
   `poll`) return primitive status/count values.
4. Hot-path methods must not call `ringloom_last_error_message()` unless the
   user explicitly asks for diagnostics after observing an error code, because
   diagnostic string materialization may allocate.

### Java Hot-Path Allocation Policy

The Java binding must avoid GC pressure in send/receive loops.

Allocation-free hot-path requirements:

1. `RingloomClient.tryClaim(..., BufferClaim)` reuses caller-provided
   `BufferClaim`.
2. `BufferClaim.commit()` and `BufferClaim.abort()` return primitive status
   codes.
3. `MessageConsumer.poll(...)` reuses one internal `RingloomMessage` view for
   handler dispatch.
4. `RingloomMessage` exposes primitive payload address/length accessors.
5. `MessageHandler` receives a borrowed view and must not be wrapped in a newly
   allocated adapter per poll.
6. No `byte[]`, `MemorySegment`, result object, exception, `Optional`, boxed
   primitive, or lambda allocation may be required per message or per send on
   the performance API.

Allowed allocations:

- service/client/consumer construction
- `RingloomClient.newClaim()` during setup
- convenience methods such as `sendOrThrow(byte[])`, `copyPayload()`, or
  diagnostics retrieval
- test-only helpers

CI or benchmark validation should include an allocation-sensitive smoke test for
the Java hot path. The exact mechanism can be chosen during implementation
(JFR allocation events, async-profiler allocation mode, JMH allocation profiler,
or a custom stress test with GC counters), but the result must demonstrate no
steady-state per-message allocation from the RingLoom binding itself.

### Java Resource Safety

All native handles must be wrapped in `AutoCloseable`.

Recommended safety mechanisms:

1. use `AtomicBoolean closed` to make close idempotent
2. use `Cleaner` only as a last-resort leak guard, not as primary lifecycle
3. keep polling upcall stubs alive for the duration of each `poll` call
4. close clients before services in tests
5. close message consumers before services in tests
6. abort active `BufferClaim`s from `close()`
7. throw `IllegalStateException` if methods are called after close

---

## 9. Java Integration Tests

Java tests must run against a real local broker process. They must not mock the
native service runtime.

### Test prerequisites

Before Java tests run:

```bash
zig build install
zig build service-c
```

The Java test task must know:

- project root
- native library directory (`zig-out/lib`)
- broker executable path (`zig-out/bin/ringloom-broker`)
- test storage directory

Pass these through system properties:

```text
-Dringloom.projectRoot=...
-Dringloom.nativeLibDir=...
-Dringloom.brokerBin=...
```

### Test workspace

Each test class should create an isolated temp directory:

```text
/tmp/ringloom-java-it-XXXXXX/
├── config/
│   └── broker_1.properties
├── logs/
│   └── broker_1.log
└── storage/
    └── ringloom-java-test/
        └── services/
```

On failure, tests should print the workspace path and preserve it. On success,
they should delete it.

### Required tests

#### `RingloomNativeSmokeIT`

Validates native loading and ABI basics:

1. load native library
2. assert ABI version equals `1`
3. assert status strings are non-empty
4. assert invalid start arguments produce `RINGLOOM_ERR_INVALID_ARGUMENT`

This test does not require a broker.

#### `RingloomServiceLifecycleIT`

Validates lifecycle against a real broker:

1. start broker via helper script or Java process helper
2. wait for broker readiness
3. start Java `RingloomService` named `java-lifecycle`
4. assert `serviceId() > 0`
5. assert `nodeId() == 1`
6. close service
7. stop broker

#### `RingloomLocalIpcIT`

Validates two Java services communicating on the same host:

1. start broker
2. start Java service `echo`
3. create `echo.messageConsumer()`
4. start an application-owned Java polling loop for `echo`
5. start Java service `ping`
6. create client from `ping` to `echo`
7. wait for discovery to deliver `echo` instance to `ping`
8. claim a send buffer from the client, write `hello` into the returned
   `MemorySegment`, and commit the claim
9. assert `echo` handler receives a borrowed payload segment containing `hello`
10. close clients/consumers/services
11. stop broker

Discovery is asynchronous, so the test should retry client send or wait on an
observable discovery condition with a bounded timeout.

#### Optional follow-up tests

- Java service to Zig echo service
- Zig ping service to Java service
- large payload fragmentation
- leader-aware send
- remote two-broker route
- backpressure status mapping

---

## 10. Broker Startup Helper Script

Add:

```text
scripts/start-test-broker.sh
```

Purpose:

- create a temporary or caller-specified workspace
- write a broker properties file
- start `zig-out/bin/ringloom-broker`
- wait for readiness
- print machine-readable environment output for tests
- stop the broker on command or via trap

Required behavior:

```bash
./scripts/start-test-broker.sh --workspace /tmp/ringloom-java-it-123 --foreground
./scripts/start-test-broker.sh --workspace /tmp/ringloom-java-it-123 --daemon
./scripts/start-test-broker.sh --workspace /tmp/ringloom-java-it-123 --stop
```

Options:

| Option | Default | Description |
|---|---|---|
| `--workspace DIR` | generated temp dir | Root for config, logs, storage, pid file |
| `--group NAME` | `ringloom-java-test` | Broker group name |
| `--node-id N` | `1` | Broker node id |
| `--host HOST` | `127.0.0.1` | Broker bind host |
| `--port PORT` | `19001` | Broker bind port |
| `--bin-dir DIR` | `zig-out/bin` | Directory containing `ringloom-broker` |
| `--foreground` | false | Run broker in foreground |
| `--daemon` | false | Run broker in background and write pid file |
| `--stop` | false | Stop broker from pid file |
| `--timeout SEC` | `10` | Readiness wait timeout |

Generated config:

```properties
broker.node.id=1
broker.local.host.port=127.0.0.1:19001
broker.group.name=ringloom-java-test
broker.storage.path=/tmp/ringloom-java-it-123/storage
broker.control.buffer.size=65536
broker.messages.buffer.size=1048576
broker.threading.mode=dedicated
broker.idle.strategy=backoff
```

Daemon mode output should be easy for tests to parse:

```text
RINGLOOM_BROKER_PID=12345
RINGLOOM_BROKER_CONFIG=/tmp/.../config/broker_1.properties
RINGLOOM_BROKER_LOG=/tmp/.../logs/broker_1.log
RINGLOOM_STORAGE_PATH=/tmp/.../storage
RINGLOOM_GROUP=ringloom-java-test
```

The script must use PID-specific termination. It must not use `pkill` or
process-name based killing.

---

## 11. Validation and Acceptance Criteria

### Native build criteria

1. `zig build service-c` succeeds.
2. `zig-out/lib/libringloom_service.so` exists on Linux, or
   `zig-out/lib/libringloom_service.dylib` exists on macOS.
3. `zig-out/lib/libringloom_service.a` exists only if optional static packaging
   is implemented.
4. `zig-out/include/ringloom_service.h` exists.
5. A small C smoke program can include `ringloom_service.h`, link the native
   library, call `ringloom_service_abi_version`, and exit successfully.
6. `zig build test` still succeeds.

### C ABI criteria

1. No exported function exposes Zig-only types.
2. All fallible exported functions return `ringloom_status_t`.
3. All handle-producing functions initialize out-handles to `NULL` on failure.
4. Invalid pointer arguments are rejected deterministically.
5. Every successful zero-copy claim can be committed or aborted exactly once.
6. Polling receive delivers borrowed payload bytes without copying.
7. Native lifecycle is leak-free under Zig test allocator/debug allocator.
8. Calling stop/close paths does not leave broker-visible stale service state
   beyond the expected heartbeat/cleanup behavior.

### Java binding criteria

1. Java build compiles from `bindings/java`.
2. Java native loader checks ABI version.
3. Java wrappers are `AutoCloseable`.
4. Hot-path Java APIs return primitive integer status/count values instead of
   throwing exceptions.
5. Exception-throwing APIs are limited to startup/configuration or explicitly
   named convenience wrappers.
6. Java `BufferClaim` is reusable and exposes writable borrowed ring-buffer
   memory without an intermediate payload copy.
7. Java `MessageConsumer.poll(...)` drives receive progress from a Java-owned
   thread and does not allocate per message.
8. Java `RingloomMessage` is a reusable borrowed view that exposes primitive
   payload address/length accessors and does not copy unless the user explicitly
   calls a copy method.
9. Java hot-path allocation validation shows no steady-state per-message
   allocation from the binding.
10. Java services can start, register, and close against a local broker.
11. Two Java services can exchange a local IPC payload through the native service
   runtime.

### Helper script criteria

1. Script creates workspace directories.
2. Script writes broker config.
3. Script starts broker in daemon mode and writes pid file.
4. Script waits for readiness.
5. Script stops only the recorded broker PID.
6. Script preserves logs for diagnostics.

---

## 12. Implementation Phases

### Phase 1 — Native ABI foundation

1. Add `include/ringloom_service.h`.
2. Add `src/service/c_abi.zig`.
3. Implement ABI version, status strings, diagnostics, config conversion.
4. Implement service start/stop/destroy and identity accessors.
5. Add shared library build target and header install.
6. Add C ABI smoke tests.

### Phase 2 — Native client, zero-copy send, and polling receive

1. Implement client create/destroy.
2. Implement send, send-to, send-to-leader.
3. Implement claim/commit/abort for zero-copy sends.
4. Implement message consumer create/destroy.
5. Implement `ringloom_message_consumer_poll`.
6. Adapt `MessageConsumer` if needed to provide metadata-rich polled messages.
7. Add native tests for client, claim, and polling error paths.

### Phase 3 — Broker helper script

1. Add `scripts/start-test-broker.sh`.
2. Support foreground, daemon, stop, workspace, group, port options.
3. Use the same readiness marker conventions as the existing test harness.
4. Document script usage in `bindings/java/README.md`.

### Phase 4 — Java FFM binding

1. Create `bindings/java` Gradle project.
2. Implement low-level `RingloomNative` symbol bindings.
3. Implement high-level service/client wrappers.
4. Implement `BufferClaim` as a reusable object with primitive status-code
   commit/abort.
5. Implement `MessageConsumer.poll(...)` and reusable borrowed
   `RingloomMessage` views.
6. Implement native status constants for the performance API.
7. Implement native status to Java exception mapping only for convenience
   wrappers.

### Phase 5 — Java integration tests

1. Add native smoke integration test.
2. Add broker-backed service lifecycle test.
3. Add two-Java-service local IPC test using claim/commit send and Java-driven
   polling receive.
4. Add an allocation-sensitive Java hot-path smoke test.
5. Wire `zig build test-java`.
6. Document prerequisites and commands.

---

## 13. Open Decisions

1. **Optional static packaging:** Java only needs the shared library. Decide
   whether `libringloom_service.a` is still useful for C/C++ embedders in the
   same milestone or should be deferred.

2. **Java baseline:** Prefer Java 22+ for finalized FFM APIs. If Java 21 support
   is required, the binding and test commands must include preview flags.

3. **Polling metadata completeness:** The first ABI can expose payload and
   template id immediately. Full routing metadata may require changes in the
   internal message consumer path.

4. **Generated vs handwritten headers:** A handwritten checked-in header is
   recommended for stable ABI review. If Zig-generated headers are added later,
   CI should compare generated declarations against the checked-in public header.
