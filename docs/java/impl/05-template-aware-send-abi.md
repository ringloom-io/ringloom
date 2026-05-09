# Phase 5 — Template-aware send ABI

## Objective

Add copy-based send APIs that accept a template id. The current low-level Java
binding exposes template ids for zero-copy `tryClaim(...)`, but copy-based
`send(...)`, `sendTo(...)`, and `sendToLeader(...)` use the default application
message type. Generated ergonomic clients need template-aware copy sends.

## Deliverables

1. Zig service-client methods for template-aware copy sends.
2. C ABI declarations and implementations.
3. Java FFM binding methods.
4. Correlation-aware claim and send variants needed by request/response.
5. Generated client integration.
6. Unit and integration tests for local and remote paths.

## Native service changes

Add methods to `ServiceClient`:

```zig
pub fn sendMessage(self: *Self, template_id: u16, payload: []const u8) SendError!void
pub fn sendToMessage(
    self: *Self,
    target_node_id: i16,
    target_service_id: i32,
    template_id: u16,
    payload: []const u8,
) SendError!void
pub fn sendToLeaderMessage(self: *Self, template_id: u16, payload: []const u8) SendError!void
```

Implementation should reuse existing `tryClaimToInstance` and
`tryClaimRemoteService` logic:

1. Claim with the provided template id.
2. Copy payload into claimed payload memory.
3. Commit claim.
4. Abort claim on any error after claim.
5. Preserve existing counter updates.

Existing no-template methods should delegate with template id `0` to preserve
compatibility.

## C ABI changes

Increment ABI version only if the project requires strict symbol-set versioning.
If additive symbols are allowed under the current versioning policy, keep the
version and document the optional symbols. Otherwise bump the C header and Java
constant together.

Add declarations:

```c
ringloom_status_t ringloom_client_send_message(
    ringloom_client_t *client,
    uint16_t template_id,
    const uint8_t *payload,
    size_t payload_len
);

ringloom_status_t ringloom_client_send_to_message(
    ringloom_client_t *client,
    int16_t target_node_id,
    int32_t target_service_id,
    uint16_t template_id,
    const uint8_t *payload,
    size_t payload_len
);

ringloom_status_t ringloom_client_send_to_leader_message(
    ringloom_client_t *client,
    uint16_t template_id,
    const uint8_t *payload,
    size_t payload_len
);
```

Validation:

1. `client` must be active.
2. `payload` may be null only when `payload_len == 0`.
3. Template id is an unsigned 16-bit value.
4. Errors map to existing `ringloom_status_t` values.

## Java binding changes

Add low-level methods to `RingloomClient`:

```java
public int sendMessage(int templateId, MemorySegment payload);
public int sendToMessage(short targetNodeId, int targetServiceId, int templateId, MemorySegment payload);
public int sendToLeaderMessage(int templateId, MemorySegment payload);
```

Convenience wrappers:

```java
public void sendMessageOrThrow(int templateId, MemorySegment payload);
public void sendMessageOrThrow(int templateId, byte[] payload);
```

Hot-path methods should return integer status codes and must not allocate.

## Correlation-aware request sends

Request/response requires generated clients to choose the outgoing
`correlation_id`. Add correlation-aware variants for every generated send path.
Existing methods remain as compatibility wrappers that pass `correlation_id = 0`.

Native service additions:

```zig
pub fn tryClaimRequest(
    self: *Self,
    template_id: u16,
    correlation_id: i64,
    payload_len: usize,
) SendError!SendClaim

pub fn tryClaimToRequest(
    self: *Self,
    target_node_id: i16,
    target_service_id: i32,
    template_id: u16,
    correlation_id: i64,
    payload_len: usize,
) SendError!SendClaim

pub fn sendMessageRequest(
    self: *Self,
    template_id: u16,
    correlation_id: i64,
    payload: []const u8,
) SendError!void
```

C ABI additions should mirror the template-aware functions with an added
`int64_t correlation_id`, for example:

```c
ringloom_status_t ringloom_client_try_claim_request(
    ringloom_client_t *client,
    uint16_t template_id,
    int64_t correlation_id,
    size_t payload_len,
    ringloom_buffer_claim_t *out_claim
);

ringloom_status_t ringloom_client_send_message_request(
    ringloom_client_t *client,
    uint16_t template_id,
    int64_t correlation_id,
    const uint8_t *payload,
    size_t payload_len
);
```

Direct-target and leader variants should be added for both claim and copy send
paths. For local IPC, the produced application message header must carry the
caller-provided `correlation_id`. For remote broker routing, the TCP frame header
must carry the same `correlation_id`.

Java binding additions:

```java
public int tryClaimRequest(int templateId, long correlationId, long payloadLength, BufferClaim claim);
public int sendMessageRequest(int templateId, long correlationId, MemorySegment payload);
```

Generated request/response clients must use these methods. If the native library
does not expose the required correlation-aware symbols, framework startup should
fail before accepting request/response client calls.

`RingloomNative` changes:

1. Add method handles and function descriptors.
2. Resolve symbols during static initialization.
3. Add template id range validation in public Java API before narrowing to
   `short`.

## Generated client integration

Generated clients should use:

1. `tryClaim(...)` for zero-copy methods.
2. `sendMessage(...)` for copy/ergonomic methods.
3. `sendToMessage(...)` for direct target routing.
4. `sendToLeaderMessage(...)` for leader routing.
5. `tryClaimRequest(...)` and `sendMessageRequest(...)` variants for every
   request/response method so the correlation id is caller-selected.

The annotation processor should fail compilation if a template id is outside
`0..65535`.

## Testing

Zig unit tests:

1. Template-aware local send writes ring-buffer message type equal to template id.
2. Template id `0` preserves existing default behavior.
3. Payload is copied correctly.
4. Claim is aborted on copy/commit error paths where applicable.
5. Correlation-aware local claim writes the selected correlation id.
6. Correlation-aware remote claim writes the TCP frame correlation id.

Java integration tests:

1. `sendMessage(templateId, payload)` reaches local handler with template id.
2. `sendToMessage(...)` reaches selected target.
3. `sendToLeaderMessage(...)` reaches leader target.
4. Remote broker path preserves TCP frame template id.
5. Existing no-template APIs still work.
6. `tryClaimRequest(...)` and `sendMessageRequest(...)` preserve correlation ids.

Generated framework tests:

1. Ergonomic generated client method uses template-aware copy send.
2. Duplicate template id validation remains processor responsibility.
3. Request/response generated clients use correlation-aware send methods.

## Acceptance criteria

1. Copy-based Java sends can set application template ids.
2. Existing APIs remain source-compatible.
3. Local and remote paths preserve template ids.
4. Generated ergonomic clients no longer need to claim buffers just to set a
   template id.
5. Generated request/response clients can set correlation ids on local and remote
   sends.
