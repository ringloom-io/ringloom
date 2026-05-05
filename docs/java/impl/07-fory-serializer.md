# Phase 7 — Apache Fory serializer

## Objective

Provide an optional ergonomic serializer module using Apache Fory. This module is
not the primary zero-allocation path; it is for developers who value object
serialization convenience and can accept its allocation and latency profile.

## Deliverables

1. `ringloom-serializer-fory` artifact.
2. Fory serializer module implementation.
3. Configuration support for registration and compatibility options.
4. Annotation processor integration.
5. Documentation and tests covering allocation expectations.

## Dependencies

The Fory artifact depends on:

1. `ringloom-framework-core`.
2. Apache Fory.

Fory must remain optional and must not be pulled into core or SBE users.

## Configuration

YAML example:

```yaml
ringloom:
  serializers:
    entries:
      fory:
        type: fory
        requireRegistration: true
        compatibleMode: false
        referenceTracking: false
```

Configuration record:

```java
public record ForySerializerConfig(
    boolean requireRegistration,
    boolean compatibleMode,
    boolean referenceTracking,
    List<String> registeredTypes
) {}
```

Defaults should favor predictable latency:

1. `requireRegistration = true`.
2. `referenceTracking = false` unless object graphs require it.
3. Pre-create serializer instances at runtime startup.

## Serializer behavior

Inbound:

1. Read borrowed payload segment.
2. Decode to an application object.
3. Pass object to handler.

Outbound:

1. Serialize object into a reusable buffer when exact encoded size is unknown.
2. Prefer direct encode into `BufferClaim` only if Fory exposes a safe reusable
   output abstraction that can write to claimed memory.
3. Otherwise use template-aware copy send from phase 5.

Allocation profile:

1. Decoding arbitrary object graphs may allocate.
2. Encoding may allocate if Fory requires internal buffers.
3. The module should minimize steady-state allocations with reusable buffers but
   must not claim zero allocation unless tests prove it for a constrained
   signature.

## Annotation processor integration

Supported client methods:

```java
@RingloomRequest(templateId = 301, serializer = "fory")
int send(MyPojo message);

@RingloomRequest(templateId = 301, serializer = "fory", errorPolicy = ErrorPolicy.THROWING)
void sendOrThrow(MyPojo message);
```

Supported handler methods:

```java
@RingloomHandler(templateId = 301, serializer = "fory")
int onMessage(MyPojo message, MessageContext context);
```

The processor should warn if Fory is used in a handler or client marked as
zero-allocation/hot-path.

## Buffer management

Use a per-event-loop `ForyEncodeContext`:

1. Reusable byte buffer or native segment.
2. Growth policy with maximum configured payload size.
3. Explicit reset between messages.
4. No sharing across event-loop threads unless synchronized externally.

If the encoded payload exceeds RingLoom maximum message length, return
`RINGLOOM_ERR_MESSAGE_TOO_LONG` or framework equivalent.

## Error handling

Serialization errors:

1. Status-returning methods return a framework serialization error code.
2. Throwing methods wrap errors in a `RingloomSerializationException`.
3. If an error occurs after a claim is acquired, abort the claim.

Do not swallow Fory configuration errors during startup.

## Testing

Unit tests:

1. Registered POJO round-trips through Fory serializer.
2. Unregistered class fails when registration is required.
3. Buffer growth respects configured maximum.
4. Serialization error maps to expected status/exception policy.

Integration tests:

1. Generated Fory client sends to generated Fory handler.
2. Template id is preserved.
3. Message too long is handled without corrupting claims.

Allocation tests:

1. Measure and document steady-state allocations for simple registered POJO.
2. Ensure no unexpected framework allocations beyond Fory behavior.

## Acceptance criteria

1. Fory users can send and receive POJO messages with minimal wiring.
2. Fory remains optional and isolated.
3. Documentation clearly distinguishes Fory ergonomics from SBE zero-copy.
4. Generated code uses template-aware copy sends unless direct claimed-memory
   encoding is proven safe.
