# Phase 4 — SBE serializer

## Objective

Provide the primary zero-copy serialization path for RingLoom Java services using
SBE flyweights. Inbound messages should wrap borrowed RingLoom payload memory;
outbound messages should encode directly into claimed RingLoom buffer memory.

## Deliverables

1. `ringloom-serializer-sbe` artifact.
2. SBE serializer SPI implementation.
3. MemorySegment-backed adapters for generated SBE codecs.
4. Annotation processor support for SBE handler and client signatures.
5. Generated primitive partition-key extraction from SBE field paths.
6. Allocation tests for inbound dispatch, partitioned worker enqueue, and
   outbound zero-copy sends.

## Dependencies

Keep dependencies minimal:

1. `ringloom-framework-core`.
2. SBE/Agrona runtime only if required by generated SBE codecs.

Do not make SBE dependencies transitive through core.

## Serializer model

The SBE module should expose:

```java
public final class SbeSerializerModule implements SerializerModule {
    public String name();
    public void register(SerializerRegistry.Builder builder, SbeConfig config);
}
```

Core SPI implementations:

1. `SbeMessageEncoder<T>`.
2. `SbeFlyweightDecoder<T>`.
3. `SbeCodecFactory`.
4. `SbeEncodeContext`.
5. `SbeDecodeContext`.

Generated code should use concrete codec classes directly when possible.

## Inbound zero-copy path

Inbound flow:

1. `MessageConsumer` provides a reusable `RingloomMessage`.
2. Generated dispatcher reads `payloadSegment()`.
3. SBE decoder/flyweight wraps the borrowed `MemorySegment`.
4. Handler receives flyweight and reusable `MessageContext`.
5. Handler returns before payload memory is reused.

Rules:

1. Do not copy payload bytes.
2. Do not allocate a flyweight per message.
3. Do not retain borrowed payload after handler return.
4. Support byte order required by generated SBE codecs.

This zero-copy inbound path applies only to `consumerThread` execution. In
`partitionedWorkers`, generated ingress should use SBE flyweights on the consumer
thread only to extract the primitive partition key, then copy the payload bytes
into the selected worker's preallocated SPSC slot before returning from the poll
callback. The worker wraps its own slot memory, not the low-level borrowed
payload. This preserves zero allocations but adds one copy. In `virtualThreads`,
the SBE module may decode to objects or copy to task-owned buffers and does not
claim a no-allocation profile.

## Outbound zero-copy path

Outbound flow:

1. Generated client calculates encoded length from SBE block length and repeating
   group sizes.
2. Generated client calls `RingloomClient.tryClaim(templateId, length, claim)`.
3. SBE encoder wraps `claim.payloadSegment()`.
4. Encoder writes directly into claimed memory.
5. Generated client commits or aborts the claim.

If encoding fails after claim, generated code must abort the claim before
returning or throwing.

## Buffer adapters

Some SBE Java codecs expect Agrona `MutableDirectBuffer` or `DirectBuffer`.
Provide reusable adapters over `MemorySegment`:

1. `MemorySegmentDirectBuffer`.
2. `MemorySegmentMutableDirectBuffer`.

Adapter constraints:

1. Reusable and resettable.
2. No per-wrap allocation.
3. Bounds checks consistent with SBE expectations.
4. Clear documentation of borrowed memory lifetime.

## Annotation processor integration

Supported handler shapes:

```java
@RingloomHandler(templateId = 101, serializer = "sbe")
int onOrder(NewOrderDecoder order, MessageContext context);

@RingloomHandler(templateId = 101, serializer = "sbe")
int onOrder(MemorySegment payload, MessageContext context);

@RingloomHandler(templateId = 101, serializer = "sbe", partitionKey = "orderId")
int onOrderPartitioned(NewOrderDecoder order, MessageContext context);
```

Supported client shapes:

```java
@RingloomRequest(templateId = 201, serializer = "sbe")
int send(NewOrderEncoderSource source, DirectSendContext context);

@RingloomRequest(templateId = 201, serializer = "sbe")
int send(MemorySegment encodedPayload);
```

The processor should verify that a registered SBE codec can satisfy the method
shape. If a type requires allocation to decode, the processor should not label
the method zero-copy.

Partition-key extraction rules:

1. Extractors run on the consumer thread against borrowed payload memory.
2. The extracted key must be a primitive `long`, `int`, `short`, or `byte` widened
   to `long`; textual or byte-array keys require a generated allocation-free hash
   over borrowed bytes.
3. Extractors must not retain the SBE decoder or borrowed `MemorySegment`.
4. Missing or incompatible field paths are compile-time errors.

## Error handling

Expected send failures return RingLoom status values:

1. Buffer full.
2. Backpressure.
3. No available instance.
4. Message too long.
5. Peer disconnected.

Encoding errors that indicate invalid application data can return a framework
status code or throw in convenience methods. Hot-path methods should prefer a
status code.

## Testing

Unit tests:

1. MemorySegment direct-buffer adapters read/write primitive values correctly.
2. Flyweight decoder wraps new payload memory without allocation.
3. Encoder writes into a supplied target segment.
4. Claim abort occurs when encoding fails.
5. SBE partition-key extractor reads primitive fields without allocation.

Integration tests:

1. Generated SBE client sends to generated SBE handler.
2. Handler reads fields from borrowed flyweight.
3. Remote broker path preserves template id and payload.
4. Local IPC path preserves template id and payload.
5. Partitioned-worker SBE handler wraps worker-owned slot memory, not borrowed
   consumer memory.

Allocation tests:

1. Steady-state inbound dispatch with SBE flyweight handler.
2. Steady-state outbound zero-copy send.
3. Repeated direct-buffer adapter wraps.
4. Repeated partitioned-worker enqueue with preallocated slots.

## Acceptance criteria

1. SBE handlers can process messages without payload copies.
2. SBE clients can encode directly into `BufferClaim`.
3. Generated code has a documented no-allocation profile.
4. SBE remains optional and isolated from core.
5. SBE partition-key extraction supports zero-allocation partitioned dispatch.
