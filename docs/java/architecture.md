# RingLoom Java framework architecture

This document proposes a higher-level Java framework on top of the existing
`bindings/java` FFM service bindings. The current binding is intentionally
low-level: it starts a service, polls control messages, creates clients, polls
inbound messages, and exposes `BufferClaim`/`RingloomMessage` for zero-copy
hot paths. The framework should keep that low-level layer available while adding
compile-time wiring, bootstrap, serialization, event loops, and metrics access.

## Goals

1. Let developers create RingLoom microservices with minimal manual wiring.
2. Keep application code clean: interfaces for clients, annotated methods for
   handlers, explicit generated code instead of runtime reflection.
3. Preserve a zero-allocation hot-path profile for services that opt into it.
4. Support pluggable serialization with initial SBE and Apache Fory modules.
5. Expose RingLoom runtime metrics counters and gauges to Java applications.
6. Keep dependencies minimal and IoC-friendly for Micronaut, Spring, Avaje, and
   similar frameworks.

Non-goals for the initial framework:

1. Running a broker from Java. The broker remains a native process.
2. Runtime classpath scanning or reflection-based dispatch.
3. Hiding the low-level `io.ringloom.service` bindings from advanced users.
4. Application-level tracing implementation, beyond reserving extension points.

## Confirmed design decisions

1. The framework targets **Java 25** initially, matching the current Java FFM
   binding baseline.
2. The first IoC integration target is **Avaje**. Spring and Micronaut adapters
   can follow after the core generated-code contracts are stable.
3. Message template ids are **globally unique per RingLoom service**. They are
   not scoped by serializer, client interface, or handler class.
4. Application custom metrics are deferred. The first metrics ABI change should
   expose native RingLoom counters and derived gauges to Java.

## Artifact layout

The framework should be split into small artifacts so latency-sensitive services
do not pay for unused features.

| Artifact | Purpose | Required dependencies |
|---|---|---|
| `ringloom-java-bindings` | Existing FFM bindings to `libringloom_service` | none beyond JDK |
| `ringloom-framework-core` | Runtime, event loops, generated-code contracts, serializers SPI, metrics API | `ringloom-java-bindings`, `slf4j-api` |
| `ringloom-framework-processor` | Annotation processor that generates client proxies, handler dispatchers, and bootstrap metadata | Java annotation processing APIs |
| `ringloom-framework-yaml` | YAML configuration loader and standalone bootstrapper | core, SnakeYAML Engine |
| `ringloom-serializer-sbe` | SBE encoder/decoder adapter | core, Agrona/SBE runtime as needed by generated SBE codecs |
| `ringloom-serializer-fory` | Apache Fory serializer adapter | core, Apache Fory |
| `ringloom-ioc-avaje` | First IoC integration module | core plus Avaje APIs |
| `ringloom-ioc-*` | Later optional integrations for Spring, Micronaut, etc. | core plus chosen IoC API |

The annotations should live in `ringloom-framework-core` or a tiny
`ringloom-framework-api` artifact, not in the processor. Applications need
annotations at compile time and possibly in source retention only; the processor
owns generated implementation details.

## Layered architecture

```text
Application code
  @RingloomClient interfaces
  @RingloomHandler methods
  application serializers/codecs
        |
        v
Generated code, created at compile time
  client proxy classes
  handler dispatcher
  bootstrap metadata
  IoC bean descriptors where supported
        |
        v
ringloom-framework-core
  RingloomRuntime
  EventLoop / AgentRunner / IdleStrategy
  ClientInvoker / MessageDispatcher
  SerializerRegistry
  Metrics facade
        |
        v
ringloom-java-bindings
  RingloomService
  RingloomClient
  MessageConsumer
  BufferClaim
  RingloomMessage
        |
        v
libringloom_service C ABI
  shared-memory IPC
  control agent
  service counters
  metadata mappings
        |
        v
RingLoom broker
```

The framework must not use reflection on the hot path. Startup may use generated
metadata and `ServiceLoader` for standalone mode; IoC integration modules should
prefer framework-native compile-time registration where possible.

## Developer model

### Service entry point

A service can be bootstrapped from YAML:

```java
public final class OrdersMain {
    public static void main(String[] args) throws Exception {
        try (RingloomApplication app = RingloomBootstrap.fromYaml(args[0]).start()) {
            app.awaitShutdown();
        }
    }
}
```

In an IoC framework, `RingloomRuntime`, generated clients, generated dispatcher,
metrics facade, and configured serializers should be exposed as beans. The core
types should use constructor injection and should not require static global
state.

### Client interfaces

Client definitions should be plain interfaces:

```java
@RingloomClient(service = "pricing")
public interface PricingClient {
    @RingloomRequest(templateId = PricingTemplates.PRICE_REQUEST, serializer = "sbe")
    int requestPrice(PriceRequest request);

    @RingloomRequest(templateId = PricingTemplates.PRICE_REQUEST, serializer = "sbe")
    int requestPrice(PriceRequestFlyweight request, DirectSendContext context);
}
```

The processor generates an implementation such as
`PricingClient_RingloomClient` that:

1. Receives a low-level `RingloomClient` from `RingloomRuntime`.
2. Uses a serializer selected at compile time from annotation metadata.
3. Uses `RingloomClient.tryClaim(templateId, payloadLength, reusableClaim)` for
   zero-copy methods.
4. Uses status-code return values for hot-path methods.
5. Provides optional throwing convenience methods only outside hot paths.

The generated proxy should be injectable as a bean. For IoC-neutral use, it can
also be created by `RingloomRuntime.client(PricingClient.class)` using generated
metadata rather than reflection over annotations.

### Message handlers

Handlers should be ordinary classes:

```java
@RingloomServiceComponent
public final class OrderHandlers {
    private final PricingClient pricing;

    public OrderHandlers(PricingClient pricing) {
        this.pricing = pricing;
    }

    @RingloomHandler(templateId = OrderTemplates.NEW_ORDER, serializer = "sbe")
    public int onNewOrder(NewOrderFlyweight order, MessageContext context) {
        return pricing.requestPrice(order.toPriceRequest(), context.directSend());
    }
}
```

The processor generates a dispatcher table keyed by template id. Dispatch is an
array lookup or generated switch, not a map lookup built from reflection.
Because template ids are globally unique per service, the processor should
reject duplicate `@RingloomHandler(templateId = ...)` values across all handler
classes compiled for the same service.

Handler methods should support two profiles:

1. **Ergonomic profile**: decode to application records/classes; allocations are
   acceptable.
2. **Hot-path profile**: decode to flyweights or borrowed buffers; no
   allocation per message.

The generated dispatcher owns reusable flyweight instances and context objects
per event-loop thread. It must document that borrowed payload memory is valid
only during the handler call, matching `RingloomMessage`.

## Annotation model

Initial annotations:

| Annotation | Target | Purpose |
|---|---|---|
| `@RingloomApplication` | type | Optional marker for generated standalone bootstrap metadata. |
| `@RingloomServiceComponent` | type | Marks a class that contains RingLoom handlers or lifecycle hooks. |
| `@RingloomClient` | interface | Declares a logical target service client. |
| `@RingloomRequest` | method | Declares template id, serializer, routing mode, and error policy for a send method. |
| `@RingloomHandler` | method | Declares template id and serializer for inbound messages. |
| `@RingloomLifecycleHandler` | method | Handles service availability/unavailability events for a generated client. |
| `@RingloomMetric` | field or method | Optional application metric registration in service metadata, if supported later. |

Generated artifacts:

1. `*_RingloomClient` proxy implementations.
2. `*_RingloomDispatcher` handler dispatchers.
3. `META-INF/services/...` entries for standalone bootstrap.
4. Optional framework descriptors, for example Spring `BeanDefinition` import
   metadata or Micronaut bean introspection metadata, in integration modules.

The processor should fail compilation for ambiguous template ids, missing
serializers, unsupported hot-path signatures, or handler return types that do not
map to a known error policy.

## Bootstrap and YAML configuration

The YAML module should parse configuration into immutable records and then build
the runtime from explicit factories. YAML parsing is startup-only and is not part
of the hot path.

Example:

```yaml
ringloom:
  service:
    name: orders
    storagePath: /dev/shm
    group: default
    brokerNodeId: 1
    controlBufferLength: 65536
    messagesBufferLength: 1048576
    heartbeatTimeoutMillis: 10000
    leaderElectionEnabled: false

  runtime:
    control:
      thread: dedicated
      idleStrategy: backoff
      pollLimit: 256
    messages:
      thread: dedicated
      idleStrategy: busySpin
      pollLimit: 256
    lifecycle:
      shutdownHook: true

  serializers:
    default: sbe
    sbe:
      package: com.example.codec
    fory:
      requireRegistration: true

  clients:
    pricing:
      service: pricing
      routing: loadBalanced
      serializer: sbe
    risk:
      service: risk
      routing: leader
      serializer: fory
```

Core configuration records:

| Type | Role |
|---|---|
| `RingloomApplicationConfig` | Full immutable configuration root. |
| `RingloomServiceConfig` | Maps to existing `ServiceConfig`. |
| `RingloomRuntimeConfig` | Threading, event-loop, idle strategy, and poll limits. |
| `RingloomClientConfig` | Logical client settings, routing, and serializer defaults. |
| `SerializerConfig` | Serializer module configuration. |

`RingloomBootstrap` should have these creation paths:

1. `fromYaml(Path)` for standalone applications.
2. `fromConfig(RingloomApplicationConfig)` for tests and IoC frameworks.
3. `builder()` for programmatic setup.

## Runtime and common data structures

### RingloomRuntime

`RingloomRuntime` owns the low-level service handle, generated clients, message
consumer, dispatcher, control loop, metrics facade, and lifecycle.

Responsibilities:

1. Start `RingloomService` from config.
2. Create low-level `RingloomClient` instances by logical target name.
3. Connect generated client proxies to serializers and low-level clients.
4. Create one `MessageConsumer` and one generated dispatcher.
5. Start and stop configured event-loop threads.
6. Expose `RingloomMetrics` and lifecycle state.

It should implement `AutoCloseable` and provide deterministic shutdown. It
should not hide status codes from hot-path code; higher-level throwing wrappers
can be separate.

### EventLoop

The framework should define a Java event-loop abstraction similar to the native
agent model:

```java
public interface Agent {
    int doWork();
    default void onStart() {}
    default void onClose() {}
}

public final class EventLoop implements AutoCloseable {
    public EventLoop(String name, Agent agent, IdleStrategy idleStrategy);
    public void run();
    public void startThread(ThreadFactory factory);
    public void close();
}
```

Common agents:

| Agent | Uses | Work |
|---|---|---|
| `ControlAgent` | `RingloomService` | Calls `pollControl(limit)` to drive discovery, lifecycle callbacks, and heartbeats. |
| `MessageConsumerAgent` | `MessageConsumer` | Calls `poll(dispatcher, limit)` and invokes generated handlers. |
| `CompositeAgent` | multiple agents | Runs multiple agents on one thread for compact deployments. |
| `LifecycleAgent` | runtime state | Optional startup/shutdown hooks and health transitions. |

Threading modes:

| Mode | Threads | Intended use |
|---|---|---|
| `dedicated` | separate control and message threads | Lowest and most predictable message latency. |
| `shared` | one event loop runs control and message agents | Fewer threads, acceptable for smaller services. |
| `external` | caller polls manually | IoC/container-managed event loops or tests. |

### Idle strategies

Initial Java idle strategies:

1. `BusySpinIdleStrategy` for lowest latency and pinned cores.
2. `YieldingIdleStrategy` for lower CPU burn.
3. `SleepingIdleStrategy` for background services.
4. `BackoffIdleStrategy` for default balanced behavior.
5. `NoOpIdleStrategy` for externally managed loops.

All idle strategies should be allocation-free and configurable through YAML.

### Reusable contexts

The framework should provide reusable per-thread context objects:

| Type | Purpose |
|---|---|
| `MessageContext` | Read-only source/target ids, correlation id, flags, payload segment, and reply helpers. |
| `DirectSendContext` | Reusable `BufferClaim`, selected route, and serializer scratch state for zero-copy client sends. |
| `DecodeContext` | Serializer-specific reusable state. |
| `HandlerResult` | Status code constants or small value type for generated dispatch. |

Contexts should be owned by a single event-loop thread. Sharing across threads
should be explicit and documented as unsupported unless a type states otherwise.

## Serialization SPI

Serialization must be pluggable and should distinguish copy-based and
zero-copy/flyweight codecs.

Core interfaces:

```java
public interface MessageEncoder<T> {
    int templateId();
    int encodedLength(T value, EncodeContext context);
    int encode(T value, WritableMessageBuffer target, EncodeContext context);
}

public interface MessageDecoder<T> {
    int templateId();
    T decode(ReadableMessageBuffer source, DecodeContext context);
}

public interface FlyweightDecoder<T> {
    int templateId();
    T wrap(MemorySegment payload, DecodeContext context);
}
```

Buffer abstractions should wrap `MemorySegment` and expose primitive accessors
without copying. They should avoid forcing `ByteBuffer` allocation on every
message.

### SBE module

SBE should be the primary zero-copy serializer:

1. Generated SBE flyweights wrap the borrowed inbound payload segment.
2. Outbound generated proxies claim RingLoom buffer space first, then encode
   directly into the claimed payload.
3. Decoder and encoder instances are allocated during runtime startup and reused
   per event-loop thread.
4. If an application uses SBE codecs that require Agrona buffers, the SBE module
   should provide reusable `MemorySegment`-backed adapter objects.

### Apache Fory module

Apache Fory should be supported as an ergonomic serializer:

1. It is optional and isolated in `ringloom-serializer-fory`.
2. It can encode into claimed native memory when Fory supports writing through a
   reusable output abstraction.
3. It may allocate during encode/decode depending on object graphs. That must be
   documented clearly.
4. For predictable latency, require class registration and pre-created serializer
   instances at startup.

### Serializer selection

Serializer lookup should happen in generated code at startup, not per message.
Generated dispatchers should keep direct references to encoder/decoder objects.
Per-message dispatch should only do:

1. Read template id.
2. Select generated handler branch.
3. Wrap/decode payload.
4. Invoke handler.
5. Return status.

## Zero-allocation hot path

The hot path can be allocation-free when an application follows these rules:

1. Use `@RingloomHandler` methods with flyweight or `MemorySegment` payload
   parameters.
2. Use generated clients with `DirectSendContext` or generated reusable send
   contexts.
3. Prefer SBE flyweights or custom serializers that encode directly into
   `BufferClaim`.
4. Return status codes instead of throwing exceptions for expected outcomes.
5. Avoid retaining `RingloomMessage`, `MessageContext`, or payload segments after
   handler return.
6. Pre-create clients, serializers, dispatchers, counters, gauges, and contexts
   at startup.

The framework should include a hot-path allocation test suite similar to the
current Java binding allocation tests. Tests should verify steady-state polling
and generated zero-copy sends do not allocate.

## Metrics API

The application-facing API should expose RingLoom counters and gauges without
adding work to the native hot path.

```java
public interface RingloomMetrics {
    Counter counter(String name);
    Gauge gauge(String name);
    Iterable<MetricSample> samples();
    RingStats ringStats(String ringName);
}

public interface Counter {
    long value();
}

public interface Gauge {
    long value();
}
```

Suggested first metric sources:

1. Native service counters already maintained in service metadata.
2. Derived ring-buffer gauges from producer/consumer positions.
3. Broker metrics through existing observability process or separate metadata
   reader, not through service hot-path calls.

Java should expose a read facade, not aggregate metrics in application memory by
default. For Prometheus, the preferred deployment remains the out-of-process
`ringloom-observability` process. The Java facade is for health endpoints,
application decisions, tests, and IoC metrics bridges.

Application-defined service counters and gauges should be designed after the
native RingLoom metrics reader is available in Java.

## Required native, C ABI, and Java binding changes

The current low-level binding is sufficient for lifecycle, polling, client
discovery, and zero-copy claims. The framework needs additional surfaces.

### Template-aware copy sends

Current copy-based send APIs do not expose `templateId`; only
`ringloom_client_try_claim` does. Generated ergonomic clients need copy-based
template-aware sends for non-zero-copy methods.

Add native service methods and C ABI functions:

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

The existing no-template methods can remain as convenience APIs with template id
`0`, preserving compatibility.

### Metrics reader ABI

Expose stable metadata reads through the service ABI instead of making Java parse
internal metadata layouts directly:

```c
typedef struct ringloom_metrics_reader ringloom_metrics_reader_t;

typedef enum ringloom_metric_kind {
    RINGLOOM_METRIC_COUNTER = 1,
    RINGLOOM_METRIC_GAUGE = 2
} ringloom_metric_kind_t;

typedef struct ringloom_metric_descriptor {
    const char *name;
    size_t name_len;
    ringloom_metric_kind_t kind;
    int64_t value;
} ringloom_metric_descriptor_t;

typedef struct ringloom_ring_stats {
    uint64_t capacity_bytes;
    uint64_t used_bytes;
    uint64_t free_bytes;
    uint64_t producer_position;
    uint64_t consumer_position;
} ringloom_ring_stats_t;

ringloom_status_t ringloom_service_create_metrics_reader(
    ringloom_service_t *service,
    ringloom_metrics_reader_t **out_reader
);

void ringloom_metrics_reader_destroy(ringloom_metrics_reader_t *reader);

ringloom_status_t ringloom_metrics_reader_counter_count(
    ringloom_metrics_reader_t *reader,
    size_t *out_count
);

ringloom_status_t ringloom_metrics_reader_counter_at(
    ringloom_metrics_reader_t *reader,
    size_t index,
    ringloom_metric_descriptor_t *out_metric
);

ringloom_status_t ringloom_metrics_reader_ring_stats(
    ringloom_metrics_reader_t *reader,
    const char *ring_name,
    size_t ring_name_len,
    ringloom_ring_stats_t *out_stats
);
```

The Zig implementation should:

1. Read existing service metadata counter regions with acquire loads.
2. Derive gauges from ring trailers.
3. Keep descriptor names borrowed from stable metadata storage or copy them into
   reader-owned memory at reader creation.
4. Avoid heap allocation during individual metric reads.
5. Not add counter aggregation to the message hot path.

The Java binding should wrap this in `RingloomMetricsReader` and the framework
should adapt it to `RingloomMetrics`.

### Application counters and gauges

For custom Java service metrics, add an optional startup-time registration API:

```c
ringloom_status_t ringloom_service_register_counter(
    ringloom_service_t *service,
    const char *name,
    size_t name_len,
    int32_t *out_counter_id
);

ringloom_status_t ringloom_service_counter_add(
    ringloom_service_t *service,
    int32_t counter_id,
    int64_t delta
);

ringloom_status_t ringloom_service_counter_set(
    ringloom_service_t *service,
    int32_t counter_id,
    int64_t value
);
```

These calls should be optional for the first framework iteration if only native
RingLoom metrics are needed. Based on the current scope, they should be deferred
until after Java can read native RingLoom metrics.

## IoC integration

Core framework classes should be normal Java objects with explicit constructors:

1. `RingloomRuntime`
2. `RingloomApplicationConfig`
3. generated client implementations
4. generated dispatcher
5. `SerializerRegistry`
6. `RingloomMetrics`

The core module should not depend on Spring, Micronaut, Avaje, Jakarta, or CDI.
The first IoC-specific module should target Avaje. Later modules can adapt
generated metadata to other frameworks:

| Integration | Strategy |
|---|---|
| Avaje | Generated module implementing Avaje's bean registration model. |
| Spring | Generated/imported bean definitions for runtime, clients, dispatchers, serializers. |
| Micronaut | Generated bean definitions or introspection metadata. |
| Plain Java | `ServiceLoader` plus `RingloomBootstrap`. |

Generated client classes should have stable constructor signatures so any IoC
container can instantiate them without reflection over RingLoom annotations.

## Error handling

Hot-path generated methods should return integer RingLoom status codes or a
small enum-like result type. Expected runtime outcomes include buffer full,
backpressure, no available instance, peer disconnected, and message too long.

Exceptions are appropriate for:

1. Startup and configuration failures.
2. Invalid annotation usage detected by generated code.
3. Programmer errors, such as using a closed runtime.
4. Convenience APIs explicitly named `sendOrThrow` or similar.

Generated code should propagate low-level `RingloomStatus` values without
allocating exception objects on expected send or poll failures.

## Future tracing

OpenTelemetry should be designed after metrics and generated dispatch are stable.
There are two viable layers:

| Layer | Benefits | Costs |
|---|---|---|
| Zig service layer | Can trace native queueing, IPC, routing, flow-control, broker hop timing, and cross-host transport; language-neutral. | Requires trace context in native message headers or payload conventions; can add overhead to the lowest-level runtime. |
| Java framework layer | Natural integration with Java OpenTelemetry SDK and IoC frameworks; can instrument handlers and generated clients with application context. | Cannot see all native broker/service timing; per-message spans can allocate unless carefully sampled or disabled. |

Recommendation:

1. Start with Java framework hooks only: generated clients and handlers can call a
   `TraceAdapter` interface that defaults to no-op.
2. Keep trace propagation serializer-aware and opt-in.
3. Add native tracing later only for sampled messages or diagnostic modes, after
   measuring overhead. Native tracing should prefer counters/timestamps in
   metadata first, then spans only where there is a clear latency-analysis need.

## Example generated flow

Startup:

1. YAML is parsed into immutable config.
2. `RingloomRuntime` starts `RingloomService`.
3. Generated bootstrap metadata creates low-level clients.
4. Generated client proxies and handler dispatcher are constructed.
5. Serializers and per-thread contexts are preallocated.
6. Control and message event loops start.

Inbound message:

1. `MessageConsumerAgent` polls `MessageConsumer`.
2. Low-level binding updates one reusable `RingloomMessage`.
3. Generated dispatcher reads `templateId`.
4. Serializer wraps or decodes payload.
5. Handler is invoked with a reusable `MessageContext`.
6. Handler optionally sends responses through generated clients.
7. Handler returns a status code.

Outbound zero-copy send:

1. Generated proxy calculates encoded length.
2. Proxy calls `RingloomClient.tryClaim(templateId, length, claim)`.
3. Serializer writes directly into `claim.payloadSegment()`.
4. Proxy commits the claim.
5. Status is returned to the caller.

## Implementation phases

Each phase has a detailed implementation specification under `docs/java/impl/`.

1. **Framework skeleton**:
   [01-framework-skeleton.md](impl/01-framework-skeleton.md).
2. **Annotation processor**:
   [02-annotation-processor.md](impl/02-annotation-processor.md).
3. **YAML bootstrap**:
   [03-yaml-bootstrap.md](impl/03-yaml-bootstrap.md).
4. **SBE serializer**:
   [04-sbe-serializer.md](impl/04-sbe-serializer.md).
5. **Template-aware send ABI**:
   [05-template-aware-send-abi.md](impl/05-template-aware-send-abi.md).
6. **Metrics reader ABI**:
   [06-metrics-reader-abi.md](impl/06-metrics-reader-abi.md).
7. **Fory serializer**:
   [07-fory-serializer.md](impl/07-fory-serializer.md).
8. **Avaje IoC integration**:
   [08-avaje-ioc.md](impl/08-avaje-ioc.md).
9. **Tracing design**:
   [09-tracing-design.md](impl/09-tracing-design.md).
