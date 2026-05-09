# Phase 8 — Avaje IoC integration

## Objective

Provide the first IoC adapter for Avaje. The adapter should make generated
RingLoom clients, runtime, dispatcher, serializers, and metrics injectable as
Avaje beans without runtime annotation scanning by the RingLoom framework.

## Deliverables

1. `ringloom-ioc-avaje` artifact.
2. Generated Avaje module or bean registration metadata.
3. Integration with generated application metadata from the annotation
   processor.
4. Tests using an Avaje application context.

## Dependencies

The Avaje module depends on:

1. `ringloom-framework-core`.
2. `ringloom-framework-yaml` only if YAML bootstrap is supported directly from
   Avaje.
3. Avaje Inject APIs.

It should not depend on Spring or Micronaut.

## Bean model

Beans to expose:

1. `RingloomApplicationConfig`.
2. `SerializerRegistry`.
3. `GeneratedRingloomApplication`.
4. `RingloomRuntime`.
5. `RingloomApplication`.
6. `RingloomMetrics`.
7. `MessageExecutionPolicy`.
8. `RequestResponseRegistry`.
9. Generated client implementations.
10. Generated dispatcher.
11. Application handler components.

Generated clients should be injectable by their interface type:

```java
@Singleton
public final class OrderService {
    private final PricingClient pricing;

    public OrderService(PricingClient pricing) {
        this.pricing = pricing;
    }
}
```

## Generated registration strategy

Preferred strategy:

1. RingLoom annotation processor generates framework-neutral metadata.
2. Avaje integration processor or adapter reads that metadata at compile time.
3. It emits Avaje-compatible bean/module registration.

Fallback strategy:

1. Require users to import a generated RingLoom Avaje module explicitly.
2. The module constructs runtime and generated clients with explicit
   constructors.

The adapter should avoid RingLoom-specific reflection. If Avaje itself uses
generated metadata internally, that is acceptable.

## Runtime lifecycle

`RingloomRuntime` should start according to configuration:

1. Eager startup by default for standalone services.
2. Optional lazy startup for tests.
3. Close runtime when Avaje context shuts down.

The adapter should ensure shutdown order:

1. Stop event loops.
2. Close generated clients if they own resources.
3. Close low-level clients and service.
4. Close metrics reader.
5. Complete pending request/response callbacks or virtual-thread waiters with a
   shutdown status.

## Configuration

Supported configuration sources:

1. Programmatic `RingloomApplicationConfig` bean.
2. YAML file path supplied as an Avaje property.
3. Prebuilt `SerializerRegistry` bean.

If both config bean and YAML path are present, fail with a clear ambiguity error
unless one source is explicitly marked primary by the adapter's configuration
rules.

## Serializer beans

Serializer modules can be Avaje beans:

```java
@Singleton
public final class OrdersSerializers {
    @Bean
    SerializerRegistry serializerRegistry(List<SerializerModule> modules) {
        return SerializerRegistry.builder().modules(modules).build();
    }
}
```

Generated clients and dispatchers should receive concrete serializers through
the registry or direct generated constructor parameters.

## Metrics beans

Expose:

1. `RingloomMetrics`.
2. `RingloomMetricsReader` only if users opt into low-level binding access.

No custom application metric registration is required in this phase.

## Testing

Compile-time tests:

1. Avaje module generation includes generated client beans.
2. Duplicate bean definitions produce clear diagnostics.
3. Missing `GeneratedRingloomApplication` fails at startup or compile time.

Integration tests:

1. Avaje context starts RingLoom runtime.
2. A handler component receives an injected generated client.
3. Generated client interface is injectable.
4. `RingloomMetrics` is injectable.
5. `RequestResponseRegistry` is injectable for advanced users.
6. Context shutdown closes RingLoom runtime and completes pending requests.

End-to-end test:

1. Start test broker.
2. Start Avaje context for a service.
3. Send a message through injected client.
4. Handler processes message.
5. Close context and verify service unregisters.

## Acceptance criteria

1. Avaje users can use constructor injection for RingLoom clients and metrics.
2. RingLoom runtime lifecycle is tied to Avaje context lifecycle.
3. No RingLoom runtime reflection/scanning is introduced.
4. The integration remains separate from core.
