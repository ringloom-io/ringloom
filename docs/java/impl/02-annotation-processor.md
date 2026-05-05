# Phase 2 — Annotation processor

## Objective

Generate RingLoom clients, message dispatchers, and bootstrap metadata at compile
time. Runtime dispatch must not depend on reflection or annotation scanning.

## Deliverables

1. Annotation API.
2. `ringloom-framework-processor` artifact.
3. Generated client implementations.
4. Generated message dispatcher.
5. Generated application metadata for standalone bootstrap and Avaje integration.
6. Compile-time validation for template ids, serializers, handler signatures,
   and client method signatures.

## Annotation API

Initial annotations:

```java
@Target(TYPE)
@Retention(SOURCE)
public @interface RingloomApplication {
    String service() default "";
}

@Target(TYPE)
@Retention(SOURCE)
public @interface RingloomServiceComponent {
}

@Target(TYPE)
@Retention(SOURCE)
public @interface RingloomClient {
    String service();
}

@Target(METHOD)
@Retention(SOURCE)
public @interface RingloomRequest {
    int templateId();
    String serializer() default "";
    RoutingMode routing() default RoutingMode.LOAD_BALANCED;
    ErrorPolicy errorPolicy() default ErrorPolicy.STATUS;
}

@Target(METHOD)
@Retention(SOURCE)
public @interface RingloomHandler {
    int templateId();
    String serializer() default "";
}

@Target(METHOD)
@Retention(SOURCE)
public @interface RingloomLifecycleHandler {
    String client() default "";
}
```

Template ids are globally unique per service. The processor must reject duplicate
handler template ids across all compiled service components for the same service.

## Generated clients

For every interface annotated with `@RingloomClient`, generate an implementation:

```text
<package>/<InterfaceName>_RingloomClient.java
```

The generated class should:

1. Implement the annotated interface.
2. Have an explicit constructor accepting `RingloomRuntime`,
   low-level `RingloomClient`, and serializer dependencies.
3. Resolve all serializers during construction.
4. Preallocate reusable claim/context objects for non-thread-safe hot-path use or
   document thread affinity.
5. Return status codes for methods using `ErrorPolicy.STATUS`.
6. Provide no runtime reflection over the interface methods.

Supported method shapes:

1. Copy/ergonomic:
   `int send(MyMessage message)`.
2. Zero-copy context:
   `int send(MyFlyweight message, DirectSendContext context)`.
3. Raw payload:
   `int send(MemorySegment payload)`.
4. Throwing convenience:
   `void sendOrThrow(MyMessage message)`.

Unsupported signatures should fail compilation with actionable diagnostics.

## Generated dispatcher

Generate one dispatcher per service:

```text
<generated-package>/<ServiceName>_RingloomDispatcher.java
```

The dispatcher should:

1. Implement `GeneratedMessageDispatcher`.
2. Use a generated `switch` on globally unique template id.
3. Resolve handler target instances through constructor injection.
4. Pre-create serializer decoder/flyweight instances during construction.
5. Reuse `MessageContext` and serializer contexts supplied by the event-loop
   thread.
6. Return a status code for each message.

Example generated shape:

```java
public final class Orders_RingloomDispatcher implements GeneratedMessageDispatcher {
    private final OrderHandlers orderHandlers;
    private final OrderDecoder orderDecoder;

    public int onMessage(RingloomMessage message, MessageContext context) {
        context.updateFrom(message);
        return switch (message.templateId()) {
            case OrderTemplates.NEW_ORDER -> orderHandlers.onNewOrder(
                orderDecoder.wrap(message.payloadSegment(), context.decodeContext()),
                context
            );
            default -> RingloomHandlerStatus.UNKNOWN_TEMPLATE_ID;
        };
    }
}
```

## Generated application metadata

Generate a class implementing `GeneratedRingloomApplication`:

```text
<generated-package>/<ServiceName>_RingloomApplication.java
```

It should expose:

1. Service name.
2. Client bindings.
3. Dispatcher factory.
4. Component requirements for IoC modules.
5. Serializer names used by clients and handlers.

For plain Java bootstrap, also generate a `META-INF/services` entry for a
provider interface such as `GeneratedRingloomApplicationProvider`.

## Processor validation

Validation failures should be compile errors:

1. Duplicate globally unique handler template id.
2. Template id outside unsigned 16-bit range.
3. Unknown serializer name when it can be resolved statically.
4. Client interface contains default/private/static methods that cannot be
   generated safely.
5. Handler method has unsupported parameter or return types.
6. Handler class cannot be constructed by generated bootstrap metadata.
7. Multiple applications or services are present without an explicit selected
   service name.
8. A hot-path method requests zero-copy but uses a serializer that only supports
   allocation/copy semantics.

Warnings:

1. Throwing client methods on a hot-path interface.
2. Fory serializer usage in handlers marked hot path.
3. Handler retaining borrowed payload is not statically detectable; document this
   in generated Javadocs.

## Processor implementation notes

Use standard `javax.annotation.processing` APIs:

1. `AbstractProcessor`.
2. `RoundEnvironment`.
3. `Messager` for diagnostics.
4. `Filer` for generated Java and service resources.
5. `Elements` and `Types` for model inspection.

Avoid external code-generation dependencies initially. Generated Java can be
written with small internal renderers to keep dependencies minimal.

## Incremental compilation

Generated outputs should be deterministic:

1. Stable ordering by package and type name.
2. Stable ordering by template id.
3. No timestamps or machine-specific paths.
4. One generated file per source type where practical.

This helps Gradle and future build tools cache annotation processing results.

## Testing

Compile-time tests:

1. Minimal client interface generates implementation.
2. Minimal handler class generates dispatcher.
3. Duplicate template id fails compilation.
4. Unsupported signatures fail compilation.
5. Generated files contain no reflection over RingLoom annotations.

Runtime tests:

1. Generated client sends to a real broker/service setup.
2. Generated dispatcher handles an inbound message.
3. Generated standalone metadata can build `RingloomRuntime`.
4. Generated client with zero-copy context does not allocate on steady-state
   sends.

## Acceptance criteria

1. Applications can define client interfaces and handler classes with
   annotations only.
2. Generated code compiles on Java 25.
3. Runtime startup uses generated metadata, not annotation scanning.
4. Handler dispatch is a generated switch or equivalent constant-time table.
5. Duplicate template ids are rejected at compile time.
