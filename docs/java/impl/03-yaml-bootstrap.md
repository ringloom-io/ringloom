# Phase 3 — YAML bootstrap

## Objective

Provide a standalone bootstrapper that starts a complete RingLoom Java service
from a YAML configuration file. YAML is startup-only and must not introduce a
dependency into the core framework.

## Deliverables

1. `ringloom-framework-yaml` artifact.
2. YAML schema mapped to immutable core configuration records.
3. `RingloomBootstrap.fromYaml(Path)` entry point.
4. Configuration validation with clear startup errors.
5. Tests for defaults, validation, and end-to-end startup.

## Dependencies

The YAML artifact depends on:

1. `ringloom-framework-core`.
2. SnakeYAML Engine.
3. `slf4j-api` through core.

It should not depend on SBE, Fory, Avaje, Spring, Micronaut, or test-only
libraries in production scope.

## YAML schema

Top-level shape:

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
    mode: dedicated
    control:
      idleStrategy: backoff
      pollLimit: 256
    messages:
      idleStrategy: busySpin
      pollLimit: 256
      execution:
        mode: consumerThread
        partitioned:
          workers: 4
          queueCapacity: 1024
          maxPayloadBytes: 4096
          backpressure: parkConsumer
        virtualThreads:
          maxInFlight: 10000
    requests:
      maxPending: 65536
      defaultTimeoutMillis: 5000
      pooledPendingRequests: true
    lifecycle:
      shutdownHook: true

  serializers:
    default: sbe
    entries:
      sbe:
        type: sbe
      fory:
        type: fory
        requireRegistration: true

  clients:
    pricing:
      service: pricing
      routing: loadBalanced
      serializer: sbe
```

The schema should be strict by default: unknown keys are configuration errors,
not silently ignored.

## Configuration mapping

YAML maps to these core records:

1. `RingloomApplicationConfig`.
2. `RingloomServiceRuntimeConfig`.
3. `RingloomEventLoopConfig`.
4. `RingloomClientRuntimeConfig`.
5. `RingloomSerializerConfig`.

All defaults should be defined in the core config records, not in ad hoc YAML
parsing code.

## Bootstrap API

```java
public final class RingloomBootstrap {
    public static RingloomBootstrap fromYaml(Path path);
    public static RingloomBootstrap fromConfig(RingloomApplicationConfig config);
    public static RingloomBootstrap builder();

    public RingloomBootstrap generatedApplication(GeneratedRingloomApplication generated);
    public RingloomBootstrap serializerRegistry(SerializerRegistry serializers);
    public RingloomApplication start();
}
```

`fromYaml(Path)` loads config but should not start the service until `start()` is
called.

Startup sequence:

1. Parse YAML into raw tree.
2. Validate top-level structure and unknown keys.
3. Map into immutable config records.
4. Load generated application metadata through an explicit parameter or
   `ServiceLoader`.
5. Build serializer registry from configured modules.
6. Construct `RingloomRuntime`.
7. Start event loops according to config.
8. Optionally register shutdown hook.

## ServiceLoader usage

Plain Java bootstrap can use `ServiceLoader` for generated application metadata.
This is startup-only and acceptable. It must not scan annotations or inspect
handler methods reflectively.

If multiple generated applications are found, require one of:

1. `ringloom.service.name` in YAML matching exactly one generated application.
2. Explicit `generatedApplication(...)` call in programmatic bootstrap.

## Validation rules

Startup should fail before native service start when:

1. Required `ringloom.service.name` is missing.
2. Buffer lengths are not positive powers of two.
3. Poll limits are negative.
4. Idle strategy name is unknown.
5. Runtime mode is unknown.
6. Client references an unknown serializer.
7. Serializer config declares an unknown type.
8. Generated application service name does not match YAML service name.
9. Duplicate client aliases map to incompatible target services.
10. Message execution mode is unknown.
11. Partitioned execution has non-positive worker count, non-power-of-two queue
    capacity, non-positive max payload size, or unknown backpressure policy.
12. Partitioned execution is enabled but generated metadata lacks required
    partition-key extractors.
13. Virtual-thread execution has non-positive `maxInFlight`.
14. Request/response config has non-positive `maxPending` or timeout values.

Validation errors should include YAML path-like locations where possible, for
example `ringloom.runtime.messages.pollLimit`.

## Shutdown behavior

When `shutdownHook` is true:

1. Register one hook per `RingloomApplication`.
2. Hook calls `close()` idempotently.
3. Hook does not block indefinitely; configurable timeout can be added later.

`awaitShutdown()` should wait on runtime state and react to interrupts by
restoring interrupt status.

## Testing

Unit tests:

1. Full YAML maps to expected config records.
2. Defaults match core defaults.
3. Unknown keys fail.
4. Invalid idle strategy fails.
5. Invalid buffer length fails.
6. Missing generated application fails with clear message.
7. Invalid message execution mode fails.
8. Partitioned execution validates worker count, queue capacity, max payload, and
   generated partition-key metadata.
9. Request/response defaults map to core config records.

Integration tests:

1. Start a service from YAML against a test broker.
2. Generated metadata is discovered via `ServiceLoader`.
3. Dedicated and shared modes start the expected event loops.
4. Shutdown hook path closes runtime idempotently.
5. Partitioned-worker execution starts the configured number of workers.
6. Virtual-thread execution uses bounded in-flight configuration.

## Acceptance criteria

1. Users can bootstrap a service from a YAML file with one line of code.
2. YAML parsing remains isolated from core.
3. Configuration errors are deterministic and descriptive.
4. Runtime behavior is identical to programmatic config after parsing.
5. Message execution and request/response config is fully represented in core
   immutable records.
