# Phase 1 — Framework skeleton

## Objective

Create the Java framework core without annotation processing, YAML, serializer
plugins, or IoC adapters. This phase establishes the runtime contracts that
generated code and integrations will use later.

## Deliverables

1. New Gradle module or source set for `ringloom-framework-core`.
2. Public runtime API for starting, polling, and stopping a service.
3. Event-loop, agent, and idle-strategy data structures.
4. Message execution policy contracts for consumer-thread, partitioned-worker,
   and virtual-thread dispatch.
5. Request/response registry contracts and reusable pending request state.
6. Serializer SPI placeholders, with no concrete SBE/Fory implementation yet.
7. Generated-code contracts that can be implemented manually in tests.
8. Baseline tests proving lifecycle, polling, shutdown, and no hot-path
   allocations in the manual zero-copy path.

## Package structure

```text
io.ringloom.framework
io.ringloom.framework.config
io.ringloom.framework.dispatch
io.ringloom.framework.eventloop
io.ringloom.framework.generated
io.ringloom.framework.metrics
io.ringloom.framework.request
io.ringloom.framework.serialization
io.ringloom.framework.status
```

The core artifact depends on:

1. `ringloom-java-bindings`.
2. `slf4j-api`.

It should not depend on YAML, SBE, Fory, Avaje, Spring, Micronaut, JUnit, or
Prometheus libraries.

## Core runtime types

### `RingloomRuntime`

Responsibilities:

1. Own the low-level `RingloomService`.
2. Create and retain low-level `RingloomClient` instances by logical service
   name.
3. Own the single `MessageConsumer` for the local service ingress.
4. Own the configured message execution policy.
5. Own the request/response registry.
6. Own the configured control and message event loops.
7. Expose manual polling for externally managed runtimes.
8. Expose lifecycle state and deterministic shutdown.

Suggested constructor:

```java
public RingloomRuntime(
    RingloomApplicationConfig config,
    GeneratedRingloomApplication generatedApplication,
    SerializerRegistry serializers,
    RingloomMetrics metrics,
    Logger logger
);
```

For phase 1, tests can pass a manually implemented
`GeneratedRingloomApplication`.

Lifecycle methods:

```java
public void start();
public int pollControl();
public int pollMessages();
public void startEventLoops(ThreadFactory threadFactory);
public void awaitShutdown() throws InterruptedException;
public RingloomClient lowLevelClient(String targetServiceName);
public <T> T generatedClient(Class<T> clientType);
public MessageExecutionPolicy messageExecutionPolicy();
public RequestResponseRegistry requestResponseRegistry();
public RingloomMetrics metrics();
public void close();
```

`close()` must be idempotent and must stop event loops before closing clients,
consumer, and service handles.

### `RingloomApplication`

`RingloomApplication` is the standalone lifecycle wrapper returned by bootstrap
APIs.

```java
public final class RingloomApplication implements AutoCloseable {
    public RingloomRuntime runtime();
    public void awaitShutdown() throws InterruptedException;
    public void close();
}
```

It should not expose annotation-processing details.

## Generated-code contracts

The annotation processor will implement these contracts later. Phase 1 should
define and test them with manual classes.

```java
public interface GeneratedRingloomApplication {
    String serviceName();
    List<GeneratedClientBinding<?>> clients();
    GeneratedMessageDispatcher dispatcher();
    default void onRuntimeStarted(RingloomRuntime runtime) {}
    default void onRuntimeStopping(RingloomRuntime runtime) {}
}

public interface GeneratedClientBinding<T> {
    Class<T> clientType();
    String targetServiceName();
    T create(RingloomRuntime runtime, RingloomClient lowLevelClient, SerializerRegistry serializers);
}

public interface GeneratedMessageDispatcher extends MessageHandler {
    int onMessage(RingloomMessage message, MessageContext context);
}

public interface GeneratedPartitionKeyExtractor {
    long partitionKey(RingloomMessage message, MessageContext context);
}
```

The actual low-level `MessageConsumer` expects `MessageHandler`. The framework
should adapt the generated dispatcher or execution policy without allocating per
message.

## Event-loop data structures

### `Agent`

```java
public interface Agent {
    int doWork();
    default void onStart() {}
    default void onClose() {}
    default String name() { return getClass().getSimpleName(); }
}
```

`doWork()` returns the amount of work completed. Idle strategies use this value
to decide whether to spin, yield, park, or reset backoff.

### `EventLoop`

`EventLoop` owns one `Agent` and an `IdleStrategy`.

Required behavior:

1. `run()` loops until closed.
2. `startThread(ThreadFactory)` creates exactly one thread.
3. `close()` requests shutdown and waits for thread termination when the loop
   started its own thread.
4. Agent `onStart()` and `onClose()` are called once on the event-loop thread.
5. Runtime exceptions are logged and recorded in runtime state; they must not be
   silently swallowed.

### `CompositeAgent`

`CompositeAgent` runs a fixed list of agents on one thread.

Hot-path constraints:

1. Store agents in an array created at startup.
2. Do not allocate during `doWork()`.
3. Sum work counts with saturation at `Integer.MAX_VALUE`.

### `ControlAgent`

Wraps `RingloomService.pollControl(limit)`.

Configuration:

1. `pollLimit`, default `256`.
2. Error policy for native failures: log and stop runtime for unexpected
   exceptions; status-code expected failures are not expected from current
   `pollControl` wrapper.

### `MessageConsumerAgent`

Wraps `MessageConsumer.poll(ingress, limit)`.

Responsibilities:

1. Own one reusable `MessageContext`.
2. Delegate to the configured `MessageExecutionPolicy`.
3. Return `-1` or throw only according to a configured error policy.
4. Preserve the borrowed-payload lifetime contract from `RingloomMessage`.

The ingress callback must finish all work that depends on the borrowed
`RingloomMessage` before returning to the low-level consumer. Consumer-thread
dispatch can invoke generated handlers directly. Async policies must copy message
headers and payload bytes into policy-owned storage before returning.

## Message execution policy contracts

Phase 1 should define the policy interfaces and provide simple implementations
that can be tested with manual dispatchers.

```java
public interface MessageExecutionPolicy extends AutoCloseable {
    int onMessage(RingloomMessage message, MessageContext ingressContext);
}

public interface PartitionKeyExtractor {
    long partitionKey(RingloomMessage message, MessageContext ingressContext);
}
```

Required policies:

| Policy | Required phase-1 behavior |
|---|---|
| `ConsumerThreadExecutionPolicy` | Calls `GeneratedMessageDispatcher.onMessage(...)` on the polling thread with reusable contexts. |
| `PartitionedWorkerExecutionPolicy` | Routes by `PartitionKeyExtractor`, copies into a preallocated SPSC slot, and wakes the target worker. |
| `VirtualThreadExecutionPolicy` | Copies or decodes payload into task-owned state and submits bounded virtual-thread work. |

Partitioned workers must use fixed worker counts and fixed hash mapping for the
life of the runtime. Each worker owns its queue, dispatcher context, decode
context, and flyweights. Queue-full behavior defaults to parking/backing off the
consumer thread until the target queue accepts the message; single-message drop
is not allowed because it can break per-key ordering.

Phase 1 does not need a production SPSC implementation, but the interfaces must
make the cross-thread copy explicit so later zero-allocation worker queues do not
depend on borrowed low-level payload memory.

## Idle strategies

Implement these allocation-free strategies:

1. `BusySpinIdleStrategy`: calls `Thread.onSpinWait()` when no work.
2. `YieldingIdleStrategy`: spins for N cycles, then `Thread.yield()`.
3. `SleepingIdleStrategy`: parks with configured nanoseconds.
4. `BackoffIdleStrategy`: spin, yield, then park with capped exponential backoff.
5. `NoOpIdleStrategy`: does nothing; useful in manual tests.

All strategies expose `void idle(int workCount)` and `void reset()`.

## Configuration records

Core config should be immutable Java records:

1. `RingloomApplicationConfig`.
2. `RingloomServiceRuntimeConfig`.
3. `RingloomEventLoopConfig`.
4. `RingloomClientRuntimeConfig`.
5. `RingloomSerializerConfig`.

Validation should happen at construction or through a separate validator. Invalid
configuration should fail before starting the low-level service.

## Message context

`MessageContext` is reused per message-consumer or worker thread.

Fields:

1. Correlation id.
2. Source node id.
3. Source service id.
4. Target node id.
5. Target service id.
6. Template id.
7. Flags.
8. Borrowed payload segment.
9. Runtime reference for reply/client lookup.

`MessageContext.updateFrom(RingloomMessage)` must not allocate.

## Request/response core contracts

Phase 1 should define the public contracts without implementing generated client
methods yet:

```java
public interface ResponseCallback<T> {
    int onResponse(T response, MessageContext context, Object userContext);
    default int onTimeout(Object userContext) { return RingloomHandlerStatus.OK; }
    default int onFailure(int status, Object userContext) { return status; }
}

public interface RequestResponseRegistry {
    PendingRequest acquire();
    int register(PendingRequest request);
    PendingRequest resolve(long correlationId, int responseTemplateId);
    void cancel(PendingRequest request, int status);
    void completeAll(int status);
}
```

`PendingRequest` should be reusable and owned by the registry or caller-provided
`DirectRequestContext`. It stores correlation id, generation, expected response
template id, callback, user context, timeout deadline, and waiter state for
virtual-thread blocking calls. Reused slots must include a generation check so a
late response cannot complete a different request after timeout and reuse.

Virtual-thread blocking support should be represented by a small `RequestAwaiter`
abstraction that can park/unpark the current thread with public JDK APIs. Phase 1
should not depend on JDK-internal virtual-thread scheduler classes.

## Serialization placeholders

Define minimal SPI types so generated-code contracts can compile:

1. `SerializerRegistry`.
2. `MessageEncoder<T>`.
3. `MessageDecoder<T>`.
4. `FlyweightDecoder<T>`.
5. `EncodeContext`.
6. `DecodeContext`.
7. `ReadableMessageBuffer`.
8. `WritableMessageBuffer`.

Phase 1 can include a `RawSegmentSerializer` for tests only.

## Testing

Unit tests:

1. Idle strategies transition correctly and do not allocate after construction.
2. `CompositeAgent` invokes all agents and sums work.
3. `EventLoop` starts, stops, and calls lifecycle hooks exactly once.
4. `MessageContext` updates from a reusable message view.
5. Runtime shutdown is idempotent.
6. Consumer-thread execution policy invokes a manual dispatcher without
   allocation.
7. Request registry rejects late responses after slot reuse.

Integration tests:

1. Start broker and low-level service through `RingloomRuntime`.
2. Manually implemented generated dispatcher receives a message.
3. Manual generated client can send through low-level `RingloomClient`.
4. Dedicated and shared event-loop modes both work.
5. Partitioned-worker mode preserves ordering for one partition key in a manual
   dispatcher test.

Allocation tests:

1. Repeated `MessageConsumerAgent.doWork()` with a no-op dispatcher.
2. Repeated `CompositeAgent.doWork()`.
3. Repeated zero-copy send using a manually implemented client binding.
4. Repeated callback request registration with pooled pending state.

## Acceptance criteria

1. Core compiles and tests run with Java 25.
2. Core has no dependencies beyond low-level bindings and `slf4j-api`.
3. A service can be started, polled, and stopped without annotations.
4. The event-loop model can be reused by control and message agents.
5. Message execution policy contracts are stable enough for the annotation
   processor phase.
6. Request/response registry contracts are stable enough for generated clients.
