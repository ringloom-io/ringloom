# RingLoom Java bindings

These bindings expose the `ringloom_service` native library through the Java Foreign Function & Memory API on Java 25.

The Gradle build embeds the current platform's shared library into the jar under the classpath, and the Java API loads that packaged copy by default. You can still override loading with `-Dringloom.nativeLibPath=/absolute/path/to/libringloom_service.so` or `-Dringloom.nativeLibDir=/directory/containing/the/library`.

`RingloomClient.targetServices()` returns the client's cached discovered targets. Each `TargetService` contains both `targetNodeId()` and `targetServiceId()` plus the leader flag; pass both ids to `sendTo(targetNodeId, targetServiceId, ...)` because service ids can repeat on different broker nodes.

Java services own control-plane progress. Call `RingloomService.pollControl(limit)` from an application thread to drain discovery/control messages, keep service heartbeats fresh, and update each client's cached target list. A `RingloomClient` can register `onLifecycle(...)`; availability and unavailability callbacks run synchronously on the thread that polls the control plane.

## API usage

Start a service, create clients from that service, and keep polling control messages while the service is active:

```java
try (RingloomService service = RingloomService.start(ServiceConfig.of("orders"));
     RingloomClient client = service.createClient("pricing")) {
    service.pollControl(256);
    for (TargetService target : client.targetServices()) {
        client.sendTo(target.targetNodeId(), target.targetServiceId(), MemorySegment.NULL);
    }
}
```

For hot-path sends, allocate a `BufferClaim` during setup with `client.newClaim()`, reuse it with `tryClaim(...)`, write to `claim.payloadSegment()`, and finish with `commit()` or `abort()`. `MessageConsumer.poll(...)` reuses a single `RingloomMessage` view; copy payload bytes inside the handler if they must outlive the callback.

## Prerequisites

- Java 25+
- Gradle 9+
- Zig 0.16.x

## Build

```bash
cd bindings/java
gradle classes
```

To build the distributable jar with the embedded native library:

```bash
cd bindings/java
gradle jar
```

When Gradle builds the embedded library itself, it uses `zig build service-c -Doptimize=ReleaseSmall`.

## Test

```bash
zig build test-java
```

You can also run Gradle directly:

```bash
cd bindings/java
gradle test \
  -Dringloom.projectRoot=/path/to/ringloom \
  -Dringloom.brokerBin=/path/to/ringloom/zig-out/bin/ringloom-broker
```

## Broker helper

The integration tests use `scripts/start-test-broker.sh`. Example manual usage:

```bash
# Single broker
./scripts/start-test-broker.sh --workspace /tmp/ringloom-java-it-demo --daemon
./scripts/start-test-broker.sh --workspace /tmp/ringloom-java-it-demo --stop

# Two connected brokers in the same workspace
./scripts/start-test-broker.sh --workspace /tmp/ringloom-java-it-demo --node-id 1 --port 19001 --peer 2@127.0.0.1:19002 --daemon
./scripts/start-test-broker.sh --workspace /tmp/ringloom-java-it-demo --node-id 2 --port 19002 --peer 1@127.0.0.1:19001 --daemon
./scripts/start-test-broker.sh --workspace /tmp/ringloom-java-it-demo --node-id 1 --stop
./scripts/start-test-broker.sh --workspace /tmp/ringloom-java-it-demo --node-id 2 --stop
```
