# RingLoom Java bindings

These bindings expose the `ringloom_service` native library through the Java Foreign Function & Memory API on Java 25.

`RingloomClient.targetServices()` returns the currently discovered target service IDs for that client together with each target's leader flag, which makes `sendTo(targetServiceId, ...)` usable from Java without an out-of-band discovery channel.

Java services own control-plane progress. Call `RingloomService.pollControl(limit)` from an application thread to drain discovery/control messages and keep service heartbeats fresh. A `RingloomClient` can register `onLifecycle(...)`; availability and unavailability callbacks run synchronously on the thread that polls the control plane.

## Prerequisites

- Java 25+
- Gradle 9+
- Native artifacts built from the repository root:

```bash
zig build service-c
```

## Build

```bash
cd bindings/java
gradle classes
```

## Test

```bash
zig build test-java
```

You can also run Gradle directly:

```bash
cd bindings/java
gradle test \
  -Dringloom.projectRoot=/path/to/ringloom \
  -Dringloom.nativeLibDir=/path/to/ringloom/zig-out/lib \
  -Dringloom.brokerBin=/path/to/ringloom/zig-out/bin/ringloom-broker
```

## Broker helper

The integration tests use `scripts/start-test-broker.sh`. Example manual usage:

```bash
./scripts/start-test-broker.sh --workspace /tmp/ringloom-java-it-demo --daemon
./scripts/start-test-broker.sh --workspace /tmp/ringloom-java-it-demo --stop
```
