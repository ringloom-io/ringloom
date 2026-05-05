# Phase 9 — Tracing design

## Objective

Design tracing extension points after the runtime, generated dispatch, metrics,
and first IoC integration are stable. The initial implementation should be a
no-op Java trace adapter with generated-code hooks. Native Zig tracing should be
evaluated separately with latency measurements.

## Deliverables

1. Java tracing SPI in core.
2. No-op default implementation.
3. Generated client and handler hooks.
4. Documentation for Java-layer versus native-layer tracing.
5. Prototype OpenTelemetry adapter only if it does not compromise the core
   dependency model.

## Tracing SPI

Core should define interfaces without depending on OpenTelemetry:

```java
public interface TraceAdapter {
    TraceScope onSendStart(ClientTraceContext context);
    TraceScope onReceiveStart(MessageContext context);
    void onSendComplete(ClientTraceContext context, int status);
    void onHandlerComplete(MessageContext context, int status);
}

public interface TraceScope extends AutoCloseable {
    @Override
    void close();
}
```

Default:

```java
public final class NoopTraceAdapter implements TraceAdapter {
    // returns singleton no-op scope
}
```

Generated code should call the adapter only when tracing is enabled in runtime
config. The disabled path should be a predictable branch and should not allocate.

## Java framework tracing

Java-layer tracing can observe:

1. Generated client method entry/exit.
2. Serialization encode/decode time.
3. Handler method entry/exit.
4. RingLoom status outcomes.
5. Application template ids.

It cannot fully observe:

1. Broker sender and receiver event-loop timing.
2. Cross-host TCP queueing.
3. Native flow-control decisions unless exposed through metrics/events.

Java tracing should be opt-in and sampled. Per-message spans can allocate in
OpenTelemetry SDKs, so hot-path services should keep tracing disabled or sampled
heavily.

## Native tracing analysis

Native Zig tracing could observe:

1. Service-to-broker IPC enqueue time.
2. Broker send-ring drain time.
3. TCP frame send/receive times.
4. Receiver routing latency.
5. Flow-control and backpressure timing.

Costs:

1. Additional message metadata or trace-context propagation.
2. Potential hot-path branches and writes.
3. Export pipeline complexity.
4. Need for language-neutral trace context conventions.

Recommendation:

1. Start with native counters and derived gauges from phase 6.
2. Add Java no-op hooks and optional Java OpenTelemetry adapter.
3. Benchmark overhead before adding native spans.
4. If native tracing is added, make it opt-in and sampling-aware.

## Trace context propagation

Potential propagation options:

1. Application payload includes trace context.
2. Serializer-specific envelope includes trace context.
3. RingLoom protocol header extension carries trace flags/context.
4. Side-channel metadata maps correlation id to trace context.

Initial recommendation: serializer-specific or application payload propagation,
because it avoids native protocol changes. Revisit protocol header extensions
only after tracing requirements are concrete.

## Generated-code hooks

Generated client send:

1. Build `ClientTraceContext` from client name, target service, template id,
   routing mode, and payload length.
2. Call `TraceAdapter.onSendStart(...)`.
3. Encode/send.
4. Call `onSendComplete(...)`.
5. Close scope in `finally` for throwing methods; for status methods use a
   generated cleanup path without allocating exceptions.

Generated handler:

1. Update `MessageContext`.
2. Call `TraceAdapter.onReceiveStart(...)`.
3. Decode and invoke handler.
4. Call `onHandlerComplete(...)`.
5. Close scope.

The no-op implementation should make these calls cheap but the runtime config
should still support fully disabling hooks.

## OpenTelemetry adapter

If implemented, put it in a separate artifact:

```text
ringloom-tracing-opentelemetry
```

Dependencies:

1. `ringloom-framework-core`.
2. OpenTelemetry API.
3. OpenTelemetry SDK only if the adapter owns SDK configuration; otherwise depend
   only on API and accept injected tracer.

The core framework must not depend on OpenTelemetry.

## Testing

Unit tests:

1. No-op adapter returns no-op scopes and does not allocate in disabled path.
2. Generated client invokes trace hooks in the correct order.
3. Generated dispatcher invokes trace hooks in the correct order.
4. Exceptions/status failures still close scopes.

Integration tests:

1. Optional OpenTelemetry adapter records send and handler spans.
2. Sampling disabled produces no spans.
3. Tracing disabled path preserves allocation expectations.

Benchmarking:

1. Measure generated client send with tracing disabled.
2. Measure generated handler dispatch with tracing disabled.
3. Measure sampled tracing overhead with Java adapter.
4. Use results to decide whether native Zig tracing is warranted.

## Acceptance criteria

1. Core has tracing extension points with no OpenTelemetry dependency.
2. Generated code can be instrumented without reflection.
3. Disabled tracing does not compromise hot-path latency goals.
4. Native tracing remains a measured follow-up decision, not an assumption.
