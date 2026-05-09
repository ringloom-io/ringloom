# Phase 11 — Request/response APIs

## Objective

Add generated request/response clients that correlate responses to requests. The
API should support allocation-free callback usage for hot paths and synchronous
blocking usage on virtual threads for ergonomic code.

## Deliverables

1. `RequestResponseRegistry` and pooled `PendingRequest` implementation.
2. Correlation id generation with generation checks for slot reuse safety.
3. Generated callback request/response client methods.
4. Generated virtual-thread blocking request methods.
5. Response resolution before normal handler dispatch.
6. Timeout, cancellation, interruption, and shutdown behavior.
7. Tests for local and remote response routing.

## Correlation and reply routing

Every request/response send must set a caller-selected `correlation_id`. The
request sender registers the pending request before publishing the message. The
responder sends replies to the request `MessageContext` source node and source
service id and echoes the correlation id.

Correlation ids should include:

1. A request sequence.
2. A generation counter to reject late responses after timeout and slot reuse.
3. Optional worker index bits for partitioned-worker callback affinity.

The registry must validate both correlation id and expected response template id
before completing a pending request.

## Callback API

Hot-path generated method shape:

```java
@RingloomRequest(
    templateId = PricingTemplates.PRICE_REQUEST,
    responseTemplateId = PricingTemplates.PRICE_RESPONSE,
    serializer = "sbe"
)
int requestPrice(
    PriceRequestFlyweight request,
    ResponseCallback<PriceResponseFlyweight> callback,
    Object userContext,
    DirectRequestContext context
);
```

Rules:

1. `DirectRequestContext` owns or references a reusable pending slot and send
   claim.
2. The callback should be a stateless singleton or otherwise caller-reused object.
3. `userContext` lets callers pass pooled per-request state without capturing a
   lambda.
4. The framework must not allocate callback wrappers, boxed ids, futures, or
   result objects on the hot path.
5. Response flyweights or memory segments passed to the callback are valid only
   during the callback invocation.

Ergonomic callback overloads may allocate decoded response objects or futures,
but they must be named and documented so they are not confused with the hot-path
profile.

## Pending request registry

`PendingRequest` fields:

1. Correlation id.
2. Generation.
3. Expected response template id.
4. Serializer/decoder references.
5. Callback and user context.
6. Deadline.
7. Completion status.
8. Optional virtual-thread waiter.

Lifecycle:

1. Acquire or initialize pending state.
2. Register in the correlation table.
3. Publish request send.
4. Complete from response, timeout, cancellation, interruption, or shutdown.
5. Clear references and return pooled state only after completion is visible.

Late responses with stale generations must be dropped and counted. They must not
complete a newly reused pending slot.

## Response dispatch

Response ingress flow:

1. Message execution policy receives the message.
2. Generated dispatcher checks the registry by correlation id before regular
   template dispatch.
3. If a pending request matches, the response decoder wraps or decodes the
   payload and completes the callback or waiter.
4. If no pending request matches, generated `@RingloomResponseHandler` methods
   can handle the message.
5. Otherwise the unknown-template or unknown-correlation policy applies.

In partitioned-worker mode, response completion should execute on the worker that
owns the request's partition when thread affinity is required. The correlation id
can encode the originating worker index, or the registry can maintain a
preallocated correlation-to-worker table.

## Virtual-thread blocking API

Generated method shape:

```java
@RingloomRequest(
    templateId = PricingTemplates.PRICE_REQUEST,
    responseTemplateId = PricingTemplates.PRICE_RESPONSE,
    serializer = "sbe",
    mode = RequestMode.VIRTUAL_THREAD_BLOCKING
)
PriceResponse requestPriceBlocking(PriceRequest request, RequestTimeout timeout)
    throws RingloomRequestException, InterruptedException;
```

Implementation rules:

1. Register pending state before sending.
2. Park the current virtual thread with `LockSupport.parkNanos` or use an
   equivalent Loom-friendly public blocking primitive.
3. Store the result and call `LockSupport.unpark(waitingThread)` on response,
   timeout, cancellation, or shutdown.
4. Check completion in a loop to handle spurious wakeups.
5. On interrupt, unregister the pending request, restore/propagate interrupt
   semantics by throwing `InterruptedException`, and ensure late responses are
   rejected by generation checks.
6. On timeout, unregister and throw or return a deterministic timeout status.
7. On runtime shutdown, unpark all waiters with a shutdown exception/status.

The implementation must not depend on JDK-internal virtual-thread scheduler
classes or attempt to override the JDK virtual-thread harness.

## Required low-level support

Request/response depends on correlation-aware claim and send methods from
[05-template-aware-send-abi.md](05-template-aware-send-abi.md). Framework startup
must verify those symbols before enabling generated request/response clients.

## Testing

Unit tests:

1. Correlation id generation includes generation and rejects stale responses.
2. Pending registry registers, resolves, cancels, times out, and reuses slots.
3. Callback completion clears references before returning a slot to the pool.
4. Virtual-thread waiter handles response, timeout, interrupt, spurious wakeup,
   and shutdown.

Integration tests:

1. Local request/response round trip invokes callback with the expected response.
2. Remote broker request/response preserves correlation id.
3. Partitioned-worker response completes on the expected worker.
4. Virtual-thread blocking request returns response synchronously to the caller.
5. Late responses after timeout are dropped and counted.

Allocation tests:

1. Callback request registration and response completion are allocation-free with
   `DirectRequestContext` and reusable callback/context objects.
2. Virtual-thread blocking methods are excluded from hot-path allocation
   guarantees.

## Acceptance criteria

1. Generated clients can send requests and receive correlated responses.
2. Callback APIs support a documented zero-allocation profile.
3. Virtual-thread callers can write synchronous request/response code without
   JDK-internal scheduler hooks.
4. Timeouts, interrupts, cancellation, and shutdown do not leak pending requests
   or deliver late responses to reused slots.
