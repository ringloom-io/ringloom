# Phase 6 — Metrics reader ABI

## Objective

Expose native RingLoom service counters, derived gauges, and application-defined
service counters/gauges to Java through a stable C ABI and FFM binding.

## Deliverables

1. Native metrics reader handle.
2. C ABI for counter enumeration, ring statistics, and application metric
   registration/updates.
3. Java FFM binding wrapper.
4. Framework `RingloomMetrics` facade.
5. Tests against real service metadata.

## Scope

Included:

1. Existing service counters maintained in metadata.
2. Counter names and current values.
3. Derived gauges for service control and messages rings.
4. Optional liveness/identity values if already available safely.
5. Application counters/gauges registered at startup and stored in the native
   memory-mapped counter regions.

Deferred:

1. Prometheus server in Java.
2. Broker-wide metrics aggregation in Java.
3. Runtime metric label cardinality decisions beyond simple names.
4. Dynamic metric unregister/free APIs.

## C ABI design

Add opaque handle:

```c
typedef struct ringloom_metrics_reader ringloom_metrics_reader_t;
```

Metric types:

```c
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
```

Functions:

```c
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

ringloom_status_t ringloom_service_counter_register(
    ringloom_service_t *service,
    const char *name,
    size_t name_len,
    int32_t *out_counter_id
);

ringloom_status_t ringloom_service_gauge_register(
    ringloom_service_t *service,
    const char *name,
    size_t name_len,
    int32_t *out_gauge_id
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

ringloom_status_t ringloom_service_gauge_set(
    ringloom_service_t *service,
    int32_t gauge_id,
    int64_t value
);
```

Ring names:

1. `control`.
2. `messages`.

## Native implementation

The reader should reference the owning service handle and retain it safely while
the reader is alive.

Counter enumeration:

1. Read counter metadata slots from service metadata.
2. Expose allocated counters only.
3. Read values with acquire semantics.
4. Treat names as bounded byte slices.
5. Avoid allocation during `counter_at`.

Ring statistics:

1. Read ring producer and consumer positions.
2. Derive `used = producer - consumer`, clamped to capacity.
3. Derive `free = capacity - used`.
4. Return `INVALID_ARGUMENT` for unknown ring names.

Application metrics:

1. Register counters/gauges by allocating slots in the service metadata counter
   manager.
2. Publish metadata before marking slots allocated, so readers never observe a
   partially initialized metric.
3. Store metric kind in the counter metadata type id without changing the mmap
   layout.
4. Reject empty or overlong names instead of truncating Java-provided metric
   names.
5. Updates mutate the native memory-mapped `i64` value slots; Java must register
   metrics at startup, not per request.

Error handling:

1. Invalid reader/service pointers return `RINGLOOM_ERR_INVALID_ARGUMENT`.
2. Out-of-range counter index returns `RINGLOOM_ERR_INVALID_ARGUMENT`.
3. Inconsistent metadata returns `RINGLOOM_ERR_INTERNAL` with thread-local error
   detail.

## Java binding

Add `RingloomMetricsReader implements AutoCloseable`.

API:

```java
public final class RingloomMetricsReader implements AutoCloseable {
    public int counterCount();
    public MetricSample counterAt(int index);
    public RingStats ringStats(String ringName);
    public List<MetricSample> countersSnapshot();
    public void close();
}

public final class RingloomService implements AutoCloseable {
    public RingloomMetricsReader metricsReader();
    public NativeCounter registerCounter(String name);
    public NativeGauge registerGauge(String name);
}

public final class NativeCounter {
    public int counterId();
    public void increment();
    public void add(long delta);
    public void set(long value);
}

public final class NativeGauge {
    public int gaugeId();
    public void set(long value);
}
```

Value types:

```java
public record MetricSample(String name, MetricKind kind, long value) {}
public enum MetricKind { COUNTER, GAUGE }
public record RingStats(long capacityBytes, long usedBytes, long freeBytes,
                        long producerPosition, long consumerPosition) {}
```

`counterAt` may allocate a `String` and value object; this is not a message hot
path API. If a no-allocation snapshot is needed later, add visitor-style APIs.

## Framework facade

`RingloomMetrics` should wrap `RingloomMetricsReader`:

```java
public interface RingloomMetrics {
    MetricSample sample(String name);
    List<MetricSample> samples();
    RingStats ringStats(String ringName);
}
```

The facade should be injectable and should not require users to depend on the
low-level binding package directly.

## Testing

Zig tests:

1. Create service metadata with counters and read count.
2. Read known counter by index.
3. Ring stats derive expected capacity/used/free values.
4. Unknown ring name returns invalid argument.
5. Reader retains service handle safely.

Java tests:

1. Start service and create metrics reader.
2. Send/receive messages and observe counters change.
3. Read control/messages ring stats.
4. Closing reader is idempotent.
5. Registering Java counters/gauges stores off-heap native metric slots.
6. Counter/gauge updates are visible through `RingloomMetricsReader`.

Framework tests:

1. `RingloomRuntime.metrics()` returns a working facade.
2. Metrics facade can be injected manually into a test component.

## Acceptance criteria

1. Java can read native RingLoom service counters.
2. Java can read derived service ring gauges.
3. No new metric aggregation is added to native hot paths.
4. Application counters/gauges can be registered and updated from Java without
   Java heap-backed metric state.
