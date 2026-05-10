// SPDX-License-Identifier: Apache-2.0
package io.ringloom.service;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public final class RingloomMetricsReader implements AutoCloseable {
    private final MemorySegment nativeHandle;
    private final AtomicBoolean closed;

    RingloomMetricsReader(MemorySegment nativeHandle) {
        this.nativeHandle = nativeHandle;
        this.closed = new AtomicBoolean(false);
    }

    public int counterCount() {
        ensureOpen();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outCount = arena.allocate(ValueLayout.JAVA_LONG);
            int status = RingloomNative.metricsCounterCount(nativeHandle, outCount);
            RingloomNative.throwForStatus("ringloom_metrics_reader_counter_count", status);
            long count = outCount.get(ValueLayout.JAVA_LONG, 0);
            if (count > Integer.MAX_VALUE) {
                throw new IllegalStateException("counter count is out of range: " + count);
            }
            return (int) count;
        }
    }

    public MetricSample counterAt(int index) {
        ensureOpen();
        if (index < 0) {
            throw new IllegalArgumentException("index must be non-negative");
        }

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outMetric = arena.allocate(RingloomNative.METRIC_DESCRIPTOR_SIZE, 8);
            int status = RingloomNative.metricsCounterAt(nativeHandle, index, outMetric);
            RingloomNative.throwForStatus("ringloom_metrics_reader_counter_at", status);

            MemorySegment nameAddress = outMetric.get(RingloomNative.ADDRESS, RingloomNative.METRIC_NAME_OFFSET);
            long nameLength = outMetric.get(ValueLayout.JAVA_LONG, RingloomNative.METRIC_NAME_LEN_OFFSET);
            int kind = outMetric.get(ValueLayout.JAVA_INT, RingloomNative.METRIC_KIND_OFFSET);
            long value = outMetric.get(ValueLayout.JAVA_LONG, RingloomNative.METRIC_VALUE_OFFSET);
            return new MetricSample(readUtf8(nameAddress, nameLength), MetricKind.fromNative(kind), value);
        }
    }

    public List<MetricSample> countersSnapshot() {
        int count = counterCount();
        ArrayList<MetricSample> samples = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            samples.add(counterAt(i));
        }
        return List.copyOf(samples);
    }

    public RingStats ringStats(String ringName) {
        ensureOpen();
        Objects.requireNonNull(ringName, "ringName");
        if (ringName.isEmpty()) {
            throw new IllegalArgumentException("ringName must not be empty");
        }

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment nativeName = arena.allocateFrom(ringName);
            MemorySegment outStats = arena.allocate(RingloomNative.RING_STATS_SIZE, 8);
            int status = RingloomNative.metricsRingStats(
                nativeHandle,
                nativeName,
                ringName.getBytes(StandardCharsets.UTF_8).length,
                outStats
            );
            RingloomNative.throwForStatus("ringloom_metrics_reader_ring_stats", status);
            return new RingStats(
                outStats.get(ValueLayout.JAVA_LONG, RingloomNative.RING_STATS_CAPACITY_OFFSET),
                outStats.get(ValueLayout.JAVA_LONG, RingloomNative.RING_STATS_USED_OFFSET),
                outStats.get(ValueLayout.JAVA_LONG, RingloomNative.RING_STATS_FREE_OFFSET),
                outStats.get(ValueLayout.JAVA_LONG, RingloomNative.RING_STATS_PRODUCER_OFFSET),
                outStats.get(ValueLayout.JAVA_LONG, RingloomNative.RING_STATS_CONSUMER_OFFSET)
            );
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        RingloomNative.destroyMetricsReader(nativeHandle);
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException("RingloomMetricsReader is closed");
        }
    }

    private static String readUtf8(MemorySegment address, long length) {
        if (length == 0 || address.address() == 0) {
            return "";
        }
        if (length > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("string length is out of range: " + length);
        }

        MemorySegment bytes = address.reinterpret(length);
        byte[] copy = new byte[(int) length];
        MemorySegment.copy(bytes, ValueLayout.JAVA_BYTE, 0, copy, 0, copy.length);
        return new String(copy, StandardCharsets.UTF_8);
    }
}
