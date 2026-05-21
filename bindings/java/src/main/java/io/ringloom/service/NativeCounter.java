// SPDX-License-Identifier: Apache-2.0
package io.ringloom.service;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.VarHandle;
import java.util.Objects;

public final class NativeCounter {
    private static final VarHandle VALUE = ValueLayout.JAVA_LONG.varHandle();

    private final RingloomService service;
    private final int counterId;
    private final MemorySegment valueSegment;

    NativeCounter(RingloomService service, int counterId, MemorySegment valueSegment) {
        this.service = Objects.requireNonNull(service, "service");
        this.counterId = counterId;
        this.valueSegment = Objects.requireNonNull(valueSegment, "valueSegment");
    }

    public int counterId() {
        service.ensureOpen();
        return counterId;
    }

    public long value() {
        service.ensureOpen();
        return (long) VALUE.getAcquire(valueSegment, 0L);
    }

    public void increment() {
        add(1);
    }

    public void add(long delta) {
        service.ensureOpen();
        VALUE.getAndAddRelease(valueSegment, 0L, delta);
    }

    public void set(long value) {
        service.ensureOpen();
        VALUE.setRelease(valueSegment, 0L, value);
    }
}
