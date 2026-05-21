// SPDX-License-Identifier: Apache-2.0
package io.ringloom.service;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.VarHandle;
import java.util.Objects;

public final class NativeGauge {
    private static final VarHandle VALUE = ValueLayout.JAVA_LONG.varHandle();

    private final RingloomService service;
    private final int gaugeId;
    private final MemorySegment valueSegment;

    NativeGauge(RingloomService service, int gaugeId, MemorySegment valueSegment) {
        this.service = Objects.requireNonNull(service, "service");
        this.gaugeId = gaugeId;
        this.valueSegment = Objects.requireNonNull(valueSegment, "valueSegment");
    }

    public int gaugeId() {
        service.ensureOpen();
        return gaugeId;
    }

    public long value() {
        service.ensureOpen();
        return (long) VALUE.getAcquire(valueSegment, 0L);
    }

    public void set(long value) {
        service.ensureOpen();
        VALUE.setRelease(valueSegment, 0L, value);
    }
}
