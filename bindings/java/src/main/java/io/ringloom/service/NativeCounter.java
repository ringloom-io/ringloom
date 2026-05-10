// SPDX-License-Identifier: Apache-2.0
package io.ringloom.service;

import java.util.Objects;

public final class NativeCounter {
    private final RingloomService service;
    private final int counterId;

    NativeCounter(RingloomService service, int counterId) {
        this.service = Objects.requireNonNull(service, "service");
        this.counterId = counterId;
    }

    public int counterId() {
        service.ensureOpen();
        return counterId;
    }

    public void increment() {
        add(1);
    }

    public void add(long delta) {
        int status = RingloomNative.serviceCounterAdd(service.nativeHandle(), counterId, delta);
        RingloomNative.throwForStatus("ringloom_service_counter_add", status);
    }

    public void set(long value) {
        int status = RingloomNative.serviceCounterSet(service.nativeHandle(), counterId, value);
        RingloomNative.throwForStatus("ringloom_service_counter_set", status);
    }
}
