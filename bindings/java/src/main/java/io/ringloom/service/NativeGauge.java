// SPDX-License-Identifier: Apache-2.0
package io.ringloom.service;

import java.util.Objects;

public final class NativeGauge {
    private final RingloomService service;
    private final int gaugeId;

    NativeGauge(RingloomService service, int gaugeId) {
        this.service = Objects.requireNonNull(service, "service");
        this.gaugeId = gaugeId;
    }

    public int gaugeId() {
        service.ensureOpen();
        return gaugeId;
    }

    public void set(long value) {
        int status = RingloomNative.serviceGaugeSet(service.nativeHandle(), gaugeId, value);
        RingloomNative.throwForStatus("ringloom_service_gauge_set", status);
    }
}
