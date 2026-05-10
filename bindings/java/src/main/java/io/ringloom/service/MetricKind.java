// SPDX-License-Identifier: Apache-2.0
package io.ringloom.service;

public enum MetricKind {
    COUNTER,
    GAUGE;

    static MetricKind fromNative(int nativeKind) {
        return switch (nativeKind) {
            case 1 -> COUNTER;
            case 2 -> GAUGE;
            default -> throw new IllegalArgumentException("unknown native metric kind: " + nativeKind);
        };
    }
}
