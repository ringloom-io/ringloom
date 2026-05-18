// SPDX-License-Identifier: Apache-2.0
package io.ringloom.service;

/**
 * Last observed direct Aeron publication state for a RingLoom client.
 */
public enum AeronPublicationStatus {
    UNKNOWN(0),
    CLAIMED(1),
    NOT_CONNECTED(2),
    BACK_PRESSURED(3),
    ADMIN_ACTION(4),
    CLOSED(5),
    MAX_POSITION_EXCEEDED(6),
    FAILED(7);

    private final int nativeCode;

    AeronPublicationStatus(int nativeCode) {
        this.nativeCode = nativeCode;
    }

    public int nativeCode() {
        return nativeCode;
    }

    public static AeronPublicationStatus fromNative(int nativeCode) {
        for (AeronPublicationStatus status : values()) {
            if (status.nativeCode == nativeCode) {
                return status;
            }
        }
        return UNKNOWN;
    }
}
