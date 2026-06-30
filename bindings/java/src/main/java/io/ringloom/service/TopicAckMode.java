// SPDX-License-Identifier: Apache-2.0
package io.ringloom.service;

/**
 * Per-publish acknowledgement mode for topic messages.
 *
 * <p>Corresponds to the {@code ack_mode} parameter of {@code ringloom_publish_to_topic}.</p>
 */
public enum TopicAckMode {
    /** No ack tracking; the message is sent best-effort. */
    FIRE_AND_FORGET(0),
    /** Ack completes once at least one replica has applied the message. */
    REPLICATE_ONCE(1);

    private final int nativeValue;

    TopicAckMode(int nativeValue) {
        this.nativeValue = nativeValue;
    }

    /** Returns the native {@code ack_mode} value. */
    public int nativeValue() {
        return nativeValue;
    }
}
