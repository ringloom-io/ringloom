// SPDX-License-Identifier: Apache-2.0
package io.ringloom.service;

/**
 * Starting position for a topic subscription, mirroring {@code ringloom_topic_start_t}.
 */
public enum TopicStart {
    /** Read from the earliest retained message. */
    EARLIEST(0),
    /** Read only messages published after subscribing. */
    LATEST(1);

    private final int nativeValue;

    TopicStart(int nativeValue) {
        this.nativeValue = nativeValue;
    }

    /** Returns the native {@code ringloom_topic_start_t} constant. */
    public int nativeValue() {
        return nativeValue;
    }
}
