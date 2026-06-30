// SPDX-License-Identifier: Apache-2.0
package io.ringloom.service;

/**
 * Result of a topic publish operation, carrying the native status and assigned index.
 *
 * <p>The {@code publishIndex} is meaningful only for {@link TopicAckMode#REPLICATE_ONCE}.</p>
 */
public record TopicPublishResult(int status, long publishIndex) {
    /** Returns {@code true} when the native status is {@link RingloomStatus#OK}. */
    public boolean ok() {
        return status == RingloomStatus.OK;
    }
}
