// SPDX-License-Identifier: Apache-2.0
package io.ringloom.service;

import java.lang.foreign.MemorySegment;

/**
 * Reusable holder for a borrowed topic poll payload.
 *
 * <p>The payload segment returned by {@link #payloadSegment()} is valid only until the
 * next {@link TopicSubscription#poll(TopicPollResult)} call on the owning subscription.
 * The binding does not copy the payload; it provides a zero-copy view into native memory.</p>
 *
 * <p>Callers must reuse the same {@code TopicPollResult} across poll calls to satisfy the
 * hot-path zero-allocation guarantee. Creating a new object per poll is safe but allocates.</p>
 */
public final class TopicPollResult {
    private long payloadAddress;
    private long payloadLength;
    private long index;

    /** Creates a new poll result holder ready for reuse. */
    public TopicPollResult() {
    }

    /**
     * Refreshes this holder with borrowed pointers from the native poll.
     *
     * <p>Called only by {@link TopicSubscription#poll(TopicPollResult)}.</p>
     */
    void refreshFromNative(MemorySegment outPayload, long outLen, long outIndex) {
        this.payloadAddress = outPayload == null || outPayload.address() == 0 ? 0 : outPayload.address();
        this.payloadLength = outLen;
        this.index = outIndex;
    }

    /**
     * Returns a borrowed view of the polled payload.
     *
     * <p>The returned segment is valid only until the next {@code poll} on the owning
     * subscription. Copy the data if it must survive beyond the next poll.</p>
     *
     * @return borrowed payload segment, or a zero-length segment when no payload was polled
     */
    public MemorySegment payloadSegment() {
        if (payloadAddress == 0 || payloadLength == 0) {
            return MemorySegment.NULL;
        }
        return MemorySegment.ofAddress(payloadAddress).reinterpret(payloadLength);
    }

    /**
     * Returns the ringloom-queue index of the last polled message.
     *
     * <p>Meaningful only when a poll returned {@link RingloomStatus#OK}.</p>
     *
     * @return message index
     */
    public long index() {
        return index;
    }
}
