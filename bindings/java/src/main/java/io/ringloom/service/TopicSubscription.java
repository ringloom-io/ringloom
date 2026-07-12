// SPDX-License-Identifier: Apache-2.0
package io.ringloom.service;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Handle-backed subscription to a persistent topic.
 *
 * <p>Created via {@link RingloomClient#subscribeTopic(String, TopicStart)}.
 * Callers poll for messages with {@link #poll(TopicPollResult)} which returns borrowed
 * payload views (zero-copy). The payload is valid only until the next poll on this
 * subscription.</p>
 *
 * <p>{@link #maintenancePoll(int)} drives ringloom-queue maintenance work and is
 * safe to call from any thread that does not poll the same subscription concurrently.</p>
 *
 * <p>{@code close()} unsubscribes and is idempotent.</p>
 */
public final class TopicSubscription implements AutoCloseable {

    private final MemorySegment nativeHandle;
    private final AtomicBoolean closed;
    private final String topic;
    private final long topicId;

    // Preallocated out-parameter segments for zero-allocation poll.
    // Each is a single-element buffer the native side writes into.
    private final Arena scratchArena;
    private final MemorySegment outPayload;
    private final MemorySegment outLen;
    private final MemorySegment outIndex;

    TopicSubscription(MemorySegment nativeHandle, String topic) {
        this.nativeHandle = Objects.requireNonNull(
            nativeHandle,
            "nativeHandle"
        );
        this.closed = new AtomicBoolean(false);
        this.topic = Objects.requireNonNull(topic, "topic");
        this.scratchArena = Arena.ofShared();
        this.outPayload = scratchArena.allocate(RingloomNative.ADDRESS);
        this.outLen = scratchArena.allocate(ValueLayout.JAVA_LONG);
        this.outIndex = scratchArena.allocate(ValueLayout.JAVA_LONG);
        // Resolve the broker-assigned topic id once at construction. On older
        // native builds without the accessor symbol this is 0 ("unavailable").
        this.topicId = RingloomNative.topicSubscriptionId(nativeHandle);
    }

    /**
     * The broker-assigned topic id for this subscription.
     *
     * <p>Resolved once at construction; stable for the lifetime of the handle.
     * Returns {@code 0} on older native builds that lack the accessor.</p>
     *
     * @return the topic id, or {@code 0} if unavailable
     */
    public long topicId() {
        return topicId;
    }

    /**
     * Polls one message from the subscription.
     *
     * <p>On success writes the borrowed payload address/length into {@code out}
     * and returns {@link RingloomStatus#OK}. When no message is available returns
     * {@link RingloomStatus#NOT_READY}. The payload is valid only until the next
     * {@link #poll(TopicPollResult)} on this subscription.</p>
     *
     * <p>This method is zero-allocation on the hot path: the out-parameter segments
     * are allocated once at construction time and reused across calls.</p>
     *
     * @param out reusable result holder; must not be null
     * @return {@link RingloomStatus#OK} when a message was polled,
     *         {@link RingloomStatus#NOT_READY} when no message is available,
     *         or another non-zero status code on error
     */
    public int poll(TopicPollResult out) {
        ensureOpen();
        Objects.requireNonNull(out, "out");
        int status = RingloomNative.topicPoll(
            nativeHandle,
            outPayload,
            outLen,
            outIndex
        );
        if (status == RingloomStatus.OK) {
            out.refreshFromNative(
                outPayload.get(RingloomNative.ADDRESS, 0),
                outLen.get(ValueLayout.JAVA_LONG, 0),
                outIndex.get(ValueLayout.JAVA_LONG, 0)
            );
        }
        return status;
    }

    /**
     * Drives ringloom-queue maintenance/cleaner work and read-page pre-touch for
     * this subscription's tailer.
     *
     * <p>Safe to call from any thread that does not poll the same subscription
     * concurrently. Never advances the read cursor the poll path consumes.</p>
     *
     * @param maxWorkUnits maximum work units to process; non-positive is a no-op
     * @return {@link RingloomStatus#OK}, or a non-zero status code on error
     */
    public int maintenancePoll(int maxWorkUnits) {
        ensureOpen();
        if (maxWorkUnits < 0) {
            throw new IllegalArgumentException(
                "maxWorkUnits must be non-negative"
            );
        }
        return RingloomNative.topicSubscriptionMaintenancePoll(
            nativeHandle,
            maxWorkUnits
        );
    }

    /** Returns the topic name this subscription is bound to. */
    public String topic() {
        return topic;
    }

    /** Returns {@code true} when this subscription has been closed. */
    public boolean closed() {
        return closed.get();
    }

    /**
     * Unsubscribes from the topic. Idempotent.
     */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        RingloomNative.topicUnsubscribe(nativeHandle);
        scratchArena.close();
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException("TopicSubscription is closed");
        }
    }
}
