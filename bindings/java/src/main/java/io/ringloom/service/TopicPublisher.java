// SPDX-License-Identifier: Apache-2.0
package io.ringloom.service;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Handle-backed publisher for a persistent topic.
 *
 * <p>Created via {@link RingloomClient#registerTopicPublication(String, TopicConfig)}.
 * Hot-path publish methods return native status ints without allocating. The ergonomic
 * {@link #publishOrThrow(byte[])} copies into a confined arena and throws on non-OK status.</p>
 *
 * <p>{@code close()} unregisters the topic publication and is idempotent.</p>
 */
public final class TopicPublisher implements AutoCloseable {

    private final MemorySegment nativeHandle;
    private final AtomicBoolean closed;
    private final String topic;
    private final long[] discardedIndexHolder = new long[1];

    // Preallocated out-parameter segment for zero-allocation publish.
    private final Arena scratchArena;
    private final MemorySegment outIndex;

    TopicPublisher(MemorySegment nativeHandle, String topic) {
        this.nativeHandle = Objects.requireNonNull(
            nativeHandle,
            "nativeHandle"
        );
        this.closed = new AtomicBoolean(false);
        this.topic = Objects.requireNonNull(topic, "topic");
        this.scratchArena = Arena.ofShared();
        this.outIndex = scratchArena.allocate(ValueLayout.JAVA_LONG);
    }

    /**
     * Hot-path fire-and-forget publish. Returns a {@link RingloomStatus} int.
     * No ack tracking; the index assigned by the leader is not surfaced.
     *
     * @param payload borrowed payload segment, or {@code null} for an empty payload
     * @return {@link RingloomStatus#OK} on success, or a non-zero status code
     */
    public int publish(MemorySegment payload) {
        return publish(
            payload,
            TopicAckMode.FIRE_AND_FORGET,
            0,
            discardedIndexHolder
        );
    }

    /**
     * Hot-path publish with explicit ack mode.
     *
     * <p>For {@link TopicAckMode#REPLICATE_ONCE} the assigned publish index is written
     * into {@code outIndexHolder[0]} so the caller can poll {@link #isAcked(long)}.
     * The index is ignored for {@link TopicAckMode#FIRE_AND_FORGET}.
     * This method does not allocate on the hot path.</p>
     *
     * @param payload         borrowed payload segment, or {@code null} for an empty payload
     * @param ackMode         acknowledgement mode
     * @param correlationId   application correlation id, or 0
     * @param outIndexHolder  caller-owned {@code long[1]} reused across calls; must not be null
     * @return {@link RingloomStatus#OK} on success, or a non-zero status code
     */
    public int publish(
        MemorySegment payload,
        TopicAckMode ackMode,
        long correlationId,
        long[] outIndexHolder
    ) {
        ensureOpen();
        Objects.requireNonNull(ackMode, "ackMode");
        Objects.requireNonNull(outIndexHolder, "outIndexHolder");
        MemorySegment segment = payload == null ? MemorySegment.NULL : payload;
        int status = RingloomNative.topicPublish(
            nativeHandle,
            RingloomNative.payloadPointer(segment),
            segment.byteSize(),
            correlationId,
            ackMode.nativeValue(),
            outIndex
        );
        outIndexHolder[0] = outIndex.get(ValueLayout.JAVA_LONG, 0);
        return status;
    }

    /**
     * Ergonomic fire-and-forget copy publish.
     *
     * <p>Copies the payload into a confined arena and throws on non-OK status.
     * This method allocates and is not suitable for hot-path use.</p>
     *
     * @param payload payload bytes; must not be null
     * @return the publish result
     * @throws RingloomException on non-OK status
     */
    public TopicPublishResult publishOrThrow(byte[] payload) {
        Objects.requireNonNull(payload, "payload");
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = arena.allocateFrom(
                ValueLayout.JAVA_BYTE,
                payload
            );
            long[] indexHolder = new long[1];
            int status = publish(
                segment,
                TopicAckMode.FIRE_AND_FORGET,
                0,
                indexHolder
            );
            RingloomNative.throwForStatus("ringloom_publish_to_topic", status);
            return new TopicPublishResult(status, indexHolder[0]);
        }
    }

    /**
     * Non-blocking ack check for {@link TopicAckMode#REPLICATE_ONCE}.
     *
     * <p>Returns {@code true} once the publish index has been applied by at least one
     * replica (or appended on a single-node broker). Never blocks.</p>
     *
     * @param publishIndex the index returned in {@code outIndexHolder[0]} on publish
     * @return {@code true} if acked, {@code false} if still pending
     */
    public boolean isAcked(long publishIndex) {
        ensureOpen();
        return RingloomNative.topicIsAcked(nativeHandle, publishIndex) != 0;
    }

    /** Returns the topic name this publisher was registered for. */
    public String topic() {
        return topic;
    }

    /**
     * Unregisters the topic publication. Idempotent.
     */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        RingloomNative.topicUnregisterPublication(nativeHandle);
        scratchArena.close();
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException("TopicPublisher is closed");
        }
    }
}
