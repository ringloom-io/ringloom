package io.ringloom.service;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Reusable zero-copy send claim returned by {@link RingloomClient#newClaim()}.
 *
 * <p>A claim borrows native ring-buffer memory until {@link #commit()} publishes it or
 * {@link #abort()} releases it. Close aborts an active claim before releasing Java-side state.</p>
 */
public final class BufferClaim implements AutoCloseable {
    private final Arena arena;
    private final MemorySegment nativeStruct;
    private final AtomicBoolean closed;
    private boolean active;
    private long payloadAddress;
    private long payloadLength;

    BufferClaim() {
        this.arena = Arena.ofShared();
        this.nativeStruct = arena.allocate(RingloomNative.BUFFER_CLAIM_SIZE, 8);
        this.closed = new AtomicBoolean(false);
        refreshFromNative();
    }

    MemorySegment nativeStruct() {
        ensureOpen();
        return nativeStruct;
    }

    void refreshFromNative() {
        payloadAddress = nativeStruct.get(RingloomNative.ADDRESS, RingloomNative.BUFFER_CLAIM_PAYLOAD_OFFSET).address();
        payloadLength = nativeStruct.get(ValueLayout.JAVA_LONG, RingloomNative.BUFFER_CLAIM_PAYLOAD_LEN_OFFSET);
        active = nativeStruct.get(ValueLayout.JAVA_BYTE, RingloomNative.BUFFER_CLAIM_ACTIVE_OFFSET) != 0;
    }

    /**
     * Returns the native address of the claimed payload region.
     *
     * @return payload address, or {@code 0} when no claim is active
     */
    public long payloadAddress() {
        ensureOpen();
        return payloadAddress;
    }

    /**
     * Returns the length of the claimed payload region.
     *
     * @return payload length in bytes
     */
    public long payloadLength() {
        ensureOpen();
        return payloadLength;
    }

    /**
     * Returns a writable segment view over the claimed payload bytes.
     *
     * @return borrowed writable payload segment
     */
    public MemorySegment payloadSegment() {
        ensureOpen();
        return MemorySegment.ofAddress(payloadAddress).reinterpret(payloadLength);
    }

    /**
     * Publishes the active claim.
     *
     * @return a {@link RingloomStatus} integer
     */
    public int commit() {
        ensureOpen();
        int status = RingloomNative.claimCommit(nativeStruct);
        refreshFromNative();
        return status;
    }

    /**
     * Aborts the active claim.
     *
     * @return a {@link RingloomStatus} integer
     */
    public int abort() {
        ensureOpen();
        int status = RingloomNative.claimAbort(nativeStruct);
        refreshFromNative();
        return status;
    }

    /**
     * Returns whether this object currently holds an active native claim.
     *
     * @return {@code true} when commit or abort is required
     */
    public boolean active() {
        ensureOpen();
        return active;
    }

    /**
     * Aborts any active claim and releases Java-side native memory. This method is idempotent.
     */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        if (active) {
            RingloomNative.claimAbort(nativeStruct);
            refreshFromNative();
        }
        arena.close();
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException("BufferClaim is closed");
        }
    }
}
