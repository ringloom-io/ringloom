package io.ringloom.service;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.concurrent.atomic.AtomicBoolean;

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

    public long payloadAddress() {
        ensureOpen();
        return payloadAddress;
    }

    public long payloadLength() {
        ensureOpen();
        return payloadLength;
    }

    public MemorySegment payloadSegment() {
        ensureOpen();
        return MemorySegment.ofAddress(payloadAddress).reinterpret(payloadLength);
    }

    public int commit() {
        ensureOpen();
        int status = RingloomNative.claimCommit(nativeStruct);
        refreshFromNative();
        return status;
    }

    public int abort() {
        ensureOpen();
        int status = RingloomNative.claimAbort(nativeStruct);
        refreshFromNative();
        return status;
    }

    public boolean active() {
        ensureOpen();
        return active;
    }

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
