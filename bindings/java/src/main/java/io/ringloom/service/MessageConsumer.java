package io.ringloom.service;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Polling consumer for application messages delivered to a {@link RingloomService}.
 *
 * <p>The consumer reuses one {@link RingloomMessage} view for callback dispatch. Handlers must
 * copy payload bytes if they need to retain them after the callback returns.</p>
 */
public final class MessageConsumer implements AutoCloseable {
    private static final FunctionDescriptor MESSAGE_HANDLER_DESCRIPTOR =
        FunctionDescriptor.ofVoid(RingloomNative.ADDRESS, RingloomNative.ADDRESS);

    private final MemorySegment nativeHandle;
    private final MemorySegment pollUpcallStub;
    private final Arena callbackArena;
    private final Arena stateArena;
    private final MemorySegment outCount;
    private final RingloomMessage messageView;
    private final AtomicBoolean closed;

    private volatile MessageHandler currentHandler;
    private volatile int lastStatus;

    MessageConsumer(MemorySegment nativeHandle) {
        this.nativeHandle = nativeHandle;
        this.callbackArena = Arena.ofShared();
        this.stateArena = Arena.ofShared();
        this.outCount = stateArena.allocate(ValueLayout.JAVA_INT);
        this.messageView = new RingloomMessage();
        this.closed = new AtomicBoolean(false);

        try {
            MethodHandle dispatch = MethodHandles.lookup()
                .findVirtual(
                    MessageConsumer.class,
                    "dispatchMessage",
                    MethodType.methodType(void.class, MemorySegment.class, MemorySegment.class)
                )
                .bindTo(this);
            this.pollUpcallStub = RingloomNative.LINKER.upcallStub(dispatch, MESSAGE_HANDLER_DESCRIPTOR, callbackArena);
        } catch (NoSuchMethodException | IllegalAccessException ex) {
            throw new IllegalStateException("Failed to create RingLoom message consumer callback", ex);
        }
    }

    /**
     * Polls application messages and dispatches each message to the handler.
     *
     * @param handler callback receiving a borrowed reusable message view
     * @param limit maximum number of messages to process
     * @return number of processed messages, or {@code -1} when {@link #lastStatus()} is non-OK
     */
    public int poll(MessageHandler handler, int limit) {
        ensureOpen();
        Objects.requireNonNull(handler, "handler");
        currentHandler = handler;
        outCount.set(ValueLayout.JAVA_INT, 0, 0);

        int status = RingloomNative.pollMessageConsumer(nativeHandle, pollUpcallStub, MemorySegment.NULL, limit, outCount);
        if (!RingloomStatus.isOk(status)) {
            lastStatus = status;
            return -1;
        }

        lastStatus = RingloomStatus.OK;
        return outCount.get(ValueLayout.JAVA_INT, 0);
    }

    /**
     * Returns the status from the most recent {@link #poll(MessageHandler, int)} call.
     *
     * @return a {@link RingloomStatus} integer
     */
    public int lastStatus() {
        return lastStatus;
    }

    /**
     * Destroys the native consumer handle. This method is idempotent.
     */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        RingloomNative.destroyMessageConsumer(nativeHandle);
        callbackArena.close();
        stateArena.close();
    }

    private void dispatchMessage(MemorySegment userData, MemorySegment message) {
        @SuppressWarnings("unused")
        MemorySegment ignored = userData;
        MessageHandler handler = currentHandler;
        if (handler == null) {
            return;
        }

        messageView.updateFromNative(message);
        handler.onMessage(messageView);
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException("MessageConsumer is closed");
        }
    }
}
