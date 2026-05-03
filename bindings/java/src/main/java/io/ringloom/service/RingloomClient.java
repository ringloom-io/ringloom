package io.ringloom.service;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public final class RingloomClient implements AutoCloseable {
    private static final FunctionDescriptor LIFECYCLE_HANDLER_DESCRIPTOR =
        FunctionDescriptor.ofVoid(RingloomNative.ADDRESS, RingloomNative.ADDRESS);

    private final MemorySegment nativeHandle;
    private final MemorySegment lifecycleUpcallStub;
    private final Arena callbackArena;
    private final AtomicBoolean closed;

    private volatile ServiceLifecycleHandler lifecycleHandler;

    RingloomClient(MemorySegment nativeHandle) {
        this.nativeHandle = nativeHandle;
        this.callbackArena = Arena.ofShared();
        this.closed = new AtomicBoolean(false);

        try {
            MethodHandle dispatch = MethodHandles.lookup()
                .findVirtual(
                    RingloomClient.class,
                    "dispatchLifecycle",
                    MethodType.methodType(void.class, MemorySegment.class, MemorySegment.class)
                )
                .bindTo(this);
            this.lifecycleUpcallStub = RingloomNative.LINKER.upcallStub(
                dispatch,
                LIFECYCLE_HANDLER_DESCRIPTOR,
                callbackArena
            );
        } catch (NoSuchMethodException | IllegalAccessException ex) {
            throw new IllegalStateException("Failed to create RingLoom lifecycle callback", ex);
        }
    }

    public BufferClaim newClaim() {
        ensureOpen();
        return new BufferClaim();
    }

    public int tryClaim(int templateId, long payloadLength, BufferClaim claim) {
        ensureOpen();
        Objects.requireNonNull(claim, "claim");
        int status = RingloomNative.clientTryClaim(nativeHandle, (short) templateId, payloadLength, claim.nativeStruct());
        claim.refreshFromNative();
        return status;
    }

    public List<TargetService> targetServices() {
        ensureOpen();

        long capacity = 0L;
        while (true) {
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment outCount = arena.allocate(ValueLayout.JAVA_LONG);
                MemorySegment nativeTargets = capacity == 0
                    ? MemorySegment.NULL
                    : arena.allocate(RingloomNative.CLIENT_TARGET_SIZE * capacity, 8);

                int status = RingloomNative.clientListTargets(nativeHandle, nativeTargets, capacity, outCount);
                RingloomNative.throwForStatus("ringloom_client_list_targets", status);

                long actualCount = outCount.get(ValueLayout.JAVA_LONG, 0);
                if (actualCount == 0) {
                    return List.of();
                }
                if (actualCount < 0 || actualCount > Integer.MAX_VALUE) {
                    throw new IllegalStateException("target count is out of range: " + actualCount);
                }
                if (actualCount > capacity) {
                    capacity = actualCount;
                    continue;
                }

                ArrayList<TargetService> targets = new ArrayList<>((int) actualCount);
                for (long i = 0; i < actualCount; i++) {
                    long offset = i * RingloomNative.CLIENT_TARGET_SIZE;
                    targets.add(new TargetService(
                        nativeTargets.get(ValueLayout.JAVA_INT, offset + RingloomNative.CLIENT_TARGET_SERVICE_ID_OFFSET),
                        nativeTargets.get(ValueLayout.JAVA_BYTE, offset + RingloomNative.CLIENT_TARGET_IS_LEADER_OFFSET) != 0
                    ));
                }
                return List.copyOf(targets);
            }
        }
    }

    public void onLifecycle(ServiceLifecycleHandler handler) {
        ensureOpen();
        ServiceLifecycleHandler nonNullHandler = Objects.requireNonNull(handler, "handler");
        int status = RingloomNative.clientSetLifecycleHandler(
            nativeHandle,
            lifecycleUpcallStub,
            MemorySegment.NULL
        );
        RingloomNative.throwForStatus("ringloom_client_set_lifecycle_handler", status);
        lifecycleHandler = nonNullHandler;
    }

    public void clearLifecycleHandler() {
        ensureOpen();
        lifecycleHandler = null;
        int status = RingloomNative.clientSetLifecycleHandler(
            nativeHandle,
            MemorySegment.NULL,
            MemorySegment.NULL
        );
        RingloomNative.throwForStatus("ringloom_client_set_lifecycle_handler", status);
    }

    public int send(MemorySegment payload) {
        ensureOpen();
        MemorySegment segment = payload == null ? MemorySegment.NULL : payload;
        return RingloomNative.clientSend(nativeHandle, RingloomNative.payloadPointer(segment), segment.byteSize());
    }

    public int sendTo(int targetServiceId, MemorySegment payload) {
        ensureOpen();
        MemorySegment segment = payload == null ? MemorySegment.NULL : payload;
        return RingloomNative.clientSendTo(nativeHandle, targetServiceId, RingloomNative.payloadPointer(segment), segment.byteSize());
    }

    public int sendToLeader(MemorySegment payload) {
        ensureOpen();
        MemorySegment segment = payload == null ? MemorySegment.NULL : payload;
        return RingloomNative.clientSendToLeader(nativeHandle, RingloomNative.payloadPointer(segment), segment.byteSize());
    }

    public void sendOrThrow(byte[] payload) {
        Objects.requireNonNull(payload, "payload");
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = arena.allocateFrom(ValueLayout.JAVA_BYTE, payload);
            sendOrThrow(segment);
        }
    }

    public void sendOrThrow(MemorySegment payload) {
        int status = send(payload);
        RingloomNative.throwForStatus("ringloom_client_send", status);
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        RingloomNative.clientSetLifecycleHandler(nativeHandle, MemorySegment.NULL, MemorySegment.NULL);
        RingloomNative.destroyClient(nativeHandle);
        callbackArena.close();
    }

    private void dispatchLifecycle(MemorySegment userData, MemorySegment nativeEvent) {
        @SuppressWarnings("unused")
        MemorySegment ignored = userData;
        ServiceLifecycleHandler handler = lifecycleHandler;
        if (handler == null) {
            return;
        }

        MemorySegment event = nativeEvent.reinterpret(RingloomNative.LIFECYCLE_EVENT_SIZE);
        int type = event.get(ValueLayout.JAVA_INT, RingloomNative.LIFECYCLE_EVENT_TYPE_OFFSET);
        int serviceId = event.get(ValueLayout.JAVA_INT, RingloomNative.LIFECYCLE_EVENT_SERVICE_ID_OFFSET);
        short nodeId = event.get(ValueLayout.JAVA_SHORT, RingloomNative.LIFECYCLE_EVENT_NODE_ID_OFFSET);
        boolean leader = event.get(ValueLayout.JAVA_BYTE, RingloomNative.LIFECYCLE_EVENT_IS_LEADER_OFFSET) != 0;
        MemorySegment serviceNameAddress = event.get(
            RingloomNative.ADDRESS,
            RingloomNative.LIFECYCLE_EVENT_SERVICE_NAME_OFFSET
        );
        long serviceNameLength = event.get(ValueLayout.JAVA_LONG, RingloomNative.LIFECYCLE_EVENT_SERVICE_NAME_LEN_OFFSET);
        String serviceName = readUtf8(serviceNameAddress, serviceNameLength);

        handler.onServiceLifecycle(new ServiceLifecycleEvent(
            ServiceLifecycleEventType.fromNative(type),
            serviceName,
            serviceId,
            nodeId,
            leader
        ));
    }

    private static String readUtf8(MemorySegment address, long length) {
        if (length == 0 || address.address() == 0) {
            return "";
        }
        if (length > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("service name length is out of range: " + length);
        }

        MemorySegment bytes = address.reinterpret(length);
        byte[] copy = new byte[(int) length];
        MemorySegment.copy(bytes, ValueLayout.JAVA_BYTE, 0, copy, 0, copy.length);
        return new String(copy, StandardCharsets.UTF_8);
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException("RingloomClient is closed");
        }
    }
}
