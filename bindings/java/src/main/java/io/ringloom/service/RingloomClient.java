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

/**
 * Client-side proxy for sending messages to instances of one named RingLoom service.
 *
 * <p>The client keeps a cached, immutable target list updated by native service lifecycle
 * callbacks. Applications drive those callbacks by polling the owning {@link RingloomService}
 * control plane.</p>
 */
public final class RingloomClient implements AutoCloseable {
    private static final FunctionDescriptor LIFECYCLE_HANDLER_DESCRIPTOR =
        FunctionDescriptor.ofVoid(RingloomNative.ADDRESS, RingloomNative.ADDRESS);

    private final MemorySegment nativeHandle;
    private final MemorySegment lifecycleUpcallStub;
    private final Arena callbackArena;
    private final AtomicBoolean closed;

    private volatile List<TargetService> targetServices;
    private volatile ServiceLifecycleHandler lifecycleHandler;

    RingloomClient(MemorySegment nativeHandle) {
        this.nativeHandle = nativeHandle;
        this.callbackArena = Arena.ofShared();
        this.closed = new AtomicBoolean(false);
        this.targetServices = List.of();

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
            callbackArena.close();
            RingloomNative.destroyClient(nativeHandle);
            throw new IllegalStateException("Failed to create RingLoom lifecycle callback", ex);
        }

        int status = RingloomNative.clientSetLifecycleHandler(
            nativeHandle,
            lifecycleUpcallStub,
            MemorySegment.NULL
        );
        try {
            RingloomNative.throwForStatus("ringloom_client_set_lifecycle_handler", status);
            targetServices = loadTargetServicesSnapshot();
        } catch (RuntimeException | Error ex) {
            RingloomNative.clientSetLifecycleHandler(nativeHandle, MemorySegment.NULL, MemorySegment.NULL);
            RingloomNative.destroyClient(nativeHandle);
            callbackArena.close();
            throw ex;
        }
    }

    /**
     * Creates a reusable zero-copy send claim holder.
     *
     * @return a claim object that can be reused with {@link #tryClaim(int, long, BufferClaim)}
     */
    public BufferClaim newClaim() {
        ensureOpen();
        return new BufferClaim();
    }

    /**
     * Attempts to claim writable memory for a load-balanced send.
     *
     * @param templateId application template id to write into the message header
     * @param payloadLength number of payload bytes the caller will write
     * @param claim caller-owned reusable claim object to populate
     * @return a {@link RingloomStatus} integer
     */
    public int tryClaim(int templateId, long payloadLength, BufferClaim claim) {
        ensureOpen();
        Objects.requireNonNull(claim, "claim");
        int status = RingloomNative.clientTryClaim(nativeHandle, narrowTemplateId(templateId), payloadLength, claim.nativeStruct());
        claim.refreshFromNative();
        return status;
    }

    public int tryClaimRequest(int templateId, long correlationId, long payloadLength, BufferClaim claim) {
        ensureOpen();
        Objects.requireNonNull(claim, "claim");
        int status = RingloomNative.clientTryClaimRequest(
            nativeHandle,
            narrowTemplateId(templateId),
            correlationId,
            payloadLength,
            claim.nativeStruct()
        );
        claim.refreshFromNative();
        return status;
    }

    public int tryClaimTo(
        short targetNodeId,
        int targetServiceId,
        int templateId,
        long payloadLength,
        BufferClaim claim
    ) {
        ensureOpen();
        Objects.requireNonNull(claim, "claim");
        int status = RingloomNative.clientTryClaimTo(
            nativeHandle,
            targetNodeId,
            targetServiceId,
            narrowTemplateId(templateId),
            payloadLength,
            claim.nativeStruct()
        );
        claim.refreshFromNative();
        return status;
    }

    public int tryClaimToRequest(
        short targetNodeId,
        int targetServiceId,
        int templateId,
        long correlationId,
        long payloadLength,
        BufferClaim claim
    ) {
        ensureOpen();
        Objects.requireNonNull(claim, "claim");
        int status = RingloomNative.clientTryClaimToRequest(
            nativeHandle,
            targetNodeId,
            targetServiceId,
            narrowTemplateId(templateId),
            correlationId,
            payloadLength,
            claim.nativeStruct()
        );
        claim.refreshFromNative();
        return status;
    }

    public int tryClaimToLeader(int templateId, long payloadLength, BufferClaim claim) {
        ensureOpen();
        Objects.requireNonNull(claim, "claim");
        int status = RingloomNative.clientTryClaimToLeader(
            nativeHandle,
            narrowTemplateId(templateId),
            payloadLength,
            claim.nativeStruct()
        );
        claim.refreshFromNative();
        return status;
    }

    public int tryClaimToLeaderRequest(int templateId, long correlationId, long payloadLength, BufferClaim claim) {
        ensureOpen();
        Objects.requireNonNull(claim, "claim");
        int status = RingloomNative.clientTryClaimToLeaderRequest(
            nativeHandle,
            narrowTemplateId(templateId),
            correlationId,
            payloadLength,
            claim.nativeStruct()
        );
        claim.refreshFromNative();
        return status;
    }

    /**
     * Returns the current cached target instances for this client.
     *
     * <p>The returned list is immutable and reused until a service lifecycle callback changes the
     * target set. This method does not poll the control plane; call
     * {@link RingloomService#pollControl(int)} from an application thread to keep discovery fresh.</p>
     *
     * @return immutable cached target list
     */
    public List<TargetService> targetServices() {
        ensureOpen();
        return targetServices;
    }

    public AeronPublicationStatus lastAeronSendStatus() {
        ensureOpen();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outStatus = arena.allocate(ValueLayout.JAVA_INT);
            int status = RingloomNative.clientLastAeronSendStatus(nativeHandle, outStatus);
            RingloomNative.throwForStatus("ringloom_client_last_aeron_send_status", status);
            return AeronPublicationStatus.fromNative(outStatus.get(ValueLayout.JAVA_INT, 0));
        }
    }

    /**
     * Registers a user lifecycle handler.
     *
     * <p>The client always keeps its internal lifecycle subscription active for target cache
     * maintenance. This handler is invoked after the internal cache has been updated.</p>
     *
     * @param handler callback invoked synchronously on the thread polling the control plane
     */
    public void onLifecycle(ServiceLifecycleHandler handler) {
        ensureOpen();
        lifecycleHandler = Objects.requireNonNull(handler, "handler");
    }

    /**
     * Clears the user lifecycle handler without disabling the client's internal target cache
     * subscription.
     */
    public void clearLifecycleHandler() {
        ensureOpen();
        lifecycleHandler = null;
    }

    /**
     * Sends a payload to one discovered instance selected by the native load balancer.
     *
     * @param payload borrowed payload segment, or {@code null} for an empty payload
     * @return a {@link RingloomStatus} integer
     */
    public int send(MemorySegment payload) {
        ensureOpen();
        MemorySegment segment = payload == null ? MemorySegment.NULL : payload;
        return RingloomNative.clientSend(nativeHandle, RingloomNative.payloadPointer(segment), segment.byteSize());
    }

    public int sendMessage(int templateId, MemorySegment payload) {
        ensureOpen();
        MemorySegment segment = payload == null ? MemorySegment.NULL : payload;
        return RingloomNative.clientSendMessage(
            nativeHandle,
            narrowTemplateId(templateId),
            RingloomNative.payloadPointer(segment),
            segment.byteSize()
        );
    }

    public int sendMessageRequest(int templateId, long correlationId, MemorySegment payload) {
        ensureOpen();
        MemorySegment segment = payload == null ? MemorySegment.NULL : payload;
        return RingloomNative.clientSendMessageRequest(
            nativeHandle,
            narrowTemplateId(templateId),
            correlationId,
            RingloomNative.payloadPointer(segment),
            segment.byteSize()
        );
    }

    /**
     * Sends a payload to a specific target instance.
     *
     * @param targetNodeId node id from {@link TargetService#targetNodeId()}
     * @param targetServiceId service id from {@link TargetService#targetServiceId()}
     * @param payload borrowed payload segment, or {@code null} for an empty payload
     * @return a {@link RingloomStatus} integer
     */
    public int sendTo(short targetNodeId, int targetServiceId, MemorySegment payload) {
        ensureOpen();
        MemorySegment segment = payload == null ? MemorySegment.NULL : payload;
        return RingloomNative.clientSendTo(
            nativeHandle,
            targetNodeId,
            targetServiceId,
            RingloomNative.payloadPointer(segment),
            segment.byteSize()
        );
    }

    public int sendToMessage(short targetNodeId, int targetServiceId, int templateId, MemorySegment payload) {
        ensureOpen();
        MemorySegment segment = payload == null ? MemorySegment.NULL : payload;
        return RingloomNative.clientSendToMessage(
            nativeHandle,
            targetNodeId,
            targetServiceId,
            narrowTemplateId(templateId),
            RingloomNative.payloadPointer(segment),
            segment.byteSize()
        );
    }

    public int sendToMessageRequest(
        short targetNodeId,
        int targetServiceId,
        int templateId,
        long correlationId,
        MemorySegment payload
    ) {
        ensureOpen();
        MemorySegment segment = payload == null ? MemorySegment.NULL : payload;
        return RingloomNative.clientSendToMessageRequest(
            nativeHandle,
            targetNodeId,
            targetServiceId,
            narrowTemplateId(templateId),
            correlationId,
            RingloomNative.payloadPointer(segment),
            segment.byteSize()
        );
    }

    /**
     * Sends a payload to the currently discovered leader instance.
     *
     * @param payload borrowed payload segment, or {@code null} for an empty payload
     * @return a {@link RingloomStatus} integer
     */
    public int sendToLeader(MemorySegment payload) {
        ensureOpen();
        MemorySegment segment = payload == null ? MemorySegment.NULL : payload;
        return RingloomNative.clientSendToLeader(nativeHandle, RingloomNative.payloadPointer(segment), segment.byteSize());
    }

    public int sendToLeaderMessage(int templateId, MemorySegment payload) {
        ensureOpen();
        MemorySegment segment = payload == null ? MemorySegment.NULL : payload;
        return RingloomNative.clientSendToLeaderMessage(
            nativeHandle,
            narrowTemplateId(templateId),
            RingloomNative.payloadPointer(segment),
            segment.byteSize()
        );
    }

    public int sendToLeaderMessageRequest(int templateId, long correlationId, MemorySegment payload) {
        ensureOpen();
        MemorySegment segment = payload == null ? MemorySegment.NULL : payload;
        return RingloomNative.clientSendToLeaderMessageRequest(
            nativeHandle,
            narrowTemplateId(templateId),
            correlationId,
            RingloomNative.payloadPointer(segment),
            segment.byteSize()
        );
    }

    /**
     * Convenience wrapper that copies a byte array into native memory and throws on failure.
     *
     * @param payload payload bytes to send
     */
    public void sendOrThrow(byte[] payload) {
        Objects.requireNonNull(payload, "payload");
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = arena.allocateFrom(ValueLayout.JAVA_BYTE, payload);
            sendOrThrow(segment);
        }
    }

    /**
     * Convenience wrapper around {@link #send(MemorySegment)} that throws on non-OK status.
     *
     * @param payload borrowed payload segment, or {@code null} for an empty payload
     */
    public void sendOrThrow(MemorySegment payload) {
        int status = send(payload);
        RingloomNative.throwForStatus("ringloom_client_send", status);
    }

    public void sendMessageOrThrow(int templateId, MemorySegment payload) {
        int status = sendMessage(templateId, payload);
        RingloomNative.throwForStatus("ringloom_client_send_message", status);
    }

    public void sendMessageOrThrow(int templateId, byte[] payload) {
        Objects.requireNonNull(payload, "payload");
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = arena.allocateFrom(ValueLayout.JAVA_BYTE, payload);
            sendMessageOrThrow(templateId, segment);
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        lifecycleHandler = null;
        targetServices = List.of();
        RingloomNative.clientSetLifecycleHandler(nativeHandle, MemorySegment.NULL, MemorySegment.NULL);
        RingloomNative.destroyClient(nativeHandle);
        callbackArena.close();
    }

    private List<TargetService> loadTargetServicesSnapshot() {
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
                        nativeTargets.get(ValueLayout.JAVA_SHORT, offset + RingloomNative.CLIENT_TARGET_NODE_ID_OFFSET),
                        nativeTargets.get(ValueLayout.JAVA_BYTE, offset + RingloomNative.CLIENT_TARGET_IS_LEADER_OFFSET) != 0
                    ));
                }
                return List.copyOf(targets);
            }
        }
    }

    private void dispatchLifecycle(MemorySegment userData, MemorySegment nativeEvent) {
        @SuppressWarnings("unused")
        MemorySegment ignored = userData;

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

        ServiceLifecycleEvent lifecycleEvent = new ServiceLifecycleEvent(
            ServiceLifecycleEventType.fromNative(type),
            serviceName,
            serviceId,
            nodeId,
            leader
        );
        updateTargetServices(lifecycleEvent);

        ServiceLifecycleHandler handler = lifecycleHandler;
        if (handler != null) {
            handler.onServiceLifecycle(lifecycleEvent);
        }
    }

    private synchronized void updateTargetServices(ServiceLifecycleEvent event) {
        TargetService target = new TargetService(event.serviceId(), event.nodeId(), event.leader());
        ArrayList<TargetService> updated = new ArrayList<>(targetServices);
        int index = indexOfTarget(updated, target.targetNodeId(), target.targetServiceId());

        if (event.type() == ServiceLifecycleEventType.AVAILABLE) {
            if (index >= 0) {
                if (updated.get(index).equals(target)) {
                    return;
                }
                updated.set(index, target);
            } else {
                updated.add(target);
            }
        } else if (index >= 0) {
            updated.remove(index);
        } else {
            return;
        }

        targetServices = List.copyOf(updated);
    }

    private static int indexOfTarget(List<TargetService> targets, short targetNodeId, int targetServiceId) {
        for (int i = 0; i < targets.size(); i++) {
            TargetService target = targets.get(i);
            if (target.targetNodeId() == targetNodeId && target.targetServiceId() == targetServiceId) {
                return i;
            }
        }
        return -1;
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

    private static short narrowTemplateId(int templateId) {
        if (templateId < 0 || templateId > 0xFFFF) {
            throw new IllegalArgumentException("templateId must be in range 0..65535");
        }
        return (short) templateId;
    }
}
