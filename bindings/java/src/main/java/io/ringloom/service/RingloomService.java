package io.ringloom.service;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public final class RingloomService implements AutoCloseable {
    private final MemorySegment nativeHandle;
    private final AtomicBoolean closed;

    private RingloomService(MemorySegment nativeHandle) {
        this.nativeHandle = nativeHandle;
        this.closed = new AtomicBoolean(false);
    }

    public static RingloomService start(ServiceConfig config) {
        Objects.requireNonNull(config, "config");

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment nativeConfig = arena.allocate(RingloomNative.SERVICE_CONFIG_SIZE, 8);
            MemorySegment storagePath = arena.allocateFrom(config.storagePath());
            MemorySegment group = arena.allocateFrom(config.group());
            MemorySegment serviceName = arena.allocateFrom(config.serviceName());
            MemorySegment outService = arena.allocate(RingloomNative.ADDRESS);
            outService.set(RingloomNative.ADDRESS, 0, MemorySegment.NULL);

            nativeConfig.set(RingloomNative.ADDRESS, RingloomNative.CONFIG_STORAGE_PATH_OFFSET, storagePath);
            nativeConfig.set(ValueLayout.JAVA_LONG, RingloomNative.CONFIG_STORAGE_PATH_LEN_OFFSET, config.storagePath().getBytes().length);
            nativeConfig.set(RingloomNative.ADDRESS, RingloomNative.CONFIG_GROUP_OFFSET, group);
            nativeConfig.set(ValueLayout.JAVA_LONG, RingloomNative.CONFIG_GROUP_LEN_OFFSET, config.group().getBytes().length);
            nativeConfig.set(RingloomNative.ADDRESS, RingloomNative.CONFIG_SERVICE_NAME_OFFSET, serviceName);
            nativeConfig.set(ValueLayout.JAVA_LONG, RingloomNative.CONFIG_SERVICE_NAME_LEN_OFFSET, config.serviceName().getBytes().length);
            nativeConfig.set(ValueLayout.JAVA_SHORT, RingloomNative.CONFIG_BROKER_NODE_ID_OFFSET, config.brokerNodeId());
            nativeConfig.set(ValueLayout.JAVA_BOOLEAN, RingloomNative.CONFIG_BLOCKING_MODE_OFFSET, config.blockingMode());
            nativeConfig.set(ValueLayout.JAVA_INT, RingloomNative.CONFIG_HEARTBEAT_TIMEOUT_OFFSET, config.heartbeatTimeoutMillis());
            nativeConfig.set(ValueLayout.JAVA_LONG, RingloomNative.CONFIG_CONTROL_BUFFER_LENGTH_OFFSET, config.controlBufferLength());
            nativeConfig.set(ValueLayout.JAVA_LONG, RingloomNative.CONFIG_MESSAGES_BUFFER_LENGTH_OFFSET, config.messagesBufferLength());
            nativeConfig.set(ValueLayout.JAVA_BOOLEAN, RingloomNative.CONFIG_LEADER_ELECTION_OFFSET, config.leaderElectionEnabled());

            int status = RingloomNative.serviceStart(nativeConfig, outService);
            RingloomNative.throwForStatus("ringloom_service_start", status);

            return new RingloomService(outService.get(RingloomNative.ADDRESS, 0));
        }
    }

    public int serviceId() {
        ensureOpen();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outValue = arena.allocate(ValueLayout.JAVA_INT);
            int status = RingloomNative.serviceId(nativeHandle, outValue);
            RingloomNative.throwForStatus("ringloom_service_id", status);
            return outValue.get(ValueLayout.JAVA_INT, 0);
        }
    }

    public short nodeId() {
        ensureOpen();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outValue = arena.allocate(ValueLayout.JAVA_SHORT);
            int status = RingloomNative.serviceNodeId(nativeHandle, outValue);
            RingloomNative.throwForStatus("ringloom_service_node_id", status);
            return outValue.get(ValueLayout.JAVA_SHORT, 0);
        }
    }

    public RingloomClient createClient(String targetServiceName) {
        ensureOpen();
        Objects.requireNonNull(targetServiceName, "targetServiceName");
        if (targetServiceName.isEmpty()) {
            throw new IllegalArgumentException("targetServiceName must not be empty");
        }

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment nativeName = arena.allocateFrom(targetServiceName);
            MemorySegment outClient = arena.allocate(RingloomNative.ADDRESS);
            outClient.set(RingloomNative.ADDRESS, 0, MemorySegment.NULL);

            int status = RingloomNative.createClient(nativeHandle, nativeName, targetServiceName.getBytes().length, outClient);
            RingloomNative.throwForStatus("ringloom_service_create_client", status);
            return new RingloomClient(outClient.get(RingloomNative.ADDRESS, 0));
        }
    }

    public int pollControl(int limit) {
        ensureOpen();
        if (limit < 0) {
            throw new IllegalArgumentException("limit must be non-negative");
        }

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outCount = arena.allocate(ValueLayout.JAVA_INT);
            int status = RingloomNative.servicePollControl(nativeHandle, limit, outCount);
            RingloomNative.throwForStatus("ringloom_service_poll_control", status);
            return outCount.get(ValueLayout.JAVA_INT, 0);
        }
    }

    public int pollControl() {
        return pollControl(256);
    }

    public MessageConsumer messageConsumer() {
        ensureOpen();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outConsumer = arena.allocate(RingloomNative.ADDRESS);
            outConsumer.set(RingloomNative.ADDRESS, 0, MemorySegment.NULL);

            int status = RingloomNative.createMessageConsumer(nativeHandle, outConsumer);
            RingloomNative.throwForStatus("ringloom_service_create_message_consumer", status);
            return new MessageConsumer(outConsumer.get(RingloomNative.ADDRESS, 0));
        }
    }

    public void stop() {
        if (closed.get()) {
            return;
        }
        RingloomNative.serviceStop(nativeHandle);
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        RingloomNative.serviceDestroy(nativeHandle);
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException("RingloomService is closed");
        }
    }
}
