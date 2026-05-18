package io.ringloom.service;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Running RingLoom service connected to a local broker.
 *
 * <p>A service owns control-plane progress for clients created from it. Applications should call
 * {@link #pollControl(int)} regularly to process registration, discovery, lifecycle, and heartbeat
 * work.</p>
 */
public final class RingloomService implements AutoCloseable {
    private static final int MAX_METRIC_NAME_BYTES = 244;

    private final MemorySegment nativeHandle;
    private final AtomicBoolean closed;

    private RingloomService(MemorySegment nativeHandle) {
        this.nativeHandle = nativeHandle;
        this.closed = new AtomicBoolean(false);
    }

    /**
     * Starts and registers a service with the configured broker.
     *
     * @param config service configuration
     * @return started service handle
     */
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

    /**
     * Returns the broker-assigned service id.
     *
     * @return service id
     */
    public int serviceId() {
        ensureOpen();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outValue = arena.allocate(ValueLayout.JAVA_INT);
            int status = RingloomNative.serviceId(nativeHandle, outValue);
            RingloomNative.throwForStatus("ringloom_service_id", status);
            return outValue.get(ValueLayout.JAVA_INT, 0);
        }
    }

    /**
     * Returns the broker node id this service registered with.
     *
     * @return node id
     */
    public short nodeId() {
        ensureOpen();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outValue = arena.allocate(ValueLayout.JAVA_SHORT);
            int status = RingloomNative.serviceNodeId(nativeHandle, outValue);
            RingloomNative.throwForStatus("ringloom_service_node_id", status);
            return outValue.get(ValueLayout.JAVA_SHORT, 0);
        }
    }

    public String aeronDirectory() {
        ensureOpen();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outDirectory = arena.allocate(RingloomNative.ADDRESS);
            MemorySegment outDirectoryLen = arena.allocate(ValueLayout.JAVA_LONG);
            int status = RingloomNative.serviceAeronDirectory(nativeHandle, outDirectory, outDirectoryLen);
            RingloomNative.throwForStatus("ringloom_service_aeron_directory", status);

            MemorySegment directory = outDirectory.get(RingloomNative.ADDRESS, 0);
            long length = outDirectoryLen.get(ValueLayout.JAVA_LONG, 0);
            if (directory.address() == 0 || length == 0) {
                return "";
            }
            if (length > Integer.MAX_VALUE) {
                throw new IllegalStateException("Aeron directory length is out of range: " + length);
            }
            byte[] bytes = new byte[(int) length];
            MemorySegment.copy(directory.reinterpret(length), ValueLayout.JAVA_BYTE, 0, bytes, 0, bytes.length);
            return new String(bytes, StandardCharsets.UTF_8);
        }
    }

    public int aeronInboundStreamId() {
        ensureOpen();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outValue = arena.allocate(ValueLayout.JAVA_INT);
            int status = RingloomNative.serviceAeronInboundStreamId(nativeHandle, outValue);
            RingloomNative.throwForStatus("ringloom_service_aeron_inbound_stream_id", status);
            return outValue.get(ValueLayout.JAVA_INT, 0);
        }
    }

    public boolean publicationConnected() {
        ensureOpen();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outValue = arena.allocate(ValueLayout.JAVA_BOOLEAN);
            int status = RingloomNative.servicePublicationConnected(nativeHandle, outValue);
            RingloomNative.throwForStatus("ringloom_service_publication_connected", status);
            return outValue.get(ValueLayout.JAVA_BOOLEAN, 0);
        }
    }

    /**
     * Creates a client for a logical target service name.
     *
     * @param targetServiceName target service name to discover and send to
     * @return client bound to the target service name
     */
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

    /**
     * Polls control-plane work for this service.
     *
     * <p>This drives service discovery and {@link RingloomClient} lifecycle callbacks. A return
     * value of {@code 0} means no control messages were processed.</p>
     *
     * @param limit maximum number of control messages to process
     * @return number of processed control messages
     */
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

    /**
     * Polls control-plane work using the default limit.
     *
     * @return number of processed control messages
     */
    public int pollControl() {
        return pollControl(256);
    }

    /**
     * Creates a message consumer for application messages delivered to this service.
     *
     * @return message consumer handle
     */
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

    public RingloomMetricsReader metricsReader() {
        ensureOpen();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment outReader = arena.allocate(RingloomNative.ADDRESS);
            outReader.set(RingloomNative.ADDRESS, 0, MemorySegment.NULL);

            int status = RingloomNative.createMetricsReader(nativeHandle, outReader);
            RingloomNative.throwForStatus("ringloom_service_create_metrics_reader", status);
            return new RingloomMetricsReader(outReader.get(RingloomNative.ADDRESS, 0));
        }
    }

    public NativeCounter registerCounter(String name) {
        ensureOpen();
        byte[] nameBytes = metricNameBytes(name);

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment nativeName = arena.allocateFrom(ValueLayout.JAVA_BYTE, nameBytes);
            MemorySegment outCounterId = arena.allocate(ValueLayout.JAVA_INT);
            int status = RingloomNative.serviceCounterRegister(
                nativeHandle,
                nativeName,
                nameBytes.length,
                outCounterId
            );
            RingloomNative.throwForStatus("ringloom_service_counter_register", status);
            return new NativeCounter(this, outCounterId.get(ValueLayout.JAVA_INT, 0));
        }
    }

    public NativeGauge registerGauge(String name) {
        ensureOpen();
        byte[] nameBytes = metricNameBytes(name);

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment nativeName = arena.allocateFrom(ValueLayout.JAVA_BYTE, nameBytes);
            MemorySegment outGaugeId = arena.allocate(ValueLayout.JAVA_INT);
            int status = RingloomNative.serviceGaugeRegister(
                nativeHandle,
                nativeName,
                nameBytes.length,
                outGaugeId
            );
            RingloomNative.throwForStatus("ringloom_service_gauge_register", status);
            return new NativeGauge(this, outGaugeId.get(ValueLayout.JAVA_INT, 0));
        }
    }

    /**
     * Stops the service registration without destroying the Java wrapper.
     */
    public void stop() {
        if (closed.get()) {
            return;
        }
        RingloomNative.serviceStop(nativeHandle);
    }

    /**
     * Destroys the native service handle. This method is idempotent.
     */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        RingloomNative.serviceDestroy(nativeHandle);
    }

    MemorySegment nativeHandle() {
        ensureOpen();
        return nativeHandle;
    }

    void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException("RingloomService is closed");
        }
    }

    private static byte[] metricNameBytes(String name) {
        Objects.requireNonNull(name, "name");
        byte[] bytes = name.getBytes(StandardCharsets.UTF_8);
        if (bytes.length == 0) {
            throw new IllegalArgumentException("name must not be empty");
        }
        if (bytes.length > MAX_METRIC_NAME_BYTES) {
            throw new IllegalArgumentException("name must be at most " + MAX_METRIC_NAME_BYTES + " UTF-8 bytes");
        }
        return bytes;
    }
}
