package io.ringloom.service;

import java.util.Objects;

/**
 * Configuration for starting a {@link RingloomService}.
 *
 * @param serviceName logical service name to register
 * @param storagePath shared-memory storage path, or {@code null} for the default
 * @param group broker group name, or {@code null} for the default
 * @param brokerNodeId broker node id, or {@code 0} for the default
 * @param blockingMode whether native operations should use blocking behavior where supported
 * @param heartbeatTimeoutMillis heartbeat timeout, or {@code 0} for the default
 * @param controlBufferLength control ring buffer size, or {@code 0} for the default
 * @param messagesBufferLength message ring buffer size, or {@code 0} for the default
 * @param leaderElectionEnabled whether this service participates in leader election
 */
public record ServiceConfig(
    String serviceName,
    String storagePath,
    String group,
    short brokerNodeId,
    boolean blockingMode,
    int heartbeatTimeoutMillis,
    long controlBufferLength,
    long messagesBufferLength,
    boolean leaderElectionEnabled
) {
    public static final String DEFAULT_STORAGE_PATH = "/dev/shm";
    public static final String DEFAULT_GROUP = "default";
    public static final short DEFAULT_BROKER_NODE_ID = 1;
    public static final int DEFAULT_HEARTBEAT_TIMEOUT_MILLIS = 10_000;
    public static final long DEFAULT_CONTROL_BUFFER_LENGTH = 65_536L;
    public static final long DEFAULT_MESSAGES_BUFFER_LENGTH = 1_048_576L;

    /**
     * Validates the service name and applies defaults for optional zero or null fields.
     */
    public ServiceConfig {
        serviceName = Objects.requireNonNull(serviceName, "serviceName");
        if (serviceName.isEmpty()) {
            throw new IllegalArgumentException("serviceName must not be empty");
        }

        storagePath = storagePath == null ? DEFAULT_STORAGE_PATH : storagePath;
        group = group == null ? DEFAULT_GROUP : group;
        brokerNodeId = brokerNodeId == 0 ? DEFAULT_BROKER_NODE_ID : brokerNodeId;
        heartbeatTimeoutMillis = heartbeatTimeoutMillis == 0
            ? DEFAULT_HEARTBEAT_TIMEOUT_MILLIS
            : heartbeatTimeoutMillis;
        controlBufferLength = controlBufferLength == 0
            ? DEFAULT_CONTROL_BUFFER_LENGTH
            : controlBufferLength;
        messagesBufferLength = messagesBufferLength == 0
            ? DEFAULT_MESSAGES_BUFFER_LENGTH
            : messagesBufferLength;
    }

    /**
     * Creates a configuration using default values for all optional fields.
     *
     * @param serviceName logical service name to register
     * @return service configuration with defaults applied
     */
    public static ServiceConfig of(String serviceName) {
        return new ServiceConfig(serviceName, null, null, (short) 0, false, 0, 0, 0, false);
    }
}
