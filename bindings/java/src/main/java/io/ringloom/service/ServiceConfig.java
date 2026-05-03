package io.ringloom.service;

import java.util.Objects;

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

    public static ServiceConfig of(String serviceName) {
        return new ServiceConfig(serviceName, null, null, (short) 0, false, 0, 0, 0, false);
    }
}
