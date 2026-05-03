package io.ringloom.service;

public record ServiceLifecycleEvent(
    ServiceLifecycleEventType type,
    String serviceName,
    int serviceId,
    short nodeId,
    boolean leader
) {
}
