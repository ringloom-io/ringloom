package io.ringloom.service;

/**
 * Service discovery notification delivered to a {@link RingloomClient}.
 *
 * @param type availability state represented by this event
 * @param serviceName logical service name the client subscribed to
 * @param serviceId broker-assigned service id
 * @param nodeId broker node that owns the service instance
 * @param leader whether this instance is the current leader for the service name
 */
public record ServiceLifecycleEvent(
    ServiceLifecycleEventType type,
    String serviceName,
    int serviceId,
    short nodeId,
    boolean leader
) {
}
