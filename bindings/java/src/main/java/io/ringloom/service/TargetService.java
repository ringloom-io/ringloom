package io.ringloom.service;

/**
 * A discovered service instance that a {@link RingloomClient} can address.
 *
 * @param targetServiceId broker-assigned service id for the target instance
 * @param targetNodeId broker node that owns the target instance
 * @param leader whether this instance is the current leader for the service name
 */
public record TargetService(
    int targetServiceId,
    short targetNodeId,
    boolean leader
) {
}
