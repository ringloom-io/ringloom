package io.ringloom.service;

/**
 * Lifecycle event kind for discovered service instances.
 */
public enum ServiceLifecycleEventType {
    /**
     * A target instance is available, or its cached metadata such as leader status changed.
     */
    AVAILABLE,
    /**
     * A target instance is no longer available.
     */
    UNAVAILABLE;

    static ServiceLifecycleEventType fromNative(int value) {
        return switch (value) {
            case 1 -> AVAILABLE;
            case 2 -> UNAVAILABLE;
            default -> throw new IllegalArgumentException("Unknown service lifecycle event type: " + value);
        };
    }
}
