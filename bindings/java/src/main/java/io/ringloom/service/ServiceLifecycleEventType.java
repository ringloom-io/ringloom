package io.ringloom.service;

public enum ServiceLifecycleEventType {
    AVAILABLE,
    UNAVAILABLE;

    static ServiceLifecycleEventType fromNative(int value) {
        return switch (value) {
            case 1 -> AVAILABLE;
            case 2 -> UNAVAILABLE;
            default -> throw new IllegalArgumentException("Unknown service lifecycle event type: " + value);
        };
    }
}
