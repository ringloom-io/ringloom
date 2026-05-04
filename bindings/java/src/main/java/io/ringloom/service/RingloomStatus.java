package io.ringloom.service;

/**
 * Native RingLoom status code constants returned by hot-path Java APIs.
 */
public final class RingloomStatus {
    /**
     * Operation completed successfully.
     */
    public static final int OK = 0;
    /**
     * One or more arguments were invalid.
     */
    public static final int INVALID_ARGUMENT = 1;
    /**
     * Native allocation failed.
     */
    public static final int OUT_OF_MEMORY = 2;
    /**
     * The configured broker could not be found.
     */
    public static final int BROKER_NOT_FOUND = 3;
    /**
     * Service registration timed out.
     */
    public static final int REGISTRATION_TIMEOUT = 4;
    /**
     * A ring buffer did not have enough writable capacity.
     */
    public static final int BUFFER_FULL = 5;
    /**
     * No discovered target instance is currently available.
     */
    public static final int NO_AVAILABLE_INSTANCE = 6;
    /**
     * Flow control rejected the send because the target is under backpressure.
     */
    public static final int BACKPRESSURE = 7;
    /**
     * A remote peer needed for routing is disconnected.
     */
    public static final int PEER_DISCONNECTED = 8;
    /**
     * A claim operation required an active claim.
     */
    public static final int CLAIM_NOT_ACTIVE = 9;
    /**
     * The payload exceeds the target buffer's maximum message length.
     */
    public static final int MESSAGE_TOO_LONG = 10;
    /**
     * Unexpected native failure.
     */
    public static final int INTERNAL = 255;

    private RingloomStatus() {
    }

    /**
     * Returns whether a status code is {@link #OK}.
     *
     * @param status status code to test
     * @return {@code true} when the status is OK
     */
    public static boolean isOk(int status) {
        return status == OK;
    }
}
