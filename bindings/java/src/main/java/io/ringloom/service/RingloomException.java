package io.ringloom.service;

/**
 * Exception thrown by convenience APIs that map non-OK native statuses to Java failures.
 */
public final class RingloomException extends RuntimeException {
    /**
     * Native status code returned by the failed operation.
     */
    private final int statusCode;
    /**
     * Native symbolic status name.
     */
    private final String statusName;
    /**
     * Native diagnostic message captured for the failed operation.
     */
    private final String nativeMessage;

    /**
     * Creates an exception from a native status and diagnostic message.
     *
     * @param statusCode native status code
     * @param statusName native status name
     * @param nativeMessage native diagnostic message
     * @param action operation that failed
     */
    public RingloomException(int statusCode, String statusName, String nativeMessage, String action) {
        super(action + " failed with " + statusName + " (" + statusCode + "): " + nativeMessage);
        this.statusCode = statusCode;
        this.statusName = statusName;
        this.nativeMessage = nativeMessage;
    }

    /**
     * Returns the native status code.
     *
     * @return status code
     */
    public int statusCode() {
        return statusCode;
    }

    /**
     * Returns the native status name.
     *
     * @return status name
     */
    public String statusName() {
        return statusName;
    }

    /**
     * Returns the native diagnostic message.
     *
     * @return native diagnostic message
     */
    public String nativeMessage() {
        return nativeMessage;
    }
}
