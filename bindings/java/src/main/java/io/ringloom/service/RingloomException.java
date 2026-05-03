package io.ringloom.service;

public final class RingloomException extends RuntimeException {
    private final int statusCode;
    private final String statusName;
    private final String nativeMessage;

    public RingloomException(int statusCode, String statusName, String nativeMessage, String action) {
        super(action + " failed with " + statusName + " (" + statusCode + "): " + nativeMessage);
        this.statusCode = statusCode;
        this.statusName = statusName;
        this.nativeMessage = nativeMessage;
    }

    public int statusCode() {
        return statusCode;
    }

    public String statusName() {
        return statusName;
    }

    public String nativeMessage() {
        return nativeMessage;
    }
}
