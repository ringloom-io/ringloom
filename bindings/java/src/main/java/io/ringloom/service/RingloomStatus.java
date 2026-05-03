package io.ringloom.service;

public final class RingloomStatus {
    public static final int OK = 0;
    public static final int INVALID_ARGUMENT = 1;
    public static final int OUT_OF_MEMORY = 2;
    public static final int BROKER_NOT_FOUND = 3;
    public static final int REGISTRATION_TIMEOUT = 4;
    public static final int BUFFER_FULL = 5;
    public static final int NO_AVAILABLE_INSTANCE = 6;
    public static final int BACKPRESSURE = 7;
    public static final int PEER_DISCONNECTED = 8;
    public static final int CLAIM_NOT_ACTIVE = 9;
    public static final int MESSAGE_TOO_LONG = 10;
    public static final int INTERNAL = 255;

    private RingloomStatus() {
    }

    public static boolean isOk(int status) {
        return status == OK;
    }
}
