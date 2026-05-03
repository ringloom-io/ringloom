package io.ringloom.service;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

public final class RingloomMessage {
    private long correlationId;
    private short sourceNodeId;
    private short sourceServiceId;
    private short targetNodeId;
    private short targetServiceId;
    private short templateId;
    private byte flags;
    private long payloadAddress;
    private long payloadLength;

    RingloomMessage() {
    }

    void updateFromNative(MemorySegment nativeMessage) {
        MemorySegment view = nativeMessage.reinterpret(RingloomNative.MESSAGE_SIZE);
        correlationId = view.get(ValueLayout.JAVA_LONG, RingloomNative.MESSAGE_CORRELATION_ID_OFFSET);
        sourceNodeId = view.get(ValueLayout.JAVA_SHORT, RingloomNative.MESSAGE_SOURCE_NODE_ID_OFFSET);
        sourceServiceId = view.get(ValueLayout.JAVA_SHORT, RingloomNative.MESSAGE_SOURCE_SERVICE_ID_OFFSET);
        targetNodeId = view.get(ValueLayout.JAVA_SHORT, RingloomNative.MESSAGE_TARGET_NODE_ID_OFFSET);
        targetServiceId = view.get(ValueLayout.JAVA_SHORT, RingloomNative.MESSAGE_TARGET_SERVICE_ID_OFFSET);
        templateId = view.get(ValueLayout.JAVA_SHORT, RingloomNative.MESSAGE_TEMPLATE_ID_OFFSET);
        flags = view.get(ValueLayout.JAVA_BYTE, RingloomNative.MESSAGE_FLAGS_OFFSET);
        payloadAddress = view.get(RingloomNative.ADDRESS, RingloomNative.MESSAGE_PAYLOAD_OFFSET).address();
        payloadLength = view.get(ValueLayout.JAVA_LONG, RingloomNative.MESSAGE_PAYLOAD_LEN_OFFSET);
    }

    public long correlationId() {
        return correlationId;
    }

    public short sourceNodeId() {
        return sourceNodeId;
    }

    public short sourceServiceId() {
        return sourceServiceId;
    }

    public short targetNodeId() {
        return targetNodeId;
    }

    public short targetServiceId() {
        return targetServiceId;
    }

    public int templateId() {
        return Short.toUnsignedInt(templateId);
    }

    public int flags() {
        return Byte.toUnsignedInt(flags);
    }

    public long payloadAddress() {
        return payloadAddress;
    }

    public long payloadLength() {
        return payloadLength;
    }

    public MemorySegment payloadSegment() {
        return MemorySegment.ofAddress(payloadAddress).reinterpret(payloadLength);
    }

    public byte[] copyPayload() {
        return payloadSegment().toArray(ValueLayout.JAVA_BYTE);
    }
}
