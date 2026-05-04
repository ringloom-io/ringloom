package io.ringloom.service;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Reusable borrowed view over one received RingLoom message.
 *
 * <p>{@link MessageConsumer} updates the same instance for each callback. Do not retain this
 * object or its payload segment after the callback returns unless the payload has been copied.</p>
 */
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

    /**
     * Returns the message correlation id.
     *
     * @return correlation id
     */
    public long correlationId() {
        return correlationId;
    }

    /**
     * Returns the source node id.
     *
     * @return source node id
     */
    public short sourceNodeId() {
        return sourceNodeId;
    }

    /**
     * Returns the source service id.
     *
     * @return source service id
     */
    public short sourceServiceId() {
        return sourceServiceId;
    }

    /**
     * Returns the target node id.
     *
     * @return target node id
     */
    public short targetNodeId() {
        return targetNodeId;
    }

    /**
     * Returns the target service id.
     *
     * @return target service id
     */
    public short targetServiceId() {
        return targetServiceId;
    }

    /**
     * Returns the unsigned message template id.
     *
     * @return template id in the range {@code 0..65535}
     */
    public int templateId() {
        return Short.toUnsignedInt(templateId);
    }

    /**
     * Returns the unsigned message flags byte.
     *
     * @return flags in the range {@code 0..255}
     */
    public int flags() {
        return Byte.toUnsignedInt(flags);
    }

    /**
     * Returns the native payload address.
     *
     * @return payload address
     */
    public long payloadAddress() {
        return payloadAddress;
    }

    /**
     * Returns the payload length.
     *
     * @return payload length in bytes
     */
    public long payloadLength() {
        return payloadLength;
    }

    /**
     * Returns a borrowed segment view over the payload bytes.
     *
     * @return borrowed payload segment
     */
    public MemorySegment payloadSegment() {
        return MemorySegment.ofAddress(payloadAddress).reinterpret(payloadLength);
    }

    /**
     * Copies the payload bytes into a new byte array.
     *
     * @return copied payload bytes
     */
    public byte[] copyPayload() {
        return payloadSegment().toArray(ValueLayout.JAVA_BYTE);
    }
}
