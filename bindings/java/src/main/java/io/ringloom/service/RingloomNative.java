package io.ringloom.service;

import java.lang.foreign.AddressLayout;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

final class RingloomNative {
    static final int ABI_VERSION = 1;

    static final Linker LINKER = Linker.nativeLinker();
    static final Arena LIBRARY_ARENA = Arena.ofShared();
    static final AddressLayout ADDRESS = ValueLayout.ADDRESS;
    static final long SERVICE_CONFIG_SIZE = 80L;
    static final long MESSAGE_SIZE = 40L;
    static final long BUFFER_CLAIM_SIZE = 40L;
    static final long CLIENT_TARGET_SIZE = 8L;
    static final long LIFECYCLE_EVENT_SIZE = 32L;

    static final long CONFIG_STORAGE_PATH_OFFSET = 0L;
    static final long CONFIG_STORAGE_PATH_LEN_OFFSET = 8L;
    static final long CONFIG_GROUP_OFFSET = 16L;
    static final long CONFIG_GROUP_LEN_OFFSET = 24L;
    static final long CONFIG_SERVICE_NAME_OFFSET = 32L;
    static final long CONFIG_SERVICE_NAME_LEN_OFFSET = 40L;
    static final long CONFIG_BROKER_NODE_ID_OFFSET = 48L;
    static final long CONFIG_BLOCKING_MODE_OFFSET = 50L;
    static final long CONFIG_HEARTBEAT_TIMEOUT_OFFSET = 52L;
    static final long CONFIG_CONTROL_BUFFER_LENGTH_OFFSET = 56L;
    static final long CONFIG_MESSAGES_BUFFER_LENGTH_OFFSET = 64L;
    static final long CONFIG_LEADER_ELECTION_OFFSET = 72L;

    static final long MESSAGE_CORRELATION_ID_OFFSET = 0L;
    static final long MESSAGE_SOURCE_NODE_ID_OFFSET = 8L;
    static final long MESSAGE_SOURCE_SERVICE_ID_OFFSET = 10L;
    static final long MESSAGE_TARGET_NODE_ID_OFFSET = 12L;
    static final long MESSAGE_TARGET_SERVICE_ID_OFFSET = 14L;
    static final long MESSAGE_TEMPLATE_ID_OFFSET = 16L;
    static final long MESSAGE_FLAGS_OFFSET = 18L;
    static final long MESSAGE_PAYLOAD_OFFSET = 24L;
    static final long MESSAGE_PAYLOAD_LEN_OFFSET = 32L;

    static final long BUFFER_CLAIM_PAYLOAD_OFFSET = 0L;
    static final long BUFFER_CLAIM_PAYLOAD_LEN_OFFSET = 8L;
    static final long BUFFER_CLAIM_ACTIVE_OFFSET = 36L;
    static final long CLIENT_TARGET_SERVICE_ID_OFFSET = 0L;
    static final long CLIENT_TARGET_IS_LEADER_OFFSET = 4L;
    static final long LIFECYCLE_EVENT_TYPE_OFFSET = 0L;
    static final long LIFECYCLE_EVENT_SERVICE_ID_OFFSET = 4L;
    static final long LIFECYCLE_EVENT_NODE_ID_OFFSET = 8L;
    static final long LIFECYCLE_EVENT_IS_LEADER_OFFSET = 10L;
    static final long LIFECYCLE_EVENT_SERVICE_NAME_OFFSET = 16L;
    static final long LIFECYCLE_EVENT_SERVICE_NAME_LEN_OFFSET = 24L;

    private static final SymbolLookup SYMBOLS;

    private static final MethodHandle ABI_VERSION_HANDLE;
    private static final MethodHandle SERVICE_START_HANDLE;
    private static final MethodHandle SERVICE_STOP_HANDLE;
    private static final MethodHandle SERVICE_DESTROY_HANDLE;
    private static final MethodHandle SERVICE_ID_HANDLE;
    private static final MethodHandle SERVICE_NODE_ID_HANDLE;
    private static final MethodHandle SERVICE_POLL_CONTROL_HANDLE;
    private static final MethodHandle CREATE_CONSUMER_HANDLE;
    private static final MethodHandle DESTROY_CONSUMER_HANDLE;
    private static final MethodHandle POLL_CONSUMER_HANDLE;
    private static final MethodHandle CREATE_CLIENT_HANDLE;
    private static final MethodHandle DESTROY_CLIENT_HANDLE;
    private static final MethodHandle CLIENT_SET_LIFECYCLE_HANDLE;
    private static final MethodHandle CLIENT_SEND_HANDLE;
    private static final MethodHandle CLIENT_SEND_TO_HANDLE;
    private static final MethodHandle CLIENT_SEND_TO_LEADER_HANDLE;
    private static final MethodHandle CLIENT_LIST_TARGETS_HANDLE;
    private static final MethodHandle CLIENT_TRY_CLAIM_HANDLE;
    private static final MethodHandle CLAIM_COMMIT_HANDLE;
    private static final MethodHandle CLAIM_ABORT_HANDLE;
    private static final MethodHandle STATUS_STRING_HANDLE;
    private static final MethodHandle LAST_ERROR_MESSAGE_HANDLE;

    static {
        try {
            Path libraryPath = nativeLibraryPath();
            System.load(libraryPath.toAbsolutePath().toString());
            SYMBOLS = SymbolLookup.libraryLookup(libraryPath, LIBRARY_ARENA);

            ABI_VERSION_HANDLE = downcall("ringloom_service_abi_version", FunctionDescriptor.of(ValueLayout.JAVA_INT));
            SERVICE_START_HANDLE = downcall("ringloom_service_start", FunctionDescriptor.of(ValueLayout.JAVA_INT, ADDRESS, ADDRESS));
            SERVICE_STOP_HANDLE = downcall("ringloom_service_stop", FunctionDescriptor.ofVoid(ADDRESS));
            SERVICE_DESTROY_HANDLE = downcall("ringloom_service_destroy", FunctionDescriptor.ofVoid(ADDRESS));
            SERVICE_ID_HANDLE = downcall("ringloom_service_id", FunctionDescriptor.of(ValueLayout.JAVA_INT, ADDRESS, ADDRESS));
            SERVICE_NODE_ID_HANDLE = downcall("ringloom_service_node_id", FunctionDescriptor.of(ValueLayout.JAVA_INT, ADDRESS, ADDRESS));
            SERVICE_POLL_CONTROL_HANDLE = downcall("ringloom_service_poll_control", FunctionDescriptor.of(ValueLayout.JAVA_INT, ADDRESS, ValueLayout.JAVA_INT, ADDRESS));
            CREATE_CONSUMER_HANDLE = downcall("ringloom_service_create_message_consumer", FunctionDescriptor.of(ValueLayout.JAVA_INT, ADDRESS, ADDRESS));
            DESTROY_CONSUMER_HANDLE = downcall("ringloom_message_consumer_destroy", FunctionDescriptor.ofVoid(ADDRESS));
            POLL_CONSUMER_HANDLE = downcall("ringloom_message_consumer_poll", FunctionDescriptor.of(ValueLayout.JAVA_INT, ADDRESS, ADDRESS, ADDRESS, ValueLayout.JAVA_INT, ADDRESS));
            CREATE_CLIENT_HANDLE = downcall("ringloom_service_create_client", FunctionDescriptor.of(ValueLayout.JAVA_INT, ADDRESS, ADDRESS, ValueLayout.JAVA_LONG, ADDRESS));
            DESTROY_CLIENT_HANDLE = downcall("ringloom_client_destroy", FunctionDescriptor.ofVoid(ADDRESS));
            CLIENT_SET_LIFECYCLE_HANDLE = downcall("ringloom_client_set_lifecycle_handler", FunctionDescriptor.of(ValueLayout.JAVA_INT, ADDRESS, ADDRESS, ADDRESS));
            CLIENT_SEND_HANDLE = downcall("ringloom_client_send", FunctionDescriptor.of(ValueLayout.JAVA_INT, ADDRESS, ADDRESS, ValueLayout.JAVA_LONG));
            CLIENT_SEND_TO_HANDLE = downcall("ringloom_client_send_to", FunctionDescriptor.of(ValueLayout.JAVA_INT, ADDRESS, ValueLayout.JAVA_INT, ADDRESS, ValueLayout.JAVA_LONG));
            CLIENT_SEND_TO_LEADER_HANDLE = downcall("ringloom_client_send_to_leader", FunctionDescriptor.of(ValueLayout.JAVA_INT, ADDRESS, ADDRESS, ValueLayout.JAVA_LONG));
            CLIENT_LIST_TARGETS_HANDLE = downcall("ringloom_client_list_targets", FunctionDescriptor.of(ValueLayout.JAVA_INT, ADDRESS, ADDRESS, ValueLayout.JAVA_LONG, ADDRESS));
            CLIENT_TRY_CLAIM_HANDLE = downcall("ringloom_client_try_claim", FunctionDescriptor.of(ValueLayout.JAVA_INT, ADDRESS, ValueLayout.JAVA_SHORT, ValueLayout.JAVA_LONG, ADDRESS));
            CLAIM_COMMIT_HANDLE = downcall("ringloom_buffer_claim_commit", FunctionDescriptor.of(ValueLayout.JAVA_INT, ADDRESS));
            CLAIM_ABORT_HANDLE = downcall("ringloom_buffer_claim_abort", FunctionDescriptor.of(ValueLayout.JAVA_INT, ADDRESS));
            STATUS_STRING_HANDLE = downcall("ringloom_status_string", FunctionDescriptor.of(ADDRESS, ValueLayout.JAVA_INT));
            LAST_ERROR_MESSAGE_HANDLE = downcall("ringloom_last_error_message", FunctionDescriptor.of(ADDRESS));

            int abiVersion = abiVersion();
            if (abiVersion != ABI_VERSION) {
                throw new IllegalStateException("Unsupported RingLoom native ABI version " + abiVersion);
            }
        } catch (RuntimeException | Error ex) {
            throw ex;
        } catch (Throwable ex) {
            throw new IllegalStateException("Failed to initialize RingLoom native bindings", ex);
        }
    }

    private RingloomNative() {
    }

    static Path nativeLibraryPath() {
        String libDir = System.getProperty("ringloom.nativeLibDir");
        if (libDir == null || libDir.isBlank()) {
            throw new IllegalStateException("ringloom.nativeLibDir system property is required");
        }

        Path path = Path.of(libDir).resolve(System.mapLibraryName("ringloom_service"));
        if (!Files.exists(path)) {
            throw new IllegalStateException("Native library not found at " + path);
        }
        return path;
    }

    static int abiVersion() {
        try {
            return (int) ABI_VERSION_HANDLE.invokeExact();
        } catch (Throwable throwable) {
            throw propagate("ringloom_service_abi_version", throwable);
        }
    }

    static int serviceStart(MemorySegment config, MemorySegment outService) {
        try {
            return (int) SERVICE_START_HANDLE.invokeExact(config, outService);
        } catch (Throwable throwable) {
            throw propagate("ringloom_service_start", throwable);
        }
    }

    static void serviceStop(MemorySegment service) {
        try {
            SERVICE_STOP_HANDLE.invokeExact(service);
        } catch (Throwable throwable) {
            throw propagate("ringloom_service_stop", throwable);
        }
    }

    static void serviceDestroy(MemorySegment service) {
        try {
            SERVICE_DESTROY_HANDLE.invokeExact(service);
        } catch (Throwable throwable) {
            throw propagate("ringloom_service_destroy", throwable);
        }
    }

    static int serviceId(MemorySegment service, MemorySegment outServiceId) {
        try {
            return (int) SERVICE_ID_HANDLE.invokeExact(service, outServiceId);
        } catch (Throwable throwable) {
            throw propagate("ringloom_service_id", throwable);
        }
    }

    static int serviceNodeId(MemorySegment service, MemorySegment outNodeId) {
        try {
            return (int) SERVICE_NODE_ID_HANDLE.invokeExact(service, outNodeId);
        } catch (Throwable throwable) {
            throw propagate("ringloom_service_node_id", throwable);
        }
    }

    static int servicePollControl(MemorySegment service, int limit, MemorySegment outCount) {
        try {
            return (int) SERVICE_POLL_CONTROL_HANDLE.invokeExact(service, limit, outCount);
        } catch (Throwable throwable) {
            throw propagate("ringloom_service_poll_control", throwable);
        }
    }

    static int createMessageConsumer(MemorySegment service, MemorySegment outConsumer) {
        try {
            return (int) CREATE_CONSUMER_HANDLE.invokeExact(service, outConsumer);
        } catch (Throwable throwable) {
            throw propagate("ringloom_service_create_message_consumer", throwable);
        }
    }

    static void destroyMessageConsumer(MemorySegment consumer) {
        try {
            DESTROY_CONSUMER_HANDLE.invokeExact(consumer);
        } catch (Throwable throwable) {
            throw propagate("ringloom_message_consumer_destroy", throwable);
        }
    }

    static int pollMessageConsumer(
        MemorySegment consumer,
        MemorySegment handler,
        MemorySegment userData,
        int limit,
        MemorySegment outCount
    ) {
        try {
            return (int) POLL_CONSUMER_HANDLE.invokeExact(consumer, handler, userData, limit, outCount);
        } catch (Throwable throwable) {
            throw propagate("ringloom_message_consumer_poll", throwable);
        }
    }

    static int createClient(MemorySegment service, MemorySegment serviceName, long serviceNameLen, MemorySegment outClient) {
        try {
            return (int) CREATE_CLIENT_HANDLE.invokeExact(service, serviceName, serviceNameLen, outClient);
        } catch (Throwable throwable) {
            throw propagate("ringloom_service_create_client", throwable);
        }
    }

    static void destroyClient(MemorySegment client) {
        try {
            DESTROY_CLIENT_HANDLE.invokeExact(client);
        } catch (Throwable throwable) {
            throw propagate("ringloom_client_destroy", throwable);
        }
    }

    static int clientSetLifecycleHandler(MemorySegment client, MemorySegment handler, MemorySegment userData) {
        try {
            return (int) CLIENT_SET_LIFECYCLE_HANDLE.invokeExact(client, handler, userData);
        } catch (Throwable throwable) {
            throw propagate("ringloom_client_set_lifecycle_handler", throwable);
        }
    }

    static int clientSend(MemorySegment client, MemorySegment payload, long payloadLen) {
        try {
            return (int) CLIENT_SEND_HANDLE.invokeExact(client, payload, payloadLen);
        } catch (Throwable throwable) {
            throw propagate("ringloom_client_send", throwable);
        }
    }

    static int clientSendTo(MemorySegment client, int targetServiceId, MemorySegment payload, long payloadLen) {
        try {
            return (int) CLIENT_SEND_TO_HANDLE.invokeExact(client, targetServiceId, payload, payloadLen);
        } catch (Throwable throwable) {
            throw propagate("ringloom_client_send_to", throwable);
        }
    }

    static int clientSendToLeader(MemorySegment client, MemorySegment payload, long payloadLen) {
        try {
            return (int) CLIENT_SEND_TO_LEADER_HANDLE.invokeExact(client, payload, payloadLen);
        } catch (Throwable throwable) {
            throw propagate("ringloom_client_send_to_leader", throwable);
        }
    }

    static int clientListTargets(MemorySegment client, MemorySegment outTargets, long targetCapacity, MemorySegment outCount) {
        try {
            return (int) CLIENT_LIST_TARGETS_HANDLE.invokeExact(client, outTargets, targetCapacity, outCount);
        } catch (Throwable throwable) {
            throw propagate("ringloom_client_list_targets", throwable);
        }
    }

    static int clientTryClaim(MemorySegment client, short templateId, long payloadLen, MemorySegment outClaim) {
        try {
            return (int) CLIENT_TRY_CLAIM_HANDLE.invokeExact(client, templateId, payloadLen, outClaim);
        } catch (Throwable throwable) {
            throw propagate("ringloom_client_try_claim", throwable);
        }
    }

    static int claimCommit(MemorySegment claim) {
        try {
            return (int) CLAIM_COMMIT_HANDLE.invokeExact(claim);
        } catch (Throwable throwable) {
            throw propagate("ringloom_buffer_claim_commit", throwable);
        }
    }

    static int claimAbort(MemorySegment claim) {
        try {
            return (int) CLAIM_ABORT_HANDLE.invokeExact(claim);
        } catch (Throwable throwable) {
            throw propagate("ringloom_buffer_claim_abort", throwable);
        }
    }

    static String statusName(int status) {
        try {
            return readCString((MemorySegment) STATUS_STRING_HANDLE.invokeExact(status), 128);
        } catch (Throwable throwable) {
            throw propagate("ringloom_status_string", throwable);
        }
    }

    static String lastErrorMessage() {
        try {
            return readCString((MemorySegment) LAST_ERROR_MESSAGE_HANDLE.invokeExact(), 8192);
        } catch (Throwable throwable) {
            throw propagate("ringloom_last_error_message", throwable);
        }
    }

    static void throwForStatus(String action, int status) {
        if (status == RingloomStatus.OK) {
            return;
        }

        String nativeMessage = lastErrorMessage();
        if (status == RingloomStatus.INVALID_ARGUMENT) {
            throw new IllegalArgumentException(action + " failed: " + nativeMessage);
        }
        if (status == RingloomStatus.OUT_OF_MEMORY) {
            throw new OutOfMemoryError(action + " failed: " + nativeMessage);
        }
        throw new RingloomException(status, statusName(status), nativeMessage, action);
    }

    static MemorySegment payloadPointer(MemorySegment payload) {
        return payload == null ? MemorySegment.NULL : payload;
    }

    static String readCString(MemorySegment address, int maxBytes) {
        if (address == null || address.address() == 0) {
            return "";
        }

        MemorySegment bytes = address.reinterpret(maxBytes);
        int len = 0;
        while (len < maxBytes && bytes.get(ValueLayout.JAVA_BYTE, len) != 0) {
            len += 1;
        }

        byte[] copy = new byte[len];
        MemorySegment.copy(bytes, ValueLayout.JAVA_BYTE, 0, copy, 0, len);
        return new String(copy, StandardCharsets.UTF_8);
    }

    private static MethodHandle downcall(String symbol, FunctionDescriptor descriptor) {
        MemorySegment address = SYMBOLS.find(symbol)
            .orElseThrow(() -> new IllegalStateException("Missing native symbol " + symbol));
        return LINKER.downcallHandle(address, descriptor);
    }

    private static RuntimeException propagate(String action, Throwable throwable) {
        if (throwable instanceof RuntimeException runtimeException) {
            return runtimeException;
        }
        if (throwable instanceof Error error) {
            throw error;
        }
        return new IllegalStateException("Native invocation failed for " + action, throwable);
    }
}
