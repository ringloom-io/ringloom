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

    static final int ABI_VERSION = 4;
    private static final String LIBRARY_BASE_NAME = "ringloom_service";
    private static final String CLASSPATH_LIBRARY_ROOT =
        "/io/ringloom/service/native";

    static final Linker LINKER = Linker.nativeLinker();
    static final Arena LIBRARY_ARENA = Arena.ofShared();
    static final AddressLayout ADDRESS = ValueLayout.ADDRESS;
    static final long SERVICE_CONFIG_SIZE = 80L;
    static final long MESSAGE_SIZE = 40L;
    static final long BUFFER_CLAIM_SIZE = 40L;
    static final long CLIENT_TARGET_SIZE = 8L;
    static final long LIFECYCLE_EVENT_SIZE = 32L;
    static final long METRIC_DESCRIPTOR_SIZE = 32L;
    static final long METRIC_SLOT_SIZE = 24L;
    static final long RING_STATS_SIZE = 40L;

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
    static final long CLIENT_TARGET_NODE_ID_OFFSET = 4L;
    static final long CLIENT_TARGET_IS_LEADER_OFFSET = 6L;
    static final long LIFECYCLE_EVENT_TYPE_OFFSET = 0L;
    static final long LIFECYCLE_EVENT_SERVICE_ID_OFFSET = 4L;
    static final long LIFECYCLE_EVENT_NODE_ID_OFFSET = 8L;
    static final long LIFECYCLE_EVENT_IS_LEADER_OFFSET = 10L;
    static final long LIFECYCLE_EVENT_SERVICE_NAME_OFFSET = 16L;
    static final long LIFECYCLE_EVENT_SERVICE_NAME_LEN_OFFSET = 24L;
    static final long METRIC_NAME_OFFSET = 0L;
    static final long METRIC_NAME_LEN_OFFSET = 8L;
    static final long METRIC_KIND_OFFSET = 16L;
    static final long METRIC_VALUE_OFFSET = 24L;
    static final long METRIC_SLOT_ID_OFFSET = 0L;
    static final long METRIC_SLOT_VALUE_OFFSET = 8L;
    static final long METRIC_SLOT_VALUE_LEN_OFFSET = 16L;
    static final long RING_STATS_CAPACITY_OFFSET = 0L;
    static final long RING_STATS_USED_OFFSET = 8L;
    static final long RING_STATS_FREE_OFFSET = 16L;
    static final long RING_STATS_PRODUCER_OFFSET = 24L;
    static final long RING_STATS_CONSUMER_OFFSET = 32L;

    // ── Topic config layout ────────────────────────────────────────────
    static final long TOPIC_CONFIG_SIZE = 28L;
    static final long TOPIC_CONFIG_SIZE_OFFSET = 0L;
    static final long TOPIC_CONFIG_ROLL_SCHEME_OFFSET = 4L;
    static final long TOPIC_CONFIG_RETENTION_OFFSET = 20L;
    static final long TOPIC_CONFIG_FLAGS_OFFSET = 24L;

    private static final SymbolLookup SYMBOLS;

    private static final MethodHandle ABI_VERSION_HANDLE;
    private static final MethodHandle SERVICE_START_HANDLE;
    private static final MethodHandle SERVICE_STOP_HANDLE;
    private static final MethodHandle SERVICE_DESTROY_HANDLE;
    private static final MethodHandle SERVICE_ID_HANDLE;
    private static final MethodHandle SERVICE_NODE_ID_HANDLE;
    private static final MethodHandle SERVICE_AERON_DIRECTORY_HANDLE;
    private static final MethodHandle SERVICE_AERON_INBOUND_STREAM_ID_HANDLE;
    private static final MethodHandle SERVICE_PUBLICATION_CONNECTED_HANDLE;
    private static final MethodHandle SERVICE_POLL_CONTROL_HANDLE;
    private static final MethodHandle CREATE_CONSUMER_HANDLE;
    private static final MethodHandle DESTROY_CONSUMER_HANDLE;
    private static final MethodHandle POLL_CONSUMER_HANDLE;
    private static final MethodHandle CREATE_METRICS_READER_HANDLE;
    private static final MethodHandle DESTROY_METRICS_READER_HANDLE;
    private static final MethodHandle METRICS_COUNTER_COUNT_HANDLE;
    private static final MethodHandle METRICS_COUNTER_AT_HANDLE;
    private static final MethodHandle METRICS_RING_STATS_HANDLE;
    private static final MethodHandle SERVICE_COUNTER_REGISTER_HANDLE;
    private static final MethodHandle SERVICE_GAUGE_REGISTER_HANDLE;
    private static final MethodHandle CREATE_CLIENT_HANDLE;
    private static final MethodHandle DESTROY_CLIENT_HANDLE;
    private static final MethodHandle CLIENT_SET_LIFECYCLE_HANDLE;
    private static final MethodHandle CLIENT_SEND_HANDLE;
    private static final MethodHandle CLIENT_SEND_MESSAGE_HANDLE;
    private static final MethodHandle CLIENT_SEND_MESSAGE_REQUEST_HANDLE;
    private static final MethodHandle CLIENT_SEND_TO_HANDLE;
    private static final MethodHandle CLIENT_SEND_TO_MESSAGE_HANDLE;
    private static final MethodHandle CLIENT_SEND_TO_MESSAGE_REQUEST_HANDLE;
    private static final MethodHandle CLIENT_SEND_TO_LEADER_HANDLE;
    private static final MethodHandle CLIENT_SEND_TO_LEADER_MESSAGE_HANDLE;
    private static final MethodHandle CLIENT_SEND_TO_LEADER_MESSAGE_REQUEST_HANDLE;
    private static final MethodHandle CLIENT_LIST_TARGETS_HANDLE;
    private static final MethodHandle CLIENT_TRY_CLAIM_HANDLE;
    private static final MethodHandle CLIENT_TRY_CLAIM_REQUEST_HANDLE;
    private static final MethodHandle CLIENT_TRY_CLAIM_TO_HANDLE;
    private static final MethodHandle CLIENT_TRY_CLAIM_TO_REQUEST_HANDLE;
    private static final MethodHandle CLIENT_TRY_CLAIM_TO_LEADER_HANDLE;
    private static final MethodHandle CLIENT_TRY_CLAIM_TO_LEADER_REQUEST_HANDLE;
    private static final MethodHandle CLIENT_LAST_AERON_SEND_STATUS_HANDLE;
    private static final MethodHandle CLAIM_COMMIT_HANDLE;
    private static final MethodHandle CLAIM_ABORT_HANDLE;
    private static final MethodHandle STATUS_STRING_HANDLE;
    private static final MethodHandle AERON_PUBLICATION_STATUS_STRING_HANDLE;
    private static final MethodHandle LAST_ERROR_MESSAGE_HANDLE;

    // ── Topic method handles (resolved optionally) ─────────────────────
    private static final MethodHandle TOPIC_REGISTER_PUBLICATION_HANDLE;
    private static final MethodHandle TOPIC_PUBLISH_HANDLE;
    private static final MethodHandle TOPIC_IS_ACKED_HANDLE;
    private static final MethodHandle TOPIC_UNREGISTER_PUBLICATION_HANDLE;
    private static final MethodHandle TOPIC_SUBSCRIBE_HANDLE;
    private static final MethodHandle TOPIC_POLL_HANDLE;
    private static final MethodHandle TOPIC_UNSUBSCRIBE_HANDLE;
    private static final MethodHandle TOPIC_SUBSCRIPTION_MAINTENANCE_POLL_HANDLE;

    /** Whether all eight topic symbols were resolved from the native library. */
    static final boolean TOPIC_SYMBOLS_PRESENT;

    static {
        try {
            Path libraryPath = nativeLibraryPath();
            System.load(libraryPath.toAbsolutePath().toString());
            SYMBOLS = SymbolLookup.libraryLookup(libraryPath, LIBRARY_ARENA);

            ABI_VERSION_HANDLE = downcall(
                "ringloom_service_abi_version",
                FunctionDescriptor.of(ValueLayout.JAVA_INT)
            );
            SERVICE_START_HANDLE = downcall(
                "ringloom_service_start",
                FunctionDescriptor.of(ValueLayout.JAVA_INT, ADDRESS, ADDRESS)
            );
            SERVICE_STOP_HANDLE = downcall(
                "ringloom_service_stop",
                FunctionDescriptor.ofVoid(ADDRESS)
            );
            SERVICE_DESTROY_HANDLE = downcall(
                "ringloom_service_destroy",
                FunctionDescriptor.ofVoid(ADDRESS)
            );
            SERVICE_ID_HANDLE = downcall(
                "ringloom_service_id",
                FunctionDescriptor.of(ValueLayout.JAVA_INT, ADDRESS, ADDRESS)
            );
            SERVICE_NODE_ID_HANDLE = downcall(
                "ringloom_service_node_id",
                FunctionDescriptor.of(ValueLayout.JAVA_INT, ADDRESS, ADDRESS)
            );
            SERVICE_AERON_DIRECTORY_HANDLE = downcall(
                "ringloom_service_aeron_directory",
                FunctionDescriptor.of(
                    ValueLayout.JAVA_INT,
                    ADDRESS,
                    ADDRESS,
                    ADDRESS
                )
            );
            SERVICE_AERON_INBOUND_STREAM_ID_HANDLE = downcall(
                "ringloom_service_aeron_inbound_stream_id",
                FunctionDescriptor.of(ValueLayout.JAVA_INT, ADDRESS, ADDRESS)
            );
            SERVICE_PUBLICATION_CONNECTED_HANDLE = downcall(
                "ringloom_service_publication_connected",
                FunctionDescriptor.of(ValueLayout.JAVA_INT, ADDRESS, ADDRESS)
            );
            SERVICE_POLL_CONTROL_HANDLE = downcall(
                "ringloom_service_poll_control",
                FunctionDescriptor.of(
                    ValueLayout.JAVA_INT,
                    ADDRESS,
                    ValueLayout.JAVA_INT,
                    ADDRESS
                )
            );
            CREATE_CONSUMER_HANDLE = downcall(
                "ringloom_service_create_message_consumer",
                FunctionDescriptor.of(ValueLayout.JAVA_INT, ADDRESS, ADDRESS)
            );
            DESTROY_CONSUMER_HANDLE = downcall(
                "ringloom_message_consumer_destroy",
                FunctionDescriptor.ofVoid(ADDRESS)
            );
            POLL_CONSUMER_HANDLE = downcall(
                "ringloom_message_consumer_poll",
                FunctionDescriptor.of(
                    ValueLayout.JAVA_INT,
                    ADDRESS,
                    ADDRESS,
                    ADDRESS,
                    ValueLayout.JAVA_INT,
                    ADDRESS
                )
            );
            CREATE_METRICS_READER_HANDLE = downcall(
                "ringloom_service_create_metrics_reader",
                FunctionDescriptor.of(ValueLayout.JAVA_INT, ADDRESS, ADDRESS)
            );
            DESTROY_METRICS_READER_HANDLE = downcall(
                "ringloom_metrics_reader_destroy",
                FunctionDescriptor.ofVoid(ADDRESS)
            );
            METRICS_COUNTER_COUNT_HANDLE = downcall(
                "ringloom_metrics_reader_counter_count",
                FunctionDescriptor.of(ValueLayout.JAVA_INT, ADDRESS, ADDRESS)
            );
            METRICS_COUNTER_AT_HANDLE = downcall(
                "ringloom_metrics_reader_counter_at",
                FunctionDescriptor.of(
                    ValueLayout.JAVA_INT,
                    ADDRESS,
                    ValueLayout.JAVA_LONG,
                    ADDRESS
                )
            );
            METRICS_RING_STATS_HANDLE = downcall(
                "ringloom_metrics_reader_ring_stats",
                FunctionDescriptor.of(
                    ValueLayout.JAVA_INT,
                    ADDRESS,
                    ADDRESS,
                    ValueLayout.JAVA_LONG,
                    ADDRESS
                )
            );
            SERVICE_COUNTER_REGISTER_HANDLE = downcall(
                "ringloom_service_counter_register",
                FunctionDescriptor.of(
                    ValueLayout.JAVA_INT,
                    ADDRESS,
                    ADDRESS,
                    ValueLayout.JAVA_LONG,
                    ADDRESS
                )
            );
            SERVICE_GAUGE_REGISTER_HANDLE = downcall(
                "ringloom_service_gauge_register",
                FunctionDescriptor.of(
                    ValueLayout.JAVA_INT,
                    ADDRESS,
                    ADDRESS,
                    ValueLayout.JAVA_LONG,
                    ADDRESS
                )
            );
            CREATE_CLIENT_HANDLE = downcall(
                "ringloom_service_create_client",
                FunctionDescriptor.of(
                    ValueLayout.JAVA_INT,
                    ADDRESS,
                    ADDRESS,
                    ValueLayout.JAVA_LONG,
                    ADDRESS
                )
            );
            DESTROY_CLIENT_HANDLE = downcall(
                "ringloom_client_destroy",
                FunctionDescriptor.ofVoid(ADDRESS)
            );
            CLIENT_SET_LIFECYCLE_HANDLE = downcall(
                "ringloom_client_set_lifecycle_handler",
                FunctionDescriptor.of(
                    ValueLayout.JAVA_INT,
                    ADDRESS,
                    ADDRESS,
                    ADDRESS
                )
            );
            CLIENT_SEND_HANDLE = downcall(
                "ringloom_client_send",
                FunctionDescriptor.of(
                    ValueLayout.JAVA_INT,
                    ADDRESS,
                    ADDRESS,
                    ValueLayout.JAVA_LONG
                )
            );
            CLIENT_SEND_MESSAGE_HANDLE = downcall(
                "ringloom_client_send_message",
                FunctionDescriptor.of(
                    ValueLayout.JAVA_INT,
                    ADDRESS,
                    ValueLayout.JAVA_SHORT,
                    ADDRESS,
                    ValueLayout.JAVA_LONG
                )
            );
            CLIENT_SEND_MESSAGE_REQUEST_HANDLE = downcall(
                "ringloom_client_send_message_request",
                FunctionDescriptor.of(
                    ValueLayout.JAVA_INT,
                    ADDRESS,
                    ValueLayout.JAVA_SHORT,
                    ValueLayout.JAVA_LONG,
                    ADDRESS,
                    ValueLayout.JAVA_LONG
                )
            );
            CLIENT_SEND_TO_HANDLE = downcall(
                "ringloom_client_send_to",
                FunctionDescriptor.of(
                    ValueLayout.JAVA_INT,
                    ADDRESS,
                    ValueLayout.JAVA_SHORT,
                    ValueLayout.JAVA_INT,
                    ADDRESS,
                    ValueLayout.JAVA_LONG
                )
            );
            CLIENT_SEND_TO_MESSAGE_HANDLE = downcall(
                "ringloom_client_send_to_message",
                FunctionDescriptor.of(
                    ValueLayout.JAVA_INT,
                    ADDRESS,
                    ValueLayout.JAVA_SHORT,
                    ValueLayout.JAVA_INT,
                    ValueLayout.JAVA_SHORT,
                    ADDRESS,
                    ValueLayout.JAVA_LONG
                )
            );
            CLIENT_SEND_TO_MESSAGE_REQUEST_HANDLE = downcall(
                "ringloom_client_send_to_message_request",
                FunctionDescriptor.of(
                    ValueLayout.JAVA_INT,
                    ADDRESS,
                    ValueLayout.JAVA_SHORT,
                    ValueLayout.JAVA_INT,
                    ValueLayout.JAVA_SHORT,
                    ValueLayout.JAVA_LONG,
                    ADDRESS,
                    ValueLayout.JAVA_LONG
                )
            );
            CLIENT_SEND_TO_LEADER_HANDLE = downcall(
                "ringloom_client_send_to_leader",
                FunctionDescriptor.of(
                    ValueLayout.JAVA_INT,
                    ADDRESS,
                    ADDRESS,
                    ValueLayout.JAVA_LONG
                )
            );
            CLIENT_SEND_TO_LEADER_MESSAGE_HANDLE = downcall(
                "ringloom_client_send_to_leader_message",
                FunctionDescriptor.of(
                    ValueLayout.JAVA_INT,
                    ADDRESS,
                    ValueLayout.JAVA_SHORT,
                    ADDRESS,
                    ValueLayout.JAVA_LONG
                )
            );
            CLIENT_SEND_TO_LEADER_MESSAGE_REQUEST_HANDLE = downcall(
                "ringloom_client_send_to_leader_message_request",
                FunctionDescriptor.of(
                    ValueLayout.JAVA_INT,
                    ADDRESS,
                    ValueLayout.JAVA_SHORT,
                    ValueLayout.JAVA_LONG,
                    ADDRESS,
                    ValueLayout.JAVA_LONG
                )
            );
            CLIENT_LIST_TARGETS_HANDLE = downcall(
                "ringloom_client_list_targets",
                FunctionDescriptor.of(
                    ValueLayout.JAVA_INT,
                    ADDRESS,
                    ADDRESS,
                    ValueLayout.JAVA_LONG,
                    ADDRESS
                )
            );
            CLIENT_TRY_CLAIM_HANDLE = downcall(
                "ringloom_client_try_claim",
                FunctionDescriptor.of(
                    ValueLayout.JAVA_INT,
                    ADDRESS,
                    ValueLayout.JAVA_SHORT,
                    ValueLayout.JAVA_LONG,
                    ADDRESS
                )
            );
            CLIENT_TRY_CLAIM_REQUEST_HANDLE = downcall(
                "ringloom_client_try_claim_request",
                FunctionDescriptor.of(
                    ValueLayout.JAVA_INT,
                    ADDRESS,
                    ValueLayout.JAVA_SHORT,
                    ValueLayout.JAVA_LONG,
                    ValueLayout.JAVA_LONG,
                    ADDRESS
                )
            );
            CLIENT_TRY_CLAIM_TO_HANDLE = downcall(
                "ringloom_client_try_claim_to",
                FunctionDescriptor.of(
                    ValueLayout.JAVA_INT,
                    ADDRESS,
                    ValueLayout.JAVA_SHORT,
                    ValueLayout.JAVA_INT,
                    ValueLayout.JAVA_SHORT,
                    ValueLayout.JAVA_LONG,
                    ADDRESS
                )
            );
            CLIENT_TRY_CLAIM_TO_REQUEST_HANDLE = downcall(
                "ringloom_client_try_claim_to_request",
                FunctionDescriptor.of(
                    ValueLayout.JAVA_INT,
                    ADDRESS,
                    ValueLayout.JAVA_SHORT,
                    ValueLayout.JAVA_INT,
                    ValueLayout.JAVA_SHORT,
                    ValueLayout.JAVA_LONG,
                    ValueLayout.JAVA_LONG,
                    ADDRESS
                )
            );
            CLIENT_TRY_CLAIM_TO_LEADER_HANDLE = downcall(
                "ringloom_client_try_claim_to_leader",
                FunctionDescriptor.of(
                    ValueLayout.JAVA_INT,
                    ADDRESS,
                    ValueLayout.JAVA_SHORT,
                    ValueLayout.JAVA_LONG,
                    ADDRESS
                )
            );
            CLIENT_TRY_CLAIM_TO_LEADER_REQUEST_HANDLE = downcall(
                "ringloom_client_try_claim_to_leader_request",
                FunctionDescriptor.of(
                    ValueLayout.JAVA_INT,
                    ADDRESS,
                    ValueLayout.JAVA_SHORT,
                    ValueLayout.JAVA_LONG,
                    ValueLayout.JAVA_LONG,
                    ADDRESS
                )
            );
            CLIENT_LAST_AERON_SEND_STATUS_HANDLE = downcall(
                "ringloom_client_last_aeron_send_status",
                FunctionDescriptor.of(ValueLayout.JAVA_INT, ADDRESS, ADDRESS)
            );
            CLAIM_COMMIT_HANDLE = downcall(
                "ringloom_buffer_claim_commit",
                FunctionDescriptor.of(ValueLayout.JAVA_INT, ADDRESS)
            );
            CLAIM_ABORT_HANDLE = downcall(
                "ringloom_buffer_claim_abort",
                FunctionDescriptor.of(ValueLayout.JAVA_INT, ADDRESS)
            );
            STATUS_STRING_HANDLE = downcall(
                "ringloom_status_string",
                FunctionDescriptor.of(ADDRESS, ValueLayout.JAVA_INT)
            );
            AERON_PUBLICATION_STATUS_STRING_HANDLE = downcall(
                "ringloom_aeron_publication_status_string",
                FunctionDescriptor.of(ADDRESS, ValueLayout.JAVA_INT)
            );
            LAST_ERROR_MESSAGE_HANDLE = downcall(
                "ringloom_last_error_message",
                FunctionDescriptor.of(ADDRESS)
            );

            // Resolve topic symbols optionally — topics are an additive feature.
            TOPIC_REGISTER_PUBLICATION_HANDLE = optionalDowncall(
                "ringloom_register_topic_publication",
                FunctionDescriptor.of(
                    ValueLayout.JAVA_INT,
                    ADDRESS,
                    ADDRESS,
                    ADDRESS,
                    ValueLayout.JAVA_LONG,
                    ADDRESS
                )
            );
            TOPIC_PUBLISH_HANDLE = optionalDowncall(
                "ringloom_publish_to_topic",
                FunctionDescriptor.of(
                    ValueLayout.JAVA_INT,
                    ADDRESS,
                    ADDRESS,
                    ValueLayout.JAVA_LONG,
                    ValueLayout.JAVA_LONG,
                    ValueLayout.JAVA_BYTE,
                    ADDRESS
                )
            );
            TOPIC_IS_ACKED_HANDLE = optionalDowncall(
                "ringloom_topic_is_acked",
                FunctionDescriptor.of(
                    ValueLayout.JAVA_INT,
                    ADDRESS,
                    ValueLayout.JAVA_LONG
                )
            );
            TOPIC_UNREGISTER_PUBLICATION_HANDLE = optionalDowncall(
                "ringloom_unregister_topic_publication",
                FunctionDescriptor.ofVoid(ADDRESS)
            );
            TOPIC_SUBSCRIBE_HANDLE = optionalDowncall(
                "ringloom_subscribe_topic",
                FunctionDescriptor.of(
                    ValueLayout.JAVA_INT,
                    ADDRESS,
                    ADDRESS,
                    ValueLayout.JAVA_LONG,
                    ValueLayout.JAVA_BYTE,
                    ADDRESS
                )
            );
            TOPIC_POLL_HANDLE = optionalDowncall(
                "ringloom_topic_poll",
                FunctionDescriptor.of(
                    ValueLayout.JAVA_INT,
                    ADDRESS,
                    ADDRESS,
                    ADDRESS,
                    ADDRESS
                )
            );
            TOPIC_UNSUBSCRIBE_HANDLE = optionalDowncall(
                "ringloom_unsubscribe_topic",
                FunctionDescriptor.ofVoid(ADDRESS)
            );
            TOPIC_SUBSCRIPTION_MAINTENANCE_POLL_HANDLE = optionalDowncall(
                "ringloom_topic_subscription_maintenance_poll",
                FunctionDescriptor.of(
                    ValueLayout.JAVA_INT,
                    ADDRESS,
                    ValueLayout.JAVA_INT
                )
            );

            TOPIC_SYMBOLS_PRESENT =
                TOPIC_REGISTER_PUBLICATION_HANDLE != null &&
                TOPIC_PUBLISH_HANDLE != null &&
                TOPIC_IS_ACKED_HANDLE != null &&
                TOPIC_UNREGISTER_PUBLICATION_HANDLE != null &&
                TOPIC_SUBSCRIBE_HANDLE != null &&
                TOPIC_POLL_HANDLE != null &&
                TOPIC_UNSUBSCRIBE_HANDLE != null &&
                TOPIC_SUBSCRIPTION_MAINTENANCE_POLL_HANDLE != null;

            int abiVersion = abiVersion();
            if (abiVersion != ABI_VERSION) {
                throw new IllegalStateException(
                    "Unsupported RingLoom native ABI version " + abiVersion
                );
            }
        } catch (RuntimeException | Error ex) {
            throw ex;
        } catch (Throwable ex) {
            throw new IllegalStateException(
                "Failed to initialize RingLoom native bindings",
                ex
            );
        }
    }

    private RingloomNative() {}

    static Path nativeLibraryPath() {
        String libPath = System.getProperty("ringloom.nativeLibPath");
        if (libPath != null && !libPath.isBlank()) {
            return requireExistingLibrary(Path.of(libPath));
        }

        String libDir = System.getProperty("ringloom.nativeLibDir");
        if (libDir != null && !libDir.isBlank()) {
            return requireExistingLibrary(
                Path.of(libDir).resolve(
                    System.mapLibraryName(LIBRARY_BASE_NAME)
                )
            );
        }

        return extractClasspathLibrary();
    }

    private static Path requireExistingLibrary(Path path) {
        if (!Files.exists(path)) {
            throw new IllegalStateException(
                "Native library not found at " + path
            );
        }
        return path;
    }

    private static Path extractClasspathLibrary() {
        String mappedLibraryName = System.mapLibraryName(LIBRARY_BASE_NAME);
        String resourcePath =
            CLASSPATH_LIBRARY_ROOT +
            "/" +
            platformIdentifier() +
            "/" +
            mappedLibraryName;

        try (
            var libraryStream = RingloomNative.class.getResourceAsStream(
                resourcePath
            )
        ) {
            if (libraryStream == null) {
                throw new IllegalStateException(
                    "Embedded native library resource not found at " +
                        resourcePath +
                        "; set ringloom.nativeLibPath or ringloom.nativeLibDir to load an external build"
                );
            }

            Path tempDir = Files.createTempDirectory("ringloom-native-");
            Path extractedLibrary = tempDir.resolve(mappedLibraryName);
            Files.copy(libraryStream, extractedLibrary);
            extractedLibrary.toFile().deleteOnExit();
            tempDir.toFile().deleteOnExit();
            return extractedLibrary;
        } catch (RuntimeException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new IllegalStateException(
                "Failed to extract embedded RingLoom native library",
                ex
            );
        }
    }

    private static String platformIdentifier() {
        return (
            normalizeOsName(System.getProperty("os.name")) +
            "-" +
            normalizeArchName(System.getProperty("os.arch"))
        );
    }

    private static String normalizeOsName(String osName) {
        String normalized = osName.toLowerCase();
        if (normalized.startsWith("linux")) {
            return "linux";
        }
        if (
            normalized.startsWith("mac os") || normalized.startsWith("darwin")
        ) {
            return "macos";
        }
        throw new IllegalStateException(
            "Unsupported operating system for embedded RingLoom native library: " +
                osName
        );
    }

    private static String normalizeArchName(String archName) {
        return switch (archName.toLowerCase()) {
            case "x86_64", "amd64" -> "x86_64";
            case "aarch64", "arm64" -> "aarch64";
            default -> throw new IllegalStateException(
                "Unsupported architecture for embedded RingLoom native library: " +
                    archName
            );
        };
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

    static int serviceAeronDirectory(
        MemorySegment service,
        MemorySegment outDirectory,
        MemorySegment outDirectoryLen
    ) {
        try {
            return (int) SERVICE_AERON_DIRECTORY_HANDLE.invokeExact(
                service,
                outDirectory,
                outDirectoryLen
            );
        } catch (Throwable throwable) {
            throw propagate("ringloom_service_aeron_directory", throwable);
        }
    }

    static int serviceAeronInboundStreamId(
        MemorySegment service,
        MemorySegment outStreamId
    ) {
        try {
            return (int) SERVICE_AERON_INBOUND_STREAM_ID_HANDLE.invokeExact(
                service,
                outStreamId
            );
        } catch (Throwable throwable) {
            throw propagate(
                "ringloom_service_aeron_inbound_stream_id",
                throwable
            );
        }
    }

    static int servicePublicationConnected(
        MemorySegment service,
        MemorySegment outConnected
    ) {
        try {
            return (int) SERVICE_PUBLICATION_CONNECTED_HANDLE.invokeExact(
                service,
                outConnected
            );
        } catch (Throwable throwable) {
            throw propagate(
                "ringloom_service_publication_connected",
                throwable
            );
        }
    }

    static int servicePollControl(
        MemorySegment service,
        int limit,
        MemorySegment outCount
    ) {
        try {
            return (int) SERVICE_POLL_CONTROL_HANDLE.invokeExact(
                service,
                limit,
                outCount
            );
        } catch (Throwable throwable) {
            throw propagate("ringloom_service_poll_control", throwable);
        }
    }

    static int createMessageConsumer(
        MemorySegment service,
        MemorySegment outConsumer
    ) {
        try {
            return (int) CREATE_CONSUMER_HANDLE.invokeExact(
                service,
                outConsumer
            );
        } catch (Throwable throwable) {
            throw propagate(
                "ringloom_service_create_message_consumer",
                throwable
            );
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
            return (int) POLL_CONSUMER_HANDLE.invokeExact(
                consumer,
                handler,
                userData,
                limit,
                outCount
            );
        } catch (Throwable throwable) {
            throw propagate("ringloom_message_consumer_poll", throwable);
        }
    }

    static int createMetricsReader(
        MemorySegment service,
        MemorySegment outReader
    ) {
        try {
            return (int) CREATE_METRICS_READER_HANDLE.invokeExact(
                service,
                outReader
            );
        } catch (Throwable throwable) {
            throw propagate(
                "ringloom_service_create_metrics_reader",
                throwable
            );
        }
    }

    static void destroyMetricsReader(MemorySegment reader) {
        try {
            DESTROY_METRICS_READER_HANDLE.invokeExact(reader);
        } catch (Throwable throwable) {
            throw propagate("ringloom_metrics_reader_destroy", throwable);
        }
    }

    static int metricsCounterCount(
        MemorySegment reader,
        MemorySegment outCount
    ) {
        try {
            return (int) METRICS_COUNTER_COUNT_HANDLE.invokeExact(
                reader,
                outCount
            );
        } catch (Throwable throwable) {
            throw propagate("ringloom_metrics_reader_counter_count", throwable);
        }
    }

    static int metricsCounterAt(
        MemorySegment reader,
        long index,
        MemorySegment outMetric
    ) {
        try {
            return (int) METRICS_COUNTER_AT_HANDLE.invokeExact(
                reader,
                index,
                outMetric
            );
        } catch (Throwable throwable) {
            throw propagate("ringloom_metrics_reader_counter_at", throwable);
        }
    }

    static int metricsRingStats(
        MemorySegment reader,
        MemorySegment ringName,
        long ringNameLen,
        MemorySegment outStats
    ) {
        try {
            return (int) METRICS_RING_STATS_HANDLE.invokeExact(
                reader,
                ringName,
                ringNameLen,
                outStats
            );
        } catch (Throwable throwable) {
            throw propagate("ringloom_metrics_reader_ring_stats", throwable);
        }
    }

    static int serviceCounterRegister(
        MemorySegment service,
        MemorySegment name,
        long nameLen,
        MemorySegment outSlot
    ) {
        try {
            return (int) SERVICE_COUNTER_REGISTER_HANDLE.invokeExact(
                service,
                name,
                nameLen,
                outSlot
            );
        } catch (Throwable throwable) {
            throw propagate("ringloom_service_counter_register", throwable);
        }
    }

    static int serviceGaugeRegister(
        MemorySegment service,
        MemorySegment name,
        long nameLen,
        MemorySegment outSlot
    ) {
        try {
            return (int) SERVICE_GAUGE_REGISTER_HANDLE.invokeExact(
                service,
                name,
                nameLen,
                outSlot
            );
        } catch (Throwable throwable) {
            throw propagate("ringloom_service_gauge_register", throwable);
        }
    }

    static int createClient(
        MemorySegment service,
        MemorySegment serviceName,
        long serviceNameLen,
        MemorySegment outClient
    ) {
        try {
            return (int) CREATE_CLIENT_HANDLE.invokeExact(
                service,
                serviceName,
                serviceNameLen,
                outClient
            );
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

    static int clientSetLifecycleHandler(
        MemorySegment client,
        MemorySegment handler,
        MemorySegment userData
    ) {
        try {
            return (int) CLIENT_SET_LIFECYCLE_HANDLE.invokeExact(
                client,
                handler,
                userData
            );
        } catch (Throwable throwable) {
            throw propagate("ringloom_client_set_lifecycle_handler", throwable);
        }
    }

    static int clientSend(
        MemorySegment client,
        MemorySegment payload,
        long payloadLen
    ) {
        try {
            return (int) CLIENT_SEND_HANDLE.invokeExact(
                client,
                payload,
                payloadLen
            );
        } catch (Throwable throwable) {
            throw propagate("ringloom_client_send", throwable);
        }
    }

    static int clientSendMessage(
        MemorySegment client,
        short templateId,
        MemorySegment payload,
        long payloadLen
    ) {
        try {
            return (int) CLIENT_SEND_MESSAGE_HANDLE.invokeExact(
                client,
                templateId,
                payload,
                payloadLen
            );
        } catch (Throwable throwable) {
            throw propagate("ringloom_client_send_message", throwable);
        }
    }

    static int clientSendMessageRequest(
        MemorySegment client,
        short templateId,
        long correlationId,
        MemorySegment payload,
        long payloadLen
    ) {
        try {
            return (int) CLIENT_SEND_MESSAGE_REQUEST_HANDLE.invokeExact(
                client,
                templateId,
                correlationId,
                payload,
                payloadLen
            );
        } catch (Throwable throwable) {
            throw propagate("ringloom_client_send_message_request", throwable);
        }
    }

    static int clientSendTo(
        MemorySegment client,
        short targetNodeId,
        int targetServiceId,
        MemorySegment payload,
        long payloadLen
    ) {
        try {
            return (int) CLIENT_SEND_TO_HANDLE.invokeExact(
                client,
                targetNodeId,
                targetServiceId,
                payload,
                payloadLen
            );
        } catch (Throwable throwable) {
            throw propagate("ringloom_client_send_to", throwable);
        }
    }

    static int clientSendToMessage(
        MemorySegment client,
        short targetNodeId,
        int targetServiceId,
        short templateId,
        MemorySegment payload,
        long payloadLen
    ) {
        try {
            return (int) CLIENT_SEND_TO_MESSAGE_HANDLE.invokeExact(
                client,
                targetNodeId,
                targetServiceId,
                templateId,
                payload,
                payloadLen
            );
        } catch (Throwable throwable) {
            throw propagate("ringloom_client_send_to_message", throwable);
        }
    }

    static int clientSendToMessageRequest(
        MemorySegment client,
        short targetNodeId,
        int targetServiceId,
        short templateId,
        long correlationId,
        MemorySegment payload,
        long payloadLen
    ) {
        try {
            return (int) CLIENT_SEND_TO_MESSAGE_REQUEST_HANDLE.invokeExact(
                client,
                targetNodeId,
                targetServiceId,
                templateId,
                correlationId,
                payload,
                payloadLen
            );
        } catch (Throwable throwable) {
            throw propagate(
                "ringloom_client_send_to_message_request",
                throwable
            );
        }
    }

    static int clientSendToLeader(
        MemorySegment client,
        MemorySegment payload,
        long payloadLen
    ) {
        try {
            return (int) CLIENT_SEND_TO_LEADER_HANDLE.invokeExact(
                client,
                payload,
                payloadLen
            );
        } catch (Throwable throwable) {
            throw propagate("ringloom_client_send_to_leader", throwable);
        }
    }

    static int clientSendToLeaderMessage(
        MemorySegment client,
        short templateId,
        MemorySegment payload,
        long payloadLen
    ) {
        try {
            return (int) CLIENT_SEND_TO_LEADER_MESSAGE_HANDLE.invokeExact(
                client,
                templateId,
                payload,
                payloadLen
            );
        } catch (Throwable throwable) {
            throw propagate(
                "ringloom_client_send_to_leader_message",
                throwable
            );
        }
    }

    static int clientSendToLeaderMessageRequest(
        MemorySegment client,
        short templateId,
        long correlationId,
        MemorySegment payload,
        long payloadLen
    ) {
        try {
            return (int) CLIENT_SEND_TO_LEADER_MESSAGE_REQUEST_HANDLE.invokeExact(
                client,
                templateId,
                correlationId,
                payload,
                payloadLen
            );
        } catch (Throwable throwable) {
            throw propagate(
                "ringloom_client_send_to_leader_message_request",
                throwable
            );
        }
    }

    static int clientListTargets(
        MemorySegment client,
        MemorySegment outTargets,
        long targetCapacity,
        MemorySegment outCount
    ) {
        try {
            return (int) CLIENT_LIST_TARGETS_HANDLE.invokeExact(
                client,
                outTargets,
                targetCapacity,
                outCount
            );
        } catch (Throwable throwable) {
            throw propagate("ringloom_client_list_targets", throwable);
        }
    }

    static int clientTryClaim(
        MemorySegment client,
        short templateId,
        long payloadLen,
        MemorySegment outClaim
    ) {
        try {
            return (int) CLIENT_TRY_CLAIM_HANDLE.invokeExact(
                client,
                templateId,
                payloadLen,
                outClaim
            );
        } catch (Throwable throwable) {
            throw propagate("ringloom_client_try_claim", throwable);
        }
    }

    static int clientTryClaimRequest(
        MemorySegment client,
        short templateId,
        long correlationId,
        long payloadLen,
        MemorySegment outClaim
    ) {
        try {
            return (int) CLIENT_TRY_CLAIM_REQUEST_HANDLE.invokeExact(
                client,
                templateId,
                correlationId,
                payloadLen,
                outClaim
            );
        } catch (Throwable throwable) {
            throw propagate("ringloom_client_try_claim_request", throwable);
        }
    }

    static int clientTryClaimTo(
        MemorySegment client,
        short targetNodeId,
        int targetServiceId,
        short templateId,
        long payloadLen,
        MemorySegment outClaim
    ) {
        try {
            return (int) CLIENT_TRY_CLAIM_TO_HANDLE.invokeExact(
                client,
                targetNodeId,
                targetServiceId,
                templateId,
                payloadLen,
                outClaim
            );
        } catch (Throwable throwable) {
            throw propagate("ringloom_client_try_claim_to", throwable);
        }
    }

    static int clientTryClaimToRequest(
        MemorySegment client,
        short targetNodeId,
        int targetServiceId,
        short templateId,
        long correlationId,
        long payloadLen,
        MemorySegment outClaim
    ) {
        try {
            return (int) CLIENT_TRY_CLAIM_TO_REQUEST_HANDLE.invokeExact(
                client,
                targetNodeId,
                targetServiceId,
                templateId,
                correlationId,
                payloadLen,
                outClaim
            );
        } catch (Throwable throwable) {
            throw propagate("ringloom_client_try_claim_to_request", throwable);
        }
    }

    static int clientTryClaimToLeader(
        MemorySegment client,
        short templateId,
        long payloadLen,
        MemorySegment outClaim
    ) {
        try {
            return (int) CLIENT_TRY_CLAIM_TO_LEADER_HANDLE.invokeExact(
                client,
                templateId,
                payloadLen,
                outClaim
            );
        } catch (Throwable throwable) {
            throw propagate("ringloom_client_try_claim_to_leader", throwable);
        }
    }

    static int clientTryClaimToLeaderRequest(
        MemorySegment client,
        short templateId,
        long correlationId,
        long payloadLen,
        MemorySegment outClaim
    ) {
        try {
            return (int) CLIENT_TRY_CLAIM_TO_LEADER_REQUEST_HANDLE.invokeExact(
                client,
                templateId,
                correlationId,
                payloadLen,
                outClaim
            );
        } catch (Throwable throwable) {
            throw propagate(
                "ringloom_client_try_claim_to_leader_request",
                throwable
            );
        }
    }

    static int clientLastAeronSendStatus(
        MemorySegment client,
        MemorySegment outStatus
    ) {
        try {
            return (int) CLIENT_LAST_AERON_SEND_STATUS_HANDLE.invokeExact(
                client,
                outStatus
            );
        } catch (Throwable throwable) {
            throw propagate(
                "ringloom_client_last_aeron_send_status",
                throwable
            );
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
            return readCString(
                (MemorySegment) STATUS_STRING_HANDLE.invokeExact(status),
                128
            );
        } catch (Throwable throwable) {
            throw propagate("ringloom_status_string", throwable);
        }
    }

    static String aeronPublicationStatusName(int status) {
        try {
            return readCString(
                (MemorySegment) AERON_PUBLICATION_STATUS_STRING_HANDLE.invokeExact(
                    status
                ),
                128
            );
        } catch (Throwable throwable) {
            throw propagate(
                "ringloom_aeron_publication_status_string",
                throwable
            );
        }
    }

    static String lastErrorMessage() {
        try {
            return readCString(
                (MemorySegment) LAST_ERROR_MESSAGE_HANDLE.invokeExact(),
                8192
            );
        } catch (Throwable throwable) {
            throw propagate("ringloom_last_error_message", throwable);
        }
    }

    // ── Topic native methods ───────────────────────────────────────────

    static int topicRegisterPublication(
        MemorySegment client,
        MemorySegment cfg,
        MemorySegment name,
        long nameLen,
        MemorySegment outPublisher
    ) {
        try {
            return (int) TOPIC_REGISTER_PUBLICATION_HANDLE.invokeExact(
                client,
                cfg,
                name,
                nameLen,
                outPublisher
            );
        } catch (Throwable throwable) {
            throw propagate("ringloom_register_topic_publication", throwable);
        }
    }

    static int topicPublish(
        MemorySegment publisher,
        MemorySegment payload,
        long payloadLen,
        long correlationId,
        int ackMode,
        MemorySegment outIndex
    ) {
        try {
            return (int) TOPIC_PUBLISH_HANDLE.invokeExact(
                publisher,
                payload,
                payloadLen,
                correlationId,
                (byte) ackMode,
                outIndex
            );
        } catch (Throwable throwable) {
            throw propagate("ringloom_publish_to_topic", throwable);
        }
    }

    static int topicIsAcked(MemorySegment publisher, long publishIndex) {
        try {
            return (int) TOPIC_IS_ACKED_HANDLE.invokeExact(
                publisher,
                publishIndex
            );
        } catch (Throwable throwable) {
            throw propagate("ringloom_topic_is_acked", throwable);
        }
    }

    static void topicUnregisterPublication(MemorySegment publisher) {
        try {
            TOPIC_UNREGISTER_PUBLICATION_HANDLE.invokeExact(publisher);
        } catch (Throwable throwable) {
            throw propagate("ringloom_unregister_topic_publication", throwable);
        }
    }

    static int topicSubscribe(
        MemorySegment client,
        MemorySegment name,
        long nameLen,
        int start,
        MemorySegment outSubscription
    ) {
        try {
            return (int) TOPIC_SUBSCRIBE_HANDLE.invokeExact(
                client,
                name,
                nameLen,
                (byte) start,
                outSubscription
            );
        } catch (Throwable throwable) {
            throw propagate("ringloom_subscribe_topic", throwable);
        }
    }

    static int topicPoll(
        MemorySegment subscription,
        MemorySegment outPayload,
        MemorySegment outLen,
        MemorySegment outIndex
    ) {
        try {
            return (int) TOPIC_POLL_HANDLE.invokeExact(
                subscription,
                outPayload,
                outLen,
                outIndex
            );
        } catch (Throwable throwable) {
            throw propagate("ringloom_topic_poll", throwable);
        }
    }

    static void topicUnsubscribe(MemorySegment subscription) {
        try {
            TOPIC_UNSUBSCRIBE_HANDLE.invokeExact(subscription);
        } catch (Throwable throwable) {
            throw propagate("ringloom_unsubscribe_topic", throwable);
        }
    }

    static int topicSubscriptionMaintenancePoll(
        MemorySegment subscription,
        int maxWorkUnits
    ) {
        try {
            return (int) TOPIC_SUBSCRIPTION_MAINTENANCE_POLL_HANDLE.invokeExact(
                subscription,
                maxWorkUnits
            );
        } catch (Throwable throwable) {
            throw propagate(
                "ringloom_topic_subscription_maintenance_poll",
                throwable
            );
        }
    }

    static void throwForStatus(String action, int status) {
        if (status == RingloomStatus.OK || status == RingloomStatus.NOT_READY) {
            return;
        }

        String nativeMessage = lastErrorMessage();
        if (status == RingloomStatus.INVALID_ARGUMENT) {
            throw new IllegalArgumentException(
                action + " failed: " + nativeMessage
            );
        }
        if (status == RingloomStatus.OUT_OF_MEMORY) {
            throw new OutOfMemoryError(action + " failed: " + nativeMessage);
        }
        throw new RingloomException(
            status,
            statusName(status),
            nativeMessage,
            action
        );
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

    private static MethodHandle downcall(
        String symbol,
        FunctionDescriptor descriptor
    ) {
        MemorySegment address = SYMBOLS.find(symbol).orElseThrow(() ->
            new IllegalStateException("Missing native symbol " + symbol)
        );
        return LINKER.downcallHandle(address, descriptor);
    }

    private static MethodHandle optionalDowncall(
        String symbol,
        FunctionDescriptor descriptor
    ) {
        return SYMBOLS.find(symbol)
            .map(address -> LINKER.downcallHandle(address, descriptor))
            .orElse(null);
    }

    private static RuntimeException propagate(
        String action,
        Throwable throwable
    ) {
        if (throwable instanceof RuntimeException runtimeException) {
            return runtimeException;
        }
        if (throwable instanceof Error error) {
            throw error;
        }
        return new IllegalStateException(
            "Native invocation failed for " + action,
            throwable
        );
    }
}
