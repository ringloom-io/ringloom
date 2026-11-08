#ifndef RINGLOOM_SERVICE_H
#define RINGLOOM_SERVICE_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define RINGLOOM_SERVICE_ABI_VERSION 6u

typedef struct ringloom_service ringloom_service_t;
typedef struct ringloom_client ringloom_client_t;
typedef struct ringloom_message_consumer ringloom_message_consumer_t;
typedef struct ringloom_metrics_reader ringloom_metrics_reader_t;

typedef enum ringloom_status {
    RINGLOOM_OK = 0,
    RINGLOOM_ERR_INVALID_ARGUMENT = 1,
    RINGLOOM_ERR_OUT_OF_MEMORY = 2,
    RINGLOOM_ERR_BROKER_NOT_FOUND = 3,
    RINGLOOM_ERR_REGISTRATION_TIMEOUT = 4,
    RINGLOOM_ERR_BUFFER_FULL = 5,
    RINGLOOM_ERR_NO_AVAILABLE_INSTANCE = 6,
    RINGLOOM_ERR_BACKPRESSURE = 7,
    RINGLOOM_ERR_PEER_DISCONNECTED = 8,
    RINGLOOM_ERR_CLAIM_NOT_ACTIVE = 9,
    RINGLOOM_ERR_MESSAGE_TOO_LONG = 10,
    RINGLOOM_NOT_READY = 11,
    RINGLOOM_ERR_TOPIC_LEADER_UNAVAILABLE = 12,
    RINGLOOM_ERR_TOPIC_DISABLED = 13,
    RINGLOOM_ERR_TOPIC_CONFIG_MISMATCH = 14,
    RINGLOOM_ERR_TOPIC_UNKNOWN = 15,
    RINGLOOM_KV_MISSING = 32,
    RINGLOOM_ERR_KV_BUFFER_TOO_SMALL = 33,
    RINGLOOM_ERR_KV_NOT_WRITER = 34,
    RINGLOOM_ERR_KV_HELD_BY_OTHER = 35,
    RINGLOOM_ERR_KV_NOT_AVAILABLE_LOCALLY = 36,
    RINGLOOM_ERR_KV_ENGINE_MISMATCH = 37,
    RINGLOOM_ERR_KV_KEY_TOO_LARGE = 38,
    RINGLOOM_ERR_KV_VALUE_TOO_LARGE = 39,
    RINGLOOM_ERR_KV_BATCH_FULL = 40,
    RINGLOOM_ERR_INTERNAL = 255
} ringloom_status_t;

typedef struct ringloom_service_config {
    const char *storage_path;
    size_t storage_path_len;
    const char *group;
    size_t group_len;
    const char *service_name;
    size_t service_name_len;
    int16_t broker_node_id;
    bool blocking_mode;
    int32_t heartbeat_timeout_ms;
    size_t control_buffer_length;
    size_t messages_buffer_length;
    bool leader_election_enabled;
} ringloom_service_config_t;

typedef struct ringloom_message {
    int64_t correlation_id;
    int16_t source_node_id;
    int16_t source_service_id;
    int16_t target_node_id;
    int16_t target_service_id;
    uint16_t template_id;
    uint8_t flags;
    const uint8_t *payload;
    size_t payload_len;
} ringloom_message_t;

typedef struct ringloom_buffer_claim {
    uint8_t *payload;
    size_t payload_len;
    uintptr_t _ring_buffer;
    size_t _header_index;
    int32_t _record_length;
    uint8_t _active;
} ringloom_buffer_claim_t;

typedef struct ringloom_client_target {
    int32_t target_service_id;
    int16_t target_node_id;
    bool is_leader;
} ringloom_client_target_t;

typedef enum ringloom_service_lifecycle_event_type {
    RINGLOOM_SERVICE_AVAILABLE = 1,
    RINGLOOM_SERVICE_UNAVAILABLE = 2
} ringloom_service_lifecycle_event_type_t;

typedef struct ringloom_service_lifecycle_event {
    ringloom_service_lifecycle_event_type_t event_type;
    int32_t service_id;
    int16_t node_id;
    bool is_leader;
    const char *service_name;
    size_t service_name_len;
} ringloom_service_lifecycle_event_t;

typedef enum ringloom_metric_kind {
    RINGLOOM_METRIC_COUNTER = 1,
    RINGLOOM_METRIC_GAUGE = 2
} ringloom_metric_kind_t;

typedef struct ringloom_metric_descriptor {
    const char *name;
    size_t name_len;
    ringloom_metric_kind_t kind;
    int64_t value;
} ringloom_metric_descriptor_t;

typedef struct ringloom_metric_slot {
    int32_t metric_id;
    uint32_t reserved;
    int64_t *value;
    size_t value_len;
} ringloom_metric_slot_t;

typedef struct ringloom_ring_stats {
    uint64_t capacity_bytes;
    uint64_t used_bytes;
    uint64_t free_bytes;
    uint64_t producer_position;
    uint64_t consumer_position;
} ringloom_ring_stats_t;

typedef enum ringloom_aeron_publication_status {
    RINGLOOM_AERON_PUBLICATION_UNKNOWN = 0,
    RINGLOOM_AERON_PUBLICATION_CLAIMED = 1,
    RINGLOOM_AERON_PUBLICATION_NOT_CONNECTED = 2,
    RINGLOOM_AERON_PUBLICATION_BACK_PRESSURED = 3,
    RINGLOOM_AERON_PUBLICATION_ADMIN_ACTION = 4,
    RINGLOOM_AERON_PUBLICATION_CLOSED = 5,
    RINGLOOM_AERON_PUBLICATION_MAX_POSITION_EXCEEDED = 6,
    RINGLOOM_AERON_PUBLICATION_FAILED = 7
} ringloom_aeron_publication_status_t;

typedef void (*ringloom_message_handler_t)(
    void *user_data,
    const ringloom_message_t *message
);

typedef void (*ringloom_service_lifecycle_handler_t)(
    void *user_data,
    const ringloom_service_lifecycle_event_t *event
);

uint32_t ringloom_service_abi_version(void);

ringloom_status_t ringloom_service_start(
    const ringloom_service_config_t *config,
    ringloom_service_t **out_service
);

void ringloom_service_stop(ringloom_service_t *service);
void ringloom_service_destroy(ringloom_service_t *service);

ringloom_status_t ringloom_service_id(
    const ringloom_service_t *service,
    int32_t *out_service_id
);

ringloom_status_t ringloom_service_node_id(
    const ringloom_service_t *service,
    int16_t *out_node_id
);

ringloom_status_t ringloom_service_aeron_directory(
    const ringloom_service_t *service,
    const char **out_directory,
    size_t *out_directory_len
);

ringloom_status_t ringloom_service_aeron_inbound_stream_id(
    const ringloom_service_t *service,
    int32_t *out_stream_id
);

ringloom_status_t ringloom_service_publication_connected(
    const ringloom_service_t *service,
    bool *out_connected
);

ringloom_status_t ringloom_service_poll_control(
    ringloom_service_t *service,
    uint32_t limit,
    uint32_t *out_count
);

ringloom_status_t ringloom_service_create_message_consumer(
    ringloom_service_t *service,
    ringloom_message_consumer_t **out_consumer
);

void ringloom_message_consumer_destroy(ringloom_message_consumer_t *consumer);

ringloom_status_t ringloom_message_consumer_poll(
    ringloom_message_consumer_t *consumer,
    ringloom_message_handler_t handler,
    void *user_data,
    uint32_t limit,
    uint32_t *out_count
);

ringloom_status_t ringloom_service_create_metrics_reader(
    ringloom_service_t *service,
    ringloom_metrics_reader_t **out_reader
);

void ringloom_metrics_reader_destroy(ringloom_metrics_reader_t *reader);

ringloom_status_t ringloom_metrics_reader_counter_count(
    ringloom_metrics_reader_t *reader,
    size_t *out_count
);

ringloom_status_t ringloom_metrics_reader_counter_at(
    ringloom_metrics_reader_t *reader,
    size_t index,
    ringloom_metric_descriptor_t *out_metric
);

ringloom_status_t ringloom_metrics_reader_ring_stats(
    ringloom_metrics_reader_t *reader,
    const char *ring_name,
    size_t ring_name_len,
    ringloom_ring_stats_t *out_stats
);

ringloom_status_t ringloom_service_counter_register(
    ringloom_service_t *service,
    const char *name,
    size_t name_len,
    ringloom_metric_slot_t *out_slot
);

ringloom_status_t ringloom_service_gauge_register(
    ringloom_service_t *service,
    const char *name,
    size_t name_len,
    ringloom_metric_slot_t *out_slot
);

ringloom_status_t ringloom_service_create_client(
    ringloom_service_t *service,
    const char *target_service_name,
    size_t target_service_name_len,
    ringloom_client_t **out_client
);

void ringloom_client_destroy(ringloom_client_t *client);

ringloom_status_t ringloom_client_set_lifecycle_handler(
    ringloom_client_t *client,
    ringloom_service_lifecycle_handler_t handler,
    void *user_data
);

ringloom_status_t ringloom_client_send(
    ringloom_client_t *client,
    const uint8_t *payload,
    size_t payload_len
);

ringloom_status_t ringloom_client_send_message(
    ringloom_client_t *client,
    uint16_t template_id,
    const uint8_t *payload,
    size_t payload_len
);

ringloom_status_t ringloom_client_send_message_request(
    ringloom_client_t *client,
    uint16_t template_id,
    int64_t correlation_id,
    const uint8_t *payload,
    size_t payload_len
);

ringloom_status_t ringloom_client_try_claim(
    ringloom_client_t *client,
    uint16_t template_id,
    size_t payload_len,
    ringloom_buffer_claim_t *out_claim
);

ringloom_status_t ringloom_client_try_claim_request(
    ringloom_client_t *client,
    uint16_t template_id,
    int64_t correlation_id,
    size_t payload_len,
    ringloom_buffer_claim_t *out_claim
);

ringloom_status_t ringloom_client_try_claim_to(
    ringloom_client_t *client,
    int16_t target_node_id,
    int32_t target_service_id,
    uint16_t template_id,
    size_t payload_len,
    ringloom_buffer_claim_t *out_claim
);

ringloom_status_t ringloom_client_try_claim_to_request(
    ringloom_client_t *client,
    int16_t target_node_id,
    int32_t target_service_id,
    uint16_t template_id,
    int64_t correlation_id,
    size_t payload_len,
    ringloom_buffer_claim_t *out_claim
);

ringloom_status_t ringloom_client_try_claim_to_leader(
    ringloom_client_t *client,
    uint16_t template_id,
    size_t payload_len,
    ringloom_buffer_claim_t *out_claim
);

ringloom_status_t ringloom_client_try_claim_to_leader_request(
    ringloom_client_t *client,
    uint16_t template_id,
    int64_t correlation_id,
    size_t payload_len,
    ringloom_buffer_claim_t *out_claim
);

ringloom_status_t ringloom_buffer_claim_commit(
    ringloom_buffer_claim_t *claim
);

ringloom_status_t ringloom_buffer_claim_abort(
    ringloom_buffer_claim_t *claim
);

ringloom_status_t ringloom_client_send_to(
    ringloom_client_t *client,
    int16_t target_node_id,
    int32_t target_service_id,
    const uint8_t *payload,
    size_t payload_len
);

ringloom_status_t ringloom_client_send_to_message(
    ringloom_client_t *client,
    int16_t target_node_id,
    int32_t target_service_id,
    uint16_t template_id,
    const uint8_t *payload,
    size_t payload_len
);

ringloom_status_t ringloom_client_send_to_message_request(
    ringloom_client_t *client,
    int16_t target_node_id,
    int32_t target_service_id,
    uint16_t template_id,
    int64_t correlation_id,
    const uint8_t *payload,
    size_t payload_len
);

ringloom_status_t ringloom_client_send_to_leader(
    ringloom_client_t *client,
    const uint8_t *payload,
    size_t payload_len
);

ringloom_status_t ringloom_client_send_to_leader_message(
    ringloom_client_t *client,
    uint16_t template_id,
    const uint8_t *payload,
    size_t payload_len
);

ringloom_status_t ringloom_client_send_to_leader_message_request(
    ringloom_client_t *client,
    uint16_t template_id,
    int64_t correlation_id,
    const uint8_t *payload,
    size_t payload_len
);

ringloom_status_t ringloom_client_list_targets(
    ringloom_client_t *client,
    ringloom_client_target_t *out_targets,
    size_t target_capacity,
    size_t *out_count
);

ringloom_status_t ringloom_client_last_aeron_send_status(
    ringloom_client_t *client,
    ringloom_aeron_publication_status_t *out_status
);

const char *ringloom_status_string(ringloom_status_t status);
const char *ringloom_aeron_publication_status_string(ringloom_aeron_publication_status_t status);
const char *ringloom_last_error_message(void);

// ── Persistent topics ────────────────────────────────────────────────

typedef struct ringloom_topic_publisher ringloom_topic_publisher_t;
typedef struct ringloom_topic_subscription ringloom_topic_subscription_t;

typedef struct ringloom_topic_config {
    uint32_t size;                      // sizeof(ringloom_topic_config_t)
    char roll_scheme[16];
    uint32_t retention_cycles;
    uint32_t flags;
} ringloom_topic_config_t;

typedef enum ringloom_topic_start {
    RINGLOOM_TOPIC_START_EARLIEST = 0,
    RINGLOOM_TOPIC_START_LATEST = 1
} ringloom_topic_start_t;

ringloom_status_t ringloom_register_topic_publication(
    ringloom_client_t *client,
    const ringloom_topic_config_t *cfg,
    const char *name,
    size_t name_len,
    ringloom_topic_publisher_t **out_publisher
);

/// ack_mode: 0 = fire_and_forget, 1 = replicate_once.
/// out_index receives the assigned publish sequence (for replicate_once).
ringloom_status_t ringloom_publish_to_topic(
    ringloom_topic_publisher_t *publisher,
    const uint8_t *payload,
    size_t payload_len,
    int64_t correlation_id,
    uint8_t ack_mode,
    uint64_t *out_index
);

/// Non-blocking check: returns 1 if acked, 0 if pending.
int ringloom_topic_is_acked(
    ringloom_topic_publisher_t *publisher,
    uint64_t publish_index
);

void ringloom_unregister_topic_publication(
    ringloom_topic_publisher_t *publisher
);

ringloom_status_t ringloom_subscribe_topic(
    ringloom_client_t *client,
    const char *name,
    size_t name_len,
    ringloom_topic_start_t start,
    ringloom_topic_subscription_t **out_subscription
);

/// Poll for the next message. out_payload is borrowed (valid until next poll).
ringloom_status_t ringloom_topic_poll(
    ringloom_topic_subscription_t *subscription,
    const uint8_t **out_payload,
    size_t *out_len,
    int64_t *out_index
);

/// Advances the subscription's ringloom-queue maintenance/cleaner work and
/// pre-touches read pages ahead of the tailer. Safe to call from any thread that
/// does not poll the same subscription concurrently. max_work_units <= 0 is a no-op.
ringloom_status_t ringloom_topic_subscription_maintenance_poll(
    ringloom_topic_subscription_t *subscription,
    int max_work_units
);

void ringloom_unsubscribe_topic(
    ringloom_topic_subscription_t *subscription
);

// ── Topic accessors (for producer ack tracking / dispatch keying) ──────

// Returns the broker-assigned topic_id for a publisher.
uint64_t ringloom_topic_publisher_id(const ringloom_topic_publisher_t *publisher);

// Returns the publisher's current leader epoch (advances on leader change).
uint64_t ringloom_topic_publisher_leader_epoch(const ringloom_topic_publisher_t *publisher);

// Returns this publisher's highest sequence token acknowledged as replicated
// to >=1 replica (or appended, single-node). Every token <= this value is
// acked; publishes from other publishers on the same topic do not count.
uint64_t ringloom_topic_publisher_replicated_count(const ringloom_topic_publisher_t *publisher);

// Returns the broker-assigned topic_id for a subscription.
uint64_t ringloom_topic_subscription_id(const ringloom_topic_subscription_t *subscription);


/* KV ABI 6. Initialize all option/status size fields to sizeof(struct).
 * NULL reader/writer options select defaults. Config is required.
 * Sessions belong to one thread. Free sessions before readers; close writers
 * and readers before destroying the service. A view pins a snapshot until
 * release; get/view/iteration reuse that snapshot while it is held.
 * Callbacks must not re-enter the KV API or close their handles. A visit
 * callback returns nonzero to continue and zero to stop. Iteration returns OK
 * for both completion and an explicit callback stop.
 * Poll ringloom_service_poll_control regularly for heartbeat, grant/reassert,
 * acknowledgements and transport maintenance (the C service uses manual control).
 * ack_mode: 0=fire_and_forget, 1=replicate_once. Buffered writes return seq=0.
 * Missing and buffer-too-small are distinct from an empty value (OK, len=0).
 */
typedef struct ringloom_kv_reader ringloom_kv_reader_t;
typedef struct ringloom_kv_session ringloom_kv_session_t;
typedef struct ringloom_kv_writer ringloom_kv_writer_t;
typedef enum ringloom_kv_writer_state {
    RINGLOOM_KV_GRANTED=0, RINGLOOM_KV_QUEUED=1,
    RINGLOOM_KV_REVOKED=2, RINGLOOM_KV_REASSERTING=3
} ringloom_kv_writer_state_t;
typedef void (*ringloom_kv_state_fn)(void*, uint8_t state, uint64_t writer_epoch);
typedef int (*ringloom_kv_visit_fn)(void*, const uint8_t*, size_t, const uint8_t*, size_t);
typedef struct ringloom_kv_store_config {
    uint32_t size;
    uint16_t format_version, _pad0;
    uint16_t max_key_len, _pad1;
    uint32_t max_value_len;
    uint64_t default_ttl_ms, map_size_hint;
    char topic_roll_scheme[16];
    uint32_t flags;
    uint8_t _reserved[28];
} ringloom_kv_store_config_t;
typedef struct ringloom_kv_reader_options {
    uint32_t size, wait_ready_timeout_ms;
    uint8_t prefault, ttl_clock; /* 0=precise, 1=coarse */
    uint8_t _pad[2];
    uint32_t max_readers;
} ringloom_kv_reader_options_t;
typedef struct ringloom_kv_writer_options {
    uint32_t size;
    uint8_t acquire; /* 0=fail_if_held, 1=wait */
    uint8_t _pad[3];
    uint32_t open_timeout_ms, max_frame_length;
    ringloom_kv_state_fn on_state;
    void *user_data;
} ringloom_kv_writer_options_t;
typedef struct ringloom_kv_status {
    uint32_t size;
    uint8_t state, replication_live; /* state: 0=initializing,1=catching_up,2=live,3=rebuilding,4=failed */
    uint8_t _pad[2];
    uint64_t generation, applied_index, applied_leader_epoch, applied_at_ns;
    uint64_t local_log_tip_index, lag_entries, heartbeat_age_ms, generation_switches;
} ringloom_kv_status_t;
/* Set config defaults then customize limits as needed. The entire largest
 * record plus KV and topic headers must fit max_frame_length (default 65536).
 * Default max_value_len=64929 leaves room for a 511-byte key and padding. */
void ringloom_kv_store_config_init(ringloom_kv_store_config_t*);
void ringloom_kv_reader_options_init(ringloom_kv_reader_options_t*);
void ringloom_kv_writer_options_init(ringloom_kv_writer_options_t*);
int ringloom_kv_open_reader(ringloom_client_t*, const char*, const ringloom_kv_reader_options_t*, ringloom_kv_reader_t**);
int ringloom_kv_reader_status(const ringloom_kv_reader_t*, ringloom_kv_status_t*);
int ringloom_kv_session_new(ringloom_kv_reader_t*, ringloom_kv_session_t**);
int ringloom_kv_get(ringloom_kv_session_t*, const uint8_t*, size_t, uint8_t*, size_t, size_t*);
int ringloom_kv_view(ringloom_kv_session_t*, const uint8_t*, size_t, const uint8_t**, size_t*);
void ringloom_kv_release(ringloom_kv_session_t*);
int ringloom_kv_for_each(ringloom_kv_session_t*, ringloom_kv_visit_fn, void*);
void ringloom_kv_session_free(ringloom_kv_session_t*);
void ringloom_kv_close_reader(ringloom_kv_reader_t*);
int ringloom_kv_open_writer(ringloom_client_t*, const char*, const ringloom_kv_store_config_t*, const ringloom_kv_writer_options_t*, ringloom_kv_writer_t**);
int ringloom_kv_put(ringloom_kv_writer_t*, const uint8_t*, size_t, const uint8_t*, size_t, int64_t ttl_ms, uint8_t ack_mode, uint64_t*);
int ringloom_kv_delete(ringloom_kv_writer_t*, const uint8_t*, size_t, uint8_t ack_mode, uint64_t*);
int ringloom_kv_clear(ringloom_kv_writer_t*, uint8_t ack_mode, uint64_t*);
int ringloom_kv_begin_batch(ringloom_kv_writer_t*);
int ringloom_kv_commit_batch(ringloom_kv_writer_t*, uint8_t ack_mode, uint64_t*);
int ringloom_kv_is_acked(ringloom_kv_writer_t*, uint64_t);
int ringloom_kv_writer_state(const ringloom_kv_writer_t*);
void ringloom_kv_close_writer(ringloom_kv_writer_t*);

#ifdef __cplusplus
}
#endif

#endif
