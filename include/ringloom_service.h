#ifndef RINGLOOM_SERVICE_H
#define RINGLOOM_SERVICE_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define RINGLOOM_SERVICE_ABI_VERSION 4u

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

#ifdef __cplusplus
}
#endif

#endif
