/* SPDX-License-Identifier: Apache-2.0 */
#ifndef RINGLOOM_AERON_ZIG_API_H
#define RINGLOOM_AERON_ZIG_API_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#define AERON_PUBLICATION_NOT_CONNECTED (-1L)
#define AERON_PUBLICATION_BACK_PRESSURED (-2L)
#define AERON_PUBLICATION_ADMIN_ACTION (-3L)
#define AERON_PUBLICATION_CLOSED (-4L)
#define AERON_PUBLICATION_MAX_POSITION_EXCEEDED (-5L)

typedef enum aeron_threading_mode_enum
{
    AERON_THREADING_MODE_DEDICATED = 0,
    AERON_THREADING_MODE_SHARED_NETWORK = 1,
    AERON_THREADING_MODE_SHARED = 2,
    AERON_THREADING_MODE_INVOKER = 3
}
aeron_threading_mode_t;

typedef struct aeron_context_stct aeron_context_t;
typedef struct aeron_stct aeron_t;
typedef struct aeron_driver_context_stct aeron_driver_context_t;
typedef struct aeron_driver_stct aeron_driver_t;
typedef struct aeron_publication_stct aeron_publication_t;
typedef struct aeron_exclusive_publication_stct aeron_exclusive_publication_t;
typedef struct aeron_subscription_stct aeron_subscription_t;
typedef struct aeron_header_stct aeron_header_t;
typedef struct aeron_fragment_assembler_stct aeron_fragment_assembler_t;
typedef struct aeron_controlled_fragment_assembler_stct aeron_controlled_fragment_assembler_t;
typedef struct aeron_client_registering_resource_stct aeron_async_add_publication_t;
typedef struct aeron_client_registering_resource_stct aeron_async_add_exclusive_publication_t;
typedef struct aeron_client_registering_resource_stct aeron_async_add_subscription_t;

typedef int64_t (*aeron_reserved_value_supplier_t)(void *clientd, uint8_t *buffer, size_t frame_length);
typedef void (*aeron_fragment_handler_t)(void *clientd, const uint8_t *buffer, size_t length, aeron_header_t *header);
typedef void (*aeron_notification_t)(void *clientd);

typedef enum aeron_controlled_fragment_handler_action_en
{
    AERON_ACTION_ABORT = 1,
    AERON_ACTION_BREAK = 2,
    AERON_ACTION_COMMIT = 3,
    AERON_ACTION_CONTINUE = 4
}
aeron_controlled_fragment_handler_action_t;

typedef aeron_controlled_fragment_handler_action_t (*aeron_controlled_fragment_handler_t)(
    void *clientd, const uint8_t *buffer, size_t length, aeron_header_t *header);

typedef struct aeron_buffer_claim_stct
{
    uint8_t *frame_header;
    uint8_t *data;
    size_t length;
}
aeron_buffer_claim_t;

int aeron_errcode(void);
const char *aeron_errmsg(void);

int aeron_driver_context_init(aeron_driver_context_t **context);
int aeron_driver_context_close(aeron_driver_context_t *context);
int aeron_driver_context_set_dir(aeron_driver_context_t *context, const char *value);
int aeron_driver_context_set_threading_mode(aeron_driver_context_t *context, aeron_threading_mode_t mode);
int aeron_driver_context_set_dir_delete_on_start(aeron_driver_context_t *context, bool value);
int aeron_driver_context_set_dir_delete_on_shutdown(aeron_driver_context_t *context, bool value);
int aeron_driver_context_set_dir_warn_if_exists(aeron_driver_context_t *context, bool value);
int aeron_driver_context_set_term_buffer_length(aeron_driver_context_t *context, size_t value);
int aeron_driver_context_set_ipc_term_buffer_length(aeron_driver_context_t *context, size_t value);
int aeron_driver_context_set_mtu_length(aeron_driver_context_t *context, size_t value);
int aeron_driver_context_set_ipc_mtu_length(aeron_driver_context_t *context, size_t value);
int aeron_driver_context_set_term_buffer_sparse_file(aeron_driver_context_t *context, bool value);
int aeron_driver_context_set_publication_linger_timeout_ns(aeron_driver_context_t *context, uint64_t value);
int aeron_driver_context_set_client_liveness_timeout_ns(aeron_driver_context_t *context, uint64_t value);
int aeron_driver_context_set_network_publication_max_messages_per_send(
    aeron_driver_context_t *context, uint32_t value);
int aeron_driver_init(aeron_driver_t **driver, aeron_driver_context_t *context);
int aeron_driver_close(aeron_driver_t *driver);

int aeron_context_init(aeron_context_t **context);
int aeron_context_close(aeron_context_t *context);
int aeron_context_set_dir(aeron_context_t *context, const char *value);
int aeron_context_set_use_conductor_agent_invoker(aeron_context_t *context, bool value);
int aeron_context_set_driver_timeout_ms(aeron_context_t *context, uint64_t value);
int aeron_init(aeron_t **client, aeron_context_t *context);
int aeron_start(aeron_t *client);
int aeron_close(aeron_t *client);
int aeron_main_do_work(aeron_t *client);

int aeron_async_add_publication(
    aeron_async_add_publication_t **async, aeron_t *client, const char *uri, int32_t stream_id);
int aeron_async_add_publication_poll(aeron_publication_t **publication, aeron_async_add_publication_t *async);
int aeron_async_add_exclusive_publication(
    aeron_async_add_exclusive_publication_t **async, aeron_t *client, const char *uri, int32_t stream_id);
int aeron_async_add_exclusive_publication_poll(
    aeron_exclusive_publication_t **publication, aeron_async_add_exclusive_publication_t *async);
int aeron_async_add_subscription(
    aeron_async_add_subscription_t **async,
    aeron_t *client,
    const char *uri,
    int32_t stream_id,
    void *on_available_image,
    void *on_available_image_clientd,
    void *on_unavailable_image,
    void *on_unavailable_image_clientd);
int aeron_async_add_subscription_poll(aeron_subscription_t **subscription, aeron_async_add_subscription_t *async);

int64_t aeron_publication_offer(
    aeron_publication_t *publication,
    const uint8_t *buffer,
    size_t length,
    aeron_reserved_value_supplier_t reserved_value_supplier,
    void *clientd);
int64_t aeron_publication_try_claim(
    aeron_publication_t *publication, size_t length, aeron_buffer_claim_t *buffer_claim);
int aeron_publication_close(
    aeron_publication_t *publication,
    aeron_notification_t on_close_complete,
    void *on_close_complete_clientd);

int64_t aeron_exclusive_publication_offer(
    aeron_exclusive_publication_t *publication,
    const uint8_t *buffer,
    size_t length,
    aeron_reserved_value_supplier_t reserved_value_supplier,
    void *clientd);
int64_t aeron_exclusive_publication_try_claim(
    aeron_exclusive_publication_t *publication, size_t length, aeron_buffer_claim_t *buffer_claim);
int aeron_exclusive_publication_close(
    aeron_exclusive_publication_t *publication,
    aeron_notification_t on_close_complete,
    void *on_close_complete_clientd);
bool ringloom_aeron_exclusive_publication_is_connected(aeron_exclusive_publication_t *publication);
size_t ringloom_aeron_exclusive_publication_max_payload_length(aeron_exclusive_publication_t *publication);

int aeron_subscription_poll(
    aeron_subscription_t *subscription, aeron_fragment_handler_t handler, void *clientd, size_t fragment_limit);
int aeron_subscription_controlled_poll(
    aeron_subscription_t *subscription,
    aeron_controlled_fragment_handler_t handler,
    void *clientd,
    size_t fragment_limit);
int aeron_subscription_close(
    aeron_subscription_t *subscription, aeron_notification_t on_close_complete, void *on_close_complete_clientd);

int aeron_fragment_assembler_create(
    aeron_fragment_assembler_t **assembler,
    aeron_fragment_handler_t delegate,
    void *delegate_clientd);
int aeron_fragment_assembler_delete(aeron_fragment_assembler_t *assembler);
void aeron_fragment_assembler_handler(
    void *clientd, const uint8_t *buffer, size_t length, aeron_header_t *header);

int aeron_controlled_fragment_assembler_create(
    aeron_controlled_fragment_assembler_t **assembler,
    aeron_controlled_fragment_handler_t delegate,
    void *delegate_clientd);
int aeron_controlled_fragment_assembler_delete(aeron_controlled_fragment_assembler_t *assembler);
aeron_controlled_fragment_handler_action_t aeron_controlled_fragment_assembler_handler(
    void *clientd, const uint8_t *buffer, size_t length, aeron_header_t *header);

int aeron_buffer_claim_commit(aeron_buffer_claim_t *buffer_claim);
int aeron_buffer_claim_abort(aeron_buffer_claim_t *buffer_claim);

typedef enum ringloom_aeron_agent_kind_enum
{
    RINGLOOM_AERON_AGENT_CONDUCTOR = 0,
    RINGLOOM_AERON_AGENT_SENDER = 1,
    RINGLOOM_AERON_AGENT_RECEIVER = 2,
    RINGLOOM_AERON_AGENT_SHARED_NETWORK = 3,
    RINGLOOM_AERON_AGENT_SHARED = 4
}
ringloom_aeron_agent_kind_t;

int ringloom_aeron_driver_start_manual(aeron_driver_t *driver);
int ringloom_aeron_driver_do_work(aeron_driver_t *driver, ringloom_aeron_agent_kind_t kind);
void ringloom_aeron_driver_idle(aeron_driver_t *driver, ringloom_aeron_agent_kind_t kind, int work_count);

#endif
