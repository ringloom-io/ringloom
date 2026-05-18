/* SPDX-License-Identifier: Apache-2.0 */
#ifndef RINGLOOM_AERON_SHIM_H
#define RINGLOOM_AERON_SHIM_H

#include <stdbool.h>
#include "aeron_driver.h"
#include "aeronmd.h"

#if defined(__GNUC__) || defined(__clang__)
#define RINGLOOM_AERON_EXPORT __attribute__((visibility("default")))
#else
#define RINGLOOM_AERON_EXPORT
#endif

typedef enum ringloom_aeron_agent_kind_enum
{
    RINGLOOM_AERON_AGENT_CONDUCTOR = 0,
    RINGLOOM_AERON_AGENT_SENDER = 1,
    RINGLOOM_AERON_AGENT_RECEIVER = 2,
    RINGLOOM_AERON_AGENT_SHARED_NETWORK = 3,
    RINGLOOM_AERON_AGENT_SHARED = 4
}
ringloom_aeron_agent_kind_t;

RINGLOOM_AERON_EXPORT int ringloom_aeron_driver_start_manual(aeron_driver_t *driver);
RINGLOOM_AERON_EXPORT int ringloom_aeron_driver_do_work(aeron_driver_t *driver, ringloom_aeron_agent_kind_t kind);
RINGLOOM_AERON_EXPORT void ringloom_aeron_driver_idle(
    aeron_driver_t *driver, ringloom_aeron_agent_kind_t kind, int work_count);
RINGLOOM_AERON_EXPORT bool ringloom_aeron_exclusive_publication_is_connected(
    aeron_exclusive_publication_t *publication);
RINGLOOM_AERON_EXPORT size_t ringloom_aeron_exclusive_publication_max_payload_length(
    aeron_exclusive_publication_t *publication);

#endif
