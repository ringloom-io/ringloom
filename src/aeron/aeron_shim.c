/* SPDX-License-Identifier: Apache-2.0 */
#include "aeron_shim.h"

#include "aeron_agent.h"
#include "aeron_driver_conductor.h"
#include "aeron_driver_receiver.h"
#include "aeron_driver_sender.h"
#include "aeron_exclusive_publication.h"
#include "util/aeron_error.h"

_Static_assert(AERON_AGENT_RUNNER_CONDUCTOR == 0, "unexpected Aeron conductor runner index");
_Static_assert(AERON_AGENT_RUNNER_SENDER == 1, "unexpected Aeron sender runner index");
_Static_assert(AERON_AGENT_RUNNER_RECEIVER == 2, "unexpected Aeron receiver runner index");
_Static_assert(AERON_AGENT_RUNNER_SHARED_NETWORK == 1, "unexpected Aeron shared-network runner index");
_Static_assert(AERON_AGENT_RUNNER_SHARED == 0, "unexpected Aeron shared runner index");
_Static_assert(AERON_AGENT_RUNNER_MAX == 3, "unexpected Aeron runner count");

int ringloom_aeron_driver_start_manual(aeron_driver_t *driver)
{
    if (NULL == driver)
    {
        AERON_SET_ERR(EINVAL, "%s", "driver is null");
        return -1;
    }

    for (int i = 0; i < AERON_AGENT_RUNNER_MAX; i++)
    {
        aeron_agent_runner_t *runner = &driver->runners[i];

        if (AERON_AGENT_STATE_INITED == runner->state)
        {
            if (NULL != runner->on_start)
            {
                runner->on_start(runner->on_start_state, runner->role_name);
            }

            runner->state = AERON_AGENT_STATE_MANUAL;
        }
    }

    return 0;
}

int ringloom_aeron_driver_do_work(aeron_driver_t *driver, ringloom_aeron_agent_kind_t kind)
{
    if (NULL == driver)
    {
        AERON_SET_ERR(EINVAL, "%s", "driver is null");
        return -1;
    }

    switch (kind)
    {
        case RINGLOOM_AERON_AGENT_CONDUCTOR:
            return aeron_agent_do_work(&driver->runners[AERON_AGENT_RUNNER_CONDUCTOR]);

        case RINGLOOM_AERON_AGENT_SENDER:
            return aeron_agent_do_work(&driver->runners[AERON_AGENT_RUNNER_SENDER]);

        case RINGLOOM_AERON_AGENT_RECEIVER:
            return aeron_agent_do_work(&driver->runners[AERON_AGENT_RUNNER_RECEIVER]);

        case RINGLOOM_AERON_AGENT_SHARED_NETWORK:
            return aeron_agent_do_work(&driver->runners[AERON_AGENT_RUNNER_SHARED_NETWORK]);

        case RINGLOOM_AERON_AGENT_SHARED:
            return aeron_agent_do_work(&driver->runners[AERON_AGENT_RUNNER_SHARED]);

        default:
            AERON_SET_ERR(EINVAL, "%s", "unknown RingLoom Aeron agent kind");
            return -1;
    }
}

void ringloom_aeron_driver_idle(aeron_driver_t *driver, ringloom_aeron_agent_kind_t kind, int work_count)
{
    if (NULL == driver)
    {
        return;
    }

    switch (kind)
    {
        case RINGLOOM_AERON_AGENT_CONDUCTOR:
            aeron_agent_idle(&driver->runners[AERON_AGENT_RUNNER_CONDUCTOR], work_count);
            break;

        case RINGLOOM_AERON_AGENT_SENDER:
            aeron_agent_idle(&driver->runners[AERON_AGENT_RUNNER_SENDER], work_count);
            break;

        case RINGLOOM_AERON_AGENT_RECEIVER:
            aeron_agent_idle(&driver->runners[AERON_AGENT_RUNNER_RECEIVER], work_count);
            break;

        case RINGLOOM_AERON_AGENT_SHARED_NETWORK:
            aeron_agent_idle(&driver->runners[AERON_AGENT_RUNNER_SHARED_NETWORK], work_count);
            break;

        case RINGLOOM_AERON_AGENT_SHARED:
            aeron_agent_idle(&driver->runners[AERON_AGENT_RUNNER_SHARED], work_count);
            break;
    }
}

bool ringloom_aeron_exclusive_publication_is_connected(aeron_exclusive_publication_t *publication)
{
    if (NULL == publication)
    {
        return false;
    }

    int32_t is_connected;
    AERON_GET_ACQUIRE(is_connected, publication->log_meta_data->is_connected);
    return 1 == is_connected;
}

size_t ringloom_aeron_exclusive_publication_max_payload_length(aeron_exclusive_publication_t *publication)
{
    if (NULL == publication)
    {
        return 0;
    }

    return publication->max_payload_length;
}
