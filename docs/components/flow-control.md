# Flow Control and Back-Pressure

RingLoom has multiple pressure points because local delivery and remote delivery use
different mechanisms. The API surfaces those pressures through one send-error model
and exposes counters for diagnosis.

## Pressure points

| Route stage | Authoritative pressure source |
|---|---|
| Local service -> local service | Target service messages ring buffer remaining capacity. |
| Service -> remote broker | Aeron UDP publication status and publisher limits. |
| Remote broker -> local target service | Target service messages ring buffer remaining capacity. |
| Broker admin -> peer broker | Aeron admin publication status. |

The removed broker send ring and TCP peer write queues are not authoritative in the
active architecture.

## Local sends

For same-host sends, the source service maps the target messages ring buffer and can
check or claim exact capacity. If the ring cannot fit the message, the send returns a
buffer-full/back-pressure error or follows the configured spin-with-timeout policy.

## Remote sends

For remote sends, the source service publishes to an Aeron UDP publication for the
target node. Aeron statuses map to RingLoom results:

| Aeron status | RingLoom behavior |
|---|---|
| Positive position / committed claim | Success. |
| `BACK_PRESSURED` | Return back-pressure or spin until timeout according to policy. |
| `NOT_CONNECTED` | Peer unavailable/disconnected send error. |
| `ADMIN_ACTION` | Retryable transient treated like pressure. |
| `CLOSED` | Fatal publication state; invalidate cached publication and report error. |
| `MAX_POSITION_EXCEEDED` | Fatal stream exhaustion; requires stream rotation/recreation. |
| Failed claim/offer | Remote transport unavailable or explicit send failure. |

## Broker final delivery

The target broker is responsible for converting a remote Aeron frame into a local
service ring-buffer record. If the destination service ring is full, the broker
cannot make the remote sender wait synchronously. It counts the event and applies the
configured final-delivery policy, which is best-effort in the active design.

## Flow-control region

Broker metadata can include a flow-control region that publishes advisory target
capacity and pressure state to local services. This helps clients avoid obviously
bad choices and gives operators diagnostics, but remote sends are ultimately governed
by Aeron publication status.

## Service policy knobs

`FlowControlConfig` controls whether clients:

1. Check advisory/local capacity before send.
2. Drop/fail immediately under pressure.
3. Spin with a bounded timeout.
4. Record counters for specific pressure causes.

Policies must stay bounded. Hot-path code should not allocate or block indefinitely
while waiting for capacity.

## Diagnostic counters

Useful pressure counters include:

1. Local ring-buffer full sends.
2. Aeron back-pressured, admin-action, not-connected, closed, and max-position
   counts.
3. Remote publication health and last status.
4. Broker receiver target-service-full drops.
5. Admin UDP forwarding statuses.
6. Flow-control region pressure state and update age.

When diagnosing a send failure, first identify the route: local ring, remote Aeron
publication, or remote broker final delivery.
