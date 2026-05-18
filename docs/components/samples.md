# Samples

The primary sample is the order-management application in
`samples/order-management/`. It demonstrates RingLoom as a service runtime rather
than as a synthetic benchmark.

## Order-management topology

The sample starts two broker nodes and six service types. Services are placed so the
normal flow exercises both local ring-buffer IPC and remote Aeron UDP routing.

```text
Node 1                                      Node 2
broker node_id=1                           broker node_id=2

order-simulator                            matching-engine
order-gateway                              execution-service
risk-service
portfolio-service
```

Default flow:

```text
order-simulator
  -> order-gateway       local ring buffer
  -> risk-service        local ring buffer
  -> matching-engine     direct Aeron UDP to node 2, broker final delivery
  -> execution-service   local ring buffer on node 2
  -> portfolio-service   direct Aeron UDP to node 1, broker final delivery
```

## Features demonstrated

1. Service startup and registration.
2. Service discovery by name.
3. Local ring-buffer sends.
4. Remote direct Aeron UDP sends.
5. Load balancing across multiple instances in the full profile.
6. Service leader routing in the full profile.
7. Fixed-layout application payloads.
8. Back-pressure handling and counters.
9. `ringloom-stat` and observability integration.
10. Graceful shutdown.

See [`../samples_order_management.md`](../samples_order_management.md) for the
sample-specific design and run instructions.
