# Order Management Sample

This sample starts a two-node RingLoom application that routes deterministic
orders through six services:

```text
order-simulator -> order-gateway -> risk-service -> matching-engine
    -> execution-service -> portfolio-service
```

The default topology places simulator, gateway, risk, and portfolio on broker
node 1, and matching plus execution on broker node 2. That means every run uses
same-host shared-memory IPC and cross-broker TCP routing.

## Run

```bash
samples/order-management/scripts/run.sh
samples/order-management/scripts/run.sh --profile full --orders 100000 --rate-per-sec 50000
samples/order-management/scripts/run.sh --optimize ReleaseFast --workspace /tmp/ringloom-orders
samples/order-management/scripts/run.sh --no-build --bin-dir zig-out/bin
```

The repository build also exposes:

```bash
zig build sample-order-management
zig build run-sample-order-management -- --profile default
zig build sample-order-management-smoke
```

The script prints the workspace, logs, results, storage path, and a
`ringloom-stat` command when the run finishes.

## Notes

Application payloads are fixed `extern struct` messages with a 32-byte domain
envelope. Normal service-to-service sends use `ServiceClient.tryClaim` so the
payload is written directly into RingLoom claim memory. The full profile enables
leader election on matching services, starts optional node-2 risk and node-1
matching processes, and restarts the optional risk process so lifecycle behavior
is visible in process logs.
