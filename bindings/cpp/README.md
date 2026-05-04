<!-- SPDX-License-Identifier: Apache-2.0 -->
# RingLoom C++ bindings

These bindings expose the `ringloom_service` C ABI as a C++17 RAII API. The wrapper is header-only and uses the same native `libringloom_service` artifact as the Java and Node.js bindings.

## Build

```bash
zig build cpp-bindings
```

This compiles the C++ binding test binary against `zig-out/lib/libringloom_service.so` or `.dylib`.

## Test

```bash
zig build test-cpp
```

The integration test starts a real broker with `scripts/start-test-broker.sh`, starts two C++ services, sends with a zero-copy `BufferClaim`, and receives with a polling `MessageConsumer`.

## API usage

```cpp
#include <ringloom/service.hpp>

auto service = ringloom::Service::start(ringloom::ServiceConfig::of("orders"));
auto client = service.createClient("pricing");

service.pollControl();
for (const auto &target : client.targetServices()) {
    const auto status = client.sendTo(target.target_node_id, target.target_service_id, nullptr, 0);
    ringloom::throwIfNotOk("ringloom_client_send_to", status);
}
```

Use `RingloomError` and `throwIfNotOk(...)` in setup or convenience code. Hot-path methods such as `Client::tryClaim`, `BufferClaim::commit`, `BufferClaim::abort`, `Client::send*`, and `MessageConsumer::poll` return primitive status/count values so callers can avoid exception allocation on expected runtime outcomes.
