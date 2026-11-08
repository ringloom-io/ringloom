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

## Key/value stores (ABI 6)

```cpp
auto writer = client.openKvWriter("prices");
auto reader = client.openKvReader("prices");
auto session = reader.newSession();
auto result = writer.put("ABC", "12.50", 1000,
                         ringloom::TopicAckMode::replicate_once);
// Poll service control and check writer.isAcked(result.sequence) as needed.
char buffer[64];
auto read = session.get("ABC", buffer, sizeof(buffer));
if (read.status == ringloom::Status::ok) {
    std::string value(buffer, read.length);
}
```

`KvStoreConfig`, `KvReaderOptions`, and `KvWriterOptions` initialize all fields
through the ABI's default initializers. Their public fields are the native ABI
fields, including size, limits, timeout, prefault, TTL clock (0 precise/1 coarse),
acquisition (0 fail if held/1 wait), and writer state callback/user data.
`reader.status()` returns every native status field, with its size initialized.
Writer callbacks must not throw; keep their user data alive until writer close.
Writer state callbacks must not re-enter KV APIs or close handles; `forEach`
visitor rules are described below.

Keys and values use binary-safe `std::string_view`; embedded NUL bytes are valid.
Store names must be nonempty and cannot contain NUL. `put` TTL is signed
milliseconds: -1 selects the store default, 0 never expires, positive values set
expiry. Other negative values are rejected by the native API. Writes return
`KvWriteResult{status, sequence}`; check status before using the sequence.
`erase` deletes a key and `clear` deletes the store contents. `beginBatch` starts
buffering, and `commitBatch` flushes with the requested ack mode. Buffered writes
return sequence zero; automatic flush and batch-full behavior follow the native
ABI. There is no batch cancellation API. Close releases the writer lease.

`get` copies into caller storage; `KvGetResult.length` reports required capacity
on `kv_buffer_too_small`. `kv_missing` includes expired entries and differs from
an empty value (`ok`, length zero). `view` borrows a value until `release` or
session close. While a view is held, get/view/iteration reuse its pinned snapshot;
release promptly to allow generation switching and storage reclamation.
`forEach` visits one consistent snapshot, skips expired entries, and stops when
the visitor returns false. Callback key/value views are valid only during that
callback. Nested `forEach`, `get`, and `view` on the active session return
`invalid_argument`. Callback `release` is deferred until iteration returns;
`close` or destruction detaches the wrapper immediately and defers native cleanup
until iteration returns. These rules also apply when the visitor throws.
Visitor exceptions are caught at the C boundary and rethrown to C++.

KV handles cannot be copied or moved. Destruction and explicit `close` are
idempotent. Each session belongs to one thread. A session retains its reader;
`reader.close()` defers native reader cleanup until the final session closes.
Close all sessions, readers, and writers before destroying the service. Writers
also require their client to remain alive. Returned borrowed views do not own
sessions and must never be used after release or close.

Run deterministic wrapper/ABI tests without a broker or native library:

```sh
c++ -std=c++17 -Wall -Wextra -Werror -pedantic -Iinclude -Ibindings/cpp/include \
  bindings/cpp/test/kv_bindings_test.cpp -o /tmp/ringloom-cpp-kv-test
/tmp/ringloom-cpp-kv-test

c++ -std=c++17 -g -fsanitize=address,undefined -fno-sanitize-recover=undefined \
  -fno-omit-frame-pointer -Iinclude -Ibindings/cpp/include \
  bindings/cpp/test/kv_bindings_test.cpp -o /tmp/ringloom-cpp-kv-test-asan
/tmp/ringloom-cpp-kv-test-asan
```

These tests use ABI doubles; native and broker integration remains covered by
`zig build test`, `zig build e2e`, and the existing `zig build test-cpp` suite.
