<!-- SPDX-License-Identifier: Apache-2.0 -->
# RingLoom Node.js bindings

These bindings expose the `ringloom_service` native library to Node.js through a Node-API addon. The addon uses the same C ABI as the Java bindings and loads `libringloom_service` from the addon build output.

## Prerequisites

- Node.js 20+
- npm
- Zig 0.16.x
- A C++17 compiler usable by `node-gyp`

## Build

```bash
zig build node-bindings
```

You can also build directly after creating the native RingLoom service library:

```bash
zig build service-c
cd bindings/node
npm ci --ignore-scripts
npm run build
```

## Test

```bash
zig build test-node
```

The integration tests start a real broker with `scripts/start-test-broker.sh`.

## API usage

```js
const { RingloomService, RingloomStatus, throwForStatus } = require('@ringloom/service');

const service = RingloomService.start({ serviceName: 'orders' });
try {
  const client = service.createClient('pricing');
  try {
    service.pollControl(256);
    for (const target of client.targetServices()) {
      const status = client.sendTo(target.targetNodeId, target.targetServiceId, Buffer.alloc(0));
      throwForStatus('ringloom_client_send_to', status);
    }
  } finally {
    client.close();
  }
} finally {
  service.close();
}
```

`RingloomClient.tryClaim(templateId, payloadLength, claim)` fills a reusable `BufferClaim`. `claim.payloadBuffer()` returns a borrowed writable `Buffer` over RingLoom ring-buffer memory; call `commit()` or `abort()` before reusing or closing the claim.

`MessageConsumer.poll(handler, limit)` invokes `handler(message)` synchronously on the polling thread. The message payload buffer is borrowed and is valid only during the callback; copy it if it must outlive the handler.
