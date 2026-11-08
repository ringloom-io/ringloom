<!-- SPDX-License-Identifier: Apache-2.0 -->
# RingLoom Node.js bindings

These bindings expose the `ringloom_service` native library to Node.js through a Node-API addon. The addon uses the same C ABI as the Java bindings and loads `libringloom_service` from the addon build output. Aeron is statically linked into that native library; packages do not need a separate Aeron dynamic library, and missing RingLoom native libraries fail at addon load time with the attempted path.

## Prerequisites

- Node.js 20+
- npm
- Zig 0.17.x
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

For v2 transport diagnostics, use `service.aeronDirectory()`, `service.aeronInboundStreamId()`, `service.publicationConnected()`, and `client.lastAeronSendStatus()`. `aeronInboundStreamId()` currently returns `0` because the direct-UDP data path no longer uses a service-to-broker ingress stream.

## Replicated KV stores (ABI 6)

```js
const { KvAcquire, KvTtlClock, TopicAckMode } = require('@ringloom/service');
const writer = client.openKvWriter('prices', {
  maxKeyLen: 64,
  maxValueLen: 1024,
  defaultTtlMs: 0n,
}, { acquire: KvAcquire.FAIL_IF_HELD });
const reader = client.openKvReader('prices', {
  waitReadyTimeoutMs: 5000,
  ttlClock: KvTtlClock.PRECISE,
});
const session = reader.newSession();
try {
  const write = writer.put(Buffer.from('apple'), Buffer.from('123'), {
    ttlMs: 1000n,
    ackMode: TopicAckMode.REPLICATE_ONCE,
  });
  throwForStatus('put', write.status);
  // Poll the service control plane for writer-state and acknowledgement updates.
  service.pollControl(256);
  const output = Buffer.alloc(1024);
  const read = session.get(Buffer.from('apple'), output);
  if (read.status === RingloomStatus.OK) {
    console.log(output.subarray(0, read.length).toString());
  }
} finally {
  session.close();
  reader.close();
  writer.close();
}
```

The broker must enable both topics and KV. Reads are local and replication is
asynchronous: a successful write need not be visible immediately. Keep polling
`service.pollControl()` for writer lease changes and acknowledgements.

`openKvWriter(name, config = {}, options = {})` starts from the ABI's default
initializers. Config fields are `formatVersion`, `maxKeyLen`, `maxValueLen`,
`defaultTtlMs`, `mapSizeHint`, `topicRollScheme`, and `flags`. First creation wins;
subsequent writers must supply a matching config (the native comparison ignores
`mapSizeHint`). Options are `acquire` (`KvAcquire.FAIL_IF_HELD` or `WAIT`),
`openTimeoutMs`, `maxFrameLength`, and `onState(state, writerEpoch)`. State events
are queued during native processing and delivered on the Node thread after the
native `pollControl()` call completes. Handlers must be synchronous. Operations
on the writer whose callback is running are rejected until the callback returns.
Callback exceptions propagate from `pollControl()`; remaining events from that
delivery are discarded.

`openKvReader(name, options = {})` accepts `waitReadyTimeoutMs`, `prefault`,
`ttlClock` (`KvTtlClock.PRECISE` or `COARSE`), and `maxReaders`.
`reader.status()` returns `state` (`KvStoreState`), `replicationLive`, `generation`,
`appliedIndex`, `appliedLeaderEpoch`, `appliedAtNs`, `localLogTipIndex`,
`lagEntries`, `heartbeatAgeMs`, and `generationSwitches`. All 64-bit fields,
writer epochs, and publish sequences are BigInts. Integer inputs accept BigInt
or safe integer Numbers; invalid ranges are rejected without truncation.

Keys, values, and copy destinations accept Buffer, Uint8Array, or
Uint8ClampedArray, including subarrays. `session.get(key, output)` returns
`{ status, length }`. `KV_BUFFER_TOO_SMALL` reports the required length;
`KV_MISSING` includes expired entries. Empty values return `OK` with length zero.
`session.view(key)` returns `{ status, length, value }`, where `value` is a
borrowed read-only byte facade on success and null otherwise. Indexed reads,
`length`, `byteLength`, `at(index)` (including negative indices), and iteration
read directly from the snapshot without copying. It is not a typed array and
exposes no `buffer` or `subarray`; assignments and mutating methods throw
TypeError. Typed-array prototype methods cannot be applied to it. Copy with
`Buffer.from(value)` or `Uint8Array.from(value)` to retain or modify bytes.

Call `session.release()` promptly. Release, close, or garbage collection of the
session invalidates the facade: lengths become zero, indexed reads and `at()`
return undefined, and iteration ends; a stale facade stays invalid even after
the session borrows a new view. Copies remain valid. A session permits one
outstanding view; other reads and iteration require release first. The facade
does not keep the session alive. Long-held snapshots pin old generations and
can increase store storage use.

`session.forEach((key, value) => ...)` visits one snapshot in engine-defined
order, skips expired entries, and stops when the callback returns `false`.
The callback receives **copies** that can be retained and modified. It must be
synchronous; reentrant operations on the same session are rejected. Exceptions
propagate after the native iterator releases its snapshot. The Node wrapper
allocates JS objects and copies for iteration; the native allocation-free
hot-path contract does not apply to those wrapper allocations.

Writer methods `put(key, value, options)`, `delete(key, options)`, `clear(options)`,
and `commitBatch(options)` return `{ status, publishSeq }`. Options accept
`ackMode` (`TopicAckMode`); `put` also accepts `ttlMs`. Omit `ttlMs` or use null
for the store default, use zero to disable expiry for that value, or a positive
integer for a TTL stamped on the writer's clock. Negative TTLs are rejected.
`writer.beginBatch()` returns a status; records then accumulate until commit or
a native auto-flush. Batches do not promise atomic visibility across replicas.
`writer.isAcked(publishSeq)` returns a boolean, and `writer.state()` returns a
`KvWriterState`. On unsuccessful writes, ignore `publishSeq`. Native errors such
as backpressure, a lost lease, key/value limits, or a full batch are returned as
statuses, allowing retry policies in the application. Open/setup failures throw
errors with `statusCode`, `statusName`, and `nativeMessage`.

All KV `close()` methods are idempotent. Close sessions before their reader,
and all readers/writers before their client or service; premature parent closes
throw. Native resources retain their parents through garbage collection, but
explicit close is required for predictable reader-slot and writer-lease release,
especially when state handlers capture their writer. Existing service, topic,
and messaging APIs remain available.
