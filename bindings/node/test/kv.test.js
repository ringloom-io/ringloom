// SPDX-License-Identifier: Apache-2.0
"use strict";
const assert = require("node:assert/strict");
const test = require("node:test");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { spawn } = require("node:child_process");
const { once } = require("node:events");
const {
  RingloomService, RingloomStatus: S, KvReader, KvReadSession, KvWriter,
  KvAcquire, KvTtlClock, KvWriterState, KvStoreState, TopicAckMode,
} = require("..");
const sleep = ms => Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
const b = s => Buffer.from(s);

function eventually(service, predicate) {
  const deadline = Date.now() + 8000;
  do {
    service.pollControl(256);
    if (predicate()) return;
    sleep(10);
  } while (Date.now() < deadline);
  assert.fail("KV operation did not converge within 8 seconds");
}

test("KV constants and constructors", () => {
  assert.equal(S.KV_MISSING, 32);
  assert.equal(S.KV_BATCH_FULL, 40);
  assert.equal(KvAcquire.WAIT, 1);
  assert.equal(KvTtlClock.COARSE, 1);
  assert.equal(KvWriterState.GRANTED, 0);
  assert.equal(KvStoreState.LIVE, 2);
  for (const Type of [KvReader, KvReadSession, KvWriter]) assert.throws(() => new Type(), /created/);
});

test("KV writer, sessions, TTL, batching and ownership", { timeout: 60000 }, async () => {
  const root = process.env.RINGLOOM_PROJECT_ROOT || path.resolve(__dirname, "../../..");
  const bin = process.env.RINGLOOM_BROKER_BIN || path.join(root, "zig-out/bin/ringloom-broker");
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "ringloom-node-kv-"));
  const config = path.join(dir, "broker.properties");
  const storage = path.join(dir, "storage");
  fs.mkdirSync(storage);
  fs.writeFileSync(config, [
    "broker.node.id=1", `broker.local.host.port=127.0.0.1:${22000 + Math.floor(Math.random() * 10000)}`,
    "broker.group.name=node-kv", `broker.storage.path=${storage}`,
    "broker.control.buffer.size=65536", "broker.messages.buffer.size=1048576",
    "broker.threading.mode=dedicated", "broker.idle.strategy=backoff",
    "broker.topics.enabled=true", `broker.topics.path=${dir}/topics`,
    "broker.kv.enabled=true", `broker.kv.path=${storage}/kv`,
  ].join("\n") + "\n");
  const log = path.join(dir, "broker.log");
  const fd = fs.openSync(log, "w");
  const broker = spawn(bin, ["--config", config], { stdio: ["ignore", fd, fd] });
  fs.closeSync(fd);
  let service, client, writer, reader, session, standby;
  let success = false;
  try {
    const deadline = Date.now() + 10000;
    while (!/broker started|broker ready/.test(fs.readFileSync(log, "utf8"))) {
      if (broker.exitCode !== null || Date.now() > deadline) throw new Error(fs.readFileSync(log, "utf8"));
      await new Promise(resolve => setTimeout(resolve, 25));
    }
    service = RingloomService.start({ serviceName: "node-kv", storagePath: storage, group: "node-kv", brokerNodeId: 1 });
    client = service.createClient("node-kv");
    assert.throws(() => client.openKvWriter("bad/name"), /name/);
    assert.throws(() => client.openKvWriter("bad", { maxKeyLen: -1 }), /unsigned integer/);
    assert.throws(() => client.openKvWriter("bad", { mapSizeHint: Number.MAX_SAFE_INTEGER + 1 }), /unsigned integer/);
    assert.throws(() => client.openKvReader("bad", { prefault: 1 }), /boolean/);
    const states = [];
    eventually(service, () => {
      try { writer = client.openKvWriter("store", {
      maxKeyLen: 64, maxValueLen: 1024, defaultTtlMs: 200n,
      mapSizeHint: 16n * 1024n * 1024n, topicRollScheme: "FAST_DAILY",
    }, { acquire: KvAcquire.FAIL_IF_HELD, openTimeoutMs: 5000, onState: (state, epoch) => states.push([state, epoch]) }); return true; }
      catch (err) { if (err.statusCode === S.TOPIC_LEADER_UNAVAILABLE) return false; throw err; }
    });
    assert.ok(writer instanceof KvWriter);
    assert.equal(writer.state(), KvWriterState.GRANTED);
    assert.throws(() => client.openKvWriter("store", { maxKeyLen: 64, maxValueLen: 1024, defaultTtlMs: 200n }), err => err.statusCode === S.KV_HELD_BY_OTHER);
    reader = client.openKvReader("store", { waitReadyTimeoutMs: 5000, prefault: true, ttlClock: KvTtlClock.COARSE, maxReaders: 32 });
    session = reader.newSession();
    assert.ok(reader instanceof KvReader);
    assert.ok(session instanceof KvReadSession);
    assert.equal(typeof reader.status().appliedIndex, "bigint");
    assert.equal(typeof reader.status().replicationLive, "boolean");
    assert.equal(session.get(b("missing"), Buffer.alloc(10)).status, S.KV_MISSING);
    assert.equal(session.view(b("missing")).value, null);
    assert.equal(writer.put(b("persistent"), b("value"), { ttlMs: 0n }).status, S.OK);
    let result;
    const out = Buffer.alloc(100);
    eventually(service, () => (result = session.get(b("persistent"), out)).status === S.OK);
    assert.equal(out.subarray(0, result.length).toString(), "value");
    assert.deepEqual(session.get(b("persistent"), Buffer.alloc(1)), { status: S.KV_BUFFER_TOO_SMALL, length: 5 });
    const view = session.view(b("persistent"));
    assert.equal(view.status, S.OK);
    assert.equal(Buffer.from(view.value).toString(), "value");
    assert.equal(view.value[0], 118);
    assert.equal(view.value.at(-1), 101);
    assert.equal(view.value.buffer, undefined);
    assert.equal(view.value.subarray, undefined);
    assert.equal(ArrayBuffer.isView(view.value), false);
    assert.equal(Buffer.isBuffer(view.value), false);
    assert.ok(0 in view.value); assert.ok(!(5 in view.value));
    assert.deepEqual([...view.value], [118, 97, 108, 117, 101]);
    assert.equal(Uint8Array.from(view.value).length, 5);
    const retained = Buffer.from(view.value);
    for (const mutate of [
      () => { view.value[0] = 0; },
      () => view.value.fill(0), () => view.value.set([0]),
      () => view.value.copyWithin(0, 1), () => view.value.reverse(),
      () => Object.defineProperty(view.value, "0", { value: 0 }),
      () => Uint8Array.prototype.fill.call(view.value, 0),
      () => { view.value.length = 0; }, () => { delete view.value[0]; },
      () => Object.setPrototypeOf(view.value, Uint8Array.prototype),
    ]) assert.throws(mutate, TypeError);
    assert.equal(Buffer.from(view.value).toString(), "value");
    const iterator = view.value[Symbol.iterator]();
    assert.equal(iterator.next().value, 118);
    assert.throws(() => session.get(b("persistent"), out), /release/);
    assert.throws(() => reader.close(), /sessions/);
    assert.throws(() => client.close(), /KV handles/);
    assert.throws(() => service.close(), /KV handles/);
    session.release(); session.release();
    assert.equal(view.value.byteLength, 0, "release invalidates borrowed views");
    assert.equal(view.value.length, 0);
    assert.deepEqual([...view.value], []);
    assert.equal(Buffer.from(view.value).length, 0, "copies after release are empty");
    assert.equal(view.value[0], undefined);
    assert.equal(view.value.at(0), undefined);
    assert.equal(iterator.next().done, true);
    assert.equal(retained.toString(), "value");
    // A stale view stays invalid after the session borrows a new snapshot.
    const fresh = session.view(b("persistent")).value;
    assert.equal(fresh.length, 5);
    assert.equal(view.value.length, 0);
    assert.equal(view.value[0], undefined);
    session.release();
    assert.equal(fresh.length, 0);
    const closing = reader.newSession();
    const closedView = closing.view(b("persistent")).value;
    assert.equal(closedView.at({ valueOf() { closing.close(); return 0; } }), undefined);
    closing.close();
    assert.equal(closedView.length, 0);
    assert.equal(closedView[0], undefined);
    if (global.gc) {
      // Sessions release their native reader slots when explicitly closed
      // or garbage collected, including while a borrowed view is outstanding.
      // Exercise independent native readers as well as sessions of one reader.
      const orphan = client.openKvReader("store").newSession();
      global.gc();
      await new Promise(resolve => setImmediate(resolve));
      assert.equal(orphan.get(b("persistent"), out).status, S.OK);
      orphan.close();
      let doomed = reader.newSession();
      const dangling = doomed.view(b("persistent")).value;
      doomed = null;
      for (let n = 0; n < 20 && dangling.byteLength; n++) {
        global.gc(); await new Promise(resolve => setImmediate(resolve));
      }
      assert.equal(dangling.byteLength, 0, "GC invalidates a session's borrowed views");
      assert.equal(dangling[0], undefined);
    }
    assert.equal(writer.put(b("empty"), Buffer.alloc(0), { ttlMs: 0 }).status, S.OK);
    eventually(service, () => session.get(b("empty"), Buffer.alloc(0)).status === S.OK);
    const empty = session.view(b("empty"));
    assert.equal(empty.status, S.OK); assert.equal(empty.length, 0); session.release();
    assert.throws(() => writer.put(b("x"), b("x"), { ttlMs: -2 }), /unsigned integer/);
    assert.throws(() => writer.put(b("x"), b("x"), { ackMode: 2 }), /unsigned integer/);
    assert.throws(() => writer.isAcked(-1n), /unsigned integer/);
    assert.equal(writer.put(Buffer.alloc(65), b("x")).status, S.KV_KEY_TOO_LARGE);
    assert.equal(writer.put(b("x"), Buffer.alloc(1025)).status, S.KV_VALUE_TOO_LARGE);
    const ack = writer.put(b("acked"), b("yes"), { ttlMs: 0, ackMode: TopicAckMode.REPLICATE_ONCE });
    assert.equal(ack.status, S.OK); assert.equal(typeof ack.publishSeq, "bigint");
    eventually(service, () => writer.isAcked(ack.publishSeq));
    assert.equal(writer.put(b("ttl"), b("expires")).status, S.OK);
    eventually(service, () => session.get(b("ttl"), out).status === S.OK);
    eventually(service, () => session.get(b("ttl"), out).status === S.KV_MISSING);
    assert.equal(session.get(b("persistent"), out).status, S.OK, "ttlMs=0 overrides default TTL");
    assert.equal(writer.beginBatch(), S.OK);
    assert.equal(writer.put(b("batch-a"), b("a"), { ttlMs: 0 }).status, S.OK);
    assert.equal(writer.put(b("batch-b"), b("b"), { ttlMs: 0 }).status, S.OK);
    assert.equal(session.get(b("batch-a"), out).status, S.KV_MISSING);
    assert.equal(writer.commitBatch().status, S.OK);
    eventually(service, () => session.get(b("batch-b"), out).status === S.OK);
    const entries = [];
    assert.equal(session.forEach((key, value) => { entries.push([key, value]); }), S.OK);
    assert.ok(entries.some(([key, value]) => key.toString() === "batch-a" && value.toString() === "a"));
    let visited = 0;
    session.forEach(() => { visited++; assert.throws(() => session.close(), /busy/); return false; });
    assert.equal(visited, 1);
    assert.throws(() => session.forEach(() => { throw new Error("callback failure"); }), /callback failure/);
    assert.equal(session.get(b("persistent"), out).status, S.OK);
    assert.equal(writer.delete(b("persistent")).status, S.OK);
    eventually(service, () => session.get(b("persistent"), out).status === S.KV_MISSING);
    assert.equal(writer.clear().status, S.OK);
    eventually(service, () => { let n = 0; session.forEach(() => { n++; }); return n === 0; });
    assert.ok(entries[0][0].length > 0, "iteration copies survive later writes");
    assert.ok(states.length > 0, "writer state events are delivered by pollControl");
    assert.ok(states.every(([state, epoch]) => typeof state === "number" && typeof epoch === "bigint"));
    const standbyStates = [];
    standby = client.openKvWriter("store", {
      maxKeyLen: 64, maxValueLen: 1024, defaultTtlMs: 200n,
    }, { acquire: KvAcquire.WAIT, onState: (state, epoch) => {
      standbyStates.push(state);
      assert.equal(typeof epoch, "bigint");
      // Callbacks run after native releases its context lock.
      assert.equal(typeof reader.status().generation, "bigint");
      assert.throws(() => standby.state(), /busy/);
    } });
    assert.equal(standby.state(), KvWriterState.QUEUED);
    writer.close();
    eventually(service, () => standby.state() === KvWriterState.GRANTED);
    assert.ok(standbyStates.includes(KvWriterState.GRANTED));
    standby.close();
    session.close(); session.close();
    assert.throws(() => session.release(), /closed/);
    reader.close(); reader.close(); writer.close(); writer.close();
    client.close(); client = null; service.close(); service = null;
    success = true;
  } finally {
    session?.close(); reader?.close(); writer?.close(); standby?.close(); client?.close(); service?.close();
    if (broker.exitCode === null) {
      const exited = once(broker, "exit"); broker.kill("SIGTERM");
      const timer = setTimeout(() => broker.kill("SIGKILL"), 3000);
      await exited; clearTimeout(timer);
    }
    if (success) fs.rmSync(dir, { recursive: true, force: true });
    else console.error(`Preserving KV broker artifacts: ${dir}`);
  }
});
