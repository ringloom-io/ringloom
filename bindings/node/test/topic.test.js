// SPDX-License-Identifier: Apache-2.0
"use strict";

const assert = require("node:assert/strict");
const childProcess = require("node:child_process");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const test = require("node:test");

const {
  RingloomService,
  RingloomStatus,
  TopicStart,
  TopicAckMode,
  throwForStatus,
} = require("..");

function repoRoot() {
  return (
    process.env.RINGLOOM_PROJECT_ROOT || path.resolve(__dirname, "../../..")
  );
}

function brokerBin(root) {
  return (
    process.env.RINGLOOM_BROKER_BIN ||
    path.join(root, "zig-out/bin/ringloom-broker")
  );
}

function parseEnv(output) {
  const env = {};
  for (const line of output.split(/\r?\n/)) {
    const index = line.indexOf("=");
    if (index > 0) {
      env[line.slice(0, index)] = line.slice(index + 1);
    }
  }
  if (!env.RINGLOOM_STORAGE_PATH || !env.RINGLOOM_GROUP) {
    throw new Error(
      `broker script did not emit expected environment variables:\n${output}`,
    );
  }
  return env;
}

function startBroker(root, workspace, options = {}) {
  const args = [
    path.join(root, "scripts/start-test-broker.sh"),
    "--workspace",
    workspace,
    "--node-id",
    String(options.nodeId ?? 1),
    "--port",
    String(options.port ?? 19001),
    "--group",
    options.group ?? "ringloom-java-test",
    "--daemon",
    "--bin-dir",
    path.dirname(brokerBin(root)),
  ];
  for (const peer of options.peers ?? []) {
    args.push("--peer", peer);
  }
  if (options.topics) {
    args.push("--topics");
  }
  const output = childProcess.execFileSync("bash", args, {
    cwd: root,
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
  });
  return parseEnv(output);
}

function stopBroker(root, workspace, nodeId = 1) {
  childProcess.execFileSync(
    "bash",
    [
      path.join(root, "scripts/start-test-broker.sh"),
      "--workspace",
      workspace,
      "--node-id",
      String(nodeId),
      "--stop",
    ],
    {
      cwd: root,
      encoding: "utf8",
      stdio: ["ignore", "pipe", "pipe"],
    },
  );
}

function serviceConfig(serviceName, broker) {
  return {
    serviceName,
    storagePath: broker.RINGLOOM_STORAGE_PATH,
    group: broker.RINGLOOM_GROUP,
    brokerNodeId: Number(broker.RINGLOOM_BROKER_NODE_ID || 1),
    heartbeatTimeoutMillis: 10_000,
    controlBufferLength: 65_536,
    messagesBufferLength: 1_048_576,
  };
}

function sleep(ms) {
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
}

test("topic value types and status constants", () => {
  assert.equal(TopicStart.EARLIEST, 0);
  assert.equal(TopicStart.LATEST, 1);
  assert.equal(TopicAckMode.FIRE_AND_FORGET, 0);
  assert.equal(TopicAckMode.REPLICATE_ONCE, 1);
  assert.equal(RingloomStatus.NOT_READY, 11);
  assert.equal(RingloomStatus.isOk(RingloomStatus.NOT_READY), false);
});

/**
 * Runs `body(svc, broker)` against a fresh broker and service, keeping the
 * workspace on failure for debugging.
 */
function withService(serviceName, brokerOptions, body) {
  const root = repoRoot();
  const workspace = fs.mkdtempSync(
    path.join(os.tmpdir(), "ringloom-node-topic-"),
  );
  let success = false;
  let broker;

  try {
    broker = startBroker(root, workspace, brokerOptions);
    const svc = RingloomService.start(serviceConfig(serviceName, broker));
    try {
      body(svc);
      success = true;
    } finally {
      svc.close();
    }
  } finally {
    if (broker) {
      stopBroker(root, workspace);
    }
    if (success) {
      fs.rmSync(workspace, { recursive: true, force: true });
    } else {
      console.error(`Preserving RingLoom Node workspace: ${workspace}`);
    }
  }
}

/** Polls `subscription` until `expected` messages arrive or ~5s pass. */
function drain(subscription, expected) {
  const received = [];
  const deadline = Date.now() + 5_000;
  while (received.length < expected && Date.now() < deadline) {
    let result = subscription.poll();
    while (received.length < expected && result.status === RingloomStatus.OK) {
      if (result.payload) {
        received.push(result.payload.toString("utf8"));
      }
      result = subscription.poll();
    }
    if (received.length < expected) {
      sleep(25);
    }
  }
  return received;
}

test("register topic publication and close is idempotent", () => {
  withService("node-topic-pub", { topics: true }, (svc) => {
    const client = svc.createClient("node-topic-pub");
    try {
      const publisher = client.registerTopicPublication("test-topic");
      assert.equal(typeof publisher.publish, "function");

      publisher.close();
      publisher.close();

      assert.throws(() => publisher.publish(Buffer.from("x")), /closed/i);
    } finally {
      client.close();
    }
  });
});

test("publisher publish and replicate_once ack", () => {
  withService("node-topic-ack", { topics: true }, (svc) => {
    const client = svc.createClient("node-topic-ack");
    try {
      const publisher = client.registerTopicPublication("ack-test");

      assert.equal(publisher.publish(Buffer.from("hello")), RingloomStatus.OK);

      const token = new BigUint64Array(1);
      const status = publisher.publish(
        Buffer.from("hello"),
        TopicAckMode.REPLICATE_ONCE,
        7,
        token,
      );
      assert.equal(status, RingloomStatus.OK);
      assert.ok(token[0] > 0n, "replicate_once publish should return a token");

      // A single-node broker acks on append; the ack arrives through the
      // service control plane.
      let acked = false;
      const deadline = Date.now() + 5_000;
      while (!acked && Date.now() < deadline) {
        svc.pollControl(64);
        acked = publisher.isAcked(Number(token[0]));
        if (!acked) sleep(10);
      }
      assert.ok(acked, "replicate_once publish should be acked");

      publisher.close();
    } finally {
      client.close();
    }
  });
});

test("subscriber receives published messages in order", () => {
  withService("node-topic", { topics: true }, (svc) => {
    const pubClient = svc.createClient("node-topic");
    const subClient = svc.createClient("node-topic-sub");
    try {
      const publisher = pubClient.registerTopicPublication("e2e-ff");
      // Subscribing right after registration must work: the broker creates
      // the queue on disk before replying.
      const subscription = subClient.subscribeTopic("e2e-ff", TopicStart.EARLIEST);
      try {
        const sent = ["msg-0", "msg-1", "msg-2", "msg-3", "msg-4"];
        for (const message of sent) {
          svc.pollControl(64);
          assert.equal(publisher.publish(Buffer.from(message)), RingloomStatus.OK);
        }
        assert.deepEqual(drain(subscription, sent.length), sent);
      } finally {
        subscription.close();
        publisher.close();
      }
    } finally {
      subClient.close();
      pubClient.close();
    }
  });
});

test("LATEST subscriber skips messages published before it subscribed", () => {
  withService("node-topic-latest", { topics: true }, (svc) => {
    const pubClient = svc.createClient("node-topic-latest");
    const subClient = svc.createClient("node-topic-latest-sub");
    try {
      const publisher = pubClient.registerTopicPublication("e2e-latest");
      assert.equal(publisher.publish(Buffer.from("before")), RingloomStatus.OK);
      sleep(200);

      const subscription = subClient.subscribeTopic("e2e-latest", TopicStart.LATEST);
      try {
        assert.equal(publisher.publish(Buffer.from("after")), RingloomStatus.OK);
        assert.deepEqual(drain(subscription, 1), ["after"]);
      } finally {
        subscription.close();
        publisher.close();
      }
    } finally {
      subClient.close();
      pubClient.close();
    }
  });
});

test("topic calls fail fast with TOPIC_DISABLED when the broker has topics off", () => {
  withService("node-topic-off", {}, (svc) => {
    const client = svc.createClient("node-topic-off");
    try {
      for (const call of [
        () => client.registerTopicPublication("off-topic"),
        () => client.subscribeTopic("off-topic"),
      ]) {
        const started = Date.now();
        assert.throws(call, (err) => err.statusCode === RingloomStatus.TOPIC_DISABLED);
        // The registration timeout is 5s; the broker must answer instead.
        assert.ok(Date.now() - started < 2_000, "should fail fast, not time out");
      }
    } finally {
      client.close();
    }
  });
});

test("RingloomStatus NOT_READY and isOk", () => {
  assert.notEqual(RingloomStatus.NOT_READY, undefined);
  assert.equal(RingloomStatus.isOk(RingloomStatus.NOT_READY), false);
});
