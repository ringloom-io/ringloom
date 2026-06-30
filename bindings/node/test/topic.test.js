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

test("register topic publication and close is idempotent", () => {
  const root = repoRoot();
  const workspace = fs.mkdtempSync(
    path.join(os.tmpdir(), "ringloom-node-topic-"),
  );
  let success = false;
  let broker;

  try {
    broker = startBroker(root, workspace);

    const svc = RingloomService.start(serviceConfig("node-topic-pub", broker));
    const client = svc.createClient("node-topic-pub");

    try {
      const publisher = client.registerTopicPublication("test-topic");
      assert.equal(typeof publisher.publish, "function");

      publisher.close();
      publisher.close();

      assert.throws(() => publisher.publish(Buffer.from("x")), /closed/i);

      success = true;
    } finally {
      client.close();
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
});

test("publisher publish and isAcked", () => {
  const root = repoRoot();
  const workspace = fs.mkdtempSync(
    path.join(os.tmpdir(), "ringloom-node-topic-"),
  );
  let success = false;
  let broker;

  try {
    broker = startBroker(root, workspace);

    const svc = RingloomService.start(serviceConfig("node-topic-ack", broker));
    const client = svc.createClient("node-topic-ack");

    try {
      const publisher = client.registerTopicPublication("ack-test");

      const status1 = publisher.publish(Buffer.from("hello"));
      assert.equal(typeof status1, "number");

      const status2 = publisher.publish(
        Buffer.from("hello"),
        TopicAckMode.REPLICATE_ONCE,
      );
      assert.equal(typeof status2, "number");

      assert.equal(typeof publisher.isAcked(0), "boolean");

      publisher.close();
      success = true;
    } finally {
      client.close();
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
});

test("topic subscription poll returns error when stubbed", () => {
  const root = repoRoot();
  const workspace = fs.mkdtempSync(
    path.join(os.tmpdir(), "ringloom-node-topic-"),
  );
  let success = false;
  let broker;

  try {
    broker = startBroker(root, workspace);

    const svc = RingloomService.start(serviceConfig("node-topic-sub", broker));
    const client = svc.createClient("node-topic-sub");

    try {
      assert.throws(() => client.subscribeTopic("test"), /subscribe|internal/i);

      success = true;
    } finally {
      client.close();
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
});

test("RingloomStatus NOT_READY and isOk", () => {
  assert.notEqual(RingloomStatus.NOT_READY, undefined);
  assert.equal(RingloomStatus.isOk(RingloomStatus.NOT_READY), false);
});
