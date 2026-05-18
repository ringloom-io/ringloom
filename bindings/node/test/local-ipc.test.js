// SPDX-License-Identifier: Apache-2.0
'use strict';

const assert = require('node:assert/strict');
const childProcess = require('node:child_process');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const test = require('node:test');

const { AeronPublicationStatus, BufferClaim, RingloomService, RingloomStatus, throwForStatus } = require('..');

function repoRoot() {
  return process.env.RINGLOOM_PROJECT_ROOT || path.resolve(__dirname, '../../..');
}

function brokerBin(root) {
  return process.env.RINGLOOM_BROKER_BIN || path.join(root, 'zig-out/bin/ringloom-broker');
}

function parseEnv(output) {
  const env = {};
  for (const line of output.split(/\r?\n/)) {
    const index = line.indexOf('=');
    if (index > 0) {
      env[line.slice(0, index)] = line.slice(index + 1);
    }
  }
  if (!env.RINGLOOM_STORAGE_PATH || !env.RINGLOOM_GROUP) {
    throw new Error(`broker script did not emit expected environment variables:\n${output}`);
  }
  return env;
}

function startBroker(root, workspace, options = {}) {
  const args = [
    path.join(root, 'scripts/start-test-broker.sh'),
    '--workspace',
    workspace,
    '--node-id',
    String(options.nodeId ?? 1),
    '--port',
    String(options.port ?? 19001),
    '--group',
    options.group ?? 'ringloom-java-test',
    '--daemon',
    '--bin-dir',
    path.dirname(brokerBin(root)),
  ];
  for (const peer of options.peers ?? []) {
    args.push('--peer', peer);
  }
  const output = childProcess.execFileSync('bash', args, {
    cwd: root,
    encoding: 'utf8',
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  return parseEnv(output);
}

function stopBroker(root, workspace, nodeId = 1) {
  childProcess.execFileSync('bash', [
    path.join(root, 'scripts/start-test-broker.sh'),
    '--workspace',
    workspace,
    '--node-id',
    String(nodeId),
    '--stop',
  ], {
    cwd: root,
    encoding: 'utf8',
    stdio: ['ignore', 'pipe', 'pipe'],
  });
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

test('two Node services communicate over local IPC', () => {
  const root = repoRoot();
  const workspace = fs.mkdtempSync(path.join(os.tmpdir(), 'ringloom-node-local-ipc-'));
  let success = false;
  let broker;

  try {
    broker = startBroker(root, workspace);

    const echo = RingloomService.start(serviceConfig('node-echo', broker));
    const consumer = echo.messageConsumer();
    const ping = RingloomService.start(serviceConfig('node-ping', broker));
    const client = ping.createClient('node-echo');
    const claim = new BufferClaim();

    try {
      assert.notEqual(ping.aeronDirectory(), '');
      assert.equal(ping.aeronInboundStreamId(), 0);
      assert.equal(client.lastAeronSendStatus(), AeronPublicationStatus.UNKNOWN);
      assert.equal(typeof ping.publicationConnected(), 'boolean');

      const payload = Buffer.from('hello');
      let sent = false;
      const sendDeadline = Date.now() + 5000;

      while (Date.now() < sendDeadline) {
        ping.pollControl(256);
        echo.pollControl(256);
        const status = client.tryClaim(7, payload.length, claim);
        if (status === RingloomStatus.OK) {
          payload.copy(claim.payloadBuffer());
          assert.equal(claim.commit(), RingloomStatus.OK);
          sent = true;
          break;
        }
        if (status !== RingloomStatus.NO_AVAILABLE_INSTANCE && status !== RingloomStatus.BUFFER_FULL) {
          throwForStatus('ringloom_client_try_claim', status);
        }
        sleep(20);
      }

      assert.equal(sent, true, 'message was not sent before deadline');

      let received;
      const receiveDeadline = Date.now() + 5000;
      while (Date.now() < receiveDeadline && received === undefined) {
        const work = consumer.poll((message) => {
          received = message.payload.toString('utf8');
          assert.equal(message.templateId, 7);
        }, 256);
        if (work < 0) {
          throwForStatus('ringloom_message_consumer_poll', consumer.lastStatus());
        }
        if (work === 0) {
          sleep(20);
        }
      }

      assert.equal(received, 'hello');
      assert.ok(client.targetServices().some((target) => target.targetServiceId > 0 && target.targetNodeId === 1));
      success = true;
    } finally {
      claim.close();
      client.close();
      ping.close();
      consumer.close();
      echo.close();
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

test('two Node services route remote payload over Aeron', () => {
  const root = repoRoot();
  const workspace = fs.mkdtempSync(path.join(os.tmpdir(), 'ringloom-node-remote-aeron-'));
  const group = 'ringloom-node-remote';
  let success = false;
  let brokerA;
  let brokerB;

  try {
    brokerA = startBroker(root, workspace, {
      nodeId: 1,
      port: 19201,
      group,
      peers: ['2@127.0.0.1:19202'],
    });
    brokerB = startBroker(root, workspace, {
      nodeId: 2,
      port: 19202,
      group,
      peers: ['1@127.0.0.1:19201'],
    });

    sleep(2000);

    const echo = RingloomService.start(serviceConfig('node-remote-echo', brokerB));
    const consumer = echo.messageConsumer();
    const ping = RingloomService.start(serviceConfig('node-remote-ping', brokerA));
    const client = ping.createClient('node-remote-echo');
    const claim = new BufferClaim();

    try {
      const target = awaitTarget(ping, echo, client, 2);
      const payload = Buffer.from('remote');
      let sent = false;
      const sendDeadline = Date.now() + 10_000;

      while (Date.now() < sendDeadline) {
        const status = client.tryClaimTo(target.targetNodeId, target.targetServiceId, 77, payload.length, claim);
        if (status === RingloomStatus.OK) {
          payload.copy(claim.payloadBuffer());
          assert.equal(claim.commit(), RingloomStatus.OK);
          sent = true;
          break;
        }
        if (
          status !== RingloomStatus.NO_AVAILABLE_INSTANCE &&
          status !== RingloomStatus.BACKPRESSURE &&
          status !== RingloomStatus.PEER_DISCONNECTED
        ) {
          throwForStatus('ringloom_client_try_claim_to', status);
        }
        ping.pollControl(256);
        echo.pollControl(256);
        sleep(20);
      }

      assert.equal(sent, true, 'remote claim was not committed before deadline');
      assert.equal(client.lastAeronSendStatus(), AeronPublicationStatus.CLAIMED);

      const message = pollMessage(consumer);
      assert.equal(message.payload.toString('utf8'), 'remote');
      assert.equal(message.templateId, 77);
      assert.equal(message.sourceNodeId, 1);
      assert.equal(message.targetNodeId, 2);
      success = true;
    } finally {
      claim.close();
      client.close();
      ping.close();
      consumer.close();
      echo.close();
    }
  } finally {
    if (brokerB) {
      stopBroker(root, workspace, 2);
    }
    if (brokerA) {
      stopBroker(root, workspace, 1);
    }
    if (success) {
      fs.rmSync(workspace, { recursive: true, force: true });
    } else {
      console.error(`Preserving RingLoom Node workspace: ${workspace}`);
    }
  }
});

function awaitTarget(ping, echo, client, targetNodeId) {
  const deadline = Date.now() + 10_000;
  while (Date.now() < deadline) {
    ping.pollControl(256);
    echo.pollControl(256);
    const target = client.targetServices().find((entry) => entry.targetNodeId === targetNodeId);
    if (target) {
      return target;
    }
    sleep(20);
  }
  throw new Error('timed out waiting for remote target discovery');
}

function pollMessage(consumer) {
  const deadline = Date.now() + 10_000;
  let received;
  while (Date.now() < deadline && received === undefined) {
    const work = consumer.poll((message) => {
      received = {
        ...message,
        payload: Buffer.from(message.payload),
      };
    }, 256);
    if (work < 0) {
      throwForStatus('ringloom_message_consumer_poll', consumer.lastStatus());
    }
    if (received === undefined) {
      sleep(20);
    }
  }
  assert.notEqual(received, undefined, 'timed out waiting for remote payload');
  return received;
}
