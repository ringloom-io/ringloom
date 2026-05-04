// SPDX-License-Identifier: Apache-2.0
'use strict';

const assert = require('node:assert/strict');
const childProcess = require('node:child_process');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const test = require('node:test');

const { BufferClaim, RingloomService, RingloomStatus, throwForStatus } = require('..');

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

function startBroker(root, workspace) {
  const output = childProcess.execFileSync('bash', [
    path.join(root, 'scripts/start-test-broker.sh'),
    '--workspace',
    workspace,
    '--daemon',
    '--bin-dir',
    path.dirname(brokerBin(root)),
  ], {
    cwd: root,
    encoding: 'utf8',
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  return parseEnv(output);
}

function stopBroker(root, workspace) {
  childProcess.execFileSync('bash', [
    path.join(root, 'scripts/start-test-broker.sh'),
    '--workspace',
    workspace,
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
    brokerNodeId: 1,
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
