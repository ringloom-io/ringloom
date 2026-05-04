// SPDX-License-Identifier: Apache-2.0
'use strict';

const native = require('./build/Release/ringloom_node.node');

const RingloomStatus = Object.freeze({
  OK: 0,
  INVALID_ARGUMENT: 1,
  OUT_OF_MEMORY: 2,
  BROKER_NOT_FOUND: 3,
  REGISTRATION_TIMEOUT: 4,
  BUFFER_FULL: 5,
  NO_AVAILABLE_INSTANCE: 6,
  BACKPRESSURE: 7,
  PEER_DISCONNECTED: 8,
  CLAIM_NOT_ACTIVE: 9,
  MESSAGE_TOO_LONG: 10,
  INTERNAL: 255,
  isOk(status) {
    return status === 0;
  },
});

class RingloomError extends Error {
  constructor(action, statusCode, statusName = native.statusName(statusCode), nativeMessage = native.lastErrorMessage()) {
    super(`${action} failed with ${statusName} (${statusCode}): ${nativeMessage}`);
    this.name = 'RingloomError';
    this.statusCode = statusCode;
    this.statusName = statusName;
    this.nativeMessage = nativeMessage;
  }
}

function throwForStatus(action, status) {
  if (RingloomStatus.isOk(status)) {
    return;
  }
  throw new RingloomError(action, status);
}

module.exports = {
  RingloomStatus,
  RingloomError,
  throwForStatus,
  RingloomService: native.RingloomService,
  RingloomClient: native.RingloomClient,
  MessageConsumer: native.MessageConsumer,
  BufferClaim: native.BufferClaim,
  native,
};
