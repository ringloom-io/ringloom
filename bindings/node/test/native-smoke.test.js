// SPDX-License-Identifier: Apache-2.0
"use strict";

const assert = require("node:assert/strict");
const test = require("node:test");

const { AeronPublicationStatus, RingloomStatus, native } = require("..");

test("loads native addon and exposes ABI basics", () => {
  assert.equal(native.abiVersion(), 4);
  assert.equal(native.statusName(RingloomStatus.OK), "ok");
  assert.match(native.statusName(RingloomStatus.INTERNAL), /internal/);
  assert.equal(
    native.aeronPublicationStatusName(AeronPublicationStatus.CLAIMED),
    "claimed",
  );
  assert.equal(typeof native.lastErrorMessage(), "string");
});
