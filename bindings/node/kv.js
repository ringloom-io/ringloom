// SPDX-License-Identifier: Apache-2.0
"use strict";

module.exports = function installKv(native) {
  const handles = new WeakMap();
  function options(value) {
    if (value === undefined) return {};
    if (!value || typeof value !== "object" || Array.isArray(value)) {
      throw new TypeError("options/config must be an object");
    }
    return value;
  }
  function bytes(value) {
    if (!(value instanceof Uint8Array) && !(value instanceof Uint8ClampedArray)) {
      throw new TypeError("key/value/output must be a Buffer or byte typed array");
    }
    return value;
  }
  // The native handle exposes only length and at(), never the mapped buffer.
  function readonlyBytes(handle) {
    const methods = Object.freeze({
      at: index => handle.at(index),
      [Symbol.iterator]: function* () {
        for (let i = 0; i < handle.length; i++) yield handle.at(i);
      },
    });
    const reject = () => { throw new TypeError("borrowed KV bytes are read-only"); };
    return new Proxy(Object.create(null), {
      get(_target, key) {
        if (key === "length" || key === "byteLength") return handle.length;
        if (typeof key === "string" && /^(0|[1-9][0-9]*)$/.test(key)) return handle.at(Number(key));
        if (key === "set" || key === "fill" || key === "copyWithin" || key === "reverse" || key === "sort") return reject;
        return methods[key];
      },
      has(_target, key) {
        if (typeof key === "string" && /^(0|[1-9][0-9]*)$/.test(key)) return Number(key) < handle.length;
        return key === "length" || key === "byteLength" || key in methods;
      },
      set: reject, defineProperty: reject, deleteProperty: reject, setPrototypeOf: reject,
    });
  }
  class KvHandle {
    constructor(handle) {
      if (!handle || typeof handle.call !== "function") throw new TypeError("KV handles are created by RingloomClient");
      handles.set(this, handle);
    }
    _call(op, ...args) {
      const handle = handles.get(this);
      if (!handle) throw new Error("KV handle is closed");
      return handle.call(op, ...args);
    }
    close() {
      const handle = handles.get(this);
      if (handle) { handle.call("close"); handles.delete(this); }
    }
  }
  class KvReader extends KvHandle {
    status() { return this._call("status"); }
    newSession() { return new KvReadSession(this._call("newSession")); }
  }
  class KvReadSession extends KvHandle {
    get(key, output) { return this._call("get", bytes(key), bytes(output)); }
    view(key) {
      const result = this._call("view", bytes(key));
      if (result.value !== null) result.value = readonlyBytes(result.value);
      return result;
    }
    release() { return this._call("release"); }
    forEach(callback) {
      if (typeof callback !== "function") throw new TypeError("forEach requires a callback");
      return this._call("forEach", callback);
    }
  }
  class KvWriter extends KvHandle {
    put(key, value, opts) { return this._call("put", bytes(key), bytes(value), options(opts)); }
    delete(key, opts) { return this._call("delete", bytes(key), undefined, options(opts)); }
    clear(opts) { return this._call("clear", undefined, undefined, options(opts)); }
    beginBatch() { return this._call("beginBatch"); }
    commitBatch(opts) { return this._call("commitBatch", undefined, undefined, options(opts)); }
    isAcked(seq) { return this._call("isAcked", seq); }
    state() { return this._call("state"); }
  }
  native.RingloomClient.prototype.openKvReader = function(name, opts) {
    return new KvReader(this._openKv(false, name, {}, options(opts)));
  };
  native.RingloomClient.prototype.openKvWriter = function(name, config, opts) {
    return new KvWriter(this._openKv(true, name, options(config), options(opts)));
  };
  return {
    KvReader, KvReadSession, KvWriter,
    KvAcquire: Object.freeze({ FAIL_IF_HELD: 0, WAIT: 1 }),
    KvTtlClock: Object.freeze({ PRECISE: 0, COARSE: 1 }),
    KvWriterState: Object.freeze({ GRANTED: 0, QUEUED: 1, REVOKED: 2, REASSERTING: 3 }),
    KvStoreState: Object.freeze({ INITIALIZING: 0, CATCHING_UP: 1, LIVE: 2, REBUILDING: 3, FAILED: 4 }),
  };
};
