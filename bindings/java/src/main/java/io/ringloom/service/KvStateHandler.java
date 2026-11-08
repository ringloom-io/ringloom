// SPDX-License-Identifier: Apache-2.0
package io.ringloom.service;

/** Runs synchronously during native control-plane progress. Must not re-enter KV APIs. */
@FunctionalInterface
public interface KvStateHandler { void onState(int state, long writerEpoch); }
