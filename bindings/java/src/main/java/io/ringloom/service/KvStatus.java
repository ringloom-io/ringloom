// SPDX-License-Identifier: Apache-2.0
package io.ringloom.service;

/** Reader state and local replication progress. Numeric counters preserve unsigned ABI bits. */
public record KvStatus(int state, boolean replicationLive, long generation, long appliedIndex,
        long appliedLeaderEpoch, long appliedAtNanos, long localLogTipIndex, long lagEntries,
        long heartbeatAgeMillis, long generationSwitches) {}
