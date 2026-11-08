// SPDX-License-Identifier: Apache-2.0
package io.ringloom.service;

import java.lang.foreign.*;
import static java.lang.foreign.ValueLayout.*;

/** Reusable, caller-owned target for {@link KvReader#status(KvStatusHolder)}. Backed by a preallocated
 * 72-byte native segment; filling it allocates nothing. Not thread-safe: use one holder per thread. */
public final class KvStatusHolder {
    static final long SIZE = 72;
    final MemorySegment segment = Arena.ofAuto().allocate(SIZE, 8);
    private int state;
    private boolean replicationLive;
    private long generation, appliedIndex, appliedLeaderEpoch, appliedAtNanos, localLogTipIndex, lagEntries, heartbeatAgeMillis, generationSwitches;

    void prepare() { segment.set(JAVA_INT, 0, (int) SIZE); }
    void load() {
        state = Byte.toUnsignedInt(segment.get(JAVA_BYTE, 4)); replicationLive = segment.get(JAVA_BYTE, 5) != 0;
        generation = segment.get(JAVA_LONG, 8); appliedIndex = segment.get(JAVA_LONG, 16); appliedLeaderEpoch = segment.get(JAVA_LONG, 24);
        appliedAtNanos = segment.get(JAVA_LONG, 32); localLogTipIndex = segment.get(JAVA_LONG, 40); lagEntries = segment.get(JAVA_LONG, 48);
        heartbeatAgeMillis = segment.get(JAVA_LONG, 56); generationSwitches = segment.get(JAVA_LONG, 64);
    }
    /** 0=initializing, 1=catching_up, 2=live, 3=rebuilding, 4=failed. */
    public int state() { return state; }
    public boolean replicationLive() { return replicationLive; }
    public long generation() { return generation; }
    public long appliedIndex() { return appliedIndex; }
    public long appliedLeaderEpoch() { return appliedLeaderEpoch; }
    public long appliedAtNanos() { return appliedAtNanos; }
    public long localLogTipIndex() { return localLogTipIndex; }
    public long lagEntries() { return lagEntries; }
    /** Unsigned; -1 (all bits set) means the applier never beat. */
    public long heartbeatAgeMillis() { return heartbeatAgeMillis; }
    public long generationSwitches() { return generationSwitches; }
    /** Copies into the immutable record form (allocates). */
    public KvStatus toStatus() {
        return new KvStatus(state, replicationLive, generation, appliedIndex, appliedLeaderEpoch, appliedAtNanos, localLogTipIndex, lagEntries, heartbeatAgeMillis, generationSwitches);
    }
}
