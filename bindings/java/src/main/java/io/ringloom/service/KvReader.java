// SPDX-License-Identifier: Apache-2.0
package io.ringloom.service;

import java.lang.foreign.*;
import java.util.*;
import static java.lang.foreign.ValueLayout.*;
/** Reader owns its sessions. Close every session before closing the reader ({@link #close} throws
 * otherwise; {@link #openSessionCount} reports leaks). Sessions default to
 * {@link KvSessionMode#SERIAL}. Keep the parent client and service alive until this reader is closed. */
public final class KvReader implements AutoCloseable {
    final MemorySegment handle;
    private volatile boolean closed;
    private final Set<KvSession> sessions = new HashSet<>();
    private KvStoreConfig config;
    KvReader(MemorySegment h) { handle=h; }
    void ensureOpen() { KvNative.guard(); if(closed)throw new IllegalStateException("KvReader closed"); }
    /** Creates a {@link KvSessionMode#SERIAL} session. */
    public KvSession newSession() { return newSession(KvSessionMode.SERIAL); }
    public synchronized KvSession newSession(KvSessionMode mode) {
        Objects.requireNonNull(mode, "mode");
        ensureOpen(); try(Arena a=Arena.ofConfined()) { var out=a.allocate(ADDRESS); KvNative.check("session_new",KvNative.session_new(handle,out));
            var raw=out.get(ADDRESS,0); KvSession s;
            try { s=new KvSession(this,raw,mode); } catch(Throwable t) { KvNative.session_free(raw); throw t; }
            sessions.add(s); return s; }
    }
    synchronized void removed(KvSession s) { sessions.remove(s); }
    /** Sessions created by this reader and not yet closed. */
    public synchronized int openSessionCount() { return sessions.size(); }
    /** Effective immutable store config captured when the reader opened (limits come from the store). */
    public synchronized KvStoreConfig config() {
        ensureOpen();
        if (config == null) {
            try(Arena a=Arena.ofConfined()) { var s=KvStoreConfig.allocateOut(a); KvNative.check("reader_config",KvNative.reader_config(handle,s)); config=KvStoreConfig.read(s); }
        }
        return config;
    }
    public KvStatus status() { var h=new KvStatusHolder(); KvNative.check("reader_status",status(h)); return h.toStatus(); }
    /** Allocation-free status read into a caller-owned reusable holder. Returns the native status code;
     * the holder is updated only on OK. */
    public synchronized int status(KvStatusHolder out) {
        Objects.requireNonNull(out, "out"); ensureOpen(); out.prepare();
        int status=KvNative.reader_status(handle,out.segment);
        if(status==RingloomStatus.OK) out.load();
        return status;
    }
    public synchronized void close() { KvNative.guard(); if(closed)return;
        if(!sessions.isEmpty())throw new IllegalStateException("Close sessions before reader ("+sessions.size()+" open)");
        KvNative.close_reader(handle); closed=true; }
}
