// SPDX-License-Identifier: Apache-2.0
package io.ringloom.service;

import java.lang.foreign.*;
import java.util.*;
import static java.lang.foreign.ValueLayout.*;
/** Reader owns its sessions. Close sessions on their creating thread before closing the reader.
 * Keep the parent client and service alive until this reader is closed. */
public final class KvReader implements AutoCloseable {
    final MemorySegment handle;
    private boolean closed;
    private final Set<KvSession> sessions = new HashSet<>();
    KvReader(MemorySegment h) { handle=h; }
    synchronized void ensureOpen() { KvNative.guard(); if(closed)throw new IllegalStateException("KvReader closed"); }
    public synchronized KvSession newSession() {
        ensureOpen(); try(Arena a=Arena.ofConfined()) { var out=a.allocate(ADDRESS); KvNative.check("session_new",KvNative.session_new(handle,out));
            var s=new KvSession(this,out.get(ADDRESS,0)); sessions.add(s); return s; }
    }
    synchronized void removed(KvSession s) { sessions.remove(s); }
    public synchronized KvStatus status() {
        ensureOpen(); try(Arena a=Arena.ofConfined()) { var s=a.allocate(72,8); s.set(JAVA_INT,0,72); KvNative.check("reader_status",KvNative.reader_status(handle,s));
            return new KvStatus(Byte.toUnsignedInt(s.get(JAVA_BYTE,4)),s.get(JAVA_BYTE,5)!=0,s.get(JAVA_LONG,8),s.get(JAVA_LONG,16),s.get(JAVA_LONG,24),s.get(JAVA_LONG,32),s.get(JAVA_LONG,40),s.get(JAVA_LONG,48),s.get(JAVA_LONG,56),s.get(JAVA_LONG,64)); }
    }
    public synchronized void close() { KvNative.guard(); if(closed)return;
        if(!sessions.isEmpty())throw new IllegalStateException("Close sessions before reader");
        KvNative.close_reader(handle); closed=true; }
}
