// SPDX-License-Identifier: Apache-2.0
package io.ringloom.service;

import java.lang.foreign.*;
import static java.lang.foreign.ValueLayout.*;
/** Reader startup options; ttlClock: 0 precise, 1 coarse. Unsigned u32 fields use long. */
public record KvReaderOptions(long waitReadyTimeoutMillis, boolean prefault, int ttlClock, long maxReaders) {
    public KvReaderOptions {
        if (waitReadyTimeoutMillis < 0 || waitReadyTimeoutMillis > 0xffffffffL || ttlClock < 0 || ttlClock > 1 || maxReaders < 1 || maxReaders > 0xffffffffL) throw new IllegalArgumentException("invalid reader options");
    }
    public static KvReaderOptions defaults() {
        try(Arena a=Arena.ofConfined()) { var s=a.allocate(16,4); KvNative.reader_options_init(s);
            return new KvReaderOptions(Integer.toUnsignedLong(s.get(JAVA_INT,4)),s.get(JAVA_BYTE,8)!=0,s.get(JAVA_BYTE,9),Integer.toUnsignedLong(s.get(JAVA_INT,12))); }
    }
    MemorySegment allocate(Arena a) { var s=a.allocate(16,4); KvNative.reader_options_init(s);
        s.set(JAVA_INT,4,(int)waitReadyTimeoutMillis); s.set(JAVA_BYTE,8,(byte)(prefault?1:0)); s.set(JAVA_BYTE,9,(byte)ttlClock); s.set(JAVA_INT,12,(int)maxReaders); return s; }
}
