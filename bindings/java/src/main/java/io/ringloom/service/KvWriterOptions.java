// SPDX-License-Identifier: Apache-2.0
package io.ringloom.service;

import java.lang.foreign.*;
import static java.lang.foreign.ValueLayout.*;
/** acquire: 0 fail if held, 1 queue and wait. Callback retained until writer close. */
public record KvWriterOptions(int acquire, long openTimeoutMillis, long maxFrameLength, KvStateHandler onState) {
    public KvWriterOptions {
        if (acquire < 0 || acquire > 1 || openTimeoutMillis < 0 || openTimeoutMillis > 0xffffffffL || maxFrameLength < 0 || maxFrameLength > 0xffffffffL) throw new IllegalArgumentException("invalid writer options");
    }
    public static KvWriterOptions defaults() {
        try(Arena a=Arena.ofConfined()) { var s=a.allocate(32,8); KvNative.writer_options_init(s);
            return new KvWriterOptions(s.get(JAVA_BYTE,4),Integer.toUnsignedLong(s.get(JAVA_INT,8)),Integer.toUnsignedLong(s.get(JAVA_INT,12)),null); }
    }
    MemorySegment allocate(Arena a, MemorySegment callback) { var s=a.allocate(32,8); KvNative.writer_options_init(s);
        s.set(JAVA_BYTE,4,(byte)acquire); s.set(JAVA_INT,8,(int)openTimeoutMillis); s.set(JAVA_INT,12,(int)maxFrameLength); s.set(ADDRESS,16,callback); return s; }
}
