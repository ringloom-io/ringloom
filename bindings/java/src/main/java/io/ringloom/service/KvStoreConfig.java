// SPDX-License-Identifier: Apache-2.0
package io.ringloom.service;

import java.lang.foreign.*;
import static java.lang.foreign.ValueLayout.*;
import java.nio.charset.StandardCharsets;

/** Immutable store format configuration. TTL zero means no expiry. */
public record KvStoreConfig(int formatVersion, int maxKeyLength, long maxValueLength,
        long defaultTtlMillis, long mapSizeHint, String topicRollScheme, int flags) {
    public KvStoreConfig {
        if (formatVersion < 0 || formatVersion > 65535 || maxKeyLength < 1 || maxKeyLength > 65535 || maxValueLength < 0 || maxValueLength > 0xffffffffL || defaultTtlMillis < 0 || mapSizeHint < 0)
            throw new IllegalArgumentException("invalid KV limits");
        var b = topicRollScheme.getBytes(StandardCharsets.UTF_8);
        if (b.length == 0 || b.length > 16 || topicRollScheme.indexOf('\0') >= 0) throw new IllegalArgumentException("invalid roll scheme");
    }
    public static KvStoreConfig defaults() {
        try (Arena a = Arena.ofConfined()) {
            var s = a.allocate(80, 8); KvNative.store_config_init(s);
            byte[] b = s.asSlice(32,16).toArray(JAVA_BYTE); int n=0; while(n<16 && b[n]!=0)n++;
            return new KvStoreConfig(Short.toUnsignedInt(s.get(JAVA_SHORT,4)), Short.toUnsignedInt(s.get(JAVA_SHORT,8)), Integer.toUnsignedLong(s.get(JAVA_INT,12)), s.get(JAVA_LONG,16), s.get(JAVA_LONG,24), new String(b,0,n,StandardCharsets.UTF_8),s.get(JAVA_INT,48));
        }
    }
    MemorySegment allocate(Arena a) {
        var s=a.allocate(80,8); KvNative.store_config_init(s);
        s.set(JAVA_SHORT,4,(short)formatVersion); s.set(JAVA_SHORT,8,(short)maxKeyLength); s.set(JAVA_INT,12,(int)maxValueLength);
        s.set(JAVA_LONG,16,defaultTtlMillis); s.set(JAVA_LONG,24,mapSizeHint); s.asSlice(32,16).fill((byte)0);
        MemorySegment.copy(topicRollScheme.getBytes(StandardCharsets.UTF_8),0,s,JAVA_BYTE,32,topicRollScheme.getBytes(StandardCharsets.UTF_8).length);
        s.set(JAVA_INT,48,flags); return s;
    }
}
