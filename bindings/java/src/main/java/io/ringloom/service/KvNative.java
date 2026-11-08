// SPDX-License-Identifier: Apache-2.0
package io.ringloom.service;

import java.lang.foreign.*;
import java.lang.invoke.*;
import static java.lang.foreign.ValueLayout.*;

final class KvNative {
    private static final MethodHandle STORE_CONFIG_INIT = RingloomNative.downcall("ringloom_kv_store_config_init", FunctionDescriptor.ofVoid(ADDRESS));
    static void store_config_init(MemorySegment x0) {
        try { STORE_CONFIG_INIT.invokeExact(x0); } catch (Throwable t) { throw new IllegalStateException("ringloom_kv_store_config_init", t); }
    }
    private static final MethodHandle READER_OPTIONS_INIT = RingloomNative.downcall("ringloom_kv_reader_options_init", FunctionDescriptor.ofVoid(ADDRESS));
    static void reader_options_init(MemorySegment x0) {
        try { READER_OPTIONS_INIT.invokeExact(x0); } catch (Throwable t) { throw new IllegalStateException("ringloom_kv_reader_options_init", t); }
    }
    private static final MethodHandle WRITER_OPTIONS_INIT = RingloomNative.downcall("ringloom_kv_writer_options_init", FunctionDescriptor.ofVoid(ADDRESS));
    static void writer_options_init(MemorySegment x0) {
        try { WRITER_OPTIONS_INIT.invokeExact(x0); } catch (Throwable t) { throw new IllegalStateException("ringloom_kv_writer_options_init", t); }
    }
    private static final MethodHandle OPEN_READER = RingloomNative.downcall("ringloom_kv_open_reader", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, ADDRESS, ADDRESS));
    static int open_reader(MemorySegment x0, MemorySegment x1, MemorySegment x2, MemorySegment x3) {
        try { return (int) OPEN_READER.invokeExact(x0, x1, x2, x3); } catch (Throwable t) { throw new IllegalStateException("ringloom_kv_open_reader", t); }
    }
    private static final MethodHandle READER_STATUS = RingloomNative.downcall("ringloom_kv_reader_status", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS));
    static int reader_status(MemorySegment x0, MemorySegment x1) {
        try { return (int) READER_STATUS.invokeExact(x0, x1); } catch (Throwable t) { throw new IllegalStateException("ringloom_kv_reader_status", t); }
    }
    private static final MethodHandle SESSION_NEW = RingloomNative.downcall("ringloom_kv_session_new", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS));
    static int session_new(MemorySegment x0, MemorySegment x1) {
        try { return (int) SESSION_NEW.invokeExact(x0, x1); } catch (Throwable t) { throw new IllegalStateException("ringloom_kv_session_new", t); }
    }
    static { if (ADDRESS.byteSize() != 8) throw new ExceptionInInitializerError("KV raw-address downcalls require 64-bit pointers"); }
    /** get never blocks behind Java code and never upcalls, so it is linked critical with heap access:
     * the ADDRESS form accepts heap segments (MemorySegment.ofArray). */
    private static final MethodHandle GET = RingloomNative.downcall("ringloom_kv_get", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, JAVA_LONG, ADDRESS, JAVA_LONG, ADDRESS), Linker.Option.critical(true));
    private static final MethodHandle GET_RAW = RingloomNative.downcall("ringloom_kv_get", FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_LONG, JAVA_LONG, JAVA_LONG, JAVA_LONG, JAVA_LONG), Linker.Option.critical(false));
    static int get_raw(MemorySegment session, long key, long keyLength, long out, long capacity, long outLength) {
        try { return (int) GET_RAW.invokeExact(session, key, keyLength, out, capacity, outLength); } catch (Throwable t) { throw new IllegalStateException("ringloom_kv_get", t); }
    }
    // put/delete are deliberately not critical: they spin on the native KvContext lock, which the control
    // thread holds while upcalling the Java state callback; a critical call would block the safepoint
    // that callback may need (deadlock). They therefore require native segments.
    private static final MethodHandle PUT_RAW = RingloomNative.downcall("ringloom_kv_put", FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_LONG, JAVA_LONG, JAVA_LONG, JAVA_LONG, JAVA_LONG, JAVA_BYTE, JAVA_LONG));
    static int put_raw(MemorySegment writer, long key, long keyLength, long value, long valueLength, long ttl, byte ack, long outSeq) {
        try { return (int) PUT_RAW.invokeExact(writer, key, keyLength, value, valueLength, ttl, ack, outSeq); } catch (Throwable t) { throw new IllegalStateException("ringloom_kv_put", t); }
    }
    private static final MethodHandle DELETE_RAW = RingloomNative.downcall("ringloom_kv_delete", FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_LONG, JAVA_LONG, JAVA_BYTE, JAVA_LONG));
    static int delete_raw(MemorySegment writer, long key, long keyLength, byte ack, long outSeq) {
        try { return (int) DELETE_RAW.invokeExact(writer, key, keyLength, ack, outSeq); } catch (Throwable t) { throw new IllegalStateException("ringloom_kv_delete", t); }
    }
    private static final MethodHandle READER_CONFIG = RingloomNative.downcall("ringloom_kv_reader_config", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS));
    static int reader_config(MemorySegment x0, MemorySegment x1) {
        try { return (int) READER_CONFIG.invokeExact(x0, x1); } catch (Throwable t) { throw new IllegalStateException("ringloom_kv_reader_config", t); }
    }
    private static final MethodHandle WRITER_CONFIG = RingloomNative.downcall("ringloom_kv_writer_config", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS));
    static int writer_config(MemorySegment x0, MemorySegment x1) {
        try { return (int) WRITER_CONFIG.invokeExact(x0, x1); } catch (Throwable t) { throw new IllegalStateException("ringloom_kv_writer_config", t); }
    }
    private static final MethodHandle WRITER_ACKED_SEQ = RingloomNative.downcall("ringloom_kv_writer_acked_seq", FunctionDescriptor.of(JAVA_LONG, ADDRESS));
    static long writer_acked_seq(MemorySegment x0) {
        try { return (long) WRITER_ACKED_SEQ.invokeExact(x0); } catch (Throwable t) { throw new IllegalStateException("ringloom_kv_writer_acked_seq", t); }
    }
    private static final MethodHandle WRITER_LEADER_EPOCH = RingloomNative.downcall("ringloom_kv_writer_leader_epoch", FunctionDescriptor.of(JAVA_LONG, ADDRESS));
    static long writer_leader_epoch(MemorySegment x0) {
        try { return (long) WRITER_LEADER_EPOCH.invokeExact(x0); } catch (Throwable t) { throw new IllegalStateException("ringloom_kv_writer_leader_epoch", t); }
    }
    private static final MethodHandle WRITER_EPOCH = RingloomNative.downcall("ringloom_kv_writer_epoch", FunctionDescriptor.of(JAVA_LONG, ADDRESS));
    static long writer_epoch(MemorySegment x0) {
        try { return (long) WRITER_EPOCH.invokeExact(x0); } catch (Throwable t) { throw new IllegalStateException("ringloom_kv_writer_epoch", t); }
    }
    private static final MethodHandle ABANDON_BATCH = RingloomNative.downcall("ringloom_kv_abandon_batch", FunctionDescriptor.of(JAVA_INT, ADDRESS));
    static int abandon_batch(MemorySegment x0) {
        try { return (int) ABANDON_BATCH.invokeExact(x0); } catch (Throwable t) { throw new IllegalStateException("ringloom_kv_abandon_batch", t); }
    }
    static int get(MemorySegment x0, MemorySegment x1, long x2, MemorySegment x3, long x4, MemorySegment x5) {
        try { return (int) GET.invokeExact(x0, x1, x2, x3, x4, x5); } catch (Throwable t) { throw new IllegalStateException("ringloom_kv_get", t); }
    }
    private static final MethodHandle VIEW = RingloomNative.downcall("ringloom_kv_view", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, JAVA_LONG, ADDRESS, ADDRESS));
    static int view(MemorySegment x0, MemorySegment x1, long x2, MemorySegment x3, MemorySegment x4) {
        try { return (int) VIEW.invokeExact(x0, x1, x2, x3, x4); } catch (Throwable t) { throw new IllegalStateException("ringloom_kv_view", t); }
    }
    private static final MethodHandle RELEASE = RingloomNative.downcall("ringloom_kv_release", FunctionDescriptor.ofVoid(ADDRESS));
    static void release(MemorySegment x0) {
        try { RELEASE.invokeExact(x0); } catch (Throwable t) { throw new IllegalStateException("ringloom_kv_release", t); }
    }
    private static final MethodHandle FOR_EACH = RingloomNative.downcall("ringloom_kv_for_each", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, ADDRESS));
    static int for_each(MemorySegment x0, MemorySegment x1, MemorySegment x2) {
        try { return (int) FOR_EACH.invokeExact(x0, x1, x2); } catch (Throwable t) { throw new IllegalStateException("ringloom_kv_for_each", t); }
    }
    private static final MethodHandle SESSION_FREE = RingloomNative.downcall("ringloom_kv_session_free", FunctionDescriptor.ofVoid(ADDRESS));
    static void session_free(MemorySegment x0) {
        try { SESSION_FREE.invokeExact(x0); } catch (Throwable t) { throw new IllegalStateException("ringloom_kv_session_free", t); }
    }
    private static final MethodHandle CLOSE_READER = RingloomNative.downcall("ringloom_kv_close_reader", FunctionDescriptor.ofVoid(ADDRESS));
    static void close_reader(MemorySegment x0) {
        try { CLOSE_READER.invokeExact(x0); } catch (Throwable t) { throw new IllegalStateException("ringloom_kv_close_reader", t); }
    }
    private static final MethodHandle OPEN_WRITER = RingloomNative.downcall("ringloom_kv_open_writer", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, ADDRESS, ADDRESS, ADDRESS));
    static int open_writer(MemorySegment x0, MemorySegment x1, MemorySegment x2, MemorySegment x3, MemorySegment x4) {
        try { return (int) OPEN_WRITER.invokeExact(x0, x1, x2, x3, x4); } catch (Throwable t) { throw new IllegalStateException("ringloom_kv_open_writer", t); }
    }
    private static final MethodHandle PUT = RingloomNative.downcall("ringloom_kv_put", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, JAVA_LONG, ADDRESS, JAVA_LONG, JAVA_LONG, JAVA_BYTE, ADDRESS));
    static int put(MemorySegment x0, MemorySegment x1, long x2, MemorySegment x3, long x4, long x5, byte x6, MemorySegment x7) {
        try { return (int) PUT.invokeExact(x0, x1, x2, x3, x4, x5, x6, x7); } catch (Throwable t) { throw new IllegalStateException("ringloom_kv_put", t); }
    }
    private static final MethodHandle DELETE = RingloomNative.downcall("ringloom_kv_delete", FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, JAVA_LONG, JAVA_BYTE, ADDRESS));
    static int delete(MemorySegment x0, MemorySegment x1, long x2, byte x3, MemorySegment x4) {
        try { return (int) DELETE.invokeExact(x0, x1, x2, x3, x4); } catch (Throwable t) { throw new IllegalStateException("ringloom_kv_delete", t); }
    }
    private static final MethodHandle CLEAR = RingloomNative.downcall("ringloom_kv_clear", FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_BYTE, ADDRESS));
    static int clear(MemorySegment x0, byte x1, MemorySegment x2) {
        try { return (int) CLEAR.invokeExact(x0, x1, x2); } catch (Throwable t) { throw new IllegalStateException("ringloom_kv_clear", t); }
    }
    private static final MethodHandle BEGIN_BATCH = RingloomNative.downcall("ringloom_kv_begin_batch", FunctionDescriptor.of(JAVA_INT, ADDRESS));
    static int begin_batch(MemorySegment x0) {
        try { return (int) BEGIN_BATCH.invokeExact(x0); } catch (Throwable t) { throw new IllegalStateException("ringloom_kv_begin_batch", t); }
    }
    private static final MethodHandle COMMIT_BATCH = RingloomNative.downcall("ringloom_kv_commit_batch", FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_BYTE, ADDRESS));
    static int commit_batch(MemorySegment x0, byte x1, MemorySegment x2) {
        try { return (int) COMMIT_BATCH.invokeExact(x0, x1, x2); } catch (Throwable t) { throw new IllegalStateException("ringloom_kv_commit_batch", t); }
    }
    private static final MethodHandle IS_ACKED = RingloomNative.downcall("ringloom_kv_is_acked", FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_LONG));
    static int is_acked(MemorySegment x0, long x1) {
        try { return (int) IS_ACKED.invokeExact(x0, x1); } catch (Throwable t) { throw new IllegalStateException("ringloom_kv_is_acked", t); }
    }
    private static final MethodHandle WRITER_STATE = RingloomNative.downcall("ringloom_kv_writer_state", FunctionDescriptor.of(JAVA_INT, ADDRESS));
    static int writer_state(MemorySegment x0) {
        try { return (int) WRITER_STATE.invokeExact(x0); } catch (Throwable t) { throw new IllegalStateException("ringloom_kv_writer_state", t); }
    }
    private static final MethodHandle CLOSE_WRITER = RingloomNative.downcall("ringloom_kv_close_writer", FunctionDescriptor.ofVoid(ADDRESS));
    static void close_writer(MemorySegment x0) {
        try { CLOSE_WRITER.invokeExact(x0); } catch (Throwable t) { throw new IllegalStateException("ringloom_kv_close_writer", t); }
    }
    static void check(String op, int status) { RingloomNative.throwForStatus("ringloom_kv_" + op, status); }
    static void guard() { KvCallbackGuard.check(); }
    /** For critical(true) downcalls: heap segments are allowed, read-only checks are the caller's job. */
    static MemorySegment heapOrNative(MemorySegment s) { return java.util.Objects.requireNonNull(s, "segment"); }
    static MemorySegment pointer(MemorySegment s) { java.util.Objects.requireNonNull(s, "segment"); if (!s.isNative()) throw new IllegalArgumentException("KV requires native memory segments"); return s; }
    static MemorySegment name(Arena a, String name) {
        var bytes = java.util.Objects.requireNonNull(name).getBytes(java.nio.charset.StandardCharsets.UTF_8);
        if (bytes.length == 0 || bytes.length > 64 || name.indexOf('\0') >= 0) throw new IllegalArgumentException("name must be 1..64 UTF-8 bytes without NUL");
        return a.allocateFrom(name);
    }
}
