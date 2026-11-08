// SPDX-License-Identifier: Apache-2.0
package io.ringloom.service;

import java.lang.foreign.*;
import java.lang.invoke.*;
import java.util.Objects;
import static java.lang.foreign.ValueLayout.*;

/** Reusable read session. get copies into caller-owned memory; view pins a snapshot shared by
 * subsequent reads until release.
 *
 * <p>Threading depends on {@link KvSessionMode}. In the default {@link KvSessionMode#SERIAL} mode any
 * thread may use the session, one at a time: a call overlapping another throws
 * {@link IllegalStateException} ("KvSession used concurrently"). While a {@link #view} snapshot is held
 * the session is pinned to the viewing thread and calls from other threads throw until that thread calls
 * {@link #release}. {@link #close} may be called from any thread while the session is idle and not pinned
 * by another thread. In {@link KvSessionMode#CONFINED} mode every call must come from the creating
 * thread.
 *
 * <p>The native session uses MDB_NOTLS, so moving it between threads is safe as long as uses do not
 * overlap. The in-use flag is a compare-and-set, not a lock: it detects misuse rather than waiting. */
public final class KvSession implements AutoCloseable {
    private static final VarHandle IN_USE;
    private static final MethodHandle VISIT;
    private static final FunctionDescriptor VISIT_DESCRIPTOR = FunctionDescriptor.of(JAVA_INT, JAVA_LONG, JAVA_LONG, JAVA_LONG, JAVA_LONG, JAVA_LONG);
    static {
        try {
            var lookup = MethodHandles.lookup();
            IN_USE = lookup.findVarHandle(KvSession.class, "inUse", int.class);
            VISIT = lookup.findVirtual(Visit.class, "call", MethodType.methodType(int.class, long.class, long.class, long.class, long.class, long.class));
        } catch (ReflectiveOperationException e) { throw new ExceptionInInitializerError(e); }
    }

    /** Test hook: upcall stubs created by sessions (one per session, never per call). */
    static final java.util.concurrent.atomic.AtomicLong UPCALL_STUBS_CREATED = new java.util.concurrent.atomic.AtomicLong();

    private final KvReader reader;
    private final MemorySegment handle;
    private final KvSessionMode mode;
    private final Thread owner;
    private final Arena scratch = Arena.ofShared();
    private final MemorySegment length = scratch.allocate(JAVA_LONG), pointer = scratch.allocate(ADDRESS);
    private final long lengthAddress = length.address();
    private final Visit visit = new Visit();
    private final MemorySegment visitStub;
    @SuppressWarnings("unused") private volatile int inUse;
    private volatile Thread pinnedBy;
    private volatile boolean closed;
    private Arena views;

    KvSession(KvReader r, MemorySegment h, KvSessionMode mode) {
        reader = r; handle = h; this.mode = mode;
        owner = mode == KvSessionMode.CONFINED ? Thread.currentThread() : null;
        try { visitStub = RingloomNative.LINKER.upcallStub(VISIT.bindTo(visit), VISIT_DESCRIPTOR, scratch); UPCALL_STUBS_CREATED.incrementAndGet(); }
        catch (Throwable t) { scratch.close(); throw t; }
    }

    public KvSessionMode mode() { return mode; }

    private void enter() {
        KvNative.guard();
        Thread current = Thread.currentThread();
        if (owner != null && current != owner) throw new WrongThreadException("KV session belongs to its creating thread");
        if (closed) throw new IllegalStateException("KvSession closed");
        if (!IN_USE.compareAndSet(this, 0, 1)) throw new IllegalStateException("KvSession used concurrently");
        Thread pin = pinnedBy;
        if (closed || (pin != null && pin != current)) {
            IN_USE.setRelease(this, 0);
            if (closed) throw new IllegalStateException("KvSession closed");
            throw new IllegalStateException("KvSession pinned by a view held on another thread");
        }
    }
    private void exit() { IN_USE.setRelease(this, 0); }

    /** Returns native status; outLength[0] is required length on OK or BUFFER_TOO_SMALL.
     * Missing returns KV_MISSING, distinct from OK with length zero. Heap segments
     * ({@code MemorySegment.ofArray}) are accepted. No allocation for native segments. */
    public int get(MemorySegment key, MemorySegment destination, long[] outLength) {
        Objects.requireNonNull(key, "key"); Objects.requireNonNull(destination, "destination");
        return get(key, 0, key.byteSize(), destination, 0, destination.byteSize(), outLength);
    }

    /** Offset form of {@link #get(MemorySegment, MemorySegment, long[])}. Reads the key from
     * {@code key[keyOffset, keyOffset+keyLength)} and copies at most {@code destinationCapacity} bytes to
     * {@code destination[destinationOffset, ...)}. With native segments it passes raw addresses and
     * allocates nothing; heap segments are accepted (sliced when the range is not the whole segment). */
    public int get(MemorySegment key, long keyOffset, long keyLength,
                   MemorySegment destination, long destinationOffset, long destinationCapacity,
                   long[] outLength) {
        Objects.requireNonNull(key, "key"); Objects.requireNonNull(destination, "destination"); holder(outLength);
        Objects.checkFromIndexSize(keyOffset, keyLength, key.byteSize());
        Objects.checkFromIndexSize(destinationOffset, destinationCapacity, destination.byteSize());
        if (destination.isReadOnly()) throw new IllegalArgumentException("destination is read-only");
        enter();
        try {
            length.set(JAVA_LONG, 0, 0);
            int status;
            if (key.isNative() && destination.isNative()) {
                status = KvNative.get_raw(handle, key.address() + keyOffset, keyLength, destination.address() + destinationOffset, destinationCapacity, lengthAddress);
            } else {
                status = KvNative.get(handle, slice(key, keyOffset, keyLength), keyLength, slice(destination, destinationOffset, destinationCapacity), destinationCapacity, length);
            }
            outLength[0] = length.get(JAVA_LONG, 0);
            return status;
        } finally { exit(); }
    }

    private static MemorySegment slice(MemorySegment s, long offset, long len) {
        return offset == 0 && len == s.byteSize() ? s : s.asSlice(offset, len);
    }

    /** Returns a read-only scoped borrowed value, or null for missing. Allocates a segment wrapper.
     * All returned views become inaccessible on release or close, including slices. While a view is held
     * the session is pinned to this thread. */
    public MemorySegment view(MemorySegment key) {
        enter();
        try {
            int status = KvNative.view(handle, KvNative.pointer(key), key.byteSize(), pointer, length);
            if (status == RingloomStatus.KV_MISSING) return null;
            KvNative.check("view", status);
            if (views == null) views = Arena.ofConfined();
            pinnedBy = Thread.currentThread();
            return pointer.get(ADDRESS, 0).reinterpret(length.get(JAVA_LONG, 0), views, null).asReadOnly();
        } finally { exit(); }
    }

    /** Ends the snapshot behind outstanding views and unpins the session. */
    public void release() {
        enter();
        try { releaseHeld(); } finally { exit(); }
    }
    private void releaseHeld() {
        if (views != null) { views.close(); views = null; }
        pinnedBy = null;
        KvNative.release(handle);
    }

    /** Visits every live entry. Key and value segments are read-only, global-scope views built per
     * entry (two small wrapper objects); they must not be used after the callback returns, because the
     * memory behind them is released then and access is not checked. Use {@link #forEachRaw} for
     * allocation-free iteration. Visitor exceptions are rethrown wrapped in IllegalStateException. */
    public int forEach(KvVisitor visitor) {
        Objects.requireNonNull(visitor);
        enter();
        try {
            visit.visitor = visitor;
            return iterate();
        } finally { visit.visitor = null; exit(); }
    }

    /** Allocation-free iteration over raw addresses valid only during the callback. */
    public int forEachRaw(KvRawVisitor visitor) {
        Objects.requireNonNull(visitor);
        enter();
        try {
            visit.raw = visitor;
            return iterate();
        } finally { visit.raw = null; exit(); }
    }

    private int iterate() {
        visit.failure = null;
        int status = KvNative.for_each(handle, visitStub, MemorySegment.NULL);
        Throwable failure = visit.failure;
        if (failure != null) { visit.failure = null; throw new IllegalStateException("KV visitor failed", failure); }
        return status;
    }

    static void holder(long[] h) { if (h == null || h.length < 1) throw new IllegalArgumentException("out holder needs one element"); }

    /** Frees the native session. Idempotent. In SERIAL mode callable from any thread while the session is
     * not in use and not pinned by a view held on another thread (a view held by the calling thread is
     * released). Throws IllegalStateException otherwise. */
    public void close() {
        KvNative.guard();
        if (owner != null && Thread.currentThread() != owner) throw new WrongThreadException("KV session belongs to its creating thread");
        if (closed) return;
        if (!IN_USE.compareAndSet(this, 0, 1)) throw new IllegalStateException("KvSession in use; cannot close");
        try {
            if (closed) return;
            Thread pin = pinnedBy;
            if (pin != null && pin != Thread.currentThread()) throw new IllegalStateException("KvSession pinned by a view held on another thread; cannot close");
            releaseHeld();
            KvNative.session_free(handle);
            closed = true;
            scratch.close();
            reader.removed(this);
        } finally { exit(); }
    }

    /** Single reusable upcall target; the stub is created once per session. */
    private static final class Visit {
        KvVisitor visitor; KvRawVisitor raw; Throwable failure;
        int call(long ctx, long key, long keyLength, long value, long valueLength) {
            boolean previous = KvCallbackGuard.enter();
            try {
                KvRawVisitor r = raw;
                if (r != null) return r.visit(key, keyLength, value, valueLength) ? 1 : 0;
                return visitor.visit(MemorySegment.ofAddress(key).reinterpret(keyLength).asReadOnly(),
                        MemorySegment.ofAddress(value).reinterpret(valueLength).asReadOnly()) ? 1 : 0;
            } catch (Throwable t) { failure = t; return 0; }
            finally { KvCallbackGuard.exit(previous); }
        }
    }
}
