// SPDX-License-Identifier: Apache-2.0
package io.ringloom.service;

import java.lang.foreign.*;
import java.lang.invoke.*;
import java.util.Objects;
import static java.lang.foreign.ValueLayout.*;
/** Thread-confined reusable read session. get copies into caller-owned native memory.
 * view pins a snapshot shared by subsequent reads until release. */
public final class KvSession implements AutoCloseable {
    private final KvReader reader;
    private final MemorySegment handle;
    private final Thread owner=Thread.currentThread();
    private final Arena scratch=Arena.ofConfined();
    private final MemorySegment length=scratch.allocate(JAVA_LONG), pointer=scratch.allocate(ADDRESS);
    private Arena views;
    private boolean closed;
    private static final MethodHandle VISIT;
    static { try { VISIT=MethodHandles.lookup().findVirtual(Visit.class,"call",MethodType.methodType(int.class,MemorySegment.class,MemorySegment.class,long.class,MemorySegment.class,long.class)); }
        catch(ReflectiveOperationException e){throw new ExceptionInInitializerError(e);} }
    KvSession(KvReader r,MemorySegment h){reader=r;handle=h;}
    private void check(){KvNative.guard(); if(Thread.currentThread()!=owner)throw new WrongThreadException("KV session belongs to its creating thread"); if(closed)throw new IllegalStateException("KvSession closed"); reader.ensureOpen();}
    /** Returns native status; outLength[0] is required length on OK or BUFFER_TOO_SMALL.
     * Missing returns KV_MISSING, distinct from OK with length zero. No allocation. */
    public int get(MemorySegment key,MemorySegment destination,long[] outLength){
        check(); holder(outLength); Objects.requireNonNull(destination); if(destination.isReadOnly())throw new IllegalArgumentException("destination is read-only");
        length.set(JAVA_LONG,0,0); int status=KvNative.get(handle,KvNative.pointer(key),key.byteSize(),KvNative.pointer(destination),destination.byteSize(),length);
        outLength[0]=length.get(JAVA_LONG,0); return status;
    }
    /** Returns a read-only scoped borrowed value, or null for missing. Allocates a segment wrapper.
     * All returned views become inaccessible on release or close, including slices. */
    public MemorySegment view(MemorySegment key){
        check(); int status=KvNative.view(handle,KvNative.pointer(key),key.byteSize(),pointer,length);
        if(status==RingloomStatus.KV_MISSING)return null; KvNative.check("view",status);
        if(views==null)views=Arena.ofConfined();
        return pointer.get(ADDRESS,0).reinterpret(length.get(JAVA_LONG,0),views,null).asReadOnly();
    }
    public void release(){check(); if(views!=null){views.close();views=null;} KvNative.release(handle);}
    public int forEach(KvVisitor visitor){
        check(); Objects.requireNonNull(visitor); try(Arena a=Arena.ofConfined()) {
            var v=new Visit(visitor); var stub=RingloomNative.LINKER.upcallStub(VISIT.bindTo(v),FunctionDescriptor.of(JAVA_INT,ADDRESS,ADDRESS,JAVA_LONG,ADDRESS,JAVA_LONG),a);
            int status=KvNative.for_each(handle,stub,MemorySegment.NULL); if(v.failure!=null)throw new IllegalStateException("KV visitor failed",v.failure); return status;
        }
    }
    static void holder(long[] h){if(h==null || h.length<1)throw new IllegalArgumentException("out holder needs one element");}
    public void close(){KvNative.guard();if(Thread.currentThread()!=owner)throw new WrongThreadException("KV session belongs to its creating thread");if(closed)return;
        release(); KvNative.session_free(handle); scratch.close(); closed=true;reader.removed(this);}
    private static final class Visit {
        final KvVisitor visitor; Throwable failure;
        Visit(KvVisitor v){visitor=v;}
        int call(MemorySegment ctx,MemorySegment key,long kl,MemorySegment value,long vl){
            boolean previous=KvCallbackGuard.enter();
            try(Arena a=Arena.ofConfined()) {return visitor.visit(key.reinterpret(kl,a,null).asReadOnly(),value.reinterpret(vl,a,null).asReadOnly())?1:0;}
            catch(Throwable t){failure=t;return 0;} finally{KvCallbackGuard.exit(previous);}
        }
    }
}
