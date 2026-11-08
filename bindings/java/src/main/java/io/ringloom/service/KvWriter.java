// SPDX-License-Identifier: Apache-2.0
package io.ringloom.service;

import java.lang.foreign.*;
import java.lang.invoke.*;
import java.util.Objects;
import static java.lang.foreign.ValueLayout.*;
/** Serialized writer API; keep parent client/service alive until close. Batch writes return seq=0.
 * Control polling must be serialized with writer operations and close. */
public final class KvWriter implements AutoCloseable {
    public static final int GRANTED=0, QUEUED=1, REVOKED=2, REASSERTING=3;
    public static final long DEFAULT_TTL=-1, NO_EXPIRY=0;
    private final MemorySegment handle;
    private final Arena arena;
    private final MemorySegment sequence;
    private final StateCallback callback;
    private boolean closed;
    private static final MethodHandle STATE;
    static {try { STATE=MethodHandles.lookup().findVirtual(StateCallback.class,"call",MethodType.methodType(void.class,MemorySegment.class,byte.class,long.class)); }
        catch(ReflectiveOperationException e){throw new ExceptionInInitializerError(e);} }
    private KvWriter(MemorySegment h,Arena a,StateCallback cb){handle=h;arena=a;callback=cb;sequence=a.allocate(JAVA_LONG);}
    static KvWriter open(MemorySegment client,String name,KvStoreConfig config,KvWriterOptions opts){
        KvNative.guard(); Objects.requireNonNull(config); Objects.requireNonNull(opts); Arena a=Arena.ofShared();
        try { var cb=new StateCallback(opts.onState()); var stub=opts.onState()==null?MemorySegment.NULL:RingloomNative.LINKER.upcallStub(STATE.bindTo(cb),FunctionDescriptor.ofVoid(ADDRESS,JAVA_BYTE,JAVA_LONG),a);
            var out=a.allocate(ADDRESS); KvNative.check("open_writer",KvNative.open_writer(client,KvNative.name(a,name),config.allocate(a),opts.allocate(a,stub),out));
            return new KvWriter(out.get(ADDRESS,0),a,cb);
        } catch(Throwable t){a.close();throw t;}
    }
    private void check(){KvNative.guard();if(closed)throw new IllegalStateException("KvWriter closed");}
    public synchronized int put(MemorySegment key,MemorySegment value,long ttlMillis,TopicAckMode ack,long[] outSequence){
        check();KvSession.holder(outSequence);Objects.requireNonNull(ack);if(ttlMillis < -1)throw new IllegalArgumentException("TTL must be -1, zero, or positive milliseconds");
        sequence.set(JAVA_LONG,0,0); int s=KvNative.put(handle,KvNative.pointer(key),key.byteSize(),KvNative.pointer(value),value.byteSize(),ttlMillis,(byte)ack.nativeValue(),sequence);outSequence[0]=sequence.get(JAVA_LONG,0);return s;
    }
    public synchronized int delete(MemorySegment key,TopicAckMode ack,long[] outSequence){check();KvSession.holder(outSequence);Objects.requireNonNull(ack);sequence.set(JAVA_LONG,0,0);
        int s=KvNative.delete(handle,KvNative.pointer(key),key.byteSize(),(byte)ack.nativeValue(),sequence);outSequence[0]=sequence.get(JAVA_LONG,0);return s;}
    public synchronized int clear(TopicAckMode ack,long[] outSequence){check();KvSession.holder(outSequence);Objects.requireNonNull(ack);sequence.set(JAVA_LONG,0,0);
        int s=KvNative.clear(handle,(byte)ack.nativeValue(),sequence);outSequence[0]=sequence.get(JAVA_LONG,0);return s;}
    public synchronized int beginBatch(){check();return KvNative.begin_batch(handle);}
    public synchronized int commitBatch(TopicAckMode ack,long[] outSequence){check();KvSession.holder(outSequence);Objects.requireNonNull(ack);sequence.set(JAVA_LONG,0,0);
        int s=KvNative.commit_batch(handle,(byte)ack.nativeValue(),sequence);outSequence[0]=sequence.get(JAVA_LONG,0);return s;}
    public synchronized boolean isAcked(long seq){check();return KvNative.is_acked(handle,seq)!=0;}
    public synchronized int state(){check();return KvNative.writer_state(handle);}
    /** Last callback failure, captured rather than thrown across native code. */
    public Throwable callbackFailure(){return callback.failure;}
    public synchronized void close(){KvNative.guard();if(closed)return;KvNative.close_writer(handle);closed=true;arena.close();}
    private static final class StateCallback {
        final KvStateHandler handler; volatile Throwable failure;
        StateCallback(KvStateHandler h){handler=h;}
        void call(MemorySegment ctx,byte state,long epoch){boolean previous=KvCallbackGuard.enter();try{handler.onState(Byte.toUnsignedInt(state),epoch);}catch(Throwable t){failure=t;}finally{KvCallbackGuard.exit(previous);}}
    }
}
