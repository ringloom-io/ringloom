// SPDX-License-Identifier: Apache-2.0
package io.ringloom.service;

import java.lang.foreign.*;
import java.lang.invoke.*;
import java.util.Objects;
import static java.lang.foreign.ValueLayout.*;
/** Writer API; keep parent client/service alive until close. Batch writes return seq=0.
 *
 * <p>Threading: writes, batch calls, {@link #state}, {@link #config} and {@link #close} are serialized by
 * this object's monitor. Native code serializes writer mutations and control-plane updates with an
 * internal lock, so one thread may publish while another runs {@code RingloomService.pollControl}
 * (state callbacks run on the polling thread). {@link #ackedSequence}, {@link #leaderEpoch} and
 * {@link #writerEpoch} are lock-free single downcalls that never take the monitor, so they may be called
 * from the control thread while another thread publishes. Only {@link #close} must not race with
 * {@code pollControl} or with those lock-free accessors.
 *
 * <p>Write methods require native segments (not heap segments). */
public final class KvWriter implements AutoCloseable {
    public static final int GRANTED=0, QUEUED=1, REVOKED=2, REASSERTING=3;
    public static final long DEFAULT_TTL=-1, NO_EXPIRY=0;
    private final MemorySegment handle;
    private final Arena arena;
    private final MemorySegment sequence;
    private final StateCallback callback;
    private volatile boolean closed;
    private final long sequenceAddress;
    private static final MethodHandle STATE;
    static {try { STATE=MethodHandles.lookup().findVirtual(StateCallback.class,"call",MethodType.methodType(void.class,MemorySegment.class,byte.class,long.class)); }
        catch(ReflectiveOperationException e){throw new ExceptionInInitializerError(e);} }
    private KvWriter(MemorySegment h,Arena a,StateCallback cb){handle=h;arena=a;callback=cb;sequence=a.allocate(JAVA_LONG);sequenceAddress=sequence.address();}
    static KvWriter open(MemorySegment client,String name,KvStoreConfig config,KvWriterOptions opts){
        KvNative.guard(); Objects.requireNonNull(config); Objects.requireNonNull(opts); Arena a=Arena.ofShared();
        try { var cb=new StateCallback(opts.onState()); var stub=opts.onState()==null?MemorySegment.NULL:RingloomNative.LINKER.upcallStub(STATE.bindTo(cb),FunctionDescriptor.ofVoid(ADDRESS,JAVA_BYTE,JAVA_LONG),a);
            var out=a.allocate(ADDRESS); KvNative.check("open_writer",KvNative.open_writer(client,KvNative.name(a,name),config.allocate(a),opts.allocate(a,stub),out));
            return new KvWriter(out.get(ADDRESS,0),a,cb);
        } catch(Throwable t){a.close();throw t;}
    }
    private void check(){KvNative.guard();if(closed)throw new IllegalStateException("KvWriter closed");}
    public int put(MemorySegment key,MemorySegment value,long ttlMillis,TopicAckMode ack,long[] outSequence){
        Objects.requireNonNull(key,"key");Objects.requireNonNull(value,"value");
        return put(key,0,key.byteSize(),value,0,value.byteSize(),ttlMillis,ack,outSequence);
    }
    /** Offset form: key is {@code key[keyOffset, +keyLength)}, value is {@code value[valueOffset, +valueLength)}.
     * Passes raw addresses; allocation-free. ttlMillis: {@link #DEFAULT_TTL}, {@link #NO_EXPIRY} or positive. */
    public synchronized int put(MemorySegment key,long keyOffset,long keyLength,MemorySegment value,long valueOffset,long valueLength,
                                long ttlMillis,TopicAckMode ack,long[] outSequence){
        check();KvSession.holder(outSequence);Objects.requireNonNull(ack);if(ttlMillis < -1)throw new IllegalArgumentException("TTL must be -1, zero, or positive milliseconds");
        KvNative.pointer(key);KvNative.pointer(value);
        Objects.checkFromIndexSize(keyOffset,keyLength,key.byteSize());Objects.checkFromIndexSize(valueOffset,valueLength,value.byteSize());
        sequence.set(JAVA_LONG,0,0);
        int s=KvNative.put_raw(handle,key.address()+keyOffset,keyLength,value.address()+valueOffset,valueLength,ttlMillis,(byte)ack.nativeValue(),sequenceAddress);
        outSequence[0]=sequence.get(JAVA_LONG,0);return s;
    }
    public int delete(MemorySegment key,TopicAckMode ack,long[] outSequence){
        Objects.requireNonNull(key,"key");return delete(key,0,key.byteSize(),ack,outSequence);
    }
    /** Offset form of {@link #delete(MemorySegment, TopicAckMode, long[])}; allocation-free. */
    public synchronized int delete(MemorySegment key,long keyOffset,long keyLength,TopicAckMode ack,long[] outSequence){
        check();KvSession.holder(outSequence);Objects.requireNonNull(ack);KvNative.pointer(key);Objects.checkFromIndexSize(keyOffset,keyLength,key.byteSize());
        sequence.set(JAVA_LONG,0,0);
        int s=KvNative.delete_raw(handle,key.address()+keyOffset,keyLength,(byte)ack.nativeValue(),sequenceAddress);outSequence[0]=sequence.get(JAVA_LONG,0);return s;
    }
    public synchronized int clear(TopicAckMode ack,long[] outSequence){check();KvSession.holder(outSequence);Objects.requireNonNull(ack);sequence.set(JAVA_LONG,0,0);
        int s=KvNative.clear(handle,(byte)ack.nativeValue(),sequence);outSequence[0]=sequence.get(JAVA_LONG,0);return s;}
    public synchronized int beginBatch(){check();return KvNative.begin_batch(handle);}
    public synchronized int commitBatch(TopicAckMode ack,long[] outSequence){check();KvSession.holder(outSequence);Objects.requireNonNull(ack);sequence.set(JAVA_LONG,0,0);
        int s=KvNative.commit_batch(handle,(byte)ack.nativeValue(),sequence);outSequence[0]=sequence.get(JAVA_LONG,0);return s;}
    /** Discards records buffered since {@link #beginBatch} and ends the batch. Frames auto-flushed while the
     * batch was open (buffer full) are already published and are not recalled. OK if no batch is open. */
    public synchronized int abandonBatch(){check();return KvNative.abandon_batch(handle);}
    public synchronized boolean isAcked(long seq){check();return KvNative.is_acked(handle,seq)!=0;}
    /** Highest acknowledged replicate_once sequence (monotonic; same namespace as returned sequences).
     * Lock-free single downcall; does not take this object's monitor. */
    public long ackedSequence(){check();return KvNative.writer_acked_seq(handle);}
    /** Topic leader epoch the writer publishes under (0 = none). Does not take this object's monitor. */
    public long leaderEpoch(){check();return KvNative.writer_leader_epoch(handle);}
    /** Current lease epoch; 0 while queued or revoked (kept while reasserting). Does not take this object's monitor. */
    public long writerEpoch(){check();return KvNative.writer_epoch(handle);}
    /** Effective config the broker accepted (may differ from the request in mapSizeHint only). */
    public synchronized KvStoreConfig config(){check();
        try(Arena a=Arena.ofConfined()){var s=KvStoreConfig.allocateOut(a);KvNative.check("writer_config",KvNative.writer_config(handle,s));return KvStoreConfig.read(s);}}
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
