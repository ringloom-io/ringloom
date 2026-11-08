// SPDX-License-Identifier: Apache-2.0
package io.ringloom.service;

import java.lang.foreign.*;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import static java.lang.foreign.ValueLayout.*;
import static org.junit.jupiter.api.Assertions.*;

final class KvBindingTest {
    @Test void defaultsAndLayoutsMatchNativeInitializers() {
        var c=KvStoreConfig.defaults();
        assertEquals(511,c.maxKeyLength()); assertTrue(c.maxValueLength()>0);
        try(Arena a=Arena.ofConfined()) {
            var defaults=a.allocate(80,8); KvNative.store_config_init(defaults);
            assertEquals(Integer.toUnsignedLong(defaults.get(JAVA_INT,12)),c.maxValueLength());
            var s=c.allocate(a); assertEquals(80,s.get(JAVA_INT,0)); assertEquals(511,Short.toUnsignedInt(s.get(JAVA_SHORT,8)));
            var r=KvReaderOptions.defaults().allocate(a);assertEquals(16,r.get(JAVA_INT,0));
            var w=KvWriterOptions.defaults().allocate(a,MemorySegment.NULL);assertEquals(32,w.get(JAVA_INT,0));assertEquals(65536,w.get(JAVA_INT,12));
        }
        assertThrows(IllegalArgumentException.class,()->new KvReaderOptions(0,false,2,0));
        assertThrows(IllegalArgumentException.class,()->new KvWriterOptions(2,0,0,null));
        assertThrows(IllegalArgumentException.class,()->new KvStoreConfig(1,0,10,0,0,"FAST_DAILY",0));
    }

    @Test void readerWriterBatchTtlAndScopedViews() throws Exception {
        Path workspace=TestSupport.createWorkspace("ringloom-java-kv-"); boolean success=false;
        try(TestBroker broker=TestBroker.startKvEnabled(TestSupport.repoRoot(),workspace);
            RingloomService service=RingloomService.start(TestSupport.serviceConfig("java-kv",broker));
            RingloomClient client=service.createClient("java-kv"); Arena a=Arena.ofConfined()) {
            var d=KvStoreConfig.defaults();
            var cfg=new KvStoreConfig(d.formatVersion(),d.maxKeyLength(),d.maxValueLength(),d.defaultTtlMillis(),8*1024*1024,d.topicRollScheme(),d.flags());
            try(KvWriter writer=openWriter(service,client,cfg);
                KvReader reader=client.openKvReader("java-kv",new KvReaderOptions(5000,false,0,128));
                KvSession session=reader.newSession()) {
                assertEquals(KvWriter.GRANTED,writer.state());
                assertThrows(RingloomException.class,()->client.openKvWriter("java-kv",cfg));
                var key=a.allocateFrom("key");var val=a.allocateFrom("value");var out=a.allocate(128);long[] n={0},seq={0};
                assertEquals(RingloomStatus.KV_MISSING,session.get(key,out,n));
                put(service,writer,key,val,-1,seq); long token=seq[0];
                await(service,()->session.get(key,out,n)==0);assertEquals(val.byteSize(),n[0]);
                assertEquals(RingloomStatus.KV_BUFFER_TOO_SMALL,session.get(key,a.allocate(1),n));assertEquals(val.byteSize(),n[0]);
                await(service,()->writer.isAcked(token));
                var view=session.view(key);assertTrue(view.isReadOnly());
                assertThrows(IllegalStateException.class,reader::close);
                var other=a.allocateFrom("new");put(service,writer,key,other,0,seq);
                for(int i=0;i<30;i++){service.pollControl(64);Thread.sleep(2);}
                assertArrayEquals(val.toArray(JAVA_BYTE),view.toArray(JAVA_BYTE));
                session.release();assertThrows(IllegalStateException.class,()->view.get(JAVA_BYTE,0));
                await(service,()->session.get(key,out,n)==0 && n[0]==other.byteSize());
                AtomicReference<Throwable> wrongThread=new AtomicReference<>();
                Thread t=new Thread(()->{try{session.release();}catch(Throwable e){wrongThread.set(e);}});t.start();t.join();assertInstanceOf(WrongThreadException.class,wrongThread.get());
                MemorySegment[] escaped={null};
                assertEquals(0,session.forEach((k,v)->{escaped[0]=v;assertThrows(IllegalStateException.class,session::release);return false;}));
                assertThrows(IllegalStateException.class,()->escaped[0].get(JAVA_BYTE,0));
                assertThrows(IllegalStateException.class,()->session.forEach((k,v)->{throw new RuntimeException("visitor");}));
                var empty=a.allocateFrom("empty");
                assertEquals(0,writer.beginBatch()); assertEquals(0,writer.put(empty,MemorySegment.NULL,0,TopicAckMode.REPLICATE_ONCE,seq));assertEquals(0,seq[0]);
                assertEquals(0,writer.commitBatch(TopicAckMode.REPLICATE_ONCE,seq));
                await(service,()->session.get(empty,out,n)==0);assertEquals(0,n[0]);
                var ttl=a.allocateFrom("ttl");put(service,writer,ttl,val,200,seq);await(service,()->session.get(ttl,out,n)==0);
                Thread.sleep(250);await(service,()->session.get(ttl,out,n)==RingloomStatus.KV_MISSING);
                assertEquals(0,writer.delete(key,TopicAckMode.REPLICATE_ONCE,seq));await(service,()->session.get(key,out,n)==RingloomStatus.KV_MISSING);
                assertEquals(0,writer.clear(TopicAckMode.REPLICATE_ONCE,seq));await(service,()->session.get(empty,out,n)==RingloomStatus.KV_MISSING);
                assertTrue(reader.status().generation()>0);
                session.close();session.close();assertThrows(IllegalStateException.class,()->session.view(key));
            }
            success=true;
        } finally {TestSupport.cleanupWorkspace(workspace,success);}
    }
    @Test void stateCallbackRejectsParentReentryWithoutClosingParents() throws Exception {
        Path workspace = TestSupport.createWorkspace("ringloom-java-kv-callback-");
        boolean success = false;
        try (TestBroker broker = TestBroker.startKvEnabled(TestSupport.repoRoot(), workspace);
             RingloomService service = RingloomService.start(TestSupport.serviceConfig("java-kv", broker));
             RingloomClient client = service.createClient("java-kv")) {
            var d = KvStoreConfig.defaults();
            var cfg = new KvStoreConfig(d.formatVersion(), d.maxKeyLength(), d.maxValueLength(),
                d.defaultTtlMillis(), 8 * 1024 * 1024, d.topicRollScheme(), d.flags());
            var defaults = KvWriterOptions.defaults();
            int[] callbacks = {0};
            var options = new KvWriterOptions(defaults.acquire(), defaults.openTimeoutMillis(),
                defaults.maxFrameLength(), (state, epoch) -> {
                    callbacks[0]++;
                    assertCallbackRejected(service::stop);
                    assertCallbackRejected(service::close);
                    assertCallbackRejected(service::pollControl);
                    assertCallbackRejected(() -> service.pollControl(1));
                    assertCallbackRejected(() -> service.createClient("callback-client"));
                    assertCallbackRejected(client::close);
                    assertCallbackRejected(client::clearLifecycleHandler);
                    assertCallbackRejected(() -> client.openKvWriter("nested", cfg));
                });
            KvWriter[] opened = {null};
            await(service, () -> {
                try {
                    opened[0] = client.openKvWriter("java-kv", cfg, options);
                    return true;
                } catch (RingloomException e) {
                    if (e.statusCode() != RingloomStatus.TOPIC_LEADER_UNAVAILABLE) throw e;
                    return false;
                }
            });
            try (KvWriter writer = opened[0]) {
                await(service, () -> callbacks[0] > 0);
                assertNull(writer.callbackFailure(), "callback assertions must not be swallowed");
                assertEquals(KvWriter.GRANTED, writer.state());
                assertTrue(service.serviceId() > 0, "rejected close must leave service open");
                assertNotNull(client.targetServices(), "rejected close must leave client open");
                service.pollControl(1);
            }
            // Normal lifecycle operations remain usable after callback context has cleared.
            client.close();
            client.close();
            service.stop();
            service.close();
            service.close();
            success = true;
        } finally {
            TestSupport.cleanupWorkspace(workspace, success);
        }
    }

    private static void assertCallbackRejected(org.junit.jupiter.api.function.Executable call) {
        var failure = assertThrows(IllegalStateException.class, call);
        assertTrue(failure.getMessage().contains("KV callback"), failure::getMessage);
    }

    private static KvWriter openWriter(RingloomService service,RingloomClient client,KvStoreConfig cfg)throws Exception {
        KvWriter[] result={null};
        await(service,()->{try{result[0]=client.openKvWriter("java-kv",cfg);return true;}catch(RingloomException e){if(e.statusCode()!=RingloomStatus.TOPIC_LEADER_UNAVAILABLE)throw e;return false;}});
        return result[0];
    }
    private static void put(RingloomService s,KvWriter w,MemorySegment k,MemorySegment v,long ttl,long[] seq)throws Exception {
        await(s,()->{int status=w.put(k,v,ttl,TopicAckMode.REPLICATE_ONCE,seq);if(status==RingloomStatus.BACKPRESSURE || status==RingloomStatus.TOPIC_LEADER_UNAVAILABLE)return false;assertEquals(0,status);return true;});
    }
    private static void await(RingloomService s,java.util.function.BooleanSupplier condition)throws Exception {
        long deadline=System.nanoTime()+10_000_000_000L;
        do {s.pollControl(128);if(condition.getAsBoolean())return;Thread.sleep(2);}while(System.nanoTime()<deadline);
        fail("KV condition did not converge");
    }
}
