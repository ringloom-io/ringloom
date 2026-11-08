// SPDX-License-Identifier: Apache-2.0
package io.ringloom.service;

import java.lang.foreign.*;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import static java.lang.foreign.ValueLayout.*;
import static org.junit.jupiter.api.Assertions.*;

/** KV ABI 7 binding features: serial sessions, offset overloads, reusable iteration, accessors,
 * abandonBatch and allocation-free hot paths (spec 16A §5.9). */
final class KvHotPathTest {
    private static final String STORE = "java-kv-hot";

    @FunctionalInterface interface Body { void run(RingloomService service, KvWriter writer, KvReader reader, KvStoreConfig requested) throws Exception; }

    private static void withStore(String prefix, Body body) throws Exception {
        Path workspace = TestSupport.createWorkspace(prefix);
        boolean success = false;
        try (TestBroker broker = TestBroker.startKvEnabled(TestSupport.repoRoot(), workspace);
             RingloomService service = RingloomService.start(TestSupport.serviceConfig("java-kv-hot", broker));
             RingloomClient client = service.createClient("java-kv-hot")) {
            var d = KvStoreConfig.defaults();
            var cfg = new KvStoreConfig(d.formatVersion(), 200, 4096, 0, 8 * 1024 * 1024, d.topicRollScheme(), d.flags());
            KvWriter[] opened = {null};
            await(service, () -> {
                try { opened[0] = client.openKvWriter(STORE, cfg); return true; }
                catch (RingloomException e) { if (e.statusCode() != RingloomStatus.TOPIC_LEADER_UNAVAILABLE) throw e; return false; }
            });
            try (KvWriter writer = opened[0];
                 KvReader reader = client.openKvReader(STORE, new KvReaderOptions(5000, false, 0, 128))) {
                body.run(service, writer, reader, cfg);
                assertEquals(0, reader.openSessionCount(), "test must close its sessions");
            }
            success = true;
        } finally { TestSupport.cleanupWorkspace(workspace, success); }
    }

    @Test void serialSessionsMoveBetweenThreadsAndRejectOverlap() throws Exception {
        withStore("ringloom-java-kv-serial-", (service, writer, reader, cfg) -> {
            try (Arena a = Arena.ofShared()) {
                var key = a.allocateFrom("k"); var val = a.allocateFrom("v1"); long[] seq = {0};
                put(service, writer, key, val, seq);
                ExecutorService a1 = Executors.newSingleThreadExecutor(), b1 = Executors.newSingleThreadExecutor(), c1 = Executors.newSingleThreadExecutor();
                try {
                    // 1. Created on A, used on B, closed on C.
                    KvSession session = a1.submit(() -> reader.newSession()).get();
                    assertEquals(KvSessionMode.SERIAL, session.mode());
                    assertEquals(1, reader.openSessionCount());
                    var out = a.allocate(64);
                    b1.submit(() -> { long[] n = {0}; long deadline = System.nanoTime() + 10_000_000_000L;
                        while (session.get(key, out, n) != 0) { if (System.nanoTime() > deadline) fail("no value"); Thread.sleep(2); }
                        assertEquals(val.byteSize(), n[0]); return null; }).get();
                    c1.submit(() -> { session.close(); return null; }).get();
                    assertEquals(0, reader.openSessionCount());
                    assertThrows(IllegalStateException.class, () -> session.get(key, out, new long[1]));

                    // 2. Overlapping use from two threads throws; so does close while in use.
                    try (KvSession shared = reader.newSession()) {
                        CountDownLatch inside = new CountDownLatch(1), proceed = new CountDownLatch(1);
                        Future<Integer> iterating = a1.submit(() -> shared.forEach((k, v) -> {
                            inside.countDown();
                            try { proceed.await(); } catch (InterruptedException e) { throw new RuntimeException(e); }
                            return false;
                        }));
                        assertTrue(inside.await(10, TimeUnit.SECONDS));
                        var overlap = assertThrows(ExecutionException.class, () -> b1.submit(() -> shared.get(key, out, new long[1])).get());
                        assertInstanceOf(IllegalStateException.class, overlap.getCause());
                        assertTrue(overlap.getCause().getMessage().contains("concurrently"));
                        var closeBusy = assertThrows(ExecutionException.class, () -> c1.submit(() -> { shared.close(); return null; }).get());
                        assertInstanceOf(IllegalStateException.class, closeBusy.getCause());
                        proceed.countDown();
                        assertEquals(0, iterating.get());

                        // Pinned by a view on A: B is rejected (including close) until A releases.
                        assertNotNull(a1.submit(() -> shared.view(key)).get());
                        var pinned = assertThrows(ExecutionException.class, () -> b1.submit(() -> shared.get(key, out, new long[1])).get());
                        assertInstanceOf(IllegalStateException.class, pinned.getCause());
                        assertTrue(pinned.getCause().getMessage().contains("pinned"));
                        assertInstanceOf(IllegalStateException.class, assertThrows(ExecutionException.class,
                                () -> c1.submit(() -> { shared.close(); return null; }).get()).getCause());
                        a1.submit(() -> { shared.release(); return null; }).get();
                        assertEquals(0, (int) b1.submit(() -> shared.get(key, out, new long[1])).get());
                    }

                    // CONFINED keeps the strict owner check, including close.
                    KvSession confined = a1.submit(() -> reader.newSession(KvSessionMode.CONFINED)).get();
                    assertInstanceOf(WrongThreadException.class, assertThrows(ExecutionException.class,
                            () -> b1.submit(() -> confined.get(key, out, new long[1])).get()).getCause());
                    assertInstanceOf(WrongThreadException.class, assertThrows(ExecutionException.class,
                            () -> b1.submit(() -> { confined.close(); return null; }).get()).getCause());
                    a1.submit(() -> { confined.close(); return null; }).get();
                } finally { a1.shutdownNow(); b1.shutdownNow(); c1.shutdownNow(); }
            }
        });
    }

    @Test void offsetOverloadsConfigAccessorsAndAbandonBatch() throws Exception {
        withStore("ringloom-java-kv-offsets-", (service, writer, reader, cfg) -> {
            try (Arena a = Arena.ofConfined(); KvSession session = reader.newSession()) {
                // Keys and values embedded in larger buffers.
                var keyBuf = a.allocate(32); var valBuf = a.allocate(32);
                MemorySegment.copy("xxkey1yy".getBytes(StandardCharsets.UTF_8), 0, keyBuf, JAVA_BYTE, 0, 8);
                MemorySegment.copy("--value-1--".getBytes(StandardCharsets.UTF_8), 0, valBuf, JAVA_BYTE, 0, 11);
                long[] seq = {0}, n = {0};
                await(service, () -> retryable(writer.put(keyBuf, 2, 4, valBuf, 2, 7, KvWriter.NO_EXPIRY, TopicAckMode.REPLICATE_ONCE, seq)));
                long token = seq[0];
                assertTrue(token > 0);
                var sliceKey = keyBuf.asSlice(2, 4);
                var dest = a.allocate(64); var sliceDest = a.allocate(16);
                await(service, () -> session.get(sliceKey, sliceDest, n) == 0);
                assertEquals(7, n[0]);
                assertEquals(0, session.get(keyBuf, 2, 4, dest, 5, 20, n));
                assertEquals(7, n[0]);
                assertArrayEquals(sliceDest.asSlice(0, 7).toArray(JAVA_BYTE), dest.asSlice(5, 7).toArray(JAVA_BYTE));
                assertEquals(RingloomStatus.KV_BUFFER_TOO_SMALL, session.get(keyBuf, 2, 4, dest, 5, 3, n));
                assertEquals(7, n[0]);
                assertEquals(RingloomStatus.KV_MISSING, session.get(keyBuf, 2, 3, dest, 0, 64, n));
                // Bounds.
                assertThrows(IndexOutOfBoundsException.class, () -> session.get(keyBuf, 30, 4, dest, 0, 8, n));
                assertThrows(IndexOutOfBoundsException.class, () -> session.get(keyBuf, -1, 4, dest, 0, 8, n));
                assertThrows(IndexOutOfBoundsException.class, () -> session.get(keyBuf, 2, 4, dest, 60, 8, n));
                assertThrows(IndexOutOfBoundsException.class, () -> writer.put(keyBuf, 2, 40, valBuf, 0, 1, 0, TopicAckMode.FIRE_AND_FORGET, seq));
                assertThrows(IndexOutOfBoundsException.class, () -> writer.put(keyBuf, 2, 4, valBuf, 30, 4, 0, TopicAckMode.FIRE_AND_FORGET, seq));
                assertThrows(IndexOutOfBoundsException.class, () -> writer.delete(keyBuf, 31, 2, TopicAckMode.FIRE_AND_FORGET, seq));
                // Heap segments: accepted by get (critical downcall), rejected by writes.
                byte[] heapKey = "key1".getBytes(StandardCharsets.UTF_8); byte[] heapOut = new byte[16];
                assertEquals(0, session.get(MemorySegment.ofArray(heapKey), MemorySegment.ofArray(heapOut), n));
                assertEquals("value-1", new String(heapOut, 0, (int) n[0], StandardCharsets.UTF_8));
                assertEquals(0, session.get(MemorySegment.ofArray(new byte[]{9, 'k', 'e', 'y', '1'}), 1, 4, MemorySegment.ofArray(heapOut), 8, 8, n));
                assertEquals("value-1", new String(heapOut, 8, 7, StandardCharsets.UTF_8));
                assertThrows(IllegalArgumentException.class, () -> writer.put(MemorySegment.ofArray(heapKey), valBuf, 0, TopicAckMode.FIRE_AND_FORGET, seq));

                // config() on reader and writer matches the request.
                for (KvStoreConfig c : new KvStoreConfig[]{reader.config(), writer.config()}) {
                    assertEquals(cfg.maxKeyLength(), c.maxKeyLength());
                    assertEquals(cfg.maxValueLength(), c.maxValueLength());
                    assertEquals(cfg.defaultTtlMillis(), c.defaultTtlMillis());
                    assertEquals(cfg.topicRollScheme(), c.topicRollScheme());
                    assertEquals(cfg.formatVersion(), c.formatVersion());
                }
                assertSame(reader.config(), reader.config());

                // Accessors: acked sequence catches up with a replicate_once token; epochs are live.
                await(service, () -> writer.ackedSequence() >= token);
                assertTrue(writer.isAcked(token));
                assertTrue(writer.leaderEpoch() > 0);
                assertTrue(writer.writerEpoch() > 0);

                // Status holder.
                var holder = new KvStatusHolder();
                assertEquals(0, reader.status(holder));
                assertTrue(holder.generation() > 0);
                assertEquals(holder.generation(), reader.status().generation());

                // Offset delete.
                assertEquals(0, writer.delete(keyBuf, 2, 4, TopicAckMode.REPLICATE_ONCE, seq));
                await(service, () -> session.get(sliceKey, sliceDest, n) == RingloomStatus.KV_MISSING);

                // abandonBatch drops buffered records and ends the batch.
                var dropped = a.allocateFrom("dropped"); var marker = a.allocateFrom("marker");
                assertEquals(0, writer.beginBatch());
                assertEquals(0, writer.put(dropped, valBuf, 0, TopicAckMode.REPLICATE_ONCE, seq));
                assertEquals(0, seq[0]);
                assertEquals(0, writer.abandonBatch());
                assertEquals(0, writer.abandonBatch());
                assertEquals(0, writer.commitBatch(TopicAckMode.REPLICATE_ONCE, seq));
                assertEquals(0, seq[0], "no batch open after abandon");
                put(service, writer, marker, valBuf, seq);
                await(service, () -> session.get(marker, dest, n) == 0);
                assertEquals(RingloomStatus.KV_MISSING, session.get(dropped, dest, n));
            }
        });
    }

    @Test void iterationReusesOneUpcallStubAndHotPathsAllocateNothing() throws Exception {
        withStore("ringloom-java-kv-alloc-", (service, writer, reader, cfg) -> {
            try (Arena a = Arena.ofConfined(); KvSession session = reader.newSession()) {
                var bean = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
                assertTrue(bean.isThreadAllocatedMemorySupported());
                bean.setThreadAllocatedMemoryEnabled(true);
                long[] seq = {0}, n = {0};
                var keyBuf = a.allocate(16); var valBuf = a.allocate(64);
                for (int i = 0; i < 3; i++) {
                    keyBuf.set(JAVA_BYTE, i, (byte) ('a' + i));
                    put(service, writer, keyBuf.asSlice(i, 1), valBuf.asSlice(0, 8), seq);
                }
                await(service, () -> { int[] c = {0}; session.forEach((k, v) -> { c[0]++; return true; }); return c[0] == 3; });

                // forEach: 1000 calls reuse the session's upcall stub (an upcall stub per call costs
                // kilobytes; two read-only wrappers per entry are expected).
                int[] count = {0};
                KvVisitor visitor = (k, v) -> { count[0]++; return true; };
                for (int i = 0; i < 1000; i++) session.forEach(visitor);
                long stubs = KvSession.UPCALL_STUBS_CREATED.get();
                long before = bean.getCurrentThreadAllocatedBytes();
                for (int i = 0; i < 1000; i++) assertEquals(0, session.forEach(visitor));
                long perCall = (bean.getCurrentThreadAllocatedBytes() - before) / 1000;
                assertEquals(stubs, KvSession.UPCALL_STUBS_CREATED.get(), "forEach must not create upcall stubs");
                assertEquals(6000, count[0]);
                // Three entries x two read-only wrappers; no arena or stub per call.
                assertTrue(perCall < 2048, "forEach allocated " + perCall + " bytes/call");

                // forEachRaw allocates nothing.
                long[] total = {0};
                KvRawVisitor raw = (k, kl, v, vl) -> { total[0] += kl + vl; return true; };
                assertZeroAllocation(bean, "forEachRaw", 20_000, () -> session.forEachRaw(raw));
                assertTrue(total[0] > 0);

                // Offset get and put allocate nothing in steady state.
                var dest = a.allocate(64);
                assertZeroAllocation(bean, "get(offset)", 100_000, () -> session.get(keyBuf, 0, 1, dest, 0, 64, n));
                assertZeroAllocation(bean, "put(offset)", 100_000, () -> writer.put(keyBuf, 0, 1, valBuf, 0, 8, KvWriter.NO_EXPIRY, TopicAckMode.FIRE_AND_FORGET, seq));
                assertZeroAllocation(bean, "delete(offset)", 20_000, () -> writer.delete(keyBuf, 5, 1, TopicAckMode.FIRE_AND_FORGET, seq));
                assertZeroAllocation(bean, "writer accessors", 100_000, () -> writer.ackedSequence() + writer.leaderEpoch() + writer.writerEpoch());
                var holder = new KvStatusHolder();
                assertZeroAllocation(bean, "status(holder)", 100_000, () -> reader.status(holder));
                for (int i = 0; i < 50; i++) { service.pollControl(64); Thread.sleep(1); }
            }
        });
    }

    @FunctionalInterface interface LongOp { long run() throws Exception; }

    private static void assertZeroAllocation(com.sun.management.ThreadMXBean bean, String name, int iterations, LongOp op) throws Exception {
        long sink = 0;
        for (int i = 0; i < iterations; i++) sink += op.run(); // warm-up / JIT
        long before = bean.getCurrentThreadAllocatedBytes();
        for (int i = 0; i < iterations; i++) sink += op.run();
        long allocated = bean.getCurrentThreadAllocatedBytes() - before;
        // A fixed few hundred bytes may come from the measurement itself; anything per-call shows up as >= iterations.
        assertTrue(allocated < 1024, name + " allocated " + allocated + " bytes over " + iterations + " calls (sink " + sink + ")");
    }

    private static boolean retryable(int status) {
        if (status == RingloomStatus.BACKPRESSURE || status == RingloomStatus.TOPIC_LEADER_UNAVAILABLE) return false;
        assertEquals(0, status);
        return true;
    }
    private static void put(RingloomService s, KvWriter w, MemorySegment k, MemorySegment v, long[] seq) throws Exception {
        await(s, () -> retryable(w.put(k, v, KvWriter.NO_EXPIRY, TopicAckMode.REPLICATE_ONCE, seq)));
    }
    private static void await(RingloomService s, java.util.concurrent.Callable<Boolean> condition) throws Exception {
        long deadline = System.nanoTime() + 10_000_000_000L;
        do { s.pollControl(128); if (condition.call()) return; Thread.sleep(2); } while (System.nanoTime() < deadline);
        fail("KV condition did not converge");
    }
}
