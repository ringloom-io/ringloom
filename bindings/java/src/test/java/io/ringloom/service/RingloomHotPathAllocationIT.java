package io.ringloom.service;

import com.sun.management.ThreadMXBean;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class RingloomHotPathAllocationIT {
    @Test
    void reusesHotPathObjectsWithoutSteadySenderAllocations() throws Exception {
        Path workspace = TestSupport.createWorkspace("ringloom-java-hot-path-");
        boolean success = false;

        try (TestBroker broker = TestBroker.start(TestSupport.repoRoot(), workspace);
             RingloomService echo = RingloomService.start(TestSupport.serviceConfig("echo-hot", broker));
             MessageConsumer consumer = echo.messageConsumer();
             RingloomService ping = RingloomService.start(TestSupport.serviceConfig("ping-hot", broker));
             RingloomClient client = ping.createClient("echo-hot");
             BufferClaim claim = client.newClaim()) {

            int messageCount = 256;
            int warmupCount = 256;
            int totalExpected = warmupCount + messageCount + 1;
            AtomicBoolean running = new AtomicBoolean(true);
            AtomicInteger received = new AtomicInteger();
            AtomicReference<RingloomMessage> firstMessage = new AtomicReference<>();
            AtomicBoolean reusedMessage = new AtomicBoolean(true);

            Thread pollingThread = new Thread(() -> {
                while (running.get()) {
                    try {
                        int work = consumer.poll(message -> {
                            firstMessage.compareAndSet(null, message);
                            reusedMessage.compareAndSet(true, firstMessage.get() == message);
                            received.incrementAndGet();
                        }, 512);

                        if (work == 0) {
                            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(5));
                        }
                    } catch (IllegalStateException ex) {
                        break;
                    }
                }
            }, "ringloom-java-hot-poll");
            pollingThread.start();

            try {
                byte[] hello = "hello".getBytes(StandardCharsets.UTF_8);
                warmUntilDiscovered(client, claim, hello);
                for (int i = 0; i < warmupCount; i++) {
                    sendClaimedMessage(client, claim, hello);
                }
                waitUntilReceived(received, warmupCount + 1, 10, TimeUnit.SECONDS);

                ThreadMXBean mxBean = (ThreadMXBean) ManagementFactory.getThreadMXBean();
                if (mxBean.isThreadAllocatedMemorySupported() && !mxBean.isThreadAllocatedMemoryEnabled()) {
                    mxBean.setThreadAllocatedMemoryEnabled(true);
                }

                int beforeReceived = received.get();
                long beforeAllocated = mxBean.isThreadAllocatedMemorySupported()
                    ? mxBean.getThreadAllocatedBytes(Thread.currentThread().getId())
                    : -1L;

                for (int i = 0; i < messageCount; i++) {
                    sendClaimedMessage(client, claim, hello);
                }

                long afterAllocated = mxBean.isThreadAllocatedMemorySupported()
                    ? mxBean.getThreadAllocatedBytes(Thread.currentThread().getId())
                    : -1L;

                waitUntilReceived(received, beforeReceived + messageCount, 10, TimeUnit.SECONDS);
                assertEquals(totalExpected, received.get());
                assertTrue(reusedMessage.get(), "expected RingloomMessage view to be reused");
                if (beforeAllocated >= 0 && afterAllocated >= 0) {
                    long allocatedDelta = afterAllocated - beforeAllocated;
                    assertTrue(
                        allocatedDelta < 65_536L,
                        "sender hot path allocated too much: " + allocatedDelta + " bytes"
                    );
                }

                success = true;
            } finally {
                running.set(false);
                pollingThread.join(TimeUnit.SECONDS.toMillis(5));
            }
        } finally {
            TestSupport.cleanupWorkspace(workspace, success);
        }
    }

    private static void warmUntilDiscovered(RingloomClient client, BufferClaim claim, byte[] payload) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            int status = client.tryClaim(7, payload.length, claim);
            if (status == RingloomStatus.OK) {
                MemorySegment.copy(payload, 0, claim.payloadSegment(), ValueLayout.JAVA_BYTE, 0, payload.length);
                assertEquals(RingloomStatus.OK, claim.commit());
                return;
            }

            if (status != RingloomStatus.NO_AVAILABLE_INSTANCE && status != RingloomStatus.BUFFER_FULL) {
                RingloomNative.throwForStatus("ringloom_client_try_claim", status);
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(20));
        }
        throw new AssertionError("timed out waiting for service discovery");
    }

    private static void sendClaimedMessage(RingloomClient client, BufferClaim claim, byte[] payload) {
        while (true) {
            int status = client.tryClaim(7, payload.length, claim);
            if (status == RingloomStatus.OK) {
                MemorySegment.copy(payload, 0, claim.payloadSegment(), ValueLayout.JAVA_BYTE, 0, payload.length);
                assertEquals(RingloomStatus.OK, claim.commit());
                return;
            }

            if (status == RingloomStatus.BUFFER_FULL || status == RingloomStatus.NO_AVAILABLE_INSTANCE) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                continue;
            }
            RingloomNative.throwForStatus("ringloom_client_try_claim", status);
        }
    }

    private static void waitUntilReceived(AtomicInteger received, int expected, long timeout, TimeUnit unit) {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        while (System.nanoTime() < deadline) {
            if (received.get() >= expected) {
                return;
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
        }
        throw new AssertionError("timed out waiting for " + expected + " messages, received " + received.get());
    }
}
