package io.ringloom.service;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class RingloomLocalIpcIT {
    @Test
    void twoJavaServicesCommunicateOverLocalIpc() throws Exception {
        Path workspace = TestSupport.createWorkspace("ringloom-java-local-ipc-");
        boolean success = false;

        try (TestBroker broker = TestBroker.start(TestSupport.repoRoot(), workspace);
             RingloomService echo = RingloomService.start(TestSupport.serviceConfig("echo", broker));
             MessageConsumer consumer = echo.messageConsumer();
             RingloomService ping = RingloomService.start(TestSupport.serviceConfig("ping", broker));
             RingloomClient client = ping.createClient("echo");
             BufferClaim claim = client.newClaim()) {

            AtomicBoolean running = new AtomicBoolean(true);
            CountDownLatch received = new CountDownLatch(1);
            AtomicReference<String> payload = new AtomicReference<>();
            AtomicReference<RingloomMessage> firstMessage = new AtomicReference<>();
            AtomicBoolean reusedMessage = new AtomicBoolean(true);

            Thread pollingThread = new Thread(() -> {
                while (running.get()) {
                    try {
                        int work = consumer.poll(message -> {
                            firstMessage.compareAndSet(null, message);
                            reusedMessage.compareAndSet(true, firstMessage.get() == message);
                            payload.set(new String(message.copyPayload(), StandardCharsets.UTF_8));
                            received.countDown();
                        }, 256);

                        if (work == 0) {
                            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
                        }
                    } catch (IllegalStateException ex) {
                        break;
                    }
                }
            }, "ringloom-java-echo-poll");
            pollingThread.start();

            try {
                byte[] hello = "hello".getBytes(StandardCharsets.UTF_8);
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);

                while (System.nanoTime() < deadline) {
                    int status = client.tryClaim(7, hello.length, claim);
                    if (status == RingloomStatus.OK) {
                        MemorySegment.copy(hello, 0, claim.payloadSegment(), ValueLayout.JAVA_BYTE, 0, hello.length);
                        assertEquals(RingloomStatus.OK, claim.commit());
                        break;
                    }

                    if (status != RingloomStatus.NO_AVAILABLE_INSTANCE && status != RingloomStatus.BUFFER_FULL) {
                        RingloomNative.throwForStatus("ringloom_client_try_claim", status);
                    }
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(50));
                }

                assertTrue(received.await(5, TimeUnit.SECONDS), "timed out waiting for echo payload");
                assertEquals("hello", payload.get());
                assertTrue(reusedMessage.get(), "expected RingloomMessage view to be reused across callbacks");
                assertNotNull(firstMessage.get());
                success = true;
            } finally {
                running.set(false);
                pollingThread.join(TimeUnit.SECONDS.toMillis(5));
            }
        } finally {
            TestSupport.cleanupWorkspace(workspace, success);
        }
    }
}
