package io.ringloom.service;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.foreign.Arena;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
            LinkedBlockingQueue<CapturedMessage> messages = new LinkedBlockingQueue<>();
            RingloomMessage[] firstMessage = new RingloomMessage[1];
            AtomicBoolean reusedMessage = new AtomicBoolean(true);

            Thread pollingThread = new Thread(() -> {
                while (running.get()) {
                    try {
                        int work = consumer.poll(message -> {
                            if (firstMessage[0] == null) {
                                firstMessage[0] = message;
                            }
                            reusedMessage.compareAndSet(true, firstMessage[0] == message);
                            messages.add(CapturedMessage.copyOf(message));
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
                CapturedMessage claimed = messages.poll(5, TimeUnit.SECONDS);
                assertNotNull(claimed);
                assertEquals("hello", claimed.payload());
                assertEquals(7, claimed.templateId());
                assertTrue(reusedMessage.get(), "expected RingloomMessage view to be reused across callbacks");
                assertNotNull(firstMessage[0]);

                byte[] templated = "templated".getBytes(StandardCharsets.UTF_8);
                try (Arena arena = Arena.ofConfined()) {
                    MemorySegment segment = arena.allocateFrom(ValueLayout.JAVA_BYTE, templated);
                    RingloomNative.throwForStatus("ringloom_client_send_message", client.sendMessage(42, segment));
                }
                CapturedMessage templateMessage = messages.poll(5, TimeUnit.SECONDS);
                assertNotNull(templateMessage);
                assertEquals("templated", templateMessage.payload());
                assertEquals(42, templateMessage.templateId());

                assertFalse(client.targetServices().isEmpty(), "expected at least one discovered target");
                TargetService target = client.targetServices().get(0);
                byte[] direct = "direct".getBytes(StandardCharsets.UTF_8);
                try (Arena arena = Arena.ofConfined()) {
                    MemorySegment segment = arena.allocateFrom(ValueLayout.JAVA_BYTE, direct);
                    RingloomNative.throwForStatus(
                        "ringloom_client_send_to_message",
                        client.sendToMessage(target.targetNodeId(), target.targetServiceId(), 44, segment)
                    );
                }
                CapturedMessage directMessage = messages.poll(5, TimeUnit.SECONDS);
                assertNotNull(directMessage);
                assertEquals("direct", directMessage.payload());
                assertEquals(44, directMessage.templateId());

                byte[] request = "request".getBytes(StandardCharsets.UTF_8);
                try (Arena arena = Arena.ofConfined()) {
                    MemorySegment segment = arena.allocateFrom(ValueLayout.JAVA_BYTE, request);
                    RingloomNative.throwForStatus(
                        "ringloom_client_send_message_request",
                        client.sendMessageRequest(43, 123_456_789L, segment)
                    );
                }
                CapturedMessage requestMessage = messages.poll(5, TimeUnit.SECONDS);
                assertNotNull(requestMessage);
                assertEquals("request", requestMessage.payload());
                assertEquals(43, requestMessage.templateId());
                assertEquals(123_456_789L, requestMessage.correlationId());
                assertEquals(ping.nodeId(), requestMessage.sourceNodeId());
                assertEquals(ping.serviceId(), requestMessage.sourceServiceId());
                assertEquals(echo.nodeId(), requestMessage.targetNodeId());
                assertEquals(echo.serviceId(), requestMessage.targetServiceId());

                NativeCounter customCounter = ping.registerCounter("java_requests_total");
                customCounter.increment();
                customCounter.add(41);
                NativeGauge customGauge = ping.registerGauge("java_queue_depth");
                customGauge.set(7);

                try (RingloomMetricsReader metrics = ping.metricsReader()) {
                    assertTrue(metrics.counterCount() > 0);
                    RingStats stats = metrics.ringStats("messages");
                    assertTrue(stats.capacityBytes() > 0);
                    assertEquals(stats.capacityBytes(), stats.usedBytes() + stats.freeBytes());
                    assertTrue(metrics.countersSnapshot().stream().anyMatch(sample ->
                        sample.name().equals("service_messages_sent_total") && sample.value() >= 3
                    ));
                    assertTrue(metrics.countersSnapshot().stream().anyMatch(sample ->
                        sample.name().equals("java_requests_total")
                            && sample.kind() == MetricKind.COUNTER
                            && sample.value() == 42
                    ));
                    assertTrue(metrics.countersSnapshot().stream().anyMatch(sample ->
                        sample.name().equals("java_queue_depth")
                            && sample.kind() == MetricKind.GAUGE
                            && sample.value() == 7
                    ));
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

    private record CapturedMessage(
        long correlationId,
        short sourceNodeId,
        short sourceServiceId,
        short targetNodeId,
        short targetServiceId,
        int templateId,
        String payload
    ) {
        static CapturedMessage copyOf(RingloomMessage message) {
            return new CapturedMessage(
                message.correlationId(),
                message.sourceNodeId(),
                message.sourceServiceId(),
                message.targetNodeId(),
                message.targetServiceId(),
                message.templateId(),
                new String(message.copyPayload(), StandardCharsets.UTF_8)
            );
        }
    }
}
