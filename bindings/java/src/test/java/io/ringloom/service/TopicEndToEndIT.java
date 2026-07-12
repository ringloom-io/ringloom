// SPDX-License-Identifier: Apache-2.0
package io.ringloom.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * End-to-end topic publish/subscribe against a topics-enabled two-node broker.
 *
 * <p>Node 1 is the topic leader (co-located with the producer service); node 2 is
 * a replica peer. This exercises the real control-plane registration (templates
 * 7–10), publish over Aeron IPC, the subscriber reading the local replica via a
 * ringloom-queue tailer, and {@code replicate_once} ack completion driven by
 * throttled HWM feedback (template 15).</p>
 */
final class TopicEndToEndIT {

    private static final int POLL_ATTEMPTS = 200;
    private static final long POLL_SLEEP_MS = 25;

    @Test
    void fireAndForgetPublishDeliveredInOrderToEarliestSubscriber() throws Exception {
        Path workspace = TestSupport.createWorkspace("ringloom-topic-e2e-");
        boolean success = false;
        // Single-node topics-enabled broker: pub + sub co-located with the
        // leader, so the subscriber reads the master queue directly (no
        // replication dependency). This isolates the publish/poll path.
        try (TestBroker broker = TestBroker.startTopicsEnabled(TestSupport.repoRoot(), workspace)) {
            try (
                RingloomService svc = RingloomService.start(
                    TestSupport.serviceConfig("topic-e2e", broker)
                );
                RingloomClient pubClient = svc.createClient("topic-e2e");
                RingloomClient subClient = svc.createClient("topic-e2e-sub")
            ) {
                TopicPublisher publisher = pubClient.registerTopicPublication(
                    "e2e-ff",
                    TopicConfig.DEFAULT
                );
                TopicSubscription subscription = subClient.subscribeTopic(
                    "e2e-ff",
                    TopicStart.EARLIEST
                );
                try {
                    // topicId is broker-assigned and stable across pub/sub.
                    assertTrue(publisher.topicId() != 0);
                    assertEquals(publisher.topicId(), subscription.topicId());

                    byte[][] payloads = payloads("msg-", 5);
                    for (byte[] p : payloads) {
                        // Drive the control plane so the broker is ready to accept.
                        svc.pollControl(64);
                        publishBytes(publisher, p);
                    }

                    List<byte[]> received = drain(subscription, payloads.length);
                    assertEquals(payloads.length, received.size());
                    for (int i = 0; i < payloads.length; i++) {
                        assertEquals(new String(payloads[i], StandardCharsets.UTF_8),
                                     new String(received.get(i), StandardCharsets.UTF_8));
                    }
                    success = true;
                } finally {
                    subscription.close();
                    publisher.close();
                }
            }
        } finally {
            TestSupport.cleanupWorkspace(workspace, success);
        }
    }

    @Test
    void latestSubscriberMissesPreExistingMessages() throws Exception {
        Path workspace = TestSupport.createWorkspace("ringloom-topic-latest-");
        boolean success = false;
        try (TestBroker broker = TestBroker.startTopicsEnabled(TestSupport.repoRoot(), workspace)) {
            try (
                RingloomService svc = RingloomService.start(
                    TestSupport.serviceConfig("topic-latest", broker)
                );
                RingloomClient pubClient = svc.createClient("topic-latest");
                RingloomClient subClient = svc.createClient("topic-latest-sub")
            ) {
                TopicPublisher publisher = pubClient.registerTopicPublication(
                    "e2e-latest",
                    TopicConfig.DEFAULT
                );
                try {
                    // Publish before the LATEST subscriber exists.
                    publishBytes(publisher, "before".getBytes(StandardCharsets.UTF_8));
                    // Allow the broker to append the message to the master queue.
                    Thread.sleep(200);

                    TopicSubscription subscription = subClient.subscribeTopic(
                        "e2e-latest",
                        TopicStart.LATEST
                    );
                    try {
                        // Publish after subscribing — LATEST subscriber sees only this.
                        publishBytes(publisher, "after".getBytes(StandardCharsets.UTF_8));
                        List<byte[]> received = drain(subscription, 1);
                        assertEquals(1, received.size());
                        assertEquals("after", new String(received.get(0), StandardCharsets.UTF_8));
                        success = true;
                    } finally {
                        subscription.close();
                    }
                } finally {
                    publisher.close();
                }
            }
        } finally {
            TestSupport.cleanupWorkspace(workspace, success);
        }
    }

    @Test
    void replicateOnceAckCompletesOnTwoNodeBroker() throws Exception {
        Path workspace = TestSupport.createWorkspace("ringloom-topic-ack-");
        boolean success = false;
        try (TwoNodeBrokerCluster cluster = TwoNodeBrokerCluster.start(workspace)) {
            try (
                RingloomService pubService = RingloomService.start(
                    TestSupport.serviceConfig("topic-pub-ack", cluster.leader)
                );
                RingloomClient pubClient = pubService.createClient("topic-pub-ack")
            ) {
                TopicPublisher publisher = pubClient.registerTopicPublication(
                    "e2e-ack",
                    TopicConfig.DEFAULT
                );
                try {
                    long[] outIndex = new long[1];
                    byte[] payload = "ack-me".getBytes(StandardCharsets.UTF_8);
                    int status;
                    try (Arena arena = Arena.ofConfined()) {
                        MemorySegment seg = arena.allocateFrom(ValueLayout.JAVA_BYTE, payload);
                        status = publisher.publish(seg, TopicAckMode.REPLICATE_ONCE, 0L, outIndex);
                    }
                    assertEquals(RingloomStatus.OK, status);
                    long token = outIndex[0];
                    assertTrue(token > 0, "publish should assign a positive sequence token");

                    // Drive the control plane so ack feedback is processed.
                    long deadline = System.currentTimeMillis() + 5_000;
                    boolean acked = false;
                    while (System.currentTimeMillis() < deadline) {
                        pubService.pollControl(64);
                        if (publisher.isAcked(token)) {
                            acked = true;
                            break;
                        }
                        Thread.sleep(POLL_SLEEP_MS);
                    }
                    assertTrue(acked, "replicate_once publish should be acked once the replica catches up");
                    assertTrue(publisher.replicatedCount() >= token);
                    success = true;
                } finally {
                    publisher.close();
                }
            }
        } finally {
            TestSupport.cleanupWorkspace(workspace, success);
        }
    }

    @Test
    void registerWithMismatchedConfigReportsConfigMismatch() throws Exception {
        Path workspace = TestSupport.createWorkspace("ringloom-topic-mismatch-");
        boolean success = false;
        try (TwoNodeBrokerCluster cluster = TwoNodeBrokerCluster.start(workspace)) {
            try (
                RingloomService pubService = RingloomService.start(
                    TestSupport.serviceConfig("topic-mismatch", cluster.leader)
                );
                RingloomClient pubClient = pubService.createClient("topic-mismatch")
            ) {
                TopicPublisher first = pubClient.registerTopicPublication(
                    "e2e-mismatch",
                    new TopicConfig("FAST_DAILY", 0, 0)
                );
                first.close();
                // Same topic, different geometry -> config_mismatch.
                var ex = org.junit.jupiter.api.Assertions.assertThrows(
                    RingloomException.class,
                    () -> pubClient.registerTopicPublication(
                        "e2e-mismatch",
                        new TopicConfig("DAILY", 0, 0)
                    )
                );
                assertEquals(RingloomStatus.TOPIC_CONFIG_MISMATCH, ex.statusCode());
                success = true;
            }
        } finally {
            TestSupport.cleanupWorkspace(workspace, success);
        }
    }

    // ── Helpers ─────────────────────────────────────────────────────────

    private static void publishBytes(TopicPublisher publisher, byte[] payload) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment seg = arena.allocateFrom(ValueLayout.JAVA_BYTE, payload);
            int status = publisher.publish(seg);
            assertEquals(RingloomStatus.OK, status, "fire-and-forget publish should succeed");
        }
    }

    private static List<byte[]> drain(TopicSubscription subscription, int expected) throws Exception {
        TopicPollResult result = new TopicPollResult();
        List<byte[]> received = new ArrayList<>();
        int attempts = 0;
        while (received.size() < expected && attempts < POLL_ATTEMPTS * Math.max(1, expected)) {
            while (subscription.poll(result) == RingloomStatus.OK) {
                MemorySegment seg = result.payloadSegment();
                if (seg.byteSize() > 0) {
                    received.add(seg.toArray(ValueLayout.JAVA_BYTE));
                }
                if (received.size() >= expected) break;
            }
            if (received.size() < expected) {
                Thread.sleep(POLL_SLEEP_MS);
                attempts++;
            }
        }
        return received;
    }

    private static byte[][] payloads(String prefix, int count) {
        byte[][] out = new byte[count][];
        for (int i = 0; i < count; i++) {
            out[i] = (prefix + i).getBytes(StandardCharsets.UTF_8);
        }
        return out;
    }

    /** A two-node topics-enabled broker cluster: node 1 (leader) + node 2 (peer). */
    static final class TwoNodeBrokerCluster implements AutoCloseable {
        private final Path workspace;
        private final TestBroker leader;
        private final TestBroker replica;

        private TwoNodeBrokerCluster(Path workspace, TestBroker leader, TestBroker replica) {
            this.workspace = workspace;
            this.leader = leader;
            this.replica = replica;
        }

        static TwoNodeBrokerCluster start(Path workspace) throws IOException, InterruptedException {
            Path repoRoot = TestSupport.repoRoot();
            // Node 1 (leader) knows about node 2; node 2 knows about node 1.
            TestBroker leader = TestBroker.start(
                repoRoot, workspace, (short) 1, 19001, "ringloom-topic-it",
                new String[] { "2@127.0.0.1:19002" }, true
            );
            // Brief pause so node 2 can resolve node 1's endpoint on connect.
            TestBroker replica = TestBroker.start(
                repoRoot, workspace, (short) 2, 19002, "ringloom-topic-it",
                new String[] { "1@127.0.0.1:19001" }, true
            );
            // Give the full-mesh replication a moment to establish.
            Thread.sleep(500);
            return new TwoNodeBrokerCluster(workspace, leader, replica);
        }

        @Override
        public void close() throws IOException, InterruptedException {
            try {
                replica.close();
            } finally {
                leader.close();
            }
        }
    }
}
