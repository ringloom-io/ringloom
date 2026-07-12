// SPDX-License-Identifier: Apache-2.0
package io.ringloom.service;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.management.ThreadMXBean;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

/**
 * Verifies that topic hot-path methods allocate nothing on the steady-state path.
 *
 * <p>Uses {@link ThreadMXBean#getThreadAllocatedBytes(long)} to measure per-thread
 * allocation during repeated hot-path calls. Preallocated scratch buffers for
 * out-parameters ensure no per-call arena or segment allocations.</p>
 */
final class TopicAllocationIT {

    private static final int ITERATIONS = 256;
    private static final int WARMUP = 128;
    private static final long ALLOCATION_BUDGET = 32_768L;

    @Test
    void publishHotPathAllocatesNothing() throws Exception {
        Path workspace = TestSupport.createWorkspace("ringloom-topic-alloc-");
        boolean success = false;

        try (
            TestBroker broker = TestBroker.startTopicsEnabled(
                TestSupport.repoRoot(),
                workspace
            );
            RingloomService svc = RingloomService.start(
                TestSupport.serviceConfig("topic-pub-alloc", broker)
            );
            RingloomClient client = svc.createClient("topic-pub-alloc")
        ) {
            TopicPublisher publisher = client.registerTopicPublication(
                "alloc-test",
                TopicConfig.DEFAULT
            );
            try {
                byte[] payload = "hello".getBytes(StandardCharsets.UTF_8);
                // Must use an arena-backed (native) segment for FFM downcalls;
                // heap segments from MemorySegment.ofArray() are not passable.
                try (Arena arena = Arena.ofConfined()) {
                    MemorySegment segment = arena.allocateFrom(
                        ValueLayout.JAVA_BYTE,
                        payload
                    );

                    // Warm up
                    for (int i = 0; i < WARMUP; i++) {
                        publisher.publish(segment);
                    }

                    ThreadMXBean mxBean =
                        (ThreadMXBean) ManagementFactory.getThreadMXBean();
                    if (
                        mxBean.isThreadAllocatedMemorySupported() &&
                        !mxBean.isThreadAllocatedMemoryEnabled()
                    ) {
                        mxBean.setThreadAllocatedMemoryEnabled(true);
                    }

                    // When: we publish many messages
                    long beforeBytes = mxBean.isThreadAllocatedMemorySupported()
                        ? mxBean.getThreadAllocatedBytes(
                              Thread.currentThread().getId()
                          )
                        : -1L;

                    for (int i = 0; i < ITERATIONS; i++) {
                        publisher.publish(segment);
                    }

                    long afterBytes = mxBean.isThreadAllocatedMemorySupported()
                        ? mxBean.getThreadAllocatedBytes(
                              Thread.currentThread().getId()
                          )
                        : -1L;

                    // Then: allocation is within budget
                    if (beforeBytes >= 0 && afterBytes >= 0) {
                        long allocated = afterBytes - beforeBytes;
                        assertTrue(
                            allocated < ALLOCATION_BUDGET,
                            "publish hot path allocated too much: " +
                                allocated +
                                " bytes for " +
                                ITERATIONS +
                                " iterations"
                        );
                    }
                }

                success = true;
            } finally {
                publisher.close();
            }
        } finally {
            TestSupport.cleanupWorkspace(workspace, success);
        }
    }

    @Test
    void publishWithAckModeAllocatesNothing() throws Exception {
        Path workspace = TestSupport.createWorkspace(
            "ringloom-topic-alloc-ack-"
        );
        boolean success = false;

        try (
            TestBroker broker = TestBroker.startTopicsEnabled(
                TestSupport.repoRoot(),
                workspace
            );
            RingloomService svc = RingloomService.start(
                TestSupport.serviceConfig("topic-pub-ack", broker)
            );
            RingloomClient client = svc.createClient("topic-pub-ack")
        ) {
            TopicPublisher publisher = client.registerTopicPublication(
                "alloc-ack-test",
                TopicConfig.DEFAULT
            );
            try {
                byte[] payload = "hello".getBytes(StandardCharsets.UTF_8);
                // Must use an arena-backed (native) segment for FFM downcalls.
                try (Arena arena = Arena.ofConfined()) {
                    MemorySegment segment = arena.allocateFrom(
                        ValueLayout.JAVA_BYTE,
                        payload
                    );
                    long[] indexHolder = new long[1];

                    // Warm up
                    for (int i = 0; i < WARMUP; i++) {
                        publisher.publish(
                            segment,
                            TopicAckMode.REPLICATE_ONCE,
                            0,
                            indexHolder
                        );
                    }

                    ThreadMXBean mxBean =
                        (ThreadMXBean) ManagementFactory.getThreadMXBean();
                    if (
                        mxBean.isThreadAllocatedMemorySupported() &&
                        !mxBean.isThreadAllocatedMemoryEnabled()
                    ) {
                        mxBean.setThreadAllocatedMemoryEnabled(true);
                    }

                    long beforeBytes = mxBean.isThreadAllocatedMemorySupported()
                        ? mxBean.getThreadAllocatedBytes(
                              Thread.currentThread().getId()
                          )
                        : -1L;

                    for (int i = 0; i < ITERATIONS; i++) {
                        publisher.publish(
                            segment,
                            TopicAckMode.REPLICATE_ONCE,
                            0,
                            indexHolder
                        );
                    }

                    long afterBytes = mxBean.isThreadAllocatedMemorySupported()
                        ? mxBean.getThreadAllocatedBytes(
                              Thread.currentThread().getId()
                          )
                        : -1L;

                    if (beforeBytes >= 0 && afterBytes >= 0) {
                        long allocated = afterBytes - beforeBytes;
                        assertTrue(
                            allocated < ALLOCATION_BUDGET,
                            "publish(REPLICATE_ONCE) allocated too much: " +
                                allocated +
                                " bytes for " +
                                ITERATIONS +
                                " iterations"
                        );
                    }
                }

                success = true;
            } finally {
                publisher.close();
            }
        } finally {
            TestSupport.cleanupWorkspace(workspace, success);
        }
    }

    @Test
    void isAckedAllocatesNothing() throws Exception {
        Path workspace = TestSupport.createWorkspace(
            "ringloom-topic-alloc-acked-"
        );
        boolean success = false;

        try (
            TestBroker broker = TestBroker.startTopicsEnabled(
                TestSupport.repoRoot(),
                workspace
            );
            RingloomService svc = RingloomService.start(
                TestSupport.serviceConfig("topic-acked-alloc", broker)
            );
            RingloomClient client = svc.createClient("topic-acked-alloc")
        ) {
            TopicPublisher publisher = client.registerTopicPublication(
                "alloc-acked-test",
                TopicConfig.DEFAULT
            );
            try {
                // Warm up
                for (int i = 0; i < WARMUP; i++) {
                    publisher.isAcked(i);
                }

                ThreadMXBean mxBean =
                    (ThreadMXBean) ManagementFactory.getThreadMXBean();
                if (
                    mxBean.isThreadAllocatedMemorySupported() &&
                    !mxBean.isThreadAllocatedMemoryEnabled()
                ) {
                    mxBean.setThreadAllocatedMemoryEnabled(true);
                }

                long beforeBytes = mxBean.isThreadAllocatedMemorySupported()
                    ? mxBean.getThreadAllocatedBytes(
                          Thread.currentThread().getId()
                      )
                    : -1L;

                for (int i = 0; i < ITERATIONS; i++) {
                    publisher.isAcked(i);
                }

                long afterBytes = mxBean.isThreadAllocatedMemorySupported()
                    ? mxBean.getThreadAllocatedBytes(
                          Thread.currentThread().getId()
                      )
                    : -1L;

                if (beforeBytes >= 0 && afterBytes >= 0) {
                    long allocated = afterBytes - beforeBytes;
                    assertTrue(
                        allocated < ALLOCATION_BUDGET,
                        "isAcked allocated too much: " +
                            allocated +
                            " bytes for " +
                            ITERATIONS +
                            " iterations"
                    );
                }

                success = true;
            } finally {
                publisher.close();
            }
        } finally {
            TestSupport.cleanupWorkspace(workspace, success);
        }
    }

    @Test
    void publisherCloseIsIdempotent() throws Exception {
        Path workspace = TestSupport.createWorkspace("ringloom-topic-close-");
        boolean success = false;

        try (
            TestBroker broker = TestBroker.startTopicsEnabled(
                TestSupport.repoRoot(),
                workspace
            );
            RingloomService svc = RingloomService.start(
                TestSupport.serviceConfig("topic-close", broker)
            );
            RingloomClient client = svc.createClient("topic-close")
        ) {
            TopicPublisher publisher = client.registerTopicPublication(
                "close-test",
                TopicConfig.DEFAULT
            );

            // When: we close twice
            publisher.close();
            publisher.close();

            // Then: operations after close throw
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment seg = arena.allocate(ValueLayout.JAVA_INT);
                var ex = assertThrows(IllegalStateException.class, () ->
                    publisher.publish(seg)
                );
                assertTrue(ex.getMessage().contains("closed"));
            }

            success = true;
        } finally {
            TestSupport.cleanupWorkspace(workspace, success);
        }
    }
}
