package io.ringloom.service;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class RingloomClientTargetsIT {
    @Test
    void exposesDiscoveredTargetServiceIdsWithLeaderStatus() throws Exception {
        Path workspace = TestSupport.createWorkspace("ringloom-java-targets-");
        boolean success = false;

        try (TestBroker broker = TestBroker.start(TestSupport.repoRoot(), workspace);
             RingloomService targetOne = RingloomService.start(leaderEnabledConfig("route-target", broker));
             RingloomService targetTwo = RingloomService.start(leaderEnabledConfig("route-target", broker));
             RingloomService sender = RingloomService.start(TestSupport.serviceConfig("route-sender", broker));
             RingloomClient client = sender.createClient("route-target")) {

            List<TargetService> targets = waitForTargets(sender, client, 2, true);

            assertEquals(2, targets.size());
            assertTrue(targets.stream().anyMatch(target ->
                target.targetNodeId() == targetOne.nodeId() && target.targetServiceId() == targetOne.serviceId()
            ));
            assertTrue(targets.stream().anyMatch(target ->
                target.targetNodeId() == targetTwo.nodeId() && target.targetServiceId() == targetTwo.serviceId()
            ));
            assertEquals(1L, targets.stream().filter(TargetService::leader).count());
            assertSame(targets, client.targetServices());
            success = true;
        } finally {
            TestSupport.cleanupWorkspace(workspace, success);
        }
    }

    private static ServiceConfig leaderEnabledConfig(String serviceName, TestBroker broker) {
        return new ServiceConfig(
            serviceName,
            broker.storagePath(),
            broker.group(),
            (short) 1,
            false,
            10_000,
            65_536L,
            1_048_576L,
            true
        );
    }

    private static List<TargetService> waitForTargets(
        RingloomService service,
        RingloomClient client,
        int expectedCount,
        boolean requireLeader
    ) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        List<TargetService> lastSeen = List.of();

        while (System.nanoTime() < deadline) {
            service.pollControl(256);
            lastSeen = client.targetServices();
            boolean hasExpectedCount = lastSeen.size() == expectedCount;
            boolean hasLeader = !requireLeader || lastSeen.stream().anyMatch(TargetService::leader);
            if (hasExpectedCount && hasLeader) {
                return lastSeen;
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(20));
        }

        throw new AssertionError("timed out waiting for target discovery, last seen: " + lastSeen);
    }
}
