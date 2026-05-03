package io.ringloom.service;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Predicate;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class RingloomServiceLifecycleIT {
    @Test
    void startsAndStopsAgainstRealBroker() throws Exception {
        Path workspace = TestSupport.createWorkspace("ringloom-java-lifecycle-");
        boolean success = false;
        try (TestBroker broker = TestBroker.start(TestSupport.repoRoot(), workspace);
             RingloomService service = RingloomService.start(TestSupport.serviceConfig("java-lifecycle", broker))) {
            assertTrue(service.serviceId() > 0);
            assertEquals(1, service.nodeId());
            success = true;
        } finally {
            TestSupport.cleanupWorkspace(workspace, success);
        }
    }

    @Test
    void clientLifecycleHandlerReceivesAvailabilityAndUnavailability() throws Exception {
        Path workspace = TestSupport.createWorkspace("ringloom-java-client-lifecycle-");
        boolean success = false;

        try (TestBroker broker = TestBroker.start(TestSupport.repoRoot(), workspace);
             RingloomService watcher = RingloomService.start(TestSupport.serviceConfig("java-watcher", broker));
             RingloomClient client = watcher.createClient("java-observed")) {

            List<ServiceLifecycleEvent> events = new CopyOnWriteArrayList<>();
            client.onLifecycle(events::add);

            int observedServiceId;
            RingloomService observed = RingloomService.start(TestSupport.serviceConfig("java-observed", broker));
            try {
                observedServiceId = observed.serviceId();
                ServiceLifecycleEvent available = waitForLifecycle(
                    watcher,
                    events,
                    event -> event.type() == ServiceLifecycleEventType.AVAILABLE
                        && event.serviceId() == observedServiceId,
                    "available"
                );
                assertEquals("java-observed", available.serviceName());
                assertEquals(1, available.nodeId());
            } finally {
                observed.close();
            }

            ServiceLifecycleEvent unavailable = waitForLifecycle(
                watcher,
                events,
                event -> event.type() == ServiceLifecycleEventType.UNAVAILABLE
                    && event.serviceId() == observedServiceId,
                "unavailable"
            );
            assertEquals("java-observed", unavailable.serviceName());
            assertEquals(1, unavailable.nodeId());
            success = true;
        } finally {
            TestSupport.cleanupWorkspace(workspace, success);
        }
    }

    private static ServiceLifecycleEvent waitForLifecycle(
        RingloomService service,
        List<ServiceLifecycleEvent> events,
        Predicate<ServiceLifecycleEvent> predicate,
        String description
    ) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        int seen = 0;

        while (System.nanoTime() < deadline) {
            service.pollControl(256);
            for (; seen < events.size(); seen++) {
                ServiceLifecycleEvent event = events.get(seen);
                if (predicate.test(event)) {
                    return event;
                }
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(20));
        }

        throw new AssertionError("timed out waiting for " + description + " lifecycle event, events: " + events);
    }
}
