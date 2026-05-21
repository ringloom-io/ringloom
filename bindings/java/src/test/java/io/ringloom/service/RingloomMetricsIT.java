package io.ringloom.service;

import java.nio.file.Path;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class RingloomMetricsIT {
    @Test
    void registeredApplicationMetricsUseNativeSlots() throws Exception {
        Path workspace = TestSupport.createWorkspace("ringloom-java-metrics-");
        boolean success = false;

        try (TestBroker broker = TestBroker.start(TestSupport.repoRoot(), workspace);
             RingloomService service = RingloomService.start(TestSupport.serviceConfig("metrics", broker));
             RingloomMetricsReader reader = service.metricsReader()) {

            NativeCounter counter = service.registerCounter("orders_total");
            NativeGauge gauge = service.registerGauge("queue_depth");

            counter.increment();
            counter.add(4);
            gauge.set(7);

            assertEquals(5, counter.value());
            assertEquals(7, gauge.value());

            Map<String, MetricSample> samples = reader.countersSnapshot().stream()
                .collect(Collectors.toMap(MetricSample::name, sample -> sample));
            assertEquals(new MetricSample("orders_total", MetricKind.COUNTER, 5), samples.get("orders_total"));
            assertEquals(new MetricSample("queue_depth", MetricKind.GAUGE, 7), samples.get("queue_depth"));

            success = true;
        } finally {
            TestSupport.cleanupWorkspace(workspace, success);
        }
    }
}
