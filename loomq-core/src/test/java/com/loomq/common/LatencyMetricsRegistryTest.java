package com.loomq.common;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * LatencyMetricsRegistry 导出契约测试。
 *
 * <p>round 12:直方图/分位算法语义收口至 HistogramTest;本类只锁两条活链路的
 * 导出名与采样计数(finalize 更名自 webhook,trigger/wake/total 三组死直方图已删)。</p>
 */
class LatencyMetricsRegistryTest {

    @Test
    void emptyRegistryReturnsZeroP95() {
        LatencyMetricsRegistry empty = new LatencyMetricsRegistry();
        assertEquals(0, empty.calculateP95FinalizeDuration());
        assertEquals(0, empty.calculateP95CohortFlushDuration());

        LatencyMetricsRegistry single = new LatencyMetricsRegistry();
        single.recordCohortFlushDuration(250);  // 命中 COHORT_FLUSH_BOUNDS 的 250 桶
        assertEquals(250, single.calculateP95CohortFlushDuration());
    }

    @Test
    void concurrentRecordsDoNotLoseSamples() throws Exception {
        LatencyMetricsRegistry reg = new LatencyMetricsRegistry();
        int threads = 8;
        int perThread = 500;
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        var executor = Executors.newVirtualThreadPerTaskExecutor();
        try {
            for (int t = 0; t < threads; t++) {
                executor.submit(() -> {
                    try {
                        start.await();
                        for (int i = 0; i < perThread; i++) {
                            reg.recordFinalizeDuration(i % 100);
                        }
                    } catch (InterruptedException ignored) {
                    } finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();
            assert done.await(10, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
        }
        // 导出块含 samples 计数,一并验证导出不抛异常
        StringBuilder sb = new StringBuilder();
        reg.appendPrometheusMetrics(sb);
        String exported = sb.toString();
        List<String> lines = new ArrayList<>();
        for (String line : exported.split("\n")) {
            if (line.startsWith("loomq_finalize_duration_samples ")) {
                lines.add(line);
            }
        }
        assertEquals(1, lines.size());
        assertEquals("loomq_finalize_duration_samples " + (threads * perThread), lines.get(0));
    }
}
