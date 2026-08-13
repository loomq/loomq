package com.loomq.common;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * LatencyMetricsRegistry 分位数计算与导出契约测试。
 *
 * <p>techdebt D3:三个指标 registry 此前零直接测试,分位数数值/边界从未被断言;
 * 本测试固定样本序列,锁定直方图近似语义(返回累计越过目标分位的桶上界)。</p>
 */
class LatencyMetricsRegistryTest {

    @Test
    void p95WithLightTailReturnsLowerBucketBound() {
        LatencyMetricsRegistry reg = new LatencyMetricsRegistry();
        for (int i = 0; i < 95; i++) {
            reg.recordTriggerLatency(1);
        }
        for (int i = 0; i < 5; i++) {
            reg.recordTriggerLatency(1000);
        }
        // 100 样本 p95 目标 = ceil(95) = 95;1ms 桶累计 95 即越线 → 返回 1
        assertEquals(1, reg.calculateP95Latency());
    }

    @Test
    void p95WithHeavyTailReturnsTailBucketBound() {
        LatencyMetricsRegistry reg = new LatencyMetricsRegistry();
        for (int i = 0; i < 90; i++) {
            reg.recordWakeLatency(1);
        }
        for (int i = 0; i < 10; i++) {
            reg.recordWakeLatency(5000);
        }
        // 目标 95:1ms 桶累计 90 未越线,5000ms 桶累计 100 越线 → 返回 5000(近似上偏)
        assertEquals(5000, reg.calculateP95WakeLatency());
    }

    @Test
    void emptyAndSingleSampleBounds() {
        LatencyMetricsRegistry empty = new LatencyMetricsRegistry();
        assertEquals(0, empty.calculateP95Latency());
        assertEquals(0, empty.calculateP95CohortFlushDuration());

        LatencyMetricsRegistry single = new LatencyMetricsRegistry();
        single.recordCohortFlushDuration(250);  // findCohortFlushBucket(250) → 桶 250
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
                            reg.recordWebhookLatency(i % 100);
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
            if (line.startsWith("loomq_webhook_latency_samples ")) {
                lines.add(line);
            }
        }
        assertEquals(1, lines.size());
        assertEquals("loomq_webhook_latency_samples " + (threads * perThread), lines.get(0));
    }
}
