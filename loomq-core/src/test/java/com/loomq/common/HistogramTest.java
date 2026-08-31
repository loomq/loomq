package com.loomq.common;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * 共享直方图契约测试(round 12):ceil-累积百分位语义自 LatencyMetricsRegistryTest 移植
 * (LMR 的 trigger/wake 直方图为死代码随 round 12 删除),并补齐 max/mean/边界。
 */
class HistogramTest {

    private static final int[] BOUNDS = {0, 1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000};

    @Test
    void p95WithLightTailReturnsLowerBucketBound() {
        Histogram h = new Histogram(BOUNDS);
        for (int i = 0; i < 95; i++) h.record(1);
        for (int i = 0; i < 5; i++) h.record(1000);
        // 100 样本 p95 目标 = ceil(95) = 95;1ms 桶累计 95 即越线 → 返回 1
        assertEquals(1, h.percentile(0.95));
    }

    @Test
    void p95WithHeavyTailReturnsTailBucketBound() {
        Histogram h = new Histogram(BOUNDS);
        for (int i = 0; i < 90; i++) h.record(1);
        for (int i = 0; i < 10; i++) h.record(5000);
        // 目标 95:1 桶累计 90 未越线,5000 桶累计 100 越线 → 返回 5000(近似上偏)
        assertEquals(5000, h.percentile(0.95));
    }

    @Test
    void emptyHistogramReturnsZero() {
        Histogram h = new Histogram(BOUNDS);
        assertEquals(0, h.percentile(0.95));
        assertEquals(0, h.max());
        assertEquals(0, h.mean());
        assertEquals(0, h.sampleCount());
    }

    @Test
    void boundaryValueLandsInItsOwnBucket() {
        Histogram h = new Histogram(BOUNDS);
        h.record(250);
        assertEquals(250, h.percentile(0.5));
        assertEquals(250, h.max());
    }

    @Test
    void belowSmallestBoundFallsToFirstBucket() {
        Histogram h = new Histogram(BOUNDS);   // bounds[0] = 0
        h.record(-3);
        assertEquals(0, h.percentile(0.99));
    }

    @Test
    void beyondLargestBoundClampsToLastBucket() {
        Histogram h = new Histogram(BOUNDS);
        h.record(99_999);
        assertEquals(10000, h.percentile(0.5));
        assertEquals(10000, h.max());
    }

    @Test
    void meanUsesBucketMidpoints() {
        Histogram h = new Histogram(BOUNDS);
        h.record(1);    // 桶 1:中点 (1+5)/2 = 3
        h.record(5000); // 桶 5000:中点 (5000+10000)/2 = 7500
        assertEquals((3 + 7500) / 2, h.mean());
    }

    @Test
    void lastBucketMidpointIsItsLowerBound() {
        Histogram h = new Histogram(BOUNDS);
        h.record(10000);
        assertEquals(10000, h.mean());  // 末桶 upper=lower → 中点=下界
    }

    @Test
    void concurrentRecordsDoNotLoseSamples() throws Exception {
        Histogram h = new Histogram(BOUNDS);
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
                            h.record(i % 100);
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
        assertEquals(threads * perThread, h.sampleCount());
    }
}
