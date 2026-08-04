package com.loomq.benchmark;

import static org.junit.jupiter.api.Assertions.*;

import com.loomq.LoomqEngine;
import com.loomq.common.MetricsCollector;
import com.loomq.domain.intent.AckMode;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.domain.intent.PrecisionTierCatalog;
import com.loomq.spi.DeliveryHandler;
import com.loomq.spi.IntentObserver;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * 投递路径 DURABLE 基准测试：测量 schedule -> deliver -> ACKED 全周期的吞吐与延迟。
 *
 * <p>每个档位执行 1 次预热（200 intents）+ 5 次测量（1000 intents/次）。
 * 输出 RESULT|delivery|... 标记，兼容 benchmark 脚本解析。
 * 性能断言：DURABLE delivery overhead p99 < {@value #OVERHEAD_P99_THRESHOLD_MS}ms
 * （首次基线运行后校准）。
 *
 * <p>关键指标 <b>Delivery overhead</b> = E2E p99 - wakeup p99，隔离了 finalizeIntent 中
 * awaitCommit 的代价（I2 修复引入的开销）。
 *
 * <p>注意：{@link MetricsCollector} 的 wakeup 延迟以微秒记录（尽管参数名 latencyMs），
 * 输出时 / 1000 转毫秒。
 */
@Tag("benchmark")
class DeliveryPathBenchmark {

    private static final DeliveryHandler SUCCESS = intent ->
        CompletableFuture.completedFuture(DeliveryHandler.DeliveryResult.SUCCESS);

    private static final int WARMUP_COUNT = 200;
    private static final int MEASURE_COUNT = 500;
    private static final int ITERATIONS = 5;
    private static final long FIRE_DELAY_SEC = 5; // time from creation to executeAt
    private static final long COOLDOWN_MS = 1000;  // pause between iterations
    private static final double OVERHEAD_P99_THRESHOLD_MS = 500.0; // 首次基线后校准；含 dispatch+finalize 非 awaitCommit 独占

    @Test
    void measureDeliveryThroughput_Ultra(@TempDir Path tmp) throws Exception {
        measureDelivery(tmp, PrecisionTier.ULTRA);
    }

    @Test
    void measureDeliveryThroughput_Fast(@TempDir Path tmp) throws Exception {
        measureDelivery(tmp, PrecisionTier.FAST);
    }

    @Test
    void measureDeliveryThroughput_High(@TempDir Path tmp) throws Exception {
        measureDelivery(tmp, PrecisionTier.HIGH);
    }

    @Test
    void measureDeliveryThroughput_Standard(@TempDir Path tmp) throws Exception {
        measureDelivery(tmp, PrecisionTier.STANDARD);
    }

    @Test
    void measureDeliveryThroughput_Economy(@TempDir Path tmp) throws Exception {
        measureDelivery(tmp, PrecisionTier.ECONOMY);
    }

    private void measureDelivery(Path tmp, PrecisionTier tier) throws Exception {
        int consumers = PrecisionTierCatalog.defaultCatalog().profile(tier).consumerCount();

        try (LoomqEngine engine = LoomqEngine.builder()
                .dataDir(tmp).nodeId("bench-" + tier.name())
                .deliveryHandler(SUCCESS).build()) {
            engine.start();

            // Phase 1: Warmup (no metrics recorded)
            runIteration(engine, tier, WARMUP_COUNT, null);
            Thread.sleep(COOLDOWN_MS);

            // Phase 2: Measurement iterations
            long[] createMs = new long[ITERATIONS];
            long[] deliveryMs = new long[ITERATIONS];
            ConcurrentLinkedQueue<Long> allLatencies = new ConcurrentLinkedQueue<>();

            for (int i = 0; i < ITERATIONS; i++) {
                IterationResult r = runIteration(engine, tier, MEASURE_COUNT, allLatencies);
                createMs[i] = r.createMs;
                deliveryMs[i] = r.deliveryMs;
                if (i < ITERATIONS - 1) Thread.sleep(COOLDOWN_MS);
            }

            // Read wakeup latency (cumulative across warmup + all iterations)
            MetricsCollector.LatencySnapshot wake =
                engine.getMetricsCollector().getWakeupLatencySnapshot(tier);

            // Compute E2E latency percentiles from combined samples
            long[] sorted = allLatencies.stream().mapToLong(Long::longValue).sorted().toArray();
            long e2eP50 = sorted[sorted.length / 2];
            long e2eP95 = sorted[(int) (sorted.length * 0.95)];
            long e2eP99 = sorted[(int) (sorted.length * 0.99)];

            // Compute throughput stats
            double meanCreateMs = mean(createMs);
            double stddevCreateMs = stddev(createMs, meanCreateMs);
            double meanDeliveryMs = mean(deliveryMs);
            double stddevDeliveryMs = stddev(deliveryMs, meanDeliveryMs);
            double meanQps = MEASURE_COUNT * 1000.0 / meanDeliveryMs;

            // Wakeup latency is in microseconds; convert to milliseconds
            long wakeP50Ms = wake.p50() / 1000;
            long wakeP95Ms = wake.p95() / 1000;
            long wakeP99Ms = wake.p99() / 1000;
            long overheadP99 = e2eP99 - wakeP99Ms;

            // Output RESULT marker
            System.out.printf(
                "RESULT|delivery|tier=%s|count=%d|iterations=%d|consumers=%d"
                    + "|create_mean_ms=%.0f|create_stddev_ms=%.0f"
                    + "|delivery_mean_ms=%.0f|delivery_stddev_ms=%.0f"
                    + "|qps_mean=%.0f|qps_stddev=%.0f"
                    + "|wake_p50_ms=%d|wake_p95_ms=%d|wake_p99_ms=%d"
                    + "|e2e_p50_ms=%d|e2e_p95_ms=%d|e2e_p99_ms=%d"
                    + "|overhead_p99_ms=%d%n",
                tier, MEASURE_COUNT, ITERATIONS, consumers,
                meanCreateMs, stddevCreateMs,
                meanDeliveryMs, stddevDeliveryMs,
                meanQps, stddevDeliveryMs > 0 ? meanQps * stddevDeliveryMs / meanDeliveryMs : 0,
                wakeP50Ms, wakeP95Ms, wakeP99Ms,
                e2eP50, e2eP95, e2eP99,
                overheadP99);
            System.out.printf(
                "[Benchmark] %s: %dx%d intents, %d consumers, %.0f QPS, "
                    + "E2E p99=%dms, wakeup p99=%dms, DURABLE overhead~%dms%n",
                tier, MEASURE_COUNT, ITERATIONS, consumers,
                meanQps, e2eP99, wakeP99Ms, overheadP99);

            // Performance assertion
            assertTrue(overheadP99 < OVERHEAD_P99_THRESHOLD_MS,
                "DURABLE delivery overhead p99=" + overheadP99 + "ms exceeds threshold "
                    + OVERHEAD_P99_THRESHOLD_MS + "ms");
        }
    }

    private record IterationResult(long createMs, long deliveryMs) {}

    /**
     * Run one delivery iteration: batch-create N intents with executeAt = now + 2s,
     * wait for all deliveries via CountDownLatch, return timing.
     *
     * @param latencies if non-null, collect per-intent E2E latency (ms) here; null for warmup
     */
    private IterationResult runIteration(LoomqEngine engine, PrecisionTier tier,
                                          int count,
                                          ConcurrentLinkedQueue<Long> latencies) throws Exception {
        CountDownLatch latch = new CountDownLatch(count);
        IntentObserver observer = new IntentObserver() {
            @Override public void onScheduled(Intent i) {}
            @Override public void onDelivered(Intent i, DeliveryHandler.DeliveryResult r) {
                if (latencies != null) {
                    latencies.add(System.currentTimeMillis() - i.getExecuteAt().toEpochMilli());
                }
                latch.countDown();
            }
            @Override public void onDeadLettered(Intent i) {}
            @Override public void onExpired(Intent i) {}
            @Override public void onDeliveryFailed(Intent i, Throwable e) {}
        };
        engine.registerObserver(observer);

        try {
            Instant fireAt = Instant.now().plusSeconds(FIRE_DELAY_SEC);
            List<Intent> intents = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                Intent intent = new Intent();
                intent.setExecuteAt(fireAt);
                intent.setPrecisionTier(tier);
                intents.add(intent);
            }

            long createStartNs = System.nanoTime();
            engine.createIntents(intents, AckMode.DURABLE).get();
            long createMs = (System.nanoTime() - createStartNs) / 1_000_000;

            // Measure delivery from executeAt (when intents become due) to all ACKED
            long deliveryStartMs = fireAt.toEpochMilli();
            assertTrue(latch.await(60, TimeUnit.SECONDS),
                "Not all intents delivered within 60s for tier " + tier);
            long deliveryMs = System.currentTimeMillis() - deliveryStartMs;

            return new IterationResult(createMs, deliveryMs);
        } finally {
            engine.removeObserver(observer);
        }
    }

    private static double mean(long[] values) {
        double sum = 0;
        for (long v : values) sum += v;
        return sum / values.length;
    }

    private static double stddev(long[] values, double mean) {
        double sumSq = 0;
        for (long v : values) sumSq += (v - mean) * (v - mean);
        return Math.sqrt(sumSq / values.length);
    }
}
