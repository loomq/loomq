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
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * 投递路径 DURABLE 基准测试：测量 schedule -> deliver -> ACKED 全周期的吞吐与延迟。
 *
 * <p>每个档位执行 1 次预热（200 intents）+ 5 次测量（500 intents/次）。
 * 输出 RESULT|delivery|... 标记，兼容 benchmark 脚本解析。
 * 性能断言：DURABLE delivery overhead p99 < 档位精度窗口 + 100ms（按档位校准；
 * 开销受 wake 粒度≈窗口/2 约束，粗档天然更大，统一阈值会误报 STANDARD 等粗档）。
 *
 * <p>关键指标 <b>Delivery overhead</b> = E2E p99 - wakeup p99，隔离了 finalizeIntent 中
 * awaitCommit 的代价（I2 修复引入的开销）。
 *
 * <p>注意：{@link MetricsCollector} 的 wakeup 延迟以微秒记录（尽管参数名 latencyMs），
 * 输出时 / 1000 转毫秒。
 *
 * <p><b>测量有效性（2026-08-05 迭代）</b>：
 * (1) E2E 与 drain 计时改用 nanoTime 相对计时——原 currentTimeMillis 在 Windows 有
 * ~15.6ms 量化误差，MILLI e2e SLO(5/20ms) 的测量完全无效；
 * (2) fire delay 由固定 5s 改为 4.7-5.3s 随机——5000ms 与所有 fixed-rate 档的
 * scanInterval 整除相位锁定，wake 列退化为单相位样本而非分布（p50=p95=p99 常数化）。
 */
@Tag("benchmark")
class DeliveryPathBenchmark {

    private static final DeliveryHandler SUCCESS = intent ->
        CompletableFuture.completedFuture(DeliveryHandler.DeliveryResult.SUCCESS);

    private static final int WARMUP_COUNT = 200;
    private static final int MEASURE_COUNT = 500;
    private static final int ITERATIONS = 5;
    // fire delay = 4700 + [0,600)ms 随机：破坏与 fixed-rate scanInterval 的相位锁定
    private static final long FIRE_DELAY_BASE_MS = 4_700L;
    private static final int FIRE_DELAY_JITTER_MS = 600;
    private static final long COOLDOWN_MS = 1000;  // pause between iterations
    // 无统一阈值常量：overhead 阈值按档位精度窗口计算（见 measureDelivery），
    // 因开销受 wake 粒度（≈窗口/2）约束，粗档天然更大。

    @Test
    void measureDeliveryThroughput_Ultra(@TempDir Path tmp) throws Exception {
        measureDelivery(tmp, PrecisionTier.ULTRA);
    }

    @Test
    void measureDeliveryThroughput_Fast(@TempDir Path tmp) throws Exception {
        measureDelivery(tmp, PrecisionTier.FAST);
    }

    @Test
    void measureDeliveryThroughput_Milli(@TempDir Path tmp) throws Exception {
        measureDelivery(tmp, PrecisionTier.MILLI);
    }

    @Test
    void measureDeliveryThroughput_Standard(@TempDir Path tmp) throws Exception {
        measureDelivery(tmp, PrecisionTier.STANDARD);
    }

    private void measureDelivery(Path tmp, PrecisionTier tier) throws Exception {
        var profile = PrecisionTierCatalog.defaultCatalog().profile(tier);
        int consumers = profile.consumerCount();
        // 开销阈值按档位精度窗口设置：开销受 wake 粒度（≈窗口/2）约束，粗档天然更大。
        // STANDARD(500ms) 开销≈250ms，远高于 ULTRA(10ms) 的≈4ms；统一 500ms 会误报粗档。
        long overheadThresholdMs = profile.precisionWindowMs() + 100;

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

            // 档位固定成本盘点（精简决策的收益侧）：扫描模式/消费者/队列/并发/窗口
            System.out.printf(
                "RESULT|tier_config|tier=%s|scanner=%s|consumers=%d|queue_capacity=%d"
                    + "|max_concurrency=%d|window_ms=%d|batch_size=%d%n",
                tier, profile.adaptiveScan() ? "adaptive" : "fixed-rate",
                consumers, profile.dispatchQueueCapacity(), profile.maxConcurrency(),
                profile.precisionWindowMs(), profile.batchSize());

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
            assertTrue(overheadP99 < overheadThresholdMs,
                "DURABLE delivery overhead p99=" + overheadP99 + "ms exceeds threshold "
                    + overheadThresholdMs + "ms");
        }
    }

    private record IterationResult(long createMs, long deliveryMs) {}

    /**
     * Run one delivery iteration: batch-create N intents with executeAt = now + jittered
     * delay, wait for all deliveries via CountDownLatch, return timing.
     *
     * <p>E2E 与 drain 均用 nanoTime 相对计时（免疫 Windows currentTimeMillis 量化）。
     *
     * @param latencies if non-null, collect per-intent E2E latency (ms) here; null for warmup
     */
    private IterationResult runIteration(LoomqEngine engine, PrecisionTier tier,
                                          int count,
                                          ConcurrentLinkedQueue<Long> latencies) throws Exception {
        CountDownLatch latch = new CountDownLatch(count);
        long fireDelayMs = FIRE_DELAY_BASE_MS + ThreadLocalRandom.current().nextInt(FIRE_DELAY_JITTER_MS);
        long fireAtNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(fireDelayMs);
        IntentObserver observer = new IntentObserver() {
            @Override public void onScheduled(Intent i) {}
            @Override public void onDelivered(Intent i, DeliveryHandler.DeliveryResult r) {
                if (latencies != null) {
                    latencies.add(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - fireAtNanos));
                }
                latch.countDown();
            }
            @Override public void onDeadLettered(Intent i) {}
            @Override public void onExpired(Intent i) {}
            @Override public void onDeliveryFailed(Intent i, Throwable e) {}
        };
        engine.registerObserver(observer);

        try {
            Instant fireAt = Instant.now().plusMillis(fireDelayMs);
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
            assertTrue(latch.await(60, TimeUnit.SECONDS),
                "Not all intents delivered within 60s for tier " + tier);
            long deliveryMs = (System.nanoTime() - fireAtNanos) / 1_000_000;

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
