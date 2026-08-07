package com.loomq.benchmark;

import com.loomq.LoomqEngine;
import com.loomq.common.MetricsCollector;
import com.loomq.domain.intent.*;
import com.loomq.spi.DeliveryHandler;
import com.loomq.spi.IntentObserver;
import java.nio.file.Path;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * 投递路径 DURABLE 稳态吞吐基准：闭环稳态 + 纳秒精度 + 中位数/IQR。
 * 支持 -Dsweep.consumers=4,8,16,32 扫参（对每个值各跑一遍并输出 RESULT|delivery|...|consumers=N 行）。
 */
@Tag("benchmark")
class DeliveryPathBenchmark {

    private static final DeliveryHandler SUCCESS = intent ->
        CompletableFuture.completedFuture(DeliveryHandler.DeliveryResult.SUCCESS);

    @Test void measureDeliveryThroughput_Ultra(@TempDir Path tmp) throws Exception { measureDelivery(tmp, PrecisionTier.ULTRA); }
    @Test void measureDeliveryThroughput_Fast(@TempDir Path tmp) throws Exception { measureDelivery(tmp, PrecisionTier.FAST); }
    @Test void measureDeliveryThroughput_Milli(@TempDir Path tmp) throws Exception { measureDelivery(tmp, PrecisionTier.MILLI); }
    @Test void measureDeliveryThroughput_Standard(@TempDir Path tmp) throws Exception { measureDelivery(tmp, PrecisionTier.STANDARD); }

    private void measureDelivery(Path tmp, PrecisionTier tier) throws Exception {
        var base = PrecisionTierCatalog.defaultCatalog();
        String sweepProp = System.getProperty("sweep.consumers");
        if (sweepProp != null) {
            for (var catalog : SweepDriver.catalogs(base, tier, SweepParam.CONSUMERS,
                    SweepDriver.parseValues(sweepProp))) {
                runOne(tmp, tier, catalog);
            }
        } else {
            runOne(tmp, tier, base);
        }
    }

    private void runOne(Path tmp, PrecisionTier tier, PrecisionTierCatalog catalog) throws Exception {
        var profile = catalog.profile(tier);
        BenchmarkConfig cfg = BenchmarkConfig.forTier(profile.maxConcurrency());
        try (LoomqEngine engine = LoomqEngine.builder()
                .dataDir(tmp).nodeId("bench-" + tier.name())
                .catalog(catalog).deliveryHandler(SUCCESS).build()) {
            engine.start();

            ConcurrentLinkedQueue<Long> pendingExecuteAtNs = new ConcurrentLinkedQueue<>();
            SteadyStateHarness harness = new SteadyStateHarness(cfg.inFlight(), () -> {
                long submitNs = System.nanoTime();
                long delayMs = 1 + ThreadLocalRandom.current().nextInt((int) cfg.delayRangeMs());
                pendingExecuteAtNs.add(submitNs + TimeUnit.MILLISECONDS.toNanos(delayMs));
                Intent intent = new Intent();
                intent.setExecuteAt(Instant.now().plusMillis(delayMs));
                intent.setPrecisionTier(tier);
                engine.createIntent(intent, AckMode.DURABLE);
            });
            engine.registerObserver(new IntentObserver() {
                @Override public void onDelivered(Intent i, DeliveryHandler.DeliveryResult r) {
                    Long exeNs = pendingExecuteAtNs.poll();
                    harness.onComplete(exeNs == null ? 0 : (System.nanoTime() - exeNs) / 1000);
                }
                @Override public void onScheduled(Intent i) {}
                @Override public void onDeadLettered(Intent i) {}
                @Override public void onExpired(Intent i) {}
                @Override public void onDeliveryFailed(Intent i, Throwable e) {}
            });

            SteadyStateHarness.Result result = harness.run(cfg.warmupMs(), cfg.measureWindowMs(), cfg.subWindowMs());

            MetricsCollector.LatencySnapshot wake = engine.getMetricsCollector().getWakeupLatencySnapshot(tier);
            long wakeP50 = wake.p50() / 1000;
            long wakeP99 = wake.p99() / 1000;
            long overheadP99 = result.e2eP99Us() / 1000 - wakeP99;

            String tag = sweepPropSuffix(tier, catalog);
            System.out.printf("RESULT|delivery|tier=%s%s|qps_median=%.0f|qps_iqr=%.0f|samples=%d"
                    + "|e2e_p50_ms=%d|e2e_p99_ms=%d|e2e_p999_ms=%d|wake_p50_ms=%d|wake_p99_ms=%d|overhead_p99_ms=%d%n",
                tier, tag, result.qpsMedian(), result.qpsIqr(), result.qpsSamples(),
                result.e2eP50Us() / 1000, result.e2eP99Us() / 1000, result.e2eP999Us() / 1000,
                wakeP50, wakeP99, overheadP99);
        }
    }

    private String sweepPropSuffix(PrecisionTier tier, PrecisionTierCatalog catalog) {
        if (System.getProperty("sweep.consumers") == null) return "";
        return "|consumers=" + catalog.profile(tier).consumerCount();
    }
}