package com.loomq.application.scheduler;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.common.MetricsCollector;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.domain.intent.PrecisionTierCatalog;
import com.loomq.domain.intent.PrecisionTierProfile;
import com.loomq.domain.intent.WalMode;
import com.loomq.spi.DeliveryHandler;
import com.loomq.spi.DeliveryHandler.DeliveryResult;
import com.loomq.store.ConcurrentIntentStore;
import com.loomq.tracing.IntentTraceStore;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * 背压忙转回归：dispatch 队列满时 scanAndDispatch 把到期 intent 重入桶，adaptive 扫描线程
 * 若立即重扫会 100% CPU 空转（单平台线程，饿死其他档/cohort）。
 *
 * <p>修复：最早桶仍含到期 intent 时做有界 park（scanIntervalMs）后再重试。断言固定窗口内
 * 的扫描次数有上界——修复前每 cycle 仅 2 次 1ms offer 重试 park（≈3ms/cycle），修复后
 * 每 cycle ≥ scanIntervalMs=10ms park（≈12.5ms/cycle），2.5s 窗口内 300 次上界可区分。</p>
 */
@Tag("slow")
class AdaptiveScanBackpressureTest {

    private PrecisionScheduler scheduler;
    private ConcurrentIntentStore intentStore;

    @AfterEach
    void tearDown() {
        // 先完成所有被阻塞的投递，使 stop() 的 in-flight 排空快速收敛
        done.set(true); // 之后的投递直接返回已完成 future
        pending.forEach(f -> f.complete(DeliveryResult.SUCCESS));
        if (scheduler != null) scheduler.stop();
    }

    /** 永不完成（占住 permit）的投递 future，tearDown 时统一完成。 */
    private final java.util.List<CompletableFuture<DeliveryResult>> pending =
        new java.util.concurrent.CopyOnWriteArrayList<>();
    private final java.util.concurrent.atomic.AtomicBoolean done = new java.util.concurrent.atomic.AtomicBoolean(false);

    @Test
    void adaptiveScanIsBoundedUnderBackpressure() throws Exception {
        MetricsCollector metrics = new MetricsCollector();

        // ULTRA adaptive：maxConcurrency=1（首个投递占住唯一 permit）、queue=1（队列满）
        // → 第 4 个 intent 每次扫描 offer 失败 → 背压重入桶 → 触发忙转路径。
        PrecisionTierProfile ultra = new PrecisionTierProfile(
            10, 1, 1, 1, 1, 1, WalMode.DURABLE, 10, false, true, 1000);
        PrecisionTierCatalog catalog = PrecisionTierCatalog.of(
            Map.of(PrecisionTier.ULTRA, ultra), PrecisionTier.ULTRA);

        // 永不完成的 handler：占住 permit，制造持续背压
        DeliveryHandler handler = intent -> {
            if (done.get()) {
                return CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
            }
            CompletableFuture<DeliveryResult> f = new CompletableFuture<>();
            pending.add(f);
            return f;
        };
        intentStore = new ConcurrentIntentStore();
        scheduler = new PrecisionScheduler(intentStore, handler, null, catalog, metrics, new IntentTraceStore());
        scheduler.start();

        // 4 个到期 intent：1 在途阻塞 + 1 卡消费者 acquire + 1 占队列 + 1 触发重入忙转
        for (int i = 0; i < 4; i++) {
            Intent it = new Intent("busy-" + i);
            it.setExecuteAt(Instant.now().minusMillis(200));
            it.setPrecisionTier(PrecisionTier.ULTRA);
            it.transitionTo(IntentStatus.SCHEDULED);
            intentStore.save(it);
            scheduler.schedule(it);
        }

        // 等背压稳定（队列满、重入循环建立）后开始计数
        Thread.sleep(500);
        long start = metrics.getScanSampleCountByTier(PrecisionTier.ULTRA);
        Thread.sleep(2500);
        long scans = metrics.getScanSampleCountByTier(PrecisionTier.ULTRA) - start;

        // 修复后每 cycle ≥ 10ms park → 2.5s 内 ≤ ~250 次；修复前无 park，cycle ≈ 3ms → ~800 次。
        assertTrue(scans < 300,
            "adaptive scan must be bounded under backpressure (parked retry), got " + scans + " scans in 2.5s");
    }
}
