package com.loomq.application.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.domain.intent.PrecisionTierCatalog;
import com.loomq.domain.intent.PrecisionTierProfile;
import com.loomq.domain.intent.WalMode;
import com.loomq.spi.DeliveryHandler;
import com.loomq.spi.DeliveryHandler.DeliveryResult;
import com.loomq.store.ConcurrentIntentStore;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * 高水位降级回归：BucketGroup.add() 返回 FALLBACK_TO_COHORT 时结果不得丢弃。
 *
 * <p>maxBuckets 触顶时 add() 会摘除旧桶条目并清空索引（intent 移交 cohort 管）；若调用方
 * 丢弃该结果，intent 既不在桶也不在 cohort——静默丢失，直到重启恢复才被重新调度。
 * 修复前：schedule() 的 delayMs<=0 路径与 restore() 的短延迟路径丢弃结果；背压重入路径
 * 走强制入桶（addForced）不受高水位影响。</p>
 */
class BucketHighWaterFallbackTest {

    private PrecisionScheduler scheduler;
    private ConcurrentIntentStore intentStore;

    @AfterEach
    void tearDown() {
        if (scheduler != null) scheduler.stop();
    }

    private static PrecisionTierCatalog catalogWithMaxBuckets(int maxBuckets) {
        PrecisionTierProfile ultra = new PrecisionTierProfile(
            10, 10, 1, 1, 1, 160, WalMode.DURABLE, 10, false, false, maxBuckets);
        return PrecisionTierCatalog.of(Map.of(PrecisionTier.ULTRA, ultra), PrecisionTier.ULTRA);
    }

    @Test
    void scheduleFallbackToCohortMustNotDropIntent() throws Exception {
        AtomicInteger deliveries = new AtomicInteger();
        DeliveryHandler handler = intent -> {
            deliveries.incrementAndGet();
            return CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        };
        intentStore = new ConcurrentIntentStore();
        scheduler = new PrecisionScheduler(intentStore, handler, null, catalogWithMaxBuckets(1));
        scheduler.start();
        scheduler.pause(); // 暂停扫描，确保两个 add 都在首个桶被扫描摘除前完成（确定性触发高水位）

        long now = System.currentTimeMillis();
        // 两个到期 Intent，落在不同 10ms 窗口桶：第二个 add 时 maxBuckets=1 已触顶 → FALLBACK
        Intent a = new Intent("fallback-a");
        a.setExecuteAt(Instant.ofEpochMilli(now - 100));
        a.setPrecisionTier(PrecisionTier.ULTRA);
        a.transitionTo(IntentStatus.SCHEDULED);
        intentStore.save(a);
        scheduler.schedule(a);

        Intent b = new Intent("fallback-b");
        b.setExecuteAt(Instant.ofEpochMilli(now - 50));
        b.setPrecisionTier(PrecisionTier.ULTRA);
        b.transitionTo(IntentStatus.SCHEDULED);
        intentStore.save(b);
        scheduler.schedule(b);

        scheduler.resume();

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (deliveries.get() < 2 && System.nanoTime() < deadline) {
            Thread.sleep(20);
        }
        assertEquals(2, deliveries.get(),
            "FALLBACK_TO_COHORT must not drop the intent — both intents must be delivered");
    }

    /** restore() 短延迟路径同样不得丢弃 FALLBACK_TO_COHORT 结果。 */
    @Test
    void restoreFallbackToCohortMustNotDropIntent() throws Exception {
        AtomicInteger deliveries = new AtomicInteger();
        DeliveryHandler handler = intent -> {
            deliveries.incrementAndGet();
            return CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        };
        intentStore = new ConcurrentIntentStore();
        scheduler = new PrecisionScheduler(intentStore, handler, null, catalogWithMaxBuckets(1));
        scheduler.start();
        scheduler.pause();

        long now = System.currentTimeMillis();
        Intent a = new Intent("restore-fallback-a");
        a.setExecuteAt(Instant.ofEpochMilli(now - 100));
        a.setPrecisionTier(PrecisionTier.ULTRA);
        a.transitionTo(IntentStatus.SCHEDULED);
        intentStore.save(a);
        scheduler.schedule(a);

        Intent b = new Intent("restore-fallback-b");
        b.setExecuteAt(Instant.ofEpochMilli(now - 50));
        b.setPrecisionTier(PrecisionTier.ULTRA);
        b.transitionTo(IntentStatus.SCHEDULED);
        intentStore.save(b);
        scheduler.restore(b);

        scheduler.resume();

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (deliveries.get() < 2 && System.nanoTime() < deadline) {
            Thread.sleep(20);
        }
        assertEquals(2, deliveries.get(),
            "restore() fallback must not drop the intent — both intents must be delivered");
    }
}
