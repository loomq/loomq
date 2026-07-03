package com.loomq.application.command;

import static org.junit.jupiter.api.Assertions.*;

import com.loomq.application.scheduler.PrecisionScheduler;
import com.loomq.common.MetricsCollector;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.infrastructure.wheel.GroupCommitBarrier;
import com.loomq.infrastructure.wheel.IntentLocationIndex;
import com.loomq.infrastructure.wheel.PromotionDaemon;
import com.loomq.infrastructure.wheel.SlotLocation;
import com.loomq.infrastructure.wheel.TailIndex;
import com.loomq.infrastructure.wheel.WheelConfig;
import com.loomq.infrastructure.wheel.WheelStore;
import com.loomq.store.ConcurrentIntentStore;
import java.nio.file.Path;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CancelColdConcurrencyTest {
    @TempDir Path tmp;

    /**
     * H2:同一冷 Intent 并发取消必须串行化 —— 第二个取消者重读槽已是 CANCELED(terminal),
     * transitionTo 抛 ISE 返回 false,不 double-write、不 double 计数。
     */
    @Test
    void concurrentColdCancelDoesNotDoubleWrite() throws Exception {
        AtomicLong clock = new AtomicLong(Instant.parse("2026-06-30T00:00:00Z").toEpochMilli());
        WheelConfig cfg = new WheelConfig(tmp.toString(), "t", 30, 16, 1, 10_000L, PrecisionTier.STANDARD);
        try (WheelStore store = new WheelStore(cfg, clock::get);
             TailIndex tail = new TailIndex(tmp, clock::get);
             GroupCommitBarrier barrier = new GroupCommitBarrier(store, tail, 1, 10_000)) {

            IntentLocationIndex idx = new IntentLocationIndex();
            ConcurrentIntentStore memStore = new ConcurrentIntentStore();
            AtomicBoolean running = new AtomicBoolean(true);
            AtomicLong seq = new AtomicLong();
            PrecisionScheduler scheduler = new PrecisionScheduler(memStore, intent ->
                java.util.concurrent.CompletableFuture.completedFuture(
                    com.loomq.spi.DeliveryHandler.DeliveryResult.DEAD_LETTER), null);
            PromotionDaemon daemon = new PromotionDaemon(store, tail, idx, clock::get, i -> {});
            ExecutorService cb = Executors.newVirtualThreadPerTaskExecutor();

            IntentCommandService svc = new IntentCommandService(
                memStore, scheduler, store, tail, barrier, idx, daemon,
                MetricsCollector.getInstance(), cb, running, seq, null, PrecisionTier.STANDARD, 1L);
            barrier.start(); daemon.start(); scheduler.start();

            // 冷 Intent:executeAt 远超 60min → 落 wheel(非 tail),不在内存 store
            Intent cold = new Intent("intent_cld00000001");
            cold.setExecuteAt(Instant.ofEpochMilli(clock.get() + 2 * 60 * 60 * 1000L)); // +2h
            cold.setPrecisionTier(PrecisionTier.STANDARD);
            cold.transitionTo(IntentStatus.SCHEDULED);
            cold.incrementRevision();
            var loc = store.put(cold);
            idx.put(cold.getIntentId(), loc); // 模拟 createIntent 已注册索引(冷,不进 memStore)

            long beforeCancelled = MetricsCollector.getInstance().getIntentsCancelledTotal();

            int n = 8;
            CountDownLatch ready = new CountDownLatch(n);
            CountDownLatch fire = new CountDownLatch(1);
            AtomicInteger successes = new AtomicInteger();
            AtomicInteger failures = new AtomicInteger();
            try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
                for (int i = 0; i < n; i++) {
                    pool.submit(() -> {
                        ready.countDown();
                        try { fire.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); return; }
                        if (svc.cancelIntent(cold.getIntentId())) successes.incrementAndGet();
                        else failures.incrementAndGet();
                    });
                }
                ready.await();
                fire.countDown();
                pool.shutdown();
                assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
            }

            try {
                assertEquals(1, successes.get(), "exactly one concurrent cold cancel must succeed");
                assertEquals(n - 1, failures.get(), "others must see terminal state and return false");
                assertEquals(1L, MetricsCollector.getInstance().getIntentsCancelledTotal() - beforeCancelled,
                    "cancel counter must increment exactly once (no double-count)");
            } finally {
                // scheduler/daemon 不在 try-with-resources 中,需显式关闭;
                // barrier 由外层 try-with-resources 自动关闭,不再重复 close。
                scheduler.stop();
                daemon.close();
            }
        }
    }

    /**
     * H2 tail 路径:超出 day 视界(>30d)的冷 Intent 落 TailIndex,并发取消必须恰好一个成功。
     * TailIndex.remove 返回 false 表示已并发取消者移除 → 第二取消者须返回 false,不 double 计数。
     * (appendLock 仅串行单次 append,不串行 remove→awaitCommit→metric++ 序列,故须布尔门控。)
     */
    @Test
    void concurrentTailColdCancelDoesNotDoubleCount() throws Exception {
        AtomicLong clock = new AtomicLong(Instant.parse("2026-06-30T00:00:00Z").toEpochMilli());
        WheelConfig cfg = new WheelConfig(tmp.toString(), "t", 30, 16, 1, 10_000L, PrecisionTier.STANDARD);
        try (WheelStore store = new WheelStore(cfg, clock::get);
             TailIndex tail = new TailIndex(tmp, clock::get);
             GroupCommitBarrier barrier = new GroupCommitBarrier(store, tail, 1, 10_000)) {

            IntentLocationIndex idx = new IntentLocationIndex();
            ConcurrentIntentStore memStore = new ConcurrentIntentStore();
            AtomicBoolean running = new AtomicBoolean(true);
            AtomicLong seq = new AtomicLong();
            PrecisionScheduler scheduler = new PrecisionScheduler(memStore, intent ->
                java.util.concurrent.CompletableFuture.completedFuture(
                    com.loomq.spi.DeliveryHandler.DeliveryResult.DEAD_LETTER), null);
            PromotionDaemon daemon = new PromotionDaemon(store, tail, idx, clock::get, i -> {});
            ExecutorService cb = Executors.newVirtualThreadPerTaskExecutor();

            IntentCommandService svc = new IntentCommandService(
                memStore, scheduler, store, tail, barrier, idx, daemon,
                MetricsCollector.getInstance(), cb, running, seq, null, PrecisionTier.STANDARD, 1L);
            barrier.start(); daemon.start(); scheduler.start();

            // 远期冷 Intent:executeAt > 30d 视界 → 落 tail(非 wheel),不在内存 store。
            long execMs = clock.get() + 31L * 24 * 60 * 60 * 1000L; // +31 days
            Intent cold = new Intent("intent_ctl00000001");
            cold.setExecuteAt(Instant.ofEpochMilli(execMs));
            cold.setPrecisionTier(PrecisionTier.STANDARD);
            cold.transitionTo(IntentStatus.SCHEDULED);
            cold.incrementRevision();
            tail.put(cold);                                   // 落 tail run 文件
            idx.put(cold.getIntentId(), SlotLocation.tail(execMs)); // 模拟 createIntent 已注册索引

            long beforeCancelled = MetricsCollector.getInstance().getIntentsCancelledTotal();

            int n = 8;
            CountDownLatch ready = new CountDownLatch(n);
            CountDownLatch fire = new CountDownLatch(1);
            AtomicInteger successes = new AtomicInteger();
            AtomicInteger failures = new AtomicInteger();
            try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
                for (int i = 0; i < n; i++) {
                    pool.submit(() -> {
                        ready.countDown();
                        try { fire.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); return; }
                        if (svc.cancelIntent(cold.getIntentId())) successes.incrementAndGet();
                        else failures.incrementAndGet();
                    });
                }
                ready.await();
                fire.countDown();
                pool.shutdown();
                assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
            }

            try {
                assertEquals(1, successes.get(), "exactly one concurrent tail cold cancel must succeed");
                assertEquals(n - 1, failures.get(),
                    "others' tailIndex.remove must return false (already cancelled) → return false");
                assertEquals(1L, MetricsCollector.getInstance().getIntentsCancelledTotal() - beforeCancelled,
                    "cancel counter must increment exactly once on tail path (no double-count)");
            } finally {
                scheduler.stop();
                daemon.close();
            }
        }
    }
}
