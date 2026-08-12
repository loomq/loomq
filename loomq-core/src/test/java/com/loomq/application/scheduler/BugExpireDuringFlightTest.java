package com.loomq.application.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.spi.DeliveryHandler;
import com.loomq.spi.DeliveryHandler.DeliveryResult;
import com.loomq.spi.IntentObserver;
import com.loomq.store.ConcurrentIntentStore;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * 在途过期竞态回归：deadline 在投递飞行中越过时，不得让 finalizeIntent 崩溃。
 *
 * <p>时序：consumer 出队闸门放行（未过期）→ deliverAsync 在途 → scan 线程
 * checkExpiredIntents 见 deadline 已过 → handleExpired 标 EXPIRED（终态）→
 * 投递完成回调 finalizeIntent 的 transitionTo(DUE) 从 EXPIRED 抛 ISE（修复前）。
 * 崩溃后果：onDelivered 丢失、ACK 终态未落盘 → 重启按旧 SCHEDULED 槽重复投递。</p>
 *
 * <p>修复：finalizeIntent / handleDeliveryFailure 入口加终态守卫——终态胜者在途投递
 * 结果，直接跳过结算（观察器已由 handleExpired 的 onExpired 通知）。cancel 竞态
 * （在途投递期间取消）同路径受益。</p>
 */
class BugExpireDuringFlightTest {

    private PrecisionScheduler scheduler;
    private ConcurrentIntentStore intentStore;

    @AfterEach
    void tearDown() {
        if (scheduler != null) scheduler.stop();
    }

    @Test
    void expiryDuringFlightMustNotCrashFinalize() throws Exception {
        AtomicInteger deliveries = new AtomicInteger();
        AtomicInteger onDelivered = new AtomicInteger();
        AtomicInteger onExpired = new AtomicInteger();
        AtomicReference<CompletableFuture<DeliveryResult>> pending = new AtomicReference<>();
        CountDownLatch inFlight = new CountDownLatch(1);

        DeliveryHandler handler = intent -> {
            deliveries.incrementAndGet();
            inFlight.countDown();
            CompletableFuture<DeliveryResult> f = new CompletableFuture<>();
            pending.set(f);
            return f;
        };
        intentStore = new ConcurrentIntentStore();
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        scheduler.addObserver(new IntentObserver() {
            @Override public void onDelivered(Intent i, DeliveryResult r) { onDelivered.incrementAndGet(); }
            @Override public void onScheduled(Intent i) {}
            @Override public void onDeadLettered(Intent i) {}
            @Override public void onExpired(Intent i) { onExpired.incrementAndGet(); }
            @Override public void onDeliveryFailed(Intent i, Throwable e) {}
        });
        scheduler.start();

        long now = System.currentTimeMillis();
        Intent intent = new Intent("expire-inflight");
        intent.setExecuteAt(Instant.ofEpochMilli(now - 100));  // 已到期 → 立即入队
        intent.setDeadline(Instant.ofEpochMilli(now + 500));   // 出队闸门放行,但在途期间过期
        intent.setPrecisionTier(PrecisionTier.ULTRA);
        intent.transitionTo(IntentStatus.SCHEDULED);
        intentStore.save(intent);
        scheduler.schedule(intent);

        assertTrue(inFlight.await(3, TimeUnit.SECONDS), "delivery must start");
        // 等 scan 线程的 checkExpiredIntents 观察到过期（ULTRA adaptive park 上限
        // scanIntervalMs×100 = 1s；deadline=+500ms，检测窗口 1s 覆盖）
        long expiryDeadline = System.currentTimeMillis() + 2000;
        while (intent.getStatus() != IntentStatus.EXPIRED && System.currentTimeMillis() < expiryDeadline) {
            Thread.sleep(20);
        }
        assertEquals(IntentStatus.EXPIRED, intent.getStatus(), "in-flight intent must be expired by scan thread");
        // 在途投递完成（SUCCESS）→ finalizeIntent 结算
        pending.get().complete(DeliveryResult.SUCCESS);
        Thread.sleep(500);   // 等 finalize 结算

        assertEquals(0, scheduler.getFinalizeTaskExceptions(), "finalize 不得因终态转换抛异常");
        assertEquals(IntentStatus.EXPIRED, intent.getStatus(), "终态胜者在途投递结果");
        assertEquals(1, deliveries.get(), "投递恰好发生一次（在途那次）");
        assertEquals(0, onDelivered.get(), "已过期 intent 不得再通知 onDelivered");
        assertEquals(1, onExpired.get(), "过期通知恰一次");
    }
}
