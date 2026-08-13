package com.loomq.application.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.spi.DeliveryHandler;
import com.loomq.spi.DeliveryHandler.DeliveryResult;
import com.loomq.spi.RedeliveryDecider;
import com.loomq.store.ConcurrentIntentStore;
import com.loomq.store.IntentStore;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * R12: RedeliveryDecider SPI 曾为死代码——通过 ServiceLoader/Builder 暴露,但 shouldRedeliver()
 * 全库零调用,投递异常路径恒按 maxAttempts 重试,自定义 decider 静默失效。修复:异常路径
 * 构造 DeliveryContext 并调用 decider,判定不可重投(永久失败)时直接 DEAD_LETTERED,不再重试。
 */
class RedeliveryDeciderWiringTest {

    @Test
    void deciderReturningFalseMustDeadLetterWithoutRetry() throws Exception {
        IntentStore store = new ConcurrentIntentStore();
        AtomicInteger deliveries = new AtomicInteger();
        CountDownLatch firstDelivery = new CountDownLatch(1);
        DeliveryHandler handler = intent -> {
            deliveries.incrementAndGet();
            firstDelivery.countDown();
            CompletableFuture<DeliveryResult> future = new CompletableFuture<>();
            future.completeExceptionally(new RuntimeException("permanent business failure"));
            return future;
        };
        // 自定义 decider:恒不可重投 → 首次失败即终态,不得反复重试(即使 attempts < maxAttempts)。
        RedeliveryDecider never = ctx -> false;
        PrecisionScheduler scheduler = new PrecisionScheduler(store, handler, never);
        scheduler.start();
        try {
            Intent intent = new Intent("r12-decider-0001");
            intent.setExecuteAt(Instant.now().minusMillis(50));
            intent.transitionTo(IntentStatus.SCHEDULED);
            store.save(intent);
            scheduler.schedule(intent);

            assertTrue(firstDelivery.await(5, TimeUnit.SECONDS), "first delivery must happen");

            IntentStatus st = null;
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
            while (System.nanoTime() < deadline) {
                Intent cur = store.findById("r12-decider-0001");
                st = cur != null ? cur.getStatus() : null;
                if (st == IntentStatus.DEAD_LETTERED) break;
                Thread.sleep(10);
            }
            assertEquals(IntentStatus.DEAD_LETTERED, st,
                "decider=false must dead-letter the intent on first failure");
            // 等一个结算窗口,确认没有发生第 2 次投递(重试)
            Thread.sleep(300);
            assertEquals(1, deliveries.get(),
                "decider=false must not trigger any redelivery retry");
        } finally {
            scheduler.stop();
        }
    }

    @Test
    void deciderReturningTrueMustStillRetryThenDeadLetterAfterMaxAttempts() throws Exception {
        IntentStore store = new ConcurrentIntentStore();
        AtomicInteger deliveries = new AtomicInteger();
        DeliveryHandler handler = intent -> {
            deliveries.incrementAndGet();
            CompletableFuture<DeliveryResult> future = new CompletableFuture<>();
            future.completeExceptionally(new RuntimeException("transient failure"));
            return future;
        };
        RedeliveryDecider always = ctx -> true;
        PrecisionScheduler scheduler = new PrecisionScheduler(store, handler, always);
        scheduler.start();
        try {
            Intent intent = new Intent("r12-decider-0002");
            intent.setExecuteAt(Instant.now().minusMillis(50));
            intent.transitionTo(IntentStatus.SCHEDULED);
            // maxAttempts=2, fixed 1ms 无抖动 → 快速收敛到死信
            intent.setRedelivery(new com.loomq.domain.intent.RedeliveryPolicy(2, "fixed", 1, 1, 1.0, false));
            store.save(intent);
            scheduler.schedule(intent);

            IntentStatus st = null;
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (System.nanoTime() < deadline) {
                Intent cur = store.findById("r12-decider-0002");
                st = cur != null ? cur.getStatus() : null;
                if (st == IntentStatus.DEAD_LETTERED) break;
                if (deliveries.get() > 2) break;
                Thread.sleep(10);
            }
            assertEquals(IntentStatus.DEAD_LETTERED, st,
                "decider=true must still obey maxAttempts and dead-letter");
            assertEquals(2, deliveries.get(), "exactly maxAttempts deliveries");
        } finally {
            scheduler.stop();
        }
    }
}
