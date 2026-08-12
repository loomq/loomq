package com.loomq.application.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.domain.intent.ExpiredAction;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.spi.DeliveryHandler;
import com.loomq.spi.DeliveryHandler.DeliveryResult;
import com.loomq.store.ConcurrentIntentStore;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * 过期闸门回归：deadline 已过的 Intent 不得被投递。
 *
 * <p>scanAndDispatch 先 CAS 认领 + 入队，之后才跑 checkExpiredIntents（同 cycle 竞态，入队
 * 恒胜）——过期检查的时机落后于入队。投递前的最后一道闸是消费者出队路径：出队时在
 * synchronized(intent) 内复查 deadline，过期则按 ExpiredAction 终态化、不投递。</p>
 */
class BugExpiredDispatchTest {

    private PrecisionScheduler scheduler;
    private ConcurrentIntentStore intentStore;

    @AfterEach
    void tearDown() {
        if (scheduler != null) scheduler.stop();
    }

    /** 单发消费者路径（ULTRA，batchSize=1）。 */
    @Test
    void expiredIntentIsNeverDeliveredSingleConsumer() throws Exception {
        AtomicInteger deliveries = new AtomicInteger();
        DeliveryHandler handler = intent -> {
            deliveries.incrementAndGet();
            return CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        };
        intentStore = new ConcurrentIntentStore();
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        scheduler.start();

        Intent intent = new Intent("expired-single");
        intent.setExecuteAt(Instant.now().minusSeconds(60)); // 已到期（会入队）
        intent.setDeadline(Instant.now().minusSeconds(1));   // 已过期（不得投递）
        intent.setPrecisionTier(PrecisionTier.ULTRA);
        intent.transitionTo(IntentStatus.SCHEDULED);
        intentStore.save(intent);
        scheduler.schedule(intent);

        assertTrue(waitForTerminal("expired-single", 3), "expired intent must reach a terminal state");
        assertEquals(IntentStatus.EXPIRED, intentStore.findByIdInternal("expired-single").getStatus());
        assertEquals(0, deliveries.get(), "expired intent must never be delivered");
    }

    /** DEAD_LETTER 动作：过期 → DEAD_LETTERED，仍不得投递。 */
    @Test
    void expiredIntentWithDeadLetterActionIsNeverDelivered() throws Exception {
        AtomicInteger deliveries = new AtomicInteger();
        DeliveryHandler handler = intent -> {
            deliveries.incrementAndGet();
            return CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        };
        intentStore = new ConcurrentIntentStore();
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        scheduler.start();

        Intent intent = new Intent("expired-deadletter");
        intent.setExecuteAt(Instant.now().minusSeconds(60));
        intent.setDeadline(Instant.now().minusSeconds(1));
        intent.setExpiredAction(ExpiredAction.DEAD_LETTER);
        intent.setPrecisionTier(PrecisionTier.ULTRA);
        intent.transitionTo(IntentStatus.SCHEDULED);
        intentStore.save(intent);
        scheduler.schedule(intent);

        assertTrue(waitForTerminal("expired-deadletter", 3), "expired intent must reach a terminal state");
        assertEquals(IntentStatus.DEAD_LETTERED, intentStore.findByIdInternal("expired-deadletter").getStatus());
        assertEquals(0, deliveries.get(), "expired intent must never be delivered");
    }

    /** 批量消费者路径（STANDARD，batchSize=20）。 */
    @Test
    void expiredIntentIsNeverDeliveredBatchConsumer() throws Exception {
        AtomicInteger deliveries = new AtomicInteger();
        DeliveryHandler handler = intent -> {
            deliveries.incrementAndGet();
            return CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        };
        intentStore = new ConcurrentIntentStore();
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        scheduler.start();

        Intent intent = new Intent("expired-batch");
        intent.setExecuteAt(Instant.now().minusSeconds(60));
        intent.setDeadline(Instant.now().minusSeconds(1));
        intent.setPrecisionTier(PrecisionTier.STANDARD);
        intent.transitionTo(IntentStatus.SCHEDULED);
        intentStore.save(intent);
        scheduler.schedule(intent);

        assertTrue(waitForTerminal("expired-batch", 3), "expired intent must reach a terminal state");
        assertEquals(IntentStatus.EXPIRED, intentStore.findByIdInternal("expired-batch").getStatus());
        assertEquals(0, deliveries.get(), "expired intent must never be delivered");
    }

    /** 未过期且到期的 intent 正常投递（闸门不误伤）。 */
    @Test
    void nonExpiredDueIntentIsStillDelivered() throws Exception {
        AtomicInteger deliveries = new AtomicInteger();
        DeliveryHandler handler = intent -> {
            deliveries.incrementAndGet();
            return CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        };
        intentStore = new ConcurrentIntentStore();
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        scheduler.start();

        Intent intent = new Intent("not-expired");
        intent.setExecuteAt(Instant.now().minusMillis(50)); // 已到期
        intent.setDeadline(Instant.now().plusSeconds(600)); // 未过期
        intent.setPrecisionTier(PrecisionTier.ULTRA);
        intent.transitionTo(IntentStatus.SCHEDULED);
        intentStore.save(intent);
        scheduler.schedule(intent);

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (deliveries.get() == 0 && System.nanoTime() < deadline) {
            Thread.sleep(20);
        }
        assertEquals(1, deliveries.get(), "non-expired due intent must be delivered");
        Intent stored = intentStore.findByIdInternal("not-expired");
        assertTrue(stored.getStatus().isTerminal(), "delivered intent must reach a terminal state");
    }

    private boolean waitForTerminal(String intentId, int timeoutSeconds) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
        while (System.nanoTime() < deadline) {
            Intent stored = intentStore.findByIdInternal(intentId);
            if (stored != null && stored.getStatus().isTerminal()) {
                return true;
            }
            Thread.sleep(20);
        }
        return false;
    }
}
