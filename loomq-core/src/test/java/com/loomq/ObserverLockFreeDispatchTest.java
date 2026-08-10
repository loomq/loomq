package com.loomq;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.loomq.domain.intent.AckMode;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.spi.DeliveryHandler;
import com.loomq.spi.DeliveryHandler.DeliveryResult;
import com.loomq.spi.IntentObserver;
import java.nio.file.Path;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * I5 锁外派发固化测试：观察器 dispatch 不持有 intent 锁。
 *
 * <p>设计：注册一个在 onScheduled 中永久阻塞的观察器。用 RETRY handler 触发
 * finalizeIntent RETRY -> schedule() -> onScheduled（第二次，重投递路径）。
 * 当 onScheduled 阻塞时调用 cancelIntent：若 dispatch 在锁外（I5 正确），
 * cancelIntent 立即返回；若 dispatch 在锁内（旧代码），cancelIntent 阻塞超时。</p>
 */
@Tag("integration")
class ObserverLockFreeDispatchTest {

    @Test
    void observerDispatchDoesNotHoldIntentLock(@TempDir Path tmp) throws Exception {
        CountDownLatch onScheduledEntered = new CountDownLatch(1);
        CountDownLatch blockForever = new CountDownLatch(1); // never counted down
        CountDownLatch firstDelivery = new CountDownLatch(1);
        AtomicInteger onScheduledCount = new AtomicInteger();

        // Observer: block only on the second onScheduled (RETRY reschedule)
        IntentObserver blockingObserver = new IntentObserver() {
            @Override
            public void onScheduled(Intent intent) {
                if (onScheduledCount.incrementAndGet() == 2) {
                    onScheduledEntered.countDown();
                    try {
                        blockForever.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }

            @Override
            public void onDelivered(Intent i, DeliveryResult r) {}

            @Override
            public void onDeadLettered(Intent i) {}

            @Override
            public void onExpired(Intent i) {}

            @Override
            public void onDeliveryFailed(Intent i, Throwable e) {}
        };

        // Handler: first delivery returns RETRY -> triggers reschedule -> second onScheduled
        DeliveryHandler retryHandler = intent -> {
            firstDelivery.countDown();
            return CompletableFuture.completedFuture(DeliveryResult.RETRY);
        };

        try (LoomqEngine engine = LoomqEngine.builder()
                .dataDir(tmp).nodeId("lock-free").deliveryHandler(retryHandler).build()) {
            engine.registerObserver(blockingObserver);
            engine.start();

            Intent intent = new Intent();
            String id = intent.getIntentId();
            intent.setExecuteAt(Instant.now().plusMillis(100));
            intent.setPrecisionTier(PrecisionTier.STANDARD);

            engine.createIntent(intent, AckMode.DURABLE).get();

            // Wait for first delivery (returns RETRY)
            assertTrue(firstDelivery.await(10, TimeUnit.SECONDS),
                "first delivery must happen");

            // Wait for second onScheduled (from RETRY reschedule) to be entered
            assertTrue(onScheduledEntered.await(10, TimeUnit.SECONDS),
                "onScheduled must be entered after RETRY reschedule");

            // While onScheduled is blocked, cancel the intent.
            // With lock-free dispatch: cancelIntent acquires the lock immediately.
            // With in-lock dispatch (old code): cancelIntent blocks until observer returns.
            CompletableFuture<Boolean> cancelFuture =
                CompletableFuture.supplyAsync(() -> engine.cancelIntent(id));
            try {
                Boolean cancelled = cancelFuture.get(3, TimeUnit.SECONDS);
                assertTrue(cancelled, "cancelIntent must succeed while observer is blocked");
            } catch (java.util.concurrent.TimeoutException e) {
                fail("cancelIntent timed out -- observer dispatch is holding the intent lock");
            } finally {
                blockForever.countDown(); // release the observer so engine can shut down
            }
        }
        System.gc();
        Thread.sleep(200);
    }
}
