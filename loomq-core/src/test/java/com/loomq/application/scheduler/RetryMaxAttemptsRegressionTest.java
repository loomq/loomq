package com.loomq.application.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.RedeliveryPolicy;
import com.loomq.spi.DeliveryHandler;
import com.loomq.spi.DeliveryHandler.DeliveryResult;
import com.loomq.store.ConcurrentIntentStore;
import com.loomq.store.IntentStore;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import org.junit.jupiter.api.Test;

/**
 * DeliveryResult.RETRY 结果路径必须与异常路径（handleDeliveryFailure）一致,
 * 受 maxAttempts 约束——否则 handler 恒返回 RETRY 时无限重排程,绕过重试上限契约
 * （异常路径 attempts >= maxAttempts 即 DEAD_LETTERED,结果路径无此检查）。
 */
class RetryMaxAttemptsRegressionTest {

    @Test
    void retryResultMustDeadLetterAfterMaxAttempts() throws Exception {
        IntentStore store = new ConcurrentIntentStore();
        AtomicInteger deliveries = new AtomicInteger();
        DeliveryHandler handler = intent -> {
            deliveries.incrementAndGet();
            return CompletableFuture.completedFuture(DeliveryResult.RETRY);
        };
        PrecisionScheduler scheduler = new PrecisionScheduler(store, handler, null);
        scheduler.start();
        try {
            Intent intent = new Intent("test-retry-max");
            intent.setExecuteAt(Instant.now().minusMillis(100));
            intent.transitionTo(IntentStatus.SCHEDULED);
            // maxAttempts=3, fixed 1ms 延迟、无抖动 → 快速重试
            intent.setRedelivery(new RedeliveryPolicy(3, "fixed", 1, 1, 1.0, false));
            store.save(intent);
            scheduler.schedule(intent);

            // 轮询终态;修复前重试无上限 → deliveries 快速突破 3,提前失败退出
            IntentStatus st = null;
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (System.nanoTime() < deadline) {
                Intent cur = store.findById("test-retry-max");
                st = cur != null ? cur.getStatus() : null;
                if (st == IntentStatus.DEAD_LETTERED) break;
                if (deliveries.get() > 3) break;
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(5));
            }
            assertEquals(IntentStatus.DEAD_LETTERED, st,
                "RETRY result must dead-letter after maxAttempts=3 (not retry forever)");
            assertEquals(3, deliveries.get(), "exactly maxAttempts deliveries before dead-letter");
        } finally {
            scheduler.stop();
        }
    }
}
