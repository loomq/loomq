package com.loomq.application.scheduler;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

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
import org.junit.jupiter.api.Test;

/**
 * 重启回归：stop() 之后再次 start() 必须可用。
 *
 * <p>修复前 sharedExecutor 在 stop() 中被关闭且不可重建——第二次 start() 在
 * startBatchConsumers 提交消费者任务时被 RejectedExecutionException 打断，
 * running=true 却无消费者/扫描，调度器半死不活。</p>
 */
class BugSchedulerRestartTest {

    @Test
    void startAfterStopMustWork() throws Exception {
        ConcurrentIntentStore intentStore = new ConcurrentIntentStore();
        AtomicInteger deliveries = new AtomicInteger();
        DeliveryHandler handler = intent -> {
            deliveries.incrementAndGet();
            return CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        };
        PrecisionScheduler scheduler = new PrecisionScheduler(intentStore, handler, null);
        scheduler.start();
        scheduler.stop();

        // 修复前：start() 抛 RejectedExecutionException
        assertDoesNotThrow(scheduler::start, "start() after stop() must not throw");

        Intent it = new Intent("restart-1");
        it.setExecuteAt(Instant.now().plusMillis(50));
        it.setPrecisionTier(PrecisionTier.ULTRA);
        it.transitionTo(IntentStatus.SCHEDULED);
        intentStore.save(it);
        scheduler.schedule(it);

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (deliveries.get() == 0 && System.nanoTime() < deadline) {
            Thread.sleep(20);
        }
        assertEquals(1, deliveries.get(), "intent must be delivered after scheduler restart");
        scheduler.stop();
    }
}
