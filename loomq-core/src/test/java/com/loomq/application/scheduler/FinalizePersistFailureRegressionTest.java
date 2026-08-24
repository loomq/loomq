package com.loomq.application.scheduler;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.spi.DeliveryHandler;
import com.loomq.spi.DeliveryHandler.DeliveryResult;
import com.loomq.spi.IntentObserver;
import com.loomq.store.ConcurrentIntentStore;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Issue B 回归：终态持久化失败不得吞掉 onDelivered / 不得死锁。
 *
 * <p>根因：SEC 轮桶容量溢出时，
 * finalizeIntent 的 persistStateChange 抛 SlotOverflowException，异常在 synchronized 块内
 * 传播、跳过 notifyObservers(onDelivered)，导致已投递 intent 的通知被吞、基准槽位永久
 * 泄漏、引擎停摆死锁。</p>
 *
 * <p>修复（I6）：persistStateChange 吞掉持久化失败并记录失败计数，调度流程继续，
 * onDelivered 必须仍触发。</p>
 */
class FinalizePersistFailureRegressionTest {

    private PrecisionScheduler scheduler;
    private ConcurrentIntentStore intentStore;

    @AfterEach
    void tearDown() {
        if (scheduler != null) scheduler.stop();
    }

    @Test
    void persistFailureShouldNotSwallowOnDelivered() throws Exception {
        CountDownLatch onDelivered = new CountDownLatch(1);

        DeliveryHandler handler = intent -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        intentStore = new ConcurrentIntentStore();
        scheduler = new PrecisionScheduler(intentStore, handler, null);

        // 注入恒失败的终态持久化（模拟 SEC 桶 SlotOverflowException）。
        scheduler.setStateChangeSink(new StateChangeSink() {
            @Override public void persist(Intent intent) {
                throw new IllegalStateException("bucket overflow: SEC/x (slotsPerBucket=65536)");
            }
            @Override public void persistTerminalInPlace(Intent intent) {
                throw new IllegalStateException("bucket overflow: SEC/x (slotsPerBucket=65536)");
            }
            @Override public void awaitCommit() { }
            @Override public void reclaimTerminal(String intentId) { }
        });

        scheduler.addObserver(new IntentObserver() {
            @Override public void onDelivered(Intent i, DeliveryResult r) { onDelivered.countDown(); }
            @Override public void onScheduled(Intent i) {}
            @Override public void onDeadLettered(Intent i) {}
            @Override public void onExpired(Intent i) {}
            @Override public void onDeliveryFailed(Intent i, Throwable e) {}
        });
        scheduler.start();

        Intent intent = new Intent("persist-fail-1");
        intent.setExecuteAt(Instant.now().minusMillis(100));
        intent.transitionTo(IntentStatus.SCHEDULED);
        intentStore.save(intent);
        scheduler.schedule(intent);

        assertTrue(onDelivered.await(5, TimeUnit.SECONDS),
            "终态持久化失败时 onDelivered 必须仍触发（否则槽位泄漏死锁）");
        assertTrue(scheduler.getPersistFailures() >= 1, "应记录持久化失败（persistFailures>=1）");
    }
}