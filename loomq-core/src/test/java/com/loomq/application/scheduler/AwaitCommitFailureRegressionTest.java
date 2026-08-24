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
 * Issue B 残留死锁路径回归:awaitCommit 失败(慢盘双超时)不得吞 onDelivered / 泄漏槽位。
 *
 * <p>I6 只包住了非阻塞 put(persist/persistTerminalInPlace);若锁外的 awaitCommit 抛异常,
 * reclaimTerminal 与 notifyObservers 会被跳过 → 复现 Issue B 死锁症状。本测试断言 awaitCommit
 * 失败时 onDelivered 仍触发 + persistFailures 记录。</p>
 */
class AwaitCommitFailureRegressionTest {

    private PrecisionScheduler scheduler;
    private ConcurrentIntentStore intentStore;

    @AfterEach
    void tearDown() {
        if (scheduler != null) scheduler.stop();
    }

    @Test
    void awaitCommitFailureShouldNotSwallowOnDelivered() throws Exception {
        CountDownLatch onDelivered = new CountDownLatch(1);

        DeliveryHandler handler = intent -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        intentStore = new ConcurrentIntentStore();
        scheduler = new PrecisionScheduler(intentStore, handler, null);

        // 注入 awaitCommit 恒抛的 sink(模拟慢盘双超时)。
        scheduler.setStateChangeSink(new StateChangeSink() {
            @Override public void persist(Intent intent) {}
            @Override public void persistTerminalInPlace(Intent intent) {}
            @Override public void awaitCommit() {
                throw new IllegalStateException("slow disk: await commit timed out after inline force");
            }
            @Override public void reclaimTerminal(String intentId) {}
        });

        scheduler.addObserver(new IntentObserver() {
            @Override public void onDelivered(Intent i, DeliveryResult r) { onDelivered.countDown(); }
            @Override public void onScheduled(Intent i) {}
            @Override public void onDeadLettered(Intent i) {}
            @Override public void onExpired(Intent i) {}
            @Override public void onDeliveryFailed(Intent i, Throwable e) {}
        });
        scheduler.start();

        Intent intent = new Intent("await-fail-1");
        intent.setExecuteAt(Instant.now().minusMillis(100));
        intent.transitionTo(IntentStatus.SCHEDULED);
        intentStore.save(intent);
        scheduler.schedule(intent);

        assertTrue(onDelivered.await(5, TimeUnit.SECONDS),
            "awaitCommit 失败时 onDelivered 必须仍触发(否则槽位泄漏死锁)");
        assertTrue(scheduler.getPersistFailures() >= 1, "应记录持久化失败(persistFailures>=1)");
    }
}
