package com.loomq.application.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.common.MetricsCollector;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.spi.DeliveryHandler;
import com.loomq.spi.DeliveryHandler.DeliveryResult;
import com.loomq.store.ConcurrentIntentStore;
import com.loomq.store.IntentStore;
import com.loomq.tracing.IntentTrace;
import com.loomq.tracing.IntentTraceStore;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * R21: R19 的 trace 守卫(仅当 trace 不存在才 recordCreated)引入回归——同 id 重建
 * (R8 明确支持的路径)时旧 trace 尚未 LRU 淘汰,recordCreated 被跳过,新 incarnation
 * 继承旧 createdAt/status(如 ACKED):enqueueLag/totalLag 按旧 createdAt 错算,排查
 * 归因完全错误。修复:recordCreated 记录 intent 自身的 createdAt,重建时新旧 createdAt
 * 不匹配 → 刷新 trace。
 */
class BugTraceStaleAcrossRecreateTest {

    private PrecisionScheduler scheduler;

    @AfterEach
    void tearDown() {
        if (scheduler != null) {
            scheduler.stop();
        }
    }

    @Test
    void recreateMustRefreshTrace() throws Exception {
        IntentStore store = new ConcurrentIntentStore();
        IntentTraceStore traceStore = new IntentTraceStore();
        CountDownLatch delivered = new CountDownLatch(1);
        DeliveryHandler handler = intent -> {
            delivered.countDown();
            return CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        };
        scheduler = new PrecisionScheduler(store, handler, null, null, new MetricsCollector(), traceStore);
        scheduler.start();

        // incarnation 1:投递 → ACKED(finalize 异步落 trace,需轮询)
        Intent first = new Intent("r21-recreate-0001");
        first.setExecuteAt(Instant.now().minusMillis(5));
        first.transitionTo(IntentStatus.SCHEDULED);
        store.save(first);
        scheduler.schedule(first);
        assertTrue(delivered.await(3, TimeUnit.SECONDS), "incarnation 1 must deliver");
        IntentTrace stale = awaitAcked(traceStore, "r21-recreate-0001");
        assertEquals(IntentStatus.ACKED, stale.status(), "incarnation 1 must leave an ACKED trace");

        // incarnation 2:同 id 重建(R8 路径)——修复前 recordCreated 被 R19 守卫跳过,
        // trace 继承旧 createdAt/ACKED;修复后 createdAt 不匹配 → 刷新 trace。
        Intent second = new Intent("r21-recreate-0001");
        second.setExecuteAt(Instant.now().plusSeconds(30));
        second.transitionTo(IntentStatus.SCHEDULED);
        store.save(second);
        scheduler.schedule(second);

        IntentTrace trace = traceStore.get("r21-recreate-0001");
        assertEquals(second.getCreatedAt().toEpochMilli(), trace.createdAtMs(),
            "trace must reflect the new incarnation's creation time, not the old one");
        assertEquals(IntentStatus.CREATED, trace.status(),
            "recreate must reset the trace status, not inherit the old incarnation's ACKED");
    }

    /** 轮询等待 finalize 异步把 trace 状态推进到 ACKED。 */
    private static IntentTrace awaitAcked(IntentTraceStore traceStore, String id) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (System.nanoTime() < deadline) {
            IntentTrace trace = traceStore.get(id);
            if (trace != null && trace.status() == IntentStatus.ACKED) {
                return trace;
            }
            Thread.sleep(5);
        }
        IntentTrace last = traceStore.get(id);
        throw new AssertionError("trace never reached ACKED (was "
            + (last == null ? null : last.status()) + ")");
    }
}
