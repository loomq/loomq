package com.loomq.application.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.RedeliveryPolicy;
import com.loomq.spi.DeliveryHandler;
import com.loomq.spi.DeliveryHandler.DeliveryResult;
import com.loomq.store.ConcurrentIntentStore;
import com.loomq.store.IntentStore;
import com.loomq.tracing.IntentTrace;
import com.loomq.tracing.IntentTraceStore;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/**
 * R19: schedule() 无条件调用 recordCreated——重试重排程把整条 trace 替换为一条全新的
 * CREATED 记录,清空投递历史(createdAt 被改写为重试时刻),且死信/过期终态分支从不
 * 更新 trace 状态——最终排查 "intent 为什么死了" 时看到的是 CREATED,误导观测。
 *
 * <p>复现链:首轮投递 RETRY → 重试重排程走 schedule() → recordCreated 整体替换 trace
 * (createdAt=重试时刻,status=CREATED)→ 达到 maxAttempts 终态化 DEAD_LETTERED 分支无
 * trace 更新 → 终态 trace 恒为 CREATED。</p>
 *
 * <p>修复:schedule() 仅当 trace 不存在(真正的新建)时才 recordCreated;终态分支
 * (DEAD_LETTERED/EXPIRED)补 updateStatus。</p>
 */
class BugRetryRescheduleWipesTraceTest {

    @Test
    void deadLetteredIntentTraceMustSurviveRetryReschedules() throws Exception {
        IntentStore store = new ConcurrentIntentStore();
        IntentTraceStore traceStore = new IntentTraceStore();
        AtomicLong firstAttemptAtMs = new AtomicLong();
        DeliveryHandler handler = intent -> {
            firstAttemptAtMs.compareAndSet(0, System.currentTimeMillis());
            return CompletableFuture.completedFuture(DeliveryResult.RETRY);
        };
        PrecisionScheduler scheduler = new PrecisionScheduler(
            store, handler, null, null, new com.loomq.common.MetricsCollector(), traceStore);
        scheduler.start();
        try {
            Intent intent = new Intent("r19-trace-retry-0001");
            intent.setExecuteAt(Instant.now().minusMillis(50));
            // maxAttempts=2, fixed 1ms 无抖动 → 快速收敛到死信
            intent.setRedelivery(new RedeliveryPolicy(2, "fixed", 1, 1, 1.0, false));
            intent.transitionTo(IntentStatus.SCHEDULED);
            store.save(intent);
            scheduler.schedule(intent);

            IntentStatus st = null;
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (System.nanoTime() < deadline) {
                Intent cur = store.findById("r19-trace-retry-0001");
                st = cur != null ? cur.getStatus() : null;
                if (st == IntentStatus.DEAD_LETTERED) break;
                Thread.sleep(10);
            }
            assertEquals(IntentStatus.DEAD_LETTERED, st, "intent must dead-letter after maxAttempts");

            IntentTrace trace = traceStore.get("r19-trace-retry-0001");
            assertNotNull(trace, "trace must exist for the dead-lettered intent");
            // 修复前:最后一次重试重排程的 recordCreated 把状态重置为 CREATED,且死信分支
            // 无 updateStatus → 终态 trace 显示 CREATED,误导排查。
            assertEquals(IntentStatus.DEAD_LETTERED, trace.status(),
                "terminal trace must reflect DEAD_LETTERED, not the wiped CREATED");
            // 修复前:createdAt 被改写为重试时刻(晚于首次投递时刻)。
            assertTrue(trace.createdAtMs() <= firstAttemptAtMs.get(),
                "trace createdAt must remain the original creation time, not the retry time (was "
                    + trace.createdAtMs() + ", first attempt at " + firstAttemptAtMs.get() + ")");
        } finally {
            scheduler.stop();
        }
    }
}
