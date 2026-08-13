package com.loomq.application.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.spi.DeliveryHandler;
import com.loomq.store.ConcurrentIntentStore;
import com.loomq.store.IntentStore;
import java.lang.reflect.Method;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * R21: scanDue CAS 认领成功后、重读 executeAt 之前,cancel 竞态把 intent 置终态——
 * 旧代码仅按 executeAt 决定"投递/重入桶",终态 intent 若 executeAt 尚在未来会被
 * addForced 重新注册进桶+索引:终态条目驻留到原时刻(指标/内存污染,每 cycle 被重复
 * 认领-跳过),pendingCount 虚增。修复:CAS 后补终态复查,终态直接跳过(不投递不重入)。
 *
 * <p>无重复投递(consumer 终态守卫兜底),但索引/指标污染是真实缺陷,且修复一行即可
 * 把"终态不得驻留调度结构"的不变量在扫描路径收口。</p>
 */
class BugScanDueReindexesCanceledIntentTest {

    private PrecisionScheduler scheduler;

    @AfterEach
    void tearDown() {
        if (scheduler != null) {
            scheduler.stop();
        }
    }

    @Test
    void canceledIntentMustNotBeReindexedByScanDue() throws Exception {
        IntentStore store = new ConcurrentIntentStore();
        AtomicInteger deliveries = new AtomicInteger();
        DeliveryHandler handler = intent -> {
            deliveries.incrementAndGet();
            return CompletableFuture.completedFuture(DeliveryHandler.DeliveryResult.SUCCESS);
        };
        scheduler = new PrecisionScheduler(store, handler, null);
        // 不 start():live 扫描器会抢在断言前认领(executeAt 仅有 3ms 余量),改用
        // 反射直接调 scanAndDispatch,时序完全确定。

        Intent intent = new Intent("r21-scandue-cancel-0001");
        intent.setExecuteAt(Instant.now().plusMillis(3)); // 仍在当前 500ms 桶内,scanDue 会扫到但未到期
        intent.transitionTo(IntentStatus.SCHEDULED);
        store.save(intent);
        scheduler.getBucketGroupManager().add(intent);
        assertEquals(1, scheduler.getBucketGroupManager().getPendingCounts().get(PrecisionTier.STANDARD),
            "intent must be pending in the bucket before the scan");

        // 在 CAS 认领成功后、重读 executeAt 前同步执行 cancel(模拟竞态窗口)
        BucketGroup group = scheduler.getBucketGroupManager().getBucketGroup(PrecisionTier.STANDARD);
        group.testScanPostClaimHook = () -> intent.transitionTo(IntentStatus.CANCELED);

        invokeScan(scheduler, PrecisionTier.STANDARD);

        // 修复前:终态 intent 被 addForced 重新注册 → pendingCount 恒 1(虚增)+ 索引复活;
        // 修复后:终态复查跳过 → pendingCount 归零,不驻留任何调度结构。
        assertEquals(0, scheduler.getBucketGroupManager().getPendingCounts().get(PrecisionTier.STANDARD),
            "canceled intent must not be re-indexed into the bucket (pendingCount must drop to 0)");
        assertEquals(0, deliveries.get(), "canceled intent must never be delivered");
        assertTrue(intent.getStatus().isTerminal(), "intent must stay canceled");
    }

    private static void invokeScan(PrecisionScheduler scheduler, PrecisionTier tier) throws Exception {
        Method m = PrecisionScheduler.class.getDeclaredMethod("scanAndDispatch", PrecisionTier.class);
        m.setAccessible(true);
        m.invoke(scheduler, tier);
    }
}
