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
        // 不 start():live 扫描器会抢在断言前认领,改用反射直接调 scanAndDispatch,
        // 时序完全确定(executeAt 固定在当前桶中部,见下)。

        Intent intent = new Intent("r21-scandue-cancel-0001");
        // 桶中部:无论 now 落在 500ms 桶内哪个位置,executeAt 都与 scan 时 now 同桶
        // (scanDue 的 headMap 必然扫到),且 now 在前半桶时未到期(覆盖重入路径)、
        // 后半桶时已到期但终态复查仍在投递判定之前——断言对两分支都成立。
        // 旧写法 now+3ms 在跨桶边界时会落入下一桶,scanDue 扫不到 → 偶发 flaky。
        long nowMs = System.currentTimeMillis();
        long bucketStart = nowMs - Math.floorMod(nowMs, 500);
        intent.setExecuteAt(Instant.ofEpochMilli(bucketStart + 250));
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
