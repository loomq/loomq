package com.loomq.application.scheduler;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.loomq.domain.intent.PrecisionTier;
import com.loomq.spi.DeliveryHandler;
import com.loomq.spi.DeliveryHandler.DeliveryResult;
import com.loomq.store.ConcurrentIntentStore;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

/**
 * R21 修复只覆盖了 scanSchedulers == null 的分支；如果 stop() 已经 shutdown 但尚未 clear，
 * submit 会抛 RejectedExecutionException，pending 标志同样会卡死。本测试要求该场景下标志收敛。
 */
class BugTriggerScanRejectedExecutionTest {

    @Test
    void triggerScanMustResetPendingWhenSubmitIsRejected() throws Exception {
        ConcurrentIntentStore store = new ConcurrentIntentStore();
        DeliveryHandler handler = i -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        PrecisionScheduler scheduler = new PrecisionScheduler(store, handler, null);
        scheduler.start();

        // 模拟 stop() 中 shutdown 后、clear 前的窗口：scanSchedulers 仍指向已终止的 executor。
        // scanSchedulers 已随扫描全家迁入 ScanCoordinator(Task 5)。
        ScheduledExecutorService dead = Executors.newSingleThreadScheduledExecutor();
        dead.shutdown();
        Field sf = PrecisionScheduler.class.getDeclaredField("scanCoordinator");
        sf.setAccessible(true);
        Object coordinator = sf.get(scheduler);
        Field mapField = ScanCoordinator.class.getDeclaredField("scanSchedulers");
        mapField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<PrecisionTier, ScheduledExecutorService> map =
            (Map<PrecisionTier, ScheduledExecutorService>) mapField.get(coordinator);
        map.put(PrecisionTier.STANDARD, dead);

        Method m = ScanCoordinator.class.getDeclaredMethod("triggerScan", PrecisionTier.class);
        m.setAccessible(true);
        assertDoesNotThrow(() -> m.invoke(coordinator, PrecisionTier.STANDARD),
            "triggerScan must not throw when submit is rejected; it should reset pending and return");

        AtomicBoolean pending = pendingFlag(scheduler, PrecisionTier.STANDARD);
        assertFalse(pending.get(),
            "pendingScanTrigger must converge to false even when submit is rejected");

        scheduler.stop();
    }

    @SuppressWarnings("unchecked")
    private static AtomicBoolean pendingFlag(PrecisionScheduler scheduler, PrecisionTier tier) throws Exception {
        Field sf = PrecisionScheduler.class.getDeclaredField("scanCoordinator");
        sf.setAccessible(true);
        Field f = ScanCoordinator.class.getDeclaredField("pendingScanTrigger");
        f.setAccessible(true);
        Map<PrecisionTier, AtomicBoolean> map = (Map<PrecisionTier, AtomicBoolean>) f.get(sf.get(scheduler));
        AtomicBoolean flag = map.get(tier);
        if (flag == null) {
            throw new IllegalStateException("no pending flag for " + tier);
        }
        return flag;
    }
}
