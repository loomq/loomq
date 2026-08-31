package com.loomq.application.scheduler;

import static org.junit.jupiter.api.Assertions.assertFalse;

import com.loomq.domain.intent.PrecisionTier;
import com.loomq.spi.DeliveryHandler;
import com.loomq.spi.DeliveryHandler.DeliveryResult;
import com.loomq.store.ConcurrentIntentStore;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

/**
 * R21: triggerScan 对固定频率档(fixed-rate,非 adaptive)用 pendingScanTrigger 去重:
 * CAS 置 true 后若 scanSchedulers 为 null(stop 竞态/stop 后 waker 残余 flush)则跳过
 * submit 且标志位永不复位——stop→start 后该档所有 cohort-flush 触发式扫描永久失效
 * (只剩固定 tick)。修复:调度器缺失时复位标志,使标志恒收敛。
 */
class BugTriggerScanStuckFlagTest {

    @Test
    void triggerScanMustNotStickWhenSchedulerIsNull() throws Exception {
        ConcurrentIntentStore store = new ConcurrentIntentStore();
        DeliveryHandler handler = i -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        PrecisionScheduler scheduler = new PrecisionScheduler(store, handler, null);
        scheduler.start();
        scheduler.stop();

        // 模拟 stop 竞态窗口:scanSchedulers 已清空时触发一次(如 waker 残余 flush)。
        // 修复前:标志被置 true 后无人复位,永久卡死;修复后:标志复位收敛。
        invokeTriggerScan(scheduler, PrecisionTier.STANDARD);
        AtomicBoolean pending = pendingFlag(scheduler, PrecisionTier.STANDARD);
        assertFalse(pending.get(), "flag must converge even when the trigger finds no scheduler");

        // 重启后再次触发:修复前 CAS 恒失败(标志仍 true)→ 无触发;修复后触发成功并复位
        scheduler.start();
        invokeTriggerScan(scheduler, PrecisionTier.STANDARD);

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (pending.get()) {
            if (System.nanoTime() > deadline) {
                break;
            }
            Thread.sleep(5);
        }
        assertFalse(pending.get(),
            "pendingScanTrigger must converge to false after restart (stuck flag kills cohort-flush scans)");
        scheduler.stop();
    }

    private static void invokeTriggerScan(PrecisionScheduler scheduler, PrecisionTier tier) throws Exception {
        // triggerScan 已随扫描全家迁入 ScanCoordinator(Task 5)
        Method m = ScanCoordinator.class.getDeclaredMethod("triggerScan", PrecisionTier.class);
        m.setAccessible(true);
        m.invoke(scanCoordinator(scheduler), tier);
    }

    @SuppressWarnings("unchecked")
    private static AtomicBoolean pendingFlag(PrecisionScheduler scheduler, PrecisionTier tier) throws Exception {
        Field f = ScanCoordinator.class.getDeclaredField("pendingScanTrigger");
        f.setAccessible(true);
        Map<PrecisionTier, AtomicBoolean> map = (Map<PrecisionTier, AtomicBoolean>) f.get(scanCoordinator(scheduler));
        AtomicBoolean flag = map.get(tier);
        if (flag == null) {
            throw new IllegalStateException("no pending flag for " + tier);
        }
        return flag;
    }

    private static Object scanCoordinator(PrecisionScheduler scheduler) throws Exception {
        Field f = PrecisionScheduler.class.getDeclaredField("scanCoordinator");
        f.setAccessible(true);
        return f.get(scheduler);
    }
}
