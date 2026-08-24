package com.loomq.application.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.loomq.domain.intent.PrecisionTier;
import com.loomq.domain.intent.PrecisionTierCatalog;
import com.loomq.domain.intent.PrecisionTierProfile;
import com.loomq.domain.intent.WalMode;
import com.loomq.spi.DeliveryHandler.DeliveryResult;
import com.loomq.store.ConcurrentIntentStore;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

/**
 * 跨档借用后，本档 activeDispatches 应反映真实在途数（含借用），而不是只统计本档 semaphore。
 */
class BugBackpressureIgnoresBorrowedTest {

    @Test
    void activeDispatchesMustIncludeBorrowedInflight() throws Exception {
        EnumMap<PrecisionTier, PrecisionTierProfile> profiles = new EnumMap<>(PrecisionTier.class);
        profiles.put(PrecisionTier.ULTRA, new PrecisionTierProfile(10, 1, 1, 5, 1, 16, WalMode.DURABLE, 10));
        profiles.put(PrecisionTier.FAST, new PrecisionTierProfile(50, 10, 1, 10, 1, 160, WalMode.DURABLE, 50));
        PrecisionTierCatalog catalog = PrecisionTierCatalog.of(profiles, PrecisionTier.ULTRA);

        PrecisionScheduler scheduler = new PrecisionScheduler(new ConcurrentIntentStore(),
            i -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS), null, catalog);
        scheduler.start();

        // 信号量/借用方法已随 Task 4 迁入 DispatchPipeline，经 scheduler.pipeline 反射取得。
        Field pipelineField = PrecisionScheduler.class.getDeclaredField("pipeline");
        pipelineField.setAccessible(true);
        Object pipeline = pipelineField.get(scheduler);

        Field semField = DispatchPipeline.class.getDeclaredField("tierSemaphores");
        semField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<PrecisionTier, ResizableSemaphore> sems = (Map<PrecisionTier, ResizableSemaphore>) semField.get(pipeline);
        ResizableSemaphore ultra = sems.get(PrecisionTier.ULTRA);
        ultra.acquire(); // 占用 ULTRA 唯一 own permit

        Method m = DispatchPipeline.class.getDeclaredMethod("acquireWithBorrow", PrecisionTier.class);
        m.setAccessible(true);
        ResizableSemaphore borrowed =
            (ResizableSemaphore) m.invoke(pipeline, PrecisionTier.ULTRA); // 从 FAST 借用第二个 permit

        // 模拟真实在途：两个 permit 都对应 in-flight delivery
        Field inflightField = PrecisionScheduler.class.getDeclaredField("inFlightCounters");
        inflightField.setAccessible(true);
        InFlightCounters inflight = (InFlightCounters) inflightField.get(scheduler);
        inflight.increment(PrecisionTier.ULTRA);
        inflight.increment(PrecisionTier.ULTRA);

        var status = scheduler.getBackpressureStatus().get(PrecisionTier.ULTRA);
        assertEquals(2, status.activeDispatches(),
            "activeDispatches must count borrowed in-flight deliveries, not just own semaphore permits");

        // 清理测试占用：还原在途计数并释放 permit(借用须配对 decrementBorrowed)——
        // 否则 stop() 的 in-flight 排空(上限 10s)会等满 drain 超时,测试耗时 ~10s。
        inflight.decrement(PrecisionTier.ULTRA);
        inflight.decrement(PrecisionTier.ULTRA);
        if (borrowed != ultra) {
            borrowed.decrementBorrowed();
            borrowed.release();
        }
        ultra.release();

        scheduler.stop();
    }
}
