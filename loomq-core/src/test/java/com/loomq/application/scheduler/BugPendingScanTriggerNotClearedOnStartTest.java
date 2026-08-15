package com.loomq.application.scheduler;

import static org.junit.jupiter.api.Assertions.assertFalse;

import com.loomq.domain.intent.PrecisionTier;
import com.loomq.spi.DeliveryHandler;
import com.loomq.spi.DeliveryHandler.DeliveryResult;
import com.loomq.store.ConcurrentIntentStore;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

/**
 * start() 应清空 pendingScanTrigger，避免上一生命周期残留标志导致触发式扫描失效。
 */
class BugPendingScanTriggerNotClearedOnStartTest {

    @Test
    void startMustClearPendingScanTrigger() throws Exception {
        ConcurrentIntentStore store = new ConcurrentIntentStore();
        DeliveryHandler handler = i -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        PrecisionScheduler scheduler = new PrecisionScheduler(store, handler, null);
        scheduler.start();
        scheduler.stop();

        // 模拟上一生命周期残留的 pending=true
        Field f = PrecisionScheduler.class.getDeclaredField("pendingScanTrigger");
        f.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<PrecisionTier, AtomicBoolean> map = (Map<PrecisionTier, AtomicBoolean>) f.get(scheduler);
        map.computeIfAbsent(PrecisionTier.STANDARD, k -> new AtomicBoolean(false)).set(true);

        scheduler.start();

        assertFalse(map.getOrDefault(PrecisionTier.STANDARD, new AtomicBoolean(false)).get(),
            "start() must clear pendingScanTrigger so cohort-triggered scans are not permanently disabled");
        scheduler.stop();
    }
}
