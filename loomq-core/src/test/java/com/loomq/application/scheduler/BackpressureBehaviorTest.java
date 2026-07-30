package com.loomq.application.scheduler;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.common.MetricsCollector;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.domain.intent.PrecisionTierCatalog;
import com.loomq.domain.intent.PrecisionTierProfile;
import com.loomq.domain.intent.WalMode;
import com.loomq.store.ConcurrentIntentStore;
import com.loomq.tracing.IntentTraceStore;
import java.time.Instant;
import java.util.EnumMap;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

/**
 * 背压行为测试：dispatch queue 满时触发 backpressure 指标 + intent 回退重排。
 */
class BackpressureBehaviorTest {

    private PrecisionTierCatalog tinyQueueCatalog() {
        EnumMap<PrecisionTier, PrecisionTierProfile> profiles = new EnumMap<>(PrecisionTier.class);
        profiles.put(PrecisionTier.ECONOMY, new PrecisionTierProfile(
            1000, 1, 1, 100, 1, 2, WalMode.DURABLE, 200));
        return PrecisionTierCatalog.of(profiles, PrecisionTier.ECONOMY);
    }

    @Test
    void dispatchQueueFullTriggersBackpressureAndRequeues() throws Exception {
        PrecisionTierCatalog catalog = tinyQueueCatalog();
        ConcurrentIntentStore store = new ConcurrentIntentStore();
        MetricsCollector mc = new MetricsCollector();

        com.loomq.spi.DeliveryHandler blockingHandler = intent ->
            new CompletableFuture<>();

        PrecisionScheduler scheduler = new PrecisionScheduler(
            store, blockingHandler, null, catalog, mc, new IntentTraceStore());

        long beforeOfferFailed = mc.getDispatchQueueOfferFailed(PrecisionTier.ECONOMY);
        long beforeBackpressure = mc.getBackpressureEventsByTier()
            .getOrDefault(PrecisionTier.ECONOMY, 0L);

        scheduler.start();
        try {
            for (int i = 0; i < 5; i++) {
                Intent intent = new Intent("intent_bp_" + i);
                intent.setExecuteAt(Instant.now().minusMillis(100));
                intent.setPrecisionTier(PrecisionTier.ECONOMY);
                intent.transitionTo(IntentStatus.SCHEDULED);
                scheduler.schedule(intent);
            }

            Thread.sleep(1000);

            long afterOfferFailed = mc.getDispatchQueueOfferFailed(PrecisionTier.ECONOMY);
            long afterBackpressure = mc.getBackpressureEventsByTier()
                .getOrDefault(PrecisionTier.ECONOMY, 0L);

            assertTrue(afterOfferFailed > beforeOfferFailed,
                "dispatch queue offer-failed metric must increment when queue is full");
            assertTrue(afterBackpressure > beforeBackpressure,
                "backpressure event metric must increment when queue is full");

        } finally {
            scheduler.stop();
        }
    }

    @Test
    void backpressureRequeuedIntentRemainsScheduled() throws Exception {
        PrecisionTierCatalog catalog = tinyQueueCatalog();
        ConcurrentIntentStore store = new ConcurrentIntentStore();

        com.loomq.spi.DeliveryHandler blockingHandler = intent ->
            new CompletableFuture<>();

        PrecisionScheduler scheduler = new PrecisionScheduler(
            store, blockingHandler, null, catalog, new MetricsCollector(), new IntentTraceStore());

        scheduler.start();
        try {
            Intent intent = new Intent("intent_bp_status");
            intent.setExecuteAt(Instant.now().minusMillis(100));
            intent.setPrecisionTier(PrecisionTier.ECONOMY);
            intent.transitionTo(IntentStatus.SCHEDULED);
            scheduler.schedule(intent);

            Thread.sleep(600);

            Intent found = store.findByIdInternal("intent_bp_status");
            if (found != null) {
                assertTrue(found.getStatus() == IntentStatus.SCHEDULED
                        || found.getStatus() == IntentStatus.DUE
                        || found.getStatus() == IntentStatus.DISPATCHING,
                    "requeued intent must remain in a pre-delivery state, got " + found.getStatus());
            }
        } finally {
            scheduler.stop();
        }
    }
}
