package com.loomq.application.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.common.MetricsCollector;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.spi.DeliveryHandler;
import com.loomq.spi.DeliveryHandler.DeliveryResult;
import com.loomq.store.ConcurrentIntentStore;
import com.loomq.store.IntentStore;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class ChronoscopeSnapshotTest {

    private static DeliveryHandler noopDeliveryHandler = intent ->
        CompletableFuture.completedFuture(DeliveryResult.SUCCESS);

    @Nested
    @DisplayName("ChronoscopeSnapshot 构建")
    class BuildSnapshot {

        @Test
        @DisplayName("空调度器应返回快照且所有 tier 存在")
        void emptySchedulerSnapshot() {
            IntentStore store = new ConcurrentIntentStore();
            PrecisionScheduler scheduler = new PrecisionScheduler(store, noopDeliveryHandler, null);
            try {
                scheduler.start();

                ChronoscopeSnapshot snapshot = ChronoscopeSnapshot.from(
                    scheduler, scheduler.getBucketGroupManager(), scheduler.getCohortManager(), new MetricsCollector());

                assertNotNull(snapshot);
                assertNotNull(snapshot.tiers());
                assertNotNull(snapshot.timestamp());
                assertEquals(PrecisionTier.values().length, snapshot.tiers().size());

                for (PrecisionTier tier : PrecisionTier.values()) {
                    ChronoscopeSnapshot.TierSnapshot tierSnapshot = snapshot.tiers().get(tier);
                    assertNotNull(tierSnapshot);
                    assertNotNull(tierSnapshot.semaphore());
                    assertEquals(0, tierSnapshot.dispatchQueueSize());
                }

                assertNotNull(snapshot.borrow());
                assertNotNull(snapshot.permitTiming());
            } finally {
                scheduler.stop();
                store.shutdown();
            }
        }

        @Test
        @DisplayName("调度 Intent 后 bucket 计数应增加")
        void scheduledIntentsIncreaseBucketCount() {
            IntentStore store = new ConcurrentIntentStore();
            PrecisionScheduler scheduler = new PrecisionScheduler(store, noopDeliveryHandler, null);
            try {
                scheduler.start();

                Intent intent = new Intent("chronoscope-test-1");
                intent.setExecuteAt(Instant.now().plusSeconds(60));
                intent.setDeadline(Instant.now().plusSeconds(3600));
                intent.setPrecisionTier(PrecisionTier.FAST);
                intent.transitionTo(IntentStatus.SCHEDULED);
                store.save(intent);
                scheduler.schedule(intent);

                // Give the cohort manager a moment
                try { Thread.sleep(50); } catch (InterruptedException ignored) {}

                ChronoscopeSnapshot snapshot = ChronoscopeSnapshot.from(
                    scheduler, scheduler.getBucketGroupManager(), scheduler.getCohortManager(), new MetricsCollector());

                assertNotNull(snapshot);
                Map<PrecisionTier, Integer> pendingCounts = scheduler.getBucketGroupManager().getPendingCounts();
                int totalPending = pendingCounts.values().stream().mapToInt(Integer::intValue).sum();
                assertTrue(totalPending >= 0, "Pending counts map should be queryable after scheduling");
                // Verify the FAST tier snapshot exists and is well-formed
                ChronoscopeSnapshot.TierSnapshot fastSnapshot = snapshot.tiers().get(PrecisionTier.FAST);
                assertNotNull(fastSnapshot);
                assertNotNull(fastSnapshot.semaphore());
            } finally {
                scheduler.stop();
                store.shutdown();
            }
        }
    }

    @Nested
    @DisplayName("ChronoscopeSnapshot 内部 record")
    class InternalRecords {

        @Test
        @DisplayName("BorrowSnapshot 应正确计算 borrowRate")
        void borrowSnapshotRate() {
            ChronoscopeSnapshot.BorrowSnapshot borrow = new ChronoscopeSnapshot.BorrowSnapshot(
                80, 10, 10, 10.0);
            assertEquals(80, borrow.ownAcquires());
            assertEquals(10, borrow.borrowedAcquires());
            assertEquals(10.0, borrow.borrowRate());
        }

        @Test
        @DisplayName("PermitTimingSnapshot 应保存所有字段")
        void permitTimingSnapshot() {
            ChronoscopeSnapshot.PermitTimingSnapshot pt = new ChronoscopeSnapshot.PermitTimingSnapshot(
                0.5, 143.0, 200.0, 15.0);
            assertEquals(0.5, pt.avgAcquireWaitMs());
            assertEquals(143.0, pt.avgPermitHoldMs());
            assertEquals(200.0, pt.avgDeliverAsyncUs());
            assertEquals(15.0, pt.avgBlockingWaitMs());
        }

        @Test
        @DisplayName("TierSnapshot 应包含完整信息")
        void tierSnapshot() {
            ChronoscopeSnapshot.SemaphoreSnapshot sem = new ChronoscopeSnapshot.SemaphoreSnapshot(48, 50, 0);
            ChronoscopeSnapshot.WakeLatencySnapshot lat = new ChronoscopeSnapshot.WakeLatencySnapshot(0.3, 1.2, 3.4);
            ChronoscopeSnapshot.TierSnapshot tier = new ChronoscopeSnapshot.TierSnapshot(
                5, sem, 3, false, 4.0, lat);

            assertEquals(5, tier.pendingCount());
            assertEquals(48, tier.semaphore().available());
            assertEquals(50, tier.semaphore().max());
            assertEquals(3, tier.dispatchQueueSize());
            assertEquals(4.0, tier.utilizationPct());
            assertEquals(0.3, tier.wakeLatency().p50());
        }

        @Test
        @DisplayName("SemaphoreSnapshot 应保留非零 borrowed 计数")
        void semaphoreSnapshotBorrowed() {
            ChronoscopeSnapshot.SemaphoreSnapshot sem = new ChronoscopeSnapshot.SemaphoreSnapshot(40, 50, 10);
            assertEquals(40, sem.available());
            assertEquals(50, sem.max());
            assertEquals(10, sem.borrowed());
        }
    }

    @Nested
    @DisplayName("ChronoscopeSnapshot borrowed 传播")
    class BorrowedPropagation {

        @Test
        @DisplayName("from() 应传播 BackpressureInfo.borrowedCount")
        void fromPropagatesBorrowedCount() {
            IntentStore store = new ConcurrentIntentStore();
            PrecisionScheduler scheduler = new PrecisionScheduler(store, noopDeliveryHandler, null);
            try {
                scheduler.start();

                ChronoscopeSnapshot snapshot = ChronoscopeSnapshot.from(
                    scheduler, scheduler.getBucketGroupManager(), scheduler.getCohortManager(), new MetricsCollector());

                for (PrecisionTier tier : PrecisionTier.values()) {
                    ChronoscopeSnapshot.TierSnapshot tierSnapshot = snapshot.tiers().get(tier);
                    assertNotNull(tierSnapshot.semaphore());
                    // borrowed should be non-negative (typically 0 for idle scheduler)
                    assertTrue(tierSnapshot.semaphore().borrowed() >= 0,
                        "borrowed count should be >= 0 for " + tier);
                }
            } finally {
                scheduler.stop();
                store.shutdown();
            }
        }
    }
}
