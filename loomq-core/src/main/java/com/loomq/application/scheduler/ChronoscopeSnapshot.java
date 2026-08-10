package com.loomq.application.scheduler;

import com.loomq.common.MetricsCollector;
import com.loomq.domain.intent.PrecisionTier;
import java.time.Instant;
import java.util.Map;

/**
 * 调度器内部状态快照，用于 Chronoscope 端点。
 *
 * <p>将调度器所有内部状态聚合为一个可序列化的快照，
 * 让运维人员能像 X 光一样看到调度器的实时内部状态。</p>
 */
public record ChronoscopeSnapshot(
    Map<PrecisionTier, TierSnapshot> tiers,
    int cohortCount,
    int cohortPendingIntents,
    BorrowSnapshot borrow,
    PermitTimingSnapshot permitTiming,
    String timestamp
) {

    public record TierSnapshot(
        int pendingCount,
        SemaphoreSnapshot semaphore,
        int dispatchQueueSize,
        boolean underBackpressure,
        double utilizationPct,
        WakeLatencySnapshot wakeLatency
    ) {}

    public record SemaphoreSnapshot(
        int available,
        int max,
        int borrowed
    ) {}

    public record WakeLatencySnapshot(
        double p50,
        double p95,
        double p99
    ) {}

    public record BorrowSnapshot(
        long ownAcquires,
        long ownBlockingAcquires,
        long borrowedAcquires,
        double borrowRate
    ) {}

    public record PermitTimingSnapshot(
        double avgAcquireWaitMs,
        double avgPermitHoldMs,
        double avgDeliverAsyncUs,
        double avgBlockingWaitMs
    ) {}

    public static ChronoscopeSnapshot from(PrecisionScheduler scheduler,
                                            BucketGroupManager bucketGroupManager,
                                            CohortManager cohortManager,
                                            MetricsCollector metrics) {
        Map<PrecisionTier, PrecisionScheduler.BackpressureInfo> backpressure = scheduler.getBackpressureStatus();
        Map<PrecisionTier, TierSnapshot> tierSnapshots = new java.util.EnumMap<>(PrecisionTier.class);

        for (PrecisionTier tier : PrecisionTier.values()) {
            PrecisionScheduler.BackpressureInfo bp = backpressure.get(tier);
            int pendingCount = bucketGroupManager.getPendingCounts().getOrDefault(tier, 0);

            SemaphoreSnapshot sem = new SemaphoreSnapshot(
                bp != null ? bp.availablePermits() : 0,
                bp != null ? bp.maxConcurrency() : 0,
                bp != null ? bp.borrowedCount() : 0
            );

            MetricsCollector.LatencySnapshot wakeupLatency =
                metrics.getWakeupLatencySnapshot(tier);
            WakeLatencySnapshot latency = new WakeLatencySnapshot(
                wakeupLatency.p50(), wakeupLatency.p95(), wakeupLatency.p99());

            tierSnapshots.put(tier, new TierSnapshot(
                pendingCount,
                sem,
                bp != null ? bp.queueSize() : 0,
                bp != null ? bp.underBackpressure() : false,
                bp != null ? bp.utilizationPct() : 0.0,
                latency
            ));
        }

        PrecisionScheduler.BorrowStats borrowStats = scheduler.getBorrowStats();
        BorrowSnapshot borrow = new BorrowSnapshot(
            borrowStats.ownAcquires.get(),
            borrowStats.ownBlockingAcquires.get(),
            borrowStats.borrowedAcquires.get(),
            borrowStats.borrowRate()
        );

        PrecisionScheduler.PermitTimingStats pts = scheduler.getPermitTimingStats();
        PermitTimingSnapshot permitTiming = new PermitTimingSnapshot(
            pts.avgAcquireWaitMs(),
            pts.avgPermitHoldMs(),
            pts.avgDeliverAsyncUs(),
            pts.avgBlockingWaitMs()
        );

        return new ChronoscopeSnapshot(tierSnapshots,
            cohortManager.cohortCount(), cohortManager.pendingIntentCount(),
            borrow, permitTiming, Instant.now().toString());
    }
}
