package com.loomq.metrics;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * 流水线指标注册表。
 *
 * 负责投递、复制、存储和系统状态这类运行时指标。
 */
final class PipelineMetricsRegistry {

    private final LongAdder deliveriesTotal = new LongAdder();
    private final LongAdder deliveriesSuccess = new LongAdder();
    private final LongAdder deliveriesFailed = new LongAdder();
    private final LongAdder deliveriesRetried = new LongAdder();

    private final LongAdder deliveryLatencyTotalMs = new LongAdder();
    private final AtomicLong deliveryLatencyMaxMs = new AtomicLong(0);

    private final LongAdder walRecordsWritten = new LongAdder();
    private final LongAdder walBytesWritten = new LongAdder();
    private final LongAdder snapshotsCreated = new LongAdder();

    private final AtomicLong activeDispatches = new AtomicLong(0);
    private final AtomicLong memoryUsageBytes = new AtomicLong(0);

    void recordDeliveryAttempt() {
        deliveriesTotal.increment();
    }

    void recordDeliverySuccess(long latencyMs) {
        deliveriesSuccess.increment();
        deliveryLatencyTotalMs.add(latencyMs);
        deliveryLatencyMaxMs.updateAndGet(max -> Math.max(max, latencyMs));
    }

    void recordDeliveryFailure() {
        deliveriesFailed.increment();
    }

    void recordDeliveryRetry() {
        deliveriesRetried.increment();
    }

    void recordWalRecordWritten(int bytes) {
        walRecordsWritten.increment();
        walBytesWritten.add(bytes);
    }

    void recordSnapshotCreated() {
        snapshotsCreated.increment();
    }

    void updateActiveDispatches(long count) {
        activeDispatches.set(count);
    }

    void updateMemoryUsage(long bytes) {
        memoryUsageBytes.set(bytes);
    }

    long getDeliveriesTotal() {
        return deliveriesTotal.sum();
    }

    long getDeliveriesSuccess() {
        return deliveriesSuccess.sum();
    }

    long getDeliveriesFailed() {
        return deliveriesFailed.sum();
    }

    long getDeliveriesRetried() {
        return deliveriesRetried.sum();
    }

    double calculateAverageDeliveryLatency() {
        long total = deliveriesSuccess.sum();
        if (total == 0) {
            return 0.0;
        }
        return (double) deliveryLatencyTotalMs.sum() / total;
    }

    long getDeliveryLatencyMaxMs() {
        return deliveryLatencyMaxMs.get();
    }

    long getWalRecordsWritten() {
        return walRecordsWritten.sum();
    }

    long getSnapshotsCreated() {
        return snapshotsCreated.sum();
    }

    long getActiveDispatches() {
        return activeDispatches.get();
    }

    void reset() {
        deliveriesTotal.reset();
        deliveriesSuccess.reset();
        deliveriesFailed.reset();
        deliveriesRetried.reset();
        deliveryLatencyTotalMs.reset();
        deliveryLatencyMaxMs.set(0);
        walRecordsWritten.reset();
        walBytesWritten.reset();
        snapshotsCreated.reset();
        activeDispatches.set(0);
        memoryUsageBytes.set(0);
    }
}
