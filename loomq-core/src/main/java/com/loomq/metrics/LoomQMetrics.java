package com.loomq.metrics;

import com.loomq.common.MetricsCollector;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * LoomQ 可观测性指标 (v0.5)
 *
 * 提供完整的监控指标收集和查询能力
 *
 * <p>注:持久化已由 PHTW(持久化分层时间轮)承担,原 WAL 健康指标退化为默认值
 * (恒健康、零延迟),仅为兼容 {@link MetricsSnapshot} 字段保留。</p>
 *
 * @author loomq
 * @since v0.5.0
 */
public class LoomQMetrics {

    // ==================== 流水线指标 ====================
    private final PipelineMetricsRegistry pipelineMetrics = new PipelineMetricsRegistry();

    // ==================== 单例模式 ====================

    public LoomQMetrics() {}

    // ==================== Intent 计数方法 ====================
    // 注意: Intent 生命周期计数器已统一到 MetricsCollector (com.loomq.common.MetricsCollector)
    // 此处通过 MetricsCollector 读取，避免重复计数和数据分叉

    // ==================== 投递计数方法 ====================

    public void recordDeliveryAttempt() {
        pipelineMetrics.recordDeliveryAttempt();
    }

    public void recordDeliverySuccess(long latencyMs) {
        pipelineMetrics.recordDeliverySuccess(latencyMs);
    }

    public void recordDeliveryFailure() {
        pipelineMetrics.recordDeliveryFailure();
    }

    public void recordDeliveryRetry() {
        pipelineMetrics.recordDeliveryRetry();
    }

    // ==================== 存储计数方法 ====================

    public void recordWalRecordWritten(int bytes) {
        pipelineMetrics.recordWalRecordWritten(bytes);
    }

    public void recordSnapshotCreated() {
        pipelineMetrics.recordSnapshotCreated();
    }

    // ==================== 系统状态更新 ====================

    public void updateActiveDispatches(long count) {
        pipelineMetrics.updateActiveDispatches(count);
    }

    public void updateMemoryUsage(long bytes) {
        pipelineMetrics.updateMemoryUsage(bytes);
    }

    // ==================== 指标查询 ====================

    public MetricsSnapshot snapshot(MetricsCollector mc) {
        
        Map<String, Long> statusSnapshot = new LinkedHashMap<>(mc.getIntentStatusCounts());
        long pendingIntents = mc.getPendingIntents();

        return new MetricsSnapshot(
            mc.getIntentsCreatedTotal(),
            mc.getIntentsAckSuccessTotal(),
            mc.getIntentsCancelledTotal(),
            mc.getIntentsExpiredTotal(),
            mc.getIntentsDeadLetterTotal(),
            Collections.unmodifiableMap(statusSnapshot),
            pipelineMetrics.getDeliveriesTotal(),
            pipelineMetrics.getDeliveriesSuccess(),
            pipelineMetrics.getDeliveriesFailed(),
            pipelineMetrics.getDeliveriesRetried(),
            pipelineMetrics.calculateAverageDeliveryLatency(),
            pipelineMetrics.getDeliveryLatencyMaxMs(),
            pipelineMetrics.getWalRecordsWritten(),
            pipelineMetrics.getSnapshotsCreated(),
            pendingIntents,
            pipelineMetrics.getActiveDispatches(),
            // WAL 健康指标(PHTW 无 WAL flush 概念,恒报默认健康值)
            true,
            0L,
            0L,
            0L,
            0.0,
            0L,
            0L,
            0L
        );
    }

    // ==================== 指标快照 ====================

    public record MetricsSnapshot(
        long intentsCreated,
        long intentsCompleted,
        long intentsCancelled,
        long intentsExpired,
        long intentsDeadLettered,
        Map<String, Long> intentsByStatus,
        long deliveriesTotal,
        long deliveriesSuccess,
        long deliveriesFailed,
        long deliveriesRetried,
        double avgDeliveryLatencyMs,
        long maxDeliveryLatencyMs,
        long walRecordsWritten,
        long snapshotsCreated,
        long pendingIntents,
        long activeDispatches,
        // WAL 健康指标(PHTW 下为默认值,保留字段以兼容 MetricsSnapshot)
        boolean walHealthy,
        long walLastFlushTime,
        long walIdleTimeMs,
        long walFlushErrorCount,
        double avgWalFlushLatencyMs,
        long maxWalFlushLatencyMs,
        long walPendingWrites,
        long walRingBufferSize
    ) {}

    // ==================== 重置 ====================

    public void reset(MetricsCollector mc) {
        pipelineMetrics.reset();
        mc.resetRuntimeIntentMetrics();
    }
}
