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
 * @author loomq
 * @since v0.5.0
 */
public class LoomQMetrics {

    // ==================== 流水线指标 ====================
    private final PipelineMetricsRegistry pipelineMetrics = new PipelineMetricsRegistry();

    // ==================== WAL 健康指标 ====================
    private final WalHealthMetricsRegistry walHealthMetrics = new WalHealthMetricsRegistry();

    // ==================== Raft 指标 ====================
    private final RaftMetricsRegistry raftMetrics = new RaftMetricsRegistry();

    public void updateWalLastFlushTime(long timestamp) {
        walHealthMetrics.updateWalLastFlushTime(timestamp);
    }

    public void incrementWalFlushErrorCount() {
        walHealthMetrics.incrementWalFlushErrorCount();
    }

    public void recordWalFlushLatency(long latencyMs) {
        walHealthMetrics.recordWalFlushLatency(latencyMs);
    }

    public void updateWalPendingWrites(long count) {
        walHealthMetrics.updateWalPendingWrites(count);
    }

    public void updateWalRingBufferSize(long size) {
        walHealthMetrics.updateWalRingBufferSize(size);
    }

    public void updateWalHealth(boolean healthy) {
        walHealthMetrics.updateWalHealth(healthy);
    }

    public boolean isWalHealthy() {
        return walHealthMetrics.isWalHealthy();
    }

    public long getWalLastFlushTime() {
        return walHealthMetrics.getWalLastFlushTime();
    }

    public long getWalIdleTimeMs() {
        return walHealthMetrics.getWalIdleTimeMs();
    }

    // ==================== Raft 指标更新 ====================

    public void updateRaftRole(String role) {
        raftMetrics.updateRaftRole(role);
    }

    public void updateRaftEpoch(long epoch) {
        raftMetrics.updateRaftEpoch(epoch);
    }

    public void updateRaftCommitIndex(long commitIndex) {
        raftMetrics.updateRaftCommitIndex(commitIndex);
    }

    public void updateRaftLastApplied(long lastApplied) {
        raftMetrics.updateRaftLastApplied(lastApplied);
    }

    public void updateRaftLeaderId(String leaderId) {
        raftMetrics.updateRaftLeaderId(leaderId);
    }

    public void updateRaftReplicationLag(long replicationLag) {
        raftMetrics.updateRaftReplicationLag(replicationLag);
    }

    public void updateRaftConnectedPeers(int connectedPeers) {
        raftMetrics.updateRaftConnectedPeers(connectedPeers);
    }

    public void updateRaftTotalPeers(int totalPeers) {
        raftMetrics.updateRaftTotalPeers(totalPeers);
    }

    public void updateRaftPendingWrites(long pendingWrites) {
        raftMetrics.updateRaftPendingWrites(pendingWrites);
    }

    public void recordRaftWriteProposalLatency(long latencyMs) {
        raftMetrics.recordRaftWriteProposalLatency(latencyMs);
    }

    public void incrementRaftWriteTimeouts() {
        raftMetrics.incrementRaftWriteTimeouts();
    }

    public void incrementRaftWriteStepDownAborts() {
        raftMetrics.incrementRaftWriteStepDownAborts();
    }

    public void incrementRaftWriteBackpressureRejects() {
        raftMetrics.incrementRaftWriteBackpressureRejects();
    }

    public void incrementRaftWriteRevisionConflicts() {
        raftMetrics.incrementRaftWriteRevisionConflicts();
    }

    // ==================== 单例模式 ====================
    private static final LoomQMetrics INSTANCE = new LoomQMetrics();

    public static LoomQMetrics getInstance() {
        return INSTANCE;
    }

    private LoomQMetrics() {}

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

    // ==================== 复制计数方法 ====================

    public void recordReplicationRecordSent(int bytes) {
        pipelineMetrics.recordReplicationRecordSent(bytes);
    }

    public void recordReplicationRecordAcked() {
        pipelineMetrics.recordReplicationRecordAcked();
    }

    public void updateReplicationLag(long lagMs) {
        pipelineMetrics.updateReplicationLag(lagMs);
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

    public MetricsSnapshot snapshot() {
        MetricsCollector mc = MetricsCollector.getInstance();
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
            pipelineMetrics.getReplicationRecordsSent(),
            pipelineMetrics.getReplicationRecordsAcked(),
            pipelineMetrics.getReplicationLagMs(),
            pipelineMetrics.getWalRecordsWritten(),
            pipelineMetrics.getSnapshotsCreated(),
            pendingIntents,
            pipelineMetrics.getActiveDispatches(),
            // WAL 健康指标
            walHealthMetrics.isWalHealthy(),
            walHealthMetrics.getWalLastFlushTime(),
            getWalIdleTimeMs(),
            walHealthMetrics.getWalFlushErrorCount(),
            walHealthMetrics.calculateAverageFlushLatency(pipelineMetrics.getWalRecordsWritten()),
            walHealthMetrics.getWalFlushLatencyMaxMs(),
            walHealthMetrics.getWalPendingWrites(),
            walHealthMetrics.getWalRingBufferSize(),
            raftMetrics.getRaftRole(),
            raftMetrics.getRaftLeaderId(),
            raftMetrics.getRaftEpoch(),
            raftMetrics.getRaftCommitIndex(),
            raftMetrics.getRaftLastApplied(),
            raftMetrics.getRaftCommitLag(),
            raftMetrics.getRaftReplicationLag(),
            raftMetrics.getRaftConnectedPeers(),
            raftMetrics.getRaftTotalPeers(),
            raftMetrics.getRaftPendingWrites(),
            raftMetrics.getRaftAverageWriteProposalLatencyMs(),
            raftMetrics.getRaftWriteProposalLatencyMaxMs(),
            raftMetrics.getRaftWriteTimeouts(),
            raftMetrics.getRaftWriteStepDownAborts(),
            raftMetrics.getRaftWriteBackpressureRejects(),
            raftMetrics.getRaftWriteRevisionConflicts()
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
        long replicationRecordsSent,
        long replicationRecordsAcked,
        long replicationLagMs,
        long walRecordsWritten,
        long snapshotsCreated,
        long pendingIntents,
        long activeDispatches,
        // WAL 健康指标
        boolean walHealthy,
        long walLastFlushTime,
        long walIdleTimeMs,
        long walFlushErrorCount,
        double avgWalFlushLatencyMs,
        long maxWalFlushLatencyMs,
        long walPendingWrites,
        long walRingBufferSize,
        String raftRole,
        String raftLeaderId,
        long raftEpoch,
        long raftCommitIndex,
        long raftLastApplied,
        long raftCommitLag,
        long raftReplicationLag,
        int raftConnectedPeers,
        int raftTotalPeers,
        long raftPendingWrites,
        double raftWriteProposalLatencyMs,
        long raftWriteProposalLatencyMaxMs,
        long raftWriteTimeouts,
        long raftWriteStepDownAborts,
        long raftWriteBackpressureRejects,
        long raftWriteRevisionConflicts
    ) {}

    // ==================== 重置 ====================

    public void reset() {
        pipelineMetrics.reset();
        // WAL 健康指标重置
        walHealthMetrics.reset();
        raftMetrics.reset();
        MetricsCollector.getInstance().resetRuntimeIntentMetrics();
    }
}
