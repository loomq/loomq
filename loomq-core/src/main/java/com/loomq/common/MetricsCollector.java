package com.loomq.common;

import com.loomq.domain.intent.PrecisionTier;
import java.util.Map;

/**
 * 指标采集器
 * 收集并暴露 Prometheus 格式的指标
 */
public class MetricsCollector {

    private final OperationalMetricsRegistry operationalMetrics;

    private final PrecisionTierMetricsRegistry tierMetrics;

    // 触发/唤醒/webhook/总延迟指标
    private final LatencyMetricsRegistry latencyMetrics;

    // 运行时指标
    private final RuntimeMetricsRegistry runtimeMetrics;

    public MetricsCollector() {
        this.operationalMetrics = new OperationalMetricsRegistry();
        this.tierMetrics = new PrecisionTierMetricsRegistry(com.loomq.domain.intent.PrecisionTierCatalog.defaultCatalog());
        this.latencyMetrics = new LatencyMetricsRegistry();
        this.runtimeMetrics = new RuntimeMetricsRegistry();
    }

    public void setWalDataDir(String walDataDir) {
        runtimeMetrics.setWalDataDir(walDataDir);
    }

    public void setSchedulerMaxPendingIntents(long maxPendingIntents) {
        runtimeMetrics.setSchedulerMaxPendingIntents(maxPendingIntents);
    }

    // ========== 计数器更新 ==========

    public void incrementIntentsCreated() {
        operationalMetrics.incrementIntentsCreated();
    }

    public void incrementIntentsAckSuccess() {
        operationalMetrics.incrementIntentsAckSuccess();
    }

    public void incrementIntentsFailedTerminal() {
        operationalMetrics.incrementIntentsFailedTerminal();
    }

    public void incrementIntentsCancelled() {
        operationalMetrics.incrementIntentsCancelled();
    }

    public void incrementIntentsRetry() {
        operationalMetrics.incrementIntentsRetry();
    }

    public void incrementWebhookRequests() {
        operationalMetrics.incrementWebhookRequests();
    }

    public void incrementWebhookTimeout() {
        operationalMetrics.incrementWebhookTimeout();
    }

    public void incrementWebhookError() {
        operationalMetrics.incrementWebhookError();
    }

    public void incrementIntentsExpired() {
        operationalMetrics.incrementIntentsExpired();
    }

    public void incrementIntentsDeadLetter() {
        operationalMetrics.incrementIntentsDeadLetter();
    }

    public void incrementRecoveryOverdue() {
        operationalMetrics.incrementRecoveryOverdue();
    }

    public void updateBucketMetrics(long bucketIntentCount, long readyQueueSize) {
        operationalMetrics.updateBucketMetrics(bucketIntentCount, readyQueueSize);
    }

    // ========== 精度档位指标 (v0.5.1) ==========

    /**
     * 按精度档位增加 Intent 创建计数
     */
    public void incrementIntentByTier(PrecisionTier tier) {
        tierMetrics.incrementIntentByTier(tier);
    }

    /**
     * 按精度档位增加到期 Intent 计数
     */
    public void incrementIntentDueByTier(PrecisionTier tier) {
        tierMetrics.incrementIntentDueByTier(tier);
    }

    public void addIntentDueByTier(PrecisionTier tier, int count) {
        tierMetrics.addIntentDueByTier(tier, count);
    }

    /**
     * 更新指定精度档位的 Bucket 大小
     */
    public void updateBucketSizeByTier(PrecisionTier tier, long size) {
        tierMetrics.updateBucketSizeByTier(tier, size);
    }

    /**
     * 记录指定精度档位的扫描耗时
     */
    public void recordScanDurationByTier(PrecisionTier tier, long durationMs) {
        tierMetrics.recordScanDurationByTier(tier, durationMs);
    }

    /**
     * 记录指定精度档位的唤醒延迟
     * 同时记录到手工分桶，供 P95/P99/P99.9 近似计算使用
     */
    public void recordWakeupLatencyByTier(PrecisionTier tier, long latencyMs) {
        tierMetrics.recordWakeupLatencyByTier(tier, latencyMs);
    }

    /**
     * 记录背压事件（v0.6.2）
     */
    public void incrementBackpressureEvent(PrecisionTier tier) {
        tierMetrics.incrementBackpressureEvent(tier);
    }

    /** 记录 directBucket 高水位降级（Phase 2 起使用；按实际档归因，非 defaultTier）。 */
    public void incrementMilliFallback(PrecisionTier tier) { tierMetrics.incrementMilliFallback(tier); }
    public void recordScannerPark(PrecisionTier tier) { tierMetrics.recordScannerPark(tier); }
    public void recordScannerWakeEarly(PrecisionTier tier) { tierMetrics.recordScannerWakeEarly(tier); }

    /**
     * 记录 dispatch 队列 offer 失败（队满）
     */
    public void incrementDispatchQueueOfferFailed(PrecisionTier tier) {
        tierMetrics.incrementDispatchQueueOfferFailed(tier);
    }

    /**
     * 记录 dispatch 队列信号量重试
     */
    public void incrementDispatchQueueRetry(PrecisionTier tier) {
        tierMetrics.incrementDispatchQueueRetry(tier);
    }

    /**
     * 记录 dispatch 队列最终放弃的 batch
     */
    public void incrementDispatchQueueAbandoned(PrecisionTier tier) {
        tierMetrics.incrementDispatchQueueAbandoned(tier);
    }

    /**
     * 更新 dispatch 队列深度
     */
    public void updateDispatchQueueSizeByTier(PrecisionTier tier, long size) {
        tierMetrics.updateDispatchQueueSizeByTier(tier, size);
    }

    /**
     * 记录 due→dispatch lag
     */
    public void recordDispatchQueueLagByTier(PrecisionTier tier, long lagMs) {
        tierMetrics.recordDispatchQueueLagByTier(tier, lagMs);
    }

    /**
     * 获取按精度档位的背压事件计数
     */
    public Map<PrecisionTier, Long> getBackpressureEventsByTier() {
        return tierMetrics.getBackpressureEventsByTier();
    }

    /**
     * 计算指定精度档位的 P95 唤醒延迟
     */
    public long calculateP95WakeupLatencyByTier(PrecisionTier tier) {
        return tierMetrics.calculateP95WakeupLatencyByTier(tier);
    }

    /**
     * 计算指定精度档位的 P99 唤醒延迟
     */
    public long calculateP99WakeupLatencyByTier(PrecisionTier tier) {
        return tierMetrics.calculateP99WakeupLatencyByTier(tier);
    }

    /**
     * 计算指定精度档位的 P99.9 唤醒延迟
     */
    public long calculateP999WakeupLatencyByTier(PrecisionTier tier) {
        return tierMetrics.calculateP999WakeupLatencyByTier(tier);
    }

    public long calculateP50WakeupLatencyByTier(PrecisionTier tier) {
        return tierMetrics.calculateP50WakeupLatencyByTier(tier);
    }

    public long calculateP75WakeupLatencyByTier(PrecisionTier tier) {
        return tierMetrics.calculateP75WakeupLatencyByTier(tier);
    }

    public long calculateP90WakeupLatencyByTier(PrecisionTier tier) {
        return tierMetrics.calculateP90WakeupLatencyByTier(tier);
    }

    public long calculateMaxWakeupLatencyByTier(PrecisionTier tier) {
        return tierMetrics.calculateMaxWakeupLatencyByTier(tier);
    }

    public long calculateMeanWakeupLatencyByTier(PrecisionTier tier) {
        return tierMetrics.calculateMeanWakeupLatencyByTier(tier);
    }

    public long getWakeupLatencySampleCountByTier(PrecisionTier tier) {
        return tierMetrics.getWakeupLatencySampleCountByTier(tier);
    }

    public long getDispatchQueueOfferFailed(PrecisionTier tier) {
        return tierMetrics.getDispatchQueueOfferFailed(tier);
    }

    public long getDispatchQueueRetry(PrecisionTier tier) {
        return tierMetrics.getDispatchQueueRetry(tier);
    }

    public long getDispatchQueueAbandoned(PrecisionTier tier) {
        return tierMetrics.getDispatchQueueAbandoned(tier);
    }

    public long getDispatchQueueSize(PrecisionTier tier) {
        return tierMetrics.getDispatchQueueSize(tier);
    }

    /**
     * 获取完整的延迟快照（所有分位数），用于压测报告展示。
     */
    public LatencySnapshot getWakeupLatencySnapshot(PrecisionTier tier) {
        return new LatencySnapshot(
            calculateP50WakeupLatencyByTier(tier),
            calculateP75WakeupLatencyByTier(tier),
            calculateP90WakeupLatencyByTier(tier),
            calculateP95WakeupLatencyByTier(tier),
            calculateP99WakeupLatencyByTier(tier),
            calculateP999WakeupLatencyByTier(tier),
            calculateMaxWakeupLatencyByTier(tier),
            calculateMeanWakeupLatencyByTier(tier),
            getWakeupLatencySampleCountByTier(tier)
        );
    }

    public record LatencySnapshot(long p50, long p75, long p90, long p95, long p99, long p999,
                                  long max, long mean, long sampleCount) {}

    /**
     * 计算指定档位的理论最大 QPS
     * 理论最大 QPS = 并发数 × (1000ms / 平均HTTP响应延迟)
     *
     * @param tier 精度档位
     * @param avgHttpLatencyMs 平均HTTP响应延迟（毫秒）
     * @return 理论最大 QPS
     */
    public double calculateTheoreticalMaxQps(PrecisionTier tier, double avgHttpLatencyMs) {
        int concurrency = tierMetrics.precisionTierCatalog().maxConcurrency(tier);
        if (avgHttpLatencyMs <= 0 || concurrency <= 0) return 0;
        return concurrency * (1000.0 / avgHttpLatencyMs);
    }

    /**
     * 计算实际资源利用率（效率）
     * 效率 = 实际 QPS / 理论最大 QPS
     *
     * @param tier 精度档位
     * @param measuredQps 实测 QPS
     * @param avgHttpLatencyMs 平均HTTP响应延迟（毫秒）
     * @return 资源利用率（0-1之间）
     */
    public double calculateEfficiency(PrecisionTier tier, double measuredQps, double avgHttpLatencyMs) {
        double theoreticalMaxQps = calculateTheoreticalMaxQps(tier, avgHttpLatencyMs);
        if (theoreticalMaxQps <= 0) return 0;
        return measuredQps / theoreticalMaxQps;
    }

    /**
     * 获取按精度档位的 Intent 创建计数
     */
    public Map<PrecisionTier, Long> getIntentCountsByTier() {
        return tierMetrics.getIntentCountsByTier();
    }

    /**
     * 获取按精度档位的 Bucket 大小
     */
    public Map<PrecisionTier, Long> getBucketSizesByTier() {
        return tierMetrics.getBucketSizesByTier();
    }

    // ========== 恢复指标 ==========

    public void recordRecovery(long durationMs, long intentsRecovered) {
        runtimeMetrics.recordRecovery(durationMs, intentsRecovered);
    }

    // ========== WAL 指标 ==========

    public void updateWalMetrics(long sizeBytes, int segmentCount, long recordCount) {
        runtimeMetrics.updateWalMetrics(sizeBytes, segmentCount, recordCount);
    }

    public void refreshWalSize() {
        runtimeMetrics.refreshWalSize();
    }

    // ========== 触发延迟 ==========

    /**
     * 记录触发延迟
     * @param latencyMs 从 trigger_time 到实际执行的时间差 (可能为负数表示提前执行)
     */
    public void recordTriggerLatency(long latencyMs) {
        latencyMetrics.recordTriggerLatency(latencyMs);
    }

    /**
     * 记录唤醒延迟 (sleep 结束 → 进入分发)
     * 这是纯粹的系统内部调度延迟，与 webhook 无关
     */
    public void recordWakeLatency(long latencyMs) {
        latencyMetrics.recordWakeLatency(latencyMs);
    }

    /**
     * 记录 webhook 执行延迟 (开始执行 → 收到响应)
     */
    public void recordWebhookLatency(long latencyMs) {
        latencyMetrics.recordWebhookLatency(latencyMs);
    }

    /**
     * 记录总延迟 (计划时间 → webhook 完成)
     * 这是用户可见的端到端延迟
     */
    public void recordTotalLatency(long latencyMs) {
        latencyMetrics.recordTotalLatency(latencyMs);
    }

    /**
     * 记录 Cohort flush 耗时（微秒）
     */
    public void recordCohortFlushDuration(long durationUs) {
        latencyMetrics.recordCohortFlushDuration(durationUs);
    }

    /**
     * 获取 Cohort flush P95 耗时（微秒）
     */
    public long getCohortFlushP95() {
        return latencyMetrics.calculateP95CohortFlushDuration();
    }

    /**
     * 计算 P95 触发延迟
     */
    public long calculateP95Latency() {
        return latencyMetrics.calculateP95Latency();
    }

    /**
     * 计算 P95 唤醒延迟 (系统内部调度精度)
     */
    public long calculateP95WakeLatency() {
        return latencyMetrics.calculateP95WakeLatency();
    }

    /**
     * 计算 P95 webhook 延迟
     */
    public long calculateP95WebhookLatency() {
        return latencyMetrics.calculateP95WebhookLatency();
    }

    /**
     * 计算 P95 总延迟 (用户可见)
     */
    public long calculateP95TotalLatency() {
        return latencyMetrics.calculateP95TotalLatency();
    }

    // ========== 导出指标 ==========

    /**
     * 导出 Prometheus 格式的指标（使用当前运行时状态）
     */
    public String exportPrometheusMetrics() {
        return exportPrometheusMetrics(getIntentStatusCounts());
    }

    /**
     * 导出 Prometheus 格式的指标
     */
    public String exportPrometheusMetrics(Map<String, Long> intentStats) {
        StringBuilder sb = new StringBuilder();
        operationalMetrics.appendPrometheusMetrics(intentStats, sb);
        latencyMetrics.appendPrometheusMetrics(sb);
        runtimeMetrics.appendPrometheusMetrics(sb);
        tierMetrics.appendPrometheusMetrics(sb);

        return sb.toString();
    }

    // ========== 获取器 (用于告警检查) ==========

    public long getP95LatencyMs() {
        return calculateP95Latency();
    }

    public double getWebhookTimeoutRate() {
        return operationalMetrics.getWebhookTimeoutRate();
    }

    public long getWalSizeBytes() {
        return runtimeMetrics.getWalSizeBytes();
    }

    public long getRecoveryDurationMs() {
        return runtimeMetrics.getRecoveryDurationMs();
    }

    public long getSchedulerMaxPendingIntents() {
        return runtimeMetrics.getSchedulerMaxPendingIntents();
    }

    // ========== 系统状态更新 ==========

    public void updatePendingIntents(long count) {
        runtimeMetrics.updatePendingIntents(count);
    }

    public void updateIntentStatus(String status, long count) {
        runtimeMetrics.updateIntentStatus(status, count);
    }

    public long getPendingIntents() {
        return runtimeMetrics.getPendingIntents();
    }

    public Map<String, Long> getIntentStatusCounts() {
        return runtimeMetrics.getIntentStatusCounts();
    }

    // ========== Intent 生命周期计数器 getter ==========

    public long getIntentsCreatedTotal() {
        return operationalMetrics.getIntentsCreatedTotal();
    }

    public long getIntentsAckSuccessTotal() {
        return operationalMetrics.getIntentsAckSuccessTotal();
    }

    public long getIntentsCancelledTotal() {
        return operationalMetrics.getIntentsCancelledTotal();
    }

    public long getIntentsExpiredTotal() {
        return operationalMetrics.getIntentsExpiredTotal();
    }

    public long getIntentsDeadLetterTotal() {
        return operationalMetrics.getIntentsDeadLetterTotal();
    }

    public long getRecoveryOverdueTotal() {
        return operationalMetrics.getRecoveryOverdueTotal();
    }

    public void resetRuntimeIntentMetrics() {
        runtimeMetrics.resetIntentState();
    }
}
