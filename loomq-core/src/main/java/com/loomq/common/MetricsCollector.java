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

    // 唤醒/webhook/cohort flush 延迟指标
    private final LatencyMetricsRegistry latencyMetrics;

    // 运行时指标
    private final RuntimeMetricsRegistry runtimeMetrics;

    public MetricsCollector() {
        this.operationalMetrics = new OperationalMetricsRegistry();
        this.tierMetrics = new PrecisionTierMetricsRegistry(com.loomq.domain.intent.PrecisionTierCatalog.defaultCatalog());
        this.latencyMetrics = new LatencyMetricsRegistry();
        this.runtimeMetrics = new RuntimeMetricsRegistry();
    }

    // ========== 计数器更新 ==========

    public void incrementIntentsCreated() {
        operationalMetrics.incrementIntentsCreated();
    }

    public void incrementIntentsCancelled() {
        operationalMetrics.incrementIntentsCancelled();
    }

    public void incrementRecoveryOverdue() {
        operationalMetrics.incrementRecoveryOverdue();
    }

    public long getIntentsCancelledTotal() {
        return operationalMetrics.getIntentsCancelledTotal();
    }

    public long getRecoveryOverdueTotal() {
        return operationalMetrics.getRecoveryOverdueTotal();
    }

    // ========== 精度档位指标 (v0.5.1) ==========

    /**
     * 按精度档位增加 Intent 创建计数
     */
    public void incrementIntentByTier(PrecisionTier tier) {
        tierMetrics.incrementIntentByTier(tier);
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

    /** 指定档位 scanAndDispatch 累计执行次数（诊断/测试用）。 */
    public long getScanSampleCountByTier(PrecisionTier tier) {
        return tierMetrics.getScanSampleCount(tier);
    }

    /**
     * 记录指定精度档位的唤醒延迟
     * 同时记录到手工分桶，供 P95/P99/P99.9 近似计算使用
     */
    public void recordWakeupLatencyByTier(PrecisionTier tier, long latencyUs) {
        tierMetrics.recordWakeupLatencyByTier(tier, latencyUs);
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
     * 计算指定精度档位的 P50 唤醒延迟
     */
    public long calculateP50WakeupLatencyByTier(PrecisionTier tier) {
        return tierMetrics.calculateP50WakeupLatencyByTier(tier);
    }

    /**
     * 计算指定精度档位的 P75 唤醒延迟
     */
    public long calculateP75WakeupLatencyByTier(PrecisionTier tier) {
        return tierMetrics.calculateP75WakeupLatencyByTier(tier);
    }

    /**
     * 计算指定精度档位的 P90 唤醒延迟
     */
    public long calculateP90WakeupLatencyByTier(PrecisionTier tier) {
        return tierMetrics.calculateP90WakeupLatencyByTier(tier);
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
     * 获取按精度档位的 Intent 创建计数
     */
    public Map<PrecisionTier, Long> getIntentCountsByTier() {
        return tierMetrics.getIntentCountsByTier();
    }

    // ========== 延迟记录 ==========

    /**
     * 记录 webhook 执行延迟 (开始执行 → 收到响应)
     */
    public void recordWebhookLatency(long latencyMs) {
        latencyMetrics.recordWebhookLatency(latencyMs);
    }

    /**
     * 记录 Cohort flush 耗时（微秒）
     */
    public void recordCohortFlushDuration(long durationUs) {
        latencyMetrics.recordCohortFlushDuration(durationUs);
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

    public Map<String, Long> getIntentStatusCounts() {
        return runtimeMetrics.getIntentStatusCounts();
    }
}
