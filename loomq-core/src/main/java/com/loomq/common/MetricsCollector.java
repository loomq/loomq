package com.loomq.common;

import com.loomq.domain.intent.PrecisionTier;
import com.loomq.domain.intent.PrecisionTierCatalog;
import java.util.Map;

/**
 * 指标采集器
 * 收集并暴露 Prometheus 格式的指标
 */
public class MetricsCollector {

    private final OperationalMetricsRegistry operationalMetrics;

    private final PrecisionTierMetricsRegistry tierMetrics;

    // finalize/cohort flush 延迟指标
    private final LatencyMetricsRegistry latencyMetrics;

    public MetricsCollector() {
        this(null);
    }

    /**
     * catalog 感知构造:tier 维度注册表按注入目录初始化,自定义档不再静默归并 defaultTier。
     * null 等价于 {@link PrecisionTierCatalog#defaultCatalog()}(无参构造路径)。
     * 经 {@code LoomqEngine.Builder.metricsCollector(...)} 注入的自定义 MC 由注入方自行
     * 负责其 catalog 一致性——内核不改动注入对象。
     */
    public MetricsCollector(PrecisionTierCatalog catalog) {
        this.operationalMetrics = new OperationalMetricsRegistry();
        this.tierMetrics = new PrecisionTierMetricsRegistry(
            catalog != null ? catalog : PrecisionTierCatalog.defaultCatalog());
        this.latencyMetrics = new LatencyMetricsRegistry();
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

    // ========== 调度故障收口计数(round 12) ==========

    /** 记录一次状态持久化失败(I6 容错吞掉,不阻塞调度;写点 StatePersistence)。 */
    public void incrementPersistFailures() {
        operationalMetrics.incrementPersistFailures();
    }

    /** 记录一次结算任务抛异常(finalize 异常被吞,onDelivered 可能不触发;写点 SettlementEngine)。 */
    public void incrementFinalizeTaskExceptions() {
        operationalMetrics.incrementFinalizeTaskExceptions();
    }

    /** 记录一次 CohortManager wakeLoop 异常(诊断计数,循环保活空转;写点 CohortManager)。 */
    public void incrementWakeLoopErrors() {
        operationalMetrics.incrementWakeLoopErrors();
    }

    public long getPersistFailuresTotal() {
        return operationalMetrics.getPersistFailuresTotal();
    }

    public long getFinalizeTaskExceptionsTotal() {
        return operationalMetrics.getFinalizeTaskExceptionsTotal();
    }

    public long getWakeLoopErrorsTotal() {
        return operationalMetrics.getWakeLoopErrorsTotal();
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
     * 记录单次结算任务全程耗时(毫秒;状态转换 + awaitCommit 等待,非 webhook 执行)。
     */
    public void recordFinalizeDuration(long durationMs) {
        latencyMetrics.recordFinalizeDuration(durationMs);
    }

    /**
     * 记录 Cohort flush 耗时（微秒）
     */
    public void recordCohortFlushDuration(long durationUs) {
        latencyMetrics.recordCohortFlushDuration(durationUs);
    }

    // ========== 导出指标 ==========

    /**
     * 导出 Prometheus 格式的指标(OMR → LMR → PTMR 三段拼接)。
     */
    public String exportPrometheusMetrics() {
        StringBuilder sb = new StringBuilder();
        operationalMetrics.appendPrometheusMetrics(sb);
        latencyMetrics.appendPrometheusMetrics(sb);
        tierMetrics.appendPrometheusMetrics(sb);

        return sb.toString();
    }
}
