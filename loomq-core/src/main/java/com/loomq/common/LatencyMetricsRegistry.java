package com.loomq.common;

/**
 * 通用延迟指标注册表。
 *
 * <p>round 12 起仅承载两条活链路(finalize 结算耗时、cohort flush 耗时),直方图/分位
 * 实现统一走 {@link Histogram};trigger/wake/total 三组零调用直方图已删除。
 * 名为 webhook 的历史直方图更名 finalize——其唯一写点是结算任务的 finally
 * (SettlementEngine),统计的是单次结算全程耗时(状态转换 + awaitCommit 等待),非 webhook 执行。</p>
 */
final class LatencyMetricsRegistry {

    // finalize 结算全程耗时,毫秒;写点 SettlementEngine.runFinalizeTask finally
    private static final int[] FINALIZE_BOUNDS = {0, 1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000};
    private final Histogram finalizeDuration = new Histogram(FINALIZE_BOUNDS);

    // Cohort flush 耗时(微秒);写点 CohortManager wakeLoop flush
    private static final int[] COHORT_FLUSH_BOUNDS = {0, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000};
    private final Histogram cohortFlushDuration = new Histogram(COHORT_FLUSH_BOUNDS);

    void recordFinalizeDuration(long durationMs) {
        finalizeDuration.record(durationMs);
    }

    void recordCohortFlushDuration(long durationUs) {
        cohortFlushDuration.record(durationUs);
    }

    long calculateP95FinalizeDuration() {
        return finalizeDuration.percentile(0.95);
    }

    long calculateP95CohortFlushDuration() {
        return cohortFlushDuration.percentile(0.95);
    }

    void appendPrometheusMetrics(StringBuilder sb) {
        sb.append("# HELP loomq_finalize_duration_ms_p95 P95 finalize duration in milliseconds (state transition + durable persist wait, per settlement task)\n");
        sb.append("# TYPE loomq_finalize_duration_ms_p95 gauge\n");
        sb.append(formatMetric("loomq_finalize_duration_ms_p95", calculateP95FinalizeDuration()));
        sb.append("\n");

        sb.append("# HELP loomq_finalize_duration_samples Total finalize duration samples\n");
        sb.append("# TYPE loomq_finalize_duration_samples counter\n");
        sb.append(formatMetric("loomq_finalize_duration_samples", finalizeDuration.sampleCount()));
        sb.append("\n");

        sb.append("# HELP loomq_cohort_flush_duration_us_p95 P95 cohort flush duration in microseconds\n");
        sb.append("# TYPE loomq_cohort_flush_duration_us_p95 gauge\n");
        sb.append(formatMetric("loomq_cohort_flush_duration_us_p95", calculateP95CohortFlushDuration()));
        sb.append("\n");

        sb.append("# HELP loomq_cohort_flush_samples Total cohort flush samples\n");
        sb.append("# TYPE loomq_cohort_flush_samples counter\n");
        sb.append(formatMetric("loomq_cohort_flush_samples", cohortFlushDuration.sampleCount()));
        sb.append("\n");
    }

    private String formatMetric(String name, long value) {
        return name + " " + value + "\n";
    }
}
