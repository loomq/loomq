package com.loomq.common;

import com.loomq.domain.intent.PrecisionTier;
import com.loomq.domain.intent.PrecisionTierCatalog;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 精度档位指标注册表。
 *
 * 负责管理 tier 维度的计数、延迟分布和 Prometheus 导出。
 */
final class PrecisionTierMetricsRegistry {

    // Microsecond buckets: 0, 10, 25, 50, 100, 500, 1ms, 2.5ms, 5ms, 10ms, 25ms, 50ms, 100ms, 250ms, 500ms, 1s
    private static final int[] LATENCY_BOUNDS = {0, 10, 25, 50, 100, 500, 1000, 2500, 5000, 10000, 25000, 50000, 100000, 250000, 500000, 1000000};

    // due→dispatch lag 使用毫秒边界(与导出指标名 loomq_dispatch_queue_lag_ms_p95 一致),
    // 区别于 wakeup 延迟的微秒边界(LATENCY_BOUNDS)。调度器 recordDispatchQueueLag 传入毫秒。
    private static final int[] LAG_BOUNDS_MS = {0, 1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000};

    private final PrecisionTierCatalog precisionTierCatalog;
    private final Map<PrecisionTier, AtomicLong> intentByTier = new EnumMap<>(PrecisionTier.class);
    private final Map<PrecisionTier, AtomicLong> intentDueByTier = new EnumMap<>(PrecisionTier.class);
    private final Map<PrecisionTier, AtomicLong> bucketSizeByTier = new EnumMap<>(PrecisionTier.class);
    private final Map<PrecisionTier, AtomicLong> scanDurationSamplesByTier = new EnumMap<>(PrecisionTier.class);
    private final Map<PrecisionTier, AtomicLong> scanCountByTier = new EnumMap<>(PrecisionTier.class);
    private final Map<PrecisionTier, AtomicLong> backpressureEventsByTier = new EnumMap<>(PrecisionTier.class);
    private final Map<PrecisionTier, Histogram> wakeupLatencyByTier = new EnumMap<>(PrecisionTier.class);

    // 三级漏斗：offer_failed → backpressure_events → abandoned
    private final Map<PrecisionTier, AtomicLong> dispatchQueueOfferFailedByTier = new EnumMap<>(PrecisionTier.class);
    private final Map<PrecisionTier, AtomicLong> dispatchQueueRetryByTier = new EnumMap<>(PrecisionTier.class);
    private final Map<PrecisionTier, AtomicLong> dispatchQueueAbandonedByTier = new EnumMap<>(PrecisionTier.class);

    // 队列深度 gauge
    private final Map<PrecisionTier, AtomicLong> dispatchQueueSizeByTier = new EnumMap<>(PrecisionTier.class);

    // MILLI directBucket 高水位降级（Phase 2 起使用）+ adaptive 扫描器 park/早醒
    private final Map<PrecisionTier, AtomicLong> milliFallbackByTier = new EnumMap<>(PrecisionTier.class);
    private final Map<PrecisionTier, AtomicLong> scannerParkByTier = new EnumMap<>(PrecisionTier.class);
    private final Map<PrecisionTier, AtomicLong> scannerWakeEarlyByTier = new EnumMap<>(PrecisionTier.class);

    // due→dispatch lag histogram
    private final Map<PrecisionTier, Histogram> dispatchQueueLagByTier = new EnumMap<>(PrecisionTier.class);

    PrecisionTierMetricsRegistry(PrecisionTierCatalog precisionTierCatalog) {
        this.precisionTierCatalog = precisionTierCatalog;

        for (PrecisionTier tier : precisionTierCatalog.supportedTiers()) {
            intentByTier.put(tier, new AtomicLong(0));
            intentDueByTier.put(tier, new AtomicLong(0));
            bucketSizeByTier.put(tier, new AtomicLong(0));
            scanDurationSamplesByTier.put(tier, new AtomicLong(0));
            scanCountByTier.put(tier, new AtomicLong(0));
            backpressureEventsByTier.put(tier, new AtomicLong(0));
            wakeupLatencyByTier.put(tier, new Histogram(LATENCY_BOUNDS));

            dispatchQueueOfferFailedByTier.put(tier, new AtomicLong(0));
            dispatchQueueRetryByTier.put(tier, new AtomicLong(0));
            dispatchQueueAbandonedByTier.put(tier, new AtomicLong(0));
            dispatchQueueSizeByTier.put(tier, new AtomicLong(0));
            milliFallbackByTier.put(tier, new AtomicLong(0));
            scannerParkByTier.put(tier, new AtomicLong(0));
            scannerWakeEarlyByTier.put(tier, new AtomicLong(0));
            dispatchQueueLagByTier.put(tier, new Histogram(LAG_BOUNDS_MS));
        }
    }

    PrecisionTierCatalog precisionTierCatalog() {
        return precisionTierCatalog;
    }

    void incrementIntentByTier(PrecisionTier tier) {
        resolveCounter(intentByTier, tier).incrementAndGet();
    }

    void incrementIntentDueByTier(PrecisionTier tier) {
        resolveCounter(intentDueByTier, tier).incrementAndGet();
    }

    void addIntentDueByTier(PrecisionTier tier, int count) {
        resolveCounter(intentDueByTier, tier).addAndGet(count);
    }

    void updateBucketSizeByTier(PrecisionTier tier, long size) {
        resolveCounter(bucketSizeByTier, tier).set(size);
    }

    void recordScanDurationByTier(PrecisionTier tier, long durationMs) {
        resolveCounter(scanDurationSamplesByTier, tier).addAndGet(durationMs);
        resolveCounter(scanCountByTier, tier).incrementAndGet();
    }

    /** 扫描次数（scanAndDispatch 执行次数，测试/诊断用）。 */
    long getScanSampleCount(PrecisionTier tier) {
        return resolveCounter(scanCountByTier, tier).get();
    }

    void recordWakeupLatencyByTier(PrecisionTier tier, long latencyUs) {
        wakeupLatencyByTier.get(resolveTier(tier)).record(latencyUs);
    }

    void incrementBackpressureEvent(PrecisionTier tier) {
        resolveCounter(backpressureEventsByTier, tier).incrementAndGet();
    }

    void incrementMilliFallback(PrecisionTier tier) { resolveCounter(milliFallbackByTier, tier).incrementAndGet(); }
    long getMilliFallback(PrecisionTier tier) { return resolveCounter(milliFallbackByTier, tier).get(); }
    void recordScannerPark(PrecisionTier tier) { resolveCounter(scannerParkByTier, tier).incrementAndGet(); }
    void recordScannerWakeEarly(PrecisionTier tier) { resolveCounter(scannerWakeEarlyByTier, tier).incrementAndGet(); }

    void incrementDispatchQueueOfferFailed(PrecisionTier tier) {
        resolveCounter(dispatchQueueOfferFailedByTier, tier).incrementAndGet();
    }

    void incrementDispatchQueueRetry(PrecisionTier tier) {
        resolveCounter(dispatchQueueRetryByTier, tier).incrementAndGet();
    }

    void incrementDispatchQueueAbandoned(PrecisionTier tier) {
        resolveCounter(dispatchQueueAbandonedByTier, tier).incrementAndGet();
    }

    void updateDispatchQueueSizeByTier(PrecisionTier tier, long size) {
        resolveCounter(dispatchQueueSizeByTier, tier).set(size);
    }

    void recordDispatchQueueLagByTier(PrecisionTier tier, long lagMs) {
        dispatchQueueLagByTier.get(resolveTier(tier)).record(lagMs);
    }

    Map<PrecisionTier, Long> getBackpressureEventsByTier() {
        Map<PrecisionTier, Long> result = new EnumMap<>(PrecisionTier.class);
        backpressureEventsByTier.forEach((tier, count) -> result.put(tier, count.get()));
        return result;
    }

    long getDispatchQueueOfferFailed(PrecisionTier tier) {
        return resolveCounter(dispatchQueueOfferFailedByTier, tier).get();
    }

    long getDispatchQueueRetry(PrecisionTier tier) {
        return resolveCounter(dispatchQueueRetryByTier, tier).get();
    }

    long getDispatchQueueAbandoned(PrecisionTier tier) {
        return resolveCounter(dispatchQueueAbandonedByTier, tier).get();
    }

    long getDispatchQueueSize(PrecisionTier tier) {
        return resolveCounter(dispatchQueueSizeByTier, tier).get();
    }

    long calculateP95WakeupLatencyByTier(PrecisionTier tier) {
        return percentileForTier(tier, 0.95);
    }

    long calculateP50WakeupLatencyByTier(PrecisionTier tier) {
        return percentileForTier(tier, 0.50);
    }

    long calculateP75WakeupLatencyByTier(PrecisionTier tier) {
        return percentileForTier(tier, 0.75);
    }

    long calculateP90WakeupLatencyByTier(PrecisionTier tier) {
        return percentileForTier(tier, 0.90);
    }

    long calculateP99WakeupLatencyByTier(PrecisionTier tier) {
        return percentileForTier(tier, 0.99);
    }

    long calculateP999WakeupLatencyByTier(PrecisionTier tier) {
        return percentileForTier(tier, 0.999);
    }

    long calculateMaxWakeupLatencyByTier(PrecisionTier tier) {
        return wakeupLatencyByTier.get(resolveTier(tier)).max();
    }

    long calculateMeanWakeupLatencyByTier(PrecisionTier tier) {
        return wakeupLatencyByTier.get(resolveTier(tier)).mean();
    }

    long getWakeupLatencySampleCountByTier(PrecisionTier tier) {
        return wakeupLatencyByTier.get(resolveTier(tier)).sampleCount();
    }

    private long percentileForTier(PrecisionTier tier, double p) {
        return wakeupLatencyByTier.get(resolveTier(tier)).percentile(p);
    }

    /** 由本注册表 appendPrometheusMetrics 导出 due→dispatch lag P95(毫秒;诊断/测试读数入口)。 */
    long getDispatchQueueLagP95(PrecisionTier tier) {
        return dispatchQueueLagByTier.get(resolveTier(tier)).percentile(0.95);
    }

    Map<PrecisionTier, Long> getIntentCountsByTier() {
        Map<PrecisionTier, Long> result = new EnumMap<>(PrecisionTier.class);
        intentByTier.forEach((tier, count) -> result.put(tier, count.get()));
        return result;
    }

    Map<PrecisionTier, Long> getBucketSizesByTier() {
        Map<PrecisionTier, Long> result = new EnumMap<>(PrecisionTier.class);
        bucketSizeByTier.forEach((tier, count) -> result.put(tier, count.get()));
        return result;
    }

    void appendPrometheusMetrics(StringBuilder sb) {
        sb.append("# HELP loomq_intent_total Total intents created by precision tier\n");
        sb.append("# TYPE loomq_intent_total counter\n");
        for (PrecisionTier tier : precisionTierCatalog.supportedTiers()) {
            sb.append("loomq_intent_total{precision_tier=\"")
              .append(tier.name().toLowerCase())
              .append("\"} ")
              .append(resolveCounter(intentByTier, tier).get())
              .append("\n");
        }
        sb.append("\n");

        sb.append("# HELP loomq_intent_due_total Total intents due by precision tier\n");
        sb.append("# TYPE loomq_intent_due_total counter\n");
        for (PrecisionTier tier : precisionTierCatalog.supportedTiers()) {
            sb.append("loomq_intent_due_total{precision_tier=\"")
              .append(tier.name().toLowerCase())
              .append("\"} ")
              .append(resolveCounter(intentDueByTier, tier).get())
              .append("\n");
        }
        sb.append("\n");

        sb.append("# HELP loomq_scheduler_bucket_size Current bucket size by precision tier\n");
        sb.append("# TYPE loomq_scheduler_bucket_size gauge\n");
        for (PrecisionTier tier : precisionTierCatalog.supportedTiers()) {
            sb.append("loomq_scheduler_bucket_size{precision_tier=\"")
              .append(tier.name().toLowerCase())
              .append("\"} ")
              .append(resolveCounter(bucketSizeByTier, tier).get())
              .append("\n");
        }
        sb.append("\n");

        sb.append("# HELP loomq_scheduler_wakeup_latency_us_p95 P95 wakeup latency (microseconds) by precision tier\n");
        sb.append("# TYPE loomq_scheduler_wakeup_latency_us_p95 gauge\n");
        for (PrecisionTier tier : precisionTierCatalog.supportedTiers()) {
            sb.append("loomq_scheduler_wakeup_latency_us_p95{precision_tier=\"")
              .append(tier.name().toLowerCase())
              .append("\"} ")
              .append(calculateP95WakeupLatencyByTier(tier))
              .append("\n");
        }
        sb.append("\n");

        sb.append("# HELP loomq_scheduler_wakeup_latency_us_p99 P99 wakeup latency (microseconds) by precision tier\n");
        sb.append("# TYPE loomq_scheduler_wakeup_latency_us_p99 gauge\n");
        for (PrecisionTier tier : precisionTierCatalog.supportedTiers()) {
            sb.append("loomq_scheduler_wakeup_latency_us_p99{precision_tier=\"")
              .append(tier.name().toLowerCase())
              .append("\"} ")
              .append(calculateP99WakeupLatencyByTier(tier))
              .append("\n");
        }
        sb.append("\n");

        sb.append("# HELP loomq_scheduler_wakeup_latency_us_p999 P99.9 wakeup latency (microseconds) by precision tier\n");
        sb.append("# TYPE loomq_scheduler_wakeup_latency_us_p999 gauge\n");
        for (PrecisionTier tier : precisionTierCatalog.supportedTiers()) {
            sb.append("loomq_scheduler_wakeup_latency_us_p999{precision_tier=\"")
              .append(tier.name().toLowerCase())
              .append("\"} ")
              .append(calculateP999WakeupLatencyByTier(tier))
              .append("\n");
        }
        sb.append("\n");

        // 三级漏斗：backpressure 指标
        sb.append("# HELP loomq_dispatch_queue_offer_failed_total Dispatch queue offer failures (queue full) by precision tier\n");
        sb.append("# TYPE loomq_dispatch_queue_offer_failed_total counter\n");
        for (PrecisionTier tier : precisionTierCatalog.supportedTiers()) {
            sb.append("loomq_dispatch_queue_offer_failed_total{precision_tier=\"")
              .append(tier.name().toLowerCase())
              .append("\"} ")
              .append(resolveCounter(dispatchQueueOfferFailedByTier, tier).get())
              .append("\n");
        }
        sb.append("\n");

        sb.append("# HELP loomq_backpressure_events_total Backpressure events by precision tier\n");
        sb.append("# TYPE loomq_backpressure_events_total counter\n");
        for (PrecisionTier tier : precisionTierCatalog.supportedTiers()) {
            sb.append("loomq_backpressure_events_total{precision_tier=\"")
              .append(tier.name().toLowerCase())
              .append("\"} ")
              .append(resolveCounter(backpressureEventsByTier, tier).get())
              .append("\n");
        }
        sb.append("\n");

        sb.append("# HELP loomq_dispatch_queue_retry_total Semaphore acquisition retries by precision tier\n");
        sb.append("# TYPE loomq_dispatch_queue_retry_total counter\n");
        for (PrecisionTier tier : precisionTierCatalog.supportedTiers()) {
            sb.append("loomq_dispatch_queue_retry_total{precision_tier=\"")
              .append(tier.name().toLowerCase())
              .append("\"} ")
              .append(resolveCounter(dispatchQueueRetryByTier, tier).get())
              .append("\n");
        }
        sb.append("\n");

        sb.append("# HELP loomq_dispatch_queue_abandoned_total Batches abandoned after max retries by precision tier\n");
        sb.append("# TYPE loomq_dispatch_queue_abandoned_total counter\n");
        for (PrecisionTier tier : precisionTierCatalog.supportedTiers()) {
            sb.append("loomq_dispatch_queue_abandoned_total{precision_tier=\"")
              .append(tier.name().toLowerCase())
              .append("\"} ")
              .append(resolveCounter(dispatchQueueAbandonedByTier, tier).get())
              .append("\n");
        }
        sb.append("\n");

        // dispatch 队列深度
        sb.append("# HELP loomq_dispatch_queue_size Current dispatch queue size by precision tier\n");
        sb.append("# TYPE loomq_dispatch_queue_size gauge\n");
        for (PrecisionTier tier : precisionTierCatalog.supportedTiers()) {
            sb.append("loomq_dispatch_queue_size{precision_tier=\"")
              .append(tier.name().toLowerCase())
              .append("\"} ")
              .append(resolveCounter(dispatchQueueSizeByTier, tier).get())
              .append("\n");
        }
        sb.append("\n");

        // due→dispatch lag
        sb.append("# HELP loomq_dispatch_queue_lag_ms_p95 P95 due-to-dispatch queue lag by precision tier\n");
        sb.append("# TYPE loomq_dispatch_queue_lag_ms_p95 gauge\n");
        for (PrecisionTier tier : precisionTierCatalog.supportedTiers()) {
            sb.append("loomq_dispatch_queue_lag_ms_p95{precision_tier=\"")
              .append(tier.name().toLowerCase())
              .append("\"} ")
              .append(getDispatchQueueLagP95(tier))
              .append("\n");
        }
        sb.append("\n");
    }

    private PrecisionTier resolveTier(PrecisionTier tier) {
        return tier != null && precisionTierCatalog.supportedTiers().contains(tier)
            ? tier
            : precisionTierCatalog.defaultTier();
    }

    private AtomicLong resolveCounter(Map<PrecisionTier, AtomicLong> counters, PrecisionTier tier) {
        PrecisionTier resolvedTier = resolveTier(tier);
        AtomicLong counter = counters.get(resolvedTier);
        if (counter == null) {
            counter = counters.get(precisionTierCatalog.defaultTier());
        }
        return counter;
    }
}
