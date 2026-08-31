package com.loomq.application.scheduler;

import com.loomq.common.MetricsCollector;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.PrecisionTier;
import java.util.concurrent.ConcurrentHashMap;

/** due→dispatch lag 追踪(key=intentId, value=enqueueTimeNanos);扫描器登记、消费管线上报、结算路径清理。 */
final class DispatchLagTracker {

    private final ConcurrentHashMap<String, Long> enqueueTimeNanos = new ConcurrentHashMap<>();

    void record(String intentId, long enqueueNanos) {
        enqueueTimeNanos.put(intentId, enqueueNanos);
    }

    /** 上报并清除该 intent 的 lag;无登记条目则 no-op。 */
    void report(Intent intent, PrecisionTier tier, MetricsCollector metrics) {
        Long enqueueNanos = enqueueTimeNanos.remove(intent.getIntentId());
        if (enqueueNanos != null) {
            long lagMs = (System.nanoTime() - enqueueNanos) / 1_000_000;
            metrics.recordDispatchQueueLagByTier(tier, lagMs);
        }
    }

    void clear(String intentId) {
        enqueueTimeNanos.remove(intentId);
    }
}
