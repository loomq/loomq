package com.loomq.common;

import com.loomq.LoomqEngine;
import com.loomq.application.scheduler.PrecisionScheduler;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.metrics.LoomQMetrics;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 生成叙事式健康响应。
 *
 * <p>从二元 UP/DOWN 升级为"这台机器运行正常吗？准时吗？"
 * 包含自然语言叙事、结构化 vitals、以及异常检测。</p>
 */
public final class HealthNarrator {

    private HealthNarrator() {}

    public static Map<String, Object> narrate(LoomqEngine engine) {
        try {
            return doNarrate(engine);
        } catch (Exception e) {
            Map<String, Object> error = new LinkedHashMap<>();
            error.put("status", "ERROR");
            error.put("narrative", "Health check failed: " + e.getMessage());
            return error;
        }
    }

    private static Map<String, Object> doNarrate(LoomqEngine engine) {
        LoomQMetrics.MetricsSnapshot metrics = new LoomQMetrics().snapshot(engine.getMetricsCollector());
        PrecisionScheduler scheduler = engine.getScheduler();
        var backpressure = scheduler.getBackpressureStatus();

        Map<String, Object> root = new LinkedHashMap<>();

        boolean walHealthy = metrics.walHealthy();
        boolean anyBackpressure = backpressure.values().stream()
            .anyMatch(info -> info.underBackpressure());
        boolean overallUp = walHealthy && !anyBackpressure;

        root.put("status", overallUp ? "UP" : "WARNING");
        root.put("narrative", buildNarrative(engine, metrics, backpressure));
        root.put("vitals", buildVitals(engine, metrics, backpressure));

        List<String> anomalies = detectAnomalies(engine, metrics, backpressure);
        if (!anomalies.isEmpty()) {
            root.put("anomalies", anomalies);
        }

        return root;
    }

    private static String buildNarrative(LoomqEngine engine, LoomQMetrics.MetricsSnapshot metrics,
                                          Map<PrecisionTier, PrecisionScheduler.BackpressureInfo> backpressure) {
        StringBuilder sb = new StringBuilder();

        long totalPending = engine.getStats().pendingCount();
        boolean anyBackpressure = backpressure.values().stream()
            .anyMatch(info -> info.underBackpressure());

        if (!anyBackpressure && metrics.walHealthy()) {
            sb.append("All 5 precision tiers operating normally. ");
        } else if (anyBackpressure) {
            List<String> pressureTiers = backpressure.entrySet().stream()
                .filter(e -> e.getValue().underBackpressure())
                .map(e -> e.getKey().name())
                .toList();
            sb.append("WARNING: Backpressure on ").append(String.join(", ", pressureTiers)).append(". ");
        }
        if (!metrics.walHealthy()) {
            sb.append("WAL health issue detected. ");
        }

        sb.append(totalPending).append(" intents pending. ");

        long deliverySuccess = metrics.intentsCompleted();
        long deliveryTotal = metrics.intentsCompleted() + metrics.deliveriesFailed();
        if (deliveryTotal > 0) {
            double successRate = (deliverySuccess * 100.0) / deliveryTotal;
            sb.append(String.format("Delivery success rate: %.1f%%. ", successRate));
        }

        if (metrics.walHealthy()) {
            sb.append("WAL is healthy.");
        }

        return sb.toString().trim();
    }

    private static Map<String, Object> buildVitals(LoomqEngine engine, LoomQMetrics.MetricsSnapshot metrics,
                                                    Map<PrecisionTier, PrecisionScheduler.BackpressureInfo> backpressure) {
        Map<String, Object> vitals = new LinkedHashMap<>();

        // Scheduler vitals
        Map<String, Object> schedulerVitals = new LinkedHashMap<>();
        schedulerVitals.put("totalPending", engine.getStats().pendingCount());
        long totalCapacity = backpressure.values().stream()
            .mapToLong(PrecisionScheduler.BackpressureInfo::maxConcurrency)
            .sum();
        long totalActive = backpressure.values().stream()
            .mapToLong(PrecisionScheduler.BackpressureInfo::activeDispatches)
            .sum();
        double capacityUsed = totalCapacity > 0 ? (totalActive * 100.0) / totalCapacity : 0;
        schedulerVitals.put("capacityUsed", String.format("%.2f%%", capacityUsed));

        List<String> pressureTiers = backpressure.entrySet().stream()
            .filter(e -> e.getValue().underBackpressure())
            .map(e -> e.getKey().name())
            .toList();
        schedulerVitals.put("backpressure", pressureTiers.isEmpty() ? "none" : pressureTiers);
        vitals.put("scheduler", schedulerVitals);

        // Delivery vitals
        Map<String, Object> deliveryVitals = new LinkedHashMap<>();
        long deliveryTotal = metrics.intentsCompleted() + metrics.deliveriesFailed();
        double successRate = deliveryTotal > 0 ? (metrics.intentsCompleted() * 100.0) / deliveryTotal : 100.0;
        deliveryVitals.put("successRate", String.format("%.1f%%", successRate));
        deliveryVitals.put("activeDispatches", totalActive);
        vitals.put("delivery", deliveryVitals);

        // Durability vitals
        Map<String, Object> durabilityVitals = new LinkedHashMap<>();
        durabilityVitals.put("walHealth", metrics.walHealthy() ? "healthy" : "unhealthy");
        if (metrics.avgWalFlushLatencyMs() > 0) {
            durabilityVitals.put("avgFlushLatencyMs", String.format("%.1f", metrics.avgWalFlushLatencyMs()));
        }
        vitals.put("durability", durabilityVitals);

        return vitals;
    }

    private static List<String> detectAnomalies(LoomqEngine engine, LoomQMetrics.MetricsSnapshot metrics,
                                                 Map<PrecisionTier, PrecisionScheduler.BackpressureInfo> backpressure) {
        List<String> anomalies = new ArrayList<>();

        // Check for tiers with very high utilization
        for (var entry : backpressure.entrySet()) {
            PrecisionScheduler.BackpressureInfo info = entry.getValue();
            if (info.utilizationPct() > 90.0) {
                anomalies.add(String.format("%s tier at %.0f%% utilization — approaching backpressure",
                    entry.getKey().name(), info.utilizationPct()));
            }
        }

        // Check for high WAL flush latency
        if (metrics.maxWalFlushLatencyMs() > 100) {
            anomalies.add(String.format("WAL flush latency spike: max %.0fms", metrics.maxWalFlushLatencyMs()));
        }

        // Check for high dead letter rate
        long total = metrics.intentsCompleted() + metrics.intentsDeadLettered();
        if (total > 100 && metrics.intentsDeadLettered() > 0) {
            double deadLetterRate = (metrics.intentsDeadLettered() * 100.0) / total;
            if (deadLetterRate > 5.0) {
                anomalies.add(String.format("Dead letter rate %.1f%% is above 5%% threshold", deadLetterRate));
            }
        }

        // Check for high borrow rate
        PrecisionScheduler.BorrowStats borrowStats = engine.getScheduler().getBorrowStats();
        if (borrowStats.borrowRate() > 10.0) {
            anomalies.add(String.format("Cross-tier borrow rate %.1f%% — load may be imbalanced", borrowStats.borrowRate()));
        }

        return anomalies;
    }
}
