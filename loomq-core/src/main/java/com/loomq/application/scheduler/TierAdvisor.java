package com.loomq.application.scheduler;

import com.loomq.domain.intent.PrecisionTier;
import com.loomq.domain.intent.PrecisionTierCatalog;
import com.loomq.domain.intent.Reliability;
import java.util.Comparator;

/**
 * Maps SLO declarations to precision tier recommendations.
 *
 * Uses a 2× safety margin: the tier's precision window must be at most
 * half the user's maxTardinessMs. Tiers are evaluated from cheapest
 * (largest window) to most expensive (smallest window), so the first
 * match is the most cost-effective.
 */
public final class TierAdvisor {

    private TierAdvisor() {}

    public record Recommendation(PrecisionTier tier, String rationale) {}

    private static PrecisionTier[] cheapestFirst(PrecisionTierCatalog catalog) {
        // 注意：cheapest 优先,故按 precisionWindowMs 降序（最大窗口在前）。
        // brief 原稿用升序（tightest 在前）,会令循环首个命中恒为最紧档,破坏
        // "首个命中即最有性价比"语义并使 fallback 第五节错位（见 Phase 2 报告）。
        return catalog.supportedTiers().stream()
            .sorted(Comparator.comparingLong(catalog::precisionWindowMs).reversed())
            .toArray(PrecisionTier[]::new); // cheapest → tightest
    }

    public static Recommendation recommend(long maxTardinessMs, Reliability reliability) {
        if (maxTardinessMs <= 0) {
            throw new IllegalArgumentException("maxTardinessMs must be positive, got: " + maxTardinessMs);
        }
        if (reliability == null) {
            throw new IllegalArgumentException("reliability must not be null");
        }
        PrecisionTierCatalog catalog = PrecisionTierCatalog.defaultCatalog();
        int safetyFactor = reliability == Reliability.AT_LEAST_ONCE ? 2 : 1;
        long safetyWindowMs = maxTardinessMs / safetyFactor;

        PrecisionTier[] tiers = cheapestFirst(catalog);
        for (PrecisionTier tier : tiers) {
            if (catalog.precisionWindowMs(tier) <= safetyWindowMs) {
                return new Recommendation(tier, buildRationale(tier, catalog, maxTardinessMs, safetyFactor, reliability));
            }
        }
        PrecisionTier fallback = tiers[tiers.length - 1]; // 最紧档（min precisionWindowMs）
        return new Recommendation(fallback, String.format(
            "%s tier (%dms precision) is the tightest available but does not meet "
                + "maxTardinessMs=%dms with %d× safety margin. "
                + "Consider relaxing the SLO or accepting best-effort latency.",
            fallback.name(), catalog.precisionWindowMs(fallback), maxTardinessMs, safetyFactor));
    }

    private static String buildRationale(PrecisionTier tier, PrecisionTierCatalog catalog,
                                          long maxTardinessMs, int safetyFactor,
                                          Reliability reliability) {
        double margin = maxTardinessMs / (double) catalog.precisionWindowMs(tier);
        return String.format(
            "%s tier (%dms precision, %s batching, %d max concurrency) satisfies "
                + "maxTardinessMs=%dms with %.0f× safety margin (%s).",
            tier.name(),
            catalog.precisionWindowMs(tier),
            catalog.isBatchEnabled(tier) ? "batched" : "no",
            catalog.maxConcurrency(tier),
            maxTardinessMs,
            margin,
            reliability.name());
    }
}
