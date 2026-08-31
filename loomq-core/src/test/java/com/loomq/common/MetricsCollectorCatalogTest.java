package com.loomq.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.domain.intent.PrecisionTier;
import com.loomq.domain.intent.PrecisionTierCatalog;
import com.loomq.domain.intent.PrecisionTierProfile;
import java.util.EnumMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * round 12:MC 构造硬编码 defaultCatalog,与引擎注入目录脱节——自定义目录的
 * tier 指标面错误(未支持档也建计数,自定义目录形同虚设)。修复后 tier 计数面
 * 必须与注入目录的 supportedTiers 一致。
 */
class MetricsCollectorCatalogTest {

    private static PrecisionTierCatalog subsetCatalog() {
        // 仅 ULTRA(ordinal 0) 与 MILLI(ordinal 3),跳过 FAST(1)/STANDARD(2)——模式同 TierByOrdinalSubsetCatalogTest
        EnumMap<PrecisionTier, PrecisionTierProfile> profiles = new EnumMap<>(PrecisionTier.class);
        profiles.put(PrecisionTier.ULTRA, new PrecisionTierProfile(10, 200, 1, 5, 16));
        profiles.put(PrecisionTier.MILLI, new PrecisionTierProfile(1, 100, 1, 1, 8));
        return PrecisionTierCatalog.of(profiles, PrecisionTier.ULTRA);
    }

    @Test
    void tierCounterSurfaceMatchesInjectedCatalog() {
        MetricsCollector mc = new MetricsCollector(subsetCatalog());
        Map<PrecisionTier, Long> counts = mc.getIntentCountsByTier();
        assertTrue(counts.containsKey(PrecisionTier.ULTRA));
        assertTrue(counts.containsKey(PrecisionTier.MILLI));
        assertEquals(2, counts.size(), "tier 计数面必须等于注入目录的 supportedTiers,不得出现未支持档");
    }

    @Test
    void unsupportedTierMergesIntoDefaultTier() {
        PrecisionTierCatalog catalog = subsetCatalog();
        MetricsCollector mc = new MetricsCollector(catalog);
        mc.incrementIntentByTier(PrecisionTier.FAST);   // 不在目录内
        assertEquals(1, mc.getIntentCountsByTier().get(catalog.defaultTier()),
            "未支持档应归并 defaultTier 而非丢失");
    }

    @Test
    void nullCatalogFallsBackToDefault() {
        MetricsCollector mc = new MetricsCollector(null);
        assertEquals(PrecisionTier.values().length, mc.getIntentCountsByTier().size());
    }
}
