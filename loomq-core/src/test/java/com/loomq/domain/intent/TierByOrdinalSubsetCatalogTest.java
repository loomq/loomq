package com.loomq.domain.intent;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.EnumMap;
import org.junit.jupiter.api.Test;

/**
 * R13: tierByOrdinal 旧实现按"列表下标"取档,与枚举 ordinal 混淆。自定义非连续子集目录
 * 会把 ordinal 1 错读成列表第 1 项。修复后按枚举 ordinal 直接映射,不受子集顺序影响。
 */
class TierByOrdinalSubsetCatalogTest {

    private static PrecisionTierCatalog subsetCatalog() {
        // 仅 ULTRA(ordinal 0) 与 MILLI(ordinal 3),跳过 FAST(1)/STANDARD(2)
        EnumMap<PrecisionTier, PrecisionTierProfile> profiles = new EnumMap<>(PrecisionTier.class);
        profiles.put(PrecisionTier.ULTRA, new PrecisionTierProfile(10, 200, 1, 5, 16));
        profiles.put(PrecisionTier.MILLI, new PrecisionTierProfile(1, 100, 1, 1, 8));
        return PrecisionTierCatalog.of(profiles, PrecisionTier.ULTRA);
    }

    @Test
    void ordinalMapsToEnumOrdinalNotListIndex() {
        PrecisionTierCatalog catalog = subsetCatalog();

        assertEquals(PrecisionTier.ULTRA, catalog.tierByOrdinal(0),
            "ordinal 0 must map to ULTRA (enum ordinal), which is supported");
        assertEquals(PrecisionTier.MILLI, catalog.tierByOrdinal(3),
            "ordinal 3 must map to MILLI (enum ordinal), supported even though list index is 1");
        assertEquals(catalog.defaultTier(), catalog.tierByOrdinal(1),
            "ordinal 1 (FAST) not supported -> fall back to default, not MILLI (list index confusion)");
        assertEquals(catalog.defaultTier(), catalog.tierByOrdinal(2),
            "ordinal 2 (STANDARD) not supported -> fall back to default");
    }

    @Test
    void outOfBoundsOrdinalFallsBackToDefault() {
        PrecisionTierCatalog catalog = subsetCatalog();
        assertEquals(catalog.defaultTier(), catalog.tierByOrdinal(-1));
        assertEquals(catalog.defaultTier(), catalog.tierByOrdinal(PrecisionTier.values().length));
    }
}
