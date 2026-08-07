package com.loomq.common;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.loomq.domain.intent.PrecisionTier;
import com.loomq.domain.intent.PrecisionTierCatalog;
import com.loomq.domain.intent.PrecisionTierProfile;
import com.loomq.domain.intent.WalMode;
import java.util.EnumMap;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * P2：MILLI 高水位降级指标须归因到实际档，而非 defaultTier（STANDARD）。
 */
class PrecisionTierMetricsRegistryTest {

    @Test
    @DisplayName("incrementMilliFallback 归因到传入档而非 defaultTier")
    void milliFallbackAttributeToRequestedTier() {
        EnumMap<PrecisionTier, PrecisionTierProfile> profiles = new EnumMap<>(PrecisionTier.class);
        for (PrecisionTier t : PrecisionTier.values()) {
            profiles.put(t, new PrecisionTierProfile(1, 1, 1, 1, 1, 16, WalMode.DURABLE, 1));
        }
        PrecisionTierCatalog catalog = PrecisionTierCatalog.of(profiles, PrecisionTier.STANDARD);
        PrecisionTierMetricsRegistry registry = new PrecisionTierMetricsRegistry(catalog);

        registry.incrementMilliFallback(PrecisionTier.MILLI);
        registry.incrementMilliFallback(PrecisionTier.MILLI);
        registry.incrementMilliFallback(PrecisionTier.ULTRA);

        assertEquals(2, registry.getMilliFallback(PrecisionTier.MILLI));
        assertEquals(1, registry.getMilliFallback(PrecisionTier.ULTRA));
        assertEquals(0, registry.getMilliFallback(PrecisionTier.STANDARD));  // defaultTier 不背锅
    }
}