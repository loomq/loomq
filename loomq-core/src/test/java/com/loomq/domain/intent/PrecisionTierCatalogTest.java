package com.loomq.domain.intent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class PrecisionTierCatalogTest {

    private static final Map<PrecisionTier, PrecisionTierProfile> SAMPLE_PROFILES = Map.of(
        PrecisionTier.ULTRA, new PrecisionTierProfile(10, 200, 1, 5, 16),
        PrecisionTier.MILLI, new PrecisionTierProfile(1, 100, 1, 1, 8),
        PrecisionTier.STANDARD, new PrecisionTierProfile(500, 50, 20, 100, 3)
    );

    @Test
    void shouldRejectNullProfiles() {
        assertThrows(NullPointerException.class, () ->
            PrecisionTierCatalog.of(null, PrecisionTier.STANDARD));
    }

    @Test
    void shouldRejectNullDefaultTier() {
        assertThrows(NullPointerException.class, () ->
            PrecisionTierCatalog.of(SAMPLE_PROFILES, null));
    }

    @Test
    void shouldRejectDefaultTierNotInProfiles() {
        assertThrows(IllegalArgumentException.class, () ->
            PrecisionTierCatalog.of(Map.of(PrecisionTier.ULTRA,
                new PrecisionTierProfile(10, 200, 1, 5, 16)), PrecisionTier.STANDARD));
    }

    @Test
    void shouldCreateCatalogWithValidArguments() {
        PrecisionTierCatalog catalog = PrecisionTierCatalog.of(SAMPLE_PROFILES, PrecisionTier.STANDARD);
        assertNotNull(catalog);
        assertEquals(3, catalog.tierCount());
        assertEquals(PrecisionTier.STANDARD, catalog.defaultTier());
    }

    @Test
    void shouldReturnProfileForExistingTier() {
        PrecisionTierCatalog catalog = PrecisionTierCatalog.of(SAMPLE_PROFILES, PrecisionTier.STANDARD);
        PrecisionTierProfile profile = catalog.profile(PrecisionTier.ULTRA);
        assertEquals(10, profile.precisionWindowMs());
        assertEquals(200, profile.maxConcurrency());
    }

    @Test
    void shouldFallBackToDefaultTierForNull() {
        PrecisionTierCatalog catalog = PrecisionTierCatalog.of(SAMPLE_PROFILES, PrecisionTier.STANDARD);
        PrecisionTierProfile profile = catalog.profile(null);
        assertEquals(500, profile.precisionWindowMs());
        assertEquals(50, profile.maxConcurrency());
    }

    @Test
    void shouldFallBackToDefaultTierForMissingTier() {
        // FAST is not in SAMPLE_PROFILES, so it should fall back to STANDARD
        PrecisionTierCatalog catalog = PrecisionTierCatalog.of(SAMPLE_PROFILES, PrecisionTier.STANDARD);
        PrecisionTierProfile profile = catalog.profile(PrecisionTier.FAST);
        assertEquals(500, profile.precisionWindowMs());
    }

    @Test
    void shouldReturnDefaultTierForNegativeOrdinal() {
        PrecisionTierCatalog catalog = PrecisionTierCatalog.defaultCatalog();
        assertEquals(catalog.defaultTier(), catalog.tierByOrdinal(-1));
    }

    @Test
    void shouldReturnDefaultTierForOutOfBoundsOrdinal() {
        PrecisionTierCatalog catalog = PrecisionTierCatalog.defaultCatalog();
        assertTrue(catalog.tierByOrdinal(catalog.tierCount() + 10) == catalog.defaultTier());
    }

    @Test
    void shouldReturnCorrectTierForValidOrdinal() {
        PrecisionTierCatalog catalog = PrecisionTierCatalog.defaultCatalog();
        List<PrecisionTier> tiers = catalog.supportedTiers();
        for (int i = 0; i < tiers.size(); i++) {
            assertEquals(tiers.get(i), catalog.tierByOrdinal(i));
        }
    }

    @Test
    void supportedTiersShouldBeUnmodifiable() {
        PrecisionTierCatalog catalog = PrecisionTierCatalog.defaultCatalog();
        List<PrecisionTier> tiers = catalog.supportedTiers();
        assertThrows(UnsupportedOperationException.class, () -> tiers.add(PrecisionTier.ULTRA));
    }

    @Test
    void catalogShouldBeIndependentOfInputMap() {
        EnumMap<PrecisionTier, PrecisionTierProfile> mutableMap = new EnumMap<>(PrecisionTier.class);
        mutableMap.put(PrecisionTier.STANDARD, new PrecisionTierProfile(500, 50, 20, 100, 3));
        PrecisionTierCatalog catalog = PrecisionTierCatalog.of(mutableMap, PrecisionTier.STANDARD);
        // mutate the original map
        mutableMap.clear();
        // catalog should still have STANDARD
        assertEquals(1, catalog.tierCount());
    }

    @Test
    void defaultCatalogShouldHaveAllFourTiers() {
        // v0.9.x 精简：HIGH→FAST、ECONOMY→STANDARD，余 ULTRA/FAST/STANDARD/MILLI
        PrecisionTierCatalog catalog = PrecisionTierCatalog.defaultCatalog();
        assertEquals(4, catalog.tierCount());
    }

    @Test
    void milliProfileRegisters() {
        PrecisionTierCatalog c = PrecisionTierCatalog.defaultCatalog();
        assertTrue(c.isDirectBucket(PrecisionTier.MILLI));
        assertTrue(c.isAdaptive(PrecisionTier.MILLI));
        assertEquals(200_000, c.maxBuckets(PrecisionTier.MILLI));
        assertFalse(c.isDirectBucket(PrecisionTier.STANDARD));
        assertEquals(1, c.precisionWindowMs(PrecisionTier.MILLI));
    }

    @Test
    void defaultCatalogDelegatedMethodsShouldNotThrow() {
        PrecisionTierCatalog catalog = PrecisionTierCatalog.defaultCatalog();
        // these should not throw for any valid tier
        for (PrecisionTier tier : catalog.supportedTiers()) {
            catalog.precisionWindowMs(tier);
            catalog.maxConcurrency(tier);
            catalog.batchSize(tier);
            catalog.batchWindowMs(tier);
            catalog.consumerCount(tier);
            catalog.dispatchQueueCapacity(tier);
            catalog.isBatchEnabled(tier);
        }
    }

    @Test
    void isBatchEnabledShouldDelegatedToProfile() {
        PrecisionTierCatalog catalog = PrecisionTierCatalog.defaultCatalog();
        // ULTRA has batchSize=1
        assertEquals(false, catalog.isBatchEnabled(PrecisionTier.ULTRA));
        // STANDARD has batchSize=20
        assertEquals(true, catalog.isBatchEnabled(PrecisionTier.STANDARD));
    }
}
