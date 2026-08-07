package com.loomq.benchmark;

import static org.junit.jupiter.api.Assertions.*;

import com.loomq.domain.intent.PrecisionTier;
import com.loomq.domain.intent.PrecisionTierCatalog;
import java.util.List;
import org.junit.jupiter.api.Test;

class SweepDriverTest {

    @Test void parsesCsv() {
        assertEquals(List.of(4, 8, 16, 32), SweepDriver.parseValues("4,8,16,32"));
    }
    @Test void rejectsInvalid() {
        assertThrows(IllegalArgumentException.class, () -> SweepDriver.parseValues("4,0,16"));
        assertThrows(IllegalArgumentException.class, () -> SweepDriver.parseValues(""));
    }
    @Test void catalogsOverrideTargetTierOnly() {
        var base = PrecisionTierCatalog.defaultCatalog();
        var cats = SweepDriver.catalogs(base, PrecisionTier.ULTRA, SweepParam.CONSUMERS, List.of(8, 16));
        assertEquals(2, cats.size());
        assertEquals(8, cats.get(0).consumerCount(PrecisionTier.ULTRA));
        assertEquals(16, cats.get(1).consumerCount(PrecisionTier.ULTRA));
        assertEquals(base.consumerCount(PrecisionTier.MILLI), cats.get(0).consumerCount(PrecisionTier.MILLI));
    }
}