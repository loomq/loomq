package com.loomq.infrastructure.wheel;

import static org.junit.jupiter.api.Assertions.*;

import com.loomq.domain.intent.PrecisionTier;
import org.junit.jupiter.api.Test;

class WheelConfigTest {
    @Test
    void defaultsAreSane() {
        WheelConfig c = WheelConfig.defaultConfig();
        assertEquals(30, c.horizonDays());
        assertEquals(1024, c.slotsPerBucket());
        assertEquals(1, c.groupCommitIntervalMs());
        assertEquals(PrecisionTier.STANDARD, c.defaultTier());
    }

    @Test
    void rejectsNonPositiveHorizon() {
        assertThrows(IllegalArgumentException.class,
            () -> new WheelConfig("./data", "shard-0", 0, 1024, 1, 10_000L, 60L * 60_000L, 60_000L, PrecisionTier.STANDARD));
    }

    @Test
    void rejectsBlankDataDir() {
        assertThrows(IllegalArgumentException.class,
            () -> new WheelConfig("  ", "shard-0", 30, 1024, 1, 10_000L, 60L * 60_000L, 60_000L, PrecisionTier.STANDARD));
    }

    @Test
    void rejectsBlankShardId() {
        assertThrows(IllegalArgumentException.class,
            () -> new WheelConfig("./data", "  ", 30, 1024, 1, 10_000L, 60L * 60_000L, 60_000L, PrecisionTier.STANDARD));
    }

    @Test
    void defaultsIncludeAwaitCommitTimeout() {
        WheelConfig c = WheelConfig.defaultConfig();
        assertEquals(10_000L, c.awaitCommitTimeoutMs());
    }

    @Test
    void rejectsNonPositiveAwaitCommitTimeout() {
        assertThrows(IllegalArgumentException.class,
            () -> new WheelConfig("./data", "shard-0", 30, 1024, 1, 0, 60L * 60_000L, 60_000L, PrecisionTier.STANDARD));
    }

    @Test
    void defaultsIncludeHotBoundaryAndPromotionLead() {
        WheelConfig c = WheelConfig.defaultConfig();
        assertEquals(60L * 60_000L, c.hotBoundaryMs(), "default hotBoundaryMs = 60min");
        assertEquals(60_000L, c.promotionLeadMs(), "default promotionLeadMs = 60s");
    }

    @Test
    void rejectsNonPositiveHotBoundary() {
        assertThrows(IllegalArgumentException.class,
            () -> new WheelConfig("./data", "shard-0", 30, 1024, 1, 10_000L, 0, 60_000L, PrecisionTier.STANDARD));
    }

    @Test
    void rejectsNonPositivePromotionLead() {
        assertThrows(IllegalArgumentException.class,
            () -> new WheelConfig("./data", "shard-0", 30, 1024, 1, 10_000L, 60L * 60_000L, 0, PrecisionTier.STANDARD));
    }
}
