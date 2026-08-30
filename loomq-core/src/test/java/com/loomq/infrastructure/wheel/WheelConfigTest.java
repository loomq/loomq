package com.loomq.infrastructure.wheel;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class WheelConfigTest {
    @Test
    void defaultsAreSane() {
        WheelConfig c = WheelConfig.defaultConfig();
        assertEquals(30, c.horizonDays());
        assertEquals(1024, c.slotsPerBucket());
        assertEquals(1, c.groupCommitIntervalMs());
    }

    @Test
    void rejectsNonPositiveHorizon() {
        assertThrows(IllegalArgumentException.class,
            () -> new WheelConfig("./data", 0, 1024, 1, 10_000L, 60L * 60_000L, 60_000L));
    }

    @Test
    void rejectsBlankDataDir() {
        assertThrows(IllegalArgumentException.class,
            () -> new WheelConfig("  ", 30, 1024, 1, 10_000L, 60L * 60_000L, 60_000L));
    }

    @Test
    void defaultsIncludeAwaitCommitTimeout() {
        WheelConfig c = WheelConfig.defaultConfig();
        assertEquals(10_000L, c.awaitCommitTimeoutMs());
    }

    @Test
    void rejectsNonPositiveAwaitCommitTimeout() {
        assertThrows(IllegalArgumentException.class,
            () -> new WheelConfig("./data", 30, 1024, 1, 0, 60L * 60_000L, 60_000L));
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
            () -> new WheelConfig("./data", 30, 1024, 1, 10_000L, 0, 60_000L));
    }

    @Test
    void rejectsNonPositivePromotionLead() {
        assertThrows(IllegalArgumentException.class,
            () -> new WheelConfig("./data", 30, 1024, 1, 10_000L, 60L * 60_000L, 0));
    }
}
