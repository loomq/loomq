package com.loomq.infrastructure.wheel;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/** 方向 C 溢出 spill：桶满 → 链式落到更粗档（SEC→MIN→HOUR→DAY）。 */
class SlotSpillTest {
    @Test
    void nextCoarserChain() {
        assertEquals(WheelTier.MIN, WheelTier.SEC.nextCoarser());
        assertEquals(WheelTier.HOUR, WheelTier.MIN.nextCoarser());
        assertEquals(WheelTier.DAY, WheelTier.HOUR.nextCoarser());
        assertNull(WheelTier.DAY.nextCoarser(), "DAY 是溢出链终点");
    }
}
