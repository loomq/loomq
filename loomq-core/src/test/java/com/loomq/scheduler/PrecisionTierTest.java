package com.loomq.scheduler;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.PrecisionTier;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * PrecisionTier 精度档位测试
 *
 * @author loomq
 * @since v0.5.1
 */
class PrecisionTierTest {

    @Test
    @DisplayName("精度档位枚举值正确")
    void testPrecisionTierValues() {
        assertEquals(6, PrecisionTier.values().length);

        assertEquals(10, PrecisionTier.ULTRA.getPrecisionWindowMs());
        assertEquals(50, PrecisionTier.FAST.getPrecisionWindowMs());
        assertEquals(100, PrecisionTier.HIGH.getPrecisionWindowMs());
        assertEquals(500, PrecisionTier.STANDARD.getPrecisionWindowMs());
        assertEquals(1000, PrecisionTier.ECONOMY.getPrecisionWindowMs());
        assertEquals(1, PrecisionTier.MILLI.getPrecisionWindowMs());
    }

    @Test
    @DisplayName("枚举声明序即 SlotCodec 持久化序（ordinal 稳定）")
    void ordinalIsStableForPersistence() {
        assertEquals(0, PrecisionTier.ULTRA.ordinal());
        assertEquals(1, PrecisionTier.FAST.ordinal());
        assertEquals(2, PrecisionTier.HIGH.ordinal());
        assertEquals(3, PrecisionTier.STANDARD.ordinal());
        assertEquals(4, PrecisionTier.ECONOMY.ordinal());
        assertEquals(5, PrecisionTier.MILLI.ordinal());
    }

    @Test
    @DisplayName("MILLI 是最紧档位")
    void milliIsTightest() {
        assertTrue(PrecisionTier.MILLI.getPrecisionWindowMs() < PrecisionTier.ULTRA.getPrecisionWindowMs());
    }

    @Test
    @DisplayName("fromString 方法正确解析")
    void testFromString() {
        assertEquals(PrecisionTier.ULTRA, PrecisionTier.fromString("ULTRA"));
        assertEquals(PrecisionTier.ULTRA, PrecisionTier.fromString("ultra"));
        assertEquals(PrecisionTier.ULTRA, PrecisionTier.fromString("UlTrA"));

        assertEquals(PrecisionTier.FAST, PrecisionTier.fromString("FAST"));
        assertEquals(PrecisionTier.HIGH, PrecisionTier.fromString("HIGH"));
        assertEquals(PrecisionTier.STANDARD, PrecisionTier.fromString("STANDARD"));
        assertEquals(PrecisionTier.ECONOMY, PrecisionTier.fromString("ECONOMY"));
    }

    @Test
    @DisplayName("fromString 对 null 或空字符串返回默认值 STANDARD")
    void testFromStringDefaults() {
        assertEquals(PrecisionTier.STANDARD, PrecisionTier.fromString(null));
        assertEquals(PrecisionTier.STANDARD, PrecisionTier.fromString(""));
        assertEquals(PrecisionTier.STANDARD, PrecisionTier.fromString("   "));
        assertEquals(PrecisionTier.STANDARD, PrecisionTier.fromString("INVALID"));
        assertEquals(PrecisionTier.STANDARD, PrecisionTier.fromString("unknown"));
    }

    @Test
    @DisplayName("Intent 默认精度档位为 STANDARD")
    void testIntentDefaultPrecisionTier() {
        Intent intent = new Intent();
        assertEquals(PrecisionTier.STANDARD, intent.getPrecisionTier());
    }

    @Test
    @DisplayName("Intent 可设置精度档位")
    void testIntentSetPrecisionTier() {
        Intent intent = new Intent();
        intent.setPrecisionTier(PrecisionTier.ULTRA);
        assertEquals(PrecisionTier.ULTRA, intent.getPrecisionTier());

        intent.setPrecisionTier(PrecisionTier.ECONOMY);
        assertEquals(PrecisionTier.ECONOMY, intent.getPrecisionTier());
    }

    @Test
    @DisplayName("Intent 构造函数保留默认精度档位")
    void testIntentConstructorPreservesDefaultTier() {
        Intent intent = new Intent("test-intent-id");
        assertEquals(PrecisionTier.STANDARD, intent.getPrecisionTier());
    }
}
