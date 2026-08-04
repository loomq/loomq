package com.loomq.application.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.domain.intent.PrecisionTier;
import com.loomq.domain.intent.Reliability;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class TierAdvisorTest {

    @Nested
    @DisplayName("TierAdvisor SLO 推荐")
    class Recommendation {

        @Test
        @DisplayName("maxTardinessMs=20ms → ULTRA (唯一满足 2× 安全边界的 tier)")
        void ultraForTightSlo() {
            var rec = TierAdvisor.recommend(20, Reliability.AT_LEAST_ONCE);
            assertNotNull(rec);
            assertEquals(PrecisionTier.ULTRA, rec.tier());
            assertTrue(rec.rationale().contains("ULTRA"));
        }

        @Test
        @DisplayName("maxTardinessMs=120ms → FAST (50ms window ≤ 60ms safety)")
        void fastFor120ms() {
            var rec = TierAdvisor.recommend(120, Reliability.AT_LEAST_ONCE);
            assertEquals(PrecisionTier.FAST, rec.tier());
            assertTrue(rec.rationale().contains("FAST"));
        }

        @Test
        @DisplayName("maxTardinessMs=250ms → HIGH (100ms window ≤ 125ms safety)")
        void highFor250ms() {
            var rec = TierAdvisor.recommend(250, Reliability.AT_LEAST_ONCE);
            assertEquals(PrecisionTier.HIGH, rec.tier());
        }

        @Test
        @DisplayName("maxTardinessMs=1200ms → STANDARD (500ms window ≤ 600ms safety)")
        void standardFor1200ms() {
            var rec = TierAdvisor.recommend(1200, Reliability.AT_LEAST_ONCE);
            assertEquals(PrecisionTier.STANDARD, rec.tier());
        }

        @Test
        @DisplayName("maxTardinessMs=3000ms → ECONOMY (1000ms window ≤ 1500ms safety)")
        void economyFor3000ms() {
            var rec = TierAdvisor.recommend(3000, Reliability.AT_LEAST_ONCE);
            assertEquals(PrecisionTier.ECONOMY, rec.tier());
        }

        @Test
        @DisplayName("maxTardinessMs=1ms 太紧（safetyWindow=0），应 fallback 到最紧档 MILLI 并说明原因")
        void fallbackToTightest() {
            var rec = TierAdvisor.recommend(1, Reliability.AT_LEAST_ONCE);
            assertEquals(PrecisionTier.MILLI, rec.tier());
            assertTrue(rec.rationale().contains("does not meet"));
        }

        @Test
        @DisplayName("maxTardinessMs=2ms → MILLI（safetyWindow=1ms，MILLI(1ms) 经循环满足，非 fallback）")
        void milliForTwoMs() {
            var rec = TierAdvisor.recommend(2, Reliability.AT_LEAST_ONCE);
            assertEquals(PrecisionTier.MILLI, rec.tier());
        }

        @Test
        @DisplayName("maxTardinessMs=5ms → MILLI（1ms≤2ms safety 裕量，满足 2× 边界）")
        void milliForFiveMs() {
            // 注：safetyWindow=5/2=2ms，MILLI(1ms) 满足，故回落为 MILLI 而非 ULTRA。
            var rec = TierAdvisor.recommend(5, Reliability.AT_LEAST_ONCE);
            assertEquals(PrecisionTier.MILLI, rec.tier());
        }

        @Test
        @DisplayName("rationale 应包含安全边界倍数")
        void rationaleContainsMargin() {
            var rec = TierAdvisor.recommend(200, Reliability.AT_LEAST_ONCE);
            assertTrue(rec.rationale().contains("200ms"));
        }
    }

    @Nested
    @DisplayName("Reliability → AckMode 映射")
    class ReliabilityMapping {

        @Test
        @DisplayName("AT_LEAST_ONCE → DURABLE")
        void atLeastOnce() {
            assertEquals(
                com.loomq.domain.intent.AckMode.DURABLE,
                Reliability.AT_LEAST_ONCE.toAckMode()
            );
        }

        @Test
        @DisplayName("AT_MOST_ONCE → ASYNC")
        void atMostOnce() {
            assertEquals(
                com.loomq.domain.intent.AckMode.ASYNC,
                Reliability.AT_MOST_ONCE.toAckMode()
            );
        }
    }
}
