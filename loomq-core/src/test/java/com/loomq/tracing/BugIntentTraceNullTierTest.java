package com.loomq.tracing;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import com.loomq.domain.intent.IntentStatus;
import org.junit.jupiter.api.Test;

/**
 * IntentTrace.toJson 对 null tier 应能安全导出，不能 NPE。
 */
class BugIntentTraceNullTierTest {

    @Test
    void toJsonMustHandleNullTier() {
        IntentTrace trace = new IntentTrace(
            "id", "trace", null, IntentStatus.CREATED,
            1, 0, 0, 0, 0, 0, 0, 0, 0, null, null);

        assertDoesNotThrow(trace::toJson);
    }
}
