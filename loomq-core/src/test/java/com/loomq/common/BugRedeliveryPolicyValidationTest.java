package com.loomq.common;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.RedeliveryPolicy;
import java.time.Instant;
import org.junit.jupiter.api.Test;

/**
 * R21: IntentValidator 不校验 redelivery——multiplier≤0 时 exponential 分支
 * Math.pow 产生 NaN/负值,(long)NaN=0 → setExecuteAt(now+负/零延迟) 把 intent 排到
 * 过去,结算后立即重投(重试风暴,受 maxAttempts 上限约束不无限);负 initialDelayMs
 * 同样。修复:创建入口拒绝非法 redelivery 配置。
 */
class BugRedeliveryPolicyValidationTest {

    private static Intent validBase() {
        Intent intent = new Intent("r21-rd-valid-0001");
        intent.setExecuteAt(Instant.now().plusSeconds(60));
        return intent;
    }

    @Test
    void validRedeliveryMustPass() {
        Intent intent = validBase();
        intent.setRedelivery(new RedeliveryPolicy(5, "exponential", 1000, 60_000, 2.0, true));
        assertDoesNotThrow(() -> IntentValidator.validate(intent));
    }

    @Test
    void zeroMultiplierMustBeRejected() {
        Intent intent = validBase();
        intent.setRedelivery(new RedeliveryPolicy(5, "exponential", 1000, 60_000, 0.0, false));
        assertThrows(IllegalArgumentException.class, () -> IntentValidator.validate(intent),
            "multiplier <= 0 makes Math.pow yield NaN/negative delays -> retry storm");
    }

    @Test
    void negativeInitialDelayMustBeRejected() {
        Intent intent = validBase();
        intent.setRedelivery(new RedeliveryPolicy(5, "fixed", -100, 60_000, 1.0, false));
        assertThrows(IllegalArgumentException.class, () -> IntentValidator.validate(intent),
            "negative initial delay schedules retries in the past -> immediate retry storm");
    }

    @Test
    void zeroMaxAttemptsMustBeRejected() {
        Intent intent = validBase();
        intent.setRedelivery(new RedeliveryPolicy(0, "fixed", 1000, 60_000, 1.0, false));
        assertThrows(IllegalArgumentException.class, () -> IntentValidator.validate(intent),
            "maxAttempts < 1 dead-letters on the first failure without ever retrying");
    }
}
