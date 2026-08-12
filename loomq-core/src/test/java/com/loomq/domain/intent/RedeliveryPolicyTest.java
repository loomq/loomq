package com.loomq.domain.intent;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * RedeliveryPolicy.calculateDelay 的 maxDelayMs 是硬上限:
 * jitter(±20%)不得把延迟放大到超过 maxDelayMs。
 */
class RedeliveryPolicyTest {

    @Test
    void jitterMustNotExceedMaxDelayMs() {
        // initialDelay == maxDelay == 1000:非 jitter 部分恒被钳到 1000,
        // jitter 后若再突破上限即违反"最大延迟"契约(旧实现 50% 概率返回 1001..1200)。
        RedeliveryPolicy policy = new RedeliveryPolicy(10, "exponential", 1000, 1000, 2.0, true);
        for (int i = 0; i < 10_000; i++) {
            long delay = policy.calculateDelay(5);
            assertTrue(delay <= 1000, "jitter must not push delay above maxDelayMs; got " + delay);
            assertTrue(delay >= 0, "delay must be non-negative; got " + delay);
        }
    }
}
