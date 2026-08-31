package com.loomq.application.scheduler;

import static org.junit.jupiter.api.Assertions.*;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.RedeliveryPolicy;
import org.junit.jupiter.api.Test;

class RetryPolicyTest {

    private final RetryPolicy policy = new RetryPolicy();

    @Test
    void defaultsWhenNoRedeliveryPolicy() {
        Intent intent = new Intent("intent_rp_default");
        assertEquals(5, policy.maxAttempts(intent));
        assertEquals(5000, policy.backoffDelayMs(intent));
    }

    @Test
    void usesConfiguredPolicyValues() {
        Intent intent = new Intent("intent_rp_cfg");
        intent.setRedelivery(new RedeliveryPolicy(3, "fixed", 1000, 10000, 1.0, false));
        intent.setAttempts(2);
        assertEquals(3, policy.maxAttempts(intent));
        assertEquals(1000, policy.backoffDelayMs(intent));
    }
}
