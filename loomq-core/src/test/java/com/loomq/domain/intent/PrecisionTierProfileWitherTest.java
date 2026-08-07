package com.loomq.domain.intent;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;

class PrecisionTierProfileWitherTest {

    private PrecisionTierProfile base() {
        return new PrecisionTierProfile(10, 200, 1, 5, 16, 3200,
            WalMode.DURABLE, 10, false, true);
    }

    @Test void withConsumerCountPreservesOtherFields() {
        var p = base().withConsumerCount(8);
        assertEquals(8, p.consumerCount());
        assertEquals(10, p.precisionWindowMs());
        assertEquals(200, p.maxConcurrency());
        assertEquals(3200, p.dispatchQueueCapacity());
        assertTrue(p.adaptiveScan());
    }

    @Test void withMaxConcurrencyPreservesOthers() {
        var p = base().withMaxConcurrency(100);
        assertEquals(100, p.maxConcurrency());
        assertEquals(16, p.consumerCount());
    }

    @Test void withBatchSizeAndQueueCapacity() {
        var p = base().withBatchSize(20).withDispatchQueueCapacity(800);
        assertEquals(20, p.batchSize());
        assertEquals(800, p.dispatchQueueCapacity());
        assertEquals(16, p.consumerCount());
    }

    @Test void rejectsInvalidValue() {
        assertThrows(IllegalArgumentException.class, () -> base().withConsumerCount(0));
    }
}