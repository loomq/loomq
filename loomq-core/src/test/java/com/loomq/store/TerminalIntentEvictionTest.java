package com.loomq.store;

import static org.junit.jupiter.api.Assertions.*;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import org.junit.jupiter.api.Test;

/**
 * P1-4 验证:终态 Intent 在 24h 后被驱逐,幂等记录同步清除,非终态 Intent 不受影响。
 */
class TerminalIntentEvictionTest {

    @Test
    void terminalIntentEvictedAfter24h() {
        ConcurrentIntentStore store = new ConcurrentIntentStore();

        // Create a terminal intent with updatedAt 25h ago
        Intent old = Intent.restore(
            null, "intent_old_terminal01", IntentStatus.ACKED,
            Instant.now().minus(26, ChronoUnit.HOURS),
            Instant.now().minus(25, ChronoUnit.HOURS),
            Instant.now().minusSeconds(10),
            null, null, PrecisionTier.STANDARD, null, null, null,
            null, null, null, null, 0, null, 5);
        store.upsert(old);

        // Create a fresh terminal intent (updatedAt = now)
        Intent fresh = Intent.restore(
            null, "intent_fresh_000001", IntentStatus.ACKED,
            Instant.now().minusSeconds(10),
            Instant.now().minusSeconds(5),
            Instant.now().minusSeconds(10),
            null, null, PrecisionTier.STANDARD, null, null, null,
            null, null, null, null, 0, null, 5);
        store.upsert(fresh);

        // Create a non-terminal intent updated 25h ago (should NOT be evicted)
        Intent oldScheduled = Intent.restore(
            null, "intent_old_scheduled", IntentStatus.SCHEDULED,
            Instant.now().minus(26, ChronoUnit.HOURS),
            Instant.now().minus(25, ChronoUnit.HOURS),
            Instant.now().plusSeconds(3600),
            null, null, PrecisionTier.STANDARD, null, null, null,
            null, null, null, null, 0, null, 3);
        store.upsert(oldScheduled);

        // Run cleanup
        store.testCleanupExpiredRecords();

        // Old terminal intent should be evicted
        assertNull(store.findByIdInternal("intent_old_terminal01"),
            "terminal intent older than 24h must be evicted");

        // Fresh terminal intent should remain
        assertNotNull(store.findByIdInternal("intent_fresh_000001"),
            "recent terminal intent must not be evicted");

        // Old non-terminal intent should remain
        assertNotNull(store.findByIdInternal("intent_old_scheduled"),
            "non-terminal intent must not be evicted regardless of age");

        store.shutdown();
    }

    @Test
    void evictionAlsoClearsIdempotencyRecord() {
        ConcurrentIntentStore store = new ConcurrentIntentStore();

        // createdAt is recent (within 24h idempotency window), but updatedAt is 25h ago
        // (triggers terminal eviction). This isolates eviction from idempotency expiry.
        Intent terminal = Intent.restore(
            null, "intent_idem_00000001", IntentStatus.ACKED,
            Instant.now().minusSeconds(10),
            Instant.now().minus(25, ChronoUnit.HOURS),
            Instant.now().minusSeconds(10),
            null, null, PrecisionTier.STANDARD, null, null, null,
            null, null, "idem-key-001", null, 0, null, 5);
        store.upsert(terminal);

        // Verify idempotency record exists
        IdempotencyResult result = store.checkIdempotency("idem-key-001");
        assertTrue(result.exists(), "idempotency record should exist before eviction");

        // Run cleanup
        store.testCleanupExpiredRecords();

        // After eviction, idempotency record should be cleared
        result = store.checkIdempotency("idem-key-001");
        assertFalse(result.exists(),
            "idempotency record must be cleared when terminal intent is evicted");

        store.shutdown();
    }
}
