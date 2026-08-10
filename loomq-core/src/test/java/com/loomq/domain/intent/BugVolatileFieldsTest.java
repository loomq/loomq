package com.loomq.domain.intent;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import org.junit.jupiter.api.Test;

/**
 * FIX #5 (deadline / precisionTier now volatile) - regression guard.
 *
 * <p>Previously Intent.deadline and Intent.precisionTier were plain (non-volatile) fields,
 * while status, executeAt, updatedAt, attempts and revision WERE volatile. deadline is read by
 * isExpired() and the scheduler/scan threads, and can be mutated on another thread via
 * setDeadline() (e.g. updateIntent). Without the volatile guarantee there was no happens-before
 * edge, so a thread scanning for expiry could observe a stale deadline and fail to expire (or
 * wrongly expire) an intent. This test confirms the fix: both fields are now volatile.
 */
class BugVolatileFieldsTest {

    @Test
    void deadlineAndPrecisionTierAreVolatile() throws Exception {
        Field deadline = Intent.class.getDeclaredField("deadline");
        Field tier = Intent.class.getDeclaredField("precisionTier");

        assertTrue(Modifier.isVolatile(deadline.getModifiers()),
            "FIX #5: 'deadline' is now volatile -> updates on one thread are visible to the scan/scheduler thread");
        assertTrue(Modifier.isVolatile(tier.getModifiers()),
            "FIX #5: 'precisionTier' is now volatile -> updates are visible across threads");

        // The fields the scheduler relies on for visibility remain volatile:
        assertTrue(Modifier.isVolatile(Intent.class.getDeclaredField("status").getModifiers()));
        assertTrue(Modifier.isVolatile(Intent.class.getDeclaredField("executeAt").getModifiers()));
        assertTrue(Modifier.isVolatile(Intent.class.getDeclaredField("revision").getModifiers()));
    }
}
