package com.loomq.infrastructure.wheel;

import static org.junit.jupiter.api.Assertions.*;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class WheelStoreTest {
    @TempDir Path tmp;

    private WheelStore newStore(AtomicLong clock) {
        WheelConfig cfg = new WheelConfig(tmp.toString(), "t", 30, 16, 1, null);
        return new WheelStore(cfg, clock::get);
    }

    @Test
    void shouldPlaceIntentInCorrectWheel() {
        AtomicLong clock = new AtomicLong(Instant.parse("2026-06-30T00:00:00Z").toEpochMilli());
        try (WheelStore s = newStore(clock)) {
            Intent near = new Intent("intent_near0000000001");
            near.setExecuteAt(Instant.ofEpochMilli(clock.get() + 5_000)); // +5s → sec
            near.transitionTo(IntentStatus.SCHEDULED);

            Intent mid = new Intent("intent_mid00000000002");
            mid.setExecuteAt(Instant.ofEpochMilli(clock.get() + 120_000)); // +2min → min
            mid.transitionTo(IntentStatus.SCHEDULED);

            SlotLocation locNear = s.put(near);
            SlotLocation locMid = s.put(mid);
            assertEquals(WheelTier.SEC, locNear.tier());
            assertEquals(WheelTier.MIN, locMid.tier());
            assertFalse(locNear.inTail());

            Intent readNear = s.readSlot(locNear);
            assertEquals(near.getIntentId(), readNear.getIntentId());
        }
    }

    @Test
    void shouldScanFromGivenTime() {
        AtomicLong clock = new AtomicLong(Instant.parse("2026-06-30T00:00:00Z").toEpochMilli());
        try (WheelStore s = newStore(clock)) {
            for (int i = 1; i <= 3; i++) {
                Intent it = new Intent("intent_scan0000000" + i);
                it.setExecuteAt(Instant.ofEpochMilli(clock.get() + i * 1_000));
                it.transitionTo(IntentStatus.SCHEDULED);
                s.put(it);
            }
            List<Intent> found = new ArrayList<>();
            s.scanFrom(Instant.ofEpochMilli(clock.get())).forEachRemaining(found::add);
            assertEquals(3, found.size());
        }
    }

    @Test
    void outOfHorizonGoesToTailMarker() {
        AtomicLong clock = new AtomicLong(Instant.parse("2026-06-30T00:00:00Z").toEpochMilli());
        try (WheelStore s = newStore(clock)) {
            Intent far = new Intent("intent_far0000000001");
            far.setExecuteAt(Instant.ofEpochMilli(clock.get() + 40L * 24 * 3600 * 1000)); // +40d → tail
            far.transitionTo(IntentStatus.SCHEDULED);
            SlotLocation loc = s.locate(far.getExecuteAt());
            assertTrue(loc.inTail());
        }
    }

    @Test
    void shouldNotOverwriteExistingSlotsOnRestart() {
        AtomicLong clock = new AtomicLong(Instant.parse("2026-06-30T00:00:00Z").toEpochMilli());
        WheelConfig cfg = new WheelConfig(tmp.toString(), "t", 30, 16, 1, null);
        // Phase 1: write 3 intents into the same future bucket, close.
        String id1, id2, id3;
        try (WheelStore s1 = newStore(clock)) {
            Intent a = new Intent("intent_rst0000000001");
            a.setExecuteAt(Instant.ofEpochMilli(clock.get() + 5_000));
            a.transitionTo(IntentStatus.SCHEDULED);
            Intent b = new Intent("intent_rst0000000002");
            b.setExecuteAt(Instant.ofEpochMilli(clock.get() + 5_000));
            b.transitionTo(IntentStatus.SCHEDULED);
            Intent c = new Intent("intent_rst0000000003");
            c.setExecuteAt(Instant.ofEpochMilli(clock.get() + 5_000));
            c.transitionTo(IntentStatus.SCHEDULED);
            SlotLocation la = s1.put(a), lb = s1.put(b), lc = s1.put(c);
            id1 = a.getIntentId(); id2 = b.getIntentId(); id3 = c.getIntentId();
        }
        // Phase 2: reopen, write a 4th intent into the same bucket, verify the first 3 survive.
        try (WheelStore s2 = newStore(clock)) {
            Intent d = new Intent("intent_rst0000000004");
            d.setExecuteAt(Instant.ofEpochMilli(clock.get() + 5_000));
            d.transitionTo(IntentStatus.SCHEDULED);
            s2.put(d);

            // scanFrom must return all 4 (3 original + 1 new), none overwritten
            var iter = s2.scanFrom(Instant.ofEpochMilli(clock.get()));
            java.util.Set<String> ids = new java.util.HashSet<>();
            while (iter.hasNext()) ids.add(iter.next().getIntentId());
            assertTrue(ids.contains(id1), "id1 must survive restart");
            assertTrue(ids.contains(id2), "id2 must survive restart");
            assertTrue(ids.contains(id3), "id3 must survive restart");
            assertTrue(ids.contains(d.getIntentId()), "new id4 must be present");
            assertEquals(4, ids.size(), "no overwrites — all 4 distinct");
        }
    }
}
