package com.loomq.infrastructure.wheel;
import static org.junit.jupiter.api.Assertions.*;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.testutil.TestWheelConfigs;
import java.nio.file.Path;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class IntentLocationIndexTest {
    @TempDir Path tmp;

    @Test
    void shouldTrackAndRemoveLocation() {
        AtomicLong clock = new AtomicLong(Instant.parse("2026-06-30T00:00:00Z").toEpochMilli());
        WheelConfig cfg = TestWheelConfigs.defaults(tmp);
        try (WheelStore store = new WheelStore(cfg, clock::get);
             TailIndex tail = new TailIndex(tmp, clock::get);
             IntentLocationIndex idx = new IntentLocationIndex()) {
            Intent it = new Intent("intent_idx0000000001");
            it.setExecuteAt(Instant.ofEpochMilli(clock.get() + 5_000));
            it.transitionTo(IntentStatus.SCHEDULED);
            SlotLocation loc = store.put(it);
            idx.put(it.getIntentId(), loc);

            assertEquals(loc, idx.get(it.getIntentId()));
            assertTrue(idx.remove(it.getIntentId()));
            assertNull(idx.get(it.getIntentId()));
        }
    }

    @Test
    void shouldRebuildFromWheelScan() {
        AtomicLong clock = new AtomicLong(Instant.parse("2026-06-30T00:00:00Z").toEpochMilli());
        WheelConfig cfg = TestWheelConfigs.defaults(tmp);
        try (WheelStore store = new WheelStore(cfg, clock::get);
             IntentLocationIndex idx = new IntentLocationIndex()) {
            for (int i = 0; i < 3; i++) {
                Intent it = new Intent("intent_rb000000000" + i);
                it.setExecuteAt(Instant.ofEpochMilli(clock.get() + (i + 1) * 1_000));
                it.transitionTo(IntentStatus.SCHEDULED);
                store.put(it);
            }
            // 重建:扫描所有槽
            var it = store.scanSlotsFrom(Instant.ofEpochMilli(0));
            while (it.hasNext()) {
                SlotEntry e = it.next();
                idx.put(e.intent().getIntentId(), e.loc());
            }
            assertNotNull(idx.get("intent_rb0000000000"));
            assertNotNull(idx.get("intent_rb0000000002"));
        }
    }

    @Test
    void shouldCancelColdTailIntent() {
        AtomicLong clock = new AtomicLong(Instant.parse("2026-06-30T00:00:00Z").toEpochMilli());
        WheelConfig cfg = TestWheelConfigs.defaults(tmp);
        try (WheelStore store = new WheelStore(cfg, clock::get);
             TailIndex tail = new TailIndex(tmp, clock::get);
             IntentLocationIndex idx = new IntentLocationIndex()) {
            Intent far = new Intent("intent_far0000000001");
            far.setExecuteAt(Instant.ofEpochMilli(clock.get() + 40L * 86_400_000L));
            far.transitionTo(IntentStatus.SCHEDULED);
            tail.put(far);
            idx.put(far.getIntentId(), SlotLocation.tail(far.getExecuteAt().toEpochMilli()));

            // 取消冷(tail)Intent
            SlotLocation loc = idx.get(far.getIntentId());
            assertTrue(loc.inTail());
            assertTrue(tail.remove(far.getIntentId()));
            idx.remove(far.getIntentId());
            assertEquals(0, tail.size());
            assertNull(idx.get(far.getIntentId()));
        }
    }
}
