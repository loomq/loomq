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

/** 方向 C 溢出 spill：桶满 → 链式落到更粗档（SEC→MIN→HOUR→DAY）。 */
class SlotSpillTest {
    @TempDir Path tmp;

    private WheelConfig cfg(Path dir, int slotsPerBucket) {
        return new WheelConfig(dir.toString(), "t", 30, slotsPerBucket, 1, 10_000L, 60L * 60_000L, 60_000L, null);
    }

    private static Intent intent(String id, AtomicLong clock, long executeAtMs) {
        Intent it = new Intent(id);
        it.setExecuteAt(Instant.ofEpochMilli(executeAtMs));
        it.transitionTo(IntentStatus.SCHEDULED);
        return it;
    }

    @Test
    void nextCoarserChain() {
        assertEquals(WheelTier.MIN, WheelTier.SEC.nextCoarser());
        assertEquals(WheelTier.HOUR, WheelTier.MIN.nextCoarser());
        assertEquals(WheelTier.DAY, WheelTier.HOUR.nextCoarser());
        assertNull(WheelTier.DAY.nextCoarser(), "DAY 是溢出链终点");
    }

    @Test
    void secFullSpillsToMin() {
        AtomicLong clock = new AtomicLong(Instant.parse("2026-06-30T00:00:00Z").toEpochMilli());
        try (WheelStore s = new WheelStore(cfg(tmp, 2), clock::get)) {
            long execMs = clock.get() + 5_000; // 同一秒
            SlotLocation locA = s.put(intent("intent_sp1_000001", clock, execMs));
            SlotLocation locB = s.put(intent("intent_sp1_000002", clock, execMs));
            SlotLocation locC = s.put(intent("intent_sp1_000003", clock, execMs));
            assertEquals(WheelTier.SEC, locA.tier());
            assertEquals(WheelTier.SEC, locB.tier());
            assertEquals(WheelTier.MIN, locC.tier(), "SEC 桶满(2) → 第 3 个 spill 到 MIN");
            assertNotNull(s.readSlot(locC), "spill 落点可读");
            assertEquals("intent_sp1_000003", s.readSlot(locC).getIntentId());
            assertEquals(1L, s.getSpillCounts().get(WheelTier.SEC), "SEC 溢出计数 +1");
            List<Intent> found = new ArrayList<>();
            s.scanFrom(Instant.ofEpochMilli(clock.get())).forEachRemaining(found::add);
            assertEquals(3, found.size(), "scanFrom 全可见（含 spill 落点）");
        }
    }

    @Test
    void chainSpillsThroughAllTiers() {
        AtomicLong clock = new AtomicLong(Instant.parse("2026-06-30T00:00:00Z").toEpochMilli());
        try (WheelStore s = new WheelStore(cfg(tmp, 1), clock::get)) {
            long execMs = clock.get() + 5_000; // 同一秒
            SlotLocation l1 = s.put(intent("intent_ch0000001", clock, execMs));
            SlotLocation l2 = s.put(intent("intent_ch0000002", clock, execMs));
            SlotLocation l3 = s.put(intent("intent_ch0000003", clock, execMs));
            SlotLocation l4 = s.put(intent("intent_ch0000004", clock, execMs));
            assertEquals(WheelTier.SEC, l1.tier());
            assertEquals(WheelTier.MIN, l2.tier(), "SEC 满(1) → spill MIN");
            assertEquals(WheelTier.HOUR, l3.tier(), "SEC+MIN 满 → spill HOUR");
            assertEquals(WheelTier.DAY, l4.tier(), "SEC+MIN+HOUR 满 → spill DAY");
            assertEquals(3L, s.getSpillCounts().get(WheelTier.SEC), "SEC 溢出 3 次（intent2/3/4）");
            assertEquals(2L, s.getSpillCounts().get(WheelTier.MIN), "MIN 溢出 2 次（intent3/4）");
            assertEquals(1L, s.getSpillCounts().get(WheelTier.HOUR), "HOUR 溢出 1 次（intent4）");
            assertNotNull(s.readSlot(l1));
            assertNotNull(s.readSlot(l2));
            assertNotNull(s.readSlot(l3));
            assertNotNull(s.readSlot(l4), "各档 spill 落点均可读");
        }
    }

    @Test
    void dayFullStillThrows() {
        AtomicLong clock = new AtomicLong(Instant.parse("2026-06-30T00:00:00Z").toEpochMilli());
        try (WheelStore s = new WheelStore(cfg(tmp, 1), clock::get)) {
            long execMs = clock.get() + 5_000; // 同一秒
            s.put(intent("intent_day0000001", clock, execMs)); // SEC
            s.put(intent("intent_day0000002", clock, execMs)); // MIN
            s.put(intent("intent_day0000003", clock, execMs)); // HOUR
            s.put(intent("intent_day0000004", clock, execMs)); // DAY
            assertThrows(SlotOverflowException.class,       // 第 5 个：整条链满 → 兜底抛
                () -> s.put(intent("intent_day0000005", clock, execMs)));
        }
    }
}
