package com.loomq.infrastructure.wheel;

import static org.junit.jupiter.api.Assertions.*;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import java.nio.file.Path;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Issue A compaction：终态槽原地覆写 + 回收复用。
 * 验证 WheelStore 层：overwriteSlot 原地改终态、freeSlot 清空、put 复用 freed 槽、重启重建 free-list。
 */
class SlotCompactionTest {
    @TempDir Path tmp;

    private WheelConfig cfg(Path dir) {
        return new WheelConfig(dir.toString(), "t", 30, 16, 1, 10_000L, 60L * 60_000L, 60_000L, null);
    }

    private static Intent intent(String id, AtomicLong clock, long deltaMs) {
        Intent it = new Intent(id);
        it.setExecuteAt(Instant.ofEpochMilli(clock.get() + deltaMs));
        it.transitionTo(IntentStatus.SCHEDULED);
        return it;
    }

    @Test
    void overwriteSlotReplacesContentInPlace() {
        AtomicLong clock = new AtomicLong(Instant.parse("2026-06-30T00:00:00Z").toEpochMilli());
        try (WheelStore s = new WheelStore(cfg(tmp), clock::get)) {
            Intent a = intent("intent_ovw000000001", clock, 1_000);
            SlotLocation loc = s.put(a);
            // 原地覆写为 DUE + rev2（模拟终态原地覆写；DUE 是 SCHEDULED 的合法后继）
            a.transitionTo(IntentStatus.DUE);
            a.incrementRevision();
            s.overwriteSlot(loc, SlotCodec.encode(a));
            Intent read = s.readSlot(loc);
            assertEquals(IntentStatus.DUE, read.getStatus());
            assertEquals(a.getRevision(), read.getRevision(), "覆写后 revision 应回读为覆写值");
        }
    }

    @Test
    void freeSlotThenPutReusesFreedIndex() {
        AtomicLong clock = new AtomicLong(Instant.parse("2026-06-30T00:00:00Z").toEpochMilli());
        try (WheelStore s = new WheelStore(cfg(tmp), clock::get)) {
            Intent a = intent("intent_fre000000001", clock, 1_000);
            SlotLocation locA = s.put(a);
            assertNotNull(s.readSlot(locA));
            s.freeSlot(locA);
            assertNull(s.readSlot(locA), "回收后槽应为空");
            // 新 intent 复用 freed 槽
            Intent b = intent("intent_fre000000002", clock, 1_000);
            SlotLocation locB = s.put(b);
            assertEquals(locA.slotIndex(), locB.slotIndex(), "应复用 freed 的槽序号");
        }
    }

    @Test
    void reopenRebuildsFreeListAndReusesFreedSlot() {
        AtomicLong clock = new AtomicLong(Instant.parse("2026-06-30T00:00:00Z").toEpochMilli());
        Path dir = tmp.resolve("wheel");
        int freedIdx;
        try (WheelStore s = new WheelStore(cfg(dir), clock::get)) {
            Intent a = intent("intent_rbl000000001", clock, 1_000);
            SlotLocation locA = s.put(a);
            freedIdx = locA.slotIndex();
            s.freeSlot(locA);
        }
        // 重启重建 free-list 后，应复用 freed 槽
        try (WheelStore s2 = new WheelStore(cfg(dir), clock::get)) {
            Intent b = intent("intent_rbl000000002", clock, 1_000);
            SlotLocation locB = s2.put(b);
            assertEquals(freedIdx, locB.slotIndex(), "重启后 free-list 应重建并复用槽");
        }
    }
}