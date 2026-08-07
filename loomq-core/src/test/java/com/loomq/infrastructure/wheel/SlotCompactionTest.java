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
            Intent readB = s.readSlot(locB);
            assertNotNull(readB, "复用槽后应能读到新 intent");
            assertEquals(b.getIntentId(), readB.getIntentId(), "复用槽应读到 intent b");
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

    @Test
    void doubleFreeDoesNotDuplicateFreeList() {
        AtomicLong clock = new AtomicLong(Instant.parse("2026-06-30T00:00:00Z").toEpochMilli());
        try (WheelStore s = new WheelStore(cfg(tmp), clock::get)) {
            Intent a = intent("intent_dbl000000001", clock, 1_000);
            SlotLocation locA = s.put(a);
            s.freeSlot(locA);
            s.freeSlot(locA); // 双重 free：应被防护为 no-op，不把同一槽入栈两次
            // 放入两个新 intent，若 double-free 把 locA 入栈两次，两个 alloc 会拿到同一槽
            Intent b = intent("intent_dbl000000002", clock, 1_000);
            Intent c = intent("intent_dbl000000003", clock, 1_000);
            SlotLocation locB = s.put(b);
            SlotLocation locC = s.put(c);
            assertNotEquals(locB.slotIndex(), locC.slotIndex(), "双重 free 不得把同一槽发给两个 alloc");
        }
    }

    @Test
    void concurrentAllocFreeYieldDistinctSlots() throws Exception {
        AtomicLong clock = new AtomicLong(Instant.parse("2026-06-30T00:00:00Z").toEpochMilli());
        try (WheelStore s = new WheelStore(cfg(tmp), clock::get)) {
            // 预放 4 个 intent，释放其中一半（留空闲槽 + 单调 next 均已推进）
            SlotLocation[] pre = new SlotLocation[4];
            for (int i = 0; i < 4; i++) pre[i] = s.put(intent("intent_caf00000000" + i, clock, 1_000));
            s.freeSlot(pre[0]);
            s.freeSlot(pre[2]);

            int workers = 8;
            java.util.Set<Integer> used = java.util.concurrent.ConcurrentHashMap.newKeySet();
            java.util.concurrent.ExecutorService pool = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor();
            try {
                java.util.List<java.util.concurrent.Future<?>> futures = new java.util.ArrayList<>();
                for (int i = 0; i < workers; i++) {
                    final int n = i;
                    futures.add(pool.submit(() -> {
                        SlotLocation loc = s.put(intent("intent_caf1" + n + "00000" + n, clock, 1_000));
                        used.add(loc.slotIndex());
                    }));
                }
                for (var f : futures) f.get();
            } finally {
                pool.close();
            }
            assertEquals(workers, used.size(), "并发 alloc 不得重复分发同一槽");
        }
    }
}