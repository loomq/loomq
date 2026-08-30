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
        return new WheelConfig(dir.toString(), 30, slotsPerBucket, 1, 10_000L, 60L * 60_000L, 60_000L);
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
            s.scanSlotsFrom(Instant.ofEpochMilli(clock.get())).forEachRemaining(e -> found.add(e.intent()));
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

    @Test
    void spilledSlotSupportsTerminalOverwriteAndReclaim() {
        AtomicLong clock = new AtomicLong(Instant.parse("2026-06-30T00:00:00Z").toEpochMilli());
        try (WheelStore s = new WheelStore(cfg(tmp, 1), clock::get)) {
            long sec = clock.get() + 5_000;                     // 秒 S
            long sec2 = clock.get() + 6_000;                    // 秒 S+1（同一分钟）
            SlotLocation loc1 = s.put(intent("intent_ovs0000001", clock, sec));    // SEC@S
            SlotLocation loc2 = s.put(intent("intent_ovs0000002", clock, sec));    // SEC 满 → MIN
            assertEquals(WheelTier.MIN, loc2.tier(), "第 2 个 spill 到 MIN");
            // 终态原地覆写 spill 落点（模拟 persistTerminalInPlace）
            Intent term = intent("intent_ovs0000002", clock, sec);
            term.transitionTo(IntentStatus.CANCELED);
            term.incrementRevision();
            s.overwriteSlot(loc2, SlotCodec.encode(term));
            assertEquals(IntentStatus.CANCELED, s.readSlot(loc2).getStatus(), "spill 落点覆写为终态");
            // 回收 spill 槽 → 空
            s.freeSlot(loc2);
            assertNull(s.readSlot(loc2), "回收后槽为空");
            // 秒 S+1 的 SEC 桶占满后，新 intent spill 到同一 MIN 桶 → 复用 freed 槽
            SlotLocation pre = s.put(intent("intent_ovs0000003", clock, sec2));     // SEC@S+1
            assertEquals(WheelTier.SEC, pre.tier());
            SlotLocation loc3 = s.put(intent("intent_ovs0000004", clock, sec2));    // SEC 满 → MIN
            assertEquals(WheelTier.MIN, loc3.tier());
            assertEquals(loc2.slotIndex(), loc3.slotIndex(), "spill 槽 free 后复用同一索引");
            assertEquals(loc2.bucketKey(), loc3.bucketKey(), "两次 spill 落同一 MIN 桶");
            assertNotNull(s.readSlot(loc3));
        }
    }

    @Test
    void spilledIntentSurvivesRestart() {
        AtomicLong clock = new AtomicLong(Instant.parse("2026-06-30T00:00:00Z").toEpochMilli());
        Path dir = tmp.resolve("wheel");
        try (WheelStore s = new WheelStore(cfg(dir, 2), clock::get)) {
            long execMs = clock.get() + 5_000;
            SlotLocation loc1 = s.put(intent("intent_rst0000001", clock, execMs)); // SEC slot0
            SlotLocation loc2 = s.put(intent("intent_rst0000002", clock, execMs)); // SEC slot1
            SlotLocation loc3 = s.put(intent("intent_rst0000003", clock, execMs)); // SEC 满 → MIN slot0 (spill)
            assertEquals(WheelTier.SEC, loc1.tier());
            assertEquals(WheelTier.SEC, loc2.tier());
            assertEquals(WheelTier.MIN, loc3.tier(), "SEC 满(2) → 第 3 个 spill 到 MIN");
        }
        try (WheelStore s2 = new WheelStore(cfg(dir, 2), clock::get)) {
            List<Intent> found = new ArrayList<>();
            s2.scanSlotsFrom(Instant.ofEpochMilli(clock.get())).forEachRemaining(e -> found.add(e.intent()));
            assertEquals(3, found.size(), "spill 槽重启后可恢复（含部分占用的 MIN 桶）");
            assertTrue(found.stream().anyMatch(i -> "intent_rst0000003".equals(i.getIntentId())));
        }
    }

    @Test
    void concurrentOverflowSpillsToDistinctSlots() throws Exception {
        AtomicLong clock = new AtomicLong(Instant.parse("2026-06-30T00:00:00Z").toEpochMilli());
        try (WheelStore s = new WheelStore(cfg(tmp, 5), clock::get)) {
            long execMs = clock.get() + 5_000; // 同一秒：SEC 容量 5，16 并发 → 5 SEC + 11 spill
            int workers = 16;
            java.util.Set<String> seen = java.util.concurrent.ConcurrentHashMap.newKeySet();
            java.util.concurrent.ExecutorService pool = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor();
            try {
                java.util.List<java.util.concurrent.Future<?>> futures = new java.util.ArrayList<>();
                for (int i = 0; i < workers; i++) {
                    final int n = i;
                    futures.add(pool.submit(() -> {
                        SlotLocation loc = s.put(intent("intent_csp" + n + "000000", clock, execMs));
                        assertNotNull(s.readSlot(loc));
                        seen.add(loc.tier() + "/" + loc.bucketKey() + "/" + loc.slotIndex());
                    }));
                }
                for (var f : futures) f.get();
            } finally {
                pool.close();
            }
            assertEquals(workers, seen.size(), "并发 spill 后槽位互异，无重复分发");
            List<Intent> found = new ArrayList<>();
            s.scanSlotsFrom(Instant.ofEpochMilli(clock.get())).forEachRemaining(e -> found.add(e.intent()));
            assertEquals(workers, found.size(), "并发 spill 后全部可扫描");
        }
    }

    @Test
    void payloadOverflowIsNotSpilledAsBucketOverflow() {
        AtomicLong clock = new AtomicLong(Instant.parse("2026-06-30T00:00:00Z").toEpochMilli());
        try (WheelStore s = new WheelStore(cfg(tmp, 1), clock::get)) {
            Intent big = intent("intent_plo0000001", clock, clock.get() + 5_000);
            big.setTags(java.util.Map.of("pad", "x".repeat(300)));   // encodePayload 超 210B（tags TLV 0x0E 计入容量）
            assertThrows(SlotOverflowException.class, () -> s.put(big));
            assertEquals(0L, s.getSpillCounts().getOrDefault(WheelTier.SEC, 0L),
                "payload 溢出不得计入桶溢出 spill 计数");
            List<Intent> found = new ArrayList<>();
            s.scanSlotsFrom(Instant.ofEpochMilli(clock.get())).forEachRemaining(e -> found.add(e.intent()));
            assertEquals(0, found.size(), "payload 溢出 intent 不落盘");
            // 保留槽已回滚：下一个同秒正常 put 仍落 SEC（未被烧毁导致误 spill 降级）
            SlotLocation loc = s.put(intent("intent_plo0000002", clock, clock.get() + 5_000));
            assertEquals(WheelTier.SEC, loc.tier(), "encode 失败后保留槽应回滚，同秒 put 仍落 SEC");
            assertEquals(0L, s.getSpillCounts().getOrDefault(WheelTier.SEC, 0L),
                "SEC spill 计数全程为 0（回滚后同秒 put 未误 spill）");
            List<Intent> found2 = new ArrayList<>();
            s.scanSlotsFrom(Instant.ofEpochMilli(clock.get())).forEachRemaining(e -> found2.add(e.intent()));
            assertEquals(1, found2.size(), "只有后一个正常 intent 落盘");
        }
    }
}
