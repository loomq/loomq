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
        WheelConfig cfg = new WheelConfig(tmp.toString(), "t", 30, 16, 1, 10_000L, 60L * 60_000L, 60_000L, null);
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
        WheelConfig cfg = new WheelConfig(tmp.toString(), "t", 30, 16, 1, 10_000L, 60L * 60_000L, 60_000L, null);
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

    /**
     * deleteBucket 的 TOCTOU 守卫:BucketReclaimer 在"收集引用快照"与"删除文件"之间,
     * 同 key 桶可能被并发 put() 重建(computeIfAbsent 安装新桶、重新打开文件)。
     * 若此时仍删文件,会命中新桶的落盘文件——新 Intent 的槽字节随文件消失(重启静默丢失)。
     * 守卫:close 之后仅当 map 中该 key 无其他桶对象时才删除文件。
     *
     * <p>通过 {@link WheelStore#testBeforeFileDeleteHook} 在删文件前确定性注入并发重建。</p>
     */
    @Test
    void deleteBucketMustNotDeleteRecreatedBucketFile() {
        AtomicLong clock = new AtomicLong(Instant.parse("2026-06-30T00:00:00Z").toEpochMilli());
        WheelConfig cfg = new WheelConfig(tmp.toString(), "t", 30, 16, 1, 10_000L, 60L * 60_000L, 60_000L, null);
        // 过去 40 天的 executeAt(> retention = 31 天),SEC 桶
        long pastMs = clock.get() - 40L * 24 * 60 * 60_000L;

        try (WheelStore store = new WheelStore(cfg, clock::get)) {
            Intent a = new Intent("intent_reclaim_a001");
            a.setExecuteAt(Instant.ofEpochMilli(pastMs));
            a.transitionTo(IntentStatus.SCHEDULED);
            SlotLocation locA = store.put(a);
            assertFalse(locA.inTail());

            // 文件删除前 hook:并发重建同 key 桶(BucketReclaimer 引用快照是 stale 的)
            store.testBeforeFileDeleteHook = () -> {
                Intent b = new Intent("intent_reclaim_b001");
                b.setExecuteAt(Instant.ofEpochMilli(pastMs));
                b.transitionTo(IntentStatus.SCHEDULED);
                store.put(b);
            };
            BucketReclaimer reclaimer = new BucketReclaimer(store, new IntentLocationIndex(),
                31L * 24 * 60 * 60_000L);
            reclaimer.reclaimOnce();
            store.testBeforeFileDeleteHook = null;
        }

        // 重启语义:新开 WheelStore,并发重建的 B 必须仍在磁盘上
        try (WheelStore store2 = new WheelStore(cfg, clock::get)) {
            List<Intent> slots = new ArrayList<>();
            store2.scanSlotsFrom(Instant.ofEpochMilli(0)).forEachRemaining(e -> slots.add(e.intent()));
            assertEquals(1, slots.size(), "recreated bucket file must survive deleteBucket (TOCTOU guard)");
            assertEquals("intent_reclaim_b001", slots.get(0).getIntentId());
        }
    }
}
