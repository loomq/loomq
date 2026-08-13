package com.loomq.infrastructure.wheel;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.testutil.TestWheelConfigs;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * BucketReclaimer.reclaimOnce 三分支语义测试。
 *
 * <p>techdebt D3:回收逻辑此前仅 TOCTOU 竞态有覆盖,正分支(引用跳过/未过期保留/
 * 过期无引用删除)从未直接断言;删除路径误删风险高,值得锁定。</p>
 */
class BucketReclaimerTest {

    @TempDir Path tmp;

    private static final long RETENTION_MS = 31L * 24 * 60 * 60_000L;  // 与 WheelStoreTest 一致

    private WheelStore newStore(AtomicLong clock) {
        WheelConfig cfg = TestWheelConfigs.defaults(tmp);
        return new WheelStore(cfg, clock::get);
    }

    private static Intent intent(String id, long executeAtMs) {
        Intent it = new Intent(id);
        it.setExecuteAt(Instant.ofEpochMilli(executeAtMs));
        it.setDeadline(Instant.ofEpochMilli(executeAtMs + 3_600_000));
        it.transitionTo(IntentStatus.SCHEDULED);
        return it;
    }

    private static String bucketFileName(WheelTier tier, long bucketKey) {
        return String.format("%020d.bin", bucketKey);
    }

    @Test
    void referencedBucketIsSkipped() throws Exception {
        AtomicLong clock = new AtomicLong(Instant.parse("2026-06-30T00:00:00Z").toEpochMilli());
        try (WheelStore store = newStore(clock)) {
            long pastMs = clock.get() - 40L * 24 * 60 * 60_000L;
            SlotLocation loc = store.put(intent("reclaim_ref_0001", pastMs));
            IntentLocationIndex index = new IntentLocationIndex();
            index.put("reclaim_ref_0001", loc);

            BucketReclaimer reclaimer = new BucketReclaimer(store, index, RETENTION_MS);
            assertEquals(0, reclaimer.reclaimOnce(), "有活跃引用的过期桶不得删除");

            Path file = Paths.get(tmp.toString(), loc.tier().name().toLowerCase(), bucketFileName(loc.tier(), loc.bucketKey()));
            assertTrue(Files.exists(file), "引用桶文件必须留存");
        }
    }

    @Test
    void notExpiredBucketIsKept() throws Exception {
        AtomicLong clock = new AtomicLong(Instant.parse("2026-06-30T00:00:00Z").toEpochMilli());
        try (WheelStore store = newStore(clock)) {
            long futureMs = clock.get() + 60_000;  // 未来 1 分钟,远未过期
            SlotLocation loc = store.put(intent("reclaim_new_0001", futureMs));

            BucketReclaimer reclaimer = new BucketReclaimer(store, new IntentLocationIndex(), RETENTION_MS);
            assertEquals(0, reclaimer.reclaimOnce(), "未过期桶不得删除");

            Path file = Paths.get(tmp.toString(), loc.tier().name().toLowerCase(), bucketFileName(loc.tier(), loc.bucketKey()));
            assertTrue(Files.exists(file));
        }
    }

    @Test
    void expiredUnreferencedBucketIsDeleted() throws Exception {
        AtomicLong clock = new AtomicLong(Instant.parse("2026-06-30T00:00:00Z").toEpochMilli());
        try (WheelStore store = newStore(clock)) {
            long pastMs = clock.get() - 40L * 24 * 60 * 60_000L;
            SlotLocation loc = store.put(intent("reclaim_old_0001", pastMs));

            BucketReclaimer reclaimer = new BucketReclaimer(store, new IntentLocationIndex(), RETENTION_MS);
            assertEquals(1, reclaimer.reclaimOnce(), "过期且无引用的桶应删除");

            Path file = Paths.get(tmp.toString(), loc.tier().name().toLowerCase(), bucketFileName(loc.tier(), loc.bucketKey()));
            assertFalse(Files.exists(file), "桶文件应随回收删除");
        }
    }
}
