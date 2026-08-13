package com.loomq.application.command;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.application.scheduler.PrecisionScheduler;
import com.loomq.common.MetricsCollector;
import com.loomq.domain.intent.AckMode;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.domain.intent.PrecisionTierCatalog;
import com.loomq.infrastructure.wheel.GroupCommitBarrier;
import com.loomq.infrastructure.wheel.IntentLocationIndex;
import com.loomq.infrastructure.wheel.PromotionDaemon;
import com.loomq.infrastructure.wheel.TailIndex;
import com.loomq.infrastructure.wheel.WheelConfig;
import com.loomq.infrastructure.wheel.WheelStore;
import com.loomq.spi.DeliveryHandler.DeliveryResult;
import com.loomq.store.ConcurrentIntentStore;
import java.nio.file.Path;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * R21: 冷 intent(>30 天视界,tail)被提升后 fireNow/改期/取消走 persistToWheel 的
 * wheel 分支——旧代码只写 wheel 新槽,从不移除 tail 旧记录(tail 无过期机制,byId 永久
 * 持有)。影响三层:(a) 每次此类迁移泄漏一条 tail 记录(tail.log 无界增长,compaction 保留
 * 泄漏);(b) 恢复 slotCounts 把残留 tail 记录计入 → 终态墓碑 count>1 永不回收;
 * (c) 幽灵复活——终态 wheel 桶 31 天过期删除后,残留 tail 记录(仍 SCHEDULED,原远期
 * executeAt)成为唯一幸存者,重启被恢复为活 intent 并投递(已取消/已 ACK 的 intent 重复投递)。
 *
 * <p>修复:persistToWheel 的 wheel 分支发现前序位置在 tail 时,先 tailIndex.remove(id)
 * 再 awaitCommit(tail tombstone 与 wheel 槽同批落盘,无崩溃窗口)。</p>
 */
class BugTailStaleRecordAfterWheelMigrationTest {

    @TempDir
    Path tmp;

    @Test
    void wheelMigrationMustRemoveStaleTailRecord() throws Exception {
        AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        WheelConfig cfg = new WheelConfig(tmp.toString(), "t", 30, 16, 1, 10_000L, 60L * 60_000L, 60_000L,
            PrecisionTier.STANDARD);
        try (WheelStore store = new WheelStore(cfg, clock::get);
             TailIndex tail = new TailIndex(tmp, clock::get);
             GroupCommitBarrier barrier = new GroupCommitBarrier(store, tail, 1, 10_000)) {

            IntentLocationIndex idx = new IntentLocationIndex();
            ConcurrentIntentStore memStore = new ConcurrentIntentStore();
            AtomicBoolean running = new AtomicBoolean(true);
            AtomicLong seq = new AtomicLong();
            MetricsCollector mc = new MetricsCollector();
            PrecisionScheduler scheduler = new PrecisionScheduler(memStore, i ->
                CompletableFuture.completedFuture(DeliveryResult.DEAD_LETTER), null);
            PromotionDaemon daemon = new PromotionDaemon(store, tail, idx, clock::get, (i, loc) -> {}, 60_000L);
            ExecutorService cb = Executors.newVirtualThreadPerTaskExecutor();
            IntentCommandService svc = new IntentCommandService(
                memStore, scheduler, store, tail, barrier, idx, daemon,
                mc, cb, running, seq, null, PrecisionTier.STANDARD, 1L, 60L * 60_000L,
                PrecisionTierCatalog.defaultCatalog(), new com.loomq.tracing.IntentTraceStore());
            barrier.start();
            daemon.start();
            scheduler.start();

            String id = "r21-tail-wheel-0001";
            Intent cold = new Intent(id);
            cold.setExecuteAt(Instant.ofEpochMilli(clock.get() + 40L * 24 * 3600 * 1000)); // +40d → 超 30d 视界
            cold.setPrecisionTier(PrecisionTier.STANDARD);
            svc.createIntent(cold, AckMode.DURABLE);
            assertEquals(1, tail.size(), "cold intent must land in tail (beyond day-wheel horizon)");

            // 模拟 PromotionDaemon 提升:载入内存 + 调度(真实提升路径即此两步)
            memStore.save(cold);
            scheduler.schedule(cold);
            assertTrue(svc.fireNow(id), "promoted intent must be fire-now-able");

            // 修复前:wheel 新槽写入但 tail 旧记录残留(size 恒 1);
            // 修复后:tail→wheel 迁移清除旧记录,tail 归零。
            assertEquals(0, tail.size(),
                "tail→wheel migration must remove the stale tail record (leftover would resurrect on recovery)");
            cb.shutdown();
        }
    }
}
