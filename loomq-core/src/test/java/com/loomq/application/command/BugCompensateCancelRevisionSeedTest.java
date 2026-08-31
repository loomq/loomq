package com.loomq.application.command;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.LoomqEngine;
import com.loomq.application.scheduler.PrecisionScheduler;
import com.loomq.common.MetricsCollector;
import com.loomq.domain.intent.AckMode;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.domain.intent.PrecisionTierCatalog;
import com.loomq.infrastructure.wheel.GroupCommitBarrier;
import com.loomq.infrastructure.wheel.IntentLocationIndex;
import com.loomq.infrastructure.wheel.PromotionDaemon;
import com.loomq.infrastructure.wheel.SlotEntry;
import com.loomq.infrastructure.wheel.TailIndex;
import com.loomq.infrastructure.wheel.WheelConfig;
import com.loomq.infrastructure.wheel.WheelStore;
import com.loomq.spi.DeliveryHandler;
import com.loomq.store.ConcurrentIntentStore;
import com.loomq.tracing.IntentTraceStore;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * C18-3(r18): create 失败补偿段(compensateCancel)须在冷锁内做 R8 式 revision 种子。
 * 冷命令在 create persist 与补偿段之间先手时,补偿与冷写同 revision 平局——恢复按
 * max-revision(严格 >)+ 先扫者胜仲裁可能复活 SCHEDULED(F5 残留收口)。
 * 平局仲裁确定化:冷写 newExecuteAt 早于原 executeAt → 冷写槽 bucketKey 更小 → 恢复先扫。
 *
 * <p>先手走法说明(与计划字面代码的偏差):先手必须是<b>真正的冷路径写</b>——若直接调
 * {@code svc.updateIntent},此时热副本刚被 createIntent 存入 memStore,updateIntent 走热路径
 * 原地变异活对象(revision 已被推进),补偿天然严格更高,平局不可复现、测试空洞绿。故先手
 * 先摘热副本(demote 等价:冷命令视角该 id 不在热内存,如另一写者已收口热副本),迫使
 * updateIntent 委托 updateCold——冷锁内对磁盘 transient 解码副本读-改-写,不触碰活对象。</p>
 */
class BugCompensateCancelRevisionSeedTest {

    private static final long HOT_BOUNDARY_MS = 60L * 60_000L;
    private static final long PROMOTION_LEAD_MS = 60_000L;

    @TempDir Path tmp;

    /** create 热载调度段(schedule 首调)注入"冷命令先手"后抛错——确定性触发 compensateCancel。 */
    private static final class ColdFirstMoveScheduler extends PrecisionScheduler {
        final AtomicBoolean firstCall = new AtomicBoolean(false);
        private volatile Runnable coldFirstMove = () -> { };

        ColdFirstMoveScheduler(ConcurrentIntentStore store, DeliveryHandler handler) {
            super(store, handler, null);
        }

        void onFirstSchedule(Runnable r) {
            this.coldFirstMove = r;
        }

        @Override public void schedule(Intent intent) {
            if (firstCall.compareAndSet(false, true)) {
                coldFirstMove.run();   // 冷命令先手:冷路径写 R_create+1(不触碰活对象)
                throw new IllegalStateException("injected schedule failure (C18-3)");
            }
            super.schedule(intent);    // updateCold 热载路由等后续调用放行(未到期,测试窗内不投递)
        }
    }

    /**
     * 本地组件栈(镜像 HotColdStraddleTest.compensateCancelSerializesOnColdLock 装配)。
     * WheelConfig 用引擎默认(defaultConfig)保证桶文件布局与 e2 引擎跨进程兼容;
     * barrier 必须启动——补偿段 persistToWheel(durable=true) 依赖 awaitCommit。
     * scheduler 不启动(桩的 schedule 只做结构操作)。
     */
    private static final class LocalStack implements AutoCloseable {
        final WheelStore store;
        final TailIndex tail;
        final GroupCommitBarrier barrier;
        final IntentLocationIndex idx;
        final ConcurrentIntentStore memStore;
        final ColdFirstMoveScheduler scheduler;
        final PromotionDaemon daemon;
        final IntentCommandService svc;

        LocalStack(Path dir, ConcurrentIntentStore memStore, ColdFirstMoveScheduler scheduler) {
            WheelConfig cfg = WheelConfig.defaultConfig().withDataDir(dir.toString());
            this.store = new WheelStore(cfg, System::currentTimeMillis);
            this.tail = new TailIndex(dir, System::currentTimeMillis);
            this.barrier = new GroupCommitBarrier(store, tail, 1, 10_000);
            this.idx = new IntentLocationIndex();
            this.memStore = memStore;
            this.scheduler = scheduler;
            this.daemon = new PromotionDaemon(store, tail, idx, System::currentTimeMillis,
                (i, l) -> { }, PROMOTION_LEAD_MS);
            this.svc = new IntentCommandService(memStore, scheduler,
                new IntentCommandService.PhtwStack(store, tail, barrier, idx, daemon),
                new MetricsCollector(), Executors.newVirtualThreadPerTaskExecutor(),
                new AtomicBoolean(true), new AtomicLong(), null,
                new IntentCommandService.CommandConfig(PrecisionTier.STANDARD, 1L, HOT_BOUNDARY_MS,
                    PrecisionTierCatalog.defaultCatalog()),
                new IntentTraceStore());
            barrier.start();
        }

        @Override public void close() {
            daemon.close();
            barrier.close();
            tail.close();
            store.close();
        }
    }

    @Test
    @org.junit.jupiter.api.Timeout(60)
    @org.junit.jupiter.api.DisplayName("C18-3: 冷命令先手时补偿 CANCELED 须种子到严格更高 revision——恢复仲裁确定性胜出")
    void coldWriterFirstMoveCompensateCancelMustWinTie() throws Exception {
        Path dir = tmp.resolve("c18t3");
        String id = "intent_c18t30001";
        long now = System.currentTimeMillis();
        Instant originalExec = Instant.ofEpochMilli(now + 30_000L);   // 热窗口内(60min)→ e2 可热载
        Instant earlierExec = Instant.ofEpochMilli(now + 10_000L);    // 更早 → bucketKey 更小 → 恢复先扫

        ConcurrentIntentStore memStore = new ConcurrentIntentStore();
        ColdFirstMoveScheduler scheduler = new ColdFirstMoveScheduler(memStore,
            intent -> CompletableFuture.completedFuture(DeliveryHandler.DeliveryResult.DEAD_LETTER));
        try (LocalStack stack = new LocalStack(dir, memStore, scheduler)) {
            // 冷命令先手:先摘热副本(demote 等价)迫使 updateIntent 走 updateCold 冷路径——
            // 冷锁内对磁盘解码副本写 R_create+1@earlierExec,活对象 revision 停在 R_create。
            scheduler.onFirstSchedule(() -> {
                stack.memStore.delete(id);
                stack.svc.updateIntent(id,
                    i -> i.setTags(Map.of("v", "cold-first")), earlierExec);
            });

            Intent created = new Intent(id);
            created.setExecuteAt(originalExec);
            created.setPrecisionTier(PrecisionTier.STANDARD);
            // create 失败(桩在热载调度段抛错)→ 确定性触发 compensateCancel
            assertThrows(RuntimeException.class, () -> stack.svc.createIntent(created, AckMode.ASYNC));
        }

        // e2:引擎级恢复 + 投递(真实 WheelRecovery max-revision 仲裁)。
        // 修复前(红):SCHEDULED R2 与补偿 CANCELED R2 平局 → 先扫槽(更早 bucketKey 的
        //   SCHEDULED R2)胜 → 幽灵热载投递 getIntent 非空、delivered==1;
        // 修复后(绿):CANCELED R3 严格胜 → 终态不热载 getIntent 空、delivered==0。
        AtomicInteger delivered = new AtomicInteger();
        try (LoomqEngine e2 = LoomqEngine.builder()
                .dataDir(dir).nodeId("e2")
                .deliveryHandler(i -> {
                    delivered.incrementAndGet();
                    return CompletableFuture.completedFuture(DeliveryHandler.DeliveryResult.SUCCESS);
                })
                .build()) {
            e2.start();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (System.nanoTime() < deadline && delivered.get() == 0) {
                Thread.sleep(20);
            }
            assertTrue(e2.getIntent(id).isEmpty(),
                "补偿 CANCELED 须严格高于冷写 revision(终态不热载);热载即幽灵 SCHEDULED 复活");
            assertEquals(0, delivered.get(), "创建失败的 Intent 不得投递");
        }
    }

    @Test
    @org.junit.jupiter.api.Timeout(60)
    @org.junit.jupiter.api.DisplayName("C18-3 行为锁: 无冷写先手时补偿 revision 恒为 R_create+1(种子等价 no-op)")
    void noRivalCompensateRevisionStaysCreatePlusOne() throws Exception {
        Path dir = tmp.resolve("c18t3ctl");
        String id = "intent_c18t3c001";

        ConcurrentIntentStore memStore = new ConcurrentIntentStore();
        ColdFirstMoveScheduler scheduler = new ColdFirstMoveScheduler(memStore,
            intent -> CompletableFuture.completedFuture(DeliveryHandler.DeliveryResult.DEAD_LETTER));
        try (LocalStack stack = new LocalStack(dir, memStore, scheduler)) {
            // 无冷写先手:首调直接抛错(不设 onFirstSchedule)
            Intent created = new Intent(id);
            created.setExecuteAt(Instant.now().plusSeconds(30));
            created.setPrecisionTier(PrecisionTier.STANDARD);
            assertThrows(RuntimeException.class, () -> stack.svc.createIntent(created, AckMode.ASYNC));

            // 磁盘 CANCELED 槽 revision == R_create+1 == 2(histMax == R_create → seed 等价 no-op)
            Intent canceled = null;
            Iterator<SlotEntry> it = stack.store.scanSlotsFrom(Instant.ofEpochMilli(0));
            while (it.hasNext()) {
                SlotEntry e = it.next();
                if (e.intent().getIntentId().equals(id)
                        && e.intent().getStatus() == IntentStatus.CANCELED) {
                    canceled = e.intent();
                }
            }
            assertTrue(canceled != null, "补偿 CANCELED 槽必须落盘");
            assertEquals(2, canceled.getRevision(),
                "无先手写时种子等价 no-op:补偿 revision 恒为 R_create+1(常见路径零行为变更)");
        }
    }
}
