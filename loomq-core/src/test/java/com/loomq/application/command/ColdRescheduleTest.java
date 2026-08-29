package com.loomq.application.command;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.application.scheduler.PrecisionScheduler;
import com.loomq.common.MetricsCollector;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.domain.intent.PrecisionTierCatalog;
import com.loomq.infrastructure.wheel.GroupCommitBarrier;
import com.loomq.infrastructure.wheel.IntentLocationIndex;
import com.loomq.infrastructure.wheel.PromotionDaemon;
import com.loomq.infrastructure.wheel.SlotLocation;
import com.loomq.infrastructure.wheel.TailIndex;
import com.loomq.infrastructure.wheel.WheelConfig;
import com.loomq.infrastructure.wheel.WheelStore;
import com.loomq.spi.DeliveryHandler;
import com.loomq.store.ConcurrentIntentStore;
import com.loomq.tracing.IntentTraceStore;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * 冷改期(round 13):updateIntent 对冷 Intent 生效。
 * 夹具镜像 CancelColdConcurrencyTest,但用真实时钟——updateCold 路由以
 * System.currentTimeMillis() 判热窗口,假时钟会把未来 Intent 判成过期。
 */
class ColdRescheduleTest {
    private static final long HOT_BOUNDARY_MS = 60L * 60_000L;
    private static final long PROMOTION_LEAD_MS = 60_000L;

    @TempDir Path tmp;

    /** 测试夹具:promote 回调可注入(竞态测试分段门控用,null = no-op)。 */
    private static final class Fx implements AutoCloseable {
        final WheelStore store;
        final TailIndex tail;
        final GroupCommitBarrier barrier;
        final IntentLocationIndex idx;
        final ConcurrentIntentStore memStore;
        final PrecisionScheduler scheduler;
        final PromotionDaemon daemon;
        final IntentCommandService svc;
        final MetricsCollector mc;

        Fx(Path tmp, BiConsumer<Intent, SlotLocation> onHotPromotion) {
            WheelConfig cfg = new WheelConfig(tmp.toString(), "t", 30, 16, 1, 10_000L,
                HOT_BOUNDARY_MS, PROMOTION_LEAD_MS, PrecisionTier.STANDARD);
            store = new WheelStore(cfg, System::currentTimeMillis);
            tail = new TailIndex(tmp, System::currentTimeMillis);
            barrier = new GroupCommitBarrier(store, tail, 1, 10_000);
            idx = new IntentLocationIndex();
            memStore = new ConcurrentIntentStore();
            AtomicBoolean running = new AtomicBoolean(true);
            AtomicLong seq = new AtomicLong();
            mc = new MetricsCollector();
            scheduler = new PrecisionScheduler(memStore, intent ->
                CompletableFuture.completedFuture(DeliveryHandler.DeliveryResult.DEAD_LETTER), null);
            daemon = new PromotionDaemon(store, tail, idx, System::currentTimeMillis,
                onHotPromotion != null ? onHotPromotion : (i, l) -> { }, PROMOTION_LEAD_MS);
            svc = new IntentCommandService(memStore, scheduler,
                new IntentCommandService.PhtwStack(store, tail, barrier, idx, daemon),
                mc, java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor(),
                running, seq, null,
                new IntentCommandService.CommandConfig(PrecisionTier.STANDARD, 1L, HOT_BOUNDARY_MS,
                    PrecisionTierCatalog.defaultCatalog()),
                new IntentTraceStore());
            barrier.start();
            daemon.start();
            scheduler.start();
        }

        /** 冷 Intent 落 wheel(+2h,revision=1,不进 memStore),镜像 cancelCold 测试 plant。 */
        Intent plantColdWheel(String id, Map<String, String> tags) {
            Intent cold = new Intent(id);
            cold.setExecuteAt(Instant.now().plus(2, ChronoUnit.HOURS));
            cold.setPrecisionTier(PrecisionTier.STANDARD);
            cold.transitionTo(IntentStatus.SCHEDULED);
            cold.incrementRevision();
            if (tags != null) {
                cold.setTags(tags);
            }
            SlotLocation loc = store.put(cold);
            idx.put(cold.getIntentId(), loc);
            return cold;
        }

        /** 冷 Intent 落 tail(+31d,revision=1),镜像 cancelCold tail 测试 plant。 */
        Intent plantColdTail(String id, Map<String, String> tags) {
            long execMs = System.currentTimeMillis() + 31L * 24 * 60 * 60 * 1000L;
            Intent cold = new Intent(id);
            cold.setExecuteAt(Instant.ofEpochMilli(execMs));
            cold.setPrecisionTier(PrecisionTier.STANDARD);
            cold.transitionTo(IntentStatus.SCHEDULED);
            cold.incrementRevision();
            if (tags != null) {
                cold.setTags(tags);
            }
            tail.put(cold);
            idx.put(cold.getIntentId(), SlotLocation.tail(execMs));
            return cold;
        }

        IntentCommandService svc() { return svc; }

        ConcurrentIntentStore memStore() { return memStore; }

        IntentLocationIndex idx() { return idx; }

        WheelStore store() { return store; }

        @Override public void close() {
            scheduler.stop();
            daemon.close();
            barrier.close();
            tail.close();
            store.close();
        }
    }

    @Test
    @org.junit.jupiter.api.DisplayName("冷 content-only 更新:持久化新槽、不进内存、revision+1、仍冷")
    void coldContentUpdatePersistsAndKeepsCold() {
        try (Fx fx = new Fx(tmp, null)) {
            Intent planted = fx.plantColdWheel("intent_coldupd0001", Map.of("v", "old"));
            long plantRev = planted.getRevision();

            Optional<Intent> updated = fx.svc().updateIntent("intent_coldupd0001",
                i -> i.setTags(Map.of("v", "new")), null);

            assertTrue(updated.isPresent());
            assertEquals(Map.of("v", "new"), updated.get().getTags());
            assertEquals(plantRev + 1, updated.get().getRevision());
            assertNull(fx.memStore().findByIdInternal("intent_coldupd0001"), "窗口外不热载");
            SlotLocation loc = fx.idx().get("intent_coldupd0001");
            assertNotNull(loc);
            assertFalse(loc.inTail());
            Intent onDisk = fx.store().readSlot(loc);
            assertEquals(Map.of("v", "new"), onDisk.getTags());
            assertEquals(plantRev + 1, onDisk.getRevision());
        }
    }

    @Test
    @org.junit.jupiter.api.DisplayName("冷改期 wheel→wheel:索引指向新槽,新 executeAt 落盘")
    void coldRescheduleWheelToWheel() {
        try (Fx fx = new Fx(tmp, null)) {
            Intent planted = fx.plantColdWheel("intent_w2w00000001", Map.of("v", "old"));
            long plantRev = planted.getRevision();
            Instant newAt = Instant.now().plus(3, ChronoUnit.HOURS);

            Optional<Intent> updated = fx.svc().updateIntent("intent_w2w00000001",
                i -> { }, newAt);

            assertTrue(updated.isPresent());
            assertEquals(newAt, updated.get().getExecuteAt());
            SlotLocation loc = fx.idx().get("intent_w2w00000001");
            assertFalse(loc.inTail());
            Intent onDisk = fx.store().readSlot(loc);
            assertEquals(newAt, onDisk.getExecuteAt());
            assertEquals(plantRev + 1, onDisk.getRevision());
            assertNull(fx.memStore().findByIdInternal("intent_w2w00000001"));
        }
    }

    @Test
    @org.junit.jupiter.api.DisplayName("冷改期入热窗口(+30s<lead):热载 upsert+schedule,索引仍指新槽")
    void coldRescheduleIntoHotWindow() {
        try (Fx fx = new Fx(tmp, null)) {
            fx.plantColdWheel("intent_hotwin000001", Map.of("v", "old"));
            Instant newAt = Instant.now().plusSeconds(30);   // < promotionLead 60s:cohort 立即可 firing,见热副本则 no-op

            Optional<Intent> updated = fx.svc().updateIntent("intent_hotwin000001",
                i -> i.setTags(Map.of("v", "new")), newAt);

            assertTrue(updated.isPresent());
            Intent hot = fx.memStore().findByIdInternal("intent_hotwin000001");
            assertNotNull(hot, "窗口内必须热载");
            assertEquals(IntentStatus.SCHEDULED, hot.getStatus());
            assertEquals(newAt, hot.getExecuteAt());
            assertEquals(Map.of("v", "new"), hot.getTags());
            assertNotNull(fx.idx().get("intent_hotwin000001"), "热载后索引不清(活 intent 仍可冷取消)");
        }
    }

    @Test
    @org.junit.jupiter.api.DisplayName("冷改期 wheel→tail:索引变 tail 占位,tail 记录为新 revision")
    void coldRescheduleWheelToTail() {
        try (Fx fx = new Fx(tmp, null)) {
            Intent planted = fx.plantColdWheel("intent_w2t00000001", Map.of("v", "old"));
            long plantRev = planted.getRevision();
            Instant far = Instant.now().plus(40, ChronoUnit.DAYS);

            Optional<Intent> updated = fx.svc().updateIntent("intent_w2t00000001",
                i -> { }, far);

            assertTrue(updated.isPresent());
            SlotLocation loc = fx.idx().get("intent_w2t00000001");
            assertTrue(loc.inTail(), "落 tail 后索引为 tail 占位");
            Intent inTail = fx.tail.readById("intent_w2t00000001");
            assertNotNull(inTail);
            assertEquals(far, inTail.getExecuteAt());
            assertEquals(plantRev + 1, inTail.getRevision());
            assertNull(fx.memStore().findByIdInternal("intent_w2t00000001"));
        }
    }

    @Test
    @org.junit.jupiter.api.DisplayName("冷改期 tail→tail:byId 单活换位,后写胜")
    void coldRescheduleTailToTail() {
        try (Fx fx = new Fx(tmp, null)) {
            Intent planted = fx.plantColdTail("intent_t2t00000001", Map.of("v", "old"));
            long plantRev = planted.getRevision();
            Instant newer = Instant.now().plus(32, ChronoUnit.DAYS);

            Optional<Intent> updated = fx.svc().updateIntent("intent_t2t00000001",
                i -> i.setTags(Map.of("v", "new")), newer);

            assertTrue(updated.isPresent());
            Intent inTail = fx.tail.readById("intent_t2t00000001");
            assertNotNull(inTail);
            assertEquals(newer, inTail.getExecuteAt());
            assertEquals(plantRev + 1, inTail.getRevision());
            assertEquals(Map.of("v", "new"), inTail.getTags());
        }
    }

    @Test
    @org.junit.jupiter.api.DisplayName("冷改期 tail→wheel:旧 tail 记录被迁移清除,不残留幽灵")
    void coldRescheduleTailToWheel() {
        try (Fx fx = new Fx(tmp, null)) {
            Intent planted = fx.plantColdTail("intent_t2w00000001", Map.of("v", "old"));
            long plantRev = planted.getRevision();
            Instant near = Instant.now().plus(2, ChronoUnit.HOURS);

            Optional<Intent> updated = fx.svc().updateIntent("intent_t2w00000001",
                i -> { }, near);

            assertTrue(updated.isPresent());
            SlotLocation loc = fx.idx().get("intent_t2w00000001");
            assertFalse(loc.inTail());
            assertNull(fx.tail.readById("intent_t2w00000001"), "tail→wheel 迁移必须清旧 tail 记录");
            Intent onDisk = fx.store().readSlot(loc);
            assertEquals(near, onDisk.getExecuteAt());
            assertEquals(plantRev + 1, onDisk.getRevision());
        }
    }

    @Test
    @org.junit.jupiter.api.DisplayName("校验失败矩阵:R16/R21/Validator 抛 IAE,磁盘/索引/revision 原样")
    void coldUpdateValidationFailuresLeaveDiskUntouched() {
        try (Fx fx = new Fx(tmp, null)) {
            Intent planted = fx.plantColdWheel("intent_coldinv0001", Map.of("v", "old"));
            SlotLocation locBefore = fx.idx().get("intent_coldinv0001");

            // R16:executeAt 置 null
            assertThrows(IllegalArgumentException.class,
                () -> fx.svc().updateIntent("intent_coldinv0001", i -> i.setExecuteAt(null), null));
            // R21:updater 直接转终态
            assertThrows(IllegalArgumentException.class,
                () -> fx.svc().updateIntent("intent_coldinv0001", i -> i.transitionTo(IntentStatus.CANCELED), null));
            // Validator:deadline 早于 executeAt
            assertThrows(IllegalArgumentException.class,
                () -> fx.svc().updateIntent("intent_coldinv0001",
                    i -> i.setDeadline(i.getExecuteAt().minusSeconds(60)), null));

            // R22 镜像:newExecuteAt 应用后整体校验——改期越过 deadline 必须拒绝
            // (校验先于改期会漏检:deadline 约束以旧 executeAt 为锚通过,违约态被落盘)。
            Intent withDeadline = new Intent("intent_coldinv0002");
            withDeadline.setExecuteAt(Instant.now().plus(2, ChronoUnit.HOURS));
            withDeadline.setDeadline(Instant.now().plus(3, ChronoUnit.HOURS));
            withDeadline.setPrecisionTier(PrecisionTier.STANDARD);
            withDeadline.transitionTo(IntentStatus.SCHEDULED);
            withDeadline.incrementRevision();
            SlotLocation dlLoc = fx.store.put(withDeadline);
            fx.idx.put(withDeadline.getIntentId(), dlLoc);
            assertThrows(IllegalArgumentException.class,
                () -> fx.svc().updateIntent("intent_coldinv0002", i -> { },
                    withDeadline.getDeadline().plusSeconds(60)),
                "改期越过 deadline 必须抛 IAE(校验覆盖 newExecuteAt 应用后的最终态)");

            SlotLocation locAfter = fx.idx().get("intent_coldinv0001");
            assertEquals(locBefore, locAfter, "校验失败索引不得迁移");
            Intent onDisk = fx.store().readSlot(locAfter);
            assertEquals(planted.getRevision(), onDisk.getRevision(), "校验失败 revision 不变");
            assertEquals(Map.of("v", "old"), onDisk.getTags(), "校验失败磁盘内容不变");
            assertEquals(dlLoc, fx.idx().get("intent_coldinv0002"), "newExecuteAt 越界校验失败索引不得迁移");
            assertEquals(withDeadline.getRevision(),
                fx.store().readSlot(dlLoc).getRevision(), "newExecuteAt 越界校验失败 revision 不变");
        }
    }

    @Test
    @org.junit.jupiter.api.DisplayName("不存在 → empty;冷取消后 → empty(墓碑当不存在)")
    void coldUpdateNonexistentOrTerminalReturnsEmpty() {
        try (Fx fx = new Fx(tmp, null)) {
            assertFalse(fx.svc().updateIntent("intent_absent000001", i -> { }, null).isPresent());

            fx.plantColdWheel("intent_colddead0001", Map.of("v", "x"));
            assertTrue(fx.svc().cancelIntent("intent_colddead0001"));
            assertFalse(fx.svc().updateIntent("intent_colddead0001", i -> { }, null).isPresent(),
                "冷取消后索引已清,update 必须返回 empty");
        }
    }

    @Test
    @org.junit.jupiter.api.DisplayName("fireNow 冷 Intent 仍返回 false(未实现,契约守卫)")
    void coldFireNowStillFalse() {
        try (Fx fx = new Fx(tmp, null)) {
            fx.plantColdWheel("intent_coldfire0001", null);
            assertFalse(fx.svc().fireNow("intent_coldfire0001"));
        }
    }

    @Test
    @Tag("slow")
    @org.junit.jupiter.api.DisplayName("spec 4.4 缺口:在途旧 promote 回滚删除热副本 → cohort 安全网重热载自愈")
    void inFlightPromoteRollbackHealedBySafetyNet() throws Exception {
        CountDownLatch promoteReadSlot = new CountDownLatch(1);   // promote 已读旧槽,停门
        CountDownLatch allowGuard = new CountDownLatch(1);        // 放行 guard 检查(此时 update 未开始)
        CountDownLatch guardSettled = new CountDownLatch(1);      // guard 读取完成后放行 update 提交
        CountDownLatch allowUpsert = new CountDownLatch(1);       // update 完成后放行 upsert 旧内容
        CountDownLatch allowRollback = new CountDownLatch(1);     // 放行回滚删除(伤害点)
        CountDownLatch done = new CountDownLatch(1);
        CountDownLatch allowHeal = new CountDownLatch(1);         // 放行安全网 promote 重热载(伤害态取证后)
        CountDownLatch healDone = new CountDownLatch(1);          // 安全网自愈完成信号
        AtomicInteger promotionSeq = new AtomicInteger();         // 第 1 次=在途旧 cohort,第 2 次=安全网新槽
        AtomicReference<Fx> fxRef = new AtomicReference<>();
        // 镜像 LoomqEngine promote 回调(:174-189)并四段门控,确定性复现:
        // guard 通过 → update 冷改期(热载新副本+post-check) → promote upsert 旧内容 → 回滚 delete → 内存空
        // guardSettled 串行化 guard 读与 update 提交:guard 读 → update 提交 → upsert → 回滚,
        // 杜绝 CI 饥饿下 guard 见新副本跳过回滚导致伤害态断言误判。
        Fx fx = new Fx(tmp, (hot, loc) -> {
            Fx f = fxRef.get();
            try {
                boolean first = promotionSeq.incrementAndGet() == 1;
                if (first) {
                    promoteReadSlot.countDown();
                    allowGuard.await();
                } else {
                    allowHeal.await();               // 安全网 promote:等伤害态断言完再自愈
                }
                boolean absent = f.memStore.findByIdInternal(hot.getIntentId()) == null;
                if (first) {
                    guardSettled.countDown();   // guard 已读取(结果=absent);update 须待此门后提交
                }
                if (absent) {
                    if (first) {
                        allowUpsert.await();
                    }
                    f.memStore.upsert(hot);                        // 旧内容覆盖
                    f.scheduler.schedule(hot);
                    if (first) {
                        allowRollback.await();
                    }
                    SlotLocation after = f.idx.get(hot.getIntentId());
                    if (after == null || !after.equals(loc)) {     // latest 已指新槽 → 无条件回滚删除
                        f.scheduler.removeFromSchedule(hot);
                        f.memStore.delete(hot.getIntentId());
                    }
                }
                if (first) {
                    done.countDown();
                } else {
                    healDone.countDown();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        fxRef.set(fx);
        try (fx) {
            String id = "intent_race0000001";
            Intent planted = fx.plantColdWheel(id, Map.of("v", "old"));
            long plantRev = planted.getRevision();
            // 老 cohort 立即到期(wakeAt=now-1s-60s lead):唯一触发方是后台 loop 线程(register 已
            // unpark 它)。不可在主线程 tickOnce 触发——tickOnce 内联执行本回调,主线程会阻塞在
            // allowGuard.await() 上,而唯一 countDown 它的正是主线程自身 → 死锁。
            fx.daemon.register(id, fx.idx.get(id), System.currentTimeMillis() - 1_000);
            assertTrue(promoteReadSlot.await(5, TimeUnit.SECONDS), "promote 已读旧槽并停门");
            allowGuard.countDown();                                 // 放行 guard 检查(findById==null)
            assertTrue(guardSettled.await(5, TimeUnit.SECONDS),
                "promote guard 检查必须先于 update 提交完成,否则 guard 见新副本跳过回滚,伤害态断言误判");
            Optional<Intent> updated;
            try (ExecutorService ex = Executors.newSingleThreadExecutor()) {
                // close 语义=等待终止:get 抛异常时工作线程不再泄漏(Java 19+ ExecutorService 实现 AutoCloseable)
                updated = ex.submit(() -> fx.svc.updateIntent(id,
                    i -> i.setTags(Map.of("v", "new")), Instant.now().plusSeconds(30))).get(10, TimeUnit.SECONDS);
            }
            assertTrue(updated.isPresent(), "冷改期成功(窗口内 +30s)");
            assertEquals(plantRev + 1, updated.get().getRevision());
            assertNotNull(fx.memStore.findByIdInternal(id), "窗口内已热载新副本");
            allowUpsert.countDown();                                // promote:upsert 旧内容,停 allowRollback
            allowRollback.countDown();                              // promote:复核 latest≠h.loc → delete(id)
            assertTrue(done.await(5, TimeUnit.SECONDS));
            assertNull(fx.memStore.findByIdInternal(id), "回滚删除后内存为空——4.4 伤害场景复现");
            // 安全网:新 cohort wakeAt=+30s-60s<now,旧 cohort 回调返回后 loop 空闲即消费它
            // (停在 allowHeal)→ 此处放行重热载新槽(自愈)。allowHeal 放行先于消费完成,
            // 伤害态断言必先于自愈执行;healDone 返回即证明 loop 已消费该 cohort,无需兜底驱动。
            allowHeal.countDown();
            assertTrue(healDone.await(5, TimeUnit.SECONDS), "cohort 安全网必须重热载");
            Intent healed = fx.memStore.findByIdInternal(id);
            assertNotNull(healed, "cohort 安全网必须重热载");
            assertEquals(plantRev + 1, healed.getRevision());
            assertEquals(Map.of("v", "new"), healed.getTags());
            assertEquals(IntentStatus.SCHEDULED, healed.getStatus());
        }
    }

    @Test
    @Tag("slow")
    @org.junit.jupiter.api.DisplayName("冷改期×冷取消并发(tail):共享冷锁串行化,终态二值一致无复活")
    void coldUpdateAndCancelSerializeOnTail() throws Exception {
        try (Fx fx = new Fx(tmp, null)) {
            String id = "intent_tailrace001";
            Intent planted = fx.plantColdTail(id, Map.of("v", "0"));
            long plantRev = planted.getRevision();
            long cancelledBefore = fx.mc.getIntentsCancelledTotal();

            int n = 8;
            CountDownLatch ready = new CountDownLatch(n);
            CountDownLatch fire = new CountDownLatch(1);
            AtomicInteger updateOk = new AtomicInteger();
            AtomicInteger cancelOk = new AtomicInteger();
            try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
                for (int i = 0; i < n; i++) {
                    final boolean doUpdate = i % 2 == 0;
                    pool.submit(() -> {
                        ready.countDown();
                        try {
                            fire.await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                        if (doUpdate) {
                            if (fx.svc.updateIntent(id, x -> x.setTags(Map.of("v", "1")), null).isPresent()) {
                                updateOk.incrementAndGet();
                            }
                        } else if (fx.svc.cancelIntent(id)) {
                            cancelOk.incrementAndGet();
                        }
                    });
                }
                assertTrue(ready.await(10, TimeUnit.SECONDS), "并发线程就绪");
                fire.countDown();
                pool.shutdown();
                assertTrue(pool.awaitTermination(15, TimeUnit.SECONDS));
            }
            // 终态二值一致:取消最后赢 → 索引空 + tail 无活记录 + 计数恰 1;改期最后赢 → 索引在 + 活记录 SCHEDULED
            boolean cancelled = fx.idx.get(id) == null;
            if (cancelled) {
                assertEquals(1, cancelOk.get(), "至多一次取消成功(共享锁串行 + terminal 拒绝)");
                assertNull(fx.tail.readById(id), "已取消则 tail 无活记录(tombstone)");
                assertNull(fx.memStore.findByIdInternal(id), "R13a:取消赢后不得有 ghost 热副本(路由重读索引弃用)");
                assertEquals(1L, fx.mc.getIntentsCancelledTotal() - cancelledBefore);
            } else {
                assertEquals(0, cancelOk.get(), "活态则取消必须全失败");
                Intent live = fx.tail.readById(id);
                assertNotNull(live);
                assertEquals(IntentStatus.SCHEDULED, live.getStatus());
                assertTrue(live.getRevision() >= plantRev + 1);
                assertTrue(updateOk.get() >= 1);
            }
        }
    }
}
