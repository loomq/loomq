package com.loomq.application.command;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.application.scheduler.PrecisionScheduler;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.domain.intent.PrecisionTierCatalog;
import com.loomq.infrastructure.wheel.GroupCommitBarrier;
import com.loomq.infrastructure.wheel.IntentLocationIndex;
import com.loomq.infrastructure.wheel.PromotionDaemon;
import com.loomq.infrastructure.wheel.SlotLocation;
import com.loomq.infrastructure.wheel.TailIndex;
import com.loomq.infrastructure.wheel.WheelStore;
import com.loomq.store.IntentStore;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * 冷改期(round 13):updateIntent 对冷 Intent 生效。
 * 夹具:共享 CommandStackFx(Options 真实时钟旋钮)——updateCold 路由以
 * System.currentTimeMillis() 判热窗口,假时钟会把未来 Intent 判成过期,故不可用假时钟。
 */
class ColdRescheduleTest {
    private static final long HOT_BOUNDARY_MS = 60L * 60_000L;
    private static final long PROMOTION_LEAD_MS = 60_000L;

    /** 冷改期场景固定旋钮:系统时钟、无种子、启动全件(与原 Fx 语义逐字一致)。 */
    private static final CommandStackFx.Options COLD_FX_OPTS =
        new CommandStackFx.Options(System::currentTimeMillis, null, false, true);

    @TempDir Path tmp;

    /** round 14:测试桩直构 IntentUpdater 所需的真实 reconciler(与 CommandStackFx 组件同源)。 */
    private static ColdHotReconciler newReconciler(CommandStackFx fx) {
        return new ColdHotReconciler(fx.memStore(), fx.scheduler(), fx.idx());
    }

    /** F4 测试桩:awaitDurableCommit 恒抛(提交后失败注入;persistToWheel 走真实协议)。 */
    private static final class PostCommitFailPersistence extends WheelPersistence {
        PostCommitFailPersistence(WheelStore store, TailIndex tail, GroupCommitBarrier barrier,
                                  IntentLocationIndex idx) {
            super(store, tail, barrier, idx);
        }

        @Override void awaitDurableCommit() {
            throw new IllegalStateException("simulated post-commit awaitCommit failure");
        }
    }

    /** F4 测试桩:路由恒抛(提交后失败注入)。 */
    private static final class RouteFailingUpdater extends IntentUpdater {
        RouteFailingUpdater(IntentStore memStore, PrecisionScheduler scheduler, IntentLocationIndex idx,
                            PrecisionTierCatalog catalog, WheelPersistence persistence,
                            PromotionDaemon daemon, ColdHotReconciler reconciler, long hotBoundaryMs) {
            super(memStore, scheduler, idx, catalog, persistence, daemon, reconciler, hotBoundaryMs);
        }

        @Override void routeColdAfterPersist(String intentId, Intent cold, SlotLocation newLoc) {
            throw new IllegalStateException("simulated post-commit routing failure");
        }
    }

    /** 终审 F1 测试桩:persistToWheel 恒抛(提交前失败注入;镜像热 fireNow 提交前分支)。 */
    private static final class PreCommitFailPersistence extends WheelPersistence {
        PreCommitFailPersistence(WheelStore store, TailIndex tail, GroupCommitBarrier barrier,
                                 IntentLocationIndex idx) {
            super(store, tail, barrier, idx);
        }

        @Override SlotLocation persistToWheel(Intent intent, boolean durable) {
            throw new IllegalStateException("simulated pre-commit persistToWheel failure");
        }
    }

    @Test
    @org.junit.jupiter.api.DisplayName("冷 content-only 更新:持久化新槽、不进内存、revision+1、仍冷")
    void coldContentUpdatePersistsAndKeepsCold() {
        try (CommandStackFx fx = new CommandStackFx(tmp, COLD_FX_OPTS)) {
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
        try (CommandStackFx fx = new CommandStackFx(tmp, COLD_FX_OPTS)) {
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
        try (CommandStackFx fx = new CommandStackFx(tmp, COLD_FX_OPTS)) {
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
        try (CommandStackFx fx = new CommandStackFx(tmp, COLD_FX_OPTS)) {
            Intent planted = fx.plantColdWheel("intent_w2t00000001", Map.of("v", "old"));
            long plantRev = planted.getRevision();
            Instant far = Instant.now().plus(40, ChronoUnit.DAYS);

            Optional<Intent> updated = fx.svc().updateIntent("intent_w2t00000001",
                i -> { }, far);

            assertTrue(updated.isPresent());
            SlotLocation loc = fx.idx().get("intent_w2t00000001");
            assertTrue(loc.inTail(), "落 tail 后索引为 tail 占位");
            Intent inTail = fx.tail().readById("intent_w2t00000001");
            assertNotNull(inTail);
            assertEquals(far, inTail.getExecuteAt());
            assertEquals(plantRev + 1, inTail.getRevision());
            assertNull(fx.memStore().findByIdInternal("intent_w2t00000001"));
        }
    }

    @Test
    @org.junit.jupiter.api.DisplayName("冷改期 tail→tail:byId 单活换位,后写胜")
    void coldRescheduleTailToTail() {
        try (CommandStackFx fx = new CommandStackFx(tmp, COLD_FX_OPTS)) {
            Intent planted = fx.plantColdTail("intent_t2t00000001", Map.of("v", "old"));
            long plantRev = planted.getRevision();
            Instant newer = Instant.now().plus(32, ChronoUnit.DAYS);

            Optional<Intent> updated = fx.svc().updateIntent("intent_t2t00000001",
                i -> i.setTags(Map.of("v", "new")), newer);

            assertTrue(updated.isPresent());
            Intent inTail = fx.tail().readById("intent_t2t00000001");
            assertNotNull(inTail);
            assertEquals(newer, inTail.getExecuteAt());
            assertEquals(plantRev + 1, inTail.getRevision());
            assertEquals(Map.of("v", "new"), inTail.getTags());
        }
    }

    @Test
    @org.junit.jupiter.api.DisplayName("冷改期 tail→wheel:旧 tail 记录被迁移清除,不残留幽灵")
    void coldRescheduleTailToWheel() {
        try (CommandStackFx fx = new CommandStackFx(tmp, COLD_FX_OPTS)) {
            Intent planted = fx.plantColdTail("intent_t2w00000001", Map.of("v", "old"));
            long plantRev = planted.getRevision();
            Instant near = Instant.now().plus(2, ChronoUnit.HOURS);

            Optional<Intent> updated = fx.svc().updateIntent("intent_t2w00000001",
                i -> { }, near);

            assertTrue(updated.isPresent());
            SlotLocation loc = fx.idx().get("intent_t2w00000001");
            assertFalse(loc.inTail());
            assertNull(fx.tail().readById("intent_t2w00000001"), "tail→wheel 迁移必须清旧 tail 记录");
            Intent onDisk = fx.store().readSlot(loc);
            assertEquals(near, onDisk.getExecuteAt());
            assertEquals(plantRev + 1, onDisk.getRevision());
        }
    }

    @Test
    @org.junit.jupiter.api.DisplayName("校验失败矩阵:R16/R21/Validator 抛 IAE,磁盘/索引/revision 原样")
    void coldUpdateValidationFailuresLeaveDiskUntouched() {
        try (CommandStackFx fx = new CommandStackFx(tmp, COLD_FX_OPTS)) {
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
            SlotLocation dlLoc = fx.store().put(withDeadline);
            fx.idx().put(withDeadline.getIntentId(), dlLoc);
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
        try (CommandStackFx fx = new CommandStackFx(tmp, COLD_FX_OPTS)) {
            assertFalse(fx.svc().updateIntent("intent_absent000001", i -> { }, null).isPresent());

            fx.plantColdWheel("intent_colddead0001", Map.of("v", "x"));
            assertTrue(fx.svc().cancelIntent("intent_colddead0001"));
            assertFalse(fx.svc().updateIntent("intent_colddead0001", i -> { }, null).isPresent(),
                "冷取消后索引已清,update 必须返回 empty");
        }
    }

    @Test
    @org.junit.jupiter.api.DisplayName("F4:awaitCommit 提交后失败不传播——返回成功,磁盘/索引为新槽")
    void coldUpdateSurvivesPostCommitAwaitFailure() {
        try (CommandStackFx fx = new CommandStackFx(tmp, COLD_FX_OPTS)) {
            Intent planted = fx.plantColdWheel("intent_f4await0001", Map.of("v", "old"));
            long plantRev = planted.getRevision();
            PostCommitFailPersistence stub = new PostCommitFailPersistence(
                fx.store(), fx.tail(), fx.barrier(), fx.idx());
            IntentUpdater updater = new IntentUpdater(fx.memStore(), fx.scheduler(), fx.idx(),
                PrecisionTierCatalog.defaultCatalog(), stub, fx.daemon(),
                newReconciler(fx), HOT_BOUNDARY_MS);

            Optional<Intent> updated = updater.updateIntent("intent_f4await0001",
                i -> i.setTags(Map.of("v", "new")), null);

            assertTrue(updated.isPresent(), "提交后 awaitCommit 失败必须按成功返回(C4-2)");
            assertEquals(plantRev + 1, updated.get().getRevision());
            SlotLocation loc = fx.idx().get("intent_f4await0001");
            assertNotNull(loc, "提交后失败索引不得回退");
            assertEquals(plantRev + 1, fx.store().readSlot(loc).getRevision());
            assertEquals(Map.of("v", "new"), fx.store().readSlot(loc).getTags());
        }
    }

    @Test
    @org.junit.jupiter.api.DisplayName("F4:路由提交后失败不传播——返回成功,磁盘/索引为新槽")
    void coldUpdateSurvivesPostCommitRoutingFailure() {
        try (CommandStackFx fx = new CommandStackFx(tmp, COLD_FX_OPTS)) {
            Intent planted = fx.plantColdWheel("intent_f4route0001", Map.of("v", "old"));
            long plantRev = planted.getRevision();
            RouteFailingUpdater updater = new RouteFailingUpdater(fx.memStore(), fx.scheduler(), fx.idx(),
                PrecisionTierCatalog.defaultCatalog(),
                new WheelPersistence(fx.store(), fx.tail(), fx.barrier(), fx.idx()), fx.daemon(),
                newReconciler(fx), HOT_BOUNDARY_MS);

            Optional<Intent> updated = updater.updateIntent("intent_f4route0001",
                i -> i.setTags(Map.of("v", "new")), null);

            assertTrue(updated.isPresent(), "提交后路由失败必须按成功返回(C4-2)");
            SlotLocation loc = fx.idx().get("intent_f4route0001");
            assertNotNull(loc);
            assertEquals(plantRev + 1, fx.store().readSlot(loc).getRevision());
            assertEquals(Map.of("v", "new"), fx.store().readSlot(loc).getTags());
        }
    }

    @Test
    @org.junit.jupiter.api.DisplayName("F3:tail→tail 改期后冷取消——索引清空,tail 无残留(回归锁)")
    void cancelAfterTailToTailRescheduleLeavesCleanIndex() {
        try (CommandStackFx fx = new CommandStackFx(tmp, COLD_FX_OPTS)) {
            fx.plantColdTail("intent_f3serial0001", Map.of("v", "0"));
            Instant newer = Instant.now().plus(32, ChronoUnit.DAYS);
            assertTrue(fx.svc().updateIntent("intent_f3serial0001", i -> { }, newer).isPresent());
            SlotLocation afterUpdate = fx.idx().get("intent_f3serial0001");
            assertTrue(afterUpdate.inTail(), "改期后仍落 tail(+32d)");

            assertTrue(fx.svc().cancelIntent("intent_f3serial0001"));

            assertNull(fx.idx().get("intent_f3serial0001"), "取消后索引必须清空(定向移除须命中改期后的新槽)");
            assertNull(fx.tail().readById("intent_f3serial0001"), "取消后 tail 无活记录");
        }
    }

    @Test
    @Tag("slow")
    @org.junit.jupiter.api.DisplayName("F3:tail 改期×冷取消并发——锁内重读分支,取消赢则索引干净无 stale")
    void coldTailRescheduleAndCancelSerialize() throws Exception {
        try (CommandStackFx fx = new CommandStackFx(tmp, COLD_FX_OPTS)) {
            String id = "intent_f3race0001";
            Intent planted = fx.plantColdTail(id, Map.of("v", "0"));
            long plantRev = planted.getRevision();
            long cancelledBefore = fx.mc().getIntentsCancelledTotal();
            Instant newer = Instant.now().plus(32, ChronoUnit.DAYS);   // 新 tail 槽(execMs 变化,loc 不同)

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
                            if (fx.svc().updateIntent(id, x -> x.setTags(Map.of("v", "1")), newer).isPresent()) {
                                updateOk.incrementAndGet();
                            }
                        } else if (fx.svc().cancelIntent(id)) {
                            cancelOk.incrementAndGet();
                        }
                    });
                }
                assertTrue(ready.await(10, TimeUnit.SECONDS), "并发线程就绪");
                fire.countDown();
                pool.shutdown();
                assertTrue(pool.awaitTermination(15, TimeUnit.SECONDS));
            }
            // 终态二值一致。F3 修复前:取消者按锁外 stale 快照走 tail 分支,定向移除用旧槽位
            // 变 no-op → 索引残留指向已墓碑化的记录 → 被误判为"改期赢"分支而 tail.readById
            // 为 null → assertNotNull 红。修复后锁内重读,索引/磁盘/内存三态必然一致。
            boolean cancelled = fx.idx().get(id) == null;
            if (cancelled) {
                assertEquals(1, cancelOk.get(), "至多一次取消成功(共享锁串行 + terminal 拒绝)");
                assertNull(fx.tail().readById(id), "已取消则 tail 无活记录(tombstone)");
                assertNull(fx.memStore().findByIdInternal(id), "取消赢后不得有 ghost 热副本");
                assertEquals(1L, fx.mc().getIntentsCancelledTotal() - cancelledBefore);
            } else {
                assertEquals(0, cancelOk.get(), "活态则取消必须全失败");
                Intent live = fx.tail().readById(id);
                assertNotNull(live, "F3:改期赢则索引必指向活 tail 记录(stale 索引会读到 null)");
                assertEquals(IntentStatus.SCHEDULED, live.getStatus());
                assertEquals(newer, live.getExecuteAt());
                assertTrue(live.getRevision() >= plantRev + 1);
                assertTrue(updateOk.get() >= 1);
            }
        }
    }

    @Test
    @org.junit.jupiter.api.DisplayName("冷 fireNow:executeAt=now 落新槽 + revision+1 + 立即热载调度")
    void coldFireNowPersistsAndHotLoads() {
        try (CommandStackFx fx = new CommandStackFx(tmp, COLD_FX_OPTS)) {
            Intent planted = fx.plantColdWheel("intent_coldfire001", Map.of("v", "x"));
            long plantRev = planted.getRevision();
            long before = System.currentTimeMillis();

            assertTrue(fx.svc().fireNow("intent_coldfire001"));

            SlotLocation loc = fx.idx().get("intent_coldfire001");
            assertNotNull(loc);
            assertFalse(loc.inTail());
            Intent onDisk = fx.store().readSlot(loc);
            assertEquals(plantRev + 1, onDisk.getRevision());
            assertEquals(IntentStatus.SCHEDULED, onDisk.getStatus());
            assertTrue(onDisk.getExecuteAt().toEpochMilli() >= before,
                "executeAt 已改写为 now(不早于调用前时刻)");
            Intent hot = fx.memStore().findByIdInternal("intent_coldfire001");
            assertNotNull(hot, "executeAt=now 恒在热窗口:必须立即热载");
            assertEquals(IntentStatus.SCHEDULED, hot.getStatus());
        }
    }

    @Test
    @org.junit.jupiter.api.DisplayName("冷 fireNow:不存在/冷取消后(墓碑当不存在)/索引指向失效槽 → false")
    void coldFireNowFalseCases() {
        try (CommandStackFx fx = new CommandStackFx(tmp, COLD_FX_OPTS)) {
            assertFalse(fx.svc().fireNow("intent_absent000001"));

            fx.plantColdWheel("intent_coldfire002", null);
            assertTrue(fx.svc().cancelIntent("intent_coldfire002"));
            assertFalse(fx.svc().fireNow("intent_coldfire002"), "冷取消后索引已清,fireNow 必须 false");

            // 索引被改指失效槽(读槽返回 null 的防御路径)
            fx.plantColdWheel("intent_coldfire003", null);
            fx.idx().put("intent_coldfire003",
                SlotLocation.tail(System.currentTimeMillis() + 31L * 24 * 60 * 60 * 1000L));
            assertFalse(fx.svc().fireNow("intent_coldfire003"), "槽不可读不得谎报触发");
        }
    }

    @Test
    @org.junit.jupiter.api.DisplayName("冷 fireNow:awaitCommit 提交后失败不传播(C4-3 镜像)——返回 true")
    void coldFireNowSurvivesPostCommitAwaitFailure() {
        try (CommandStackFx fx = new CommandStackFx(tmp, COLD_FX_OPTS)) {
            fx.plantColdWheel("intent_f4fire0001", null);
            PostCommitFailPersistence stub = new PostCommitFailPersistence(
                fx.store(), fx.tail(), fx.barrier(), fx.idx());
            IntentUpdater updater = new IntentUpdater(fx.memStore(), fx.scheduler(), fx.idx(),
                PrecisionTierCatalog.defaultCatalog(), stub, fx.daemon(),
                newReconciler(fx), HOT_BOUNDARY_MS);

            assertTrue(updater.fireNow("intent_f4fire0001"), "提交后失败按成功返回(C4-2/C4-3)");

            SlotLocation loc = fx.idx().get("intent_f4fire0001");
            assertEquals(IntentStatus.SCHEDULED, fx.store().readSlot(loc).getStatus());
        }
    }

    @Test
    @org.junit.jupiter.api.DisplayName("冷 fireNow:路由提交后失败不传播——返回 true")
    void coldFireNowSurvivesPostCommitRoutingFailure() {
        try (CommandStackFx fx = new CommandStackFx(tmp, COLD_FX_OPTS)) {
            fx.plantColdWheel("intent_f4fire002", null);
            RouteFailingUpdater updater = new RouteFailingUpdater(fx.memStore(), fx.scheduler(), fx.idx(),
                PrecisionTierCatalog.defaultCatalog(),
                new WheelPersistence(fx.store(), fx.tail(), fx.barrier(), fx.idx()), fx.daemon(),
                newReconciler(fx), HOT_BOUNDARY_MS);

            assertTrue(updater.fireNow("intent_f4fire002"));
        }
    }

    @Test
    @org.junit.jupiter.api.DisplayName("冷 fireNow:提交前持久化失败返回 false(镜像热 fireNow 布尔契约),磁盘/索引不变")
    void coldFireNowPreCommitFailureReturnsFalse() {
        try (CommandStackFx fx = new CommandStackFx(tmp, COLD_FX_OPTS)) {
            Intent planted = fx.plantColdWheel("intent_f1fire0001", null);
            long plantRev = planted.getRevision();
            SlotLocation locBefore = fx.idx().get("intent_f1fire0001");
            PreCommitFailPersistence stub = new PreCommitFailPersistence(
                fx.store(), fx.tail(), fx.barrier(), fx.idx());
            IntentUpdater updater = new IntentUpdater(fx.memStore(), fx.scheduler(), fx.idx(),
                PrecisionTierCatalog.defaultCatalog(), stub, fx.daemon(),
                newReconciler(fx), HOT_BOUNDARY_MS);

            assertFalse(updater.fireNow("intent_f1fire0001"), "提交前失败镜像热 fireNow:返回 false 不抛");

            assertEquals(locBefore, fx.idx().get("intent_f1fire0001"), "提交前失败索引不得迁移");
            assertEquals(plantRev, fx.store().readSlot(locBefore).getRevision(), "提交前失败磁盘 untouched");
            assertNull(fx.memStore().findByIdInternal("intent_f1fire0001"), "提交前失败不得热载");
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
        AtomicReference<CommandStackFx> fxRef = new AtomicReference<>();
        // 镜像 LoomqEngine promote 回调(:174-189)并四段门控,确定性复现:
        // guard 通过 → update 冷改期(热载新副本+post-check) → promote upsert 旧内容 → 回滚 delete → 内存空
        // guardSettled 串行化 guard 读与 update 提交:guard 读 → update 提交 → upsert → 回滚,
        // 杜绝 CI 饥饿下 guard 见新副本跳过回滚导致伤害态断言误判。
        CommandStackFx fx = new CommandStackFx(tmp,
            new CommandStackFx.Options(System::currentTimeMillis, (hot, loc) -> {
            CommandStackFx f = fxRef.get();
            try {
                boolean first = promotionSeq.incrementAndGet() == 1;
                if (first) {
                    promoteReadSlot.countDown();
                    allowGuard.await();
                } else {
                    allowHeal.await();               // 安全网 promote:等伤害态断言完再自愈
                }
                boolean absent = f.memStore().findByIdInternal(hot.getIntentId()) == null;
                if (first) {
                    guardSettled.countDown();   // guard 已读取(结果=absent);update 须待此门后提交
                }
                if (absent) {
                    if (first) {
                        allowUpsert.await();
                    }
                    f.memStore().upsert(hot);                        // 旧内容覆盖
                    f.scheduler().schedule(hot);
                    if (first) {
                        allowRollback.await();
                    }
                    SlotLocation after = f.idx().get(hot.getIntentId());
                    if (after == null || !after.equals(loc)) {     // latest 已指新槽 → 无条件回滚删除
                        f.scheduler().removeFromSchedule(hot);
                        f.memStore().delete(hot.getIntentId());
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
            }, false, true));
        fxRef.set(fx);
        try (fx) {
            String id = "intent_race0000001";
            Intent planted = fx.plantColdWheel(id, Map.of("v", "old"));
            long plantRev = planted.getRevision();
            // 老 cohort 立即到期(wakeAt=now-1s-60s lead):唯一触发方是后台 loop 线程(register 已
            // unpark 它)。不可在主线程 tickOnce 触发——tickOnce 内联执行本回调,主线程会阻塞在
            // allowGuard.await() 上,而唯一 countDown 它的正是主线程自身 → 死锁。
            fx.daemon().register(id, fx.idx().get(id), System.currentTimeMillis() - 1_000);
            assertTrue(promoteReadSlot.await(5, TimeUnit.SECONDS), "promote 已读旧槽并停门");
            allowGuard.countDown();                                 // 放行 guard 检查(findById==null)
            assertTrue(guardSettled.await(5, TimeUnit.SECONDS),
                "promote guard 检查必须先于 update 提交完成,否则 guard 见新副本跳过回滚,伤害态断言误判");
            Optional<Intent> updated;
            try (ExecutorService ex = Executors.newSingleThreadExecutor()) {
                // close 语义=等待终止:get 抛异常时工作线程不再泄漏(Java 19+ ExecutorService 实现 AutoCloseable)
                updated = ex.submit(() -> fx.svc().updateIntent(id,
                    i -> i.setTags(Map.of("v", "new")), Instant.now().plusSeconds(30))).get(10, TimeUnit.SECONDS);
            }
            assertTrue(updated.isPresent(), "冷改期成功(窗口内 +30s)");
            assertEquals(plantRev + 1, updated.get().getRevision());
            assertNotNull(fx.memStore().findByIdInternal(id), "窗口内已热载新副本");
            allowUpsert.countDown();                                // promote:upsert 旧内容,停 allowRollback
            allowRollback.countDown();                              // promote:复核 latest≠h.loc → delete(id)
            assertTrue(done.await(5, TimeUnit.SECONDS));
            assertNull(fx.memStore().findByIdInternal(id), "回滚删除后内存为空——4.4 伤害场景复现");
            // 安全网:新 cohort wakeAt=+30s-60s<now,旧 cohort 回调返回后 loop 空闲即消费它
            // (停在 allowHeal)→ 此处放行重热载新槽(自愈)。allowHeal 放行先于消费完成,
            // 伤害态断言必先于自愈执行;healDone 返回即证明 loop 已消费该 cohort,无需兜底驱动。
            allowHeal.countDown();
            assertTrue(healDone.await(5, TimeUnit.SECONDS), "cohort 安全网必须重热载");
            Intent healed = fx.memStore().findByIdInternal(id);
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
        try (CommandStackFx fx = new CommandStackFx(tmp, COLD_FX_OPTS)) {
            String id = "intent_tailrace001";
            Intent planted = fx.plantColdTail(id, Map.of("v", "0"));
            long plantRev = planted.getRevision();
            long cancelledBefore = fx.mc().getIntentsCancelledTotal();

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
                            if (fx.svc().updateIntent(id, x -> x.setTags(Map.of("v", "1")), null).isPresent()) {
                                updateOk.incrementAndGet();
                            }
                        } else if (fx.svc().cancelIntent(id)) {
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
            boolean cancelled = fx.idx().get(id) == null;
            if (cancelled) {
                assertEquals(1, cancelOk.get(), "至多一次取消成功(共享锁串行 + terminal 拒绝)");
                assertNull(fx.tail().readById(id), "已取消则 tail 无活记录(tombstone)");
                assertNull(fx.memStore().findByIdInternal(id), "R13a:取消赢后不得有 ghost 热副本(路由重读索引弃用)");
                assertEquals(1L, fx.mc().getIntentsCancelledTotal() - cancelledBefore);
            } else {
                assertEquals(0, cancelOk.get(), "活态则取消必须全失败");
                Intent live = fx.tail().readById(id);
                assertNotNull(live);
                assertEquals(IntentStatus.SCHEDULED, live.getStatus());
                assertTrue(live.getRevision() >= plantRev + 1);
                assertTrue(updateOk.get() >= 1);
            }
        }
    }
}
