package com.loomq.application.command;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.loomq.application.scheduler.PrecisionScheduler;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.infrastructure.wheel.IntentLocationIndex;
import com.loomq.infrastructure.wheel.SlotLocation;
import com.loomq.spi.DeliveryHandler;
import com.loomq.store.ConcurrentIntentStore;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * ColdHotReconciler 谓词矩阵:P1-2 协议(round 14 收口至 ColdHotReconciler)单一权威实现
 * 的三触发面行为锁。行为与原三处内联逐一保持——本测试即"结构统一不改语义"的验收面。
 */
class ColdHotReconcilerTest {

    private final ConcurrentIntentStore store = new ConcurrentIntentStore();
    private final PrecisionScheduler scheduler = new PrecisionScheduler(store,
        intent -> CompletableFuture.completedFuture(DeliveryHandler.DeliveryResult.DEAD_LETTER), null);
    private final IntentLocationIndex idx = new IntentLocationIndex();
    private final ColdHotReconciler reconciler = new ColdHotReconciler(store, scheduler, idx);

    @BeforeEach
    void startScheduler() {
        scheduler.start();
    }

    @AfterEach
    void stopScheduler() {
        scheduler.stop();
    }

    /** 构造 SCHEDULED 活副本(不落 store)。 */
    private Intent newCopy(String id, long revision) {
        Intent hot = new Intent(id);
        hot.setExecuteAt(Instant.now().plus(1, ChronoUnit.HOURS));
        hot.setPrecisionTier(PrecisionTier.STANDARD);
        hot.transitionTo(IntentStatus.SCHEDULED);
        for (long i = 0; i < revision; i++) {
            hot.incrementRevision();
        }
        return hot;
    }

    @Test
    @org.junit.jupiter.api.DisplayName("promote 侧:索引匹配 → 不回滚;失配/已清且热副本 revision 不高于载入 revision → 回滚热载")
    void rollbackPromoteHotLoadIndexDiscrimination() {
        Intent hot = newCopy("intent_rcprom0001", 1);
        store.upsert(hot);
        SlotLocation loc = SlotLocation.tail(Instant.now().plus(2, ChronoUnit.DAYS).toEpochMilli());
        idx.put(hot.getIntentId(), loc);

        reconciler.rollbackPromoteHotLoad(hot, loc, hot.getRevision());
        assertNotNull(store.findByIdInternal("intent_rcprom0001"), "索引匹配:热副本保持");

        idx.remove(hot.getIntentId(), loc);
        reconciler.rollbackPromoteHotLoad(hot, loc, hot.getRevision());
        assertNull(store.findByIdInternal("intent_rcprom0001"), "索引已清/失配:回滚热载");
    }

    @Test
    @org.junit.jupiter.api.DisplayName("冷取消侧:热副本存在即降级;不存在 no-op")
    void removeHotCopyIfPresentSemantics() {
        assertNull(store.findByIdInternal("intent_rcabsent01"));
        reconciler.removeHotCopyIfPresent("intent_rcabsent01");   // no-op 不抛

        store.upsert(newCopy("intent_rcpres0001", 1));
        reconciler.removeHotCopyIfPresent("intent_rcpres0001");
        assertNull(store.findByIdInternal("intent_rcpres0001"), "存在热副本必须降级");
    }

    @Test
    @org.junit.jupiter.api.DisplayName("冷改期侧:窗口内 revision 旧 → 降级+重热载;revision 新 → 不动;窗口外 → 降级不重热载")
    void removeHotCopyIfStaleSemantics() {
        // 窗口内,热副本 revision 旧 → 降级 + 重热载 authoritative
        store.upsert(newCopy("intent_rcstale001", 1));
        Intent authoritative = newCopy("intent_rcstale001", 2);
        reconciler.removeHotCopyIfStale("intent_rcstale001", authoritative, true);
        assertEquals(2, store.findByIdInternal("intent_rcstale001").getRevision(), "stale 降级后重热载新副本");

        // 窗口内,热副本 revision 不旧 → 不动
        store.upsert(newCopy("intent_rcfresh001", 2));
        reconciler.removeHotCopyIfStale("intent_rcfresh001", newCopy("intent_rcfresh001", 1), true);
        assertEquals(2, store.findByIdInternal("intent_rcfresh001").getRevision(), "新副本不得被旧权威降级");

        // 窗口外 → 一律降级,且不重热载
        store.upsert(newCopy("intent_rcoutwd001", 2));
        reconciler.removeHotCopyIfStale("intent_rcoutwd001", newCopy("intent_rcoutwd001", 2), false);
        assertNull(store.findByIdInternal("intent_rcoutwd001"), "窗口外热副本皆 stale,降级后不重热载");
    }

    @Test
    @org.junit.jupiter.api.DisplayName("promote 侧(round 15 F2):热副本 revision 已推进 → 不回滚;不高于载入副本 → 回滚(既有行为)")
    void rollbackPromoteHotLoadRevisionGuard() {
        // 热写者已推进(store 副本 R2 > promote 载入的 R1)且索引已失配 → 不回滚
        store.upsert(newCopy("intent_rcadv0001", 2));
        SlotLocation promotedLoc = SlotLocation.tail(Instant.now().plus(2, ChronoUnit.DAYS).toEpochMilli());
        idx.put("intent_rcadv0001",
            SlotLocation.tail(Instant.now().plus(3, ChronoUnit.DAYS).toEpochMilli()));
        reconciler.rollbackPromoteHotLoad(newCopy("intent_rcadv0001", 1), promotedLoc, 1);
        assertEquals(2, store.findByIdInternal("intent_rcadv0001").getRevision(),
            "热写者推进后的新副本不得被 promote 复核误删");

        // 热副本与载入副本同 revision(索引被冷命令迁移/清除)→ 回滚(round 14 既有行为保持)
        store.upsert(newCopy("intent_rceq00001", 1));
        idx.put("intent_rceq00001",
            SlotLocation.tail(Instant.now().plus(4, ChronoUnit.DAYS).toEpochMilli()));
        reconciler.rollbackPromoteHotLoad(newCopy("intent_rceq00001", 1),
            SlotLocation.tail(Instant.now().plus(2, ChronoUnit.DAYS).toEpochMilli()), 1);
        assertNull(store.findByIdInternal("intent_rceq00001"), "同 revision 失配仍回滚(冷命令迁移场景)");

        // 热副本不存在(已被取消侧删除)→ 走 demote(promoted) 幂等路径,不抛
        reconciler.rollbackPromoteHotLoad(newCopy("intent_rcgone0001", 1),
            SlotLocation.tail(Instant.now().plus(2, ChronoUnit.DAYS).toEpochMilli()), 1);
        assertNull(store.findByIdInternal("intent_rcgone0001"));
    }

    @Test
    @org.junit.jupiter.api.DisplayName("promote 侧(round 15 F2 终审):同对象盲区——热写者推进 upsert 的同一活对象后,以载入 revision 为判据不误删")
    void rollbackPromoteHotLoadSameObjectBlindSpot() {
        // 同对象场景:promote 回调 upsert P(载入 revision=1)后,热写者 updateIntent 守卫
        // 直写并推进同一活对象(P.incrementRevision → R2)。reconcile 时 findByIdInternal
        // 返回的仍是 P(hot == promoted 同引用)——旧判据 hot.getRevision() > promoted.getRevision()
        // 对同引用恒 false → 推进后的副本被误 demote(内存丢失)。以 upsert 前捕获的
        // promotedRevision=1 为判据:hot.revision(2) > 1 → 豁免。
        Intent sameObj = newCopy("intent_rcsame0001", 1);
        store.upsert(sameObj);
        sameObj.incrementRevision();   // 模拟热写者推进同一活对象
        SlotLocation promotedLoc = SlotLocation.tail(Instant.now().plus(2, ChronoUnit.DAYS).toEpochMilli());
        idx.put("intent_rcsame0001",
            SlotLocation.tail(Instant.now().plus(3, ChronoUnit.DAYS).toEpochMilli()));

        reconciler.rollbackPromoteHotLoad(sameObj, promotedLoc, 1);

        assertEquals(2, store.findByIdInternal("intent_rcsame0001").getRevision(),
            "热写者推进后的同引用副本不得被 promote 复核误删");
    }
}
