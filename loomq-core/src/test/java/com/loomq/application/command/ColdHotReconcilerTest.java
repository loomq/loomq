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
 * ColdHotReconciler 谓词矩阵(round 14):P1-2 协议单一权威实现的三触发面行为锁。
 * 行为与原三处内联逐一保持——本测试即"结构统一不改语义"的验收面。
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
    @org.junit.jupiter.api.DisplayName("promote 侧:索引匹配 → 不回滚;失配/已清 → 回滚热载")
    void rollbackPromoteHotLoadIndexDiscrimination() {
        Intent hot = newCopy("intent_rcprom0001", 1);
        store.upsert(hot);
        SlotLocation loc = SlotLocation.tail(Instant.now().plus(2, ChronoUnit.DAYS).toEpochMilli());
        idx.put(hot.getIntentId(), loc);

        reconciler.rollbackPromoteHotLoad(hot, loc);
        assertNotNull(store.findByIdInternal("intent_rcprom0001"), "索引匹配:热副本保持");

        idx.remove(hot.getIntentId(), loc);
        reconciler.rollbackPromoteHotLoad(hot, loc);
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
}
