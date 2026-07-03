package com.loomq.infrastructure.wheel;

import static org.junit.jupiter.api.Assertions.*;

import com.loomq.application.scheduler.PrecisionScheduler;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.spi.DeliveryHandler;
import com.loomq.store.ConcurrentIntentStore;
import com.loomq.store.IntentStore;
import java.nio.file.Path;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Task 8: WheelRecovery 扫描恢复测试。
 *
 * <p>载入热 Intent(≤ now+HOT_WINDOW_MS)到内存 + 调度器,注册冷 Intent(> HOT_WINDOW_MS)
 * 到 PromotionDaemon,重建 IntentLocationIndex。模拟重启:新实例从磁盘恢复。</p>
 *
 * <p><b>全局去重修正</b>:WheelStore 是 append-only(不清理槽位)。把 within-horizon 槽
 * reschedule 到 beyond-horizon 后,旧 day-wheel 槽(低 revision)与新 tail 条目(高 revision)
 * 共存。recover 必须按 intentId 在 day-wheel + tail 全局取最大 revision 的条目,仅处理胜者,
 * 否则旧槽会在重启时引发 ghost 投递/提升。以下测试覆盖:terminal 跳过、过期跳过、
 * 同源 day-wheel 去重、以及跨视界 reschedule(旧 day-wheel 槽 + 新 tail 条目)。</p>
 *
 * <p><b>与 brief 的两处必要偏差</b>(均限于本测试,production code 严格遵循 brief):
 * <ol>
 *   <li>测试时钟使用 {@code System.currentTimeMillis()} 而非固定午夜时刻 —— brief 的
 *       {@code WheelRecovery.recover} 用 {@code System.currentTimeMillis()} 判定热/冷
 *       与过期,固定午夜时钟会使 +5s 热 Intent 在一日内任意非午夜时刻被判定为过期而跳过。
 *       用系统时钟使 store 时钟与 recover 的 "now" 一致,契合测试所述"+5s 热 / +90min 冷"语义。</li>
 *   <li>{@link PrecisionScheduler} 构造对 {@code deliveryHandler} 做
 *       {@code Objects.requireNonNull},故传 no-op lambda(本测试仅调 {@code restore},
 *       不触发投递,no-op 安全)。</li>
 * </ol></p>
 */
class WheelRecoveryTest {
    @TempDir Path tmp;

    /** 重启恢复后的可观察产物(供各测试在 verifier 回调内断言)。 */
    private record Recovered(WheelRecoveryReport report, IntentStore mem,
                             IntentLocationIndex idx, PromotionDaemon daemon) {}

    /**
     * 模拟重启:新开 WheelStore/TailIndex 从磁盘加载 + recover,在 verifier 内断言。
     * setup 阶段先关闭 store1/tail1(落盘),再由此方法新开 store2/tail2 读取,契合"重启"语义。
     */
    private void reopenAndRecover(WheelConfig cfg, LongSupplier clock, Consumer<Recovered> verifier) {
        try (WheelStore store2 = new WheelStore(cfg, clock);
             TailIndex tail2 = new TailIndex(tmp, clock);
             ConcurrentIntentStore mem = new ConcurrentIntentStore();
             IntentLocationIndex idx = new IntentLocationIndex();
             PromotionDaemon daemon = new PromotionDaemon(store2, tail2, idx, clock, i -> {})) {
            // PrecisionScheduler 非 AutoCloseable,且未 start(),无需 close/stop。
            // 构造对 deliveryHandler 做 requireNonNull,故传 no-op(本测试仅 restore,不投递)。
            PrecisionScheduler scheduler = new PrecisionScheduler(
                mem, i -> CompletableFuture.completedFuture(DeliveryHandler.DeliveryResult.SUCCESS), null);
            WheelRecovery rec = new WheelRecovery(store2, tail2);
            WheelRecoveryReport rpt = rec.recover(mem, scheduler, idx, daemon);
            verifier.accept(new Recovered(rpt, mem, idx, daemon));
        }
    }

    @Test
    void shouldLoadHotAndRegisterColdForPromotion() {
        AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        WheelConfig cfg = new WheelConfig(tmp.toString(), "t", 30, 16, 1, 10_000L, null);
        try (WheelStore store = new WheelStore(cfg, clock::get);
             TailIndex tail = new TailIndex(tmp, clock::get)) {
            // hot: +5s(在 60min 热窗口内)
            Intent hot = new Intent("intent_hot0000000001");
            hot.setExecuteAt(Instant.ofEpochMilli(clock.get() + 5_000));
            hot.transitionTo(IntentStatus.SCHEDULED);
            store.put(hot);
            // cold: +90min(超出热窗口)
            Intent cold = new Intent("intent_cold0000000001");
            cold.setExecuteAt(Instant.ofEpochMilli(clock.get() + 90 * 60_000L));
            cold.transitionTo(IntentStatus.SCHEDULED);
            store.put(cold);
        }

        reopenAndRecover(cfg, clock::get, r -> {
            assertEquals(1, r.report().hotRestored());
            assertEquals(1, r.report().coldRegistered());
            assertNotNull(r.mem().findById("intent_hot0000000001"), "热 Intent 应载入内存");
            assertNull(r.mem().findById("intent_cold0000000001"), "冷 Intent 不应载入内存");
            assertNotNull(r.idx().get("intent_cold0000000001"), "冷 Intent 应在索引中");
            assertTrue(r.daemon().remove("intent_cold0000000001"), "冷 Intent 应已注册 promotion cohort");
        });
    }

    @Test
    void shouldSkipTerminalIntentButStillIndexIt() {
        AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        WheelConfig cfg = new WheelConfig(tmp.toString(), "t", 30, 16, 1, 10_000L, null);
        try (WheelStore store = new WheelStore(cfg, clock::get);
             TailIndex tail = new TailIndex(tmp, clock::get)) {
            // CANCELED (terminal) intent sitting in the day wheel
            Intent canceled = new Intent("intent_cancel00001");
            canceled.setExecuteAt(Instant.ofEpochMilli(clock.get() + 5_000));
            canceled.transitionTo(IntentStatus.SCHEDULED);
            canceled.transitionTo(IntentStatus.CANCELED);
            store.put(canceled);
        }

        reopenAndRecover(cfg, clock::get, r -> {
            assertEquals(0, r.report().hotRestored(), "terminal intent must not be hot-loaded");
            assertEquals(0, r.report().coldRegistered(), "terminal intent must not be registered for promotion");
            assertNull(r.mem().findById("intent_cancel00001"), "terminal intent must not be in memStore");
            assertNotNull(r.idx().get("intent_cancel00001"), "terminal intent should still be indexed for cancel locate");
        });
    }

    @Test
    void shouldSkipExpiredIntentButStillIndexIt() {
        AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        WheelConfig cfg = new WheelConfig(tmp.toString(), "t", 30, 16, 1, 10_000L, null);
        try (WheelStore store = new WheelStore(cfg, clock::get);
             TailIndex tail = new TailIndex(tmp, clock::get)) {
            // executeAt in the past (< now) → expired at recover time
            Intent expired = new Intent("intent_expired00001");
            expired.setExecuteAt(Instant.ofEpochMilli(clock.get() - 5_000));
            expired.transitionTo(IntentStatus.SCHEDULED);
            store.put(expired);
        }

        reopenAndRecover(cfg, clock::get, r -> {
            assertEquals(0, r.report().hotRestored(), "expired intent must not be hot-loaded");
            assertEquals(0, r.report().coldRegistered(), "expired intent must not be registered for promotion");
            assertNull(r.mem().findById("intent_expired00001"), "expired intent must not be in memStore");
            assertNotNull(r.idx().get("intent_expired00001"), "expired intent should still be indexed");
            assertFalse(r.daemon().remove("intent_expired00001"), "expired intent must not be registered in daemon");
        });
    }

    @Test
    void shouldDedupByMaxRevisionWithinDayWheel() {
        AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        WheelConfig cfg = new WheelConfig(tmp.toString(), "t", 30, 16, 1, 10_000L, null);
        try (WheelStore store = new WheelStore(cfg, clock::get);
             TailIndex tail = new TailIndex(tmp, clock::get)) {
            // rev 1: SCHEDULED at +5min (within hot window) → day-wheel slot
            Intent v1 = new Intent("intent_dedup000001");
            v1.setExecuteAt(Instant.ofEpochMilli(clock.get() + 5 * 60_000L));
            v1.transitionTo(IntentStatus.SCHEDULED);
            v1.incrementRevision();
            store.put(v1);
            // rev 2: CANCELED, same intentId + executeAt (→ same bucket, new slot; append-only)
            Intent v2 = v1.copy();
            v2.transitionTo(IntentStatus.CANCELED);
            v2.incrementRevision();
            store.put(v2);
        }

        reopenAndRecover(cfg, clock::get, r -> {
            // only rev 2 (CANCELED, terminal) wins → not loaded, not registered, but indexed
            assertEquals(0, r.report().hotRestored(), "stale rev-1 slot must not ghost-load to memory");
            assertEquals(0, r.report().coldRegistered(), "terminal winner must not be registered for promotion");
            assertNull(r.mem().findById("intent_dedup000001"), "terminal winner must not be in memStore");
            SlotLocation loc = r.idx().get("intent_dedup000001");
            assertNotNull(loc, "winner should be indexed");
            assertFalse(loc.inTail(), "winner loc should be a day-wheel slot, not tail");
        });
    }

    @Test
    void shouldNotGhostLoadWhenRescheduledAcrossHorizon() {
        AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        WheelConfig cfg = new WheelConfig(tmp.toString(), "t", 30, 16, 1, 10_000L, null);
        try (WheelStore store = new WheelStore(cfg, clock::get);
             TailIndex tail = new TailIndex(tmp, clock::get)) {
            // rev 1: within horizon (+2min, hot) → day-wheel slot (stale after reschedule)
            Intent v1 = new Intent("intent_resched0001");
            v1.setExecuteAt(Instant.ofEpochMilli(clock.get() + 2 * 60_000L));
            v1.transitionTo(IntentStatus.SCHEDULED);
            v1.incrementRevision();
            store.put(v1);
            // rev 2: rescheduled beyond horizon (+40d) → tail entry (higher revision, same intentId)
            Intent v2 = v1.copy();
            v2.setExecuteAt(Instant.ofEpochMilli(clock.get() + 40L * 24 * 60 * 60_000L));
            v2.incrementRevision();
            tail.put(v2);
        }

        reopenAndRecover(cfg, clock::get, r -> {
            // The stale +2min day-wheel slot (rev 1) must NOT ghost-load to memory; only the
            // +40d tail entry (rev 2) wins by max revision → cold registration only.
            assertEquals(0, r.report().hotRestored(),
                "stale +2min day-wheel slot must not ghost-load to memory");
            assertEquals(1, r.report().coldRegistered(),
                "+40d tail entry should be registered for promotion");
            assertNull(r.mem().findById("intent_resched0001"),
                "cold +40d intent must not be in memStore");
            SlotLocation loc = r.idx().get("intent_resched0001");
            assertNotNull(loc, "intent should be indexed");
            assertTrue(loc.inTail(), "winning loc should be the tail entry (rev 2), not the stale day-wheel slot");
            assertTrue(r.daemon().remove("intent_resched0001"),
                "tail entry should be registered in promotion daemon");
        });
    }
}
