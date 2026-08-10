package com.loomq.infrastructure.wheel;

import com.loomq.domain.intent.Intent;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiConsumer;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 冷→热提升 cohort daemon(镜像 CohortManager 的 cohort-wake)。
 * 冷 Intent 注册唤醒 cohort(executeAt - promotionLeadMs);单线程 sleep 到最早 cohort,
 * 唤醒时按 loc 从磁盘读 slot → 解码 → onHotPromotion 载入内存。
 * 运行时 O(提升次数),按时间驱动。tail→day 落盘由调用方在恢复时调 TailIndex.promoteInto。
 */
public final class PromotionDaemon implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(PromotionDaemon.class);

    /**
     * @deprecated 解耦为 {@link WheelConfig#hotBoundaryMs()}(创建/恢复热阈值)与
     *     {@link WheelConfig#promotionLeadMs()}(cohort 唤醒提前量)。保留此常量(= 60min)
     *     仅为旧外部引用兼容;内部代码已迁到 config 字段。
     */
    @Deprecated
    public static final long HOT_WINDOW_MS = 60L * 60_000L; // 60min

    private final WheelStore store;
    private final TailIndex tail;
    private final IntentLocationIndex locationIndex;
    private final LongSupplier clock;
    private final BiConsumer<Intent, SlotLocation> onHotPromotion;
    private final long promotionLeadMs;

    private final ConcurrentSkipListMap<Long, ConcurrentLinkedDeque<ColdHandle>> cohorts = new ConcurrentSkipListMap<>();
    private final ConcurrentHashMap<String, Long> intentIdToCohortKey = new ConcurrentHashMap<>();
    private final Thread thread;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public PromotionDaemon(WheelStore store, TailIndex tail, IntentLocationIndex locationIndex,
                           LongSupplier clock, BiConsumer<Intent, SlotLocation> onHotPromotion, long promotionLeadMs) {
        this.store = store; this.tail = tail; this.locationIndex = locationIndex;
        this.clock = clock; this.onHotPromotion = onHotPromotion;
        this.promotionLeadMs = promotionLeadMs;
        this.thread = Thread.ofPlatform().name("wheel-promotion").daemon(true).unstarted(this::loop);
    }

    public void register(String intentId, SlotLocation loc, long executeAtMs) {
        long wakeAt = executeAtMs - promotionLeadMs;
        ColdHandle handle = new ColdHandle(intentId, loc);
        cohorts.computeIfAbsent(wakeAt, k -> new ConcurrentLinkedDeque<>()).addLast(handle);
        intentIdToCohortKey.put(intentId, wakeAt);
        LockSupport.unpark(thread);
    }

    public boolean remove(String intentId) {
        Long key = intentIdToCohortKey.remove(intentId);
        if (key == null) return false;
        ConcurrentLinkedDeque<ColdHandle> cohort = cohorts.get(key);
        if (cohort != null) cohort.removeIf(h -> intentId.equals(h.intentId()));
        return true;
    }

    public void start() { if (running.compareAndSet(false, true)) { thread.start(); log.info("PromotionDaemon started, promotionLeadMs={}ms", promotionLeadMs); } }

    /** 单步:唤醒所有已到期 cohort(测试用)。 */
    public void tickOnce() {
        long now = clock.getAsLong();
        var due = cohorts.headMap(now, true).entrySet();
        for (var e : due) {
            // Detach the cohort BEFORE iterating: a concurrent register() to a past
            // cohort key would otherwise append a handle into the deque we're draining,
            // silently dropping it. Mirrors CohortManager.wakeLoop (cohorts.remove first).
            ConcurrentLinkedDeque<ColdHandle> cohort = cohorts.remove(e.getKey());
            if (cohort == null || cohort.isEmpty()) continue;
            for (ColdHandle h : cohort) {
                intentIdToCohortKey.remove(h.intentId());
                promote(h);
            }
        }
    }

    private void promote(ColdHandle h) {
        try {
            // Re-verify against the latest index state — cancel/reschedule may have moved or
            // removed this intent between cohort registration and now. Without this check, a
            // cold cancel that appends a CANCELED slot at a NEW loc (append-only WheelStore)
            // and removes the index entry would still be resurrected: promote reads the OLD
            // SCHEDULED slot at the handle's loc and upserts it into memory + scheduler.
            SlotLocation latest = locationIndex.get(h.intentId());
            if (latest == null) return;                       // cancelled/removed — do not resurrect
            if (!latest.equals(h.loc())) return;              // rescheduled to a new loc — its own cohort handles it
            Intent intent;
            if (h.loc().inTail()) {
                // tail:按 executeAtMs 查 tail entry
                var it = tail.scanFrom(h.loc().bucketKey());
                intent = null;
                while (it.hasNext()) {
                    var te = it.next();
                    Intent decoded = SlotCodec.decode(te.encodedSlot());
                    if (h.intentId().equals(decoded.getIntentId())) { intent = decoded; break; }
                }
            } else {
                intent = store.readSlot(h.loc());
            }
            if (intent == null || intent.getStatus().isTerminal()) return;
            // 把读槽用的 loc 传给回调,供其做提升后复核(P1-2:与 cancelCold 的 TOCTOU 收口)。
            onHotPromotion.accept(intent, h.loc());
        } catch (Exception ex) {
            log.error("promote failed for {}", h.intentId(), ex);
        }
    }

    private void loop() {
        while (running.get()) {
            try {
                var first = cohorts.firstEntry();
                if (first == null) { LockSupport.park(); continue; }
                long now = clock.getAsLong();
                if (first.getKey() > now) {
                    LockSupport.parkNanos(java.time.Duration.ofMillis(first.getKey() - now).toNanos());
                    continue;
                }
                tickOnce();
            } catch (Exception e) { log.error("promotion loop error", e); LockSupport.parkNanos(100_000_000L); }
        }
        log.info("PromotionDaemon stopped");
    }

    @Override public void close() {
        running.set(false); thread.interrupt();
        try { thread.join(2000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
}
