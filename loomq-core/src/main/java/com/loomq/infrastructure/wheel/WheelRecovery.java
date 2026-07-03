package com.loomq.infrastructure.wheel;

import com.loomq.application.scheduler.PrecisionScheduler;
import com.loomq.domain.intent.Intent;
import com.loomq.store.IntentStore;
import java.time.Instant;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 恢复:扫所有槽 → 重建索引 + 热载入内存 + 冷注册 promotion cohort。
 * 替代 RecoveryPipeline。无 WAL 回放——槽即当前态。一次性 O(N) 扫描(类比 WAL replay)。
 *
 * <p><b>跨视界去重</b>:WheelStore 是 append-only(不清理槽位),把 within-horizon 槽
 * reschedule 到 beyond-horizon 后,旧 day-wheel 槽(低 revision)与新 tail 条目(高 revision)
 * 共存。recover 按 intentId 在 day-wheel + tail 全局取最大 revision 的条目,仅处理胜者,
 * 避免重启时旧槽引发 ghost 投递/提升。</p>
 */
public final class WheelRecovery {
    private static final Logger log = LoggerFactory.getLogger(WheelRecovery.class);

    private final WheelStore store;
    private final TailIndex tail;
    private final long hotBoundaryMs;

    public WheelRecovery(WheelStore store, TailIndex tail, long hotBoundaryMs) {
        this.store = store; this.tail = tail; this.hotBoundaryMs = hotBoundaryMs;
    }

    public WheelRecoveryReport recover(IntentStore memStore, PrecisionScheduler scheduler,
                                       IntentLocationIndex idx, PromotionDaemon daemon) {
        long nowMs = System.currentTimeMillis();
        long hotBoundary = nowMs + hotBoundaryMs;

        // 1. tail → day (cold→cold disk reorg, once)
        tail.promoteInto(store);

        // 2. Build global latest map: intentId → SlotEntry, max revision across day-wheel
        //    slots AND tail entries. WheelStore is append-only (no slot clearing), so a
        //    reschedule across the horizon leaves a stale lower-revision day-wheel slot
        //    alongside the new higher-revision tail entry. Dedup-ing by max revision across
        //    BOTH sources ensures only the winner is processed — preventing ghost
        //    promotion/delivery of the stale slot on restart.
        Map<String, SlotEntry> latest = new HashMap<>();
        Iterator<SlotEntry> it = store.scanSlotsFrom(Instant.ofEpochMilli(0));
        while (it.hasNext()) {
            SlotEntry e = it.next();
            SlotEntry prev = latest.get(e.intent().getIntentId());
            if (prev == null || e.intent().getRevision() > prev.intent().getRevision()) {
                latest.put(e.intent().getIntentId(), e);
            }
        }
        var tailIt = tail.scanFrom(0);
        while (tailIt.hasNext()) {
            TailEntry te = tailIt.next();
            Intent intent = SlotCodec.decode(te.encodedSlot());
            SlotEntry tailEntry = new SlotEntry(SlotLocation.tail(te.executeAtMs()), intent);
            SlotEntry prev = latest.get(intent.getIntentId());
            if (prev == null || intent.getRevision() > prev.intent().getRevision()) {
                latest.put(intent.getIntentId(), tailEntry);
            }
        }

        // 3. Process only the max-revision winner per intentId
        int hot = 0, cold = 0;
        for (SlotEntry e : latest.values()) {
            Intent intent = e.intent();
            idx.put(intent.getIntentId(), e.loc());          // rebuild index with the winning loc
            if (intent.getStatus().isTerminal()) continue;
            long execMs = intent.getExecuteAt().toEpochMilli();
            if (execMs < nowMs) continue;                       // expired, skip
            if (execMs <= hotBoundary) {
                memStore.upsert(intent);
                scheduler.restore(intent);
                hot++;
            } else {
                daemon.register(intent.getIntentId(), e.loc(), execMs);
                cold++;
            }
        }

        log.info("WheelRecovery: hotRestored={}, coldRegistered={}", hot, cold);
        return new WheelRecoveryReport(hot, cold);
    }
}
