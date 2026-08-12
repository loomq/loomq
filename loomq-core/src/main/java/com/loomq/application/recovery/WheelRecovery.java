package com.loomq.application.recovery;

import com.loomq.application.scheduler.PrecisionScheduler;
import com.loomq.common.MetricsCollector;
import com.loomq.domain.intent.ExpiredAction;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.infrastructure.wheel.IntentLocationIndex;
import com.loomq.infrastructure.wheel.PromotionDaemon;
import com.loomq.infrastructure.wheel.SlotCodec;
import com.loomq.infrastructure.wheel.SlotEntry;
import com.loomq.infrastructure.wheel.SlotLocation;
import com.loomq.infrastructure.wheel.TailEntry;
import com.loomq.infrastructure.wheel.TailIndex;
import com.loomq.infrastructure.wheel.WheelStore;
import com.loomq.store.IntentStore;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 恢复:扫所有槽 → 重建索引 + 热载入内存 + 冷注册 promotion cohort。
 * 替代 RecoveryPipeline。无 WAL 回放——槽即当前态。一次性 O(N) 扫描(类比 WAL replay)。
 *
 * <p><b>跨视界去重</b>:WheelStore 非终态 append、终态单槽回收(陈旧兄弟槽仍残留),把 within-horizon 槽
 * reschedule 到 beyond-horizon 后,旧 day-wheel 槽(低 revision)与新 tail 条目(高 revision)
 * 共存。recover 按 intentId 在 day-wheel + tail 全局取最大 revision 的条目,仅处理胜者,
 * 避免重启时旧槽引发 ghost 投递/提升。</p>
 */
public final class WheelRecovery {
    private static final Logger log = LoggerFactory.getLogger(WheelRecovery.class);

    private final WheelStore store;
    private final TailIndex tail;
    private final long hotBoundaryMs;
    private final MetricsCollector metricsCollector;

    public WheelRecovery(WheelStore store, TailIndex tail, long hotBoundaryMs, MetricsCollector metricsCollector) {
        this.store = store; this.tail = tail; this.hotBoundaryMs = hotBoundaryMs;
        this.metricsCollector = metricsCollector;
    }

    public WheelRecoveryReport recover(IntentStore memStore, PrecisionScheduler scheduler,
                                       IntentLocationIndex idx, PromotionDaemon daemon) {
        long nowMs = System.currentTimeMillis();
        long hotBoundary = nowMs + hotBoundaryMs;

        // 1. tail → day (cold→cold disk reorg, once)
        tail.promoteInto(store);

        // 2. Build global latest map: intentId → SlotEntry, max revision across day-wheel
        //    slots AND tail entries. Non-terminal writes append; terminal slots are reclaimed
        //    in place (stale siblings remain), so a
        //    reschedule across the horizon leaves a stale lower-revision day-wheel slot
        //    alongside the new higher-revision tail entry. Dedup-ing by max revision across
        //    BOTH sources ensures only the winner is processed — preventing ghost
        //    promotion/delivery of the stale slot on restart.
        Map<String, SlotEntry> latest = new HashMap<>();
        Map<String, Integer> slotCounts = new HashMap<>();
        Iterator<SlotEntry> it = store.scanSlotsFrom(Instant.ofEpochMilli(0));
        while (it.hasNext()) {
            SlotEntry e = it.next();
            slotCounts.merge(e.intent().getIntentId(), 1, Integer::sum);
            SlotEntry prev = latest.get(e.intent().getIntentId());
            if (prev == null || e.intent().getRevision() > prev.intent().getRevision()) {
                latest.put(e.intent().getIntentId(), e);
            }
        }
        var tailIt = tail.scanFrom(0);
        while (tailIt.hasNext()) {
            TailEntry te = tailIt.next();
            Intent intent;
            try {
                // P1-6: tail 条目无 isTorn 预检(wheel 槽有)。一条 CRC 损坏即让 decode 抛异常
                // 中断 recover、引擎起不来。防御性解码,损坏条目跳过并告警。
                intent = SlotCodec.decode(te.encodedSlot());
            } catch (RuntimeException dex) {
                log.warn("Recovery: skipping corrupt tail entry (decode failed)", dex);
                continue;
            }
            slotCounts.merge(intent.getIntentId(), 1, Integer::sum);
            SlotEntry tailEntry = new SlotEntry(SlotLocation.tail(te.executeAtMs()), intent);
            SlotEntry prev = latest.get(intent.getIntentId());
            if (prev == null || intent.getRevision() > prev.intent().getRevision()) {
                latest.put(intent.getIntentId(), tailEntry);
            }
        }

        // 3. Process only the max-revision winner per intentId
        int hot = 0, cold = 0;
        Set<String> multiSlot = new HashSet<>();
        for (SlotEntry e : latest.values()) {
            Intent intent = e.intent();
            String id = intent.getIntentId();
            int count = slotCounts.getOrDefault(id, 0);
            if (intent.getStatus().isTerminal()) {
                // F3:唯一幸存且为终态 → 崩溃窗口泄漏的单槽终态墓碑(overwrite 与 reclaim 之间崩溃),
                // 无兄弟槽可复活,恢复期回收清空,桶容量自愈(wheel 槽;tail 内终态不在此回收)。count>1 的合法多槽墓碑保留(参与去重)。
                if (count == 1) {
                    store.freeSlot(e.loc());
                    log.info("Recovery: reclaimed leaked terminal slot {} for intent {}", e.loc(), id);
                }
                continue;
            }
            long execMs = intent.getExecuteAt().toEpochMilli();
            if (execMs < nowMs) {
                // P0-2: 停机窗口到期的 Intent 不再静默丢弃。按 ExpiredAction 补终态
                // 并持久化终态 revision,使下次重启恢复时被 terminal 跳过(避免每次重启重复处理)。
                // 不补投——投递窗口已过,补投对下游往往是过时事件;留终态痕迹与告警即可。
                IntentStatus terminal = intent.getExpiredAction() == ExpiredAction.DEAD_LETTER
                    ? IntentStatus.DEAD_LETTERED : IntentStatus.EXPIRED;
                try {
                    intent.transitionTo(terminal);
                    intent.incrementRevision();
                    SlotLocation newLoc = store.put(intent);   // 写终态 revision
                    metricsCollector.incrementRecoveryOverdue();
                    log.warn("Recovery: intent {} overdue (execMs={} < now={}); marked {}",
                        id, execMs, nowMs, terminal);
                } catch (Exception ex) {
                    log.error("Recovery: failed to mark overdue intent {}", id, ex);
                }
                continue;
            }
            if (count > 1) multiSlot.add(id);    // F1: 磁盘上幸存兄弟 >1 → 终态须保留墓碑不回收
            idx.put(id, e.loc());                // rebuild index: only non-terminal, non-overdue
            if (execMs <= hotBoundary) {
                memStore.upsert(intent);
                scheduler.restore(intent);
                hot++;
            } else {
                daemon.register(id, e.loc(), execMs);
                cold++;
            }
        }

        log.info("WheelRecovery: hotRestored={}, coldRegistered={}, multiSlot={}", hot, cold, multiSlot.size());
        return new WheelRecoveryReport(hot, cold, multiSlot);
    }
}
