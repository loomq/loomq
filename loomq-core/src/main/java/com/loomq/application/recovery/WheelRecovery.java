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

    /**
     * 时钟回拨守卫裕量:重启时钟比"最新桶文件 mtime"还早超过该值,判定停机期间时钟回拨。
     * 吸收 mtime 粒度(部分文件系统为秒级)与时钟抖动。
     */
    private static final long CLOCK_ROLLBACK_MARGIN_MS = 5 * 60_000L;

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

        // C3-2: 时钟回拨守卫。桶文件 mtime 是"引擎最后写入时刻"的磁盘证据(写入/force 推进
        // mtime);若重启时钟早于该证据(停机期间 NTP 校时回拨/运维改时钟),overdue 判定
        // (execMs < nowMs)会把回拨窗口内实际未到期的 Intent 不可逆终态化——与运行时
        // "回拨仅延迟不杀"(ClockRollbackTest)的立场不一致。检测到回拨时跳过 overdue
        // 终态化,按正常路径恢复(延迟投递而非杀)。无桶(无证据)时守卫不生效。
        boolean clockRolledBack = false;
        long newestWriteMs = store.newestBucketWriteTimeMs();
        if (newestWriteMs > 0 && nowMs + CLOCK_ROLLBACK_MARGIN_MS < newestWriteMs) {
            clockRolledBack = true;
            log.error("System clock appears rolled back: now={} < last bucket write={} (margin {}ms); "
                    + "skipping overdue terminalization — intents due during downtime will be restored "
                    + "and delivered late instead of killed",
                nowMs, newestWriteMs, CLOCK_ROLLBACK_MARGIN_MS);
        }

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
            // P1-6: tail 条目无 isTorn 预检(wheel 槽有)。一条 CRC 损坏即让 decode 抛异常
            // 中断 recover、引擎起不来。防御性解码,损坏条目跳过并告警。
            Intent intent = SlotCodec.decodeSafe(te.encodedSlot());
            if (intent == null) {
                log.warn("Recovery: skipping corrupt tail entry (decode failed)");
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
        // C18-1(r18): 磁盘上存在终态墓碑(多槽墓碑/tail 终态,不回收)的 intentId,
        // 经 markTombstones 注入命令服务 tombstoneIds,保护种子映射(C4-1)。
        Set<String> tombstonedIds = new HashSet<>();
        // 每个 intentId 的磁盘历史最高 revision(供 createIntent 重建路径种子 revision,
        // 避免重建的新 Intent 从 0 起步被旧终态墓碑遮蔽)。含终态/非终态/跨视界 tail。
        Map<String, Long> maxRevisions = new HashMap<>();
        for (SlotEntry e : latest.values()) {
            Intent intent = e.intent();
            String id = intent.getIntentId();
            int count = slotCounts.getOrDefault(id, 0);
            if (intent.getStatus().isTerminal()) {
                // F3:唯一幸存且为终态 → 崩溃窗口泄漏的单槽终态墓碑(overwrite 与 reclaim 之间崩溃),
                // 无兄弟槽可复活,恢复期回收清空,桶容量自愈(wheel 槽;tail 内终态不在此回收)。count>1 的合法多槽墓碑保留(参与去重)。
                if (count == 1 && !e.loc().inTail()) {
                    store.freeSlot(e.loc());
                    log.info("Recovery: reclaimed leaked terminal slot {} for intent {}", e.loc(), id);
                    // C3-4: 单槽 wheel 终态已回收 → 磁盘不再有该 id 的任何记录 → 无需注入
                    // maxRevisions(注入即无清理路径的内存泄漏)。tail 终态与多槽墓碑保留在
                    // 磁盘上(参与 max-revision 去重),种子映射必须随之保留。
                } else {
                    maxRevisions.put(id, intent.getRevision());
                    // C18-1a(r18): 终态多槽胜者也补标 multiSlot——overdue 变体(P0-2 经
                    // overwriteSlot 直接终态化)不产生任何进程内簿记,count>1 时陈旧兄弟槽
                    // 留盘,同进程重建→终态→回收→再重建的链条在本进程内即可复现遮蔽;补标后
                    // 重建终态 !singleSlot → tombstoneIds.add → 永久保护。
                    if (count > 1) {
                        multiSlot.add(id);
                    }
                    // C18-1b(r18): wheel count>1 墓碑 + tail 终态(count==1 也落入本臂,
                    // 与 persistTerminalInPlace tail 分支的进程内语义对齐)→ 注入墓碑集,
                    // 保护 maxRevisions 种子映射不被后续单槽 incarnation 终态回收移除(C4-1)。
                    // wheel count==1 单槽已被上方 freeSlot、磁盘无残留,不需注入(与既有注释一致)。
                    tombstonedIds.add(id);
                }
                continue;
            }
            long execMs = intent.getExecuteAt().toEpochMilli();
            if (!clockRolledBack && execMs < nowMs) {
                // P0-2: 停机窗口到期的 Intent 不再静默丢弃。按 ExpiredAction 补终态
                // 并持久化终态 revision,使下次重启恢复时被 terminal 跳过(避免每次重启重复处理)。
                // 不补投——投递窗口已过,补投对下游往往是过时事件;留终态痕迹与告警即可。
                IntentStatus terminal = intent.getExpiredAction() == ExpiredAction.DEAD_LETTER
                    ? IntentStatus.DEAD_LETTERED : IntentStatus.EXPIRED;
                try {
                    intent.transitionTo(terminal);
                    intent.incrementRevision();
                    // R10: 终态化 +1 后同步推进 maxRevisions——否则报告仍记旧 revision,
                    // 同进程重建会种子到与终态墓碑平票的 revision,重启去重(严格 >,平票
                    // 先扫到者胜)被过去时刻的终态槽遮蔽。
                    maxRevisions.put(id, intent.getRevision());
                    // 终态原地覆写,不分配新槽:原实现 store.put 会为过期 executeAt 分配新槽,
                    // 旧 SCHEDULED 槽残留(stale 兄弟),同 id 槽数恒为 2 → 终态槽因 count>1
                    // 永不回收,只能等桶文件过期(31 天)被 BucketReclaimer 删除;tail 来源的
                    // 过期条目还会在 wheel 里写一个过去时刻的槽而 tail 条目原样残留。
                    // 原地覆写:单槽即终态,下次重启 terminal+count==1 即被 freeSlot 回收自愈。
                    if (e.loc().inTail()) {
                        tail.put(intent);   // tail 条目覆盖为终态(revision 递增,重放后序生效)
                    } else {
                        store.overwriteSlot(e.loc(), SlotCodec.encode(intent));
                    }
                    metricsCollector.incrementRecoveryOverdue();
                    log.warn("Recovery: intent {} overdue (execMs={} < now={}); marked {}",
                        id, execMs, nowMs, terminal);
                } catch (Exception ex) {
                    log.error("Recovery: failed to mark overdue intent {}", id, ex);
                }
                continue;
            }
            if (count > 1) multiSlot.add(id);    // F1: 磁盘上幸存兄弟 >1 → 终态须保留墓碑不回收
            maxRevisions.put(id, intent.getRevision());   // R8 种子:非终态活记录保留在磁盘,重建须抬升其上
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
        return new WheelRecoveryReport(hot, cold, multiSlot, maxRevisions, tombstonedIds);
    }
}
