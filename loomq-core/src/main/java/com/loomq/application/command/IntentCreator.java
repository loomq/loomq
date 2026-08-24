package com.loomq.application.command;

import com.loomq.application.scheduler.PrecisionScheduler;
import com.loomq.common.IntentValidator;
import com.loomq.common.MetricsCollector;
import com.loomq.domain.intent.AckMode;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.domain.intent.PrecisionTierCatalog;
import com.loomq.domain.intent.WalMode;
import com.loomq.infrastructure.wheel.IntentLocationIndex;
import com.loomq.infrastructure.wheel.PromotionDaemon;
import com.loomq.infrastructure.wheel.SlotCodec;
import com.loomq.infrastructure.wheel.SlotLocation;
import com.loomq.infrastructure.wheel.TailIndex;
import com.loomq.infrastructure.wheel.WheelStore;
import com.loomq.store.IntentStore;
import com.loomq.tracing.IntentTraceStore;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Intent 创建路径（round 10 自 IntentCommandService 拆出）:
 * 单条/批量创建、失败补偿取消、活跃副本预检。
 * 持久化经 {@link WheelPersistence};revision 种子经其 maxRevisionOf 缝读取。
 */
final class IntentCreator {

    private static final Logger logger = LoggerFactory.getLogger(IntentCreator.class);

    private final IntentStore intentStore;
    private final PrecisionScheduler scheduler;
    private final PromotionDaemon promotionDaemon;
    private final MetricsCollector metricsCollector;
    private final IntentTraceStore traceStore;
    private final AtomicLong sequenceNumber;
    private final PrecisionTier defaultTier;
    private final long groupCommitIntervalMs;
    private final long hotBoundaryMs;
    private final PrecisionTierCatalog precisionTierCatalog;
    // hasActiveDuplicate 只读依赖（与 WheelPersistence 共享叶子，非所有权）
    private final WheelStore wheelStore;
    private final TailIndex tailIndex;
    private final IntentLocationIndex locationIndex;
    private final WheelPersistence persistence;

    IntentCreator(IntentStore intentStore, PrecisionScheduler scheduler,
                  PromotionDaemon promotionDaemon, MetricsCollector metricsCollector,
                  IntentTraceStore traceStore, AtomicLong sequenceNumber,
                  PrecisionTier defaultTier, long groupCommitIntervalMs, long hotBoundaryMs,
                  PrecisionTierCatalog precisionTierCatalog,
                  WheelStore wheelStore, TailIndex tailIndex, IntentLocationIndex locationIndex,
                  WheelPersistence persistence) {
        this.intentStore = intentStore;
        this.scheduler = scheduler;
        this.promotionDaemon = promotionDaemon;
        this.metricsCollector = metricsCollector;
        this.traceStore = traceStore;
        this.sequenceNumber = sequenceNumber;
        this.defaultTier = defaultTier;
        this.groupCommitIntervalMs = groupCommitIntervalMs;
        this.hotBoundaryMs = hotBoundaryMs;
        this.precisionTierCatalog = precisionTierCatalog;
        this.wheelStore = wheelStore;
        this.tailIndex = tailIndex;
        this.locationIndex = locationIndex;
        this.persistence = persistence;
    }

    /**
     * 创建 Intent 并调度。
     *
     * <p>持久化语义取决于 ackMode:
     * <ul>
     *   <li><b>DURABLE</b>(默认):写入 PHTW 后阻塞到 group-commit msync,崩溃不丢数据</li>
     *   <li><b>ASYNC</b>:写入 mmap 后立即返回(由 group-commit daemon 异步刷盘),崩溃时可能丢失最近创建的 Intent</li>
     * </ul>
     *
     * @return 序列号
     */
    long createIntent(Intent intent, AckMode ackMode) {
        IntentValidator.validate(intent);
        // R6: 同一 intentId 双活副本 → 双投递内容分叉 + store 条目来回覆写 + 磁盘残留
        // 非终态 stale 槽（recovery 按 max revision 去重，败者槽永不回收，桶容量永久
        // 泄漏，I1 破坏）。locationIndex 是"活 intent"登记表；磁盘终态（含补偿 CANCELED）
        // 的 intent 允许同 id 重建（幂等重试）。检查在补偿路径之外——否则失败补偿会把
        // 已存在的 intent 覆写成 CANCELED。并发同 id 创建仍有预检 TOCTOU 窗口，由调用方
        // 序列化（与 checkIdempotency 同纪律）。
        if (hasActiveDuplicate(intent.getIntentId())) {
            throw new IllegalArgumentException(
                "duplicate intentId: " + intent.getIntentId() + " already active");
        }
        // 归一化放在重复检查之后，避免重复创建被拒时仍然修改调用方传入的 Intent。
        IntentCommandService.normalizePrecisionTier(precisionTierCatalog, intent);

        long seq = sequenceNumber.incrementAndGet();

        try {
            // Apply engine-level default tier if configured
            if (defaultTier != null) {
                intent.setPrecisionTier(defaultTier);
            }

            intent.transitionTo(IntentStatus.SCHEDULED);
            // R8: 重建同 intentId 时种子 revision 到磁盘历史最高值之上——否则新 Intent 从 0
            // 起步,recovery 按 max revision 去重会被旧终态墓碑(更高 revision,multiSlot 保留)
            // 遮蔽,新 Intent 静默丢失。种子后 incrementRevision 使新槽 revision 严格大于历史最高。
            Long histMax = persistence.maxRevisionOf(intent.getIntentId());
            if (histMax != null && histMax >= intent.getRevision()) {
                intent.setRevision(histMax);
            }
            intent.incrementRevision();

            WalMode effectiveMode =
                IntentCommandService.resolveWalMode(precisionTierCatalog, intent, ackMode);

            // H1:准确陈述崩溃窗口。group-commit daemon 每 groupCommitIntervalMs 对所有脏桶批量
            // fsync,非 DURABLE 写入后字节已在 mmap,崩溃窗口 ≤ groupCommitIntervalMs。
            // 仅在间隔较大(>100ms)时提示风险,小间隔降级 debug。
            if (effectiveMode != WalMode.DURABLE) {
                if (groupCommitIntervalMs > 100) {
                    logger.warn("Intent {} using non-DURABLE walMode={}, crash window <= {}ms",
                        intent.getIntentId(), effectiveMode, groupCommitIntervalMs);
                } else {
                    logger.debug("Intent {} using non-DURABLE walMode={}, crash window <= {}ms",
                        intent.getIntentId(), effectiveMode, groupCommitIntervalMs);
                }
            }

            // 3. 写入持久化分层时间轮(磁盘权威)+ 更新索引 + DURABLE 阻塞到 group-commit msync。
            //    wheelStore.put 返回实际分配槽位(slotIndex>=0);locate() 仅返回 slotIndex=-1 的占位。
            SlotLocation loc = persistence.persistToWheel(intent, effectiveMode == WalMode.DURABLE);

            // 5. 热(≤hotBoundaryMs)→ 进内存热尖 + 调度;冷 → 注册 promotion cohort(到点由 PromotionDaemon 载入)
            long deltaMs = intent.getExecuteAt().toEpochMilli() - System.currentTimeMillis();
            if (deltaMs <= hotBoundaryMs) {
                intentStore.save(intent);
                scheduler.schedule(intent);
            } else {
                promotionDaemon.register(intent.getIntentId(), loc, intent.getExecuteAt().toEpochMilli());
                // R21: 冷 create 从未 schedule,补 trace 条目——否则冷 intent 排查无迹可循
                // (recordCreatedIfNew 幂等:同 incarnation 已存在则跳过)
                long createdAtMs = intent.getCreatedAt() != null
                    ? intent.getCreatedAt().toEpochMilli() : System.currentTimeMillis();
                traceStore.recordCreatedIfNew(intent.getIntentId(), intent.getTraceId(),
                    intent.getPrecisionTier(), createdAtMs);
            }

            metricsCollector.incrementIntentsCreated();
            // R21: intent_total 统计创建数(HELP 语义),按 tier 维度——旧实现在
            // finalizeIntent 结算路径计数,重试虚增、冷 intent 不计
            metricsCollector.incrementIntentByTier(intent.getPrecisionTier());
            logger.debug("Intent created: id={}, ackMode={}, walMode={}, seq={}",
                intent.getIntentId(), ackMode, effectiveMode, seq);

            return seq;
        } catch (Exception e) {
            logger.error("Failed to finalize intent creation: id={} (wheel write may already be persisted)",
                intent.getIntentId(), e);
            compensateCancel(intent);
            throw new RuntimeException(
                "Failed to create intent " + intent.getIntentId()
                    + " (compensation attempted; see logs for persistence state)", e);
        }
    }

    /**
     * Compensate cancel: rollback memory state + write CANCELED terminal revision.
     * Shared by createIntent / createIntents failure paths.
     */
    private void compensateCancel(Intent intent) {
        try {
            scheduler.removeFromSchedule(intent);
        } catch (Exception ex) {
            logger.error("Rollback removeFromSchedule failed for intent {}", intent.getIntentId(), ex);
        }
        try {
            intentStore.delete(intent.getIntentId());
        } catch (Exception ex) {
            logger.error("Rollback store.delete failed for intent {}", intent.getIntentId(), ex);
        }
        try {
            promotionDaemon.remove(intent.getIntentId());
        } catch (Exception ex) {
            logger.error("Rollback promotionDaemon.remove failed for intent {}", intent.getIntentId(), ex);
        }
        try {
            locationIndex.remove(intent.getIntentId());
        } catch (Exception ex) {
            logger.error("Rollback locationIndex.remove failed for intent {}", intent.getIntentId(), ex);
        }
        try {
            intent.transitionTo(IntentStatus.CANCELED);
            intent.incrementRevision();
            persistence.persistToWheel(intent, true);
            logger.warn("Compensation cancel written for intent {}", intent.getIntentId());
        } catch (Exception compEx) {
            logger.error("Compensation cancel failed for intent {}; recovery may resurrect it",
                intent.getIntentId(), compEx);
        }
    }

    /**
     * Batch create: share a single awaitCommit for all intents.
     * Phase 1: write all to wheel (no awaitCommit). Phase 2: single awaitCommit. Phase 3: memory schedule.
     */
    List<Long> createIntents(List<Intent> intents, AckMode ackMode) {
        if (intents.isEmpty()) return List.of();

        for (Intent intent : intents) {
            IntentValidator.validate(intent);
        }

        // R6: 批量创建预检——与已有活 intent 冲突或批内重复 intentId → 写盘前整体拒绝。
        // 预检失败不触发补偿（补偿只对已写入条目生效），避免把已存在的 intent 覆写为
        // CANCELED。
        Set<String> seenIds = new HashSet<>(intents.size());
        for (Intent intent : intents) {
            if (!seenIds.add(intent.getIntentId()) || hasActiveDuplicate(intent.getIntentId())) {
                throw new IllegalArgumentException(
                    "duplicate intentId: " + intent.getIntentId() + " already active");
            }
        }

        // 逐条解析持久化语义：整批的耐久性不能由首条 intent 决定——混合批次（如首条
        // ASYNC、后条 DURABLE）会让 DURABLE 条目跳过 awaitCommit，崩溃即丢失。
        // 任一条 DURABLE 即整批 awaitCommit 一次（barrier ticket 覆盖所有已入 mmap 的写入，
        // ASYNC 条目顺带落盘无害）。
        boolean durable = false;
        for (Intent intent : intents) {
            if (IntentCommandService.resolveWalMode(precisionTierCatalog, intent, ackMode)
                == WalMode.DURABLE) {
                durable = true;
                break;
            }
        }
        List<Long> seqs = new ArrayList<>(intents.size());
        List<SlotLocation> locs = new ArrayList<>(intents.size());
        int written = 0;
        long[] oldRevisions = new long[intents.size()];
        IntentStatus[] oldStatuses = new IntentStatus[intents.size()];
        Instant[] oldUpdatedAts = new Instant[intents.size()];
        PrecisionTier[] oldTiers = new PrecisionTier[intents.size()];

        try {
            for (int i = 0; i < intents.size(); i++) {
                Intent intent = intents.get(i);
                long seq = sequenceNumber.incrementAndGet();
                seqs.add(seq);
                oldStatuses[i] = intent.getStatus();
                oldRevisions[i] = intent.getRevision();
                oldUpdatedAts[i] = intent.getUpdatedAt();
                oldTiers[i] = intent.getPrecisionTier();
                if (defaultTier != null) {
                    intent.setPrecisionTier(defaultTier);
                }
                IntentCommandService.normalizePrecisionTier(precisionTierCatalog, intent);
                intent.transitionTo(IntentStatus.SCHEDULED);
                // R8: 同 createIntent——重建同 intentId 时种子 revision 到历史最高值之上,
                // 否则 recovery max-revision 去重会遮蔽新 Intent。
                Long histMax = persistence.maxRevisionOf(intent.getIntentId());
                if (histMax != null && histMax >= intent.getRevision()) {
                    intent.setRevision(histMax);
                }
                intent.incrementRevision();
                SlotLocation loc = persistence.persistToWheel(intent, false);
                locs.add(loc);
                written++;
            }
            if (durable) {
                persistence.awaitDurableCommit();
            }
        } catch (Exception e) {
            logger.error("Batch createIntent persistence failed; {} intents written, compensating", written, e);
            for (int i = 0; i < written; i++) {
                compensateCancel(intents.get(i));
            }
            for (int i = written; i < intents.size(); i++) {
                Intent intent = intents.get(i);
                // R14: 回滚 updatedAt 须用原始值,而非变异后的 intent.getUpdatedAt()——
                // 否则非持久化 intent 回滚后 updatedAt 残留 transitionTo/incrementRevision
                // 的时间戳,与已还原的 status/revision 不一致。
                // R24: 回滚同样恢复 precisionTier——defaultTier 覆盖不是用户意愿。
                // 注意：setPrecisionTier 会更新 updatedAt，因此必须在 rollbackStatus 之前执行，
                // 由 rollbackStatus 统一还原 updatedAt。
                intent.setPrecisionTier(oldTiers[i]);
                intent.rollbackStatus(oldStatuses[i], oldUpdatedAts[i], oldRevisions[i]);
            }
            throw new RuntimeException("Batch createIntent persistence failed; " + written + " intents compensated", e);
        }

        RuntimeException schedulingFailure = null;
        for (int i = 0; i < intents.size(); i++) {
            Intent intent = intents.get(i);
            try {
                long deltaMs = intent.getExecuteAt().toEpochMilli() - System.currentTimeMillis();
                if (deltaMs <= hotBoundaryMs) {
                    intentStore.save(intent);
                    scheduler.schedule(intent);
                } else {
                    promotionDaemon.register(intent.getIntentId(), locs.get(i), intent.getExecuteAt().toEpochMilli());
                    // R21: 冷 create 补 trace 条目(同 createIntent 单条路径)
                    long createdAtMs = intent.getCreatedAt() != null
                        ? intent.getCreatedAt().toEpochMilli() : System.currentTimeMillis();
                    traceStore.recordCreatedIfNew(intent.getIntentId(), intent.getTraceId(),
                        intent.getPrecisionTier(), createdAtMs);
                }
                metricsCollector.incrementIntentsCreated();
                // R21: intent_total 按 tier 统计创建数(同 createIntent 单条路径)
                metricsCollector.incrementIntentByTier(intent.getPrecisionTier());
            } catch (Exception e) {
                // 不中断循环：其余 intent 已在 phase 1 持久化（wheel/tail 磁盘权威），
                // 跳过调度会留下"已提交但本进程不投递、仅重启后恢复"的不一致尾巴。
                // 补偿失败的条目并继续调度剩余条目，循环结束后统一向调用方报告首个失败。
                logger.error("Post-persist scheduling failed for intent {}", intent.getIntentId(), e);
                compensateCancel(intent);
                if (schedulingFailure == null) {
                    schedulingFailure = new RuntimeException(
                        "Post-persist scheduling failed for intent " + intent.getIntentId(), e);
                }
            }
        }
        if (schedulingFailure != null) {
            throw schedulingFailure;
        }
        return seqs;
    }

    /**
     * 判定 intentId 是否已有"活跃"副本（非终态）。
     *
     * <p>磁盘为权威：locationIndex 指向的最新槽非终态即活跃。终态（含补偿 CANCELED，
     * 其索引条目在补偿后仍保留）允许同 id 重建——这是"创建失败后幂等重试"的合法路径。
     * tail 位置经 {@code scanFrom} 解码判定（终态化 tail 条目走回退追加，同样可判）。
     * 槽空但索引在（不一致状态）按活跃保守拒绝。
     */
    private boolean hasActiveDuplicate(String intentId) {
        SlotLocation loc = locationIndex.get(intentId);
        if (loc == null) {
            return false;
        }
        if (loc.inTail()) {
            var it = tailIndex.scanFrom(loc.bucketKey());
            while (it.hasNext()) {
                // R21: 防御解码(与 R4/P1-6 恢复路径同款)——tail 记录"结构完整但槽
                // 字节损坏"(bit rot)时裸 decode 抛 CRC ISE 穿透 createIntent;
                // 损坏条目按不存在跳过(恢复侧同样跳过,索引本就不该引用它)。
                Intent cur = SlotCodec.decodeSafe(it.next().encodedSlot());
                if (cur == null) {
                    logger.warn("hasActiveDuplicate: skipping corrupt tail entry for id {}", intentId);
                    continue;
                }
                if (intentId.equals(cur.getIntentId()) && !cur.getStatus().isTerminal()) {
                    return true;
                }
            }
            return false;
        }
        Intent cur = wheelStore.readSlot(loc);
        return cur != null && !cur.getStatus().isTerminal();
    }
}
