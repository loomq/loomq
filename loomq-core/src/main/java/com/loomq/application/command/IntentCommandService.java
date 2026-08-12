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
import com.loomq.infrastructure.wheel.GroupCommitBarrier;
import com.loomq.infrastructure.wheel.IntentLocationIndex;
import com.loomq.infrastructure.wheel.PromotionDaemon;
import com.loomq.infrastructure.wheel.SlotCodec;
import com.loomq.infrastructure.wheel.SlotLocation;
import com.loomq.infrastructure.wheel.TailIndex;
import com.loomq.infrastructure.wheel.WheelStore;
import com.loomq.spi.CallbackHandler;
import com.loomq.store.IdempotencyResult;
import com.loomq.store.IntentStore;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 核心命令服务。
 *
 * 统一承接 intent 的创建、更新、取消和立即触发，避免业务命令逻辑分散在
 * LoomqEngine、HTTP 适配层和调度器之间。
 *
 * <p>持久化由持久化分层时间轮(PHTW)栈承担:写入 {@link WheelStore}(磁盘权威)或
 * {@link TailIndex}(超 day 视界),{@link GroupCommitBarrier} 提供 DURABLE group-commit,
 * {@link IntentLocationIndex} 记录 intentId→槽位以支持冷取消,{@link PromotionDaemon}
 * 负责冷→热提升 cohort。
 */
public final class IntentCommandService {

    private static final Logger logger = LoggerFactory.getLogger(IntentCommandService.class);

    /** updateIntent 认领分支跳过重排的告警模板（两处分支共用，避免字符串漂移）。 */
    private static final String CLAIMED_SKIP_WARN =
        "updateIntent: intent {} already claimed by scanDue; update will be persisted but "
            + "in-flight delivery may carry pre-update content (no reschedule)";

    private final IntentStore intentStore;
    private final PrecisionScheduler scheduler;
    private final WheelStore wheelStore;
    private final TailIndex tailIndex;
    private final GroupCommitBarrier commitBarrier;
    private final IntentLocationIndex locationIndex;
    private final PromotionDaemon promotionDaemon;
    private final MetricsCollector metricsCollector;
    private final Executor callbackExecutor;
    private final AtomicBoolean running;
    private final AtomicLong sequenceNumber;

    private volatile CallbackHandler callbackHandler;
    private final PrecisionTier defaultTier;
    private final long groupCommitIntervalMs;
    private final long hotBoundaryMs;

    /**
     * 冷取消按 intentId 串行化的细粒度锁注册表。
     *
     * <p>wheel 路径专用:wheelStore.readSlot 每次返回新解码实例,synchronized(cold) 锁的是 transient
     * 副本,无法阻塞并发取消。改为按 intentId 取一把稳定锁对象,串行化读-改-写。tail 路径不进此表
     * ——其并发由 tailIndex.remove 的布尔返回值门控(appendLock 仅串行单次 append,不串行
     * remove→awaitCommit→metric++ 序列)。锁对象在 synchronized 块的 finally 中以 identity 校验
     * 移除(computeIfPresent),只删自己放入的对象,避免误删后到者的锁。</p>
     */
    private final ConcurrentHashMap<String, Object> coldCancelLocks = new ConcurrentHashMap<>();

    /** 曾发生第 2 次及以上槽写入的 Intent（重排程/改期/取消追加）→ 终态不回收，保留 tombstone。 */
    private final java.util.Set<String> multiSlotIntents = java.util.concurrent.ConcurrentHashMap.newKeySet();
    /** 终态原地覆写后的待回收记录：终态槽在 awaitCommit 落盘后清空。 */
    private final java.util.Map<String, PendingReclaim> pendingReclaims = new java.util.concurrent.ConcurrentHashMap<>();
    /** 待回收记录：loc 为终态槽位置；singleSlot 为 true 才清空复用。 */
    public record PendingReclaim(SlotLocation loc, boolean singleSlot) {}

    public IntentCommandService(
        IntentStore intentStore,
        PrecisionScheduler scheduler,
        WheelStore wheelStore,
        TailIndex tailIndex,
        GroupCommitBarrier commitBarrier,
        IntentLocationIndex locationIndex,
        PromotionDaemon promotionDaemon,
        MetricsCollector metricsCollector,
        Executor callbackExecutor,
        AtomicBoolean running,
        AtomicLong sequenceNumber,
        CallbackHandler callbackHandler,
        PrecisionTier defaultTier,
        long groupCommitIntervalMs,
        long hotBoundaryMs
    ) {
        this.intentStore = intentStore;
        this.scheduler = scheduler;
        this.wheelStore = wheelStore;
        this.tailIndex = tailIndex;
        this.commitBarrier = commitBarrier;
        this.locationIndex = locationIndex;
        this.promotionDaemon = promotionDaemon;
        this.metricsCollector = metricsCollector;
        this.callbackExecutor = callbackExecutor;
        this.running = running;
        this.sequenceNumber = sequenceNumber;
        this.callbackHandler = callbackHandler;
        this.defaultTier = defaultTier;
        this.groupCommitIntervalMs = groupCommitIntervalMs;
        this.hotBoundaryMs = hotBoundaryMs;
    }

    public void registerCallbackHandler(CallbackHandler handler) {
        this.callbackHandler = handler;
        if (handler != null) {
            logger.info("Callback handler registered: {}", handler.getClass().getSimpleName());
        } else {
            logger.info("Callback handler cleared");
        }
    }

    public IdempotencyResult checkIdempotency(String idempotencyKey) {
        return intentStore.checkIdempotency(idempotencyKey);
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
    public long createIntent(Intent intent, AckMode ackMode) {
        ensureRunning();
        IntentValidator.validate(intent);

        long seq = sequenceNumber.incrementAndGet();

        try {
            // Apply engine-level default tier if configured
            if (defaultTier != null) {
                intent.setPrecisionTier(defaultTier);
            }

            intent.transitionTo(IntentStatus.SCHEDULED);
            intent.incrementRevision();

            WalMode effectiveMode = resolveWalMode(intent, ackMode);

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
            SlotLocation loc = persistToWheel(intent, effectiveMode == WalMode.DURABLE);

            // 5. 热(≤hotBoundaryMs)→ 进内存热尖 + 调度;冷 → 注册 promotion cohort(到点由 PromotionDaemon 载入)
            long deltaMs = intent.getExecuteAt().toEpochMilli() - System.currentTimeMillis();
            if (deltaMs <= hotBoundaryMs) {
                intentStore.save(intent);
                scheduler.schedule(intent);
            } else {
                promotionDaemon.register(intent.getIntentId(), loc, intent.getExecuteAt().toEpochMilli());
            }

            metricsCollector.incrementIntentsCreated();
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
            persistToWheel(intent, true);
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
    public List<Long> createIntents(List<Intent> intents, AckMode ackMode) {
        ensureRunning();
        if (intents.isEmpty()) return List.of();

        for (Intent intent : intents) {
            IntentValidator.validate(intent);
        }

        WalMode effectiveMode = resolveWalMode(intents.get(0), ackMode);
        boolean durable = effectiveMode == WalMode.DURABLE;
        List<Long> seqs = new ArrayList<>(intents.size());
        List<SlotLocation> locs = new ArrayList<>(intents.size());
        int written = 0;
        long[] oldRevisions = new long[intents.size()];
        IntentStatus[] oldStatuses = new IntentStatus[intents.size()];

        try {
            for (int i = 0; i < intents.size(); i++) {
                Intent intent = intents.get(i);
                long seq = sequenceNumber.incrementAndGet();
                seqs.add(seq);
                if (defaultTier != null) {
                    intent.setPrecisionTier(defaultTier);
                }
                oldStatuses[i] = intent.getStatus();
                oldRevisions[i] = intent.getRevision();
                intent.transitionTo(IntentStatus.SCHEDULED);
                intent.incrementRevision();
                SlotLocation loc = persistToWheel(intent, false);
                locs.add(loc);
                written++;
            }
            if (durable) {
                commitBarrier.awaitCommit();
            }
        } catch (Exception e) {
            logger.error("Batch createIntent persistence failed; {} intents written, compensating", written, e);
            for (int i = 0; i < written; i++) {
                compensateCancel(intents.get(i));
            }
            for (int i = written; i < intents.size(); i++) {
                Intent intent = intents.get(i);
                intent.rollbackStatus(oldStatuses[i], intent.getUpdatedAt(), oldRevisions[i]);
            }
            throw new RuntimeException("Batch createIntent persistence failed; " + written + " intents compensated", e);
        }

        for (int i = 0; i < intents.size(); i++) {
            Intent intent = intents.get(i);
            try {
                long deltaMs = intent.getExecuteAt().toEpochMilli() - System.currentTimeMillis();
                if (deltaMs <= hotBoundaryMs) {
                    intentStore.save(intent);
                    scheduler.schedule(intent);
                } else {
                    promotionDaemon.register(intent.getIntentId(), locs.get(i), intent.getExecuteAt().toEpochMilli());
                }
                metricsCollector.incrementIntentsCreated();
            } catch (Exception e) {
                logger.error("Post-persist scheduling failed for intent {}", intent.getIntentId(), e);
                compensateCancel(intent);
                throw new RuntimeException("Post-persist scheduling failed for intent " + intent.getIntentId(), e);
            }
        }
        return seqs;
    }

    public Optional<Intent> updateIntent(String intentId, Consumer<Intent> updater) {
        return updateIntent(intentId, updater, null);
    }

    /**
     * 更新 Intent（可选改期）。
     *
     * <p><b>与在途投递的竞态语义：</b>若 scanDue 已 CAS 认领该 Intent（投递在途），
     * 本方法仍会应用 updater 的变更并 DURABLE 持久化，但<b>不重排程</b>。首轮投递
     * 携带派发时刻的内容快照（更新前）；若投递失败重试，重投携带最新持久化内容
     * （更新后）。更新在重试路径或崩溃恢复（max-revision 胜者）中确定生效。</p>
     *
     * @param intentId     待更新 Intent ID
     * @param updater      变更消费者（在 synchronized(intent) 内执行）
     * @param newExecuteAt 新的执行时间；null 表示不改期
     * @return 更新后的 Intent；intent 不存在时返回 empty；intent 已处终态时返回未修改的
     *         intent（no-op，不持久化）
     */
    public Optional<Intent> updateIntent(String intentId, Consumer<Intent> updater, Instant newExecuteAt) {
        ensureRunning();

        Intent intent = intentStore.findByIdInternal(intentId);
        if (intent == null) {
            return Optional.empty();
        }

        boolean reschedule = false;
        try {
            synchronized (intent) {
                if (intent.getStatus().isTerminal()) {
                    logger.warn("Cannot update intent {} in terminal state {}; no-op", intentId, intent.getStatus());
                    return Optional.of(intent);
                }
                Instant oldExecuteAt = intent.getExecuteAt();
                reschedule = newExecuteAt != null && !newExecuteAt.equals(oldExecuteAt);

                if (reschedule) {
                    // P1-5: 只对可调度状态(SCHEDULED/DUE)执行 removeFromSchedule。
                    // DISPATCHING/DELIVERED 在途投递,removeFromSchedule 已执行而 schedule/restore
                    // 会拒收(只认 CREATED/SCHEDULED)→ Intent 从调度结构消失直到重启。
                    IntentStatus st = intent.getStatus();
                    if (st == IntentStatus.SCHEDULED || st == IntentStatus.DUE) {
                        boolean wasScheduled = scheduler.removeFromSchedule(intent);
                        if (!wasScheduled) {
                            // 已被 scanDue CAS 认领，在途投递正在执行。
                            // updater 的变更随后仍会持久化，但不重排--在途投递可能携带
                            // 旧内容，更新仅在重试/崩溃恢复路径确定生效（见方法 javadoc）。
                            logger.warn(CLAIMED_SKIP_WARN, intentId);
                            reschedule = false;
                        }
                    } else {
                        logger.warn("Cannot reschedule intent {} in {} state; keeping original schedule", intentId, st);
                        reschedule = false;
                    }
                }

                updater.accept(intent);

                // 安全网：updater 可能直接通过 intent.setExecuteAt() 修改了执行时间，
                // 此时 newExecuteAt 为 null 导致 reschedule 初始为 false，需要补检。
                if (!reschedule) {
                    Instant actualExecuteAt = intent.getExecuteAt();
                    if (actualExecuteAt != null && !actualExecuteAt.equals(oldExecuteAt)) {
                        IntentStatus st = intent.getStatus();
                        if (st == IntentStatus.SCHEDULED || st == IntentStatus.DUE) {
                            // 必须在 updater 已修改 executeAt 之后、重新调度之前,
                            // 用旧的 executeAt 清理索引(removeFromSchedule 内部用的是当前 executeAt)
                            boolean wasScheduled = scheduler.removeFromSchedule(intent, oldExecuteAt);
                            if (wasScheduled) {
                                reschedule = true;
                            } else {
                                logger.warn(CLAIMED_SKIP_WARN, intentId);
                            }
                        } else {
                            logger.warn("Cannot reschedule intent {} in {} state after updater; keeping original", intentId, st);
                        }
                    }
                }

                if (newExecuteAt != null) {
                    intent.setExecuteAt(newExecuteAt);
                }

                intent.incrementRevision();
                persistIntentState(intent, AckMode.DURABLE);
                intentStore.update(intent);
                // I5: schedule/restore 移到锁外 (deferred)
                // -- schedule() 有自己的 synchronized + 终态检查
            }
            // I5: schedule/restore 在锁外调用
            if (reschedule) {
                if (intent.getStatus() == IntentStatus.DUE) {
                    scheduler.restore(intent);
                } else {
                    scheduler.schedule(intent);
                }
            }
            return Optional.of(intent);
        } catch (RuntimeException e) {
            logger.error("Failed to update intent: id={}", intentId, e);
            throw e;
        }
    }

    /**
     * 取消 Intent。
     *
     * <p><b>语义：best-effort。</b>取消操作对于已进入投递流程（DISPATCHING）的 Intent 无效--
     * 异步投递可能已完成，事件可能已到达下游。调用方必须确保下游处理逻辑的幂等性。</p>
     *
     * <p><b>与投递快照的交互：</b>DeliveryHandler 收到的是派发时刻快照，其 status 反映
     * 派发时刻值。若 cancel 在快照之后到达，handler 不会观察到 CANCELED 状态--
     * 取消感知须由下游幂等承担（本就是 best-effort 契约）。</p>
     *
     * <p>对于热 Intent（在内存 store 中）：经 synchronized(intent) 串行化状态迁移，
     * 成功则 removeFromSchedule + DURABLE 落盘 CANCELED 终态。</p>
     *
     * <p>对于冷 Intent（不在内存 store 中）：经 locationIndex 定位磁盘槽，
     * 互斥写 CANCELED 终态槽。</p>
     *
     * @param intentId 待取消的 Intent ID
     * @return true 如果取消成功；false 如果 Intent 不存在或已处于终态
     */
    public boolean cancelIntent(String intentId) {
        ensureRunning();

        Intent intent = intentStore.findByIdInternal(intentId);
        if (intent == null) {
            // 冷意图:不在内存 store,经 locationIndex 定位磁盘槽取消
            return cancelCold(intentId);
        }

        IntentStatus oldStatus = null;
        Instant oldUpdatedAt = null;
        long oldRevision = 0;
        try {
            Intent callbackSnapshot = null;
            synchronized (intent) {
                // 在 transitionTo 之前记录原始状态，用于回滚。
                oldStatus = intent.getStatus();
                oldUpdatedAt = intent.getUpdatedAt();
                oldRevision = intent.getRevision();

                // transitionTo 单独 try-catch：仅捕获状态机校验失败，
                // 不会误吞 persistIntentState / intentStore.update 抛出的 ISE。
                try {
                    intent.transitionTo(IntentStatus.CANCELED);
                } catch (IllegalStateException e) {
                    logger.warn("Cannot cancel intent {}: {}", intentId, e.getMessage());
                    return false;
                }
                scheduler.removeFromSchedule(intent);
                intent.incrementRevision();
                persistTerminalInPlace(intent);   // 原地覆写终态,不追加新槽(无索引/tail 时回退追加)
                intentStore.update(intent);
                locationIndex.remove(intentId);  // 终态 Intent 不保留索引(桶回收依赖)
                // I5: 锁内取快照，锁外派发
                callbackSnapshot = intent.copy();
            }

            // 锁外：等 group-commit 落盘后再回收终态槽（VT 可正常 unmount）
            awaitDurableCommit();
            reclaimTerminal(intentId);

            // 在 synchronized 块外派发回调——回滚窗口已关闭，
            // callback 异常（如 RejectedExecutionException）不会触发状态回滚。
            // I5: 传递防御性快照，用户代码无法触达内核活状态
            if (callbackSnapshot != null) {
                dispatchCallback(callbackSnapshot, CallbackHandler.EventType.CANCELLED, null);
            }

            metricsCollector.incrementIntentsCancelled();
            logger.info("Intent cancelled: id={}", intentId);
            return true;
        } catch (RuntimeException e) {
            logger.error("Failed to cancel intent: id={}", intentId, e);
            // 回滚：清理待回收记录（不 free 槽——intent 回滚为活态，槽仍是其 tombstone）
            pendingReclaims.remove(intentId);
            multiSlotIntents.remove(intentId);
            if (oldStatus != null && intent.getStatus() != oldStatus) {
                intent.rollbackStatus(oldStatus, oldUpdatedAt, oldRevision);
                scheduler.restore(intent);
            }
            throw e;
        }
    }

    /**
     * 冷取消:Intent 不在内存 store(>hotBoundaryMs 未提升或 >horizon 落 tail),经 locationIndex
     * 定位磁盘槽位取消。cancel 为状态变更操作:成功路径恒为 DURABLE(写新槽/tombstone +
     * awaitCommit);槽位缺失/损坏时返回 false,不谎报未持久化的取消。
     *
     * <ul>
     *   <li>tail:追加 TOMBSTONE 到 run 文件 + awaitCommit 强制落盘;以 TailIndex.remove 返回值门控 ——
 *       remove 返回 false 表示已被并发取消者移除(已追加 tombstone),直接返回 false 不重复计数。
 *       appendLock 仅串行单次 append,不串行 remove→awaitCommit→metric++ 序列,故需布尔门控</li>
     *   <li>wheel:按 intentId 串行化(computeIfAbsent 锁)→ 锁内重读索引取最新槽 → 读槽解码 →
     *       transitionTo(CANCELED) + incrementRevision → 写新槽(recovery 按 max revision
     *       去重,terminal 跳过)→ 索引指向新 CANCELED 槽(在 awaitCommit 之前,杜绝在途 promote 复活)
     *       + awaitCommit。串行化保证后到者重读得到先到者写入的 CANCELED 槽(terminal)→ transitionTo
     *       抛 ISE → 返回 false,杜绝 double-write + double 计数</li>
     * </ul>
     * 槽位可读时最后移除 promotion cohort 与索引项;槽位缺失时直接返回 false(不动索引/cohort)。
     */
    private boolean cancelCold(String intentId) {
        SlotLocation loc = locationIndex.get(intentId);
        if (loc == null) {
            return false;                                   // 不存在(无索引项)
        }

        if (loc.inTail()) {
            // tail 路径:TailIndex.remove 返回 false 表示该 intent 已被并发取消者移除(已追加 tombstone)。
            // 此时不可谎报成功或重复计数 —— 直接返回 false(镜像 wheel 路径第二取消者行为)。
            // appendLock 仅串行单次 append,不串行 remove→awaitCommit→metric++ 序列,故需此布尔门控。
            if (!tailIndex.remove(intentId)) {
                return false;
            }
            commitBarrier.awaitCommit();                    // cancel 恒 DURABLE:确保 tombstone 落盘
        } else {
            // wheel 路径:readSlot 每次返回新解码实例,synchronized(cold) 锁的是 transient 副本,
            // 并发取消互不阻塞 → double-write + double 计数。按 intentId 串行化:第二个取消者
            // 串行进入后重读索引拿到先到者写入的 CANCELED 新槽(terminal)→ transitionTo 抛 ISE → 返回 false。
            Object lock = coldCancelLocks.computeIfAbsent(intentId, k -> new Object());
            synchronized (lock) {
                try {
                    // 必须在锁内重读索引:锁外拿到的 loc 是先到者写新槽前的旧槽位,指向 SCHEDULED 旧槽
                    // (WheelStore 状态变更写新槽,旧槽不被覆写)。重读得到先到者更新后的 CANCELED 槽才能让
                    // 后到者见到 terminal 状态。若先到者已走出锁并移除索引项,这里拿到 null → 返回 false。
                    SlotLocation latest = locationIndex.get(intentId);
                    if (latest == null) {
                        return false;
                    }
                    Intent cold = wheelStore.readSlot(latest);
                    if (cold == null) {
                        // 槽位缺失/损坏(空槽、撕裂写或桶已回收):无法持久化取消。
                        // 不可谎报成功——返回 false,调用方得知取消未生效(索引与 cohort 保持原状)。
                        return false;
                    }
                    try {
                        cold.transitionTo(IntentStatus.CANCELED);
                    } catch (IllegalStateException e) {
                        logger.warn("Cannot cancel cold intent {}: {}", intentId, e.getMessage());
                        return false;
                    }
                    cold.incrementRevision();
                    // 状态变更写新槽(revision 更高),recovery 按 intentId
                    // 取 max revision 胜者,terminal 状态跳过——不会重复投递。
                    // 关键:在 awaitCommit 之前把索引指向新 CANCELED 槽。否则 awaitCommit 窗口内,
                    // locationIndex 仍指向旧 SCHEDULED 槽,PromotionDaemon.promote 的 latest.equals(h.loc())
                    // 复核会通过(索引=旧槽=handle 槽)→ 读旧 SCHEDULED 槽 → onHotPromotion 复活已取消
                    // 的冷 Intent(ghost 投递)。指向新槽后:promote 见 latest≠h.loc() 直接跳过;即便
                    // 读到新槽也是 CANCELED(terminal)→ 跳过。两路均杜绝复活。
                    SlotLocation canceledLoc = wheelStore.put(cold);
                    locationIndex.put(intentId, canceledLoc);
                    commitBarrier.awaitCommit();            // cancel 恒 DURABLE
                } finally {
                    // 只移除自己放入的锁对象,避免误删后到者的锁(computeIfPresent + identity)。
                    // 后到者若通过 computeIfAbsent 拿到本锁对象(先到者尚未移除),会串行等待;其 cleanup
                    // 时若 map 仍持有同一对象则移除,若已被先到者移除或被更新者的新锁替换则 no-op。
                    coldCancelLocks.computeIfPresent(intentId, (k, v) -> v == lock ? null : v);
                }
            }
        }

        promotionDaemon.remove(intentId);                   // 取消提升 cohort
        locationIndex.remove(intentId);
        // P1-2: 与 promote 竞态收口——取消生效期间 intent 可能被 PromotionDaemon 并发提升入内存。
        // 若已热载,需从调度结构+store 移除,否则 ghost 投递直到重启 max-revision 纠正。
        // 与 LoomqEngine 中 promote 回调的 post-check 形成双向清理,确定性关闭竞态窗口。
        Intent hot = intentStore.findByIdInternal(intentId);
        if (hot != null) {
            scheduler.removeFromSchedule(hot);
            intentStore.delete(intentId);
            logger.warn("Cold cancel raced with promotion; removed hot copy of intent {}", intentId);
        }
        metricsCollector.incrementIntentsCancelled();
        logger.info("Cold intent cancelled: id={}", intentId);
        return true;
    }

    public boolean fireNow(String intentId) {
        ensureRunning();

        Intent intent = intentStore.findByIdInternal(intentId);
        if (intent == null) {
            // 冷意图尚在磁盘(未提升入内存),无法立即触发
            if (locationIndex.get(intentId) != null) {
                logger.warn("Cannot fire-now a cold intent (not yet promoted into memory): id={}", intentId);
            }
            return false;
        }

        if (intent.getStatus().isTerminal()) {
            logger.warn("Cannot fire intent in terminal state: id={}, status={}",
                intentId, intent.getStatus());
            return false;
        }

        Instant oldExecuteAt = null;
        long oldRevision = 0;
        boolean wasScheduled = false;
        try {
            synchronized (intent) {
                oldExecuteAt = intent.getExecuteAt();
                oldRevision = intent.getRevision();
                wasScheduled = scheduler.removeFromSchedule(intent);
                if (wasScheduled) {
                    intent.setExecuteAt(Instant.now());
                    intent.incrementRevision();
                    persistIntentState(intent, AckMode.DURABLE);
                    intentStore.update(intent);
                    // I5: restore 移到锁外 (deferred)
                } else {
                    // 已被 scanDue CAS 认领（索引条目已消耗），在途投递即为 fireNow 的效果。
                    // 不改写 executeAt/revision、不落盘 SCHEDULED@now：消除重复投递（in-flight
                    // delivery 已是 fireNow 的语义等价），避免写下一条 executeAt=now 的
                    // SCHEDULED@now 记录与原 SCHEDULED 槽自相矛盾（两者 max-revision 仲裁无意义）。
                    logger.debug("fireNow: intent {} already claimed by scanDue; in-flight delivery serves as fire-now",
                        intentId);
                }
            }
            // I5: restore 在锁外调用 (restore() 有终态检查)
            if (wasScheduled) {
                scheduler.restore(intent);
            }

            logger.info(wasScheduled
                ? "Intent fired immediately: id={}"
                : "Intent already in-flight; fireNow served by in-flight delivery: id={}", intentId);
            return true;
        } catch (RuntimeException e) {
            logger.error("Failed to fire intent: id={}", intentId, e);
            // 回滚：恢复 executeAt 和 revision，重新加入调度
            if (oldExecuteAt != null) {
                intent.setExecuteAt(oldExecuteAt);
                intent.rollbackRevision(oldRevision);
                scheduler.restore(intent);
            }
            return false;
        }
    }

    private void dispatchCallback(Intent intent, CallbackHandler.EventType eventType, Throwable error) {
        CallbackHandler handler = callbackHandler;
        if (handler == null) {
            return;
        }

        callbackExecutor.execute(() -> {
            try {
                handler.onIntentEvent(intent, eventType, error);
            } catch (Exception e) {
                logger.error("Callback handler error for intent {} event {}", intent.getIntentId(), eventType, e);
            }
        });
    }

    private static WalMode resolveWalMode(Intent intent, AckMode ackMode) {
        // 1. Explicit AckMode wins (backward compatibility)
        if (ackMode != null) {
            return switch (ackMode) {
                case ASYNC -> WalMode.ASYNC;
                case DURABLE, REPLICATED -> WalMode.DURABLE;
            };
        }
        // 2. Intent-level walMode override
        if (intent.getWalMode() != null) {
            return intent.getWalMode();
        }
        // 3. Fall back to tier default
        return PrecisionTierCatalog.defaultCatalog().walMode(intent.getPrecisionTier());
    }

    /** 包级测试入口(同包测试断言 resolveWalMode 行为)。不作为公共 API。 */
    static WalMode resolveWalModeForTest(Intent intent, AckMode ackMode) {
        return resolveWalMode(intent, ackMode);
    }

    /**
     * PHTW 写协议:locate→(inTail? tail.put : wheel.put)→locationIndex.put→(durable? awaitCommit)。
     * 返回实际分配槽位(wheel 路径为 put 捕获的真实槽;tail 路径为 locate 的占位)。
     * createIntent 与 persistIntentState 共用,避免副本漂移。
     */
    private SlotLocation persistToWheel(Intent intent, boolean durable) {
        String id = intent.getIntentId();
        SlotLocation loc = wheelStore.locate(intent.getExecuteAt());
        if (loc.inTail()) {
            tailIndex.put(intent);
        } else {
            loc = wheelStore.put(intent);                   // 捕获分配的真实槽位
        }
        if (locationIndex.get(id) != null) {
            multiSlotIntents.add(id);          // 2nd+ 写入 → 多槽（重排程/改期/取消）
        }
        locationIndex.put(id, loc);
        if (durable) {
            commitBarrier.awaitCommit();
        }
        return loc;
    }

    /**
     * 持久化 intent 当前态到 PHTW 并同步索引。状态变更操作（update/cancel/fireNow 正常路径）
     * 经此方法恒为 DURABLE；fireNow 认领分支不调用本方法——在途投递即为效果，不落盘
     * SCHEDULED@now（避免崩溃恢复将其判 overdue 丢弃）。
     *
     * <p>非终态状态变更分配新槽,旧槽残留;终态原地覆写 + 单槽回收。recovery 按 intentId 取
     * max revision 胜者,故旧槽不会引发 ghost 投递。</p>
     */
    private void persistIntentState(Intent intent, AckMode ackMode) {
        persistToWheel(intent, resolveWalMode(intent, ackMode) == WalMode.DURABLE);
    }

    /**
     * 状态变更持久化的 "put-only" 入口（供 PrecisionScheduler 在 synchronized(intent) 内调用）。
     * 仅写 PHTW + 索引（非阻塞 mmap），不 awaitCommit——持久化等待由调度器在锁外调用
     * {@link #awaitDurableCommit()} 完成，避免 VT 在 synchronized 内 park 导致 carrier pinning。
     */
    public void persistStateChangePutOnly(Intent intent) {
        persistToWheel(intent, false);   // durable=false → 跳过 awaitCommit，仅 put
    }

    /**
     * 终态原地覆写（非阻塞 mmap）：把 locationIndex 指向的最新槽覆写为终态，不追加新槽。
     * 单槽 Intent 排队待回收（pendingReclaims），落盘后由 reclaimTerminal 清空；多槽只覆写不回收。
     * 无索引或 tail 时回退追加（tail 超视界非回收路径）。失败由调用方按 I6 吞掉。
     */
    public void persistTerminalInPlace(Intent intent) {
        String id = intent.getIntentId();
        SlotLocation loc = locationIndex.get(id);
        if (loc == null || loc.inTail()) {
            persistToWheel(intent, false);     // 回退追加；awaitCommit 由调用方在锁外完成
            return;
        }
        wheelStore.overwriteSlot(loc, SlotCodec.encode(intent));
        pendingReclaims.put(id, new PendingReclaim(loc, !multiSlotIntents.contains(id)));
    }

    /**
     * 终态槽回收（须在 awaitCommit 之后调用）：先清 locationIndex（终态不索引，杜绝 freed 槽被
     * stale 别名误复用），单槽清空入 free-list 复用；多槽保留 tombstone。无论单多槽都清理
     * multiSlotIntents，避免集合泄漏。
     */
    public void reclaimTerminal(String intentId) {
        PendingReclaim pr = pendingReclaims.remove(intentId);
        try {
            locationIndex.remove(intentId);            // 先清索引，再回收槽（对齐热取消顺序）
            if (pr != null && pr.singleSlot() && !pr.loc().inTail()) {
                wheelStore.freeSlot(pr.loc());
                logger.debug("Reclaimed terminal slot {} for intent {}", pr.loc(), intentId);
            }
        } catch (Exception e) {
            logger.warn("reclaimTerminal failed for intent {}: {}", intentId, e);
        } finally {
            multiSlotIntents.remove(intentId);
        }
    }

    /** 恢复期由 WheelRecovery 注入:磁盘上幸存槽数 >1 的 Intent 视为多槽(终态保留墓碑不回收)。 */
    public void markMultiSlot(java.util.Set<String> ids) {
        multiSlotIntents.addAll(ids);
    }

    /**
     * 阻塞到覆盖最近一次 put 的 group-commit frontier 落盘。
     * 安全地在 synchronized(intent) 之外调用（此时 VT 可正常 unmount，不 pin carrier）。
     */
    public void awaitDurableCommit() {
        commitBarrier.awaitCommit();
    }

    private void ensureRunning() {
        if (!running.get()) {
            throw new IllegalStateException("Engine is not running");
        }
    }
}
