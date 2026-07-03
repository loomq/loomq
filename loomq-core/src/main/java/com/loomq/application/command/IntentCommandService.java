package com.loomq.application.command;

import com.loomq.application.scheduler.PrecisionScheduler;
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
import com.loomq.infrastructure.wheel.SlotLocation;
import com.loomq.infrastructure.wheel.TailIndex;
import com.loomq.infrastructure.wheel.WheelStore;
import com.loomq.spi.CallbackHandler;
import com.loomq.store.IdempotencyResult;
import com.loomq.store.IntentStore;
import java.time.Instant;
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

            // 3. 写入持久化分层时间轮(磁盘权威)+ 更新索引
            //    wheelStore.put 返回实际分配槽位的 SlotLocation(slotIndex>=0);
            //    locate() 仅返回 slotIndex=-1 的占位,不能直接入索引(否则冷取消/提升读槽失败)。
            SlotLocation loc = wheelStore.locate(intent.getExecuteAt());
            if (loc.inTail()) {
                tailIndex.put(intent);                  // 远期(>horizon):落 tail
            } else {
                loc = wheelStore.put(intent);           // 捕获分配的真实槽位
            }
            locationIndex.put(intent.getIntentId(), loc);

            // 4. DURABLE: 阻塞到 group-commit msync(覆盖本次写入的 force 完成)
            if (effectiveMode == WalMode.DURABLE) {
                commitBarrier.awaitCommit();
            }

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
            logger.error("Failed to create intent: id={}", intent.getIntentId(), e);
            // 回滚内存态:从调度器/store/索引/cohort 移除。
            // 注意:已落盘的 wheel/tail 写入不回滚——若写入已成功,崩溃恢复会重建该 intent,
            // 这与 DURABLE 语义一致(写入成功即持久)。
            try {
                scheduler.removeFromSchedule(intent);
            } catch (Exception ignored) {}
            try {
                intentStore.delete(intent.getIntentId());
            } catch (Exception rollbackEx) {
                logger.error("Rollback failed for intent {}: store.delete",
                    intent.getIntentId(), rollbackEx);
            }
            try {
                promotionDaemon.remove(intent.getIntentId());
            } catch (Exception ignored) {}
            try {
                locationIndex.remove(intent.getIntentId());
            } catch (Exception ignored) {}
            throw new RuntimeException("Failed to create intent", e);
        }
    }

    public List<Long> createIntents(List<Intent> intents, AckMode ackMode) {
        ensureRunning();
        return intents.stream()
            .map(intent -> createIntent(intent, ackMode))
            .toList();
    }

    public Optional<Intent> updateIntent(String intentId, Consumer<Intent> updater) {
        return updateIntent(intentId, updater, null);
    }

    public Optional<Intent> updateIntent(String intentId, Consumer<Intent> updater, Instant newExecuteAt) {
        ensureRunning();

        Intent intent = intentStore.findByIdInternal(intentId);
        if (intent == null) {
            return Optional.empty();
        }

        try {
            synchronized (intent) {
                Instant oldExecuteAt = intent.getExecuteAt();
                boolean reschedule = newExecuteAt != null && !newExecuteAt.equals(oldExecuteAt);

                if (reschedule) {
                    scheduler.removeFromSchedule(intent);
                }

                updater.accept(intent);

                // 安全网：updater 可能直接通过 intent.setExecuteAt() 修改了执行时间，
                // 此时 newExecuteAt 为 null 导致 reschedule 初始为 false，需要补检。
                if (!reschedule) {
                    Instant actualExecuteAt = intent.getExecuteAt();
                    if (actualExecuteAt != null && !actualExecuteAt.equals(oldExecuteAt)) {
                        reschedule = true;
                        // 必须在 updater 已修改 executeAt 之后、重新调度之前，
                        // 用旧的 executeAt 清理索引（removeFromSchedule 内部用的是当前 executeAt）
                        scheduler.removeFromSchedule(intent, oldExecuteAt);
                    }
                }

                if (newExecuteAt != null) {
                    intent.setExecuteAt(newExecuteAt);
                }

                intent.incrementRevision();
                persistIntentState(intent, AckMode.DURABLE);
                intentStore.update(intent);

                if (reschedule) {
                    if (intent.getStatus() == IntentStatus.DUE) {
                        scheduler.restore(intent);
                    } else {
                        scheduler.schedule(intent);
                    }
                }
            }
            return Optional.of(intent);
        } catch (RuntimeException e) {
            logger.error("Failed to update intent: id={}", intentId, e);
            throw e;
        }
    }

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
                persistIntentState(intent, AckMode.DURABLE);
                intentStore.update(intent);
            }

            // 在 synchronized 块外派发回调——回滚窗口已关闭，
            // callback 异常（如 RejectedExecutionException）不会触发状态回滚。
            dispatchCallback(intent, CallbackHandler.EventType.CANCELLED, null);

            metricsCollector.incrementIntentsCancelled();
            logger.info("Intent cancelled: id={}", intentId);
            return true;
        } catch (RuntimeException e) {
            logger.error("Failed to cancel intent: id={}", intentId, e);
            // 回滚：transitionTo 已成功但持久化失败，恢复原状态并重新加入调度
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
     *       transitionTo(CANCELED) + incrementRevision → 写新槽(append-only,recovery 按 max revision
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
                    // (WheelStore append-only,旧槽不被覆写)。重读得到先到者更新后的 CANCELED 槽才能让
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
                    // WheelStore 为 append-only:此处写入新槽(revision 更高),recovery 按 intentId
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
        try {
            synchronized (intent) {
                oldExecuteAt = intent.getExecuteAt();
                oldRevision = intent.getRevision();
                scheduler.removeFromSchedule(intent);
                intent.setExecuteAt(Instant.now());
                intent.incrementRevision();
                persistIntentState(intent, AckMode.DURABLE);
                intentStore.update(intent);
                scheduler.restore(intent);
            }

            logger.info("Intent fired immediately: id={}", intentId);
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
            // H1: BATCH_DEFERRED 在 group-commit 架构下等价 ASYNC(周期 fsync 已覆盖)
            return intent.getWalMode() == WalMode.BATCH_DEFERRED ? WalMode.ASYNC : intent.getWalMode();
        }
        // 3. Fall back to tier default; tier 默认若为 BATCH_DEFERRED 亦归一为 ASYNC
        WalMode tierDefault = PrecisionTierCatalog.defaultCatalog().walMode(intent.getPrecisionTier());
        return tierDefault == WalMode.BATCH_DEFERRED ? WalMode.ASYNC : tierDefault;
    }

    /** 包级测试入口(同包测试断言 resolveWalMode 行为)。不作为公共 API。 */
    static WalMode resolveWalModeForTest(Intent intent, AckMode ackMode) {
        return resolveWalMode(intent, ackMode);
    }

    /**
     * 持久化 intent 当前态到 PHTW 并同步索引。状态变更操作(update/cancel/fireNow)恒为 DURABLE。
     *
     * <p>WheelStore 为 append-only:每次写入分配新槽,旧槽残留。recovery 按 intentId 取
     * max revision 胜者,故旧槽不会引发 ghost 投递。</p>
     */
    private void persistIntentState(Intent intent, AckMode ackMode) {
        SlotLocation loc = wheelStore.locate(intent.getExecuteAt());
        if (loc.inTail()) {
            tailIndex.put(intent);
        } else {
            loc = wheelStore.put(intent);                   // 捕获分配的真实槽位
        }
        locationIndex.put(intent.getIntentId(), loc);       // 同步索引
        WalMode effectiveMode = resolveWalMode(intent, ackMode);
        if (effectiveMode == WalMode.DURABLE) {
            commitBarrier.awaitCommit();
        }
    }

    private void ensureRunning() {
        if (!running.get()) {
            throw new IllegalStateException("Engine is not running");
        }
    }
}
