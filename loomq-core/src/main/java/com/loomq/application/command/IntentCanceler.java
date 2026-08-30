package com.loomq.application.command;

import com.loomq.application.scheduler.PrecisionScheduler;
import com.loomq.common.MetricsCollector;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.infrastructure.wheel.IntentLocationIndex;
import com.loomq.infrastructure.wheel.PromotionDaemon;
import com.loomq.infrastructure.wheel.SlotLocation;
import com.loomq.infrastructure.wheel.TailIndex;
import com.loomq.infrastructure.wheel.WheelStore;
import com.loomq.spi.CallbackHandler;
import com.loomq.store.IntentStore;
import com.loomq.tracing.IntentTraceStore;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Intent 取消路径（round 10 自 IntentCommandService 拆出）:热取消(synchronized(intent) 串行化
 * 状态迁移) + 冷取消(locationIndex 定位磁盘槽,冷锁注册表迁 WheelPersistence 共享
 * (冷改期/冷 fireNow round 13/14 同锁))。
 * 回调派发经 CallbackDispatcher 端口回调 facade(锁外派发 + I5 防御性快照时序原样)。
 * F2(round 15):热取消写终态前持 per-id 冷锁复核 revision(stale → 回滚 transitionTo + demote
 * + 委托冷取消;冷锁现为既有 Intent 的所有状态变更持久化写者的序列化点(create 主写入 W8
 * 例外不入冷锁,见 WheelPersistence 类头),权威成文见 WheelPersistence 类头)。
 */
final class IntentCanceler {

    private static final Logger logger = LoggerFactory.getLogger(IntentCanceler.class);

    private final IntentStore intentStore;
    private final PrecisionScheduler scheduler;
    private final PromotionDaemon promotionDaemon;
    private final IntentLocationIndex locationIndex;
    private final WheelStore wheelStore;
    private final TailIndex tailIndex;
    private final MetricsCollector metricsCollector;
    private final IntentTraceStore traceStore;
    private final WheelPersistence persistence;
    private final ColdHotReconciler reconciler;
    private final CallbackDispatcher callbackDispatcher;

    IntentCanceler(IntentStore intentStore, PrecisionScheduler scheduler,
                   PromotionDaemon promotionDaemon, IntentLocationIndex locationIndex,
                   WheelStore wheelStore, TailIndex tailIndex, MetricsCollector metricsCollector,
                   IntentTraceStore traceStore, WheelPersistence persistence,
                   ColdHotReconciler reconciler, CallbackDispatcher callbackDispatcher) {
        this.intentStore = intentStore;
        this.scheduler = scheduler;
        this.promotionDaemon = promotionDaemon;
        this.locationIndex = locationIndex;
        this.wheelStore = wheelStore;
        this.tailIndex = tailIndex;
        this.metricsCollector = metricsCollector;
        this.traceStore = traceStore;
        this.persistence = persistence;
        this.reconciler = reconciler;
        this.callbackDispatcher = callbackDispatcher;
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
     * <p><b>stale 委托(round 15 F2)：</b>热副本可能是在途 promote 载入的旧 revision 副本,且磁盘
     * 已被并发冷命令推进。终态提交段持 per-id 冷锁复核 revision:磁盘历史最高 revision 严格
     * 高于本副本 base revision(即磁盘已前进)才判 stale;stale 时不覆写——回滚 transitionTo +
     * demote + 自动委托 {@link #cancelCold} 在磁盘 fresh 态上取消(冷取消语义:无 CANCELLED
     * 回调派发,best-effort 契约不变);磁盘未前进则行为与既有热路径一致(原地覆写终态)。</p>
     *
     * @param intentId 待取消的 Intent ID
     * @return true 如果取消成功；false 如果 Intent 不存在或已处于终态
     */
    boolean cancelIntent(String intentId) {
        Intent intent = intentStore.findByIdInternal(intentId);
        if (intent == null) {
            // 冷意图:不在内存 store,经 locationIndex 定位磁盘槽取消
            return cancelCold(intentId);
        }

        IntentStatus oldStatus = null;
        Instant oldUpdatedAt = null;
        long oldRevision = 0;
        // C4-2: 终态已提交标记——persistTerminalInPlace 成功(mmap)后置位,此后任何失败
        // 不得回滚内存(磁盘/内存分歧)。仅终态写入前的失败才走回滚分支。
        boolean terminalPersisted = false;
        // F2(round 15): 冷锁复核检出磁盘已前进(stale)时置位——收口与 catch 守卫据此分派
        boolean stale = false;
        try {
            Intent callbackSnapshot = null;
            synchronized (intent) {
                // 在 transitionTo 之前记录原始状态，用于回滚。
                oldStatus = intent.getStatus();
                oldUpdatedAt = intent.getUpdatedAt();
                oldRevision = intent.getRevision();

                // transitionTo 单独 try-catch：仅捕获状态机校验失败，
                // 不会误吞 persistTerminalInPlace / intentStore.update 抛出的 ISE。
                try {
                    intent.transitionTo(IntentStatus.CANCELED);
                } catch (IllegalStateException e) {
                    logger.warn("Cannot cancel intent {}: {}", intentId, e.getMessage());
                    return false;
                }
                scheduler.removeFromSchedule(intent);
                // F2(round 15): 冷锁内 revision 复核 + 原子写——磁盘已前进时本副本 stale,
                // 原地覆写会以 stale 内容覆盖索引当前槽(冷写者刚写的新槽)。判据单一成文于
                // WheelPersistence.writeFreshUnderColdLock(round 16 原语收口)。
                stale = !persistence.writeFreshUnderColdLock(intentId,
                    intent.getRevision() + 1L, () -> {
                        intent.incrementRevision();
                        persistence.persistTerminalInPlace(intent);   // 原地覆写终态,不追加新槽(无索引/tail 时回退追加)
                    });
                if (!stale) {
                    terminalPersisted = true;         // 终态已提交(mmap):此后失败不回滚
                    intentStore.update(intent);
                    // 索引清理移交 reclaimTerminal 的定向移除(C2-1):终态提交后、awaitCommit 窗口内
                    // 并发重建可能已把索引指向新槽,此处无条件移除会抹掉新 incarnation 的索引。
                    traceStore.updateStatus(intentId, IntentStatus.CANCELED);
                    // I5: 锁内取快照，锁外派发
                    callbackSnapshot = intent.copy();
                }
            }

            if (stale) {
                // F2(round 15): stale 热副本 → 回滚 transitionTo(stale 分支 revision 未动,2 参足够)
                // → demote → 委托冷取消(磁盘 fresh 态重放;其成功路径自带 removeHotCopyIfPresent
                // 二次兜底)。注意:委托后走冷取消语义(无 CANCELLED 回调派发,best-effort 契约)。
                intent.rollbackVolatileState(oldStatus, oldUpdatedAt);
                reconciler.removeHotCopyIfPresent(intentId);
                logger.info("Hot cancel detected stale copy (disk advanced); delegating to cold path: id={}",
                    intentId);
                return cancelCold(intentId);
            }

            // 锁外：等 group-commit 落盘后再回收终态槽（VT 可正常 unmount）
            persistence.awaitDurableCommit();
            persistence.reclaimTerminal(intentId);

            // 在 synchronized 块外派发回调——回滚窗口已关闭，
            // callback 异常（如 RejectedExecutionException）不会触发状态回滚。
            // I5: 传递防御性快照，用户代码无法触达内核活状态
            if (callbackSnapshot != null) {
                callbackDispatcher.dispatch(callbackSnapshot, CallbackHandler.EventType.CANCELLED, null);
            }

            metricsCollector.incrementIntentsCancelled();
            logger.info("Intent cancelled: id={}", intentId);
            return true;
        } catch (RuntimeException e) {
            if (stale) {
                // F2(round 15): stale 委托路径(cancelCold)自身异常原样传播;禁止回滚/restore
                //(本副本已 demote)。
                throw e;
            }
            logger.error("Failed to cancel intent: id={}", intentId, e);
            if (terminalPersisted) {
                // C4-2: 提交后失败(awaitCommit 超时/回调 REE/指标/store 更新)——磁盘已 CANCELED
                // (或 mmap 中,由 group-commit daemon 落盘)。回滚内存会让磁盘/内存分歧:
                // 重启按 max-revision 取 CANCELED,调用方却按"失败"处理(重试/按活态重排)。
                // 提交即生效:告警 + 返回成功,不做回滚。
                logger.warn("Cancel committed; ignoring post-commit failure: id={}", intentId);
                return true;
            }
            // 回滚：清理待回收记录（不 free 槽——intent 回滚为活态，槽仍是其 tombstone）
            persistence.discardTerminalBooking(intentId);
            if (oldStatus != null && intent.getStatus() != oldStatus) {
                intent.rollbackVolatileState(oldStatus, oldUpdatedAt, oldRevision);
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
     *   <li>tail:round 13 起同持 per-id 冷锁(与冷改期 tailIndex.put 串行,防复活);
     *       round 14 F3:分支决策移入锁内(锁内重读索引),stale 快照不再决定分支——布尔门控保留作
     *       纵深防御;追加 TOMBSTONE 到 run 文件 + awaitCommit 强制落盘;以 TailIndex.remove 返回值门控 ——
     *       remove 返回 false 表示已被并发取消者移除(已追加 tombstone),直接返回 false 不重复计数。
     *       appendLock 仅串行单次 append,不串行 remove→awaitCommit→metric++ 序列,故需布尔门控</li>
     *   <li>wheel:按 intentId 串行化(per-id 冷锁,注册表现居 `WheelPersistence` 冷锁缝,与冷改期
     *       updateCold/冷 fireNow fireNowCold 共享)→ 锁内重读索引取最新槽 → 读槽解码 →
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
            return false;                                   // 不存在(无索引项)——锁外快路径
        }
        SlotLocation terminalLoc = null;
        // round 14 F3:单锁块 + 锁内重读索引分支——分支依据曾用锁外快照(loc),tail→wheel 迁移
        // 或新改期先到时按 stale 槽走错分支:tail→wheel 竞态返回假 false(可重试);tail→tail
        // 竞态(改期换了 execMs,槽位不同)以旧 terminalLoc 定向移除变 no-op,残留 stale 索引。
        // 镜像 wheel 分支既有的锁内重读语义。
        Object lock = persistence.acquireColdLock(intentId);
        synchronized (lock) {
            try {
                SlotLocation latest = locationIndex.get(intentId);
                if (latest == null) {
                    return false;                           // 并发取消/终态回收已清索引
                }
                if (latest.inTail()) {
                    // tail 路径:TailIndex.remove 返回 false 表示该 intent 已被并发取消者移除(已追加 tombstone)。
                    // 此时不可谎报成功或重复计数 —— 直接返回 false(镜像 wheel 路径第二取消者行为)。
                    if (!tailIndex.remove(intentId)) {
                        return false;
                    }
                    persistence.awaitDurableCommit();           // cancel 恒 DURABLE:确保 tombstone 落盘
                    terminalLoc = latest;
                    // 冷取消 = 追加 TOMBSTONE:run 文件中残留到 compaction。按多槽墓碑语义保护
                    // revision 种子映射(C4-1)——磁盘痕迹未清,同 id 重建须抬升到历史最高之上。
                    persistence.markColdCancelTombstone(intentId);
                } else {
                    // wheel 路径:readSlot 每次返回新解码实例,synchronized(cold) 锁的是 transient 副本,
                    // 并发取消互不阻塞 → double-write + double 计数。按 intentId 串行化:第二个取消者
                    // 串行进入后重读索引拿到先到者写入的 CANCELED 新槽(terminal)→ transitionTo
                    // 抛 ISE → 返回 false。
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
                    persistence.trackMaxRevision(cold);      // R9: 冷取消墓碑同样推进历史最高 revision
                    // 冷取消 = 新槽 + 旧槽残留 → 多槽墓碑语义:终态回收保留墓碑、种子映射保留(C4-1)
                    persistence.markColdCancelTombstone(intentId);
                    terminalLoc = canceledLoc;
                    persistence.awaitDurableCommit();       // cancel 恒 DURABLE
                }
            } finally {
                // F1(round 15): release 为空操作(锁对象不移除,防 waiter 竞态),保留配对形态。
                persistence.releaseColdLock(intentId, lock);
            }
        }

        promotionDaemon.remove(intentId);                   // 取消提升 cohort
        // C2-1: 定向移除——并发重建(磁盘终态允许同 id 重建)已把索引指向新槽时不得抹除,
        // 否则冷 intent 到点不被 promote,静默不投递直到重启。
        locationIndex.remove(intentId, terminalLoc);
        // R21: 冷取消同样反映到 trace(computeIfPresent:无 trace 则 no-op,如从未被冷 create 记录的旧数据)
        traceStore.updateStatus(intentId, IntentStatus.CANCELED);
        // P1-2 → round 14:promote 竞态 post-check 收口至 ColdHotReconciler(单一权威实现)。
        reconciler.removeHotCopyIfPresent(intentId);
        metricsCollector.incrementIntentsCancelled();
        logger.info("Cold intent cancelled: id={}", intentId);
        return true;
    }
}
