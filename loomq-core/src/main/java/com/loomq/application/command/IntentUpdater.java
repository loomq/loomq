package com.loomq.application.command;

import com.loomq.application.scheduler.PrecisionScheduler;
import com.loomq.common.IntentValidator;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.domain.intent.PrecisionTierCatalog;
import com.loomq.infrastructure.wheel.IntentLocationIndex;
import com.loomq.infrastructure.wheel.PromotionDaemon;
import com.loomq.infrastructure.wheel.SlotLocation;
import com.loomq.store.IntentStore;
import java.time.Instant;
import java.util.Optional;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Intent 更新/立即触发路径（round 10 自 IntentCommandService 拆出）。
 * updateIntent 与 fireNow 共享三条纪律:removeFromSchedule 布尔返回 = scanDue CAS 认领判别;
 * 锁内 put / 锁外 awaitCommit / 锁外 schedule·restore(VT pinning 规避);
 * 提交后失败不回滚(C4-2/C4-3 同款,磁盘/索引为权威)。
 * 冷路径(round 13/14):updateIntent 对冷 Intent 走 updateCold,fireNow 对冷 Intent 走 fireNowCold
 * (per-id 冷锁 + persistToWheel 写协议 + cohort 安全网;冷 fireNow 把 executeAt 改写为 now 后
 * 立即热载投递,不跑 IntentValidator——镜像热路径)。
 * F2(round 15):热路径写槽前持 per-id 冷锁复核 revision(stale → 还原 + demote + 委托冷路径
 * fresh 重放);投递路径守卫见 IntentCommandService.persistStateChangePutOnly(F6 起为单一守卫门)。
 * 非 final(round 14):测试桩子类覆写 routeColdAfterPersist 注入提交后路由失败。
 */
class IntentUpdater {

    private static final Logger logger = LoggerFactory.getLogger(IntentUpdater.class);

    /** updateIntent 认领分支跳过重排的告警模板（两处分支共用，避免字符串漂移）。 */
    private static final String CLAIMED_SKIP_WARN =
        "updateIntent: intent {} already claimed by scanDue; update will be persisted but "
            + "in-flight delivery may carry pre-update content (no reschedule)";

    private final IntentStore intentStore;
    private final PrecisionScheduler scheduler;
    private final IntentLocationIndex locationIndex;
    private final PrecisionTierCatalog precisionTierCatalog;
    private final WheelPersistence persistence;
    private final PromotionDaemon promotionDaemon;
    private final ColdHotReconciler reconciler;
    private final long hotBoundaryMs;

    IntentUpdater(IntentStore intentStore, PrecisionScheduler scheduler,
                  IntentLocationIndex locationIndex, PrecisionTierCatalog precisionTierCatalog,
                  WheelPersistence persistence, PromotionDaemon promotionDaemon,
                  ColdHotReconciler reconciler, long hotBoundaryMs) {
        this.intentStore = intentStore;
        this.scheduler = scheduler;
        this.locationIndex = locationIndex;
        this.precisionTierCatalog = precisionTierCatalog;
        this.persistence = persistence;
        this.promotionDaemon = promotionDaemon;
        this.reconciler = reconciler;
        this.hotBoundaryMs = hotBoundaryMs;
    }

    Optional<Intent> updateIntent(String intentId, Consumer<Intent> updater) {
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
     * <p><b>提交后失败语义：</b>一旦新状态已写入 PHTW mmap 与 locationIndex（即
     * {@code persistStateChangePutOnly} 成功），后续内存镜像更新或 awaitCommit 失败
     * <b>不会回滚</b>，本方法按成功返回；磁盘/索引为权威，重启后按新 revision 恢复。</p>
     *
     * <p><b>冷路径(round 13)：</b>Intent 不在内存热窗口时不走上述热路径,而是委托
     * {@link #updateCold}:持 per-id 冷锁(注册表在 {@link WheelPersistence},与冷取消共享)对磁盘
     * 槽位串行读-改-写,写路径复用 {@link WheelPersistence#persistToWheel} 写协议(索引先行/多槽
     * 墓碑/DURABLE),锁外注册 promotion cohort 安全网并按需热载;成功返回更新后副本,校验失败
     * 抛 IAE 同热路径(副本未持久化,磁盘 untouched)。完整语义见 {@link #updateCold} javadoc。</p>
     *
     * <p><b>stale 委托(round 15 F2)：</b>热副本可能是在途 promote 载入的旧 revision 副本,且磁盘
     * 已被并发冷命令推进。提交段持 per-id 冷锁复核 revision:磁盘历史最高 revision 严格高于
     * 本副本 base revision(即磁盘已前进)才判 stale;stale 时不落盘——还原调度字段 + demote +
     * 自动委托 {@link #updateCold} 在磁盘 fresh 态上重放同一 updater,返回值基于 fresh 态
     * (成功即更新生效);委托路径自身异常按其契约传播,不复活已 demote 的 stale 副本。
     * 磁盘未前进则行为与既有热路径一致(直写新槽)。</p>
     *
     * @param intentId     待更新 Intent ID
     * @param updater      变更消费者（热路径在 synchronized(intent) 内执行；冷路径在 per-id
     *                     冷锁内作用于 transient 解码副本，无 synchronized(intent) 监视器）
     * @param newExecuteAt 新的执行时间；null 表示不改期
     * @return 更新后的 Intent；intent 不存在时返回 empty；intent 已处终态时返回未修改的
     *         intent（no-op，不持久化）;冷 Intent(不在内存热窗口)经磁盘槽位读-改-写同样生效
     *         (见 {@link #updateCold});冷槽不可读/并发取消已生效时返回 empty
     */
    Optional<Intent> updateIntent(String intentId, Consumer<Intent> updater, Instant newExecuteAt) {
        Intent intent = intentStore.findByIdInternal(intentId);
        if (intent == null) {
            // 冷意图:不在内存 store,经 locationIndex 定位磁盘槽更新/改期(round 13)。
            return updateCold(intentId, updater, newExecuteAt);
        }

        boolean reschedule = false;
        boolean persisted = false;
        boolean removedForReschedule = false;
        // F2(round 15): 冷锁复核检出磁盘已前进(stale)时置位——收口与 catch 守卫据此分派
        boolean stale = false;
        // C4-5: 提升到 catch 作用域——updater 先变异后抛时,回滚须还原这些"更新前"值
        Instant oldExecuteAt = null;
        IntentStatus statusBeforeUpdater = null;
        Instant updatedAtBeforeUpdater = null;
        PrecisionTier tierBeforeUpdater = null;
        try {
            synchronized (intent) {
                if (intent.getStatus().isTerminal()) {
                    logger.warn("Cannot update intent {} in terminal state {}; no-op", intentId, intent.getStatus());
                    // C4-4: I5 快照隔离——返回副本,调用方不得持有/变异内核活状态
                    return Optional.of(intent.copy());
                }
                oldExecuteAt = intent.getExecuteAt();
                reschedule = newExecuteAt != null && !newExecuteAt.equals(oldExecuteAt);

                if (reschedule) {
                    // P1-5: 只对可调度状态(SCHEDULED/DUE)执行 removeFromSchedule。
                    // DISPATCHING/DELIVERED 在途投递,removeFromSchedule 已执行而 schedule/restore
                    // 会拒收(只认 CREATED/SCHEDULED)→ Intent 从调度结构消失直到重启。
                    removedForReschedule =
                        tryDetachForReschedule(intent, null,
                            "Cannot reschedule intent {} in {} state; keeping original schedule")
                            == DetachResult.DETACHED;
                    if (!removedForReschedule) {
                        reschedule = false;
                    }
                }

                // R11: 捕获 updater 前的状态，用于终态转换拒绝时回滚（终态经 append 持久化
                // 会绕过 removeFromSchedule/locationIndex.remove/reclaimTerminal 清理）。
                statusBeforeUpdater = intent.getStatus();
                updatedAtBeforeUpdater = intent.getUpdatedAt();
                tierBeforeUpdater = intent.getPrecisionTier();

                updater.accept(intent);

                // R11: 生命周期变更(取消/过期/死信)须走 cancelIntent/finalize/handleExpired 专用
                // 路径——updater 直接 transitionTo(终态) 会绕过调度结构摘除、locationIndex 清理、
                // 终态槽回收，遗留 locationIndex/intentExpiryIndex/multiSlot 泄漏直到重启。
                // 拒绝并回滚状态，指引调用方使用 cancelIntent。
                if (intent.getStatus().isTerminal()) {
                    intent.rollbackVolatileState(statusBeforeUpdater, updatedAtBeforeUpdater);
                    throw new IllegalArgumentException(
                        "updater must not transition intent " + intentId + " to terminal state "
                            + intent.getStatus() + "; use cancelIntent");
                }

                // R16: updater 置空 executeAt 会污染调度器——persistToWheel.locate(null) NPE,
                // 且 scanDue 的 intent.getExecuteAt().isAfter(now) 持续 NPE 使该档扫描永久卡死。
                // 拒绝并回滚 executeAt/updatedAt,维持内核不变量(executeAt 恒非空)。
                // 注意顺序:setExecuteAt 会重写 updatedAt,须先还原 executeAt 再 rollbackVolatileState
                // (后者同时还原 status 与 updatedAt),避免 updatedAt 残留变异时间戳。
                if (intent.getExecuteAt() == null) {
                    intent.setExecuteAt(oldExecuteAt);
                    intent.rollbackVolatileState(statusBeforeUpdater, updatedAtBeforeUpdater);
                    throw new IllegalArgumentException(
                        "updater must not set executeAt to null for intent " + intentId);
                }

                // R21: updater 只允许停留在 SCHEDULED/DUE——DUE→DISPATCHING(合法迁移,
                // R11 只拦终态、R16 只拦 executeAt=null)会把"无投递在途"的 DISPATCHING
                // 持久化:内存态永久卡死;finalize 的 DUE 起步守卫从 DISPATCHING 转 DUE
                // 抛 ISE 被 runFinalizeTask 吞 → ACK 不落盘/onDelivered 不通知;重启后
                // recovery 把非终态 DISPATCHING 当活 intent 重复投递。拒绝并回滚。
                if (intent.getStatus() != IntentStatus.SCHEDULED && intent.getStatus() != IntentStatus.DUE) {
                    // 先捕获违规状态再回滚——rollbackVolatileState 会还原 status,消息须点名违规值
                    IntentStatus offender = intent.getStatus();
                    intent.rollbackVolatileState(statusBeforeUpdater, updatedAtBeforeUpdater);
                    throw new IllegalArgumentException(
                        "updater must not move intent " + intentId + " to " + offender
                            + "; only SCHEDULED/DUE are valid post-update states");
                }

                // 安全网：updater 可能直接通过 intent.setExecuteAt() 修改了执行时间，
                // 此时 newExecuteAt 为 null 导致 reschedule 初始为 false，需要补检。
                if (!reschedule) {
                    Instant actualExecuteAt = intent.getExecuteAt();
                    if (actualExecuteAt != null && !actualExecuteAt.equals(oldExecuteAt)) {
                        // 必须在 updater 已修改 executeAt 之后、重新调度之前,
                        // 用旧的 executeAt 清理索引(removeFromSchedule 内部用的是当前 executeAt)
                        if (tryDetachForReschedule(intent, oldExecuteAt,
                                "Cannot reschedule intent {} in {} state after updater; keeping original")
                            == DetachResult.DETACHED) {
                            reschedule = true;
                            removedForReschedule = true;
                        }
                    }
                }

                if (newExecuteAt != null) {
                    intent.setExecuteAt(newExecuteAt);
                }

                // R22: updateIntent 同样执行入口校验——updater/newExecuteAt 可能把
                // deadline/redelivery/expiredAction 改成非法值，创建路径已拦截，更新路径不能漏。
                IntentValidator.validate(intent);
                // 归一化放在校验之后，避免校验失败时仍然修改调用方传入的 Intent。
                IntentCommandService.normalizePrecisionTier(precisionTierCatalog, intent);

                // F2(round 15): per-id 冷锁内 revision 复核 + 原子写——磁盘历史最高 revision
                // 严格高于本副本 base revision 说明本副本 stale(在途 promote 载入的旧副本被
                // 并发冷命令推进过)。直写的双写论证(round 15 终审 g):diskMax==base+1 时带
                // stale 内容直写与磁盘权威槽同 revision 双写,recovery 平票仲裁不确定(冷更新可
                // 静默丢失);diskMax>base+1 时直写为更低 revision 的 stale 槽并偷指索引(进程内
                // 读到 stale)。两情形守卫均覆盖。复核+increment+persist 必须同一冷锁临界区
                // (检查-写原子);锁序 synchronized(intent) → 冷锁,冷临界区不取 intent 监视器
                // (无死锁环);awaitCommit 仍在锁外。
                Object coldLock = persistence.acquireColdLock(intentId);
                try {
                    synchronized (coldLock) {
                        Long diskMax = persistence.maxRevisionOf(intentId);
                        stale = diskMax != null && diskMax > intent.getRevision();
                        if (!stale) {
                            intent.incrementRevision();
                            // 锁内仅非阻塞 put;DURABLE 等待移到锁外（VT 可正常 unmount，不 pin carrier）
                            persistence.persistStateChangePutOnly(intent);
                        }
                    }
                } finally {
                    persistence.releaseColdLock(intentId, coldLock);
                }
                if (!stale) {
                    persisted = true;
                    intentStore.update(intent);
                }
                // I5: schedule/restore 移到锁外 (deferred)
                // -- schedule() 有自己的 synchronized + 终态检查
            }
            if (stale) {
                // F2(round 15): stale 热副本 → 还原调度字段 + demote + 委托冷路径 fresh 重放。
                // updateCold 在冷锁内重读磁盘最新态并重放同一 updater(冷路径既有契约)。
                // 归因(round 15 终审 c):stale 副本的主收口是本分支显式的
                // reconciler.removeHotCopyIfPresent;routeColdAfterPersist 的 removeHotCopyIfStale
                // 是竞态兜底(demote + 窗口内重热载)。内容变异不可回滚(C4-5),副本随即被 demote 弃用。
                restoreSchedulingFields(intent, oldExecuteAt, statusBeforeUpdater, updatedAtBeforeUpdater,
                    tierBeforeUpdater);
                reconciler.removeHotCopyIfPresent(intentId);
                logger.info("Hot update detected stale copy (disk advanced); delegating to cold path: id={}",
                    intentId);
                return updateCold(intentId, updater, newExecuteAt);
            }
            // DURABLE 等待在锁外完成（同调度器模式：锁内 put、锁外 awaitCommit）
            if (persisted) {
                persistence.awaitDurableCommit();
            }
            // I5: schedule/restore 在锁外调用
            if (reschedule) {
                if (intent.getStatus() == IntentStatus.DUE) {
                    scheduler.restore(intent);
                } else {
                    scheduler.schedule(intent);
                }
            }
            // C4-4: I5 快照隔离——返回副本,调用方不得持有/变异内核活状态(与 findById 同纪律)
            return Optional.of(intent.copy());
        } catch (RuntimeException e) {
            if (stale) {
                // F2(round 15): stale 委托路径(updateCold)自身异常按其契约传播;此处禁止
                // restore/reschedule——本副本已 demote,复活会留下与磁盘权威态分叉的 ghost。
                throw e;
            }
            if (persisted) {
                // C4-2/C4-3 同款提交后保护：persistStateChangePutOnly 已把新状态写入 mmap +
                // 索引，之后的 store.update/awaitCommit 失败不能回滚，否则内存旧、磁盘新，
                // 重启后按 max-revision 恢复出新调度，调用方却以为失败。
                logger.warn("Update committed; ignoring post-commit failure: id={}", intentId, e);
                // 提交后失败时，内存镜像可能未更新（store.update 抛错）。用 save 做 best-effort
                // 补偿，尽量让 ConcurrentIntentStore 的 statusCounts/pendingCount 跟上已提交状态。
                try {
                    intentStore.save(intent);
                } catch (Exception saveEx) {
                    logger.warn("Failed to refresh in-memory store after committed update: id={}", intentId, saveEx);
                }
                // 补做锁外调度，使内存与磁盘/索引一致（与正常成功路径同一收口）。
                if (reschedule) {
                    try {
                        if (intent.getStatus() == IntentStatus.DUE) {
                            scheduler.restore(intent);
                        } else if (intent.getStatus() == IntentStatus.SCHEDULED) {
                            scheduler.schedule(intent);
                        }
                    } catch (Exception re) {
                        logger.error(
                            "Failed to schedule committed update for intent {}; it will be delivered after restart",
                            intentId, re);
                    }
                }
                return Optional.of(intent.copy());
            }
            logger.error("Failed to update intent: id={}", intentId, e);
            restoreSchedulingFields(intent, oldExecuteAt, statusBeforeUpdater, updatedAtBeforeUpdater,
                tierBeforeUpdater);
            // 更新失败但调度结构已摘除（updater 抛异常或持久化失败）→ 按原 executeAt
            // 重新调度——否则 intent 在 store 中为 SCHEDULED/DUE 却不在任何
            // bucket/cohort，静默永不投递直到重启恢复（投递延迟丢失）。
            // 镜像正常 reschedule 分支（见下方）：DUE 走 restore()、SCHEDULED 走 schedule()。
            if (removedForReschedule) {
                try {
                    if (intent.getStatus() == IntentStatus.DUE) {
                        scheduler.restore(intent);
                    } else if (intent.getStatus() == IntentStatus.SCHEDULED) {
                        scheduler.schedule(intent);
                    }
                } catch (Exception re) {
                    logger.error(
                        "Failed to re-schedule intent {} after update failure; it will not be delivered until restart",
                        intentId, re);
                }
            }
            throw e;
        }
    }

    /** 重排程摘除结果(updateIntent 两分支共用)。 */
    private enum DetachResult {
        /** 已从调度结构摘除,可安全重排。 */
        DETACHED,
        /** 已被 scanDue CAS 认领,在途投递正在执行——变更仍持久化但不重排。 */
        CLAIMED,
        /** 状态不可调度(DISPATCHING/DELIVERED 等),保持原调度。 */
        UNSUPPORTED
    }

    /**
     * 提交前失败/stale 的调度字段还原(executeAt/status/updatedAt/precisionTier)。
     * C4-5: updater 可能已变异这些字段后才失败——按"当前值"重排会把变异半应用
     * (与"失败即还原"语义不符,见 BugUpdateUpdaterFailureTest)。
     * 非调度字段(内容)的变异不可回滚(无更新前快照),属用户 updater 契约边界。
     * 顺序:setExecuteAt/setPrecisionTier 会重写 updatedAt,须先执行,再由
     * rollbackVolatileState 统一还原 status 与 updatedAt。
     * F2(round 15):catch 回滚分支与 stale 委托分支共用本方法。
     */
    private void restoreSchedulingFields(Intent intent, Instant oldExecuteAt, IntentStatus statusBeforeUpdater,
                                         Instant updatedAtBeforeUpdater, PrecisionTier tierBeforeUpdater) {
        if (oldExecuteAt != null) {
            intent.setExecuteAt(oldExecuteAt);
        }
        if (statusBeforeUpdater != null) {
            // R28: precisionTier 也是调度字段(决定 cohort/桶路由),updater 污染后必须
            // 还原,否则活对象残留 null/错误档,与磁盘(recovery 恢复值)短暂不一致。
            intent.setPrecisionTier(tierBeforeUpdater);
            intent.rollbackVolatileState(statusBeforeUpdater, updatedAtBeforeUpdater);
        }
    }

    /**
     * 重排程摘除收口:状态白名单校验 + removeFromSchedule(claimed 判别)+ 告警。
     * 收敛 updateIntent 两处孪生分支(状态白名单→摘除→CLAIMED_SKIP_WARN 逻辑曾各自漂移)。
     */
    private DetachResult tryDetachForReschedule(Intent intent, Instant oldExecuteAt, String unsupportedWarn) {
        IntentStatus st = intent.getStatus();
        if (st != IntentStatus.SCHEDULED && st != IntentStatus.DUE) {
            logger.warn(unsupportedWarn, intent.getIntentId(), st);
            return DetachResult.UNSUPPORTED;
        }
        boolean wasScheduled = oldExecuteAt != null
            ? scheduler.removeFromSchedule(intent, oldExecuteAt)
            : scheduler.removeFromSchedule(intent);
        if (!wasScheduled) {
            // 已被 scanDue CAS 认领，在途投递正在执行。
            // updater 的变更随后仍会持久化，但不重排--在途投递可能携带
            // 旧内容，更新仅在重试/崩溃恢复路径确定生效（见方法 javadoc）。
            logger.warn(CLAIMED_SKIP_WARN, intent.getIntentId());
            return DetachResult.CLAIMED;
        }
        return DetachResult.DETACHED;
    }

    /**
     * 立即触发 Intent(热路径):removeFromSchedule 布尔返回 = scanDue CAS 认领判别——已认领
     * 则在途投递即为 fireNow 语义等价(不改写 executeAt/不落盘,防重复投递);认领成功则
     * executeAt=now + revision+1 + DURABLE 落盘。提交后失败不回滚(C4-3:restore + 返回 true,
     * 磁盘 @now 槽为权威);提交前失败回滚 executeAt/revision 并 restore + 返回 false。
     * F2(round 15):提交段持 per-id 冷锁复核 revision——磁盘已前进(stale 热副本)→ demote +
     * 委托 {@link #fireNowCold} 从磁盘 fresh 态重放(executeAt=now + 立即热载,布尔契约同冷路径);
     * 磁盘未前进则行为不变。stale 委托路径的异常按 fireNowCold 契约处理:fireNowCold 自身失败
     * 已内含为返回 false,逸出异常按原样透传不额外上抛包装,亦不触发本方法回滚。
     * 冷 Intent 走 {@link #fireNowCold}(round 14)。
     */
    boolean fireNow(String intentId) {
        Intent intent = intentStore.findByIdInternal(intentId);
        if (intent == null) {
            // 冷意图:不在内存 store 但索引在——走冷 fireNow(round 14):磁盘槽 executeAt=now
            // + 立即热载,语义镜像热路径。不存在(索引也空)仍返回 false。
            if (locationIndex.get(intentId) == null) {
                return false;
            }
            return fireNowCold(intentId);
        }

        if (intent.getStatus().isTerminal()) {
            logger.warn("Cannot fire intent in terminal state: id={}, status={}",
                intentId, intent.getStatus());
            return false;
        }

        Instant oldExecuteAt = null;
        long oldRevision = 0;
        boolean wasScheduled = false;
        boolean persisted = false;
        // F2(round 15): 冷锁复核检出磁盘已前进(stale)时置位——收口与 catch 守卫据此分派
        boolean stale = false;
        try {
            synchronized (intent) {
                oldExecuteAt = intent.getExecuteAt();
                oldRevision = intent.getRevision();
                wasScheduled = scheduler.removeFromSchedule(intent);
                if (wasScheduled) {
                    // F2(round 15): 冷锁内 revision 复核 + 原子写(镜像 updateIntent 守卫)。
                    // 复核+setExecuteAt+increment+persist 同一冷锁临界区;锁序 intent 监视器 → 冷锁。
                    Object coldLock = persistence.acquireColdLock(intentId);
                    try {
                        synchronized (coldLock) {
                            Long diskMax = persistence.maxRevisionOf(intentId);
                            stale = diskMax != null && diskMax > intent.getRevision();
                            if (!stale) {
                                intent.setExecuteAt(Instant.now());
                                intent.incrementRevision();
                                // 锁内仅非阻塞 put；DURABLE 等待移到锁外（VT 可正常 unmount）
                                persistence.persistStateChangePutOnly(intent);
                            }
                        }
                    } finally {
                        persistence.releaseColdLock(intentId, coldLock);
                    }
                    if (!stale) {
                        persisted = true;
                        intentStore.update(intent);
                    }
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
            if (stale) {
                // F2(round 15): 磁盘已前进——本副本 stale。CAS 认领已消耗(schedule 条目已摘除):
                // demote 本副本,委托冷 fireNow 从磁盘 fresh 态重放(executeAt=now + 立即热载),
                // 杜绝 stale 内容投递/同 revision 双写。fireNowCold 复用 routeColdAfterPersist
                // (upsert fresh + removeHotCopyIfStale 收口)。stale 分支无字段变异(setExecuteAt/
                // increment 在 !stale 内),无需回滚。
                reconciler.removeHotCopyIfPresent(intentId);
                logger.info("Hot fireNow detected stale copy (disk advanced); delegating to cold path: id={}",
                    intentId);
                return fireNowCold(intentId);
            }
            // DURABLE 等待在锁外完成（同调度器模式：锁内 put、锁外 awaitCommit）
            if (persisted) {
                persistence.awaitDurableCommit();
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
            if (stale) {
                // F2(round 15): fireNowCold 自身失败已内含为返回 false;异常传播到此处属预期外,
                // 按原样透传(不额外包装)并禁止 restore——本副本已 demote,复活会留下 ghost。
                throw e;
            }
            logger.error("Failed to fire intent: id={}", intentId, e);
            if (persisted) {
                // C4-3: 提交后失败(SCHEDULED@now 已入 mmap,索引已指向新槽)——回滚 executeAt/
                // revision 会造成"内存旧时刻 vs 磁盘 now"分歧:重启 recovery 见 @now 槽已过期,
                // 按 overdue 终态化,未来投递静默丢失。磁盘/索引/内存已一致(@now):直接 restore
                // 让其在途按 now 投递,不再回滚。
                try {
                    if (wasScheduled) {
                        scheduler.restore(intent);
                    }
                } catch (Exception re) {
                    logger.error("Failed to restore intent {} after committed fireNow", intentId, re);
                }
                return true;
            }
            // 仅持久化前失败：恢复 executeAt 和 revision，重新加入调度
            if (oldExecuteAt != null) {
                intent.setExecuteAt(oldExecuteAt);
                intent.rollbackRevision(oldRevision);
                scheduler.restore(intent);
            }
            return false;
        }
    }

    /**
     * 冷 fireNow(round 14):Intent 不在内存 store 时,经磁盘槽位把 executeAt 改写为 now
     * 并 DURABLE 落新槽,随后锁外路由立即热载调度(下个扫描 tick 投递)。
     *
     * <p><b>语义镜像热 fireNow</b>:不跑 IntentValidator、提交后失败不回滚(C4-3 同款,
     * 返回 true)、布尔契约(不存在/索引已清/槽不可读/终态/非 SCHEDULED/提交前持久化失败
     * → false)。与热路径的差异仅两点:冷域无 CAS 认领分支(无在途投递可充当 fireNow);写槽前
     * normalize 档位(executeAt 变更后槽必须自洽——热路径由调度器按内存对象重排,无此需求)。
     * deadline 已过时的 fireNow 行为两侧一致:照常触发,由结算/过期逻辑按既有语义处理。</p>
     *
     * <p><b>锁内段</b>:镜像 updateCold——per-id 冷锁 → 锁内重读索引 → readColdSlot →
     * terminal/非 SCHEDULED 拒绝 → setExecuteAt(now) + normalize → incrementRevision →
     * persistToWheel → awaitCommit 窄 try/catch 守卫(C4-2:提交后失败仅 warn,无回滚)。
     * tail Intent 落 wheel 新槽时由 persistToWheel 的 tail→wheel 迁移免费清旧 tail 记录。</p>
     */
    private boolean fireNowCold(String intentId) {
        SlotLocation newLoc = null;
        Intent cold = null;
        Object lock = persistence.acquireColdLock(intentId);
        synchronized (lock) {
            try {
                SlotLocation latest = locationIndex.get(intentId);
                if (latest == null) {
                    return false;                        // 并发取消已清索引
                }
                Intent current = persistence.readColdSlot(intentId, latest);
                if (current == null) {
                    logger.warn("fireNowCold: slot unreadable for cold intent {}, loc={}", intentId, latest);
                    return false;
                }
                if (current.getStatus().isTerminal()) {
                    logger.warn("Cannot fire intent in terminal state: id={}, status={}",
                        intentId, current.getStatus());
                    return false;
                }
                if (current.getStatus() != IntentStatus.SCHEDULED) {
                    // 冷域只有 SCHEDULED(DUE 是热扫描态,不会落盘);见到即数据异常,不谎报触发。
                    logger.warn("Cannot fire cold intent in non-SCHEDULED state: id={}, status={}",
                        intentId, current.getStatus());
                    return false;
                }
                current.setExecuteAt(Instant.now());
                // 冷写槽必须自洽:executeAt 变更后档位随 delay 重算(热路径由调度器按内存对象重排)。
                IntentCommandService.normalizePrecisionTier(precisionTierCatalog, current);
                current.incrementRevision();
                try {
                    newLoc = persistence.persistToWheel(current, false);
                } catch (RuntimeException e) {
                    // 提交前持久化失败(槽分配/编码/写 put 抛出):磁盘 untouched(副本 transient,
                    // 无需回滚)——镜像热 fireNow 提交前分支(回滚 + 返回 false),不向上抛。
                    logger.warn("Failed to persist cold fireNow: id={}", intentId, e);
                    return false;
                }
                try {
                    persistence.awaitDurableCommit();
                } catch (RuntimeException e) {
                    // C4-2/C4-3 同款:提交后失败不回滚(镜像热 fireNow 提交后分支)。
                    logger.warn("Cold fireNow committed; ignoring post-commit awaitCommit failure: id={}",
                        intentId, e);
                }
                cold = current;
            } finally {
                persistence.releaseColdLock(intentId, lock);
            }
        }
        try {
            routeColdAfterPersist(intentId, cold, newLoc);   // executeAt=now 恒在热窗口:upsert+schedule
        } catch (RuntimeException e) {
            logger.warn("Cold fireNow committed; ignoring post-commit routing failure: id={}", intentId, e);
        }
        logger.info("Cold intent fired immediately: id={}", intentId);
        return true;
    }

    /**
     * 冷改期/冷更新(round 13):Intent 不在内存 store(>hotBoundaryMs 未提升或 >day 视界落 tail),
     * 经 locationIndex 定位磁盘槽,持 per-id 冷锁读-改-写新槽。写路径全走
     * {@link WheelPersistence#persistToWheel}(tail→wheel 迁移清旧 tail 记录 / multiSlot 墓碑 /
     * 索引先行 / revision 种子全部免费),不新增持久化协议。
     *
     * <p><b>锁内段</b>:锁内重读索引(镜像 cancelCold:先到者可能已改槽/取消)→ 读槽防御解码 →
     * updater 变异 transient 副本 → 校验镜像热路径(R16/R21/IntentValidator/normalize)→
     * incrementRevision → persistToWheel(索引先于 awaitCommit,杜绝在途 promote 复活 ghost)→
     * awaitCommit(per-id 锁非 synchronized(intent),VT 可正常 unmount,cancelCold wheel 路径同款)。
     * persistToWheel 成功前任何失败磁盘 untouched——副本未持久化,无需回滚机制(IAE 直接传播,
     * 语义同热路径)。</p>
     *
     * <p><b>锁外路由</b>:cohort 安全网注册(窗口内外统一,见 {@link #routeColdAfterPersist})→
     * 窗口内热载 → P1-2 post-check(round 14 收口至 {@link ColdHotReconciler};竞态兜底——
     * stale 副本的主收口是窗口内 fresh 副本 upsert 本身)。</p>
     *
     * <p><b>C4-2 提交后守卫(round 14 F4)</b>:persistToWheel 成功即提交——此后 awaitCommit 或
     * 锁外路由失败一律 warn 后按成功返回,不回滚 revision、不作废新槽(镜像热路径提交后分支)。
     * 与热路径的差异:热路径提交后失败有 intentStore.save 补偿面;冷路径提交后磁盘/索引已一致,
     * 路由属增强,无补偿面。提交前失败(updater IAE/校验 IAE/槽不可读)行为不变。</p>
     *
     * @return 更新后的 Intent 副本;不存在(无索引)/并发取消已清索引/槽缺失损坏/已终态返回 empty;
     *         提交后失败(awaitCommit/路由)按成功返回(C4-2)
     */
    private Optional<Intent> updateCold(String intentId, Consumer<Intent> updater, Instant newExecuteAt) {
        if (locationIndex.get(intentId) == null) {
            return Optional.empty();                        // 不存在(无索引项)
        }
        SlotLocation newLoc = null;
        Intent cold = null;
        Object lock = persistence.acquireColdLock(intentId);
        synchronized (lock) {
            try {
                // 必须在锁内重读索引:入口 loc 是锁外快照,先到者(并发取消/改期)可能已写新槽/移除索引。
                SlotLocation latest = locationIndex.get(intentId);
                if (latest == null) {
                    return Optional.empty();                // 并发取消/终态回收已清索引
                }
                Intent current = persistence.readColdSlot(intentId, latest);
                if (current == null) {
                    // 槽位缺失/损坏(空槽、撕裂写、桶已回收、tail CRC 损坏):不谎报成功。
                    logger.warn("updateCold: slot unreadable for cold intent {}, loc={}", intentId, latest);
                    return Optional.empty();
                }
                if (current.getStatus().isTerminal()) {
                    // 防御:终态墓碑不应仍被索引(cancelCold 已清索引);见到即当不存在。
                    return Optional.empty();
                }
                updater.accept(current);
                // 校验镜像热路径(顺序一致);失败抛 IAE,副本未持久化磁盘 untouched。
                if (current.getExecuteAt() == null) {
                    throw new IllegalArgumentException(
                        "updater must not set executeAt to null for cold intent " + intentId);
                }
                if (current.getStatus() != IntentStatus.SCHEDULED) {
                    // 冷域只有 SCHEDULED(DUE 是热扫描态,不会落盘)。
                    throw new IllegalArgumentException(
                        "updater must not move cold intent " + intentId + " to " + current.getStatus()
                            + "; only SCHEDULED is valid for cold update");
                }
                // R22 镜像:先应用 newExecuteAt 再校验——校验必须覆盖最终落盘态
                // (deadline>executeAt 等约束以 executeAt 为锚,校验后再改期会漏掉越界改期)。
                if (newExecuteAt != null) {
                    current.setExecuteAt(newExecuteAt);
                }
                IntentValidator.validate(current);
                // 归一化放在校验之后(热路径同序)。
                IntentCommandService.normalizePrecisionTier(precisionTierCatalog, current);
                current.incrementRevision();
                // 写协议全复用:locate→put(tail/wheel)→tail→wheel 迁移清旧记录→multiSlot.add→
                // locationIndex.put(索引先行)→trackMaxRevision。round 13 冷改期恒 DURABLE。
                newLoc = persistence.persistToWheel(current, false);
                try {
                    persistence.awaitDurableCommit();
                } catch (RuntimeException e) {
                    // C4-2(round 14 F4):persistToWheel 成功即提交(新槽已在 mmap + 索引),
                    // awaitCommit 失败不回滚 revision/不作废新槽——镜像热路径提交后分支
                    // (updateIntent 提交后 catch 分支),磁盘/索引为权威。
                    logger.warn("Cold update committed; ignoring post-commit awaitCommit failure: id={}",
                        intentId, e);
                }
                cold = current;
            } finally {
                persistence.releaseColdLock(intentId, lock);
            }
        }
        try {
            routeColdAfterPersist(intentId, cold, newLoc);
        } catch (RuntimeException e) {
            // C4-2(round 14 F4):路由属提交后增强(热载/cohort 注册),失败不回滚磁盘权威态;
            // cohort 缺注册由重启 recovery 重建兜底,热载缺口由 cohort 安全网复核自愈。
            logger.warn("Cold update committed; ignoring post-commit routing failure: id={}", intentId, e);
        }
        return Optional.of(cold.copy());
    }

    /**
     * 冷改期锁外路由:cohort 安全网注册(窗口内外统一)+ 窗口内热载 +
     * P1-2 post-check(round 14 收口至 ColdHotReconciler;update/fireNow 冷命令共用本路由;
     * post-check 为竞态兜底——stale 副本的主收口是窗口内 fresh 副本 upsert 本身)。
     *
     * <p><b>为什么窗口内也注册 cohort(spec 4.4 安全网)</b>:promote 回调的复核回滚是<b>条件
     * 回滚</b>(round 15 终审 F2:索引失配且热副本 revision 不高于载入 revision 才回滚,判据以
     * upsert 前捕获的载入 revision 为基准)——曾表述的"无条件按 id 删热副本、在途旧 promote
     * 交错时误删刚热载新副本"窗口已被该守卫关闭。cohort 注册仍保留为自愈安全网:覆盖副本
     * 彻底丢失(hot == null)与 post-check 时序错过的情形。cohort 到点 promote:见热副本幂等
     * no-op;副本丢失则从新槽重热载——系统自愈,不依赖 post-check 时序。窗口内注册的代价仅是
     * 到点一次幂等 no-op promote。</p>
     *
     * <p><b>post-check stale 判别</b>:窗口外任何热副本皆 stale(promote 只可能载入旧槽内容);
     * 窗口内按 revision 判别(&lt; 本次新 revision 为 stale)。与 LoomqEngine promote 回调自检
     * (latest≠h.loc 跳过 + upsert 后复核回滚)构成双向清理,P1-2 同构(round 14 双侧统一
     * 收口至 ColdHotReconciler)。</p>
     *
     * <p>package-private(round 14):测试桩覆写本方法注入提交后路由失败。</p>
     */
    void routeColdAfterPersist(String intentId, Intent cold, SlotLocation newLoc) {
        // R13a(终审修正):路由起点重读索引——释放冷锁后,并发冷取消可能已写 CANCELED 槽并清索引,
        // 或并发更新已写更高 revision 的新槽。索引不再指向本写(newLoc)说明磁盘已有更高 revision
        // 的权威态(取消/新改期):本写的热载会成为 ghost(已取消)或 stale(旧改期)热副本——
        // 弃用路由,磁盘/后到者的路由为权威。交错窗口内(取消持锁未写索引)本路由可能先行
        // upsert,由 cancelCold 的 P1-2 post-check 删热副本收口(round 14 经 ColdHotReconciler
        // 单一权威实现)。
        SlotLocation current = locationIndex.get(intentId);
        if (current == null || !current.equals(newLoc)) {
            logger.warn("Cold write superseded by cancel/newer write before routing; "
                + "skipping hot-load: id={}", intentId);
            return;
        }
        long executeAtMs = cold.getExecuteAt().toEpochMilli();
        long deltaMs = executeAtMs - System.currentTimeMillis();
        boolean withinHotWindow = deltaMs <= hotBoundaryMs;
        promotionDaemon.register(intentId, newLoc, executeAtMs);
        if (withinHotWindow) {
            intentStore.upsert(cold);
            // 冷域恒 SCHEDULED(锁内已校验),直接 schedule;镜像 create 热路由(IntentCreator 冷热分支)。
            scheduler.schedule(cold);
        }
        // P1-2 → round 14:promote 竞态 post-check 收口至 ColdHotReconciler(单一权威实现:
        // 窗口/revision 判别 + 降级 + 窗口内重热载,行为原样保持)。
        reconciler.removeHotCopyIfStale(intentId, cold, withinHotWindow);
    }
}
