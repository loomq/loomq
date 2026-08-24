package com.loomq.application.command;

import com.loomq.application.scheduler.PrecisionScheduler;
import com.loomq.common.IntentValidator;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.domain.intent.PrecisionTierCatalog;
import com.loomq.infrastructure.wheel.IntentLocationIndex;
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
 */
final class IntentUpdater {

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

    IntentUpdater(IntentStore intentStore, PrecisionScheduler scheduler,
                  IntentLocationIndex locationIndex, PrecisionTierCatalog precisionTierCatalog,
                  WheelPersistence persistence) {
        this.intentStore = intentStore;
        this.scheduler = scheduler;
        this.locationIndex = locationIndex;
        this.precisionTierCatalog = precisionTierCatalog;
        this.persistence = persistence;
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
     * @param intentId     待更新 Intent ID
     * @param updater      变更消费者（在 synchronized(intent) 内执行）
     * @param newExecuteAt 新的执行时间；null 表示不改期
     * @return 更新后的 Intent；intent 不存在时返回 empty；intent 已处终态时返回未修改的
     *         intent（no-op，不持久化）
     */
    Optional<Intent> updateIntent(String intentId, Consumer<Intent> updater, Instant newExecuteAt) {
        Intent intent = intentStore.findByIdInternal(intentId);
        if (intent == null) {
            // 冷意图尚在磁盘(未提升入内存),无法更新;与 fireNow 同模式——区分"不存在"与"冷",
            // 避免调用方把冷 intent 误判为不存在(静默 no-op)
            if (locationIndex.get(intentId) != null) {
                logger.warn("Cannot update a cold intent (not yet promoted into memory): id={}; "
                    + "cold update not implemented (AGENTS.md)", intentId);
            }
            return Optional.empty();
        }

        boolean reschedule = false;
        boolean persisted = false;
        boolean removedForReschedule = false;
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
                    intent.rollbackStatus(statusBeforeUpdater, updatedAtBeforeUpdater);
                    throw new IllegalArgumentException(
                        "updater must not transition intent " + intentId + " to terminal state "
                            + intent.getStatus() + "; use cancelIntent");
                }

                // R16: updater 置空 executeAt 会污染调度器——persistToWheel.locate(null) NPE,
                // 且 scanDue 的 intent.getExecuteAt().isAfter(now) 持续 NPE 使该档扫描永久卡死。
                // 拒绝并回滚 executeAt/updatedAt,维持内核不变量(executeAt 恒非空)。
                // 注意顺序:setExecuteAt 会重写 updatedAt,须先还原 executeAt 再 rollbackStatus
                // (后者同时还原 status 与 updatedAt),避免 updatedAt 残留变异时间戳。
                if (intent.getExecuteAt() == null) {
                    intent.setExecuteAt(oldExecuteAt);
                    intent.rollbackStatus(statusBeforeUpdater, updatedAtBeforeUpdater);
                    throw new IllegalArgumentException(
                        "updater must not set executeAt to null for intent " + intentId);
                }

                // R21: updater 只允许停留在 SCHEDULED/DUE——DUE→DISPATCHING(合法迁移,
                // R11 只拦终态、R16 只拦 executeAt=null)会把"无投递在途"的 DISPATCHING
                // 持久化:内存态永久卡死;finalize 的 DUE 起步守卫从 DISPATCHING 转 DUE
                // 抛 ISE 被 runFinalizeTask 吞 → ACK 不落盘/onDelivered 不通知;重启后
                // recovery 把非终态 DISPATCHING 当活 intent 重复投递。拒绝并回滚。
                if (intent.getStatus() != IntentStatus.SCHEDULED && intent.getStatus() != IntentStatus.DUE) {
                    // 先捕获违规状态再回滚——rollbackStatus 会还原 status,消息须点名违规值
                    IntentStatus offender = intent.getStatus();
                    intent.rollbackStatus(statusBeforeUpdater, updatedAtBeforeUpdater);
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

                intent.incrementRevision();
                // 锁内仅非阻塞 put；DURABLE 等待移到锁外（VT 可正常 unmount，不 pin carrier）
                persistence.persistStateChangePutOnly(intent);
                persisted = true;
                intentStore.update(intent);
                // I5: schedule/restore 移到锁外 (deferred)
                // -- schedule() 有自己的 synchronized + 终态检查
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
            // 回滚调度字段:executeAt/status/updatedAt/precisionTier 还原到 updater 前值。
            // C4-5: updater 可能已变异这些字段后才抛异常——按"当前值"重排会把变异半应用
            // (与"失败即还原"语义不符,见 BugUpdateUpdaterFailureTest)。
            // 非调度字段(内容)的变异不可回滚(无更新前快照),属用户 updater 契约边界。
            // 顺序:setExecuteAt/setPrecisionTier 会重写 updatedAt,须先执行,再由
            // rollbackStatus 统一还原 status 与 updatedAt。
            if (oldExecuteAt != null) {
                intent.setExecuteAt(oldExecuteAt);
            }
            if (statusBeforeUpdater != null) {
                // R28: precisionTier 也是调度字段(决定 cohort/桶路由),updater 污染后必须
                // 还原,否则活对象残留 null/错误档,与磁盘(recovery 恢复值)短暂不一致。
                intent.setPrecisionTier(tierBeforeUpdater);
                intent.rollbackStatus(statusBeforeUpdater, updatedAtBeforeUpdater);
            }
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

    boolean fireNow(String intentId) {
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
        boolean persisted = false;
        try {
            synchronized (intent) {
                oldExecuteAt = intent.getExecuteAt();
                oldRevision = intent.getRevision();
                wasScheduled = scheduler.removeFromSchedule(intent);
                if (wasScheduled) {
                    intent.setExecuteAt(Instant.now());
                    intent.incrementRevision();
                    // 锁内仅非阻塞 put；DURABLE 等待移到锁外（VT 可正常 unmount）
                    persistence.persistStateChangePutOnly(intent);
                    persisted = true;
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
}
