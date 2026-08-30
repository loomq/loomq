package com.loomq.application.command;

import com.loomq.application.scheduler.PrecisionScheduler;
import com.loomq.domain.intent.Intent;
import com.loomq.infrastructure.wheel.IntentLocationIndex;
import com.loomq.infrastructure.wheel.SlotLocation;
import com.loomq.store.IntentStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 冷↔热 reconcile 协议的单一权威实现(round 14,原 P1-2 三处内联收口)。
 *
 * <p><b>协议背景(promote↔冷命令 TOCTOU)</b>:promote 读槽→落地的窗口与冷取消/冷改期/
 * 冷 fireNow 交错时,磁盘索引与内存热副本可能短暂分歧(已取消的 ghost 投递 / 旧调度的
 * stale 热副本)。收口依赖两可见动作(locationIndex.put 与 intentStore.upsert)的全序:
 * 后到者必见先到者,双向复查确定性关闭窗口——promote 侧落地后复核索引,冷命令侧提交后
 * 复查热副本。</p>
 *
 * <p><b>三个触发面</b>(round 14 仅结构统一,行为与原三处内联逐一保持;行为变更须先改本类):
 * <ul>
 *   <li>promote 侧 {@link #rollbackPromoteHotLoad}:热载落地后复核索引,失配且内存热副本
 *       revision 不高于载入 revision(调用方在 upsert 前捕获)即回滚热载——热写者 round 15 起
 *       持冷锁复核后写新槽改指索引,其推进后的新副本不得被误删;热副本不存在则 demote(promoted)
 *       幂等(调用方:LoomqEngine promote 回调,
 *       经 {@link IntentCommandService#reconcilePromotion})</li>
 *   <li>冷取消侧 {@link #removeHotCopyIfPresent}:索引已清无从对比,热副本存在即降级
 *       (调用方:IntentCanceler.cancelCold 末尾)</li>
 *   <li>冷改期侧 {@link #removeHotCopyIfStale}:按窗口/revision 判别 stale,降级后窗口内
 *       重热载新副本(调用方:IntentUpdater.routeColdAfterPersist)</li>
 * </ul></p>
 */
final class ColdHotReconciler {

    private static final Logger logger = LoggerFactory.getLogger(ColdHotReconciler.class);

    private final IntentStore intentStore;
    private final PrecisionScheduler scheduler;
    private final IntentLocationIndex locationIndex;

    ColdHotReconciler(IntentStore intentStore, PrecisionScheduler scheduler,
                      IntentLocationIndex locationIndex) {
        this.intentStore = intentStore;
        this.scheduler = scheduler;
        this.locationIndex = locationIndex;
    }

    /**
     * promote 侧:热载落地(upsert+schedule 由调用方完成)后复核索引——索引已迁移(CANCELED 槽/
     * 新调度槽)或被移除,说明 promote 读槽→落地期间发生了冷命令或热写者:按 revision 判别后
     * 决定是否回滚热载,杜绝 ghost 投递/旧内容热载。与冷命令末尾的 removeHotCopyIfPresent/
     * IfStale 形成双向清理。
     * round 15 F2:索引失配不必然是冷命令——热写者写新槽同样改指索引,此时若内存热副本
     * revision 已高于载入 revision(热写者合法推进),不得回滚(详见方法内守卫注释)。
     * round 15 终审:判据基准改为调用方在 <b>upsert 之前</b>捕获的 {@code promotedRevision}——
     * 回调 upsert 的 P 与后续热写者推进的是同一可变对象,reconcile 时 findByIdInternal 返回的
     * 仍是 P,旧判据 {@code hot.getRevision() > promoted.getRevision()} 对同引用恒 false,
     * 推进后的副本仍被误 demote(同对象盲区);以载入时 revision 为基准后,同引用推进
     * (hot.revision > promotedRevision)豁免,冷命令迁移(hot 为不同对象且 revision 未超)照旧回滚。
     *
     * @param promoted         已 upsert 进内存的热载副本
     * @param promotedLoc      promote 读槽时的槽位(handle loc)
     * @param promotedRevision 载入副本 upsert 前的 revision(调用方在 upsert 前捕获——同对象
     *                         盲区修复的判据基准)
     */
    void rollbackPromoteHotLoad(Intent promoted, SlotLocation promotedLoc, long promotedRevision) {
        SlotLocation after = locationIndex.get(promoted.getIntentId());
        if (after != null && after.equals(promotedLoc)) {
            return;
        }
        // F2(round 15): 索引迁移不必然来自冷命令——热写者(updateIntent/fireNow,round 15 起
        // 持冷锁复核)复核通过后写新槽同样改指索引,其内存副本 revision 已高于 promote 载入的
        // 旧副本;无条件回滚会误删推进后的新副本(内存丢失直到下次命令/重启)。仅当热副本
        // revision 不高于载入 revision(promotedRevision)时回滚。三分支判据:hot 存在且
        // revision 已推进 → 豁免(热写者/同对象推进);hot 存在且 revision 未超 → 回滚
        // (冷命令迁移场景,round 14 既有行为);hot == null(副本已被取消侧删除)→ 走
        // demote(promoted) 幂等路径,不抛。
        Intent hot = intentStore.findByIdInternal(promoted.getIntentId());
        if (hot != null && hot.getRevision() > promotedRevision) {
            return;
        }
        demote(promoted, "Promotion rolled back for intent {} (raced with cold cancel/update/fireNow)");
    }

    /**
     * 冷取消侧 post-check:取消生效期间 intent 可能被在途 promote 并发热载;索引已清
     * 无从对比,热副本存在即降级,否则 ghost 投递直到重启 max-revision 纠正。
     */
    void removeHotCopyIfPresent(String intentId) {
        Intent hot = intentStore.findByIdInternal(intentId);
        if (hot != null) {
            demote(hot, "Cold cancel raced with promotion; removed hot copy of intent {}");
        }
    }

    /**
     * 冷改期侧 post-check:热副本可能是旧槽内容。判别:窗口外任何热副本皆 stale(promote 只
     * 可能载入旧槽内容);窗口内 revision &lt; 本次新 revision 为 stale。stale 则降级,并在
     * 窗口内重热载新副本(系统自愈,不依赖调用方时序)。
     */
    void removeHotCopyIfStale(String intentId, Intent authoritative, boolean withinHotWindow) {
        Intent hot = intentStore.findByIdInternal(intentId);
        if (hot == null || (withinHotWindow && hot.getRevision() >= authoritative.getRevision())) {
            return;
        }
        demote(hot, "Cold update raced with promotion; reconciled to new schedule: id={}");
        if (withinHotWindow) {
            intentStore.upsert(authoritative);
            scheduler.schedule(authoritative);
        }
    }

    /** 三触发面共同内核:调度结构摘除 + 内存删除 + 告警(单点实现)。 */
    private void demote(Intent hot, String warnTemplate) {
        scheduler.removeFromSchedule(hot);
        intentStore.delete(hot.getIntentId());
        logger.warn(warnTemplate, hot.getIntentId());
    }
}
