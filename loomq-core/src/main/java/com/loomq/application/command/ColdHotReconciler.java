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
 *   <li>promote 侧 {@link #rollbackPromoteHotLoad}:热载落地后复核索引,失配即回滚热载
 *       (调用方:LoomqEngine promote 回调,经 {@link IntentCommandService#reconcilePromotion})</li>
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
     * 新调度槽)或被移除,说明 promote 读槽→落地期间发生了冷命令:回滚热载,杜绝 ghost 投递/
     * 旧内容热载。与冷命令末尾的 removeHotCopyIfPresent/IfStale 形成双向清理。
     *
     * @param promoted    已 upsert 进内存的热载副本
     * @param promotedLoc promote 读槽时的槽位(handle loc)
     */
    void rollbackPromoteHotLoad(Intent promoted, SlotLocation promotedLoc) {
        SlotLocation after = locationIndex.get(promoted.getIntentId());
        if (after != null && after.equals(promotedLoc)) {
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
