package com.loomq.application.command;

import com.loomq.domain.intent.Intent;
import com.loomq.infrastructure.wheel.GroupCommitBarrier;
import com.loomq.infrastructure.wheel.IntentLocationIndex;
import com.loomq.infrastructure.wheel.SlotCodec;
import com.loomq.infrastructure.wheel.SlotLocation;
import com.loomq.infrastructure.wheel.TailIndex;
import com.loomq.infrastructure.wheel.WheelStore;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PHTW 持久化协议 + 簿记四件套单主所有（round 10 自 IntentCommandService 拆出;非 final——round 14 测试桩子类注入提交后失败用）。
 *
 * 写协议:locate→(inTail? tail.put : wheel.put)→locationIndex.put→(durable? awaitCommit);
 * 终态经 persistTerminalInPlace 原地覆写,reclaimTerminal 在 awaitCommit 后定向回收。
 * multiSlotIntents/tombstoneIds/pendingReclaims/maxRevisions 仅本类可触碰;
 * 兄弟组件经簿记缝访问(列举不计数,免得每次加缝改这行):maxRevisionOf /
 * markColdCancelTombstone / discardTerminalBooking / trackMaxRevision /
 * markMultiSlot / markMaxRevisions(恢复期经命令服务注入) /
 * acquireColdLock / releaseColdLock / readColdSlot(round 13 增冷改期三缝)。
 */
class WheelPersistence {

    private static final Logger logger = LoggerFactory.getLogger(WheelPersistence.class);

    private final WheelStore wheelStore;
    private final TailIndex tailIndex;
    private final GroupCommitBarrier commitBarrier;
    private final IntentLocationIndex locationIndex;

    /** 曾发生第 2 次及以上槽写入的 Intent（重排程/改期/取消追加）→ 终态不回收，保留 tombstone。 */
    private final Set<String> multiSlotIntents = ConcurrentHashMap.newKeySet();

    /**
     * 曾保留终态墓碑的 intentId（C4-1,R15 守卫）。墓碑留在磁盘上（多槽终态不回收、冷取消
     * 追加新槽）,参与 recovery 的 max-revision 去重——因此本进程内的 {@link #maxRevisions}
     * 种子映射必须随之保留,不可在后续单槽 incarnation 终态回收时移除;否则同进程重建从
     * 0 起步,重启被旧墓碑遮蔽(静默丢失)。一旦置位,进程内不清理(墓碑何时被桶文件过期
     * 回收无从得知;重启后由 WheelRecovery 重新从磁盘扫描注入)。
     */
    private final Set<String> tombstoneIds = ConcurrentHashMap.newKeySet();
    /** 终态原地覆写后的待回收记录：终态槽在 awaitCommit 落盘后清空。 */
    private final Map<String, PendingReclaim> pendingReclaims = new ConcurrentHashMap<>();

    /**
     * 待回收记录：loc 为终态槽位置；singleSlot 为 true 才清空复用。
     * （原 IntentCommandService.PendingReclaim，round 10 迁入降包私有，外部零引用）
     */
    record PendingReclaim(SlotLocation loc, boolean singleSlot) {}

    /**
     * intentId → 磁盘历史最高 revision 的活映射。createIntent 重建同 intentId 时需把新
     * Intent 的 revision 种子到历史最高值之上——否则新 Intent 从 0 起步,recovery 按
     * max revision 去重会被旧终态墓碑(更高 revision,multiSlot 保留)遮蔽,新 Intent 静默丢失。
     *
     * <p>来源有二:(1) 恢复期由 WheelRecovery 注入({@link #markMaxRevisions});(2) 本进程
     * 内每次持久化写(append/原地覆写)后 merge 当前 revision——覆盖"同进程内 create→cancel→
     * 重建同 id→重启"的路径,此时重建前无恢复注入,须靠写路径持续维护。</p>
     */
    private final ConcurrentHashMap<String, Long> maxRevisions =
        new ConcurrentHashMap<>();

    /**
     * 冷路径(冷取消/冷改期 round 13/冷 fireNow round 14)按 intentId 串行化的细粒度锁注册表(共享单主)。
     *
     * <p>wheel 路径:wheelStore.readSlot 每次返回新解码实例,synchronized(cold) 锁的是 transient
     * 副本,无法阻塞并发写;按 intentId 取一把稳定锁对象,串行化读-改-写。tail 路径同持此锁
     * (round 13):冷改期的 tailIndex.put 与冷取消的 remove 交错,会让被取消 Intent 以更高
     * revision 复活(remove 的布尔门控盖不住"remove 后 put"窗口)。锁对象在 synchronized 块的
     * finally 中以 identity 校验移除,只删自己放入的对象,避免误删后到者的锁。</p>
     *
     * <p>兄弟组件(IntentCanceler/IntentUpdater)经 {@link #acquireColdLock} /
     * {@link #releaseColdLock} 两缝使用,禁止直接持有注册表引用。</p>
     */
    private final ConcurrentHashMap<String, Object> coldWriteLocks = new ConcurrentHashMap<>();

    WheelPersistence(WheelStore wheelStore, TailIndex tailIndex,
                     GroupCommitBarrier commitBarrier, IntentLocationIndex locationIndex) {
        this.wheelStore = wheelStore;
        this.tailIndex = tailIndex;
        this.commitBarrier = commitBarrier;
        this.locationIndex = locationIndex;
    }

    /**
     * PHTW 写协议:locate→(inTail? tail.put : wheel.put)→locationIndex.put→(durable? awaitCommit)。
     * 返回实际分配槽位(wheel 路径为 put 捕获的真实槽;tail 路径为 locate 的占位)。
     * createIntent 与 persistStateChangePutOnly 共用,避免副本漂移。
     */
    SlotLocation persistToWheel(Intent intent, boolean durable) {
        String id = intent.getIntentId();
        SlotLocation loc = wheelStore.locate(intent.getExecuteAt());
        SlotLocation prevLoc = locationIndex.get(id);
        if (loc.inTail()) {
            tailIndex.put(intent);
        } else {
            loc = wheelStore.put(intent);                   // 捕获分配的真实槽位
            // R21: tail→wheel 迁移(提升后 fireNow/改期/取消)必须清除旧 tail 记录——
            // 否则残留 SCHEDULED 旧记录:恢复 slotCounts 虚增使终态墓碑永不回收,
            // 且 wheel 桶文件过期(31 天)后该记录成为唯一幸存者,重启被当活 intent
            // 复活投递(幽灵)。tail tombstone 与 wheel 槽同批 awaitCommit 落盘,无崩溃窗口。
            if (prevLoc != null && prevLoc.inTail()) {
                tailIndex.remove(id);
            }
        }
        if (prevLoc != null) {
            multiSlotIntents.add(id);          // 2nd+ 写入 → 多槽(重排程/改期/取消)
        }
        locationIndex.put(id, loc);
        trackMaxRevision(intent);              // R9: 维持活映射,同进程重建时种子 revision
        if (durable) {
            commitBarrier.awaitCommit();
        }
        return loc;
    }

    /**
     * 状态变更持久化的 "put-only" 入口（供 PrecisionScheduler 在 synchronized(intent) 内调用）。
     * 仅写 PHTW + 索引（非阻塞 mmap），不 awaitCommit——持久化等待由调度器在锁外调用
     * {@link #awaitDurableCommit()} 完成，避免 VT 在 synchronized 内 park 导致 carrier pinning。
     */
    void persistStateChangePutOnly(Intent intent) {
        persistToWheel(intent, false);   // durable=false → 跳过 awaitCommit，仅 put
    }

    /**
     * 终态原地覆写（非阻塞 mmap）：把 locationIndex 指向的最新槽覆写为终态，不追加新槽。
     * 单槽 Intent 排队待回收（pendingReclaims），落盘后由 reclaimTerminal 清空；多槽只覆写不回收。
     * 无索引或 tail 时回退追加（tail 超视界非回收路径）。失败由调用方按 I6 吞掉。
     */
    void persistTerminalInPlace(Intent intent) {
        String id = intent.getIntentId();
        SlotLocation loc = locationIndex.get(id);
        if (loc == null || loc.inTail()) {
            if (loc != null && loc.inTail()) {
                // tail 终态:记录残留 run 文件(不回收,C3-3)→ 按墓碑语义保护种子映射
                tombstoneIds.add(id);
                pendingReclaims.put(id, new PendingReclaim(loc, false));
            }
            persistToWheel(intent, false);     // 回退追加；awaitCommit 由调用方在锁外完成
            return;
        }
        wheelStore.overwriteSlot(loc, SlotCodec.encode(intent));
        trackMaxRevision(intent);              // R9: 终态原地覆写同样推进历史最高 revision
        boolean singleSlot = !multiSlotIntents.contains(id);
        if (!singleSlot) {
            // 多槽终态 → 磁盘保留墓碑(不回收),revision 种子映射须随之保留(C4-1)
            tombstoneIds.add(id);
        }
        pendingReclaims.put(id, new PendingReclaim(loc, singleSlot));
    }

    /**
     * 终态槽回收（须在 awaitCommit 之后调用）：先清 locationIndex（终态不索引，杜绝 freed 槽被
     * stale 别名误复用），单槽清空入 free-list 复用；多槽保留 tombstone。无论单多槽都清理
     * multiSlotIntents，避免集合泄漏。
     */
    void reclaimTerminal(String intentId) {
        PendingReclaim pr = pendingReclaims.remove(intentId);
        try {
            // C2-1: 定向移除——只清"自己终态化的槽"的索引。并发重建(磁盘终态允许同 id
            // 重建)已把索引指向新槽时,无条件按 intentId 移除会抹掉新 incarnation 的索引
            // (冷 intent 到点不被 promote,静默不投递直到重启)。
            boolean indexRemoved;
            if (pr != null) {
                indexRemoved = locationIndex.remove(intentId, pr.loc());
            } else {
                // 无待回收记录(补偿等回退追加路径):维持无条件清理
                locationIndex.remove(intentId);
                indexRemoved = true;
            }
            if (pr != null && pr.singleSlot() && !pr.loc().inTail()) {
                wheelStore.freeSlot(pr.loc());
                // R15: 单槽终态被释放后磁盘上通常不再有该 intentId 的竞争槽,可安全移除历史
                // revision 映射——否则 maxRevisions 按 distinct intentId 无界累积(内存泄漏)。
                // C4-1 守卫:曾保留终态墓碑(历史多槽墓碑/冷取消墓碑仍留磁盘,参与 max-revision
                // 去重)或索引被并发重建移动(indexRemoved=false)时不可移除——否则同进程重建
                // 从 0 起步,重启被墓碑遮蔽(静默丢失)。
                if (indexRemoved && !tombstoneIds.contains(intentId)) {
                    maxRevisions.remove(intentId);
                    logger.debug("Reclaimed terminal slot {} for intent {}", pr.loc(), intentId);
                }
            }
        } catch (Exception e) {
            logger.warn("reclaimTerminal failed for intent {}: {}", intentId, e.getMessage(), e);
        } finally {
            multiSlotIntents.remove(intentId);
        }
    }

    /** 恢复期由 WheelRecovery 注入:磁盘上幸存槽数 >1 的 Intent 视为多槽(终态保留墓碑不回收)。 */
    void markMultiSlot(Set<String> ids) {
        multiSlotIntents.addAll(ids);
    }

    /** 恢复期由 WheelRecovery 注入:intentId → 磁盘历史最高 revision(含终态墓碑与跨视界 tail)。 */
    void markMaxRevisions(Map<String, Long> revisions) {
        revisions.forEach((id, rev) -> maxRevisions.merge(id, rev, Math::max));
    }

    /** 记录某 intentId 的最新持久化 revision(写路径调用,维持活映射)。 */
    void trackMaxRevision(Intent intent) {
        maxRevisions.merge(intent.getIntentId(), intent.getRevision(), Math::max);
    }

    /**
     * 阻塞到覆盖最近一次 put 的 group-commit frontier 落盘。
     * 安全地在 synchronized(intent) 之外调用（此时 VT 可正常 unmount，不 pin carrier）。
     */
    void awaitDurableCommit() {
        commitBarrier.awaitCommit();
    }

    /**
     * 恢复注入与活映射读取的缝——恒等实现见计划缝方法表，禁止增删操作。
     * 兄弟组件可触达成员(列举不计数):maxRevisionOf / markColdCancelTombstone /
     * discardTerminalBooking / trackMaxRevision / markMultiSlot / markMaxRevisions /
     * acquireColdLock / releaseColdLock / readColdSlot(round 13 增冷改期三缝)。
     */
    Long maxRevisionOf(String intentId) {
        return maxRevisions.get(intentId);
    }

    void markColdCancelTombstone(String intentId) {
        multiSlotIntents.add(intentId);
        tombstoneIds.add(intentId);
    }

    void discardTerminalBooking(String intentId) {
        pendingReclaims.remove(intentId);
        multiSlotIntents.remove(intentId);
    }

    /** 冷路径 per-id 互斥进:computeIfAbsent 取稳定锁对象(契约见 coldWriteLocks)。 */
    Object acquireColdLock(String intentId) {
        return coldWriteLocks.computeIfAbsent(intentId, k -> new Object());
    }

    /** 冷路径 per-id 互斥出:identity 校验 computeIfPresent,只移除自己放入的锁对象。 */
    void releaseColdLock(String intentId, Object lock) {
        coldWriteLocks.computeIfPresent(intentId, (k, v) -> v == lock ? null : v);
    }

    /**
     * 冷路径读缝:按索引槽位读 Intent(wheel 直读 / tail 键读)。
     * 返回 null 表示槽位缺失/损坏(防御解码,promote 同款,不抛异常)。
     * 调用方须持 {@link #acquireColdLock} 串行化读-改-写。
     */
    Intent readColdSlot(String intentId, SlotLocation loc) {
        return loc.inTail() ? tailIndex.readById(intentId) : wheelStore.readSlot(loc);
    }
}
