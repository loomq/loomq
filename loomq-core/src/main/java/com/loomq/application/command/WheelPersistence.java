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
 * acquireColdLock / releaseColdLock / readColdSlot(round 13 增冷改期三缝) / writeFreshUnderColdLock(round 16 增守卫原语)。
 *
 * <p><b>F2 冷锁序列化点(round 15;round 16 原语收口)</b>:per-id 冷锁现覆盖<b>既有 Intent
 * 的所有状态变更持久化写者</b>——冷命令(updateCold/cancelCold/fireNowCold,锁内重读磁盘)
 * 与热命令写者,后者按复核语义分两族:
 * <ul>
 * <li><b>复核族(W1-W4)</b>:updateIntent/fireNow/cancelIntent 的提交段、投递重试路径
 *     persistStateChangePutOnly(F6 起为单一守卫门面)——统一经
 *     {@link #writeFreshUnderColdLock} 在冷锁内复核后原子写;复核判据的唯一权威成文见
 *     该方法 javadoc(此处不复述)。</li>
 * <li><b>互斥族(F4/F5)</b>:终态原地覆写 persistTerminalInPlace(F4)与 create 失败补偿段
 *     compensateCancel(F5)——持冷锁互斥但<b>不做 revision 复核</b>(终态必落盘/补偿语义
 *     保持),归属残留见各自 javadoc。</li>
 * </ul>
 * 例外:create 主写入路径(W8)不入冷锁——其活跃副本预检 hasActiveDuplicate 为既有守卫域
 * (冷锁外预检)。stale 热副本(磁盘已前进)由命令族委托冷路径 fresh 重放、投递路径跳过落盘
 * (磁盘最新态为权威;cancelCold 竞态子情形下权威为 CANCELED 终态槽,不再投递)。
 * 锁序:热路径 {@code synchronized(intent) → 冷锁};冷命令路径的临界区仅碰 transient 解码
 * 副本,不取 intent 监视器(热守卫临界区 mutate 的是活对象,与此消歧——round 15 终审 d)
 * ——无死锁环;awaitCommit 恒在 <b>intent 监视器</b>之外(复核族经原语 javadoc 契约强制;
 * 冷命令路径在冷锁内 await 为 round 13 既有契约,per-id 锁非监视器,VT 可正常 unmount)。
 * 互斥族残留:F4 平局窗已由冷锁互斥关闭(round 15 终审 F4),残留仅为"终态 vs 后到冷写"
 * 的语义归属——投递已发生,终态覆写胜出(冷命令锁内重读索引见终态槽即拒绝,安全收敛),
 * 非恢复仲裁不确定;F5 同构残留见 IntentCreator.compensateCancel javadoc。</p>
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
     * revision 复活(remove 的布尔门控盖不住"remove 后 put"窗口)。</p>
     *
     * <p><b>锁对象永不移除(round 15 F1)</b>:曾以"release 时 identity 校验移除、只删自己放入
     * 的对象"回收锁对象,但该方案存在 waiter 竞态——等待者 B 已持 L1 引用阻塞在监视器上,
     * 持有者 A release 移除注册表项后,新到者 C 经 {@code computeIfAbsent} 得到新对象 L2,
     * B(L1)/C(L2) 同时进入同一 intentId 的临界区,F2"冷锁内复核-写原子"前提被击穿。
     * 故 release 不再移除:注册表按去重后的 intentId 有界增长(与 {@link #maxRevisions} 同阶,
     * 每条仅一个空 Object),换取临界区互斥严格成立。</p>
     *
     * <p>用户面:IntentUpdater / IntentCanceler / IntentCommandService 三兄弟——冷命令
     * (updateCold/cancelCold/fireNowCold)、热命令守卫(W1 updateIntent/W2 fireNow/W3
     * cancelIntent)、投递路径守卫(W4)、终态原地覆写(persistTerminalInPlace)与 create
     * 失败补偿段(compensateCancel)皆持此锁——本注册表是<b>既有 Intent 的所有状态变更
     * 持久化写者的序列化点</b>
     * (create 主写入路径例外,见类头 W8 句)。</p>
     *
     * <p>兄弟组件经 {@link #acquireColdLock} / {@link #releaseColdLock} 两缝使用,禁止直接持有注册表引用。</p>
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
     * 状态变更持久化的 "put-only" 入口:仅写 PHTW + 索引(非阻塞 mmap),不 awaitCommit。
     * 调用方 = IntentCommandService 守卫门(经 {@link #writeFreshUnderColdLock} 复核)与
     * IntentUpdater W1/W2 守卫临界区(write 回调体内),均在 per-id 冷锁内执行。
     * DURABLE 等待由调用方在 intent 监视器与冷锁之外完成(投递路径:StatePersistence.awaitCommit
     * → IntentCommandService.awaitDurableCommit → {@link #awaitDurableCommit()}),
     * 避免 VT 在锁内 park 导致 carrier pinning。
     */
    void persistStateChangePutOnly(Intent intent) {
        persistToWheel(intent, false);   // durable=false → 跳过 awaitCommit，仅 put
    }

    /**
     * 终态原地覆写（非阻塞 mmap）：把 locationIndex 指向的最新槽覆写为终态，不追加新槽。
     * 单槽 Intent 排队待回收（pendingReclaims），落盘后由 reclaimTerminal 清空；多槽只覆写不回收。
     * 无索引或 tail 时回退追加（tail 超视界非回收路径）。失败由调用方按 I6 吞掉。
     *
     * <p><b>F4(round 15 终审): 全体纳入 per-id 冷锁互斥</b>——锁外 {@code locationIndex.get}
     * 与冷写者 {@code index.put} 交错可产生 terminal R+1 / SCHEDULED R+1 同 revision 平局,
     * 恢复按扫描序仲裁(与类头"无双写平局"旧表述矛盾)。临界区内无 revision 复核、无 skip
     * 语义——终态必落盘;无 awaitCommit(回退追加分支的 persistToWheel 亦 durable=false,
     * 非阻塞,可接受),awaitCommit 仍由调用方在锁外完成。可重入安全:W3 守卫临界区已持
     * 同锁(F1 起锁对象不移除),同线程 synchronized 重入。</p>
     */
    void persistTerminalInPlace(Intent intent) {
        String id = intent.getIntentId();
        Object coldLock = acquireColdLock(id);
        try {
            synchronized (coldLock) {
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
        } finally {
            releaseColdLock(id, coldLock);
        }
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
     * acquireColdLock / releaseColdLock / readColdSlot(round 13 增冷改期三缝) / writeFreshUnderColdLock(round 16 增守卫原语)。
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

    /**
     * 既有 Intent 状态变更持久化写者的单一守卫原语(W1-W4 唯一实现):per-id 冷锁内复核
     * 磁盘历史最高 revision,判据 {@code diskMax != null && diskMax >= writeRev} → stale,
     * 跳过 write 返回 false;fresh 执行 write 返回 true。
     * writeRev 约定 = 本次的写入 revision:W1-W3(increment 族)传 base+1;
     * W4(StatePersistence 已预 increment)传当前值。
     * 复核+写原子于同一冷锁临界区;临界区内禁止 awaitCommit(调用方锁外等待,防 VT pin);
     * 锁序:调用方已持 intent 监视器 → 本冷锁;冷命令路径的临界区不取 intent 监视器,无死锁环。
     * stale 政策(还原/demote/委托冷路径 fresh 重放/skip+warn/revision 回滚)由调用方在
     * 返回 false 后处置——本原语只裁决"写或不写",不承载语义。
     */
    boolean writeFreshUnderColdLock(String intentId, long writeRev, Runnable write) {
        Object coldLock = acquireColdLock(intentId);
        try {
            synchronized (coldLock) {
                Long diskMax = maxRevisionOf(intentId);
                if (diskMax != null && diskMax >= writeRev) {
                    return false;
                }
                write.run();
                return true;
            }
        } finally {
            releaseColdLock(intentId, coldLock);
        }
    }

    /** 冷路径 per-id 互斥进:computeIfAbsent 取稳定锁对象(契约见 coldWriteLocks)。 */
    Object acquireColdLock(String intentId) {
        return coldWriteLocks.computeIfAbsent(intentId, k -> new Object());
    }

    /**
     * 冷路径 per-id 互斥出(round 15 F1 起为空操作):锁对象不再移除——移除会让阻塞中的等待者
     * 持旧锁对象、新到者经 computeIfAbsent 得新锁对象,两者同临界区并发(竞态推演与取舍见
     * {@link #coldWriteLocks})。保留方法形态以维持 acquire/finally release 配对契约的可读性;
     * 注册表随 intentId 永存,无需配对清理。
     */
    void releaseColdLock(String intentId, Object lock) {
        // F1(round 15): 故意留空——绝不 coldWriteLocks.remove。identity 校验移除的旧实现
        // 存在 waiter 竞态(见 coldWriteLocks javadoc),互斥正确性优先于注册表即时回收。
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
