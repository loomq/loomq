package com.loomq.application.command;

import com.loomq.application.scheduler.PrecisionScheduler;
import com.loomq.common.MetricsCollector;
import com.loomq.domain.intent.AckMode;
import com.loomq.domain.intent.Intent;
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
import com.loomq.tracing.IntentTraceStore;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
    private final Executor callbackExecutor;
    private final AtomicBoolean running;

    private volatile CallbackHandler callbackHandler;

    /** PHTW 持久化协议 + 簿记四件套单主(round 10 自本类拆出,见 WheelPersistence)。 */
    private final WheelPersistence persistence;

    /** 冷↔热 reconcile 单一权威实现(round 14,原 P1-2 三处内联收口)。 */
    private final ColdHotReconciler reconciler;

    /** 创建路径组件(round 10 自本类拆出):单条/批量创建、失败补偿取消、活跃副本预检。 */
    private final IntentCreator creator;

    /** 更新/立即触发路径组件(round 10 自本类拆出):updateIntent ×2 重载、fireNow。 */
    private final IntentUpdater updaterService;

    /** 取消路径组件(round 10 自本类拆出):热取消 + 冷取消(冷锁注册表现居 WheelPersistence,与 updateCold 共享)。 */
    private final IntentCanceler canceler;

    /**
     * PHTW 持久化栈(构造器归并组 1):时间轮/尾部/组提交/索引/提升 daemon。
     * 与 LoomqEngine 的字段组同构,新增依赖不再动 17 实参组装点。
     */
    public record PhtwStack(WheelStore wheelStore, TailIndex tailIndex, GroupCommitBarrier commitBarrier,
                            IntentLocationIndex locationIndex, PromotionDaemon promotionDaemon) {}

    /** 配置组(构造器归并组 2):默认档/组提交间隔/热边界/档位目录。 */
    public record CommandConfig(PrecisionTier defaultTier, long groupCommitIntervalMs, long hotBoundaryMs,
                                PrecisionTierCatalog precisionTierCatalog) {}

    public IntentCommandService(
        IntentStore intentStore,
        PrecisionScheduler scheduler,
        PhtwStack phtw,
        MetricsCollector metricsCollector,
        Executor callbackExecutor,
        AtomicBoolean running,
        AtomicLong sequenceNumber,
        CallbackHandler callbackHandler,
        CommandConfig config,
        IntentTraceStore traceStore
    ) {
        this.persistence = new WheelPersistence(phtw.wheelStore(), phtw.tailIndex(),
            phtw.commitBarrier(), phtw.locationIndex());
        this.reconciler = new ColdHotReconciler(intentStore, scheduler, phtw.locationIndex());
        this.intentStore = intentStore;
        this.callbackExecutor = callbackExecutor;
        this.running = running;
        this.callbackHandler = callbackHandler;
        // R21: catalog 归一(自定义目录的 walMode 默认必须被尊重)与 traceStore
        // (冷 create/取消/fireNow 的 trace 生命周期归命令服务管)
        PrecisionTierCatalog catalog = config.precisionTierCatalog() != null
            ? config.precisionTierCatalog()
            : PrecisionTierCatalog.defaultCatalog();
        // 组件组装顺序:persistence → creator → updater → canceler(round 10 拆分终态;
        // canceler 末参经 CallbackDispatcher 端口回指 facade 的锁外派发)
        this.creator = new IntentCreator(intentStore, scheduler, phtw.promotionDaemon(),
            metricsCollector, traceStore, sequenceNumber, config.defaultTier(),
            config.groupCommitIntervalMs(), config.hotBoundaryMs(), catalog,
            phtw.wheelStore(), phtw.tailIndex(), phtw.locationIndex(), this.persistence);
        this.updaterService = new IntentUpdater(intentStore, scheduler,
            phtw.locationIndex(), catalog, this.persistence,
            phtw.promotionDaemon(), this.reconciler, config.hotBoundaryMs());
        this.canceler = new IntentCanceler(intentStore, scheduler, phtw.promotionDaemon(),
            phtw.locationIndex(), phtw.wheelStore(), phtw.tailIndex(), metricsCollector,
            traceStore, this.persistence, this.reconciler, this::dispatchCallback);
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

    /** 归一化 null precisionTier，避免内部 live 状态与外部 copy 不一致。 */
    static void normalizePrecisionTier(PrecisionTierCatalog catalog, Intent intent) {
        if (intent.getPrecisionTier() == null) {
            intent.setPrecisionTier(catalog.defaultTier());
        }
    }

    public long createIntent(Intent intent, AckMode ackMode) {
        ensureRunning();
        return creator.createIntent(intent, ackMode);
    }

    public List<Long> createIntents(List<Intent> intents, AckMode ackMode) {
        ensureRunning();
        return creator.createIntents(intents, ackMode);
    }

    public Optional<Intent> updateIntent(String intentId, Consumer<Intent> updater) {
        ensureRunning();
        return updaterService.updateIntent(intentId, updater);
    }

    public Optional<Intent> updateIntent(String intentId, Consumer<Intent> updater, Instant newExecuteAt) {
        ensureRunning();
        return updaterService.updateIntent(intentId, updater, newExecuteAt);
    }

    public boolean cancelIntent(String intentId) {
        ensureRunning();
        return canceler.cancelIntent(intentId);
    }

    public boolean fireNow(String intentId) {
        ensureRunning();
        return updaterService.fireNow(intentId);
    }

    /**
     * promote 侧冷↔热 reconcile(round 14):委托 {@link ColdHotReconciler}——P1-2 协议的
     * 单一权威实现,协议语义见其类 javadoc。供 LoomqEngine 的 promote 回调调用。
     *
     * <p>round 15 终审 F2:{@code promotedRevision} 必须由调用方在 <b>upsert 之前</b>捕获传入
     * ——回调 upsert 的 P 与后续热写者推进的是同一可变对象,reconcile 时 findByIdInternal 返回
     * 的仍是 P,以 {@code promoted.getRevision()} 为判据对同引用恒 false(同对象盲区),推进后
     * 的副本仍被误 demote。</p>
     *
     * @param intent           promote 热载副本(调用方已 upsert)
     * @param loc              promote 读槽时的槽位
     * @param promotedRevision 载入副本 upsert 前的 revision
     */
    public void reconcilePromotion(Intent intent, SlotLocation loc, long promotedRevision) {
        reconciler.rollbackPromoteHotLoad(intent, loc, promotedRevision);
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

    /**
     * 解析持久化语义。三级优先级:显式 AckMode > intent 级 walMode > 档位默认。
     *
     * <p>R21: 第 3 级回退使用注入的 catalog——旧实现硬编码 defaultCatalog,自定义目录
     * 中档位的 walMode 默认(如 ASYNC)被静默替换为 DURABLE,耐久性/性能与配置不符。</p>
     */
    static WalMode resolveWalMode(PrecisionTierCatalog catalog, Intent intent, AckMode ackMode) {
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
        // 3. Fall back to tier default (injected catalog)
        return catalog.walMode(intent.getPrecisionTier());
    }

    /**
     * 投递路径状态变更持久化的守卫门面(round 15;round 16 判据收口至
     * {@link WheelPersistence#writeFreshUnderColdLock}):per-id 冷锁内复核 revision 后落盘。
     * StatePersistence.persistStateChange 已先 incrementRevision——writeRev = 当前 revision
     * (已含本次递增),判据单点见原语 javadoc:磁盘已到/超过本次写入 revision 说明内存副本
     * stale(在途投递副本 + 并发冷写者推进),直写会与磁盘权威槽同 revision 双写。stale 时
     * <b>跳过持久化</b>并
     * warn:磁盘保留冷写者的最新态(权威;cancelCold 竞态子情形下权威为 CANCELED 终态槽,
     * 不再投递),崩溃后按其恢复——优于双写平局。skip 同时回滚本次 increment
     * ({@code rollbackRevision(revision - 1)},round 15 终审 F3):否则平局副本被当 fresh
     * (revision 洗白),下次重试/热更新以 stale 内容确定性覆盖冷写者更新;回退后活对象
     * revision 回 base,再 increment 仍命中守卫恒 skip,W1-W3 守卫仍判 stale → 委托冷路径。
     *
     * <p><b>F6(round 15 终审)单一守卫门</b>:本方法即守卫本体——曾并存零调用的裸写口与
     * 独立的守卫变体,裸写口可绕过 stale 复核直达写槽,已合并:守卫实现移入本方法、守卫变体
     * 删除。改名理由:门面只应存在一个持久化写口。投递重试/改期路径(LoomqEngine 的
     * StateChangeSink.persist)经此落盘。
     * 终态路径(persistTerminalInPlace)不守卫(不加复核/不 skip——终态必落盘;其平局窗口经
     * 冷锁互斥关闭,见 WheelPersistence.persistTerminalInPlace)。重试/改期路径无
     * reclaimTerminal 交互,skip 无回收副作用。</p>
     *
     * <p>锁序:调用方(StatePersistence.persistStateChange)在 synchronized(intent) 内调用——
     * intent 监视器 → 冷锁,与命令族守卫同序,无死锁环。</p>
     */
    public void persistStateChangePutOnly(Intent intent) {
        String intentId = intent.getIntentId();
        // StatePersistence.persistStateChange 已预 increment → writeRev = 当前 revision;
        // 守卫判据单一成文于 WheelPersistence.writeFreshUnderColdLock(round 16 原语收口)。
        boolean fresh = persistence.writeFreshUnderColdLock(intentId, intent.getRevision(),
            () -> persistence.persistStateChangePutOnly(intent));
        if (!fresh) {
            logger.warn("Delivery state-change persist skipped (stale copy, disk advanced): id={}, rev={}",
                intentId, intent.getRevision());
            // F3(round 15 终审): 回退 StatePersistence 已做的 increment,把活对象 revision
            // 还原到 base——杜绝 skip 后平局副本被当 fresh 的 revision 洗白。
            intent.rollbackRevision(intent.getRevision() - 1);
        }
    }

    /**
     * 终态原地覆写（非阻塞 mmap）：把 locationIndex 指向的最新槽覆写为终态，不追加新槽。
     * 单槽 Intent 排队待回收（WheelPersistence 内部 pendingReclaims 队列），落盘后由 reclaimTerminal 清空；多槽只覆写不回收。
     * 无索引或 tail 时回退追加（tail 超视界非回收路径）。失败由调用方按 I6 吞掉。
     */
    public void persistTerminalInPlace(Intent intent) {
        persistence.persistTerminalInPlace(intent);
    }

    /**
     * 终态槽回收（须在 awaitCommit 之后调用）：先清 locationIndex（终态不索引，杜绝 freed 槽被
     * stale 别名误复用），单槽清空入 free-list 复用；多槽保留 tombstone。无论单多槽都清理
     * WheelPersistence 内部的多槽登记（multiSlotIntents），避免集合泄漏。
     */
    public void reclaimTerminal(String intentId) {
        persistence.reclaimTerminal(intentId);
    }

    /** 恢复期由 WheelRecovery 注入:磁盘上幸存槽数 >1 的 Intent 视为多槽(终态保留墓碑不回收)。 */
    public void markMultiSlot(Set<String> ids) {
        persistence.markMultiSlot(ids);
    }

    /** 恢复期由 WheelRecovery 注入:intentId → 磁盘历史最高 revision(含终态墓碑与跨视界 tail)。 */
    public void markMaxRevisions(Map<String, Long> revisions) {
        persistence.markMaxRevisions(revisions);
    }

    /**
     * 阻塞到覆盖最近一次 put 的 group-commit frontier 落盘。
     * 安全地在 synchronized(intent) 之外调用（此时 VT 可正常 unmount，不 pin carrier）。
     */
    public void awaitDurableCommit() {
        persistence.awaitDurableCommit();
    }

    private void ensureRunning() {
        if (!running.get()) {
            throw new IllegalStateException("Engine is not running");
        }
    }
}
