package com.loomq.application.scheduler;

import com.loomq.common.MetricsCollector;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.domain.intent.PrecisionTierCatalog;
import com.loomq.spi.DefaultRedeliveryDecider;
import com.loomq.spi.DeliveryHandler;
import com.loomq.spi.IntentObserver;
import com.loomq.spi.RedeliveryDecider;
import com.loomq.store.IntentStore;
import com.loomq.tracing.IntentTraceStore;
import java.time.Duration;
import java.time.Instant;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 精度调度器：调度入口与生命周期。
 *
 * 支持多精度档位的 Intent 调度，每个档位独立的扫描线程。
 * 核心架构：cohort 批量唤醒（CSA）+ 分层 Bucket 扫描。
 * 扫描/消费/结算分别由 ScanCoordinator/DispatchPipeline/SettlementEngine 承担。
 *
 * <h2>核心不变量</h2>
 * <ul>
 *   <li><b>I1 单持有方</b>: 任一 intentId 同一时刻最多存在于一个调度结构（bucket / cohort / 派发队列 / promotion cohort）</li>
 *   <li><b>I2 持久化先于承诺</b>: 状态对调用方可见前必须已对磁盘可见（DURABLE 落盘;I6 容错下终态持久化失败降级为 best-effort,onDelivered 仍触发）</li>
 *   <li><b>I3 revision 单调 + 终态不可逆</b>: 非终态 append + 终态原地覆写,recovery 按 max-revision 去重的基础</li>
 *   <li><b>I4 索引即所有权账本</b>: 认领必须经 {@code intentIndex} 原子摘除（CAS）</li>
 * </ul>
 *
 * @author loomq
 */
public class PrecisionScheduler {

    private static final Logger logger = LoggerFactory.getLogger(PrecisionScheduler.class);

    private final PrecisionTierCatalog precisionTierCatalog;
    private final BucketGroupManager bucketGroupManager;
    private final RedeliveryDecider redeliveryDecider;

    // Intent 生命周期观察器列表（线程安全）
    private final ObserverNotifier notifier = new ObserverNotifier();

    // 重试/backoff 默认值唯一来源（finalize 与失败结算路径共用）
    private final RetryPolicy retryPolicy = new RetryPolicy();

    // 共享虚拟线程池（所有档位共享）。非 final：stop() 后 start() 需重建（否则提交被拒）。
    private ExecutorService sharedExecutor;

    /**
     * 在途投递计数（含跨档借用的投递）。
     * stop() 排空的可靠依据：semaphore 只能感知本档 permit，借用他档 permit 的
     * 在途投递会被漏检，导致 sharedExecutor 提前关闭、完成回调被 reject、
     * ACK/重试决策丢失。dispatch 前 +1，finalize 任务结束 -1。
     * 依赖 precisionTierCatalog，须在构造器体 catalog 赋值后创建。
     */
    private final InFlightCounters inFlightCounters;

    /**
     * 状态变更持久化通道（StateChangeSink 包装，由 LoomqEngine 注入 DURABLE 落盘）。
     * 重试重排程是新的调度承诺而非中间态：必须落盘，否则崩溃恢复看到的是
     * 旧 executeAt 的 SCHEDULED 槽，重试链静默丢失。(I6 容错下持久化失败仅记 persistFailures,不阻塞调度)
     */
    private final StatePersistence persistence;

    // 投递结算引擎:终态化/重试/死信/过期 + 结算任务提交;消费循环经 DeliverySettlement 回调进入
    private final SettlementEngine settlementEngine;

    // 消费管线:有界队列 + permit 跨档借用 + 消费循环 + 背压
    private final DispatchPipeline pipeline;

    // 扫描协调器:adaptive 事件驱动扫描 + fixed-rate 扫描 + 过期分频检查 + pause 语义;
    // running 经 BooleanSupplier 注入(running 仍为本调度器 volatile 字段)
    private final ScanCoordinator scanCoordinator;

    // due→dispatch lag 追踪(由 DispatchLagTracker 登记/上报,结算路径清理)
    private final DispatchLagTracker lagTracker = new DispatchLagTracker();

    /**
     * 按 executeAt 索引活跃 intent（替代全量扫描）。scanner 遍历、结算路径摘除。
     */
    private final ExpiryIndex expiryIndex = new ExpiryIndex();

    // Cohort-based batched wakeup (CSA-inspired): replaces per-intent VT sleep
    private final CohortManager cohortManager;

    private volatile boolean running = false;

    // Metrics
    private final MetricsCollector metrics;
    private final IntentTraceStore traceStore;

    /**
     * 创建调度器（完整参数）。
     *
     * @param intentStore       Intent 存储
     * @param deliveryHandler   投递处理器（必须非 null）
     * @param redeliveryDecider 重投决策器（null 则使用默认）
     */
    public PrecisionScheduler(IntentStore intentStore, DeliveryHandler deliveryHandler, RedeliveryDecider redeliveryDecider) {
        this(intentStore, deliveryHandler, redeliveryDecider, null, new MetricsCollector(), new IntentTraceStore());
    }

    public PrecisionScheduler(IntentStore intentStore,
                              DeliveryHandler deliveryHandler,
                              RedeliveryDecider redeliveryDecider,
                              PrecisionTierCatalog precisionTierCatalog) {
        this(intentStore, deliveryHandler, redeliveryDecider, precisionTierCatalog, new MetricsCollector(), new IntentTraceStore());
    }

    public PrecisionScheduler(IntentStore intentStore,
                              DeliveryHandler deliveryHandler,
                              RedeliveryDecider redeliveryDecider,
                              PrecisionTierCatalog precisionTierCatalog,
                              MetricsCollector metricsCollector,
                              IntentTraceStore traceStore) {
        Objects.requireNonNull(deliveryHandler, "deliveryHandler must not be null");
        this.metrics = metricsCollector;
        this.traceStore = traceStore;
        this.precisionTierCatalog = precisionTierCatalog != null
            ? precisionTierCatalog
            : PrecisionTierCatalog.defaultCatalog();
        this.bucketGroupManager = new BucketGroupManager(this.precisionTierCatalog);

        // 加载重投决策器
        if (redeliveryDecider != null) {
            this.redeliveryDecider = redeliveryDecider;
        } else {
            ServiceLoader<RedeliveryDecider> deciderLoader = ServiceLoader.load(RedeliveryDecider.class);
            this.redeliveryDecider = deciderLoader.findFirst().orElseGet(DefaultRedeliveryDecider::new);
        }

        // 初始化共享虚拟线程池
        this.sharedExecutor = Executors.newVirtualThreadPerTaskExecutor();

        // 初始化在途投递计数
        this.inFlightCounters = new InFlightCounters(this.precisionTierCatalog.supportedTiers());

        // 持久化通道注入 MC:I6 容错计数(persistFailures)收口到中央导出(round 12)
        this.persistence = new StatePersistence(metricsCollector);

        // 投递结算引擎:叶子组件(持久化/观察器/重试/过期索引/lag/在途计数)与 redeliveryDecider
        // 解析均完成后创建;executor 由 start() 经 bindExecutor 注入。
        this.settlementEngine = new SettlementEngine(
            intentStore, persistence, notifier, retryPolicy, expiryIndex, lagTracker,
            inFlightCounters, metrics, traceStore, this.redeliveryDecider, this::schedule);

        // 消费管线:有界队列 + permit 跨档借用 + 消费循环 + 背压。running 为 volatile,
        // 经 BooleanSupplier 注入(消费者循环顶 running.getAsBoolean()),stop() 置 false 后自退。
        this.pipeline = new DispatchPipeline(deliveryHandler, this.precisionTierCatalog, metrics, traceStore,
            lagTracker, inFlightCounters, settlementEngine, () -> running);

        // 扫描协调器:扫描循环/过期检查/pause 语义。须先于 cohortManager 创建——
        // CohortManager 的 flush 回调 lambda 捕获 scanCoordinator 调用 triggerScan。
        this.scanCoordinator = new ScanCoordinator(this.precisionTierCatalog, this.bucketGroupManager,
            metrics, traceStore, lagTracker, expiryIndex, intentStore, pipeline, settlementEngine,
            notifier, () -> running);

        // Cohort-based batched wakeup (CSA-inspired): replaces per-intent VT sleep
        this.cohortManager = new CohortManager(this.bucketGroupManager, this.precisionTierCatalog,
            flushedIntents -> {
                Set<PrecisionTier> tiers = EnumSet.noneOf(PrecisionTier.class);
                for (Intent intent : flushedIntents) tiers.add(intent.getPrecisionTier());
                for (PrecisionTier tier : tiers) scanCoordinator.triggerScan(tier);
            }, this.metrics);
    }

    /**
     * 启动调度器
     */
    public void start() {
        if (running) return;
        if (sharedExecutor.isShutdown()) {
            // stop() 后重启：旧 executor 已关，重建共享虚拟线程池——
            // 否则 pipeline.start 的消费者 submit 被 RejectedExecutionException 打断，
            // running=true 却无消费者/无扫描，调度器半死不活。
            sharedExecutor = Executors.newVirtualThreadPerTaskExecutor();
        }
        // 结算引擎绑定共享执行器(须在 pipeline.start 之前——消费者仅在绑定后
        // 运行,结算提交路径不可达 null executor)。
        settlementEngine.bindExecutor(sharedExecutor);
        running = true;

        logger.info("PrecisionScheduler starting...");

        // 为每个精度档位启动独立的扫描循环(含残留触发标志清理与暂停态复位,见 ScanCoordinator.start)
        scanCoordinator.start();

        // 启动消费管线(档位级批量消费者;执行器共享,所有权仍归本调度器)
        pipeline.start(sharedExecutor);

        // 启动 cohort 批量唤醒器（CSA 风格：替代 per-intent 虚拟线程休眠）
        cohortManager.start();

        logger.info("PrecisionScheduler started with {} precision tiers", precisionTierCatalog.tierCount());
    }

    /**
     * 停止调度器
     */
    public void stop() {
        running = false;

        // 消费线程经 running 标志自退;显式 unpark 保底加速退出(消费者 park 上限 1ms)
        pipeline.stopConsumers();

        // 停止 cohort 唤醒器
        cohortManager.stop();

        // 停止全部扫描循环(unpark + join + cancel + shutdown,见 ScanCoordinator.stop)
        scanCoordinator.stop();

        // 排空 in-flight dispatch: 依据 per-tier 在途计数而非 semaphore——
        // 借用他档 permit 的投递 semaphore 感知不到,提前关 sharedExecutor 会让
        // 完成回调的 submit 被 reject,ACK/重试决策静默丢失。
        inFlightCounters.drainAll();

        // 关闭共享虚拟线程池
        sharedExecutor.shutdown();
        try {
            if (!sharedExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                sharedExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            sharedExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        logger.info("PrecisionScheduler stopped");
    }

    /**
     * 暂停调度器
     */
    public void pause() {
        scanCoordinator.pause();
    }

    /**
     * 恢复调度器
     */
    public void resume() {
        scanCoordinator.resume();
    }

    public boolean isPaused() {
        return scanCoordinator.isPaused();
    }

    /**
     * 调度 Intent
     *
     * 按 executeAt 与精度档位路由到对应桶或 cohort（批次唤醒）。
     *
     * @implNote 维护 I1：将 intent 放入唯一调度结构（bucket 或 cohort）。
     * @param intent Intent 实例
     */
    public void schedule(Intent intent) {
        // 接受 CREATED 或 SCHEDULED 状态的任务
        if (intent.getStatus() != IntentStatus.CREATED && intent.getStatus() != IntentStatus.SCHEDULED) {
            logger.warn("Cannot schedule intent {} with status {}",
                intent.getIntentId(), intent.getStatus());
            return;
        }

        // Trace: record intent creation (仅真正新建时——recordCreated 整体替换 trace,
        // 重排程重复调用会清空投递历史)。同 id 重建是新 incarnation:createdAt 不匹配时
        // 须刷新,否则新 intent 继承旧 createdAt/status(如 ACKED),lag 按旧值错算。
        // 记录 intent 自身的 createdAt(而非调度时刻),使同 incarnation 重排程与 trace 恒等。
        long createdAtMs = intent.getCreatedAt() != null
            ? intent.getCreatedAt().toEpochMilli()
            : System.currentTimeMillis();
        traceStore.recordCreatedIfNew(
            intent.getIntentId(), intent.getTraceId(), intent.getPrecisionTier(), createdAtMs);

        // I5: synchronized 内完成状态迁移 + 索引 + 路由 + 快照；
        // onScheduled 在锁外派发（不持有 intent 锁），但仍同步执行于调用线程。
        // 锁释放与锁外 schedule() 调用之间的交错窗口是良性的：
        // - BucketGroup.add() 的 intentIndex.compute() 原子覆盖同 intentId 旧条目
        // - scanDue CAS 捕获过期条目（revision 不匹配则跳过）
        // - CohortManager.remove 的 removeIf 清理所有匹配条目
        Consumer<IntentObserver> deferredNotify = null;
        synchronized (intent) {
            // 锁内终态复查 -- cancel 可能在锁释放与 schedule() 之间发生
            if (intent.getStatus() != IntentStatus.CREATED && intent.getStatus() != IntentStatus.SCHEDULED) {
                logger.debug("Skipping schedule for intent {} (status changed to {})",
                    intent.getIntentId(), intent.getStatus());
            } else {
                if (intent.getStatus() == IntentStatus.CREATED) {
                    intent.transitionTo(IntentStatus.SCHEDULED);
                }

                expiryIndex.index(intent);

                Instant executeAt = intent.getExecuteAt();
                if (executeAt == null) {
                    // null executeAt 会污染调度器(locate NPE、过期索引语义丢失);
                    // create 入口由 IntentValidator 拦截,此处兜底直连 API 调用。
                    throw new IllegalArgumentException(
                        "executeAt must not be null when scheduling intent " + intent.getIntentId());
                }
                Instant now = Instant.now();
                long delayMs = Duration.between(now, executeAt).toMillis();

                PrecisionTier tier = intent.getPrecisionTier();
                long precisionWindowMs = precisionTierCatalog.precisionWindowMs(tier);

                if (precisionTierCatalog.isDirectBucket(tier)) {
                    // MILLI：cohort 旁路直插桶（1ms 窗口下交接链延迟吃预算，整条消除）。
                    // 内存高水位降级 → 回退 cohort（精度劣化，指标计数）。
                    addToBucketWithFallback(intent, tier);
                } else if (delayMs <= 0) {
                    addToBucketAndDispatch(intent);
                } else if (delayMs > precisionWindowMs) {
                    // CSA-inspired: cohort-based batched wakeup replaces per-intent VT sleep
                    cohortManager.register(intent);
                } else {
                    // 短延迟：直接入桶（无需休眠，bucket 本身提供精度窗口）
                    addToBucketAndDispatch(intent);
                }

                logger.debug("Scheduled intent {} with tier {}, delay {}ms",
                    intent.getIntentId(), tier, delayMs);

                if (!notifier.isEmpty()) {
                    final Intent snapshot = intent.copy();
                    deferredNotify = o -> o.onScheduled(snapshot);
                }
            }
        }
        // I5: onScheduled 在锁外派发
        if (deferredNotify != null) {
            notifier.notifyObservers(deferredNotify);
        }
    }

    /**
     * 添加到桶并等待调度
     */
    private void addToBucketAndDispatch(Intent intent) {
        addToBucketWithFallback(intent, intent.getPrecisionTier());
    }

    /** 入桶收口:高水位 FALLBACK_TO_COHORT 时回退 cohort 并计数。
     *  降级结果不能丢弃——否则 intent 既不在桶也不在 cohort,静默丢失直到重启恢复。 */
    private void addToBucketWithFallback(Intent intent, PrecisionTier tier) {
        BucketGroup.AddResult r = bucketGroupManager.add(intent);
        if (r == BucketGroup.AddResult.FALLBACK_TO_COHORT) {
            cohortManager.register(intent);
            metrics.incrementMilliFallback(tier);
        }
    }

    /**
     * 恢复 Intent 到调度器。
     *
     * 恢复路径不走创建时的状态机约束，但保留 delay-based 路由：
     * 长延迟 Intent 进入 CohortManager（批量唤醒），短延迟直接入桶。
     */
    public void restore(Intent intent) {
        if (intent == null || intent.getExecuteAt() == null) {
            return;
        }
        // 恢复/重排程恢复路径补 trace(recordCreatedIfNew 幂等,与 schedule() 同款):
        // 重启恢复的热 Intent 若在 IntentTraceStore 无条目,后续 record 全为 no-op,
        // 与冷路径(promote 经 schedule)行为不一致;同 incarnation 重复 restore 由
        // createdAt 匹配守卫跳过,不刷新投递历史。
        long createdAtMs = intent.getCreatedAt() != null
            ? intent.getCreatedAt().toEpochMilli()
            : System.currentTimeMillis();
        traceStore.recordCreatedIfNew(
            intent.getIntentId(), intent.getTraceId(), intent.getPrecisionTier(), createdAtMs);
        // I5: synchronized + 终态复查 -- cancel 可能在锁释放与 restore() 之间发生。
        // 重复注册（并发 fireNow/reschedule）是良性的：BucketGroup.add() 原子覆盖，
        // scanDue CAS 去重，CohortManager.remove 的 removeIf 清理全部匹配条目。
        synchronized (intent) {
            if (intent.getStatus().isTerminal()) {
                logger.debug("Skipping restore for terminal intent {}", intent.getIntentId());
                return;
            }
            long delayMs = Duration.between(Instant.now(), intent.getExecuteAt()).toMillis();
            PrecisionTier tier = intent.getPrecisionTier();
            long precisionWindowMs = precisionTierCatalog.precisionWindowMs(tier);

            if (precisionTierCatalog.isDirectBucket(tier)) {
                addToBucketWithFallback(intent, tier);
            } else if (delayMs > precisionWindowMs) {
                cohortManager.register(intent);
            } else {
                addToBucketWithFallback(intent, tier);
            }
            expiryIndex.index(intent);
        }
    }

    /**
     * 从调度器中完全移除 Intent（包括 bucket 和 cohort）。
     *
     * @param intent 要移除的 Intent
     * @return true 如果从任一位置成功移除
     */
    public boolean removeFromSchedule(Intent intent) {
        if (intent == null) return false;
        expiryIndex.unindex(intent.getIntentId(), executeAtMs(intent));
        boolean removedFromBucket = bucketGroupManager.remove(intent);
        boolean removedFromCohort = cohortManager.remove(intent.getIntentId());
        return removedFromBucket || removedFromCohort;
    }

    /**
     * 从调度结构中移除指定 Intent，并使用指定的旧 executeAt 清理过期索引。
     *
     * <p>当 caller 已修改 intent.executeAt 但需要清理旧索引条目时使用此重载。</p>
     *
     * @param intent       要移除的 Intent
     * @param oldExecuteAt 索引中存储的旧 executeAt 时间
     * @return true 如果从任一位置成功移除
     */
    public boolean removeFromSchedule(Intent intent, Instant oldExecuteAt) {
        if (intent == null) return false;
        if (oldExecuteAt != null) {
            expiryIndex.unindex(intent.getIntentId(), oldExecuteAt.toEpochMilli());
        }
        boolean removedFromBucket = bucketGroupManager.remove(intent);
        boolean removedFromCohort = cohortManager.remove(intent.getIntentId());
        return removedFromBucket || removedFromCohort;
    }

    /** 诊断委托：permit 时序统计（消费管线持有）。 */
    public DispatchPipeline.PermitTimingStats getPermitTimingStats() { return pipeline.permitTimingStats(); }

    /** 诊断委托：跨档借用统计（消费管线持有）。 */
    public DispatchPipeline.BorrowStats getBorrowStats() { return pipeline.borrowStats(); }

    /** 检查档位是否处于背压状态（委托消费管线） */
    public boolean isTierUnderBackpressure(PrecisionTier tier) { return pipeline.isTierUnderBackpressure(tier); }

    /** 获取档位背压信息（委托消费管线） */
    public Map<PrecisionTier, DispatchPipeline.BackpressureInfo> getBackpressureStatus() { return pipeline.backpressureStatus(); }

    /** 诊断：最近的 finalize 异常样本（类名:消息），有界，供取证。 */
    public List<String> getFinalizeExceptionSamples() { return settlementEngine.finalizeExceptionSamples(); }

    public CohortManager getCohortManager() {
        return cohortManager;
    }

    /** 设置 Intent 生命周期观察器列表（由 LoomqEngine 调用） */
    public void setObservers(List<IntentObserver> observers) {
        notifier.setObservers(observers);
    }

    /**
     * 运行时新增观察器——立即生效(observer 列表为 CopyOnWriteArrayList;
     * LoomqEngine.start 时 setObservers 的快照不影响之后的运行时新增)。
     */
    public void addObserver(IntentObserver observer) {
        notifier.add(observer);
    }

    public void removeObserver(IntentObserver observer) {
        notifier.remove(observer);
    }

    /** 注入状态变更持久化通道(接到 IntentCommandService 的 DURABLE 落盘)。 */
    public void setStateChangeSink(StateChangeSink sink) {
        persistence.setSink(sink);
    }

    private static long executeAtMs(Intent intent) {
        return intent.getExecuteAt() != null ? intent.getExecuteAt().toEpochMilli() : 0L;
    }

    /**
     * 获取桶组管理器
     */
    public BucketGroupManager getBucketGroupManager() {
        return bucketGroupManager;
    }

    public PrecisionTierCatalog getPrecisionTierCatalog() {
        return precisionTierCatalog;
    }
}
