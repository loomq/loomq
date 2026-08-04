package com.loomq.application.scheduler;

import com.loomq.common.MetricsCollector;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.domain.intent.PrecisionTierCatalog;
import com.loomq.spi.DefaultRedeliveryDecider;
import com.loomq.spi.DeliveryHandler;
import com.loomq.spi.DeliveryHandler.DeliveryResult;
import com.loomq.spi.IntentObserver;
import com.loomq.spi.RedeliveryDecider;
import com.loomq.store.IntentStore;
import com.loomq.tracing.IntentTraceStore;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 精度调度器。
 *
 * 支持多精度档位的 Intent 调度，每个档位独立的扫描线程。
 * 核心架构：虚拟线程独立休眠 + 分层 Bucket 唤醒。
 *
 * <h2>核心不变量</h2>
 * <ul>
 *   <li><b>I1 单持有方</b>: 任一 intentId 同一时刻最多存在于一个调度结构（bucket / cohort / 派发队列 / promotion cohort）</li>
 *   <li><b>I2 持久化先于承诺</b>: 状态对调用方可见前必须已对磁盘可见（DURABLE 落盘）</li>
 *   <li><b>I3 revision 单调 + 终态不可逆</b>: append-only + max-revision 去重的基础</li>
 *   <li><b>I4 索引即所有权账本</b>: 认领必须经 {@code intentIndex} 原子摘除（CAS）</li>
 * </ul>
 *
 * @author loomq
 */
public class PrecisionScheduler {

    private static final Logger logger = LoggerFactory.getLogger(PrecisionScheduler.class);

    private static final long BACKPRESSURE_LOG_INTERVAL_MS = 1000;
    // AdapTBF constraints: max lend ratio per tier (protects low-priority tiers)
    private static final double MAX_LEND_RATIO = 0.5; // lend at most 50% of tier's slots
    // 投递超时(单条/批量统一);后续如需可配,移入 PrecisionTierCatalog
    private static final long DELIVERY_TIMEOUT_SECONDS = 30;

    private final IntentStore intentStore;
    private final PrecisionTierCatalog precisionTierCatalog;
    private final BucketGroupManager bucketGroupManager;
    private final DeliveryHandler deliveryHandler;
    private final RedeliveryDecider redeliveryDecider;

    // Intent 生命周期观察器列表（线程安全）
    private final List<IntentObserver> observers = new CopyOnWriteArrayList<>();

    // 共享虚拟线程池（所有档位共享）
    private final ExecutorService sharedExecutor;

    // 档位级并发控制（可动态调整上限）
    private final Map<PrecisionTier, ResizableSemaphore> tierSemaphores;

    /**
     * 在途投递计数（含跨档借用的投递）。
     * stop() 排空的可靠依据：semaphore 只能感知本档 permit，借用他档 permit 的
     * 在途投递会被漏检，导致 sharedExecutor 提前关闭、完成回调被 reject、
     * ACK/重试决策丢失。dispatch 前 +1，finalize 任务结束 -1。
     */
    private final Map<PrecisionTier, AtomicInteger> tierInFlight;

    /**
     * 状态变更持久化钩子（由 LoomqEngine 注入，接到 IntentCommandService 的 DURABLE 落盘）。
     * 重试重排程是新的调度承诺而非中间态：必须落盘，否则崩溃恢复看到的是
     * 旧 executeAt 的 SCHEDULED 槽，重试链静默丢失。
     */
    private volatile Consumer<Intent> stateChangePersister;

    // 档位级有界队列（容量 = maxConcurrency × 4，满时触发 backpressure）
    private final Map<PrecisionTier, BlockingQueue<Intent>> tierDispatchQueues;

    // 按精度档位的扫描调度器
    private final Map<PrecisionTier, ScheduledExecutorService> scanSchedulers;
    private final Map<PrecisionTier, ScheduledFuture<?>> scanFutures;

    // scanTrigger 去重：同一 tier 同时最多一个待执行的 triggered scan
    private final Map<PrecisionTier, AtomicBoolean> pendingScanTrigger = new ConcurrentHashMap<>();

    // due→dispatch lag 追踪（key=intentId, value=enqueueTimeNanos）
    private final ConcurrentHashMap<String, Long> enqueueTimeNanos = new ConcurrentHashMap<>();

    // 过期检查分频计数器
    private final Map<PrecisionTier, AtomicLong> expiredCheckCounters;

    /**
     * 按 executeAt 索引活跃 intent（替代全量扫描）。
     *
     * 数据结构：ConcurrentSkipListMap&lt;epochMs, Set&lt;intentId&gt;&gt;
     * 每个唯一的毫秒时间戳创建一个 HashSet。在大多数生产场景下，
     * intent 的 executeAt 会聚集在有限的时间桶中（秒级精度），
     * 因此 HashSet 数量远小于 intent 总数。
     *
     * 极端情况（每个 intent 有唯一毫秒时间戳）：内存 ≈ intent数 × (16B set头 + 指针)。
     * 对于百万级 intent，约 32MB 额外开销，可接受。
     */
    private final ConcurrentSkipListMap<Long, Set<String>> intentExpiryIndex = new ConcurrentSkipListMap<>();

    // Cohort-based batched wakeup (CSA-inspired): replaces per-intent VT sleep
    private final CohortManager cohortManager;

    // 限频日志时间戳
    private final AtomicLong lastBackpressureLogTimeMs = new AtomicLong(0);

    // Arrow-inspired cross-tier slot borrowing metrics
    private final BorrowStats borrowStats = new BorrowStats();

    /**
     * 异步投递异常处理。
     *
     * 注意：onDeliveryFailed 通知在 retry/dead-letter 决策之前触发。
     * 观察器接收到的是原始投递失败事件，此时 intent 状态尚未被修改。
     * 观察器不应依赖 intent 状态来推断调度器的后续决策。
     */
    private void handleDeliveryException(Intent intent, PrecisionTier tier, Throwable ex) {
        notifyObservers(o -> o.onDeliveryFailed(intent, ex));
        if (ex instanceof java.util.concurrent.TimeoutException) {
            logger.warn("Delivery timeout for intent {}", intent.getIntentId());
        } else {
            logger.error("Delivery exception for intent {}: {}", intent.getIntentId(), ex.getMessage(), ex);
        }
        handleDeliveryFailure(intent);
    }

    // Permit timing diagnostics
    private final PermitTimingStats permitTimingStats = new PermitTimingStats();
    public static class PermitTimingStats {
        public final AtomicLong totalAcquireWaitNanos = new AtomicLong(0);
        public final AtomicLong totalPermitHoldNanos = new AtomicLong(0);
        public final AtomicLong totalDeliverAsyncNanos = new AtomicLong(0);
        public final AtomicLong totalBlockingWaitNanos = new AtomicLong(0);
        public final AtomicInteger deliverySampleCount = new AtomicInteger(0);
        public final AtomicInteger blockingWaitCount = new AtomicInteger(0);

        public double avgAcquireWaitMs() {
            int n = deliverySampleCount.get();
            return n > 0 ? (totalAcquireWaitNanos.get() / (double) n) / 1_000_000.0 : 0;
        }
        public double avgPermitHoldMs() {
            int n = deliverySampleCount.get();
            return n > 0 ? (totalPermitHoldNanos.get() / (double) n) / 1_000_000.0 : 0;
        }
        public double avgDeliverAsyncUs() {
            int n = deliverySampleCount.get();
            return n > 0 ? (totalDeliverAsyncNanos.get() / (double) n) / 1_000.0 : 0;
        }
        public double avgBlockingWaitMs() {
            int n = blockingWaitCount.get();
            return n > 0 ? (totalBlockingWaitNanos.get() / (double) n) / 1_000_000.0 : 0;
        }
        public void reset() {
            totalAcquireWaitNanos.set(0);
            totalPermitHoldNanos.set(0);
            totalDeliverAsyncNanos.set(0);
            totalBlockingWaitNanos.set(0);
            deliverySampleCount.set(0);
            blockingWaitCount.set(0);
        }
    }

    public PermitTimingStats getPermitTimingStats() { return permitTimingStats; }

    private volatile boolean running = false;
    private volatile boolean paused = false;

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
        this.intentStore = intentStore;
        this.deliveryHandler = Objects.requireNonNull(deliveryHandler, "deliveryHandler must not be null");
        this.metrics = metricsCollector;
        this.traceStore = traceStore;
        this.precisionTierCatalog = precisionTierCatalog != null
            ? precisionTierCatalog
            : PrecisionTierCatalog.defaultCatalog();
        this.bucketGroupManager = new BucketGroupManager(this.precisionTierCatalog);
        this.scanSchedulers = new ConcurrentHashMap<>();
        this.scanFutures = new ConcurrentHashMap<>();
        this.cohortManager = new CohortManager(this.bucketGroupManager, this.precisionTierCatalog, flushedIntents -> {
            Set<PrecisionTier> tiers = EnumSet.noneOf(PrecisionTier.class);
            for (Intent intent : flushedIntents) {
                tiers.add(intent.getPrecisionTier());
            }
            for (PrecisionTier tier : tiers) {
                AtomicBoolean pending = pendingScanTrigger.computeIfAbsent(tier, k -> new AtomicBoolean(false));
                if (pending.compareAndSet(false, true)) {
                    ScheduledExecutorService scanScheduler = scanSchedulers.get(tier);
                    if (scanScheduler != null) {
                        scanScheduler.submit(() -> {
                            pending.set(false);
                            scanAndDispatch(tier);
                        });
                    }
                }
            }
        }, this.metrics);

        // 加载重投决策器
        if (redeliveryDecider != null) {
            this.redeliveryDecider = redeliveryDecider;
        } else {
            ServiceLoader<RedeliveryDecider> deciderLoader = ServiceLoader.load(RedeliveryDecider.class);
            this.redeliveryDecider = deciderLoader.findFirst().orElseGet(DefaultRedeliveryDecider::new);
        }

        // 初始化共享虚拟线程池
        this.sharedExecutor = Executors.newVirtualThreadPerTaskExecutor();

        // 初始化档位级信号量和队列
        this.tierSemaphores = new EnumMap<>(PrecisionTier.class);
        this.tierDispatchQueues = new EnumMap<>(PrecisionTier.class);
        this.tierInFlight = new EnumMap<>(PrecisionTier.class);
        this.expiredCheckCounters = new EnumMap<>(PrecisionTier.class);

        for (PrecisionTier tier : this.precisionTierCatalog.supportedTiers()) {
            tierSemaphores.put(tier, new ResizableSemaphore(this.precisionTierCatalog.maxConcurrency(tier)));
            int queueCapacity = this.precisionTierCatalog.dispatchQueueCapacity(tier);
            tierDispatchQueues.put(tier, new ArrayBlockingQueue<>(queueCapacity));
            expiredCheckCounters.put(tier, new AtomicLong(0));
            tierInFlight.put(tier, new AtomicInteger(0));
        }

        if (this.deliveryHandler == null) {
            logger.warn("No DeliveryHandler configured - intents will not be delivered!");
        }
    }

    /**
     * 启动调度器
     */
    public void start() {
        if (running) return;
        running = true;

        logger.info("PrecisionScheduler starting...");

        // 为每个精度档位启动独立的扫描循环
        for (PrecisionTier tier : precisionTierCatalog.supportedTiers()) {
            startScanCycle(tier);
        }

        // 启动档位级批量消费者
        for (PrecisionTier tier : precisionTierCatalog.supportedTiers()) {
            startBatchConsumers(tier);
        }

        // 启动 cohort 批量唤醒器（CSA 风格：替代 per-intent 虚拟线程休眠）
        cohortManager.start();

        logger.info("PrecisionScheduler started with {} precision tiers", precisionTierCatalog.tierCount());
    }

    /**
     * 启动指定档位的批量消费者
     */
    private void startBatchConsumers(PrecisionTier tier) {
        int consumerCount = precisionTierCatalog.consumerCount(tier);

        for (int i = 0; i < consumerCount; i++) {
            final int consumerId = i;
            sharedExecutor.submit(() -> {
                Thread.currentThread().setName("batch-consumer-" + tier.name().toLowerCase() + "-" + consumerId);
                runBatchConsumer(tier);
            });
        }

        logger.info("Started {} batch consumers for tier {}", consumerCount, tier);
    }

    /**
     * 启动指定精度档位的扫描任务
     */
    private void startScanCycle(PrecisionTier tier) {
        long intervalMs = precisionTierCatalog.scanIntervalMs(tier);

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "scan-" + tier.name().toLowerCase());
            t.setDaemon(true);
            return t;
        });
        scanSchedulers.put(tier, scheduler);

        ScheduledFuture<?> future = scheduler.scheduleAtFixedRate(
            () -> scanAndDispatch(tier),
            intervalMs,  // 初始延迟
            intervalMs,  // 扫描间隔
            TimeUnit.MILLISECONDS
        );

        scanFutures.put(tier, future);
        logger.info("Started scan cycle for tier {} with interval {}ms", tier, intervalMs);
    }

    /**
     * 停止调度器
     */
    public void stop() {
        running = false;

        // 停止 cohort 唤醒器
        cohortManager.stop();

        // 取消所有扫描循环
        for (ScheduledFuture<?> future : scanFutures.values()) {
            future.cancel(false);
        }
        scanFutures.clear();

        // 关闭所有扫描调度器
        for (ScheduledExecutorService scheduler : scanSchedulers.values()) {
            scheduler.shutdown();
        }
        scanSchedulers.clear();

        // 排空 in-flight dispatch: 依据 per-tier 在途计数而非 semaphore——
        // 借用他档 permit 的投递 semaphore 感知不到,提前关 sharedExecutor 会让
        // 完成回调的 submit 被 reject,ACK/重试决策静默丢失。
        long drainDeadlineNs = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        for (Map.Entry<PrecisionTier, AtomicInteger> entry : tierInFlight.entrySet()) {
            PrecisionTier tier = entry.getKey();
            AtomicInteger inFlight = entry.getValue();
            while (inFlight.get() > 0) {
                if (System.nanoTime() > drainDeadlineNs) {
                    logger.warn("Tier {} has {} intents still in-flight after drain timeout",
                        tier, inFlight.get());
                    break;
                }
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
            }
        }

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
        if (!paused) {
            paused = true;
            logger.info("PrecisionScheduler paused");
        }
    }

    /**
     * 恢复调度器
     */
    public void resume() {
        if (paused) {
            paused = false;
            logger.info("PrecisionScheduler resumed");
        }
    }

    public boolean isPaused() {
        return paused;
    }

    /**
     * 调度 Intent
     *
     * 根据 executeAt 和 precisionTier 计算休眠时间，然后添加到对应桶。
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

        // Trace: record intent creation
        traceStore.recordCreated(
            intent.getIntentId(), intent.getTraceId(), intent.getPrecisionTier());

        Instant executeAt = intent.getExecuteAt();
        Instant now = Instant.now();
        long delayMs = Duration.between(now, executeAt).toMillis();

        PrecisionTier tier = intent.getPrecisionTier();
        long precisionWindowMs = precisionTierCatalog.precisionWindowMs(tier);

        // Transition to SCHEDULED if still in CREATED state
        if (intent.getStatus() == IntentStatus.CREATED) {
            intent.transitionTo(IntentStatus.SCHEDULED);
        }

        notifyObservers(o -> o.onScheduled(intent));
        indexIntent(intent);

        if (delayMs <= 0) {
            // 已到期，直接投递
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
    }

    /**
     * 添加到桶并等待调度
     */
    private void addToBucketAndDispatch(Intent intent) {
        bucketGroupManager.add(intent);
    }

    /**
     * 从调度桶中移除 Intent。
     *
     * @param intent Intent 实例
     */
    public void unschedule(Intent intent) {
        bucketGroupManager.remove(intent);
    }

    /**
     * 恢复 Intent 到调度器。
     *
     * 恢复路径不走创建时的状态机约束，但保留 delay-based 路由：
     * 长延迟 Intent 进入 CohortManager（批量唤醒），短延迟直接入桶。
     */
    public void restore(Intent intent) {
        if (intent == null || intent.getExecuteAt() == null || intent.getStatus().isTerminal()) {
            return;
        }

        long delayMs = Duration.between(Instant.now(), intent.getExecuteAt()).toMillis();
        PrecisionTier tier = intent.getPrecisionTier();
        long precisionWindowMs = precisionTierCatalog.precisionWindowMs(tier);

        if (delayMs > precisionWindowMs) {
            cohortManager.register(intent);
        } else {
            bucketGroupManager.add(intent);
        }
        indexIntent(intent);
    }

    /**
     * 从调度器中完全移除 Intent（包括 bucket 和 cohort）。
     *
     * @param intent 要移除的 Intent
     * @return true 如果从任一位置成功移除
     */
    public boolean removeFromSchedule(Intent intent) {
        if (intent == null) return false;
        unindexIntent(intent.getIntentId(), executeAtMs(intent));
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
            unindexIntent(intent.getIntentId(), oldExecuteAt.toEpochMilli());
        }
        boolean removedFromBucket = bucketGroupManager.remove(intent);
        boolean removedFromCohort = cohortManager.remove(intent.getIntentId());
        return removedFromBucket || removedFromCohort;
    }

    /**
     * 用 committed state 重建调度器状态。
     *
     * 该方法会清空当前桶、cohort 和过期索引，再按 store 里的当前态
     * 重新挂载所有非终态 intent。适用于快照后的调度重建。
     */
    public void rebuildFromCommittedState(Collection<Intent> intents) {
        bucketGroupManager.clear();
        intentExpiryIndex.clear();
        cohortManager.clear();

        if (intents == null || intents.isEmpty()) {
            return;
        }

        for (Intent intent : intents) {
            if (intent == null || intent.getExecuteAt() == null || intent.getStatus().isTerminal()) {
                continue;
            }
            restore(intent);
        }
    }

    /**
     * 扫描并投递指定精度档位的到期任务
     */
    private void scanAndDispatch(PrecisionTier tier) {
        if (paused) return;

        long startTime = System.nanoTime();
        Instant now = Instant.now();
        BucketGroup group = resolveBucketGroup(tier);

        try {
            List<Intent> dueIntents = group.scanDue(now);

            if (!dueIntents.isEmpty()) {
                logger.debug("Found {} due intents for tier {}", dueIntents.size(), tier);

                // Batch metrics: single addAndGet instead of N incrementAndGet
                metrics.addIntentDueByTier(tier, dueIntents.size());

                // Batch enqueue timestamp: one nanoTime for the entire scan cycle
                long batchEnqueueNanos = System.nanoTime();

                for (Intent intent : dueIntents) {
                    // 记录唤醒延迟
                    recordWakeupLatency(intent, now);

                    // 追踪入队时间（用于 due→dispatch lag 计算）
                    enqueueTimeNanos.put(intent.getIntentId(), batchEnqueueNanos);

                    // Trace: record enqueued
                    traceStore.recordEnqueued(intent.getIntentId());

                    // 提交到档位队列（有界，带短重试）
                    BlockingQueue<Intent> queue = tierDispatchQueues.get(tier);
                    boolean offered = false;
                    int offerRetries = 0;
                    while (!offered && offerRetries < 3) {
                        offered = queue.offer(intent);
                        if (!offered) {
                            offerRetries++;
                            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                        }
                    }
                    if (!offered) {
                        metrics.incrementDispatchQueueOfferFailed(tier);
                        metrics.incrementBackpressureEvent(tier);
                        logger.error("CRITICAL: Backpressure — dispatch queue full for tier {}, requeuing intent {} for next scan cycle",
                            tier, intent.getIntentId());
                        notifyObservers(o -> o.onDeliveryFailed(intent,
                            new com.loomq.common.exception.BackPressureException(
                                "Dispatch queue full for tier " + tier, null, 1000)));
                        // 重新放回调度结构等待下次 scan cycle（intent 仍为 SCHEDULED 状态，无需回退）
                        addToBucketAndDispatch(intent);
                    }
                }

            }

            // 更新桶大小指标
            metrics.updateBucketSizeByTier(tier, group.getPendingCount());

            // 更新队列深度指标
            BlockingQueue<Intent> queue = tierDispatchQueues.get(tier);
            metrics.updateDispatchQueueSizeByTier(tier, queue.size());

            // 检查过期任务（分频）
            if (shouldCheckExpired(tier)) {
                checkExpiredIntents(tier);
            }

        } catch (Exception e) {
            // 扫描循环安全网：单个 tier 的异常不应杀死整个扫描线程
            logger.error("Error scanning tier {}", tier, e);
        }

        // 记录扫描耗时
        long durationMs = (System.nanoTime() - startTime) / 1_000_000;
        metrics.recordScanDurationByTier(tier, durationMs);
    }

    /**
     * 限频日志：每秒最多 1 条，避免 I/O 阻塞扫描线程
     */
    private void logRateLimited(String format, Object... args) {
        long now = System.currentTimeMillis();
        long last = lastBackpressureLogTimeMs.get();
        if (now - last >= BACKPRESSURE_LOG_INTERVAL_MS && lastBackpressureLogTimeMs.compareAndSet(last, now)) {
            logger.warn(format, args);
        }
    }

    /**
     * 判断当前 scan cycle 是否需要执行过期检查（分频策略）
     *
     * ULTRA/FAST: 每 cycle（延迟敏感）
     * HIGH: 每 3 cycle（最大过期延迟 400ms）
     * STANDARD: 每 5 cycle（最大过期延迟 3000ms）
     * ECONOMY: 每 10 cycle（最大过期延迟 11000ms）
     */
    private boolean shouldCheckExpired(PrecisionTier tier) {
        long count = expiredCheckCounters.get(tier).incrementAndGet();
        int interval = switch (tier) {
            case ULTRA, FAST -> 1;
            case HIGH -> 3;
            case STANDARD -> 5;
            case ECONOMY -> 10;
        };
        return count % interval == 0;
    }

    /**
     * 投递消费者循环 (真正 fire-and-forget)。
     *
     * Semaphore 控制 in-flight 并发。消费者只负责取任务 + 调用异步投递，
     * permit 在 Netty/异步回调中释放。消费者线程与 HTTP 往返完全解耦。
     */
    private void runBatchConsumer(PrecisionTier tier) {
        BlockingQueue<Intent> queue = tierDispatchQueues.get(tier);
        int batchSize = precisionTierCatalog.batchSize(tier);
        int batchWindowMs = precisionTierCatalog.batchWindowMs(tier);

        if (batchSize <= 1) {
            // Single-intent mode (ULTRA, FAST)
            runSingleIntentConsumer(tier, queue);
        } else {
            // Batch mode (HIGH, STANDARD, ECONOMY)
            runBatchDrainConsumer(tier, queue, batchSize, batchWindowMs);
        }
    }

    /**
     * 单 Intent 消费循环（batchSize == 1）。
     * 流程：acquire → poll(1) → I5 快照(synchronized+copy) → deliver(snapshot) → release in callback。
     */
    private void runSingleIntentConsumer(PrecisionTier tier, BlockingQueue<Intent> queue) {
        while (running) {
            long acquireStartNs = System.nanoTime();
            ResizableSemaphore acquired;
            try {
                acquired = acquireWithBorrow(tier);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            long acquireEndNs = System.nanoTime();

            Intent intent = queue.poll();
            if (intent == null) {
                releasePermit(tier, acquired);   // Fix 7: 空 poll 释放同样配对 decrementBorrowed
                long parkNanos = switch (tier) {
                    case ULTRA -> 100_000L;    // 100μs (10ms precision window ≈ 1%)
                    case FAST  -> 200_000L;    // 200μs
                    default    -> 1_000_000L;  // 1ms
                };
                LockSupport.parkNanos(parkNanos);
                continue;
            }

            // I5: 派发即快照——在 synchronized(intent) 内原子完成终态复查 + 防御性拷贝。
            // 快照与 cancel/update 互斥：要么取自变更前的完整状态，要么复查拦截（不投递）。
            // 撕裂读在结构上消除；SPI 边界只过快照，用户代码无法触达内核活状态。
            Intent snapshot;
            synchronized (intent) {
                if (intent.getStatus().isTerminal()) {
                    enqueueTimeNanos.remove(intent.getIntentId());
                    releasePermit(tier, acquired);
                    continue;
                }
                snapshot = intent.copy();
            }

            // Trace: record dequeued (right after pollFirst, before any other processing)
            traceStore.recordDequeued(intent.getIntentId());
            recordDispatchQueueLag(intent, tier);

            // Only accumulate acquire wait for actual deliveries so the average
            // denominator (deliverySampleCount) matches.
            permitTimingStats.totalAcquireWaitNanos.addAndGet(acquireEndNs - acquireStartNs);
            final long permitAcquiredNs = acquireEndNs;

            // Fix 5: 在途计数(含借用),stop() 据此排空而非 semaphore
            tierInFlight.get(tier).incrementAndGet();

            long deliverStartNs = System.nanoTime();
            try {
                deliveryHandler.deliverAsync(snapshot)
                    .orTimeout(DELIVERY_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .whenComplete((result, ex) -> {
                        long releaseNs = System.nanoTime();

                        permitTimingStats.totalPermitHoldNanos.addAndGet(releaseNs - permitAcquiredNs);
                        permitTimingStats.deliverySampleCount.incrementAndGet();

                        // Release permit before finalizeIntent to maximize concurrency
                        releasePermit(tier, acquired);

                        // Defer finalization off the event loop; Fix 5: 末尾 -1 在途计数
                        submitFinalize(intent, tier, () -> {
                            if (ex != null) {
                                handleDeliveryException(intent, tier, ex);
                            } else {
                                finalizeIntent(intent, tier, result);
                            }
                        });
                    });
            } catch (RuntimeException deliverEx) {
                // P1-9: SPI 同步抛异常会杀死消费者 VT——按失败结算,消费者继续存活
                releasePermit(tier, acquired);
                submitFinalize(intent, tier, () -> handleDeliveryException(intent, tier, deliverEx));
            }
            permitTimingStats.totalDeliverAsyncNanos.addAndGet(System.nanoTime() - deliverStartNs);
        }
    }

    /**
     * 批量消费循环（batchSize > 1）。
     *
     * 流程：
     * 1. drainTo(batch, batchSize) — 一次性取出最多 batchSize 个 intent
     * 2. 若队列为空，poll 等首个 intent，再 drain 剩余
     * 3. I5: synchronized(intent) 终态复查 + copy, 构建 liveBatch + snapshotBatch
     * 4. 为 liveBatch 中每个 intent acquire 一个信号量 permit
     * 5. 调用 deliverBatchAsync(snapshotBatch)
     * 6. 在回调中释放所有 permit + finalize(liveBatch)
     */
    private void runBatchDrainConsumer(PrecisionTier tier, BlockingQueue<Intent> queue,
                                       int batchSize, int batchWindowMs) {
        while (running) {
            List<Intent> batch = new ArrayList<>(batchSize);

            // Phase 1: drain what's immediately available
            queue.drainTo(batch, batchSize);

            if (batch.isEmpty()) {
                // Phase 2: non-blocking poll for first intent
                Intent first = queue.poll();
                if (first == null) {
                    // Queue is truly empty — park briefly and retry
                    long parkNanos = switch (tier) {
                        case ULTRA -> 100_000L;    // 100μs
                        case FAST  -> 200_000L;    // 200μs
                        default    -> 1_000_000L;  // 1ms
                    };
                    LockSupport.parkNanos(parkNanos);
                    continue;
                }
                batch.add(first);
                // Drain remaining (non-blocking) up to batchSize
                queue.drainTo(batch, batchSize - batch.size());
            }

            // I5: 派发即快照--在 synchronized(intent) 内原子完成终态复查 + 防御性拷贝。
            // 构建 liveBatch（结算用活对象）和 snapshotBatch（SPI 用快照）并行列表。
            // 许可获取在确定非终态数量之后，同时修掉“先申请再剥除终态”的 permit 临时过配问题。
            List<Intent> liveBatch = new ArrayList<>(batch.size());
            List<Intent> snapshotBatch = new ArrayList<>(batch.size());
            for (Intent intent : batch) {
                synchronized (intent) {
                    if (intent.getStatus().isTerminal()) {
                        enqueueTimeNanos.remove(intent.getIntentId());
                        continue;
                    }
                    liveBatch.add(intent);
                    snapshotBatch.add(intent.copy());
                }
            }
            if (liveBatch.isEmpty()) {
                continue;
            }

            // Phase 3: acquire permits for liveBatch (non-terminal count)
            List<ResizableSemaphore> acquiredPermits = new ArrayList<>(liveBatch.size());
            long acquireStartNs = System.nanoTime();
            try {
                for (int i = 0; i < liveBatch.size(); i++) {
                    acquiredPermits.add(acquireWithBorrow(tier));
                }
            } catch (InterruptedException e) {
                // Fix 7: 中断释放同样配对 decrementBorrowed
                for (ResizableSemaphore s : acquiredPermits) {
                    releasePermit(tier, s);
                }
                Thread.currentThread().interrupt();
                break;
            }
            long acquireEndNs = System.nanoTime();

            // Trace: record dequeued for each intent (内核内部操作，用活对象)
            for (Intent intent : liveBatch) {
                traceStore.recordDequeued(intent.getIntentId());
                recordDispatchQueueLag(intent, tier);
            }

            permitTimingStats.totalAcquireWaitNanos.addAndGet(acquireEndNs - acquireStartNs);
            final long permitAcquiredNs = acquireEndNs;

            // Fix 5: 批量在途计数
            AtomicInteger inFlight = tierInFlight.get(tier);
            for (int i = 0; i < liveBatch.size(); i++) inFlight.incrementAndGet();

            // Phase 4: batch delivery - Fix 4: per-future 独立结算,一条失败不连坐整批。
            // I5: deliverBatchAsync 收到 snapshotBatch（快照），finalize 用 liveBatch（活对象）。
            long deliverStartNs = System.nanoTime();
            List<CompletableFuture<DeliveryHandler.DeliveryResult>> futures;
            try {
                futures = deliveryHandler.deliverBatchAsync(snapshotBatch);
            } catch (RuntimeException deliverEx) {
                // P1-9: SPI 同步抛异常--按失败结算每个 intent,消费者继续存活
                for (int i = 0; i < liveBatch.size(); i++) {
                    Intent intent = liveBatch.get(i);
                    releasePermit(tier, acquiredPermits.get(i));
                    submitFinalize(intent, tier, () -> handleDeliveryException(intent, tier, deliverEx));
                }
                permitTimingStats.totalDeliverAsyncNanos.addAndGet(System.nanoTime() - deliverStartNs);
                continue;
            }
            if (futures == null) {
                // SPI returned null instead of throwing - treat entire batch as failures
                for (int i = 0; i < liveBatch.size(); i++) {
                    Intent intent = liveBatch.get(i);
                    releasePermit(tier, acquiredPermits.get(i));
                    submitFinalize(intent, tier, () -> handleDeliveryException(intent, tier,
                        new IllegalStateException("deliverBatchAsync returned null")));
                }
                permitTimingStats.totalDeliverAsyncNanos.addAndGet(System.nanoTime() - deliverStartNs);
                continue;
            }
            if (futures.size() != liveBatch.size()) {
                logger.error("deliverBatchAsync returned {} futures for batch of {}; missing futures treated as failures",
                    futures.size(), liveBatch.size());
            }
            for (int i = 0; i < liveBatch.size(); i++) {
                final Intent intent = liveBatch.get(i);
                final ResizableSemaphore permit = acquiredPermits.get(i);
                CompletableFuture<DeliveryHandler.DeliveryResult> f =
                    i < futures.size() ? futures.get(i) : null;
                if (f == null) {
                    releasePermit(tier, permit);
                    submitFinalize(intent, tier, () -> handleDeliveryException(intent, tier,
                        new IllegalStateException("deliverBatchAsync returned no future for this intent")));
                    continue;
                }
                f.orTimeout(DELIVERY_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .whenComplete((result, ex) -> {
                        long releaseNs = System.nanoTime();
                        permitTimingStats.totalPermitHoldNanos.addAndGet(releaseNs - permitAcquiredNs);
                        permitTimingStats.deliverySampleCount.incrementAndGet();
                        releasePermit(tier, permit);
                        submitFinalize(intent, tier, () -> {
                            if (ex != null) {
                                handleDeliveryException(intent, tier, ex);
                            } else {
                                finalizeIntent(intent, tier, result);
                            }
                        });
                    });
            }
            permitTimingStats.totalDeliverAsyncNanos.addAndGet(System.nanoTime() - deliverStartNs);
        }
    }

    /**
     * Arrow-inspired cross-tier slot acquisition.
     * Non-blocking try on own tier, then non-blocking borrow from lower-priority
     * tiers (AdapTBF-bounded), then blocking fallback on own tier.
     */
    private ResizableSemaphore acquireWithBorrow(PrecisionTier tier) throws InterruptedException {
        ResizableSemaphore own = tierSemaphores.get(tier);
        if (own.tryAcquire()) {
            borrowStats.ownAcquires.incrementAndGet();
            return own;
        }

        // Non-blocking borrow from lower-priority tiers (AdapTBF: bounded lending)
        PrecisionTier[] allTiers = PrecisionTier.values();
        for (int i = tier.ordinal() + 1; i < allTiers.length; i++) {
            ResizableSemaphore other = tierSemaphores.get(allTiers[i]);

            if (other.availablePermits() <= 0) continue;
            if (other.getBorrowedCount() >= (int) (other.getCurrentMax() * MAX_LEND_RATIO)) continue;

            if (other.tryAcquire()) {
                other.incrementBorrowed();
                borrowStats.borrowedAcquires.incrementAndGet();
                return other;
            }
        }

        // Fallback: block on own tier until a completing delivery returns a permit
        long blockingStartNs = System.nanoTime();
        own.acquire();
        long blockingEndNs = System.nanoTime();
        permitTimingStats.totalBlockingWaitNanos.addAndGet(blockingEndNs - blockingStartNs);
        permitTimingStats.blockingWaitCount.incrementAndGet();
        borrowStats.ownBlockingAcquires.incrementAndGet();
        return own;
    }

    public BorrowStats getBorrowStats() { return borrowStats; }

    /** 设置 Intent 生命周期观察器列表（由 LoomqEngine 调用） */
    public void setObservers(List<IntentObserver> observers) {
        this.observers.clear();
        if (observers != null) {
            this.observers.addAll(observers);
        }
    }

    /**
     * 运行时新增观察器——立即生效(observer 列表为 CopyOnWriteArrayList)。
     * 修复原"启动后注册的 observer 永远收不到事件"的问题(LoomqEngine.start 时
     * setObservers 拷贝了快照,之后 registerObserver 只改 LoomqEngine 侧列表)。
     */
    public void addObserver(IntentObserver observer) {
        if (observer != null) observers.add(observer);
    }

    public void removeObserver(IntentObserver observer) {
        observers.remove(observer);
    }

    /**
     * 释放 permit 并在跨档借用时配对 decrementBorrowed。
     * 统一所有 release 路径,杜绝 borrowedCount 泄漏导致跨档借用静默永久失效。
     */
    private void releasePermit(PrecisionTier tier, ResizableSemaphore acquired) {
        if (acquired != tierSemaphores.get(tier)) {
            acquired.decrementBorrowed();
        }
        acquired.release();
    }

    /**
     * 把投递结算任务提交到 sharedExecutor,统一处理 RejectedExecutionException
     * 并在任务结束时 -1 在途计数(Fix 5)。stop() 等在途归零后才关 executor,
     * 正常路径不应 reject;此处的兜底仅作防御,避免在途决策静默丢失。
     */
    private void submitFinalize(Intent intent, PrecisionTier tier, Runnable task) {
        try {
            sharedExecutor.submit(() -> {
                try {
                    task.run();
                } catch (Exception e) {
                    logger.error("Error in delivery callback for intent {}", intent.getIntentId(), e);
                } finally {
                    tierInFlight.get(tier).decrementAndGet();
                }
            });
        } catch (RejectedExecutionException e) {
            tierInFlight.get(tier).decrementAndGet();
            logger.error("sharedExecutor rejected finalize for intent {}; outcome may be lost",
                intent.getIntentId(), e);
        }
    }

    /** 注入状态变更持久化钩子(接到 IntentCommandService 的 DURABLE 落盘)。 */
    public void setStateChangePersister(Consumer<Intent> persister) {
        this.stateChangePersister = persister;
    }

    /**
     * 状态变更持久化(I3 不变量收口):revision 递增 + DURABLE 落盘。
     * 所有调用方只需调此方法,revision 递增由本方法统一负责,
     * 杜绝未来调用方遗忘递增导致 recovery 去重失效。
     */
    private void persistStateChange(Intent intent) {
        intent.incrementRevision();
        Consumer<Intent> p = stateChangePersister;
        if (p != null) {
            p.accept(intent);
        }
    }

    /** 安全通知所有观察器，单个异常不影响其他 observer 和调度循环 */
    private void notifyObservers(java.util.function.Consumer<IntentObserver> action) {
        for (IntentObserver o : observers) {
            try {
                action.accept(o);
            } catch (Exception e) {
                logger.error("Observer error", e);
            }
        }
    }

    /** 将 intent 按 executeAt 加入过期索引 */
    private void indexIntent(Intent intent) {
        if (intent.getExecuteAt() == null) return;
        long key = intent.getExecuteAt().toEpochMilli();
        intentExpiryIndex.computeIfAbsent(key, k -> ConcurrentHashMap.newKeySet())
                         .add(intent.getIntentId());
    }

    /** 从过期索引移除 intent（原子操作，避免并发添加时误删 bucket） */
    private void unindexIntent(String intentId, long executeAtMs) {
        intentExpiryIndex.computeIfPresent(executeAtMs, (key, ids) -> {
            ids.remove(intentId);
            return ids.isEmpty() ? null : ids;
        });
    }

    private static long executeAtMs(Intent intent) {
        return intent.getExecuteAt() != null ? intent.getExecuteAt().toEpochMilli() : 0L;
    }

    /**
     * 记录 due→dispatch lag
     */
    private void recordDispatchQueueLag(Intent intent, PrecisionTier tier) {
        Long enqueueNanos = enqueueTimeNanos.remove(intent.getIntentId());
        if (enqueueNanos != null) {
            long lagMs = (System.nanoTime() - enqueueNanos) / 1_000_000;
            metrics.recordDispatchQueueLagByTier(tier, lagMs);
        }
    }

    /**
     * 记录唤醒延迟
     */
    private void recordWakeupLatency(Intent intent, Instant actualTime) {
        Instant executeAt = intent.getExecuteAt();
        long latencyUs = Duration.between(executeAt, actualTime).toNanos() / 1_000;
        metrics.recordWakeupLatencyByTier(intent.getPrecisionTier(), latencyUs);
    }

    /**
     * 检查过期任务。
     *
     * 扫描所有非终态且已过期的 intent（包括未入队的 SCHEDULED），
     * 适配轻量级 dispatch() 中中间态不持久化的设计。
     */
    private void checkExpiredIntents(PrecisionTier tier) {
        long nowMs = System.currentTimeMillis();
        var expiredEntries = intentExpiryIndex.headMap(nowMs, true);
        for (var entry : expiredEntries.entrySet()) {
            for (String intentId : entry.getValue()) {
                Intent intent = intentStore.findByIdInternal(intentId);
                if (intent == null) continue;  // 惰性清理：可能已被删除
                if (intent.getPrecisionTier() != tier) continue;
                if (!intent.isExpired()) continue;
                if (intent.getStatus().isTerminal()) continue;

                if (intent.getStatus() == IntentStatus.DUE ||
                    intent.getStatus() == IntentStatus.DELIVERED ||
                    intent.getStatus() == IntentStatus.SCHEDULED ||
                    intent.getStatus() == IntentStatus.DISPATCHING) {
                    handleExpired(intent);
                }
            }
        }
    }

    /**
     * 处理过期任务。
     *
     * P1-3: synchronized(intent) 与 finalizeIntent 串行化——Intent.transitionTo 的
     * validate+set 非原子,不加锁时两线程可同时通过校验导致状态撕裂(EXPIRED 被 DUE 回跳等)。
     *
     * @implNote 维护 I2/I3：过期终态落盘，防止恢复时被改写。
     */
    private void handleExpired(Intent intent) {
        synchronized (intent) {
            unindexIntent(intent.getIntentId(), executeAtMs(intent));
            logger.info("Intent expired: id={}, deadline={}", intent.getIntentId(), intent.getDeadline());

            switch (intent.getExpiredAction()) {
                case DISCARD:
                    intent.transitionTo(IntentStatus.EXPIRED);
                    break;
                case DEAD_LETTER:
                    intent.transitionTo(IntentStatus.DEAD_LETTERED);
                    break;
            }
            intentStore.update(intent);
            persistStateChange(intent);           // P1-1

            // Notify AFTER transition so observers see the terminal state
            notifyObservers(o -> o.onExpired(intent));
        }
    }

    /**
     * 终态处理——根据异步投递结果更新状态并持久化。
     *
     * 在 Netty/异步回调线程中执行。单次 intentStore.update() 写入终态。
     *
     * @implNote 维护 I2/I3：终态落盘（persistStateChange）确保磁盘权威记录。
     */
    private void finalizeIntent(Intent intent, PrecisionTier tier, DeliveryResult result) {
        long startTime = System.nanoTime();
        try {
            synchronized (intent) {
                // 内存中状态转换（不持久化 — 终态才做一次 upsert）
                intent.transitionTo(IntentStatus.DUE);
                intent.transitionTo(IntentStatus.DISPATCHING);
                intent.incrementAttempts();

                // Trace: record delivered
                traceStore.recordDelivered(intent.getIntentId());

                final DeliveryResult finalResult = result != null ? result : DeliveryResult.RETRY;
                if (result == null) {
                    logger.warn("Delivery handler returned null for intent {}, treating as RETRY", intent.getIntentId());
                }

                switch (finalResult) {
                    case SUCCESS:
                        intent.transitionTo(IntentStatus.DELIVERED);
                        intent.transitionTo(IntentStatus.ACKED);
                        intentStore.update(intent);
                        persistStateChange(intent);           // P1-1: incrementRevision + DURABLE write
                        notifyObservers(o -> o.onDelivered(intent, finalResult));
                        unindexIntent(intent.getIntentId(), executeAtMs(intent));
                        // Trace: record acked
                        traceStore.recordAcked(intent.getIntentId());
                        traceStore.updateStatus(intent.getIntentId(), IntentStatus.ACKED);
                        logger.debug("Intent {} delivered successfully", intent.getIntentId());
                        break;

                    case RETRY: {
                        long oldExecuteAtMs = executeAtMs(intent);
                        long delayMs = intent.getRedelivery() != null
                            ? intent.getRedelivery().calculateDelay(intent.getAttempts())
                            : 5000;
                        logger.info("Scheduling redelivery for intent={}, attempt={}, delay={}ms",
                            intent.getIntentId(), intent.getAttempts(), delayMs);
                        intent.setExecuteAt(Instant.now().plusMillis(delayMs));
                        intent.transitionTo(IntentStatus.SCHEDULED);
                        intentStore.update(intent);
                        // Fix 6: 重排程是新的调度承诺而非中间态——必须 DURABLE 落盘,
                        // 否则崩溃恢复看到旧 executeAt 的 SCHEDULED 槽(已过期),又被
                        // WheelRecovery 的 overdue 路径丢弃,重试链静默丢失。
                        persistStateChange(intent);
                        unindexIntent(intent.getIntentId(), oldExecuteAtMs);
                        schedule(intent);
                        break;
                    }

                    case DEAD_LETTER:
                        intent.transitionTo(IntentStatus.DEAD_LETTERED);
                        intentStore.update(intent);
                        persistStateChange(intent);           // P1-1
                        notifyObservers(o -> o.onDeadLettered(intent));
                        unindexIntent(intent.getIntentId(), executeAtMs(intent));
                        logger.warn("Intent {} dead-lettered", intent.getIntentId());
                        break;

                    case EXPIRED:
                        intent.transitionTo(IntentStatus.EXPIRED);
                        intentStore.update(intent);
                        persistStateChange(intent);           // P1-1
                        notifyObservers(o -> o.onExpired(intent));
                        unindexIntent(intent.getIntentId(), executeAtMs(intent));
                        logger.info("Intent {} expired", intent.getIntentId());
                        break;
                }
            }
        } finally {
            long durationMs = (System.nanoTime() - startTime) / 1_000_000;
            metrics.recordWebhookLatency(durationMs);
            metrics.incrementIntentByTier(tier);
        }
    }

    /**
     * 处理投递失败
     *
     * @implNote 维护 I2/I3：死信终态落盘；重试路径 persistStateChange 递增 revision。
     */
    private void handleDeliveryFailure(Intent intent) {
        synchronized (intent) {
            int maxAttempts = intent.getRedelivery() != null
                ? intent.getRedelivery().getMaxAttempts()
                : 5;

            intent.transitionTo(IntentStatus.DUE);
            intent.transitionTo(IntentStatus.DISPATCHING);
            intent.incrementAttempts();

            if (intent.getAttempts() >= maxAttempts) {
                intent.transitionTo(IntentStatus.DEAD_LETTERED);
                intentStore.update(intent);
                persistStateChange(intent);           // P1-1
                unindexIntent(intent.getIntentId(), executeAtMs(intent));
                notifyObservers(o -> o.onDeadLettered(intent));
                logger.warn("Intent dead-lettered after max attempts: id={}", intent.getIntentId());
            } else {
                long oldExecuteAtMs = executeAtMs(intent);
                long delayMs = intent.getRedelivery() != null
                    ? intent.getRedelivery().calculateDelay(intent.getAttempts())
                    : 5000;
                logger.info("Scheduling redelivery for intent={} after failure, attempt={}, delay={}ms",
                    intent.getIntentId(), intent.getAttempts(), delayMs);
                intent.setExecuteAt(Instant.now().plusMillis(delayMs));
                intent.transitionTo(IntentStatus.SCHEDULED);
                intentStore.update(intent);
                // Fix 6: 重排程落盘,见 finalizeIntent RETRY 分支同款说明
                persistStateChange(intent);
                unindexIntent(intent.getIntentId(), oldExecuteAtMs);
                schedule(intent);
            }
        }
    }

    public static class BorrowStats {
        public final AtomicLong ownAcquires = new AtomicLong(0);
        public final AtomicLong ownBlockingAcquires = new AtomicLong(0);
        public final AtomicLong borrowedAcquires = new AtomicLong(0);
        public long totalBorrowed() { return borrowedAcquires.get(); }
        public double borrowRate() {
            long own = ownAcquires.get();
            long blocking = ownBlockingAcquires.get();
            long borrowed = borrowedAcquires.get();
            long total = own + blocking + borrowed;
            return total > 0 ? (double) borrowed / total * 100.0 : 0.0;
        }
    }

    /**
     * 检查档位是否处于背压状态
     */
    public boolean isTierUnderBackpressure(PrecisionTier tier) {
        ResizableSemaphore semaphore = tierSemaphores.get(tier);
        BlockingQueue<Intent> queue = tierDispatchQueues.get(tier);

        boolean semaphoreExhausted = semaphore.availablePermits() == 0;
        boolean queueBackedUp = queue.size() >= precisionTierCatalog.maxConcurrency(tier) * 2;

        return semaphoreExhausted || queueBackedUp;
    }

    /**
     * 获取档位背压信息
     */
    public Map<PrecisionTier, BackpressureInfo> getBackpressureStatus() {
        Map<PrecisionTier, BackpressureInfo> status = new EnumMap<>(PrecisionTier.class);

        for (PrecisionTier tier : precisionTierCatalog.supportedTiers()) {
            ResizableSemaphore semaphore = tierSemaphores.get(tier);
            BlockingQueue<Intent> queue = tierDispatchQueues.get(tier);

            int availablePermits = semaphore.availablePermits();
            int queueSize = queue.size();
            boolean underPressure = isTierUnderBackpressure(tier);

            status.put(tier, new BackpressureInfo(
                precisionTierCatalog.maxConcurrency(tier),
                availablePermits,
                queueSize,
                underPressure,
                precisionTierCatalog.maxConcurrency(tier) - availablePermits,
                semaphore.getBorrowedCount()
            ));
        }

        return status;
    }

    /**
     * 背压信息记录
     */
    public record BackpressureInfo(
        int maxConcurrency,
        int availablePermits,
        int queueSize,
        boolean underBackpressure,
        int activeDispatches,
        int borrowedCount
    ) {
        /** Utilization percentage: active dispatches / max concurrency × 100 */
        public double utilizationPct() {
            return maxConcurrency > 0 ? (activeDispatches * 100.0) / maxConcurrency : 0.0;
        }
    }

    /**
     * 获取桶组管理器
     */
    public BucketGroupManager getBucketGroupManager() {
        return bucketGroupManager;
    }

    public CohortManager getCohortManager() {
        return cohortManager;
    }

    private BucketGroup resolveBucketGroup(PrecisionTier tier) {
        BucketGroup group = bucketGroupManager.getBucketGroup(tier);
        if (group == null) {
            group = bucketGroupManager.getBucketGroup(precisionTierCatalog.defaultTier());
        }
        return group;
    }
}
