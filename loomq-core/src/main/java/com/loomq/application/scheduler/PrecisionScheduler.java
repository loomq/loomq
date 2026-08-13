package com.loomq.application.scheduler;

import com.loomq.common.MetricsCollector;
import com.loomq.domain.intent.ExpiredAction;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.domain.intent.PrecisionTierCatalog;
import com.loomq.spi.DefaultRedeliveryDecider;
import com.loomq.spi.DeliveryContext;
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
 *   <li><b>I2 持久化先于承诺</b>: 状态对调用方可见前必须已对磁盘可见（DURABLE 落盘;I6 容错下终态持久化失败降级为 best-effort,onDelivered 仍触发）</li>
 *   <li><b>I3 revision 单调 + 终态不可逆</b>: 非终态 append + 终态原地覆写,recovery 按 max-revision 去重的基础</li>
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

    // 共享虚拟线程池（所有档位共享）。非 final：stop() 后 start() 需重建（否则提交被拒）。
    private ExecutorService sharedExecutor;

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
     * 旧 executeAt 的 SCHEDULED 槽，重试链静默丢失。(I6 容错下持久化失败仅记 persistFailures,不阻塞调度)
     */
    private volatile StateChangeSink stateChangeSink;

    // 档位级有界队列（容量 = maxConcurrency × 4，满时触发 backpressure）
    private final Map<PrecisionTier, BlockingQueue<Intent>> tierDispatchQueues;

    // 按精度档位的扫描调度器
    private final Map<PrecisionTier, ScheduledExecutorService> scanSchedulers;
    private final Map<PrecisionTier, ScheduledFuture<?>> scanFutures;

    // scanTrigger 去重：同一 tier 同时最多一个待执行的 triggered scan
    private final Map<PrecisionTier, AtomicBoolean> pendingScanTrigger = new ConcurrentHashMap<>();

    // Adaptive 扫描器：park 目标（MAX_VALUE = 无目标）+ 平台线程引用
    private final Map<PrecisionTier, AtomicLong> scannerParkTarget = new ConcurrentHashMap<>();
    private final Map<PrecisionTier, Thread> scannerThreads = new ConcurrentHashMap<>();

    // 单发消费者线程引用（按档），供 scanAndDispatch 在 offer 后 unpark
    private final Map<PrecisionTier, Thread[]> consumerThreads = new ConcurrentHashMap<>();
    private final Map<PrecisionTier, AtomicInteger> consumerPickup = new ConcurrentHashMap<>();

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
     * 观察器接收到的是派发时刻的防御性快照，其状态反映投递失败时的值。
     * 观察器不应依赖快照状态来推断调度器的后续决策。
     */
    private void handleDeliveryException(Intent intent, PrecisionTier tier, Throwable ex) {
        // I5: 快照在锁内取，dispatch 在锁外
        final Intent snapshot;
        synchronized (intent) {
            snapshot = intent.copy();
        }
        notifyObservers(o -> o.onDeliveryFailed(snapshot, ex));
        if (ex instanceof java.util.concurrent.TimeoutException) {
            logger.warn("Delivery timeout for intent {}", intent.getIntentId());
        } else {
            logger.error("Delivery exception for intent {}: {}", intent.getIntentId(), ex.getMessage(), ex);
        }
        // R12: RedeliveryDecider 实际参与重投决策——decider 判定不可重投(永久失败,如业务
        // 4xx/非法请求)→ 直接终态 DEAD_LETTERED,不再按 maxAttempts 反复重试;可重投 →
        // 走既有重试链(attempts >= maxAttempts 终态)。decider 默认(DefaultRedeliveryDecider)
        // 对异常恒可重投,行为与修复前一致。
        handleDeliveryFailure(intent, !shouldRedeliver(intent, ex));
    }

    /**
     * 用 RedeliveryDecider 判定异常是否值得重投。
     *
     * <p>decider 抛异常时保守按"可重投"处理(与无 decider 的既有行为一致),不因用户 SPI
     * 异常阻断调度链。deliveryId 用 lastDeliveryId(缺省回退 intentId)仅作上下文标识。</p>
     */
    private boolean shouldRedeliver(Intent intent, Throwable ex) {
        DeliveryContext ctx = new DeliveryContext(
            intent.getLastDeliveryId() != null ? intent.getLastDeliveryId() : intent.getIntentId(),
            intent.getIntentId(),
            // attempt 语义为"本次失败投递是第几次尝试"(从 1 开始)。此时 incrementAttempts()
            // 尚未执行(它在 handleDeliveryFailure 内),故当前 attempts 是上一次的值,需 +1。
            intent.getAttempts() + 1);
        ctx.markFailure(ex);
        try {
            return redeliveryDecider.shouldRedeliver(ctx);
        } catch (Exception deciderEx) {
            logger.error("RedeliveryDecider threw for intent {}; defaulting to redeliver",
                intent.getIntentId(), deciderEx);
            return true;
        }
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
        this.cohortManager = new CohortManager(this.bucketGroupManager, this.precisionTierCatalog,
            flushedIntents -> {
                Set<PrecisionTier> tiers = EnumSet.noneOf(PrecisionTier.class);
                for (Intent intent : flushedIntents) tiers.add(intent.getPrecisionTier());
                for (PrecisionTier tier : tiers) triggerScan(tier);
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
        if (sharedExecutor.isShutdown()) {
            // stop() 后重启：旧 executor 已关，重建共享虚拟线程池——
            // 否则 startBatchConsumers 的 submit 被 RejectedExecutionException 打断，
            // running=true 却无消费者/无扫描，调度器半死不活。
            sharedExecutor = Executors.newVirtualThreadPerTaskExecutor();
        }
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
        Thread[] threads = new Thread[consumerCount];
        consumerThreads.put(tier, threads);
        consumerPickup.put(tier, new AtomicInteger(0));
        for (int i = 0; i < consumerCount; i++) {
            final int idx = i;
            sharedExecutor.submit(() -> {
                Thread t = Thread.currentThread();
                t.setName("batch-consumer-" + tier.name().toLowerCase() + "-" + idx);
                threads[idx] = t;
                runBatchConsumer(tier);
            });
        }
        logger.info("Started {} batch consumers for tier {}", consumerCount, tier);
    }

    /**
     * 启动指定精度档位的扫描任务
     */
    private void startScanCycle(PrecisionTier tier) {
        if (precisionTierCatalog.isAdaptive(tier)) {
            Thread t = Thread.ofPlatform()
                .name("adaptive-scan-" + tier.name().toLowerCase())
                .daemon(true)
                .unstarted(() -> adaptiveScanLoop(tier));
            scannerThreads.put(tier, t);
            scannerParkTarget.put(tier, new AtomicLong(Long.MAX_VALUE));
            // 新桶（更早 key）出现时 unpark：setBucketAddListener 在 add 的 intentIndex.compute
            // 完成后、锁外触发（unpark 幂等，触发点无功能影响）
            resolveBucketGroup(tier).setBucketAddListener(key -> {
                AtomicLong target = scannerParkTarget.get(tier);
                if (target != null && key <= target.get()) {
                    LockSupport.unpark(t);
                }
            });
            t.start();
            logger.info("Started adaptive scan for tier {} (event-driven)", tier);
            return;
        }
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

    /** 触发一次 scan：adaptive 档 unpark 扫描线程，fixed-rate 档走 pendingScanTrigger 去重提交。 */
    private void triggerScan(PrecisionTier tier) {
        if (precisionTierCatalog.isAdaptive(tier)) {
            Thread t = scannerThreads.get(tier);
            if (t != null) LockSupport.unpark(t);
            return;
        }
        AtomicBoolean pending = pendingScanTrigger.computeIfAbsent(tier, k -> new AtomicBoolean(false));
        if (pending.compareAndSet(false, true)) {
            ScheduledExecutorService scanScheduler = scanSchedulers.get(tier);
            if (scanScheduler != null) {
                scanScheduler.submit(() -> { pending.set(false); scanAndDispatch(tier); });
            }
        }
    }

    /** 事件驱动扫描循环：park 至最早 bucketKey，add 新更早键时被 unpark。 */
    private void adaptiveScanLoop(PrecisionTier tier) {
        BucketGroup group = resolveBucketGroup(tier);
        AtomicLong parkTarget = scannerParkTarget.get(tier);
        while (running) {
            try {
                // 步骤 1：先发布"即将 park"信号，再读 firstKey（防 lost wakeup）
                parkTarget.set(Long.MAX_VALUE);
                if (paused) {
                    // paused：空转保底（park 上限 scanIntervalMs×100），resume() 时被 unpark 立即恢复
                    parkWithFallback(tier, Long.MAX_VALUE);
                    continue;
                }
                Long earliest = group.earliestBucketKey();
                long nowMs = System.currentTimeMillis();

                if (earliest == null) {
                    // 空表 → 保底 tick（scanIntervalMs x 100）；空窗期无 scanAndDispatch，
                    // 过期检查在此分频运行，保证 deadline 检查的最大延迟有界（§4.2 用途 1）。
                    parkWithFallback(tier, Long.MAX_VALUE);
                    if (shouldCheckExpired(tier)) checkExpiredIntents(tier);
                    continue;
                }
                if (earliest <= nowMs) {
                    // 本分支扫完即走：扫描期间出现的新桶由下方 reEarliest 复查兜底（循环顶
                    // 亦会重查），无需 bucket-add unpark——先抑制 listener（parkTarget=MIN_VALUE），
                    // 避免 scanAndDispatch 内背压重入桶触发 unpark，遗留 permit 打断后续节流 park。
                    parkTarget.set(Long.MIN_VALUE);
                    scanAndDispatch(tier);   // 内部已含 shouldCheckExpired + checkExpiredIntents
                    // P3-4：scanDue 把未到期 intent 重入同一 floor 桶，若最早桶仍 <= now，
                    // park 到最早桶内最小精确 executeAt（避免忙转）。仅未到期路径触发，O(bucket) 有界。
                    long afterScanMs = System.currentTimeMillis();
                    Long reEarliest = group.earliestBucketKey();
                    if (reEarliest != null && reEarliest <= afterScanMs) {
                        Long next = group.earliestExecuteAt();
                        if (next != null && next > afterScanMs) {
                            parkTarget.set(next);
                            parkWithFallback(tier, TimeUnit.MILLISECONDS.toNanos(next - afterScanMs));
                            if (shouldCheckExpired(tier)) checkExpiredIntents(tier);
                            continue;
                        }
                        // 最早桶内仍有到期 intent（dispatch 队列满时 scanAndDispatch 把 intent
                        // 重入桶，立即重扫路径无任何 park → 单平台线程 100% CPU 忙转，饿死
                        // 其他档/cohort）。有界 park（scanIntervalMs）后再重试，背压解除即恢复。
                        // parkTarget 保持 MIN_VALUE：背压期间队列已满，新 intent 反正无法入队，
                        // 延迟 ≤ scanIntervalMs 无害。
                        parkWithFallback(tier, TimeUnit.MILLISECONDS.toNanos(precisionTierCatalog.scanIntervalMs(tier)));
                        continue;
                    }
                    continue;
                }
                // 步骤 2b：发布精确 park 目标，然后复查 firstKey（关键：发布后再查一次）
                long target = earliest;
                parkTarget.set(target);
                Long recheck = group.earliestBucketKey();
                if (recheck != null && recheck < target) {
                    metrics.recordScannerWakeEarly(tier);
                    continue;
                }
                parkWithFallback(tier, TimeUnit.MILLISECONDS.toNanos(target - nowMs));
                // 未来桶 park 后同样分频跑过期检查（至多 100ms 一次）
                if (shouldCheckExpired(tier)) checkExpiredIntents(tier);
            } catch (Exception e) {
                logger.error("Adaptive scan error for tier {}", tier, e);
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
            }
        }
        logger.info("Adaptive scan stopped for tier {}", tier);
    }

    /** 单次 park 带上限（保底 tick），unpark 可立即返回。 */
    private void parkWithFallback(PrecisionTier tier, long parkNanos) {
        long capNanos = TimeUnit.MILLISECONDS.toNanos(precisionTierCatalog.scanIntervalMs(tier) * 100L);
        long effective = Math.min(parkNanos, capNanos);
        metrics.recordScannerPark(tier);
        if (effective <= 0) return;
        LockSupport.parkNanos(effective);
    }

    /**
     * 停止调度器
     */
    public void stop() {
        running = false;

        // 停止 cohort 唤醒器
        cohortManager.stop();

        // 唤醒 adaptive 扫描线程，使其观察 running=false 后退出
        for (Thread t : scannerThreads.values()) {
            LockSupport.unpark(t);
        }
        // 等扫描线程退出：stop() 后 start() 会重建扫描线程，若旧线程仍存活且观察到
        // running=true 会继续扫描——同档双扫描线程（正确性靠 CAS 兜底，但指标/时序失真）。
        for (Thread t : scannerThreads.values()) {
            try {
                t.join(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        scannerThreads.clear();
        scannerParkTarget.clear();

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
            // 立即唤醒 adaptive 扫描线程，避免等待 park 上限（scanIntervalMs×100）
            for (Thread t : scannerThreads.values()) {
                LockSupport.unpark(t);
            }
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

        // Trace: record intent creation (仅真正新建时——recordCreated 会整体替换 trace,
        // 重试重排程/改期重排程若重复调用会把投递历史清空、createdAt 改写为重排程时刻,
        // 终态 trace 恒显 CREATED,误导"为什么死了"的排查)
        if (!traceStore.contains(intent.getIntentId())) {
            traceStore.recordCreated(
                intent.getIntentId(), intent.getTraceId(), intent.getPrecisionTier());
        }

        // I5: synchronized 内完成状态迁移 + 索引 + 路由 + 快照；
        // onScheduled 在锁外派发（不持有 intent 锁），但仍同步执行于调用线程。
        // 锁释放与锁外 schedule() 调用之间的交错窗口是良性的：
        // - BucketGroup.add() 的 intentIndex.compute() 原子覆盖同 intentId 旧条目
        // - scanDue CAS 捕获过期条目（revision 不匹配则跳过）
        // - CohortManager.remove 的 removeIf 清理所有匹配条目
        java.util.function.Consumer<IntentObserver> deferredNotify = null;
        synchronized (intent) {
            // 锁内终态复查 -- cancel 可能在锁释放与 schedule() 之间发生
            if (intent.getStatus() != IntentStatus.CREATED && intent.getStatus() != IntentStatus.SCHEDULED) {
                logger.debug("Skipping schedule for intent {} (status changed to {})",
                    intent.getIntentId(), intent.getStatus());
            } else {
                if (intent.getStatus() == IntentStatus.CREATED) {
                    intent.transitionTo(IntentStatus.SCHEDULED);
                }

                indexIntent(intent);

                Instant executeAt = intent.getExecuteAt();
                Instant now = Instant.now();
                long delayMs = Duration.between(now, executeAt).toMillis();

                PrecisionTier tier = intent.getPrecisionTier();
                long precisionWindowMs = precisionTierCatalog.precisionWindowMs(tier);

                if (precisionTierCatalog.isDirectBucket(tier)) {
                    // MILLI：cohort 旁路直插桶（1ms 窗口下交接链延迟吃预算，整条消除）。
                    // 内存高水位降级 → 回退 cohort（精度劣化，指标计数）。
                    BucketGroup.AddResult r = bucketGroupManager.add(intent);
                    if (r == BucketGroup.AddResult.FALLBACK_TO_COHORT) {
                        cohortManager.register(intent);
                        metrics.incrementMilliFallback(tier);
                    }
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

                if (!observers.isEmpty()) {
                    final Intent snapshot = intent.copy();
                    deferredNotify = o -> o.onScheduled(snapshot);
                }
            }
        }
        // I5: onScheduled 在锁外派发
        if (deferredNotify != null) {
            notifyObservers(deferredNotify);
        }
    }

    /**
     * 添加到桶并等待调度
     */
    private void addToBucketAndDispatch(Intent intent) {
        BucketGroup.AddResult r = bucketGroupManager.add(intent);
        if (r == BucketGroup.AddResult.FALLBACK_TO_COHORT) {
            // 高水位降级：新桶未建、旧桶条目已摘、索引已清（intent 现归 cohort 管）。
            // 结果不能丢弃——否则 intent 既不在桶也不在 cohort，静默丢失直到重启恢复。
            cohortManager.register(intent);
            metrics.incrementMilliFallback(intent.getPrecisionTier());
        }
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
        if (intent == null || intent.getExecuteAt() == null) {
            return;
        }
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
                BucketGroup.AddResult r = bucketGroupManager.add(intent);
                if (r == BucketGroup.AddResult.FALLBACK_TO_COHORT) {
                    cohortManager.register(intent);
                    metrics.incrementMilliFallback(tier);
                }
            } else if (delayMs > precisionWindowMs) {
                cohortManager.register(intent);
            } else {
                BucketGroup.AddResult r = bucketGroupManager.add(intent);
                if (r == BucketGroup.AddResult.FALLBACK_TO_COHORT) {
                    // 高水位降级：结果不能丢弃（同 addToBucketAndDispatch 说明）
                    cohortManager.register(intent);
                    metrics.incrementMilliFallback(tier);
                }
            }
            indexIntent(intent);
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
                    if (offered) {
                        // 事件驱动消费端：offer 成功后 unpark 一个消费者（round-robin）。
                        // unpark 幂等且廉价，无需判空队列优化；backpressure 重试路径不变。
                        Thread[] threads = consumerThreads.get(tier);
                        AtomicInteger pickup = consumerPickup.get(tier);
                        if (threads != null && threads.length > 0 && pickup != null) {
                            Thread t = threads[Math.floorMod(pickup.getAndIncrement(), threads.length)];
                            if (t != null) LockSupport.unpark(t);
                        }
                    }
                    if (!offered) {
                        metrics.incrementDispatchQueueOfferFailed(tier);
                        metrics.incrementBackpressureEvent(tier);
                        logger.error("CRITICAL: Backpressure — dispatch queue full for tier {}, requeuing intent {} for next scan cycle",
                            tier, intent.getIntentId());
                        // 清理本次 enqueue 时间戳：重入后下次 offer 会重新登记；
                        // 否则 intent 若在重入期间被取消，条目永不清理（内存泄漏）。
                        enqueueTimeNanos.remove(intent.getIntentId());
                        // I5: 快照在锁内取，dispatch 在锁外
                        final Intent bpSnapshot;
                        synchronized (intent) {
                            bpSnapshot = intent.copy();
                        }
                        notifyObservers(o -> o.onDeliveryFailed(bpSnapshot,
                            new com.loomq.common.exception.BackPressureException(
                                "Dispatch queue full for tier " + tier, null, 1000)));
                        // 重新放回调度结构等待下次 scan cycle（intent 仍为 SCHEDULED 状态，无需回退）。
                        // 走强制入桶（addForced）：intent 已被系统接受（scanDue 已认领），
                        // 忽略高水位降级——否则 FALLBACK_TO_COHORT 会把结果丢弃，intent 静默丢失。
                        bucketGroupManager.addForced(intent);
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
     * MILLI/ULTRA/FAST: 每 cycle（延迟敏感）
     * STANDARD: 每 5 cycle（最大过期延迟 2500ms）
     */
    private boolean shouldCheckExpired(PrecisionTier tier) {
        long count = expiredCheckCounters.get(tier).incrementAndGet();
        int interval = switch (tier) {
            case MILLI, ULTRA, FAST -> 1;
            case STANDARD -> 5;
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
            // Single-intent mode (MILLI, ULTRA, FAST)
            runSingleIntentConsumer(tier, queue);
        } else {
            // Batch mode (STANDARD)
            runBatchDrainConsumer(tier, queue, batchSize, batchWindowMs);
        }
    }

    /**
     * 单 Intent 消费循环（batchSize == 1）。
     * 流程：poll → acquire → I5 快照(synchronized+copy) → deliver(snapshot) → release in callback。
     */
    private void runSingleIntentConsumer(PrecisionTier tier, BlockingQueue<Intent> queue) {
        while (running) {
            Intent intent = queue.poll();
            if (intent == null) {
                // 事件驱动：offer 后 unpark 立即返回；1ms 上限作 lost-wakeup 保底。
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                continue;
            }

            long acquireStartNs = System.nanoTime();
            ResizableSemaphore acquired;
            try {
                acquired = acquireWithBorrow(tier);
            } catch (InterruptedException e) {
                // intent 已出队但不重新入队：中断仅在 shutdown 路径发生，
                // intent 持久化在 wheel 中，重启经 WheelRecovery 恢复。
                // R19: 清理 enqueue 时间戳——intent 已被丢弃,不清理则条目
                // 泄漏到 scheduler 生命周期末(与 offer 失败路径的清理标准一致)。
                enqueueTimeNanos.remove(intent.getIntentId());
                Thread.currentThread().interrupt();
                break;
            }
            long acquireEndNs = System.nanoTime();

            // I5: 派发即快照——在 synchronized(intent) 内原子完成终态复查 + 防御性拷贝。
            // 快照与 cancel/update 互斥：要么取自变更前的完整状态，要么复查拦截（不投递）。
            // 撕裂读在结构上消除；SPI 边界只过快照，用户代码无法触达内核活状态。
            Intent snapshot = null;
            boolean expired = false;
            synchronized (intent) {
                if (intent.getStatus().isTerminal()) {
                    enqueueTimeNanos.remove(intent.getIntentId());
                    releasePermit(tier, acquired);
                    continue;
                }
                // 过期闸门：scanAndDispatch 先入队、后跑 checkExpiredIntents（同 cycle 竞态，
                // 入队恒胜），此处是投递前最后一道闸——deadline 已过则按 ExpiredAction 终态化，
                // 不得投递（deadline = 最晚有效时间契约）。
                expired = intent.isExpired();
                if (!expired) {
                    snapshot = intent.copy();
                }
            }
            if (expired) {
                enqueueTimeNanos.remove(intent.getIntentId());
                releasePermit(tier, acquired);
                // 锁外终态化：handleExpired 自带 synchronized + 锁外 awaitCommit（不 pin carrier）
                handleExpired(intent);
                continue;
            }

            // Trace: record dequeued (right after pollFirst, before any other processing)
            traceStore.recordDequeued(intent.getIntentId());
            recordDispatchQueueLag(intent, tier);

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
                    // 事件驱动：offer 后 unpark 立即返回；1ms 上限作 lost-wakeup 保底。
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                    continue;
                }
                batch.add(first);
                // Drain remaining (non-blocking) up to batchSize
                queue.drainTo(batch, batchSize - batch.size());
            }

            // R15: 批量窗口——未满批次等待至多 batchWindowMs 累计更多 intent,提升批量吞吐
            // (契合 batchSize>1 档的聚合语义)。用 parkNanos(可被 offer→unpark 立即唤醒)而非
            // poll(timeout)(后者阻塞在 Condition,unpark 无法唤醒,破坏事件驱动快速路径)。
            // 已满批(batch.size()==batchSize)或单发档(batchWindowMs<=0)跳过,零额外延迟。
            if (batch.size() < batchSize && batchWindowMs > 0) {
                long deadlineNs = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(batchWindowMs);
                while (batch.size() < batchSize) {
                    long remainingNs = deadlineNs - System.nanoTime();
                    if (remainingNs <= 0) break;
                    LockSupport.parkNanos(remainingNs);
                    queue.drainTo(batch, batchSize - batch.size());
                }
            }

            // I5: 派发即快照--在 synchronized(intent) 内原子完成终态复查 + 防御性拷贝。
            // 构建 liveBatch（结算用活对象）和 snapshotBatch（SPI 用快照）并行列表。
            // 许可获取在 synchronized(intent) 终态复查 + copy 之后，
            // 确保只为非终态 intent 占用 permit。终态复查与快照在同一个
            // 监视器下原子完成，消除了旧 removeIf 路径的 TOCTOU 窗口。
            // 过期闸门：deadline 已过（投递前最后一道闸）→ 收集到锁外终态化，不得投递。
            List<Intent> liveBatch = new ArrayList<>(batch.size());
            List<Intent> snapshotBatch = new ArrayList<>(batch.size());
            List<Intent> expiredBatch = new ArrayList<>(0);
            for (Intent intent : batch) {
                synchronized (intent) {
                    if (intent.getStatus().isTerminal()) {
                        enqueueTimeNanos.remove(intent.getIntentId());
                        continue;
                    }
                    if (intent.isExpired()) {
                        enqueueTimeNanos.remove(intent.getIntentId());
                        expiredBatch.add(intent);
                        continue;
                    }
                    liveBatch.add(intent);
                    snapshotBatch.add(intent.copy());
                }
            }
            // 锁外终态化（handleExpired 自带 synchronized + 锁外 awaitCommit）
            for (Intent intent : expiredBatch) {
                handleExpired(intent);
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
                // R19: 批次已出队即被丢弃(重启经 WheelRecovery 恢复)——清理
                // enqueue 时间戳,避免条目泄漏(与单发中断路径同款修复)。
                for (Intent dropped : liveBatch) {
                    enqueueTimeNanos.remove(dropped.getIntentId());
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
            // 禁止从 directBucket（MILLI）档借用：MILLI 因枚举序位于末尾，却被当作
            // 最低优先级档当作借用源；其槽位是延迟敏感资源，不得被低优先级档抢占。
            if (precisionTierCatalog.isDirectBucket(allTiers[i])) continue;
            ResizableSemaphore other = tierSemaphores.get(allTiers[i]);
            if (other == null) continue;   // 自定义 catalog 可能未含该档（signal 缺失档无信号量）

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

    /** 诊断：submitFinalize 结算任务抛异常次数（finalize 异常被吞，onDelivered 可能不触发）。 */
    private final AtomicLong finalizeTaskExceptions = new AtomicLong();
    public long getFinalizeTaskExceptions() { return finalizeTaskExceptions.get(); }

    /** 诊断：persistStateChange 持久化失败次数（I6 容错吞掉，onDelivered 仍触发）。 */
    private final AtomicLong persistFailures = new AtomicLong();
    public long getPersistFailures() { return persistFailures.get(); }

    /** 诊断：最近的 finalize 异常样本（类名:消息），有界，供取证。 */
    private final java.util.concurrent.ConcurrentLinkedQueue<String> finalizeExceptionSamples = new java.util.concurrent.ConcurrentLinkedQueue<>();
    public java.util.List<String> getFinalizeExceptionSamples() {
        return new ArrayList<>(finalizeExceptionSamples);
    }

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
        Runnable guarded = () -> runFinalizeTask(intent, tier, task);
        try {
            sharedExecutor.submit(guarded);
        } catch (RejectedExecutionException e) {
            // stop() 排空窗口的 TOCTOU：consumer 越过 while(running) 后新入的在途投递，
            // 其结算提交会被已关闭的 executor 拒绝。原地执行保证 ACK/重试决策不丢——
            // 否则投递已成功的 intent 重启后按旧 SCHEDULED 槽重投（重复投递）。
            logger.warn("sharedExecutor rejected finalize for intent {}; running inline to preserve outcome",
                intent.getIntentId(), e);
            runFinalizeTask(intent, tier, task);
        }
    }

    /** 结算任务主体：try/catch/finally 包裹，结束时 -1 在途计数（Fix 5）。 */
    private void runFinalizeTask(Intent intent, PrecisionTier tier, Runnable task) {
        try {
            task.run();
        } catch (Exception e) {
            finalizeTaskExceptions.incrementAndGet();
            if (finalizeExceptionSamples.size() < 20) {
                finalizeExceptionSamples.add(e.getClass().getSimpleName() + ": "
                    + (e.getMessage() != null ? e.getMessage() : "(null)"));
            }
            logger.error("Error in delivery callback for intent {}", intent.getIntentId(), e);
        } finally {
            tierInFlight.get(tier).decrementAndGet();
        }
    }

    /** 注入状态变更持久化通道(接到 IntentCommandService 的 DURABLE 落盘)。 */
    public void setStateChangeSink(StateChangeSink sink) {
        this.stateChangeSink = sink;
    }

    /**
     * 状态变更持久化(I3 不变量收口):revision 递增 + 非阻塞 put。
     * 所有调用方只需调此方法,revision 递增由本方法统一负责,
     * 杜绝未来调用方遗忘递增导致 recovery 去重失效。
     * 须在 synchronized(intent) 内调用以维持 I2/I3 原子性;阻塞的持久化等待
     * 由 {@link #awaitStateChangeCommit()} 在锁外完成,避免 VT 在锁内 pin carrier。
     *
     * <p><b>I6(容错):持久化失败不阻塞调度流程。</b> 终态/状态 put 失败(如 SEC 桶
     * SlotOverflowException)时,吞掉异常并记录 —— 否则异常在 synchronized 块内传播,
     * 会跳过后续的观察器通知(如 SUCCESS 的 onDelivered),导致已投递 intent 的
     * 通知被吞、基准在途槽位永久泄漏、引擎死锁停摆(Issue B 根因,见
     * docs/development/issue-b-rootcause-2026-08-07.md)。崩溃一致性冲击:终态未落盘,
     * 重启 recovery 可能按旧 revision 重投 —— 属可接受的耐久劣化,优于不可恢复死锁。
     */
    private void persistStateChange(Intent intent) {
        intent.incrementRevision();
        StateChangeSink s = stateChangeSink;
        if (s != null) {
            try {
                s.persist(intent);   // 仅非阻塞 put；阻塞等待由 awaitStateChangeCommit 在锁外完成
            } catch (Exception e) {
                persistFailures.incrementAndGet();
                logger.error("persistStateChange failed for intent {} (revision {}): {}",
                    intent.getIntentId(), intent.getRevision(), e.getMessage(), e);
            }
        }
    }

    /** 终态原地覆写（I6 容错镜像 persistStateChange）：revision 递增 + 非阻塞原地覆写。 */
    private void persistTerminal(Intent intent) {
        intent.incrementRevision();
        StateChangeSink s = stateChangeSink;
        if (s != null) {
            try {
                s.persistTerminalInPlace(intent);
            } catch (Exception e) {
                persistFailures.incrementAndGet();
                logger.error("persistTerminalInPlace failed for intent {} (revision {}): {}",
                    intent.getIntentId(), intent.getRevision(), e.getMessage(), e);
            }
        }
    }

    /** 终态槽回收；须在 awaitStateChangeCommit 之后调用。 */
    private void reclaimTerminal(String intentId) {
        StateChangeSink s = stateChangeSink;
        if (s != null) {
            try { s.reclaimTerminal(intentId); }
            catch (Exception e) { logger.warn("reclaimTerminal failed for {}: {}", intentId, e); }
        }
    }

    /** 阻塞到最近一次 persistStateChange 的 put 落盘；须在 synchronized(intent) 之外调用。 */
    private void awaitStateChangeCommit() {
        StateChangeSink s = stateChangeSink;
        if (s == null) return;
        try {
            s.awaitCommit();
        } catch (Exception e) {
            // I6 容错(镜像 persistStateChange/persistTerminal):await 失败(如慢盘双超时)
            // 不阻塞调度流程——reclaimTerminal 与 onDelivered 照常执行,否则复现 Issue B
            // 死锁(onDelivered 被吞 + 槽位泄漏)。代价:终态落盘确认丢失,崩溃恢复可能按旧
            // revision 重投,属可接受耐久劣化。
            persistFailures.incrementAndGet();
            logger.error("awaitStateChangeCommit failed: {}", e.getMessage(), e);
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
        // I5: collect-then-defer -- 锁内取快照，锁外派发
        java.util.function.Consumer<IntentObserver> deferredNotify = null;
        boolean persisted = false;
        String terminalId = null;
        synchronized (intent) {
            // R6: 终态守卫——consumer 过期闸门（投递前最后一道闸）与扫描线程的
            // checkExpiredIntents 可对同一 intent 并发进入 handleExpired：败者二次
            // transitionTo 从终态抛 ISE。consumer 路径无 try/catch，ISE 直接杀死
            // 消费者 VT（固定 Thread[]，无监督）→ 档位投递容量静默永久退化；扫描
            // 路径有 try/catch 吞掉，故败者归属决定后果。锁内复查，竞态败者幂等跳过
            // （终态已由先到者落盘+回收）。
            if (intent.getStatus().isTerminal()) {
                return;
            }
            unindexIntent(intent.getIntentId(), executeAtMs(intent));
            logger.info("Intent expired: id={}, deadline={}", intent.getIntentId(), intent.getDeadline());

            // R18: expiredAction 可为 null(Intent.setExpiredAction 无守卫、create 曾不校验)。
            // switch(null) 抛 NPE——consumer 路径无 try/catch 会杀死消费者 VT(固定数组无监督,
            // 档位容量永久退化),scanner 路径吞异常反复重试致 intent 永不终态化。防御性默认
            // DISCARD(与 Intent 构造器/解码路径的默认语义一致)。
            switch (intent.getExpiredAction() != null ? intent.getExpiredAction() : ExpiredAction.DISCARD) {
                case DISCARD:
                    intent.transitionTo(IntentStatus.EXPIRED);
                    break;
                case DEAD_LETTER:
                    intent.transitionTo(IntentStatus.DEAD_LETTERED);
                    break;
            }
            intentStore.update(intent);
            persistTerminal(intent);              // 终态原地覆写（不追加）
            terminalId = intent.getIntentId();
            persisted = true;
            // R19: 终态须反映到 trace
            traceStore.updateStatus(intent.getIntentId(), intent.getStatus());

            if (!observers.isEmpty()) {
                final Intent snapshot = intent.copy();
                deferredNotify = o -> o.onExpired(snapshot);
            }
        }
        // 持久化等待移到锁外：VT 在此正常 unmount，避免在 synchronized 内 park 而 pin carrier。
        if (persisted) {
            awaitStateChangeCommit();
        }
        // 终态槽回收：须在 awaitStateChangeCommit 之后，确保原地覆写已落盘再释放槽位。
        if (terminalId != null) {
            reclaimTerminal(terminalId);
        }
        // I5: onExpired 在锁外派发
        if (deferredNotify != null) {
            notifyObservers(deferredNotify);
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
        // I5: collect-then-defer -- 锁内取快照 + 收集 deferred action，锁外派发
        java.util.function.Consumer<IntentObserver> deferredNotify = null;
        boolean needReschedule = false;
        boolean persisted = false;
        String terminalId = null;
        try {
            synchronized (intent) {
                // 终态守卫：在途投递期间 intent 可能已被并发终态化——deadline 在飞行中
                // 越过（checkExpiredIntents → handleExpired 标 EXPIRED）或 cancel 竞态
                // （标 CANCELED）。终态胜者在途投递结果：直接跳过结算（观察器已由
                // onExpired/取消路径通知），否则 transitionTo(DUE) 从终态抛 ISE →
                // onDelivered 丢失 + ACK 未落盘 → 重启按旧 SCHEDULED 槽重复投递。
                if (intent.getStatus().isTerminal()) {
                    enqueueTimeNanos.remove(intent.getIntentId());
                    return;
                }
                // 内存中状态转换（不持久化 — 终态才做一次 upsert）
                // R6: 结算前奏容错——updateIntent 的 updater 无状态白名单校验，可把
                // SCHEDULED 置为 DUE（状态机允许 SCHEDULED→DUE；updater 只在 SCHEDULED
                // 上运行，DUE 是唯一可达的非终态迁移）。DUE 起步时跳过 transitionTo(DUE)
                // ——DUE→DUE 非法，会抛 ISE 被 runFinalizeTask 吞掉：ACK 未落盘、
                // onDelivered 丢失、intent 卡死 DUE（索引已被认领消耗），重启按旧
                // SCHEDULED 槽重复投递。直接推进 DISPATCHING 续链。
                if (intent.getStatus() != IntentStatus.DUE) {
                    intent.transitionTo(IntentStatus.DUE);
                }
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
                        persistTerminal(intent);              // 终态原地覆写（不追加）
                        terminalId = intent.getIntentId();
                        persisted = true;
                        unindexIntent(intent.getIntentId(), executeAtMs(intent));
                        // Trace: record acked
                        traceStore.recordAcked(intent.getIntentId());
                        traceStore.updateStatus(intent.getIntentId(), IntentStatus.ACKED);
                        logger.debug("Intent {} delivered successfully", intent.getIntentId());
                        if (!observers.isEmpty()) {
                            final Intent snapshot = intent.copy();
                            deferredNotify = o -> o.onDelivered(snapshot, finalResult);
                        }
                        break;

                    case RETRY: {
                        // 与 handleDeliveryFailure(异常路径)对齐:RETRY 结果同样受 maxAttempts
                        // 约束——否则 handler 恒返回 RETRY 时无限重排程,绕过重试上限契约,
                        // 与异常路径(attempts >= maxAttempts → DEAD_LETTERED)语义不一致。
                        int maxAttempts = intent.getRedelivery() != null
                            ? intent.getRedelivery().getMaxAttempts()
                            : 5;
                        if (intent.getAttempts() >= maxAttempts) {
                            intent.transitionTo(IntentStatus.DEAD_LETTERED);
                            intentStore.update(intent);
                            persistTerminal(intent);              // 终态原地覆写(不追加)
                            terminalId = intent.getIntentId();
                            persisted = true;
                            unindexIntent(intent.getIntentId(), executeAtMs(intent));
                            logger.warn("Intent dead-lettered after max attempts (RETRY result): id={}",
                                intent.getIntentId());
                            // R19: 终态须反映到 trace,否则死信 Intent 的 trace 停在 CREATED
                            traceStore.updateStatus(intent.getIntentId(), IntentStatus.DEAD_LETTERED);
                            if (!observers.isEmpty()) {
                                final Intent snapshot = intent.copy();
                                deferredNotify = o -> o.onDeadLettered(snapshot);
                            }
                            break;
                        }
                        long oldExecuteAtMs = executeAtMs(intent);
                        // R19: 在途改期尊重——updateIntent(newExecuteAt) 在投递期间会把共享
                        // 活对象的 executeAt 改为用户新值并 DURABLE 持久化(claimed 后
                        // removeFromSchedule 返回 false 不重排程;契约是"更新在重试路径确定
                        // 生效")。若 executeAt 显著晚于当前(超过一档精度窗口,覆盖桶粒度
                        // ≤window 的前沿偏差——未被改期的在途 Intent 不可能超出该偏差),说明
                        // 用户已改期:按新时间重排程,而非用 backoff 覆写——否则改期被静默
                        // 丢弃,Intent 在数秒内被重投,与用户意图相悖。
                        Instant now = Instant.now();
                        long precisionWindowMs = precisionTierCatalog.precisionWindowMs(tier);
                        if (intent.getExecuteAt().isAfter(now.plusMillis(precisionWindowMs))) {
                            intent.transitionTo(IntentStatus.SCHEDULED);
                            intentStore.update(intent);
                            // Fix 6: 重排程是新的调度承诺而非中间态——必须 DURABLE 落盘,
                            // 否则崩溃恢复看到旧 executeAt 的 SCHEDULED 槽(已过期),又被
                            // WheelRecovery 的 overdue 路径丢弃,重试链静默丢失。(I6 容错下持久化失败仅记 persistFailures,不阻塞调度)
                            persistStateChange(intent);
                            persisted = true;
                            unindexIntent(intent.getIntentId(), oldExecuteAtMs);
                            // I5: schedule() 移到锁外 (deferred)
                            needReschedule = true;
                            logger.info("Honoring mid-flight reschedule for intent={}, executeAt={} (no backoff)",
                                intent.getIntentId(), intent.getExecuteAt());
                            break;
                        }
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
                        // WheelRecovery 的 overdue 路径丢弃,重试链静默丢失。(I6 容错下持久化失败仅记 persistFailures,不阻塞调度)
                        persistStateChange(intent);
                        persisted = true;
                        unindexIntent(intent.getIntentId(), oldExecuteAtMs);
                        // I5: schedule() 移到锁外 (deferred)
                        needReschedule = true;
                        break;
                    }

                    case DEAD_LETTER:
                        intent.transitionTo(IntentStatus.DEAD_LETTERED);
                        intentStore.update(intent);
                        persistTerminal(intent);              // 终态原地覆写（不追加）
                        terminalId = intent.getIntentId();
                        persisted = true;
                        unindexIntent(intent.getIntentId(), executeAtMs(intent));
                        logger.warn("Intent {} dead-lettered", intent.getIntentId());
                        // R19: 终态须反映到 trace
                        traceStore.updateStatus(intent.getIntentId(), IntentStatus.DEAD_LETTERED);
                        if (!observers.isEmpty()) {
                            final Intent snapshot = intent.copy();
                            deferredNotify = o -> o.onDeadLettered(snapshot);
                        }
                        break;

                    case EXPIRED:
                        intent.transitionTo(IntentStatus.EXPIRED);
                        intentStore.update(intent);
                        persistTerminal(intent);              // 终态原地覆写（不追加）
                        terminalId = intent.getIntentId();
                        persisted = true;
                        unindexIntent(intent.getIntentId(), executeAtMs(intent));
                        logger.info("Intent {} expired", intent.getIntentId());
                        // R19: 终态须反映到 trace
                        traceStore.updateStatus(intent.getIntentId(), IntentStatus.EXPIRED);
                        if (!observers.isEmpty()) {
                            final Intent snapshot = intent.copy();
                            deferredNotify = o -> o.onExpired(snapshot);
                        }
                        break;
                }
            }
            // 持久化等待移到锁外：VT 在此正常 unmount，避免在 synchronized 内 park 而 pin carrier。
            if (persisted) {
                awaitStateChangeCommit();
            }
            // 终态槽回收：须在 awaitStateChangeCommit 之后，确保原地覆写已落盘再释放槽位。
            if (terminalId != null) {
                reclaimTerminal(terminalId);
            }
            // I5: 观察器通知在锁外派发
            if (deferredNotify != null) {
                notifyObservers(deferredNotify);
            }
            // I5: schedule() 在锁外调用 (schedule() 有自己的 synchronized + 终态检查)
            if (needReschedule) {
                schedule(intent);
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
     * @param permanent true 表示 RedeliveryDecider 判定不可重投(永久失败),直接终态,
     *                  不再按 maxAttempts 反复重试
     * @implNote 维护 I2/I3：死信终态落盘；重试路径 persistStateChange 递增 revision。
     */
    private void handleDeliveryFailure(Intent intent, boolean permanent) {
        // I5: collect-then-defer -- 锁内取快照 + 收集 deferred action，锁外派发
        java.util.function.Consumer<IntentObserver> deferredNotify = null;
        boolean needReschedule = false;
        boolean persisted = false;
        String terminalId = null;
        synchronized (intent) {
            // 终态守卫（同 finalizeIntent）：在途投递失败结算时 intent 可能已被
            // handleExpired/cancel 终态化——跳过重试/死信决策，终态保持。
            if (intent.getStatus().isTerminal()) {
                enqueueTimeNanos.remove(intent.getIntentId());
                return;
            }
            int maxAttempts = intent.getRedelivery() != null
                ? intent.getRedelivery().getMaxAttempts()
                : 5;

            // R6: 结算前奏容错（同 finalizeIntent）：updater 置 DUE 的 intent 在投递
            // 失败结算时同样从 DUE 起步，跳过 transitionTo(DUE) 直接推进 DISPATCHING。
            if (intent.getStatus() != IntentStatus.DUE) {
                intent.transitionTo(IntentStatus.DUE);
            }
            intent.transitionTo(IntentStatus.DISPATCHING);
            intent.incrementAttempts();

            if (permanent || intent.getAttempts() >= maxAttempts) {
                intent.transitionTo(IntentStatus.DEAD_LETTERED);
                intentStore.update(intent);
                persistTerminal(intent);              // 终态原地覆写（不追加）
                terminalId = intent.getIntentId();
                persisted = true;
                unindexIntent(intent.getIntentId(), executeAtMs(intent));
                logger.warn("Intent dead-lettered after max attempts: id={}", intent.getIntentId());
                // R19: 终态须反映到 trace
                traceStore.updateStatus(intent.getIntentId(), IntentStatus.DEAD_LETTERED);
                if (!observers.isEmpty()) {
                    final Intent snapshot = intent.copy();
                    deferredNotify = o -> o.onDeadLettered(snapshot);
                }
            } else {
                long oldExecuteAtMs = executeAtMs(intent);
                // R19: 在途改期尊重(同 finalizeIntent RETRY 分支)——用户改期后的
                // executeAt 显著晚于当前时,按新时间重排程而非用 backoff 覆写。
                Instant now = Instant.now();
                long precisionWindowMs =
                    precisionTierCatalog.precisionWindowMs(intent.getPrecisionTier());
                if (intent.getExecuteAt().isAfter(now.plusMillis(precisionWindowMs))) {
                    intent.transitionTo(IntentStatus.SCHEDULED);
                    intentStore.update(intent);
                    // Fix 6: 重排程落盘,见 finalizeIntent RETRY 分支同款说明
                    persistStateChange(intent);
                    persisted = true;
                    unindexIntent(intent.getIntentId(), oldExecuteAtMs);
                    // I5: schedule() 移到锁外 (deferred)
                    needReschedule = true;
                    logger.info("Honoring mid-flight reschedule for intent={}, executeAt={} (no backoff)",
                        intent.getIntentId(), intent.getExecuteAt());
                } else {
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
                    persisted = true;
                    unindexIntent(intent.getIntentId(), oldExecuteAtMs);
                    // I5: schedule() 移到锁外 (deferred)
                    needReschedule = true;
                }
            }
        }
        // 持久化等待移到锁外：VT 在此正常 unmount，避免在 synchronized 内 park 而 pin carrier。
        if (persisted) {
            awaitStateChangeCommit();
        }
        // 终态槽回收：须在 awaitStateChangeCommit 之后，确保原地覆写已落盘再释放槽位。
        if (terminalId != null) {
            reclaimTerminal(terminalId);
        }
        // I5: 观察器通知在锁外派发
        if (deferredNotify != null) {
            notifyObservers(deferredNotify);
        }
        // I5: schedule() 在锁外调用
        if (needReschedule) {
            schedule(intent);
        }
    }

    /**
     * 状态变更持久化通道：把"非阻塞 put"与"阻塞等待落盘"分离，使调度器可在
     * synchronized(intent) 内只做 put、在锁外 awaitCommit（VT 不再 pin carrier）。
     *
     * <p>根因：VT 在 synchronized 块内 park 无法 unmount，会 pin 住 carrier；把阻塞的
     * awaitCommit 移到锁外，VT 在 LockSupport.park 上正常 unmount。</p>
     */
    public interface StateChangeSink {
        /** 非阻塞：写 PHTW + 索引，不等待落盘。须在 synchronized(intent) 内调用以保持 I2/I3 原子性。 */
        void persist(Intent intent);
        /** 非阻塞：终态原地覆写最新槽(不追加;无索引/tail 时回退追加)；须在 synchronized(intent) 内调用。 */
        void persistTerminalInPlace(Intent intent);
        /** 阻塞到持久化完成；仅在 synchronized(intent) 之外调用（VT 可正常 unmount）。 */
        void awaitCommit();
        /** 终态槽回收（单槽清空入 free-list；多槽保留 tombstone）；仅在 awaitCommit 之后调用。 */
        void reclaimTerminal(String intentId);
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

    public PrecisionTierCatalog getPrecisionTierCatalog() {
        return precisionTierCatalog;
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
