package com.loomq.application.scheduler;

import com.loomq.common.MetricsCollector;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.domain.intent.PrecisionTierCatalog;
import com.loomq.spi.DeliveryHandler;
import com.loomq.tracing.IntentTraceStore;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 档位级消费管线:有界队列 + permit 跨档借用 + 事件驱动消费者。
 * 消费循环:取任务 → acquire(跨档借用)→ I5 快照 → deliverAsync;结算经
 * {@link DeliverySettlement} 回调交给 SettlementEngine,投递前过期闸门
 * 命中时同步终态化(onExpiredInFlight)。
 * permit 借用配对(releasePermit/decrementBorrowed)全部在本类内。
 * 背压(队满)时 {@link #offer} 返回 false,由扫描协调器(ScanCoordinator.scanAndDispatch)处理。
 *
 * <p>public:嵌套诊断类型(BackpressureInfo/PermitTimingStats/BorrowStats)作为
 * {@link PrecisionScheduler#getBackpressureStatus()}/{@link PrecisionScheduler#getPermitTimingStats()}/
 * {@link PrecisionScheduler#getBorrowStats()} 的返回类型,必须保持 public 可访问。
 * 构造器保持包私有,仅经 PrecisionScheduler 创建。</p>
 */
public final class DispatchPipeline {

    private static final Logger logger = LoggerFactory.getLogger(DispatchPipeline.class);

    // AdapTBF constraints: max lend ratio per tier (protects low-priority tiers)
    private static final double MAX_LEND_RATIO = 0.5; // lend at most 50% of tier's slots
    // 投递超时(单条/批量统一)
    private static final long DELIVERY_TIMEOUT_SECONDS = 30;

    private final DeliveryHandler deliveryHandler;
    private final PrecisionTierCatalog precisionTierCatalog;
    private final MetricsCollector metrics;
    private final IntentTraceStore traceStore;
    private final DispatchLagTracker lagTracker;
    private final InFlightCounters inFlightCounters;
    private final DeliverySettlement settlement;
    private final BooleanSupplier running;

    // 档位级并发控制(可动态调整上限)
    private final Map<PrecisionTier, ResizableSemaphore> tierSemaphores;

    // 档位级有界队列(容量 = maxConcurrency × 16,满时触发 backpressure)
    private final Map<PrecisionTier, BlockingQueue<Intent>> tierDispatchQueues;

    // 单发消费者线程引用(按档),供 offer 在入队后 unpark
    private final Map<PrecisionTier, Thread[]> consumerThreads = new ConcurrentHashMap<>();
    private final Map<PrecisionTier, AtomicInteger> consumerPickup = new ConcurrentHashMap<>();

    // Arrow-inspired cross-tier slot borrowing metrics
    private final BorrowStats borrowStats = new BorrowStats();

    // Permit timing diagnostics
    private final PermitTimingStats permitTimingStats = new PermitTimingStats();

    DispatchPipeline(DeliveryHandler deliveryHandler,
                     PrecisionTierCatalog catalog,
                     MetricsCollector metrics,
                     IntentTraceStore traceStore,
                     DispatchLagTracker lagTracker,
                     InFlightCounters inFlightCounters,
                     DeliverySettlement settlement,
                     BooleanSupplier running) {
        this.deliveryHandler = deliveryHandler;
        this.precisionTierCatalog = catalog;
        this.metrics = metrics;
        this.traceStore = traceStore;
        this.lagTracker = lagTracker;
        this.inFlightCounters = inFlightCounters;
        this.settlement = settlement;
        this.running = running;

        // 初始化档位级信号量和队列
        this.tierSemaphores = new EnumMap<>(PrecisionTier.class);
        this.tierDispatchQueues = new EnumMap<>(PrecisionTier.class);

        for (PrecisionTier tier : this.precisionTierCatalog.supportedTiers()) {
            tierSemaphores.put(tier, new ResizableSemaphore(this.precisionTierCatalog.maxConcurrency(tier)));
            int queueCapacity = this.precisionTierCatalog.dispatchQueueCapacity(tier);
            tierDispatchQueues.put(tier, new ArrayBlockingQueue<>(queueCapacity));
        }
    }

    /** 启动各档位消费者(执行器归门面所有,仅注入提交)。 */
    void start(ExecutorService sharedExecutor) {
        for (PrecisionTier tier : precisionTierCatalog.supportedTiers()) {
            startBatchConsumers(tier, sharedExecutor);
        }
    }

    /**
     * 停止消费者:消费线程经 running 标志退出;执行器关闭仍由门面负责。
     * 显式 unpark 保底:消费者 park 上限仅 1ms,此处仅为加速退出路径。
     */
    void stopConsumers() {
        for (Thread[] threads : consumerThreads.values()) {
            for (Thread t : threads) {
                if (t != null) LockSupport.unpark(t);
            }
        }
    }

    /**
     * 入队档位队列(有界,带短重试),成功后 round-robin unpark 一个消费者。
     *
     * @return true 入队成功;false = 队满背压(ScanCoordinator.scanAndDispatch 负责重入桶 + observer 通知)
     */
    boolean offer(Intent intent, PrecisionTier tier) {
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
            // 事件驱动消费端:offer 成功后 unpark 一个消费者(round-robin)。
            // unpark 幂等且廉价,无需判空队列优化;backpressure 重试路径不变。
            Thread[] threads = consumerThreads.get(tier);
            AtomicInteger pickup = consumerPickup.get(tier);
            if (threads != null && threads.length > 0 && pickup != null) {
                Thread t = threads[Math.floorMod(pickup.getAndIncrement(), threads.length)];
                if (t != null) LockSupport.unpark(t);
            }
        }
        return offered;
    }

    /** 诊断/指标访问:当前档位队列深度(scanAndDispatch 的队列深度指标用)。 */
    int dispatchQueueSize(PrecisionTier tier) {
        return tierDispatchQueues.get(tier).size();
    }

    /**
     * 启动指定档位的批量消费者
     */
    private void startBatchConsumers(PrecisionTier tier, ExecutorService sharedExecutor) {
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
     * 投递消费者循环 (真正 fire-and-forget)。
     *
     * Semaphore 控制 in-flight 并发。消费者只负责取任务 + 调用异步投递,
     * permit 在异步投递完成后经 releasePermit 释放;消费者线程与投递完全解耦。
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
     * 单 Intent 消费循环(batchSize == 1)。
     * 流程:poll → acquire → I5 快照(synchronized+copy) → deliver(snapshot) → release in callback。
     */
    private void runSingleIntentConsumer(PrecisionTier tier, BlockingQueue<Intent> queue) {
        while (running.getAsBoolean()) {
            Intent intent = queue.poll();
            if (intent == null) {
                // 事件驱动:offer 后 unpark 立即返回;1ms 上限作 lost-wakeup 保底。
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                continue;
            }

            long acquireStartNs = System.nanoTime();
            ResizableSemaphore acquired;
            try {
                acquired = acquireWithBorrow(tier);
            } catch (InterruptedException e) {
                // intent 已出队但不重新入队:中断仅在 shutdown 路径发生,
                // intent 持久化在 wheel 中,重启经 WheelRecovery 恢复。
                // 清理 enqueue 时间戳:intent 已被丢弃,条目不得泄漏到调度器生命周期末
                // (与 offer 失败路径的清理标准一致)。
                lagTracker.clear(intent.getIntentId());
                Thread.currentThread().interrupt();
                break;
            }
            long acquireEndNs = System.nanoTime();

            // I5: 派发即快照——在 synchronized(intent) 内原子完成终态复查 + 防御性拷贝。
            // 快照与 cancel/update 互斥:要么取自变更前的完整状态,要么复查拦截(不投递)。
            // 撕裂读在结构上消除;SPI 边界只过快照,用户代码无法触达内核活状态。
            Intent snapshot = null;
            boolean expired = false;
            synchronized (intent) {
                if (intent.getStatus().isTerminal()) {
                    lagTracker.clear(intent.getIntentId());
                    releasePermit(tier, acquired);
                    continue;
                }
                // 过期闸门:scanAndDispatch 先入队、后跑 checkExpiredIntents(同 cycle 竞态,
                // 入队恒胜),此处是投递前最后一道闸——deadline 已过则按 ExpiredAction 终态化,
                // 不得投递(deadline = 最晚有效时间契约)。
                expired = intent.isExpired();
                if (!expired) {
                    snapshot = intent.copy();
                }
            }
            if (expired) {
                lagTracker.clear(intent.getIntentId());
                releasePermit(tier, acquired);
                // 锁外终态化:onExpiredInFlight → handleExpired 自带 synchronized + 锁外 awaitCommit(不 pin carrier)
                settlement.onExpiredInFlight(intent, tier);
                continue;
            }

            // Trace: record dequeued (right after pollFirst, before any other processing)
            traceStore.recordDequeued(intent.getIntentId());
            lagTracker.report(intent, tier, metrics);

            permitTimingStats.totalAcquireWaitNanos.addAndGet(acquireEndNs - acquireStartNs);
            final long permitAcquiredNs = acquireEndNs;

            // 在途计数(含借用),stop() 据此排空而非 semaphore
            inFlightCounters.increment(tier);

            long deliverStartNs = System.nanoTime();
            try {
                deliveryHandler.deliverAsync(snapshot)
                    .orTimeout(DELIVERY_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .whenComplete((result, ex) -> {
                        long releaseNs = System.nanoTime();

                        permitTimingStats.totalPermitHoldNanos.addAndGet(releaseNs - permitAcquiredNs);
                        permitTimingStats.deliverySampleCount.incrementAndGet();

                        // Release permit before finalization to maximize concurrency
                        releasePermit(tier, acquired);

                        // 结算委托结算引擎(锁外提交;结算任务 finally 中 -1 在途计数)
                        settlement.onCompleted(intent, tier, result, ex);
                    });
            } catch (RuntimeException deliverEx) {
                // SPI 同步抛异常:按失败结算,消费者继续存活
                releasePermit(tier, acquired);
                settlement.onException(intent, tier, deliverEx);
            }
            permitTimingStats.totalDeliverAsyncNanos.addAndGet(System.nanoTime() - deliverStartNs);
        }
    }

    /**
     * 批量消费循环(batchSize > 1)。
     *
     * 流程:
     * 1. drainTo(batch, batchSize) — 一次性取出最多 batchSize 个 intent
     * 2. 若队列为空,poll 等首个 intent,再 drain 剩余
     * 3. I5: synchronized(intent) 终态复查 + copy, 构建 liveBatch + snapshotBatch
     * 4. 为 liveBatch 中每个 intent acquire 一个信号量 permit
     * 5. 调用 deliverBatchAsync(snapshotBatch)
     * 6. 在回调中释放所有 permit + finalize(liveBatch)
     */
    private void runBatchDrainConsumer(PrecisionTier tier, BlockingQueue<Intent> queue,
                                       int batchSize, int batchWindowMs) {
        while (running.getAsBoolean()) {
            List<Intent> batch = new ArrayList<>(batchSize);

            // Phase 1: drain what's immediately available
            queue.drainTo(batch, batchSize);

            if (batch.isEmpty()) {
                // Phase 2: non-blocking poll for first intent
                Intent first = queue.poll();
                if (first == null) {
                    // Queue is truly empty — park briefly and retry
                    // 事件驱动:offer 后 unpark 立即返回;1ms 上限作 lost-wakeup 保底。
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                    continue;
                }
                batch.add(first);
                // Drain remaining (non-blocking) up to batchSize
                queue.drainTo(batch, batchSize - batch.size());
            }

            // 批量窗口:未满批次等待至多 batchWindowMs 累计更多 intent。
            // 用 parkNanos(可被 offer→unpark 立即唤醒)而非 poll(timeout)(后者阻塞在
            // Condition,unpark 无法唤醒);已满批或单发档(batchWindowMs<=0)跳过,零额外延迟。
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
            // 构建 liveBatch(结算用活对象)和 snapshotBatch(SPI 用快照)并行列表;
            // 许可获取在终态复查 + copy 之后,确保只为非终态 intent 占用 permit。
            // 过期闸门:deadline 已过(投递前最后一道闸)→ 收集到锁外终态化,不得投递。
            // 终态复查与快照在同一个监视器下原子完成,消除 cancel 与派发间的 TOCTOU 窗口(旧 removeIf 路径的竞态)。
            List<Intent> liveBatch = new ArrayList<>(batch.size());
            List<Intent> snapshotBatch = new ArrayList<>(batch.size());
            List<Intent> expiredBatch = new ArrayList<>(0);
            for (Intent intent : batch) {
                synchronized (intent) {
                    if (intent.getStatus().isTerminal()) {
                        lagTracker.clear(intent.getIntentId());
                        continue;
                    }
                    if (intent.isExpired()) {
                        lagTracker.clear(intent.getIntentId());
                        expiredBatch.add(intent);
                        continue;
                    }
                    liveBatch.add(intent);
                    snapshotBatch.add(intent.copy());
                }
            }
            // 锁外终态化(onExpiredInFlight → handleExpired 自带 synchronized + 锁外 awaitCommit)
            for (Intent intent : expiredBatch) {
                settlement.onExpiredInFlight(intent, tier);
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
                // 中断释放同样配对 decrementBorrowed
                for (ResizableSemaphore s : acquiredPermits) {
                    releasePermit(tier, s);
                }
                // 批次已出队即被丢弃(重启经 WheelRecovery 恢复):清理 enqueue
                // 时间戳,避免条目泄漏(与单发中断路径同款)。
                for (Intent dropped : liveBatch) {
                    lagTracker.clear(dropped.getIntentId());
                }
                Thread.currentThread().interrupt();
                break;
            }
            long acquireEndNs = System.nanoTime();

            // Trace: record dequeued for each intent (内核内部操作,用活对象)
            for (Intent intent : liveBatch) {
                traceStore.recordDequeued(intent.getIntentId());
                lagTracker.report(intent, tier, metrics);
            }

            permitTimingStats.totalAcquireWaitNanos.addAndGet(acquireEndNs - acquireStartNs);
            final long permitAcquiredNs = acquireEndNs;

            // 批量在途计数
            for (int i = 0; i < liveBatch.size(); i++) inFlightCounters.increment(tier);

            // Phase 4: batch delivery - per-future 独立结算,一条失败不连坐整批。
            // I5: deliverBatchAsync 收到 snapshotBatch(快照),finalize 用 liveBatch(活对象)。
            long deliverStartNs = System.nanoTime();
            List<CompletableFuture<DeliveryHandler.DeliveryResult>> futures;
            try {
                futures = deliveryHandler.deliverBatchAsync(snapshotBatch);
            } catch (RuntimeException deliverEx) {
                // SPI 同步抛异常:按失败结算每个 intent,消费者继续存活
                for (int i = 0; i < liveBatch.size(); i++) {
                    Intent intent = liveBatch.get(i);
                    releasePermit(tier, acquiredPermits.get(i));
                    settlement.onException(intent, tier, deliverEx);
                }
                permitTimingStats.totalDeliverAsyncNanos.addAndGet(System.nanoTime() - deliverStartNs);
                continue;
            }
            if (futures == null) {
                // SPI returned null instead of throwing - treat entire batch as failures
                for (int i = 0; i < liveBatch.size(); i++) {
                    Intent intent = liveBatch.get(i);
                    releasePermit(tier, acquiredPermits.get(i));
                    settlement.onException(intent, tier,
                        new IllegalStateException("deliverBatchAsync returned null"));
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
                    settlement.onException(intent, tier,
                        new IllegalStateException("deliverBatchAsync returned no future for this intent"));
                    continue;
                }
                f.orTimeout(DELIVERY_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .whenComplete((result, ex) -> {
                        long releaseNs = System.nanoTime();
                        permitTimingStats.totalPermitHoldNanos.addAndGet(releaseNs - permitAcquiredNs);
                        permitTimingStats.deliverySampleCount.incrementAndGet();
                        releasePermit(tier, permit);
                        settlement.onCompleted(intent, tier, result, ex);
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
            // 禁止从 directBucket(MILLI)档借用:MILLI 因枚举序位于末尾,却被当作
            // 最低优先级档当作借用源;其槽位是延迟敏感资源,不得被低优先级档抢占。
            if (precisionTierCatalog.isDirectBucket(allTiers[i])) continue;
            ResizableSemaphore other = tierSemaphores.get(allTiers[i]);
            if (other == null) continue;   // 自定义 catalog 可能未含该档(signal 缺失档无信号量)

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

    // ---- 诊断 ----

    PermitTimingStats permitTimingStats() {
        return permitTimingStats;
    }

    BorrowStats borrowStats() {
        return borrowStats;
    }

    /**
     * 检查档位是否处于背压状态
     */
    boolean isTierUnderBackpressure(PrecisionTier tier) {
        BlockingQueue<Intent> queue = tierDispatchQueues.get(tier);

        // 真实在途数以 inFlightCounters 为准(包含跨档借用),不能只看本档 semaphore。
        boolean concurrencyExhausted =
            inFlightCounters.get(tier) >= precisionTierCatalog.maxConcurrency(tier);
        boolean queueBackedUp = queue.size() >= precisionTierCatalog.maxConcurrency(tier) * 2;

        return concurrencyExhausted || queueBackedUp;
    }

    /**
     * 获取档位背压信息
     */
    Map<PrecisionTier, BackpressureInfo> backpressureStatus() {
        Map<PrecisionTier, BackpressureInfo> status = new EnumMap<>(PrecisionTier.class);

        for (PrecisionTier tier : precisionTierCatalog.supportedTiers()) {
            ResizableSemaphore semaphore = tierSemaphores.get(tier);
            BlockingQueue<Intent> queue = tierDispatchQueues.get(tier);

            int availablePermits = semaphore.availablePermits();
            int queueSize = queue.size();
            boolean underPressure = isTierUnderBackpressure(tier);
            int activeDispatches = inFlightCounters.get(tier);

            status.put(tier, new BackpressureInfo(
                precisionTierCatalog.maxConcurrency(tier),
                availablePermits,
                queueSize,
                underPressure,
                activeDispatches,
                semaphore.getBorrowedCount()
            ));
        }

        return status;
    }

    // ---- 嵌套诊断类型(public:经 PrecisionScheduler 诊断方法对外暴露)----

    /** Permit timing diagnostics */
    public static final class PermitTimingStats {
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
    }

    /** Arrow-inspired cross-tier slot borrowing metrics */
    public static final class BorrowStats {
        public final AtomicLong ownAcquires = new AtomicLong(0);
        public final AtomicLong ownBlockingAcquires = new AtomicLong(0);
        public final AtomicLong borrowedAcquires = new AtomicLong(0);
        public double borrowRate() {
            long own = ownAcquires.get();
            long blocking = ownBlockingAcquires.get();
            long borrowed = borrowedAcquires.get();
            long total = own + blocking + borrowed;
            return total > 0 ? (double) borrowed / total * 100.0 : 0.0;
        }
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
}
