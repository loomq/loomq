package com.loomq.application.scheduler;

import com.loomq.common.MetricsCollector;
import com.loomq.common.exception.BackPressureException;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.domain.intent.PrecisionTierCatalog;
import com.loomq.store.IntentStore;
import com.loomq.tracing.IntentTraceStore;
import java.time.Duration;
import java.time.Instant;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 档位扫描协调器:adaptive 事件驱动扫描 + fixed-rate 扫描 + 过期分频检查。
 * 到期 intent 经 {@link DispatchPipeline#offer} 入队;背压失败分支(观察器通知 + 强制回桶)
 * 在本类收口;过期终态化委托 {@link SettlementEngine#handleExpired}。
 *
 * <p>running 经构造器注入的 {@link BooleanSupplier} 捕获调度器 volatile 字段;
 * paused 标志本类持有,调度器 pause/resume/isPaused 委托至此。</p>
 */
final class ScanCoordinator {

    private static final Logger logger = LoggerFactory.getLogger(ScanCoordinator.class);

    private final PrecisionTierCatalog precisionTierCatalog;
    private final BucketGroupManager bucketGroupManager;
    private final MetricsCollector metrics;
    private final IntentTraceStore traceStore;
    private final DispatchLagTracker lagTracker;
    private final ExpiryIndex expiryIndex;
    private final IntentStore intentStore;
    private final DispatchPipeline pipeline;
    private final SettlementEngine settlement;
    private final ObserverNotifier notifier;
    private final BooleanSupplier running;

    // 按精度档位的扫描调度器
    private final Map<PrecisionTier, ScheduledExecutorService> scanSchedulers;
    private final Map<PrecisionTier, ScheduledFuture<?>> scanFutures;

    // scanTrigger 去重：同一 tier 同时最多一个待执行的 triggered scan
    private final Map<PrecisionTier, AtomicBoolean> pendingScanTrigger = new ConcurrentHashMap<>();

    // Adaptive 扫描器：park 目标（MAX_VALUE = 无目标）+ 平台线程引用
    private final Map<PrecisionTier, AtomicLong> scannerParkTarget = new ConcurrentHashMap<>();
    private final Map<PrecisionTier, Thread> scannerThreads = new ConcurrentHashMap<>();

    // 过期检查分频计数器
    private final Map<PrecisionTier, AtomicLong> expiredCheckCounters;

    private volatile boolean paused = false;

    ScanCoordinator(PrecisionTierCatalog precisionTierCatalog,
                    BucketGroupManager bucketGroupManager,
                    MetricsCollector metrics,
                    IntentTraceStore traceStore,
                    DispatchLagTracker lagTracker,
                    ExpiryIndex expiryIndex,
                    IntentStore intentStore,
                    DispatchPipeline pipeline,
                    SettlementEngine settlement,
                    ObserverNotifier notifier,
                    BooleanSupplier running) {
        this.precisionTierCatalog = precisionTierCatalog;
        this.bucketGroupManager = bucketGroupManager;
        this.metrics = metrics;
        this.traceStore = traceStore;
        this.lagTracker = lagTracker;
        this.expiryIndex = expiryIndex;
        this.intentStore = intentStore;
        this.pipeline = pipeline;
        this.settlement = settlement;
        this.notifier = notifier;
        this.running = running;
        this.scanSchedulers = new ConcurrentHashMap<>();
        this.scanFutures = new ConcurrentHashMap<>();
        this.expiredCheckCounters = new EnumMap<>(PrecisionTier.class);

        for (PrecisionTier tier : this.precisionTierCatalog.supportedTiers()) {
            expiredCheckCounters.put(tier, new AtomicLong(0));
        }
    }

    /** 启动各档位扫描循环(重启 = 全新服务:清残留触发标志、暂停态不跨重启延续)。 */
    void start() {
        // 重启 = 全新服务：清空上一生命周期可能残留的触发扫描标志，避免 fixed-rate 档
        // 的 cohort 触发式扫描永久失效。
        pendingScanTrigger.clear();
        // 重启 = 全新服务:暂停态不跨重启延续(否则 pause→stop→start 后静默不投递,
        // 直到显式 resume——运维无法从日志得知调度器处于暂停)。
        paused = false;

        for (PrecisionTier tier : precisionTierCatalog.supportedTiers()) {
            startScanCycle(tier);
        }
    }

    /** 停止全部扫描循环:unpark + join + cancel + shutdown scan schedulers。 */
    void stop() {
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
    }

    void pause() {
        if (!paused) {
            paused = true;
            logger.info("PrecisionScheduler paused");
        }
    }

    void resume() {
        if (paused) {
            paused = false;
            // 立即唤醒 adaptive 扫描线程，避免等待 park 上限（scanIntervalMs×100）
            for (Thread t : scannerThreads.values()) {
                LockSupport.unpark(t);
            }
            logger.info("PrecisionScheduler resumed");
        }
    }

    boolean isPaused() {
        return paused;
    }

    /** 触发一次 scan：adaptive 档 unpark 扫描线程，fixed-rate 档走 pendingScanTrigger 去重提交。 */
    void triggerScan(PrecisionTier tier) {
        if (precisionTierCatalog.isAdaptive(tier)) {
            Thread t = scannerThreads.get(tier);
            if (t != null) LockSupport.unpark(t);
            return;
        }
        AtomicBoolean pending = pendingScanTrigger.computeIfAbsent(tier, k -> new AtomicBoolean(false));
        if (pending.compareAndSet(false, true)) {
            ScheduledExecutorService scanScheduler = scanSchedulers.get(tier);
            if (scanScheduler != null) {
                try {
                    scanScheduler.submit(() -> { pending.set(false); scanAndDispatch(tier); });
                } catch (RejectedExecutionException e) {
                    // stop() 先 shutdown 后 clear 的窗口里 submit 会抛
                    // RejectedExecutionException;不复位则 pending 恒 true,
                    // restart 后该档 cohort-flush 触发式扫描永久失效。
                    pending.set(false);
                    logger.warn("triggerScan rejected for tier {} during shutdown; resetting pending", tier);
                }
            } else {
                // 调度器缺失(stop 竞态/重启窗口)时复位标志——否则标志恒 true,
                // stop→start 后该档 cohort-flush 触发式扫描永久失效(只剩固定 tick)。
                pending.set(false);
            }
        }
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

    /** 事件驱动扫描循环：park 至最早 bucketKey，add 新更早键时被 unpark。 */
    private void adaptiveScanLoop(PrecisionTier tier) {
        BucketGroup group = resolveBucketGroup(tier);
        AtomicLong parkTarget = scannerParkTarget.get(tier);
        while (running.getAsBoolean()) {
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
                    // scanDue 把未到期 intent 重入同一 floor 桶:若最早桶仍 <= now,
                    // park 到最早桶内最小精确 executeAt(避免忙转)。仅未到期路径触发,O(bucket) 有界。
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
                    lagTracker.record(intent.getIntentId(), batchEnqueueNanos);

                    // Trace: record enqueued
                    traceStore.recordEnqueued(intent.getIntentId());

                    // 提交到档位队列（pipeline.offer：3 次重试 + round-robin unpark）
                    boolean offered = pipeline.offer(intent, tier);
                    if (!offered) {
                        metrics.incrementDispatchQueueOfferFailed(tier);
                        metrics.incrementBackpressureEvent(tier);
                        logger.error("CRITICAL: Backpressure — dispatch queue full for tier {}, requeuing intent {} for next scan cycle",
                            tier, intent.getIntentId());
                        // 清理本次 enqueue 时间戳:重入后下次 offer 会重新登记;否则 intent 若在重入期间被取消,条目永不清理(内存泄漏)
                        lagTracker.clear(intent.getIntentId());
                        // I5: 快照在锁内取，dispatch 在锁外
                        final Intent bpSnapshot;
                        synchronized (intent) {
                            bpSnapshot = intent.copy();
                        }
                        notifier.notifyObservers(o -> o.onDeliveryFailed(bpSnapshot,
                            new BackPressureException(
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

            // 更新队列深度指标(队列由 DispatchPipeline 持有)
            metrics.updateDispatchQueueSizeByTier(tier, pipeline.dispatchQueueSize(tier));

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
     * 检查过期任务。
     *
     * 扫描所有非终态且已过期的 intent（包括未入队的 SCHEDULED），
     * 适配轻量级 dispatch() 中中间态不持久化的设计。
     */
    private void checkExpiredIntents(PrecisionTier tier) {
        long nowMs = System.currentTimeMillis();
        var expiredEntries = expiryIndex.expiredEntriesUpTo(nowMs);
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
                    settlement.handleExpired(intent);
                }
            }
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

    private BucketGroup resolveBucketGroup(PrecisionTier tier) {
        BucketGroup group = bucketGroupManager.getBucketGroup(tier);
        if (group == null) {
            group = bucketGroupManager.getBucketGroup(precisionTierCatalog.defaultTier());
        }
        return group;
    }
}
