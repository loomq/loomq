package com.loomq.application.scheduler;

import com.loomq.common.MetricsCollector;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.PrecisionTierCatalog;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Intent cohort consolidator — CSA-inspired batched wakeup.
 *
 * Instead of one virtual thread per pending intent (O(N) VTs), intents are
 * grouped into "cohorts" keyed by their target bucket key. A single platform
 * daemon thread handles all wakeups: it sleeps until the earliest cohort's time,
 * then atomically removes the entire cohort and flushes it to BucketGroupManager.
 *
 * This maps to DeepSeek V4's CSA philosophy: compress groups (tokens → intents),
 * index sparsely (bucket keys), then process only the relevant subset.
 */
public final class CohortManager {

    private static final Logger logger = LoggerFactory.getLogger(CohortManager.class);

    /** Cohorts keyed by bucket key (floorToBucket(executeAt)). */
    private final ConcurrentSkipListMap<Long, ConcurrentLinkedDeque<Intent>> cohorts;

    /** 反向索引：intentId → cohortKey，用于支持按 ID 从 cohort 中移除。 */
    private final ConcurrentHashMap<String, Long> intentIdToCohortKey;

    private final BucketGroupManager bucketGroupManager;
    private final PrecisionTierCatalog catalog;
    private final Consumer<Collection<Intent>> scanTrigger;
    private final MetricsCollector metrics;

    /** 非 final：stop() 后 start() 需重建（Java 线程不可重启）。 */
    private Thread wakeThread;
    private final AtomicBoolean running;

    // Observability counters (CSA impact measurement)
    private final AtomicLong totalRegistered = new AtomicLong(0);
    private final AtomicLong totalFlushed = new AtomicLong(0);
    private final AtomicLong wakeEventCount = new AtomicLong(0);

    CohortManager(BucketGroupManager bucketGroupManager, PrecisionTierCatalog catalog,
                  Consumer<Collection<Intent>> scanTrigger, MetricsCollector metrics) {
        this.bucketGroupManager = bucketGroupManager;
        this.catalog = catalog;
        this.scanTrigger = scanTrigger;
        this.metrics = metrics;
        this.cohorts = new ConcurrentSkipListMap<>();
        this.intentIdToCohortKey = new ConcurrentHashMap<>();
        this.running = new AtomicBoolean(false);

        this.wakeThread = Thread.ofPlatform()
            .name("cohort-waker")
            .daemon(true)
            .unstarted(this::wakeLoop);
    }

    void start() {
        if (running.compareAndSet(false, true)) {
            // stop() 后重启：旧线程已 join，重建（Java 线程不可二次 start）。
            wakeThread = Thread.ofPlatform()
                .name("cohort-waker")
                .daemon(true)
                .unstarted(this::wakeLoop);
            wakeThread.start();
            logger.info("CohortManager started");
        }
    }

    void stop() {
        running.set(false);
        LockSupport.unpark(wakeThread);
        // 等旧线程退出，杜绝重启后双 wake 线程（旧线程观察到 running=true 会继续跑）
        try {
            wakeThread.join(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * 清空所有未 flush 的 cohort。
     *
     * 用于 leader 角色切换或快照后重建调度状态。
     */
    void clear() {
        cohorts.clear();
        intentIdToCohortKey.clear();
    }

    /**
     * Register an intent into its cohort. If it creates a new earliest cohort,
     * unpark the wake thread so it re-evaluates its sleep target.
     */
    void register(Intent intent) {
        long key = cohortKey(intent);
        cohorts.computeIfAbsent(key, k -> new ConcurrentLinkedDeque<>())
               .addLast(intent);
        intentIdToCohortKey.put(intent.getIntentId(), key);
        totalRegistered.incrementAndGet();
        // Signal: a new cohort may be earlier than the current sleep target
        LockSupport.unpark(wakeThread);
    }

    /**
     * 从 cohort 中按 intentId 移除 Intent。
     *
     * 当 Intent 被取消或立即触发时调用，
     * 防止已取消的 Intent 在 cohort flush 时被重新激活。
     *
     * @param intentId 要移除的 Intent ID
     * @return true 如果成功移除
     */
    boolean remove(String intentId) {
        Long key = intentIdToCohortKey.remove(intentId);
        if (key == null) {
            return false;
        }
        ConcurrentLinkedDeque<Intent> cohort = cohorts.get(key);
        if (cohort != null) {
            return cohort.removeIf(i -> intentId.equals(i.getIntentId()));
        }
        return false;
    }

    public int cohortCount() {
        return cohorts.size();
    }

    public int pendingIntentCount() {
        return cohorts.values().stream().mapToInt(ConcurrentLinkedDeque::size).sum();
    }

    /**
     * Snapshot of a cohort wake window for the timeline API.
     */
    public record CohortWake(long wakeAtMs, int intentCount, String tier) {}

    /**
     * Return upcoming cohort wakes in the given time range.
     * Uses {@code cohorts.subMap} which is O(log N) on the skip-list.
     */
    public List<CohortWake> getUpcomingWakes(Instant from, Instant to) {
        long fromMs = from.toEpochMilli();
        long toMs = to.toEpochMilli();
        List<CohortWake> result = new ArrayList<>();
        var snapshot = new ArrayList<>(cohorts.subMap(fromMs, true, toMs, true).entrySet());
        for (var entry : snapshot) {
            ConcurrentLinkedDeque<Intent> cohort = entry.getValue();
            if (cohort.isEmpty()) {
                continue;
            }
            int count = cohort.size();
            String tier = resolveDominantTier(cohort);
            result.add(new CohortWake(entry.getKey(), count, tier));
        }
        return result;
    }

    private static String resolveDominantTier(ConcurrentLinkedDeque<Intent> cohort) {
        var first = cohort.peekFirst();
        return first != null ? first.getPrecisionTier().name() : "UNKNOWN";
    }

    public long getTotalRegistered() { return totalRegistered.get(); }
    public long getTotalFlushed()    { return totalFlushed.get(); }
    public long getWakeEventCount()  { return wakeEventCount.get(); }

    private long cohortKey(Intent intent) {
        long precisionWindowMs = catalog.precisionWindowMs(intent.getPrecisionTier());
        long executeAtMs = intent.getExecuteAt().toEpochMilli();
        // Wake at executeAt - precisionWindowMs, matching the old VT sleep cadence.
        // This ensures intents enter the bucket system at the same time as before,
        // preventing premature bucket insertion and unnecessary re-add cycles.
        long wakeAtMs = executeAtMs - precisionWindowMs;
        // Floor to bucket for cohort grouping
        return (wakeAtMs / precisionWindowMs) * precisionWindowMs;
    }

    private void wakeLoop() {
        while (running.get()) {
            try {
                var firstEntry = cohorts.firstEntry();
                if (firstEntry == null) {
                    LockSupport.park();
                    continue;
                }

                long bucketKey = firstEntry.getKey();
                long nowMs = System.currentTimeMillis();

                if (bucketKey > nowMs) {
                    // Sleep until the earliest cohort's time
                    long sleepMs = bucketKey - nowMs;
                    LockSupport.parkNanos(Duration.ofMillis(sleepMs).toNanos());
                    continue;
                }

                // Due: atomically remove and flush the cohort
                ConcurrentLinkedDeque<Intent> cohort = cohorts.remove(bucketKey);
                if (cohort == null || cohort.isEmpty()) {
                    continue;
                }

                List<Intent> validIntents = new ArrayList<>(cohort.size());
                for (Intent intent : cohort) {
                    intentIdToCohortKey.remove(intent.getIntentId());
                    // Only skip terminal intents (cancelled/expired after registration).
                    // Timing-based filtering is handled by BucketGroup.scanDue().
                    if (intent.getStatus().isTerminal()) {
                        continue;
                    }
                    validIntents.add(intent);
                }
                if (!validIntents.isEmpty()) {
                    totalFlushed.addAndGet(validIntents.size());
                    wakeEventCount.incrementAndGet();
                    long flushStartNanos = System.nanoTime();
                    bucketGroupManager.addAll(validIntents);
                    if (scanTrigger != null) {
                        scanTrigger.accept(validIntents);
                    }
                    long flushDurationUs = (System.nanoTime() - flushStartNanos) / 1_000;
                    metrics.recordCohortFlushDuration(flushDurationUs);
                    if (logger.isDebugEnabled()) {
                        logger.debug("Cohort flushed: bucketKey={}, count={}, duration={}us",
                            bucketKey, validIntents.size(), flushDurationUs);
                    }
                }
            } catch (Exception e) {
                logger.error("CohortManager wake loop error", e);
                LockSupport.parkNanos(Duration.ofMillis(100).toNanos());
            }
        }
        logger.info("CohortManager stopped");
    }
}
