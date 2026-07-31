package com.loomq.application.scheduler;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.domain.intent.PrecisionTierCatalog;
import com.loomq.domain.intent.PrecisionTierProfile;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 精度桶组
 *
 * 使用 ConcurrentSkipListMap 存储待触发任务，按时间窗口分组。
 * 支持 O(log n) 插入和范围删除，无全局锁。
 *
 * @author loomq
 * @since v0.5.1
 */
public class BucketGroup {

    private static final Logger logger = LoggerFactory.getLogger(BucketGroup.class);

    /**
     * 精度档位
     */
    private final PrecisionTier tier;

    /**
     * 精度档位参数
     */
    private final PrecisionTierProfile profile;

    /**
     * 时间桶存储
     * Key: 执行时间戳按精度窗口向下取整
     * Value: 该时间窗口内的 Intent 索引
     */
    private final ConcurrentSkipListMap<Long, ConcurrentHashMap<String, BucketEntry>> buckets;

    /**
     * Intent 到桶的反向索引，便于 O(1) 删除和重调度。
     * Value: ClaimEntry(bucketKey, revisionAtAdd) -- scanDue 认领时用 CAS 比对。
     */
    private final ConcurrentHashMap<String, ClaimEntry> intentIndex;

    /**
     * 当前待处理任务计数
     */
    private final AtomicLong pendingCount;

    /**
     * 索引条目:记录 intent 入桶时的 (bucketKey, revision)。
     * scanDue 认领时用 CAS 比对 -- 若 fireNow/重排程已替换条目(revision 不同),
     * remove(key, expectedValue) 失败,旧桶条目不被误删。
     */
    record ClaimEntry(long bucketKey, long revision) {}

    /**
     * 桶条目:封装 Intent + 入桶时的 revision 快照(不可变)。
     * scanDue 从 BucketEntry 取 revisionAtAdd 构造 CAS 期望值,
     * 而非从可变 Intent 对象或当前索引状态读取。
     */
    record BucketEntry(Intent intent, long revisionAtAdd) {}

    /**
     * 构造函数
     *
     * @param tier 精度档位
     */
    public BucketGroup(PrecisionTier tier) {
        this(tier, PrecisionTierCatalog.defaultCatalog());
    }

    /**
     * 构造函数
     *
     * @param tier    精度档位
     * @param catalog 精度档位目录
     */
    public BucketGroup(PrecisionTier tier, PrecisionTierCatalog catalog) {
        this.tier = tier;
        this.profile = catalog.profile(tier);
        this.buckets = new ConcurrentSkipListMap<>();
        this.intentIndex = new ConcurrentHashMap<>();
        this.pendingCount = new AtomicLong();
    }

    /**
     * 添加 Intent 到对应时间桶
     *
     * @param intent    Intent 实例
     * @param executeAt 执行时间
     */
    public void add(Intent intent, Instant executeAt) {
        long bucketKey = floorToBucket(executeAt.toEpochMilli());
        String intentId = intent.getIntentId();
        long rev = intent.getRevision();

        intentIndex.compute(intentId, (id, prev) -> {
            if (prev != null && prev.bucketKey() != bucketKey) {
                removeFromBucket(prev.bucketKey(), intentId);
            }

            ConcurrentHashMap<String, BucketEntry> bucket = buckets.computeIfAbsent(bucketKey, key -> new ConcurrentHashMap<>());
            if (bucket.put(intentId, new BucketEntry(intent, rev)) == null) {
                pendingCount.incrementAndGet();
            }
            return new ClaimEntry(bucketKey, rev);
        });

        if (logger.isTraceEnabled()) {
            logger.trace("Added intent {} to bucket {} for tier {}",
                intent.getIntentId(), bucketKey, tier);
        }
    }

    /**
     * 从桶中移除 Intent。
     *
     * @param intent 待移除的 Intent
     * @return true 表示已移除
     */
    public boolean remove(Intent intent) {
        String intentId = intent.getIntentId();
        ClaimEntry entry = intentIndex.remove(intentId);
        if (entry != null && removeFromBucket(entry.bucketKey(), intentId)) {
            return true;
        }

        if (entry != null) {
            return removeByScan(intentId);
        }

        return false;
    }

    /**
     * 扫描并获取所有到期任务
     *
     * @param now 当前时间
     * @return 到期的 Intent 列表
     */
    public List<Intent> scanDue(Instant now) {
        long currentBucketKey = floorToBucket(now.toEpochMilli());

        // 获取所有 <= 当前时间桶的条目
        NavigableMap<Long, ConcurrentHashMap<String, BucketEntry>> dueBuckets = buckets.headMap(currentBucketKey, true);

        if (dueBuckets.isEmpty()) {
            return Collections.emptyList();
        }

        List<Intent> dueIntents = new ArrayList<>();

        // 遍历到期桶，收集任务
        for (Long bucketKey : new ArrayList<>(dueBuckets.keySet())) {
            ConcurrentHashMap<String, BucketEntry> bucketIntents = buckets.remove(bucketKey);
            if (bucketIntents != null) {
                for (var entry : bucketIntents.entrySet()) {
                    String intentId = entry.getKey();
                    long revAtAdd = entry.getValue().revisionAtAdd();

                    // 桶已摘除,无论 CAS 成功与否都减计数(桶正在销毁)
                    pendingCount.decrementAndGet();

                    // 原子认领:只有索引条目仍是我们放入的那条时才能移除。
                    // 若 fireNow/重排程已替换条目(revision 不同),CAS 失败,跳过不投递。
                    if (!intentIndex.remove(intentId, new ClaimEntry(bucketKey, revAtAdd))) {
                        logger.debug("scanDue CAS failed for intent {}: already re-claimed by newer revision", intentId);
                        continue;  // 已被 fireNow/重排程替换 - 不投递
                    }

                    // Test hook: fires after CAS success, before dueIntents/re-add.
                    // Used by integration tests to simulate fireNow in the extreme race window.
                    if (testScanPostClaimHook != null) {
                        testScanPostClaimHook.run();
                        testScanPostClaimHook = null;
                    }

                    Intent intent = entry.getValue().intent();
                    if (!intent.getExecuteAt().isAfter(now)) {
                        dueIntents.add(intent);
                    } else {
                        // 未到期的任务重新入桶
                        add(intent, intent.getExecuteAt());
                    }
                }
            }
        }

        if (!dueIntents.isEmpty()) {
            if (dueIntents.size() > 1) {
                dueIntents.sort(Comparator
                    .comparing(Intent::getExecuteAt)
                    .thenComparing(Intent::getCreatedAt)
                    .thenComparing(Intent::getIntentId));
            }
            if (logger.isDebugEnabled()) {
                logger.debug("Scanned {} due intents from tier {} buckets", dueIntents.size(), tier);
            }
        }

        return dueIntents;
    }

    /**
     * 将时间戳按精度窗口向下取整
     *
     * @param timestampMs 时间戳（毫秒）
     * @return 桶 Key
     */
    private long floorToBucket(long timestampMs) {
        long precisionWindowMs = profile.precisionWindowMs();
        return (timestampMs / precisionWindowMs) * precisionWindowMs;
    }

    /**
     * 获取当前桶数量
     *
     * @return 桶数量
     */
    public int getBucketCount() {
        return buckets.size();
    }

    /**
     * 获取当前等待任务总数
     *
     * @return 任务总数
     */
    public int getPendingCount() {
        return Math.toIntExact(pendingCount.get());
    }

    /**
     * 清空所有桶
     */
    public void clear() {
        buckets.clear();
        intentIndex.clear();
        pendingCount.set(0);
    }

    /**
     * 获取精度档位
     *
     * @return 精度档位
     */
    public PrecisionTier getTier() {
        return tier;
    }

    /**
     * 获取精度窗口（毫秒）
     *
     * @return 精度窗口
     */
    public long getPrecisionWindowMs() {
        return profile.precisionWindowMs();
    }

    private boolean removeFromBucket(long bucketKey, String intentId) {
        ConcurrentHashMap<String, BucketEntry> bucket = buckets.get(bucketKey);
        if (bucket == null) {
            return false;
        }

        BucketEntry removed = bucket.remove(intentId);
        if (removed == null) {
            return false;
        }

        pendingCount.decrementAndGet();
        if (bucket.isEmpty()) {
            buckets.remove(bucketKey, bucket);
        }
        return true;
    }

    private boolean removeByScan(String intentId) {
        for (Long bucketKey : new ArrayList<>(buckets.keySet())) {
            if (removeFromBucket(bucketKey, intentId)) {
                // Index entry already removed by caller (remove(Intent)).
                return true;
            }
        }
        return false;
    }

    // ========== Test hooks (package-private) ==========

    /** Test-only: if set, fires after CAS claim succeeds but before dueIntents/re-add. */
    volatile Runnable testScanPostClaimHook;

    /** Test-only: detach bucket without iterating (simulates scanDue's buckets.remove step). */
    boolean testDetachBucket(Instant executeAt) {
        long bucketKey = floorToBucket(executeAt.toEpochMilli());
        return buckets.remove(bucketKey) != null;
    }

    /** Test-only: attempt CAS claim on intentIndex. Returns true if claim succeeded. */
    boolean testClaim(String intentId, long bucketKey, long revision) {
        return intentIndex.remove(intentId, new ClaimEntry(bucketKey, revision));
    }
}
