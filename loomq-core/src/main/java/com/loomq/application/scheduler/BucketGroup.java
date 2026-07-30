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
    private final ConcurrentSkipListMap<Long, ConcurrentHashMap<String, Intent>> buckets;

    /**
     * Intent 到桶的反向索引，便于 O(1) 删除和重调度
     */
    private final ConcurrentHashMap<String, Long> intentIndex;

    /**
     * 当前待处理任务计数
     */
    private final AtomicLong pendingCount;

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

        intentIndex.compute(intentId, (id, previousBucketKey) -> {
            if (previousBucketKey != null && previousBucketKey != bucketKey) {
                removeFromBucket(previousBucketKey, intentId);
            }

            ConcurrentHashMap<String, Intent> bucket = buckets.computeIfAbsent(bucketKey, key -> new ConcurrentHashMap<>());
            if (bucket.put(intentId, intent) == null) {
                pendingCount.incrementAndGet();
            }
            return bucketKey;
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
        Long bucketKey = intentIndex.remove(intentId);
        if (bucketKey != null && removeFromBucket(bucketKey, intentId)) {
            return true;
        }

        if (bucketKey != null) {
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
        NavigableMap<Long, ConcurrentHashMap<String, Intent>> dueBuckets = buckets.headMap(currentBucketKey, true);

        if (dueBuckets.isEmpty()) {
            return Collections.emptyList();
        }

        List<Intent> dueIntents = new ArrayList<>();

        // 遍历到期桶，收集任务
        for (Long bucketKey : new ArrayList<>(dueBuckets.keySet())) {
            ConcurrentHashMap<String, Intent> bucketIntents = buckets.remove(bucketKey);
            if (bucketIntents != null) {
                pendingCount.addAndGet(-bucketIntents.size());

                // 过滤真正到期的任务（考虑精度窗口内的实际执行时间）
                for (Intent intent : bucketIntents.values()) {
                    intentIndex.remove(intent.getIntentId(), bucketKey);
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
        ConcurrentHashMap<String, Intent> bucket = buckets.get(bucketKey);
        if (bucket == null) {
            return false;
        }

        Intent removed = bucket.remove(intentId);
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
                intentIndex.remove(intentId, bucketKey);
                return true;
            }
        }
        return false;
    }
}
