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
import java.util.concurrent.atomic.AtomicBoolean;
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

    /** add() 的结果：放入桶或触发高水位降级。 */
    public enum AddResult { ADDED, FALLBACK_TO_COHORT }

    /** 新桶创建监听器（仅在新桶 key 出现时触发，成员增删不触发）。 */
    public interface BucketAddListener { void onBucketAdded(long bucketKey); }

    private volatile BucketAddListener bucketAddListener;
    private final int maxBuckets;

    public Long earliestBucketKey() {
        return buckets.isEmpty() ? null : buckets.firstKey();
    }

    public void setBucketAddListener(BucketAddListener l) { this.bucketAddListener = l; }
    public int getMaxBuckets() { return maxBuckets; }

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
        this.maxBuckets = catalog.maxBuckets(tier);
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
    public AddResult add(Intent intent, Instant executeAt) {
        return add(intent, executeAt, true);
    }

    /** 再入路径（cohort flush / scanDue 未到期重入）：intent 已被系统接受，强制入桶，忽略高水位，防丢。 */
    AddResult addForced(Intent intent, Instant executeAt) {
        return add(intent, executeAt, false);
    }

    private AddResult add(Intent intent, Instant executeAt, boolean enforceHighWater) {
        long bucketKey = floorToBucket(executeAt.toEpochMilli());
        String intentId = intent.getIntentId();
        long rev = intent.getRevision();
        AtomicBoolean fallback = new AtomicBoolean(false);
        AtomicBoolean newBucketCreated = new AtomicBoolean(false);

        intentIndex.compute(intentId, (id, prev) -> {
            // 高水位（P3-2）：目标 key 为新桶且桶数已达上限 → 降级（不建桶）。
            // 先判水位再摘旧条目：降级路径移除旧桶条目并清空索引（intent 移交 cohort），
            // 避免双注册（旧桶 + cohort 的 stale-time 派发）且不留 stale index。
            // 仅初次 schedule()/restore() 路由（enforceHighWater=true）受此门控；
            // 再入路径（cohort flush / scanDue 重入）强制入桶，忽略高水位，防丢。
            if (enforceHighWater && buckets.get(bucketKey) == null && buckets.size() >= maxBuckets) {
                fallback.set(true);
                // 降级：intent 移交 cohort，移除旧桶条目并清空索引——避免双注册
                // （旧桶 + cohort 的 stale-time 派发）且不留 stale index。
                if (prev != null) {
                    removeFromBucket(prev.bucketKey(), intentId);
                }
                return null;  // 清空索引条目（intent 现归 cohort）
            }
            if (prev != null && prev.bucketKey() != bucketKey) {
                removeFromBucket(prev.bucketKey(), intentId);
            }
            ConcurrentHashMap<String, BucketEntry> newBucket = new ConcurrentHashMap<>();
            BucketEntry entry = new BucketEntry(intent, rev);
            // 原子入桶：取桶 + put 合并为一次 buckets.compute——scanDue 的 buckets.remove
            // 无法在"取桶"与"put"之间摘除目标桶，杜绝条目落入游离桶后 intentIndex 悬垂、
            // intent 永不投递的丢失窗口（旧实现 computeIfAbsent + put 两步存在该竞态）。
            // 条目要么在 map 内（下次扫描认领），要么在 scanDue 已摘除并正在迭代的桶内
            // （本 cycle 认领），不存在"游离"状态。
            buckets.compute(bucketKey, (k, existing) -> {
                ConcurrentHashMap<String, BucketEntry> b = existing != null ? existing : newBucket;
                if (b.put(intentId, entry) == null) {
                    pendingCount.incrementAndGet();
                }
                if (b == newBucket) {
                    newBucketCreated.set(true);
                }
                return b;
            });
            // Test hook: fires after the entry is attached. Used to simulate scanDue's
            // bucket detach racing the put (BugBucketDetachRaceTest).
            if (testAddPostComputeHook != null) {
                testAddPostComputeHook.run();
            }
            return new ClaimEntry(bucketKey, rev);
        });

        // P3-1：新桶监听器在 compute 完成后、锁外触发（intentIndex bin 锁已释放）。
        // addForced 新桶也触发（cohort flush 落入全新更早桶须唤醒 scanner）。
        if (newBucketCreated.get()) {
            BucketAddListener l = bucketAddListener;
            if (l != null) l.onBucketAdded(bucketKey);
        }
        if (fallback.get()) {
            return AddResult.FALLBACK_TO_COHORT;
        }
        return AddResult.ADDED;
    }

    /**
     * 返回最早桶内所有条目的最小精确 executeAt（epoch ms）。
     * 空桶已由 removeFromBucket / scanDue 从 buckets 摘除，故最早桶恒非空；
     * 防御性返回 null 于空表。
     */
    public Long earliestExecuteAt() {
        var entry = buckets.firstEntry();
        if (entry == null) return null;
        long min = Long.MAX_VALUE;
        for (BucketEntry be : entry.getValue().values()) {
            long e = be.intent().getExecuteAt().toEpochMilli();
            if (e < min) min = e;
        }
        return min == Long.MAX_VALUE ? null : min;
    }

    /**
     * 从桶中移除 Intent。
     *
     * <p>使用 peek-then-CAS 策略：先读取索引条目但不消耗它，
     * 仅在成功从桶中移除后才用条件删除消耗索引条目。
     * 这避免了 scanDue 摘除桶但 CAS 未执行时，remove 消耗索引条目
     * 导致 scanDue CAS 失败、Intent 丢失的问题。</p>
     *
     * <p><b>锁纪律（I1 补充）</b>：同一 intentId 的 add/remove 必须串行（命令路径经
     * synchronized(intent)）；scanDue 是唯一锁外变更方，其重注册恒为同 bucketKey +
     * 同 revision。桶条目与索引条目在 add 的 compute 内原子写入，remove 的两步摘除
     * 均按 revision 校验，索引永不与桶内容脱节（无悬垂索引/计数漏减）。</p>
     *
     * @param intent 待移除的 Intent
     * @return true 表示已移除
     */
    public boolean remove(Intent intent) {
        String intentId = intent.getIntentId();
        ClaimEntry entry = intentIndex.get(intentId);
        if (entry == null) return false;

        if (removeEntryFromBucket(entry.bucketKey(), intentId, entry.revision())) {
            // 成功从桶中移除后才消耗索引条目（CAS：仅当条目未被 add 替换时）
            intentIndex.remove(intentId, entry);
            return true;
        }

        if (removeByScan(intentId, entry.revision())) {
            intentIndex.remove(intentId, entry);
            return true;
        }

        // 桶未找到（可能已被 scanDue 摘除）。不消耗索引条目，
        // 让 scanDue 的 CAS 仍能成功认领该 Intent。
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
                    // R21: CAS 认领与重读 executeAt 之间的 cancel/expire 竞态——终态
                    // intent 不得重入桶或投递(consumer 终态守卫能拦投递,但 addForced
                    // 会把终态条目注册回桶+索引:pendingCount 虚增、驻留到原时刻)。
                    if (intent.getStatus().isTerminal()) {
                        continue;
                    }
                    if (!intent.getExecuteAt().isAfter(now)) {
                        dueIntents.add(intent);
                    } else {
                        // 未到期的任务重新入桶（再入路径：intent 已被系统接受，强制入桶，
                        // 忽略高水位，绝不丢弃已接受的 intent）
                        addForced(intent, intent.getExecuteAt());
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

    /**
     * 带 revision 校验的桶条目摘除：只摘除 revisionAtAdd 与索引条目一致的条目，
     * 避免并发 add（不同 bucketKey）替换后，remove 误摘新注册条目导致索引 CAS
     * 失败、留下悬垂索引与计数漏减。正常路径下索引与桶条目恒一致，行为等价于
     * 无条件摘除。
     */
    private boolean removeEntryFromBucket(long bucketKey, String intentId, long expectedRevision) {
        ConcurrentHashMap<String, BucketEntry> bucket = buckets.get(bucketKey);
        if (bucket == null) {
            return false;
        }

        BucketEntry current = bucket.get(intentId);
        if (current == null || current.revisionAtAdd() != expectedRevision) {
            return false;
        }

        // CHM.remove(key, value) 返回 boolean（值匹配才删除），等价于 CAS
        if (!bucket.remove(intentId, current)) {
            return false;
        }

        pendingCount.decrementAndGet();
        if (bucket.isEmpty()) {
            buckets.remove(bucketKey, bucket);
        }
        return true;
    }

    private boolean removeByScan(String intentId, long expectedRevision) {
        for (Long bucketKey : new ArrayList<>(buckets.keySet())) {
            if (removeEntryFromBucket(bucketKey, intentId, expectedRevision)) {
                return true;
            }
        }
        return false;
    }

    // ========== Test hooks (package-private) ==========

    /** Test-only: if set, fires after CAS claim succeeds but before dueIntents/re-add. */
    volatile Runnable testScanPostClaimHook;

    /** Test-only: if set, fires after bucket acquisition but before the entry put (add 的取桶/入桶两步之间)。 */
    volatile Runnable testAddPostComputeHook;

    /** Test-only: detach bucket without iterating (simulates scanDue's buckets.remove step). */
    boolean testDetachBucket(Instant executeAt) {
        long bucketKey = floorToBucket(executeAt.toEpochMilli());
        return buckets.remove(bucketKey) != null;
    }

    /** Test-only: 目标桶是否已含该 intent（验证 add 的取桶/入桶原子性不变量）。 */
    boolean testBucketContains(Instant executeAt, String intentId) {
        long bucketKey = floorToBucket(executeAt.toEpochMilli());
        ConcurrentHashMap<String, BucketEntry> bucket = buckets.get(bucketKey);
        return bucket != null && bucket.containsKey(intentId);
    }

    /** Test-only: attempt CAS claim on intentIndex. Returns true if claim succeeded. */
    boolean testClaim(String intentId, long bucketKey, long revision) {
        return intentIndex.remove(intentId, new ClaimEntry(bucketKey, revision));
    }
}
