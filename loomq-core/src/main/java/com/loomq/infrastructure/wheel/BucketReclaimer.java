package com.loomq.infrastructure.wheel;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 桶回收 daemon:定期删除已过期且无活跃 Intent 引用的桶文件。
 *
 * <p>基于 locationIndex(仅保非终态 Intent)判断哪些桶仍被引用:
 * <ol>
 *   <li>遍历 locationIndex 收集所有被引用的 {@code (tier, bucketKey)} 组合</li>
 *   <li>遍历磁盘桶目录,删除满足 {@code now - bucketWindowEnd > retentionMs} 且不在引用集合中的桶</li>
 * </ol>
 *
 * <p>桶的时间窗口终点 = {@code (bucketKey + 1) * tier.windowMs}。保留期默认为视界 + 1 天裕量,
 * 保证不会误删尚未到期的未来任务。回收时先 {@code Bucket.close()}(force 脏数据),
 * 再从 wheels map 移除,最后删文件。
 */
public final class BucketReclaimer implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(BucketReclaimer.class);
    private static final long SCAN_INTERVAL_MS = 5 * 60 * 1000L;  // 5 minutes

    private final WheelStore store;
    private final IntentLocationIndex locationIndex;
    private final long retentionMs;
    private final Thread thread;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public BucketReclaimer(WheelStore store, IntentLocationIndex locationIndex, long retentionMs) {
        this.store = store;
        this.locationIndex = locationIndex;
        this.retentionMs = retentionMs;
        this.thread = Thread.ofPlatform().name("bucket-reclaimer").daemon(true).unstarted(this::loop);
    }

    public void start() {
        if (running.compareAndSet(false, true)) {
            thread.start();
            log.info("BucketReclaimer started, retentionMs={}", retentionMs);
        }
    }

    /** 单步回收(测试用):扫描并删除过期无引用桶,返回删除数。 */
    public int reclaimOnce() {
        Set<String> referencedKeys = collectReferencedBuckets();
        int deleted = 0;
        LongSupplier clock = store.clock();

        for (WheelTier tier : WheelTier.values()) {
            Set<Long> bucketKeys = new HashSet<>(store.listBucketKeys(tier));
            for (long bucketKey : bucketKeys) {
                String refKey = tier + "/" + bucketKey;
                if (referencedKeys.contains(refKey)) continue;  // 仍有活跃引用

                long windowEndMs = (bucketKey + 1) * tier.windowMs;
                if (clock.getAsLong() - windowEndMs > retentionMs) {
                    store.deleteBucket(tier, bucketKey);
                    deleted++;
                }
            }
        }
        if (deleted > 0) {
            log.info("BucketReclaimer: deleted {} expired buckets", deleted);
        }
        return deleted;
    }

    /** 遍历 locationIndex 收集被引用的 "tier/bucketKey" 字符串集合。 */
    private Set<String> collectReferencedBuckets() {
        Set<String> refs = new HashSet<>();
        for (SlotLocation loc : locationIndex.allLocations()) {
            if (!loc.inTail()) {
                refs.add(loc.tier() + "/" + loc.bucketKey());
            }
        }
        return refs;
    }

    private void loop() {
        while (running.get()) {
            try {
                reclaimOnce();
            } catch (Exception e) {
                log.error("BucketReclaimer scan error", e);
            }
            try {
                Thread.sleep(SCAN_INTERVAL_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        log.info("BucketReclaimer stopped");
    }

    @Override public void close() {
        running.set(false);
        thread.interrupt();
        try { thread.join(2000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
}
