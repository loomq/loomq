package com.loomq.store;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import java.time.Instant;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 基于 ConcurrentHashMap 的 Intent 内存存储。
 *
 * 支持幂等性检查和状态管理。幂等记录每 1 小时清理一次，
 * 窗口期为 24 小时。
 *
 * @author loomq
 * @since v0.5.0
 */
public class ConcurrentIntentStore implements IntentStore {

    private static final Logger logger = LoggerFactory.getLogger(ConcurrentIntentStore.class);

    private final Map<String, StoredIntent> intents = new ConcurrentHashMap<>();
    private final Map<String, IdempotencyRecord> idempotencyRecords = new ConcurrentHashMap<>();
    private final Map<IntentStatus, AtomicLong> statusCounts = new EnumMap<>(IntentStatus.class);
    private final AtomicLong pendingCount = new AtomicLong();

    private final ScheduledExecutorService cleanupExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "idempotency-cleanup");
        t.setDaemon(true);
        return t;
    });

    public ConcurrentIntentStore() {
        for (IntentStatus status : IntentStatus.values()) {
            statusCounts.put(status, new AtomicLong());
        }

        // 每小时清理过期幂等记录（窗口期 24 小时）
        cleanupExecutor.scheduleAtFixedRate(this::cleanupExpiredRecords, 1, 1, TimeUnit.HOURS);
    }

    @Override
    public void save(Intent intent) {
        upsertInternal(intent);
        logger.debug("Intent saved: id={}, status={}", intent.getIntentId(), intent.getStatus());
    }

    @Override
    public void update(Intent intent) {
        upsertInternal(intent);
        logger.debug("Intent updated: id={}, status={}", intent.getIntentId(), intent.getStatus());
    }

    @Override
    public void upsert(Intent intent) {
        upsertInternal(intent);
        logger.debug("Intent upserted: id={}, status={}", intent.getIntentId(), intent.getStatus());
    }

    @Override
    public Intent findById(String intentId) {
        StoredIntent stored = intents.get(intentId);
        return stored != null ? stored.intent().copy() : null;
    }

    @Override
    public Intent findByIdInternal(String intentId) {
        StoredIntent stored = intents.get(intentId);
        return stored != null ? stored.intent() : null;
    }

    @Override
    public IdempotencyResult checkIdempotency(String idempotencyKey) {
        IdempotencyRecord record = idempotencyRecords.get(idempotencyKey);

        if (record == null) {
            return IdempotencyResult.newRequest();
        }

        if (!record.isInWindow()) {
            logger.debug("Idempotency window expired for key={}, treat as new request", idempotencyKey);
            return IdempotencyResult.windowExpired();
        }

        StoredIntent stored = intents.get(record.getIntentId());
        if (stored == null) {
            idempotencyRecords.remove(idempotencyKey);
            return IdempotencyResult.newRequest();
        }

        Intent intent = stored.intent();
        if (intent.getStatus().isTerminal()) {
            return IdempotencyResult.duplicateTerminal(intent.copy());
        } else {
            return IdempotencyResult.duplicateActive(intent.copy());
        }
    }

    @Override
    public void delete(String intentId) {
        StoredIntent removed = intents.remove(intentId);
        if (removed != null) {
            decrementStatus(removed.status());
            String idempotencyKey = removed.intent().getIdempotencyKey();
            if (idempotencyKey != null) {
                idempotencyRecords.remove(idempotencyKey);
            }
        }
    }

    @Override
    public void clear() {
        intents.clear();
        idempotencyRecords.clear();
        statusCounts.values().forEach(counter -> counter.set(0));
        pendingCount.set(0);
    }

    @Override
    public Map<String, Intent> getAllIntents() {
        Map<String, Intent> snapshot = new HashMap<>(intents.size());
        intents.forEach((id, stored) -> snapshot.put(id, stored.intent().copy()));
        return Map.copyOf(snapshot);
    }

    @Override
    public long count() {
        return intents.size();
    }

    @Override
    public long countByStatus(IntentStatus status) {
        AtomicLong counter = statusCounts.get(status);
        return counter != null ? counter.get() : 0L;
    }

    @Override
    public long getPendingCount() {
        return pendingCount.get();
    }

    private void cleanupExpiredRecords() {
        // 清理过期幂等记录（24h 窗口）
        AtomicLong cleaned = new AtomicLong();
        idempotencyRecords.entrySet().removeIf(entry -> {
            if (entry.getValue().isExpired()) {
                cleaned.incrementAndGet();
                return true;
            }
            return false;
        });

        // 驱逐终态 Intent（updatedAt 超过幂等窗口 = 24h）
        // P1-1 确保终态已落盘 -> 驱逐后磁盘仍有权威记录
        long cutoffMs = System.currentTimeMillis()
            - IdempotencyRecord.DEFAULT_WINDOW.toMillis();
        Instant cutoff = Instant.ofEpochMilli(cutoffMs);
        AtomicLong evicted = new AtomicLong();

        intents.forEach((id, stored) -> {
            if (stored.intent().getStatus().isTerminal()
                    && stored.intent().getUpdatedAt().isBefore(cutoff)) {
                delete(id);
                evicted.incrementAndGet();
            }
        });

        if (cleaned.get() > 0 || evicted.get() > 0) {
            logger.info("Cleanup: {} expired idempotency records, {} evicted terminal intents",
                cleaned.get(), evicted.get());
        }
    }

    /** Test-only: trigger cleanup synchronously (bypasses scheduled executor). */
    void testCleanupExpiredRecords() {
        cleanupExpiredRecords();
    }

    @Override
    public void shutdown() {
        cleanupExecutor.shutdown();
        try {
            if (!cleanupExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                cleanupExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            cleanupExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * 原子性 upsert（单 key 级别）。
     *
     * 注意：statusCounts 和 idempotencyRecords 的更新与 intents 主 map 的更新
     * 不是跨 map 原子操作。在单 writer-per-intent 场景（当前调度器保证）下，
     * 这种最终一致性是可接受的。
     *
     * 若未来引入并发写入同一 intent 的场景，需引入锁或事务包装。
     */
    private void upsertInternal(Intent intent) {
        intents.compute(intent.getIntentId(), (id, existing) -> {
            IntentStatus previousStatus = existing != null ? existing.status() : null;
            String previousIdempotencyKey = existing != null ? existing.intent().getIdempotencyKey() : null;

            if (existing != null) {
                decrementStatus(previousStatus);
                if (previousIdempotencyKey != null && !Objects.equals(previousIdempotencyKey, intent.getIdempotencyKey())) {
                    idempotencyRecords.remove(previousIdempotencyKey);
                }
            }

            StoredIntent stored = new StoredIntent(intent);
            incrementStatus(stored.status());

            String idempotencyKey = intent.getIdempotencyKey();
            if (idempotencyKey != null) {
                idempotencyRecords.put(idempotencyKey, IdempotencyRecord.fromIntent(intent));
            }

            return stored;
        });
    }

    private void incrementStatus(IntentStatus status) {
        if (status == null) return;
        AtomicLong counter = statusCounts.get(status);
        if (counter != null) counter.incrementAndGet();
        if (!status.isTerminal()) pendingCount.incrementAndGet();
    }

    private void decrementStatus(IntentStatus status) {
        if (status == null) return;
        AtomicLong counter = statusCounts.get(status);
        if (counter != null) counter.decrementAndGet();
        if (!status.isTerminal()) pendingCount.decrementAndGet();
    }

    private record StoredIntent(Intent intent, IntentStatus status) {
        private StoredIntent(Intent intent) {
            this(intent, intent.getStatus());
        }
    }
}
