package com.loomq.tracing;

import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Per-intent trace store for observability.
 *
 * Stores the last N intent traces (default 100K) with LRU eviction.
 * Used for real-time debugging: "why did intent X take so long?"
 */
public final class IntentTraceStore {

    private final int maxSize;
    private final ConcurrentHashMap<String, IntentTrace> traces;
    private final EvictionQueue evictionQueue;

    public IntentTraceStore() {
        this(100_000);
    }

    public IntentTraceStore(int maxSize) {
        this.maxSize = maxSize;
        this.traces = new ConcurrentHashMap<>();
        this.evictionQueue = new EvictionQueue();
    }

    /**
     * 仅当 trace 缺失或 createdAt 不匹配时记录创建(幂等)。
     *
     * <p>收敛 createIntent/批量 create/schedule 三处复制的守卫(R21 语义:同 id 重建是
     * 新 incarnation,旧 trace 不得继承);recordCreated 整体替换 trace,误调会清空投递历史。</p>
     */
    public void recordCreatedIfNew(String intentId, String traceId, PrecisionTier tier, long createdAtMs) {
        IntentTrace existing = traces.get(intentId);
        if (existing == null || existing.createdAtMs() != createdAtMs) {
            recordCreated(intentId, traceId, tier, createdAtMs);
        }
    }

    /**
     * Record intent creation with the intent's own createdAt.
     *
     * <p>R21: 同 id 重建(R8 支持路径)是新 incarnation——调度器据此比较 trace 的
     * createdAt 与 intent 的 createdAt,不匹配即刷新 trace,避免新 intent 继承旧
     * createdAt/status(如 ACKED)导致 lag 归因全错。</p>
     */
    public void recordCreated(String intentId, String traceId, PrecisionTier tier, long createdAtMs) {
        IntentTrace trace = new IntentTrace(
            intentId, traceId, tier, IntentStatus.CREATED,
            createdAtMs, 0, 0, 0, 0,
            0, 0, 0, 0,
            null, null
        );
        put(intentId, trace);
    }

    /**
     * Record intent enqueued into dispatch queue.
     */
    public void recordEnqueued(String intentId) {
        long nowMs = System.currentTimeMillis();
        traces.computeIfPresent(intentId, (id, t) -> t.withEnqueuedAt(nowMs));
    }

    /**
     * Record intent dequeued from dispatch queue (about to deliver).
     */
    public void recordDequeued(String intentId) {
        long nowMs = System.currentTimeMillis();
        traces.computeIfPresent(intentId, (id, t) -> t.withDequeuedAt(nowMs));
    }

    /**
     * Record delivery completed.
     */
    public void recordDelivered(String intentId) {
        long nowMs = System.currentTimeMillis();
        traces.computeIfPresent(intentId, (id, t) -> t.withDeliveredAt(nowMs));
    }

    /**
     * Record ACK confirmed.
     */
    public void recordAcked(String intentId) {
        long nowMs = System.currentTimeMillis();
        traces.computeIfPresent(intentId, (id, t) -> t.withAckedAt(nowMs));
    }

    /**
     * Update intent status.
     */
    public void updateStatus(String intentId, IntentStatus status) {
        traces.computeIfPresent(intentId, (id, t) -> t.withStatus(status));
    }

    /**
     * Record delivery failure details.
     */
    public void recordFailure(String intentId, String reason, Integer httpStatus) {
        traces.computeIfPresent(intentId, (id, t) -> t.withFailure(reason, httpStatus));
    }

    /**
     * Get trace by intent ID.
     */
    public IntentTrace get(String intentId) {
        return traces.get(intentId);
    }

    private void put(String intentId, IntentTrace trace) {
        IntentTrace existing = traces.put(intentId, trace);
        if (existing == null) {
            evictionQueue.add(intentId);
            evictIfNeeded();
        }
    }

    private void evictIfNeeded() {
        while (traces.size() > maxSize) {
            String oldest = evictionQueue.poll();
            if (oldest != null) {
                traces.remove(oldest);
            } else {
                break;
            }
        }
    }

    /**
     * Simple FIFO queue for eviction (not true LRU, but good enough).
     */
    private static class EvictionQueue {
        private final String[] buffer;
        private int head = 0;
        private int tail = 0;
        private final Object lock = new Object();

        EvictionQueue() {
            this.buffer = new String[110_000]; // Slightly larger than maxSize
        }

        void add(String intentId) {
            synchronized (lock) {
                buffer[tail] = intentId;
                tail = (tail + 1) % buffer.length;
            }
        }

        String poll() {
            synchronized (lock) {
                if (head == tail) return null;
                String result = buffer[head];
                buffer[head] = null;
                head = (head + 1) % buffer.length;
                return result;
            }
        }
    }
}
