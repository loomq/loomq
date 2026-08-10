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
     * Record intent creation.
     */
    public void recordCreated(String intentId, String traceId, PrecisionTier tier) {
        long nowMs = System.currentTimeMillis();
        IntentTrace trace = new IntentTrace(
            intentId, traceId, tier, IntentStatus.CREATED,
            nowMs, 0, 0, 0, 0,
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

    /**
     * Check if trace exists.
     */
    public boolean contains(String intentId) {
        return traces.containsKey(intentId);
    }

    /**
     * Remove trace by intent ID (for test cleanup).
     */
    public void remove(String intentId) {
        traces.remove(intentId);
    }

    /**
     * Get current size.
     */
    public int size() {
        return traces.size();
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
