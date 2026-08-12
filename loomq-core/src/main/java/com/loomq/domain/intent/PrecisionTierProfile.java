package com.loomq.domain.intent;

/**
 * 精度档位配置。
 *
 * 这是可复用的纯数据模型，负责承载调度器所需的参数，
 * 由 PrecisionTierCatalog 统一提供默认 preset。
 */
public record PrecisionTierProfile(
    long precisionWindowMs,
    int maxConcurrency,
    int batchSize,
    int batchWindowMs,
    int consumerCount,
    int dispatchQueueCapacity,
    WalMode walMode,
    long scanIntervalMs,
    boolean directBucket,
    boolean adaptiveScan,
    int maxBuckets
) {

    public PrecisionTierProfile {
        if (precisionWindowMs <= 0) {
            throw new IllegalArgumentException("precisionWindowMs must be positive");
        }
        if (maxConcurrency <= 0) {
            throw new IllegalArgumentException("maxConcurrency must be positive");
        }
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be positive");
        }
        if (batchWindowMs < 0) {
            throw new IllegalArgumentException("batchWindowMs must be non-negative");
        }
        if (consumerCount <= 0) {
            throw new IllegalArgumentException("consumerCount must be positive");
        }
        if (dispatchQueueCapacity <= 0) {
            throw new IllegalArgumentException("dispatchQueueCapacity must be positive");
        }
        if (walMode == null) {
            throw new IllegalArgumentException("walMode must not be null");
        }
        if (scanIntervalMs <= 0) {
            throw new IllegalArgumentException("scanIntervalMs must be positive");
        }
        if (maxBuckets <= 0) {
            throw new IllegalArgumentException("maxBuckets must be positive");
        }
    }

    /** 8-arg convenience: directives default to false/false and maxBuckets to Integer.MAX_VALUE (disabled). */
    public PrecisionTierProfile(long precisionWindowMs, int maxConcurrency, int batchSize,
                                int batchWindowMs, int consumerCount, int dispatchQueueCapacity,
                                WalMode walMode, long scanIntervalMs) {
        this(precisionWindowMs, maxConcurrency, batchSize, batchWindowMs, consumerCount,
             dispatchQueueCapacity, walMode, scanIntervalMs, false, false, Integer.MAX_VALUE);
    }

    /** 10-arg convenience: defaults maxBuckets to Integer.MAX_VALUE (disabled). */
    public PrecisionTierProfile(long precisionWindowMs, int maxConcurrency, int batchSize,
                                int batchWindowMs, int consumerCount, int dispatchQueueCapacity,
                                WalMode walMode, long scanIntervalMs,
                                boolean directBucket, boolean adaptiveScan) {
        this(precisionWindowMs, maxConcurrency, batchSize, batchWindowMs, consumerCount,
             dispatchQueueCapacity, walMode, scanIntervalMs, directBucket, adaptiveScan, Integer.MAX_VALUE);
    }

    /**
     * 5-argument convenience constructor using default dispatchQueueCapacity = maxConcurrency * 16,
     * default WalMode = DURABLE, and default scanIntervalMs = precisionWindowMs.
     */
    public PrecisionTierProfile(long precisionWindowMs, int maxConcurrency, int batchSize,
                                int batchWindowMs, int consumerCount) {
        this(precisionWindowMs, maxConcurrency, batchSize, batchWindowMs, consumerCount,
             maxConcurrency * 16, WalMode.DURABLE, precisionWindowMs);
    }

    /**
     * 6-argument convenience constructor using default WalMode = DURABLE
     * and default scanIntervalMs = precisionWindowMs.
     */
    public PrecisionTierProfile(long precisionWindowMs, int maxConcurrency, int batchSize,
                                int batchWindowMs, int consumerCount, int dispatchQueueCapacity) {
        this(precisionWindowMs, maxConcurrency, batchSize, batchWindowMs, consumerCount,
             dispatchQueueCapacity, WalMode.DURABLE, precisionWindowMs);
    }

    public PrecisionTierProfile withConsumerCount(int v) {
        return new PrecisionTierProfile(precisionWindowMs, maxConcurrency, batchSize, batchWindowMs,
            v, dispatchQueueCapacity, walMode, scanIntervalMs, directBucket, adaptiveScan, maxBuckets);
    }
    public PrecisionTierProfile withMaxConcurrency(int v) {
        return new PrecisionTierProfile(precisionWindowMs, v, batchSize, batchWindowMs,
            consumerCount, dispatchQueueCapacity, walMode, scanIntervalMs, directBucket, adaptiveScan, maxBuckets);
    }
    public PrecisionTierProfile withBatchSize(int v) {
        return new PrecisionTierProfile(precisionWindowMs, maxConcurrency, v, batchWindowMs,
            consumerCount, dispatchQueueCapacity, walMode, scanIntervalMs, directBucket, adaptiveScan, maxBuckets);
    }
    public PrecisionTierProfile withDispatchQueueCapacity(int v) {
        return new PrecisionTierProfile(precisionWindowMs, maxConcurrency, batchSize, batchWindowMs,
            consumerCount, v, walMode, scanIntervalMs, directBucket, adaptiveScan, maxBuckets);
    }

    public boolean isBatchEnabled() {
        return batchSize > 1;
    }
}
