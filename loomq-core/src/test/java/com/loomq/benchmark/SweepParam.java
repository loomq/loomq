package com.loomq.benchmark;

import com.loomq.domain.intent.PrecisionTierProfile;

/** 可扫档位参数。 */
public enum SweepParam {
    CONSUMERS { @Override PrecisionTierProfile apply(PrecisionTierProfile p, int v) { return p.withConsumerCount(v); } },
    MAX_CONCURRENCY { @Override PrecisionTierProfile apply(PrecisionTierProfile p, int v) { return p.withMaxConcurrency(v); } },
    BATCH_SIZE { @Override PrecisionTierProfile apply(PrecisionTierProfile p, int v) { return p.withBatchSize(v); } },
    QUEUE_CAPACITY { @Override PrecisionTierProfile apply(PrecisionTierProfile p, int v) { return p.withDispatchQueueCapacity(v); } };

    abstract PrecisionTierProfile apply(PrecisionTierProfile p, int v);
}