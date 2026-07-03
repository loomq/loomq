package com.loomq.domain.intent;

/**
 * WAL 持久化策略。
 *
 * 与精度档位解耦后，用户可在创建 Intent 时显式指定，
 * 未指定则使用精度档位的默认值。
 */
public enum WalMode {
    /** 内存映射写入,不等待 fsync。延迟最低,崩溃窗口 ≤ groupCommitIntervalMs(PHTW group-commit daemon 周期 fsync)。 */
    ASYNC,
    /**
     * 在 PHTW group-commit 架构下等价于 {@link #ASYNC}:周期性批量 fsync 已由
     * GroupCommitBarrier 对所有脏桶统一承担,无独立中间档。保留此枚举值以兼容旧调用方与
     * 精度档位默认映射,resolveWalMode 会将其归一为 ASYNC 行为。
     */
    BATCH_DEFERRED,
    /** 写入后等待覆盖其写入的 group-commit force 完成再返回。最强持久化,延迟最高,无崩溃窗口。 */
    DURABLE
}
