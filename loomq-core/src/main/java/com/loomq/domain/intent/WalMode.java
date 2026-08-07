package com.loomq.domain.intent;

/**
 * WAL 持久化策略。
 *
 * 与精度档位解耦后，用户可在创建 Intent 时显式指定，
 * 未指定则使用精度档位的默认值。
 */
public enum WalMode {
    // 警告：枚举声明序即 SlotCodec 持久化序（ordinal）。新值必须追加末尾，禁止插入重排。
    // 注：v0.9.x 精简删除了 BATCH_DEFERRED，使 DURABLE ordinal 由 2→1（与 PrecisionTier 档位
    // 断裂同一次清库）；此后"追加末尾"规则继续生效。

    /** 内存映射写入,不等待 fsync。延迟最低,崩溃窗口 ≤ groupCommitIntervalMs(PHTW group-commit daemon 周期 fsync)。 */
    ASYNC,
    /** 写入后等待覆盖其写入的 group-commit force 完成再返回。最强持久化,延迟最高,无崩溃窗口。 */
    DURABLE
}
