package com.loomq.domain.intent;

/**
 * 精度档位枚举。
 *
 * 这里只保留稳定的档位标签，运行参数由 PrecisionTierCatalog 统一提供。
 *
 * @author loomq
 * @since v0.5.1
 */
public enum PrecisionTier {
    // 警告：枚举声明序即 SlotCodec 持久化序（ordinal）。新档必须追加末尾，禁止插入重排。
    // 注：v0.9.x 档位精简（6→4：HIGH→FAST、ECONOMY→STANDARD）为一次性 ordinal 断裂，
    // 断裂前的 wheel 数据目录必须清空；此后"追加末尾"规则继续生效。

    ULTRA,
    FAST,
    STANDARD,
    MILLI;

    private static PrecisionTierCatalog catalog() {
        return PrecisionTierCatalog.defaultCatalog();
    }

    /**
     * 获取精度窗口（毫秒）
     *
     * @return 精度窗口，单位毫秒
     */
    public long getPrecisionWindowMs() {
        return catalog().precisionWindowMs(this);
    }

    /**
     * 获取最大并发数
     *
     * @return 最大并发任务数
     */
    public int getMaxConcurrency() {
        return catalog().maxConcurrency(this);
    }

    /**
     * 获取批量大小
     *
     * @return 批量大小（1表示禁用批量）
     */
    public int getBatchSize() {
        return catalog().batchSize(this);
    }

    /**
     * 获取批量窗口（毫秒）
     *
     * @return 批量等待时间
     */
    public int getBatchWindowMs() {
        return catalog().batchWindowMs(this);
    }

    /**
     * 获取消费者数量
     *
     * @return 批量消费者线程数
     */
    public int getConsumerCount() {
        return catalog().consumerCount(this);
    }

    /**
     * 是否启用批量投递
     *
     * @return true 如果 batchSize > 1
     */
    public boolean isBatchEnabled() {
        return catalog().isBatchEnabled(this);
    }

    /**
     * JSON 反序列化
     * 不区分大小写，未知值返回目录默认档位
     *
     * @param value 字符串值
     * @return 精度档位，默认目录默认档位
     */
    // v0.9.x 精简:已删除档名在 runtime/config 边界显式重映射到并入的保留档(HIGH→FAST、
    // ECONOMY→STANDARD),避免这些旧名一律回退默认档;映射到的保留档精度不粗于原档
    // (HIGH 100ms→FAST 50ms、ECONOMY 1000ms→STANDARD 500ms)。
    // 持久性副作用:旧 HIGH 档默认 walMode 为 BATCH_DEFERRED(归一 ASYNC),并入 FAST 后沿用
    // FAST 的 DURABLE 默认——存量 "HIGH" 配置实际落盘强度从非持久升为持久(方向更安全,耗时增加)。
    // 【边界】此表仅由 fromString(runtime/config 输入)读取。持久化路径(tierByOrdinal)绝不读它:
    // 新格式 ordinal 2 已是 STANDARD,若按旧 HIGH 重映射会把合法新数据窜改成 FAST(ordinal 碰撞,
    // 同一字节读不出两种含义)。存留 wheel 数据必须按 v0.9.x ordinal 断裂契约清库,不做兼容读取。
    private static final java.util.Map<String, PrecisionTier> LEGACY_REMAPS =
        java.util.Map.of("HIGH", PrecisionTier.FAST, "ECONOMY", PrecisionTier.STANDARD);

    public static PrecisionTier fromString(String value) {
        if (value == null || value.isBlank()) {
            return catalog().defaultTier();
        }
        String upper = value.toUpperCase();
        PrecisionTier remapped = LEGACY_REMAPS.get(upper);
        if (remapped != null) {
            return remapped;
        }
        try {
            return PrecisionTier.valueOf(upper);
        } catch (IllegalArgumentException e) {
            return catalog().defaultTier();
        }
    }
}
