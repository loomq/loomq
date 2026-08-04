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

    ULTRA,
    FAST,
    HIGH,
    STANDARD,
    ECONOMY,
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
     * JSON 序列化值
     */
    public String toJson() {
        return name();
    }

    /**
     * JSON 反序列化
     * 不区分大小写，未知值返回目录默认档位
     *
     * @param value 字符串值
     * @return 精度档位，默认目录默认档位
     */
    public static PrecisionTier fromString(String value) {
        if (value == null || value.isBlank()) {
            return catalog().defaultTier();
        }
        try {
            return PrecisionTier.valueOf(value.toUpperCase());
        } catch (IllegalArgumentException e) {
            return catalog().defaultTier();
        }
    }
}
