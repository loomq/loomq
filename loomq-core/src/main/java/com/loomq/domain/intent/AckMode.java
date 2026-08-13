package com.loomq.domain.intent;

/**
 * ACK 确认级别。
 *
 * 定义 Intent 创建后的持久化和可靠性保证级别。
 *
 * @author loomq
 */
public enum AckMode {

    /**
     * 异步确认：进入内存队列即返回。
     * 延迟最低（<1ms），可靠性最低（进程崩溃可能丢失）。
     */
    ASYNC,

    /**
     * 持久化确认：等待持久化落盘后返回。
     * 延迟中等（1-10ms），主机级不丢数据。
     */
    DURABLE,

    /**
     * 副本确认：等待 primary + replica 持久化确认后返回。
     *
     * @deprecated loomq-core 无副本实现,当前在 {@code resolveWalMode} 中一律折叠为
     *     DURABLE——语义与"副本确认"不符,仅为旧配置兼容保留。
     */
    @Deprecated
    REPLICATED;
}
