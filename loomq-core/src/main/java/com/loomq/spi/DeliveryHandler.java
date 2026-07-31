package com.loomq.spi;

import com.loomq.domain.intent.Intent;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * 异步投递处理器 SPI 接口。
 *
 * 内核通过此接口投递 Intent，实现方必须返回 CompletableFuture。
 * 实现必须是异步的——不得在调用线程中阻塞等待 I/O。
 *
 * <p><b>并发语义：</b>{@code deliverAsync} 可能与 {@code cancelIntent} 并发执行。
 * 已取消的 Intent 仍可能被投递（cancel 是 best-effort，在途投递不可撤销）。
 * 实现方必须确保下游处理逻辑的幂等性。</p>
 *
 * @author loomq
 * @since v0.8.0
 */
public interface DeliveryHandler {

    /**
     * 异步投递单个 Intent。
     *
     * @param intent 待投递的 Intent
     * @return 投递结果的 CompletableFuture
     */
    CompletableFuture<DeliveryResult> deliverAsync(Intent intent);

    /**
     * 异步批量投递 Intent。
     *
     * 默认实现逐个调用 {@link #deliverAsync}，实现方可覆写以优化批量 I/O。
     *
     * @param intents 待投递的 Intent 列表（非空）
     * @return 每个 Intent 投递结果的 CompletableFuture（顺序与输入一致）
     */
    default List<CompletableFuture<DeliveryResult>> deliverBatchAsync(List<Intent> intents) {
        List<CompletableFuture<DeliveryResult>> results = new ArrayList<>(intents.size());
        for (Intent intent : intents) {
            results.add(deliverAsync(intent));
        }
        return results;
    }

    enum DeliveryResult {
        /** 投递成功 → DELIVERED → ACKED */
        SUCCESS,
        /** 需要重试 → 重新调度 */
        RETRY,
        /** 投递失败，进入死信队列 */
        DEAD_LETTER,
        /** 任务已过期 */
        EXPIRED
    }
}
