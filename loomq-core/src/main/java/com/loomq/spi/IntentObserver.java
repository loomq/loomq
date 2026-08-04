package com.loomq.spi;

import com.loomq.domain.intent.Intent;
import com.loomq.spi.DeliveryHandler.DeliveryResult;

/**
 * Intent 生命周期观察器。
 *
 * 服务层（复制、锁服务、agent 调度器等）通过实现此接口，
 * 在不侵入内核调度逻辑的前提下观测 intent 状态变化。
 *
 * <p><b>I5 快照语义</b>：Intent 参数为事件发生时刻的防御性快照。修改快照不影响内核状态。
 * 观察器在 intent 锁外被调用（不持有 per-intent 监视器），但仍同步执行于调用线程--
 * 实现应保持轻量，避免阻塞结算流水线。</p>
 */
public interface IntentObserver {

    /** Intent 进入 SCHEDULED 状态 */
    void onScheduled(Intent intent);

    /** Intent 投递成功 */
    void onDelivered(Intent intent, DeliveryResult result);

    /** Intent 进入死信队列 */
    void onDeadLettered(Intent intent);

    /** Intent 过期 */
    void onExpired(Intent intent);

    /** Intent 投递异常 */
    void onDeliveryFailed(Intent intent, Throwable error);
}
