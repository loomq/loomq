package com.loomq.spi;

import com.loomq.domain.intent.Intent;

/**
 * 回调处理器接口
 *
 * 内核通过此接口回调宿主应用，通知 Intent 状态变更。
 * 内核不负责 HTTP 投递，只负责触发回调。
 *
 * <p><b>I5 快照语义</b>：Intent 参数为事件发生时刻的防御性快照。修改快照不影响内核状态。</p>
 *
 * @author loomq
 * @since v0.7.0
 */
@FunctionalInterface
public interface CallbackHandler {

    /**
     * 事件类型枚举
     */
    enum EventType {
        /**
         * Intent 到期，正常触发
         */
        DUE,

        /**
         * Intent 被取消
         */
        CANCELLED,

        /**
         * 投递失败（如 webhook 超时）
         */
        FAILED
    }

    /**
     * 当 Intent 事件发生时调用
     *
     * @param intent Intent 对象
     * @param type   事件类型
     * @param error  错误信息（仅 FAILED 时非空）
     */
    void onIntentEvent(Intent intent, EventType type, Throwable error);
}
