package com.loomq.application.scheduler;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.spi.DeliveryHandler.DeliveryResult;

/**
 * 投递结算回调(消费循环 → 结算引擎)。调用时机:releasePermit 之后、
 * 结算任务提交之前(单发/批量路径统一)。
 */
interface DeliverySettlement {

    /** 异步投递完成(成功或失败,ex 非 null 表示异常/超时)。 */
    void onCompleted(Intent intent, PrecisionTier tier, DeliveryResult result, Throwable error);

    /** deliverAsync/deliverBatchAsync 同步抛异常(SPI 违约)或返回 null/缺 future。 */
    void onException(Intent intent, PrecisionTier tier, Throwable error);

    /** 投递前过期闸门命中:按 ExpiredAction 终态化,不得投递。由 DispatchPipeline 消费循环调用。 */
    void onExpiredInFlight(Intent intent, PrecisionTier tier);
}
