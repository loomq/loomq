package com.loomq.application.scheduler;

import com.loomq.domain.intent.Intent;

/** 重试/backoff 默认值唯一来源(finalize 与失败结算路径共用,消灭 5/5000 魔法数孪生)。 */
final class RetryPolicy {

    static final int DEFAULT_MAX_ATTEMPTS = 5;
    static final long DEFAULT_BACKOFF_MS = 5000;

    /** 重试上限:未配置 RedeliveryPolicy 时用默认 5。 */
    int maxAttempts(Intent intent) {
        return intent.getRedelivery() != null ? intent.getRedelivery().getMaxAttempts() : DEFAULT_MAX_ATTEMPTS;
    }

    /** backoff 延迟:未配置 RedeliveryPolicy 时用默认 5000ms。 */
    long backoffDelayMs(Intent intent) {
        return intent.getRedelivery() != null
            ? intent.getRedelivery().calculateDelay(intent.getAttempts())
            : DEFAULT_BACKOFF_MS;
    }
}
