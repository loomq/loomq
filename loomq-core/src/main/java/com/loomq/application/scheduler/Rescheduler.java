package com.loomq.application.scheduler;

import com.loomq.domain.intent.Intent;

/** 重排程端口:结算引擎仅经此触发锁外 schedule(),不依赖门面整体(消除构造环)。 */
interface Rescheduler {
    void schedule(Intent intent);
}