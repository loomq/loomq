package com.loomq.application.command;

import com.loomq.domain.intent.Intent;
import com.loomq.spi.CallbackHandler;

/** 包私有函数端口:消除 Canceler→facade 构造环(同 round 9 Rescheduler 模式)。 */
interface CallbackDispatcher {

    void dispatch(Intent snapshot, CallbackHandler.EventType type, Throwable error);
}
