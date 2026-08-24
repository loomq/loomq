package com.loomq.application.scheduler;

import com.loomq.spi.IntentObserver;
import java.util.function.Consumer;

/**
 * I5 collect-then-defer 结算结果收集器:锁内收集(persisted/terminalId/deferredNotify/needReschedule),
 * 锁外由 SettlementEngine.flushOutcome 统一收尾。收敛 finalizeIntent/handleDeliveryFailure/
 * handleExpired 三处三元组样板。
 */
final class DeferredOutcome {

    private boolean needReschedule;
    private boolean persisted;
    private String terminalId;
    private Consumer<IntentObserver> deferredNotify;

    void markReschedule() { this.needReschedule = true; }
    void markPersisted() { this.persisted = true; }
    void markTerminal(String terminalId) { this.terminalId = terminalId; }
    void deferNotify(Consumer<IntentObserver> deferredNotify) { this.deferredNotify = deferredNotify; }

    boolean hasReschedule() { return needReschedule; }
    boolean hasPersisted() { return persisted; }
    String terminalId() { return terminalId; }
    Consumer<IntentObserver> deferredNotify() { return deferredNotify; }
}