package com.loomq.common;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 运营计数注册表:intents 生命周期计数,统一进 Prometheus 导出。
 *
 * <p>round 12 删除:8 个恒 0 死计数器(ack/failedTerminal/retry/expired/deadLetter/
 * webhookRequests/timeout/error,零调用方)、bucket/ready 手动 gauge(updateBucketMetrics
 * 零调用方)、4 个意图状态 gauge(数据源 RuntimeMetricsRegistry 已随 PHTW 时代整体删除;
 * embedder 需要状态分布时用 ConcurrentIntentStore.countByStatus() 自算)。</p>
 */
final class OperationalMetricsRegistry {

    private final AtomicLong intentsCreatedTotal = new AtomicLong(0);
    private final AtomicLong intentsCancelledTotal = new AtomicLong(0);
    private final AtomicLong recoveryOverdueTotal = new AtomicLong(0);

    // 调度故障收口计数(round 12 自 StatePersistence/SettlementEngine/CohortManager 收口)
    private final AtomicLong persistFailuresTotal = new AtomicLong(0);
    private final AtomicLong finalizeTaskExceptionsTotal = new AtomicLong(0);
    private final AtomicLong wakeLoopErrorsTotal = new AtomicLong(0);

    void incrementIntentsCreated() {
        intentsCreatedTotal.incrementAndGet();
    }

    void incrementIntentsCancelled() {
        intentsCancelledTotal.incrementAndGet();
    }

    void incrementRecoveryOverdue() {
        recoveryOverdueTotal.incrementAndGet();
    }

    long getIntentsCancelledTotal() {
        return intentsCancelledTotal.get();
    }

    long getRecoveryOverdueTotal() {
        return recoveryOverdueTotal.get();
    }

    void incrementPersistFailures() {
        persistFailuresTotal.incrementAndGet();
    }

    void incrementFinalizeTaskExceptions() {
        finalizeTaskExceptionsTotal.incrementAndGet();
    }

    void incrementWakeLoopErrors() {
        wakeLoopErrorsTotal.incrementAndGet();
    }

    long getPersistFailuresTotal() {
        return persistFailuresTotal.get();
    }

    long getFinalizeTaskExceptionsTotal() {
        return finalizeTaskExceptionsTotal.get();
    }

    long getWakeLoopErrorsTotal() {
        return wakeLoopErrorsTotal.get();
    }

    void appendPrometheusMetrics(StringBuilder sb) {
        sb.append("# HELP loomq_intents_created_total Total intents created\n");
        sb.append("# TYPE loomq_intents_created_total counter\n");
        sb.append("loomq_intents_created_total ").append(intentsCreatedTotal.get()).append("\n");
        sb.append("\n");

        sb.append("# HELP loomq_intents_cancelled_total Total intents cancelled\n");
        sb.append("# TYPE loomq_intents_cancelled_total counter\n");
        sb.append("loomq_intents_cancelled_total ").append(intentsCancelledTotal.get()).append("\n");
        sb.append("\n");

        sb.append("# HELP loomq_recovery_overdue_total Total intents marked overdue during recovery\n");
        sb.append("# TYPE loomq_recovery_overdue_total counter\n");
        sb.append("loomq_recovery_overdue_total ").append(recoveryOverdueTotal.get()).append("\n");
        sb.append("\n");

        sb.append("# HELP loomq_persist_failures_total Persist failures swallowed by I6 fault tolerance\n");
        sb.append("# TYPE loomq_persist_failures_total counter\n");
        sb.append("loomq_persist_failures_total ").append(persistFailuresTotal.get()).append("\n");
        sb.append("\n");

        sb.append("# HELP loomq_finalize_task_exceptions_total Delivery settlement task exceptions swallowed in runFinalizeTask\n");
        sb.append("# TYPE loomq_finalize_task_exceptions_total counter\n");
        sb.append("loomq_finalize_task_exceptions_total ").append(finalizeTaskExceptionsTotal.get()).append("\n");
        sb.append("\n");

        sb.append("# HELP loomq_wake_loop_errors_total CohortManager wake loop errors (diagnostic, loop keeps running)\n");
        sb.append("# TYPE loomq_wake_loop_errors_total counter\n");
        sb.append("loomq_wake_loop_errors_total ").append(wakeLoopErrorsTotal.get()).append("\n");
        sb.append("\n");
    }
}
