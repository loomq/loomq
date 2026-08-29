package com.loomq.common;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * round 12 导出契约快照:锁存活指标名单——每个名字必须有唯一写点;死指标不得回流。
 * 新增/删除指标时必须同步修改本测试(防止静默回流)。
 */
class MetricsExportSnapshotTest {

    @Test
    void exportsExactlyTheSurvivingMetricFamilies() {
        MetricsCollector mc = new MetricsCollector();
        String prom = mc.exportPrometheusMetrics();

        // OMR:调度故障收口计数(round 12)
        assertTrue(prom.contains("loomq_persist_failures_total "));
        assertTrue(prom.contains("loomq_finalize_task_exceptions_total "));
        assertTrue(prom.contains("loomq_wake_loop_errors_total "));

        // OMR:3 个活生命周期计数
        assertTrue(prom.contains("loomq_intents_created_total "));
        assertTrue(prom.contains("loomq_intents_cancelled_total "));
        assertTrue(prom.contains("loomq_recovery_overdue_total "));

        // LMR:finalize(更名后)+ cohort flush
        assertTrue(prom.contains("loomq_finalize_duration_ms_p95"));
        assertTrue(prom.contains("loomq_finalize_duration_samples "));
        assertTrue(prom.contains("loomq_cohort_flush_duration_us_p95"));
        assertTrue(prom.contains("loomq_cohort_flush_samples "));

        // PTMR:档位计数/桶/唤醒延迟/背压/lag
        assertTrue(prom.contains("loomq_intent_total{"));
        assertTrue(prom.contains("loomq_intent_due_total{"));
        assertTrue(prom.contains("loomq_scheduler_bucket_size{"));
        assertTrue(prom.contains("loomq_scheduler_wakeup_latency_us_p95"));
        assertTrue(prom.contains("loomq_dispatch_queue_offer_failed_total{"));
        assertTrue(prom.contains("loomq_backpressure_events_total{"));
        assertTrue(prom.contains("loomq_dispatch_queue_retry_total{"));
        assertTrue(prom.contains("loomq_dispatch_queue_abandoned_total{"));
        assertTrue(prom.contains("loomq_dispatch_queue_size{"));
        assertTrue(prom.contains("loomq_dispatch_queue_lag_ms_p95"));

        // 已删除的死指标/死设施不得回流
        // 注意:loomq_intents_created_total/cancelled_total 均不含子串 "loomq_intents_total";若未来新增以 loomq_intents_total 为前缀的指标名,需改用行级断言。
        assertFalse(prom.contains("loomq_intents_total"));
        assertFalse(prom.contains("loomq_intents_pending"));
        assertFalse(prom.contains("loomq_intents_scheduled"));
        assertFalse(prom.contains("loomq_intents_dispatching"));
        assertFalse(prom.contains("loomq_intents_ack_success_total"));
        assertFalse(prom.contains("loomq_intents_failed_terminal_total"));
        assertFalse(prom.contains("loomq_intents_retry_total"));
        assertFalse(prom.contains("loomq_intents_expired_total"));
        assertFalse(prom.contains("loomq_intents_dead_letter_total"));
        assertFalse(prom.contains("loomq_recovery_duration_ms"));
        assertFalse(prom.contains("loomq_recovery_intents_total"));
        assertFalse(prom.contains("loomq_wal_"));
        assertFalse(prom.contains("loomq_scheduler_max_pending_intents"));
        assertFalse(prom.contains("loomq_trigger_latency"));
        assertFalse(prom.contains("loomq_wake_latency_ms_p95"));
        assertFalse(prom.contains("loomq_total_latency_ms_p95"));
        assertFalse(prom.contains("loomq_webhook_"));
        assertFalse(prom.contains("loomq_bucket_intent_count"));
        assertFalse(prom.contains("loomq_ready_queue_size"));
    }
}
