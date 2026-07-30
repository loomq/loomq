package com.loomq.common;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 运行态业务指标注册表。
 *
 * 负责 intents 生命周期、webhook 计数和 ready/bucket 状态，避免 MetricsCollector 继续膨胀。
 */
final class OperationalMetricsRegistry {

    private final AtomicLong intentsCreatedTotal = new AtomicLong(0);
    private final AtomicLong intentsAckSuccessTotal = new AtomicLong(0);
    private final AtomicLong intentsFailedTerminalTotal = new AtomicLong(0);
    private final AtomicLong intentsCancelledTotal = new AtomicLong(0);
    private final AtomicLong intentsRetryTotal = new AtomicLong(0);
    private final AtomicLong intentsExpiredTotal = new AtomicLong(0);
    private final AtomicLong intentsDeadLetterTotal = new AtomicLong(0);
    private final AtomicLong recoveryOverdueTotal = new AtomicLong(0);
    private final AtomicLong webhookRequestsTotal = new AtomicLong(0);
    private final AtomicLong webhookTimeoutTotal = new AtomicLong(0);
    private final AtomicLong webhookErrorTotal = new AtomicLong(0);

    private volatile long bucketIntentCount = 0;
    private volatile long readyQueueSize = 0;

    void incrementIntentsCreated() {
        intentsCreatedTotal.incrementAndGet();
    }

    void incrementIntentsAckSuccess() {
        intentsAckSuccessTotal.incrementAndGet();
    }

    void incrementIntentsFailedTerminal() {
        intentsFailedTerminalTotal.incrementAndGet();
    }

    void incrementIntentsCancelled() {
        intentsCancelledTotal.incrementAndGet();
    }

    void incrementIntentsRetry() {
        intentsRetryTotal.incrementAndGet();
    }

    void incrementWebhookRequests() {
        webhookRequestsTotal.incrementAndGet();
    }

    void incrementWebhookTimeout() {
        webhookTimeoutTotal.incrementAndGet();
    }

    void incrementWebhookError() {
        webhookErrorTotal.incrementAndGet();
    }

    void incrementIntentsExpired() {
        intentsExpiredTotal.incrementAndGet();
    }

    void incrementIntentsDeadLetter() {
        intentsDeadLetterTotal.incrementAndGet();
    }

    void incrementRecoveryOverdue() {
        recoveryOverdueTotal.incrementAndGet();
    }

    void updateBucketMetrics(long bucketIntentCount, long readyQueueSize) {
        this.bucketIntentCount = bucketIntentCount;
        this.readyQueueSize = readyQueueSize;
    }

    double getWebhookTimeoutRate() {
        long totalRequests = webhookRequestsTotal.get();
        if (totalRequests == 0) {
            return 0;
        }
        return (double) webhookTimeoutTotal.get() / totalRequests * 100;
    }

    long getIntentsCreatedTotal() {
        return intentsCreatedTotal.get();
    }

    long getIntentsAckSuccessTotal() {
        return intentsAckSuccessTotal.get();
    }

    long getIntentsCancelledTotal() {
        return intentsCancelledTotal.get();
    }

    long getIntentsExpiredTotal() {
        return intentsExpiredTotal.get();
    }

    long getIntentsDeadLetterTotal() {
        return intentsDeadLetterTotal.get();
    }

    long getRecoveryOverdueTotal() {
        return recoveryOverdueTotal.get();
    }

    void appendPrometheusMetrics(Map<String, Long> intentStats, StringBuilder sb) {
        // Intent 统计
        sb.append("# HELP loomq_intents_total Total number of intents\n");
        sb.append("# TYPE loomq_intents_total gauge\n");
        appendMetric(sb, "loomq_intents_total", intentStats.getOrDefault("total", 0L));
        sb.append("\n");

        sb.append("# HELP loomq_intents_pending Number of pending intents\n");
        sb.append("# TYPE loomq_intents_pending gauge\n");
        appendMetric(sb, "loomq_intents_pending", intentStats.getOrDefault("pending", 0L));
        sb.append("\n");

        sb.append("# HELP loomq_intents_scheduled Number of scheduled intents\n");
        sb.append("# TYPE loomq_intents_scheduled gauge\n");
        appendMetric(sb, "loomq_intents_scheduled", intentStats.getOrDefault("scheduled", 0L));
        sb.append("\n");

        sb.append("# HELP loomq_intents_dispatching Number of dispatching intents\n");
        sb.append("# TYPE loomq_intents_dispatching gauge\n");
        appendMetric(sb, "loomq_intents_dispatching", intentStats.getOrDefault("dispatching", 0L));
        sb.append("\n");

        // 计数器
        sb.append("# HELP loomq_intents_created_total Total intents created\n");
        sb.append("# TYPE loomq_intents_created_total counter\n");
        appendMetric(sb, "loomq_intents_created_total", intentsCreatedTotal.get());
        sb.append("\n");

        sb.append("# HELP loomq_intents_ack_success_total Total intents acknowledged success\n");
        sb.append("# TYPE loomq_intents_ack_success_total counter\n");
        appendMetric(sb, "loomq_intents_ack_success_total", intentsAckSuccessTotal.get());
        sb.append("\n");

        sb.append("# HELP loomq_intents_failed_terminal_total Total intents failed terminal\n");
        sb.append("# TYPE loomq_intents_failed_terminal_total counter\n");
        appendMetric(sb, "loomq_intents_failed_terminal_total", intentsFailedTerminalTotal.get());
        sb.append("\n");

        sb.append("# HELP loomq_intents_cancelled_total Total intents cancelled\n");
        sb.append("# TYPE loomq_intents_cancelled_total counter\n");
        appendMetric(sb, "loomq_intents_cancelled_total", intentsCancelledTotal.get());
        sb.append("\n");

        sb.append("# HELP loomq_intents_retry_total Total intent retries\n");
        sb.append("# TYPE loomq_intents_retry_total counter\n");
        appendMetric(sb, "loomq_intents_retry_total", intentsRetryTotal.get());
        sb.append("\n");

        sb.append("# HELP loomq_intents_expired_total Total intents expired\n");
        sb.append("# TYPE loomq_intents_expired_total counter\n");
        appendMetric(sb, "loomq_intents_expired_total", intentsExpiredTotal.get());
        sb.append("\n");

        sb.append("# HELP loomq_intents_dead_letter_total Total intents in dead letter\n");
        sb.append("# TYPE loomq_intents_dead_letter_total counter\n");
        appendMetric(sb, "loomq_intents_dead_letter_total", intentsDeadLetterTotal.get());
        sb.append("\n");

        sb.append("# HELP loomq_recovery_overdue_total Total intents marked overdue during recovery\n");
        sb.append("# TYPE loomq_recovery_overdue_total counter\n");
        appendMetric(sb, "loomq_recovery_overdue_total", recoveryOverdueTotal.get());
        sb.append("\n");

        // Bucket 指标
        sb.append("# HELP loomq_bucket_intent_count Number of intents in time buckets\n");
        sb.append("# TYPE loomq_bucket_intent_count gauge\n");
        appendMetric(sb, "loomq_bucket_intent_count", bucketIntentCount);
        sb.append("\n");

        sb.append("# HELP loomq_ready_queue_size Size of ready queue\n");
        sb.append("# TYPE loomq_ready_queue_size gauge\n");
        appendMetric(sb, "loomq_ready_queue_size", readyQueueSize);
        sb.append("\n");

        // Webhook 指标
        sb.append("# HELP loomq_webhook_requests_total Total webhook requests\n");
        sb.append("# TYPE loomq_webhook_requests_total counter\n");
        appendMetric(sb, "loomq_webhook_requests_total", webhookRequestsTotal.get());
        sb.append("\n");

        sb.append("# HELP loomq_webhook_timeout_total Total webhook timeouts\n");
        sb.append("# TYPE loomq_webhook_timeout_total counter\n");
        appendMetric(sb, "loomq_webhook_timeout_total", webhookTimeoutTotal.get());
        sb.append("\n");

        sb.append("# HELP loomq_webhook_error_total Total webhook errors\n");
        sb.append("# TYPE loomq_webhook_error_total counter\n");
        appendMetric(sb, "loomq_webhook_error_total", webhookErrorTotal.get());
        sb.append("\n");

        sb.append("# HELP loomq_webhook_timeout_rate_percent Webhook timeout rate in percent\n");
        sb.append("# TYPE loomq_webhook_timeout_rate_percent gauge\n");
        sb.append(String.format("loomq_webhook_timeout_rate_percent %.2f\n", getWebhookTimeoutRate()));
        sb.append("\n");
    }

    private void appendMetric(StringBuilder sb, String name, long value) {
        sb.append(name).append(" ").append(value).append("\n");
    }
}
