package com.loomq.spi;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

/**
 * 投递上下文
 *
 * 传递给 RedeliveryDecider 的上下文信息
 *
 * @author loomq
 * @since v0.5.0
 */
public class DeliveryContext {

    /**
     * 投递唯一标识
     */
    private final String deliveryId;

    /**
     * 关联的 Intent ID
     */
    private final String intentId;

    /**
     * 当前尝试次数（从 1 开始）
     */
    private final int attempt;

    /**
     * 投递开始时间
     */
    private final Instant startedAt;

    /**
     * 投递结束时间
     */
    private Instant finishedAt;

    /**
     * HTTP 状态码（如果收到响应）
     */
    private Integer httpStatus;

    /**
     * HTTP 响应头
     */
    private Map<String, String> responseHeaders;

    /**
     * HTTP 响应体
     */
    private String responseBody;

    /**
     * 异常信息（如果发生异常）
     */
    private Throwable exception;

    /**
     * 请求耗时
     */
    private Duration latency;

    public DeliveryContext(String deliveryId, String intentId, int attempt) {
        this.deliveryId = Objects.requireNonNull(deliveryId, "deliveryId cannot be null");
        this.intentId = Objects.requireNonNull(intentId, "intentId cannot be null");
        this.attempt = attempt;
        this.startedAt = Instant.now();
    }

    /**
     * 标记为成功
     */
    public void markSuccess(int httpStatus, Map<String, String> headers, String body) {
        this.finishedAt = Instant.now();
        this.httpStatus = httpStatus;
        this.responseHeaders = headers;
        this.responseBody = body;
        this.latency = Duration.between(startedAt, finishedAt);
    }

    /**
     * 标记为失败
     */
    public void markFailure(Throwable exception) {
        this.finishedAt = Instant.now();
        this.exception = exception;
        this.latency = Duration.between(startedAt, finishedAt);
    }

    /**
     * 标记为失败（带 HTTP 状态）
     */
    public void markFailure(int httpStatus, Map<String, String> headers, String body) {
        this.finishedAt = Instant.now();
        this.httpStatus = httpStatus;
        this.responseHeaders = headers;
        this.responseBody = body;
        this.latency = Duration.between(startedAt, finishedAt);
    }

    // ========== 判定方法 ==========

    /**
     * 是否成功收到响应
     */
    public boolean hasResponse() {
        return httpStatus != null;
    }

    /**
     * 是否发生异常
     */
    public boolean hasException() {
        return exception != null;
    }

    /**
     * 是否超时
     */
    public boolean isTimeout() {
        return hasException() &&
               (exception instanceof java.net.SocketTimeoutException ||
                exception instanceof TimeoutException);
    }

    /**
     * 是否连接错误
     */
    public boolean isConnectionError() {
        if (!hasException()) return false;
        String message = exception.getMessage();
        return message != null && (
            message.contains("Connection refused") ||
            message.contains("Connection reset") ||
            message.contains("Connection timed out") ||
            exception instanceof java.net.ConnectException
        );
    }

    /**
     * 是否服务器错误 (5xx)
     */
    public boolean isServerError() {
        return hasResponse() && httpStatus >= 500 && httpStatus < 600;
    }

    /**
     * 是否客户端错误 (4xx)
     */
    public boolean isClientError() {
        return hasResponse() && httpStatus >= 400 && httpStatus < 500;
    }

    /**
     * 是否成功响应 (2xx)
     */
    public boolean isSuccess() {
        return hasResponse() && httpStatus >= 200 && httpStatus < 300;
    }

    // ========== Getters ==========

    public String getDeliveryId() {
        return deliveryId;
    }

    public String getIntentId() {
        return intentId;
    }

    public int getAttempt() {
        return attempt;
    }

    public Instant getStartedAt() {
        return startedAt;
    }

    public Optional<Instant> getFinishedAt() {
        return Optional.ofNullable(finishedAt);
    }

    public Optional<Integer> getHttpStatus() {
        return Optional.ofNullable(httpStatus);
    }

    public Optional<Map<String, String>> getResponseHeaders() {
        return Optional.ofNullable(responseHeaders);
    }

    public Optional<String> getResponseBody() {
        return Optional.ofNullable(responseBody);
    }

    public Optional<Throwable> getException() {
        return Optional.ofNullable(exception);
    }

    public Optional<Duration> getLatency() {
        return Optional.ofNullable(latency);
    }

    @Override
    public String toString() {
        return String.format("DeliveryContext{deliveryId=%s, attempt=%d, status=%s, latency=%s}",
            deliveryId, attempt,
            hasResponse() ? httpStatus : (hasException() ? "ERROR" : "PENDING"),
            latency);
    }
}
