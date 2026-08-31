package com.loomq.domain.intent;

import java.util.Objects;

/**
 * 重投策略配置
 *
 * @author loomq
 * @since v0.5.0
 */
public class RedeliveryPolicy {

    /**
     * 最大尝试次数
     */
    private int maxAttempts;

    /**
     * 退避策略：fixed, exponential
     */
    private String backoff;

    /**
     * 初始延迟（毫秒）
     */
    private long initialDelayMs;

    /**
     * 最大延迟（毫秒）
     */
    private long maxDelayMs;

    /**
     * 指数退避乘数
     */
    private double multiplier;

    /**
     * 是否启用抖动
     */
    private boolean jitter;

    public RedeliveryPolicy() {
        this.maxAttempts = 5;
        this.backoff = "exponential";
        this.initialDelayMs = 1000;
        this.maxDelayMs = 60000;
        this.multiplier = 2.0;
        this.jitter = true;
    }

    public RedeliveryPolicy(int maxAttempts, String backoff, long initialDelayMs,
                            long maxDelayMs, double multiplier, boolean jitter) {
        this.maxAttempts = maxAttempts;
        this.backoff = Objects.requireNonNullElse(backoff, "exponential");
        this.initialDelayMs = initialDelayMs;
        this.maxDelayMs = maxDelayMs;
        this.multiplier = multiplier;
        this.jitter = jitter;
    }

    /**
     * 计算下一次重试延迟
     */
    public long calculateDelay(int attempt) {
        long delay;

        if ("fixed".equalsIgnoreCase(backoff)) {
            delay = initialDelayMs;
        } else {
            // exponential
            delay = (long) (initialDelayMs * Math.pow(multiplier, attempt - 1));
        }

        delay = Math.min(delay, maxDelayMs);

        if (jitter) {
            // 添加 ±20% 的抖动
            double jitterFactor = 0.8 + Math.random() * 0.4;
            delay = (long) (delay * jitterFactor);
            // jitter 后再次钳制:±20% 抖动不得突破 maxDelayMs 硬上限
            // (旧实现 jitter 在钳制之后,50% 概率返回 maxDelayMs 的 1.0~1.2 倍)。
            delay = Math.min(delay, maxDelayMs);
        }

        return delay;
    }

    // ========== Getters / Setters ==========

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public void setMaxAttempts(int maxAttempts) {
        this.maxAttempts = maxAttempts;
    }

    public String getBackoff() {
        return backoff;
    }

    public void setBackoff(String backoff) {
        this.backoff = backoff;
    }

    public long getInitialDelayMs() {
        return initialDelayMs;
    }

    public void setInitialDelayMs(long initialDelayMs) {
        this.initialDelayMs = initialDelayMs;
    }

    public long getMaxDelayMs() {
        return maxDelayMs;
    }

    public void setMaxDelayMs(long maxDelayMs) {
        this.maxDelayMs = maxDelayMs;
    }

    public double getMultiplier() {
        return multiplier;
    }

    public void setMultiplier(double multiplier) {
        this.multiplier = multiplier;
    }

    public boolean isJitter() {
        return jitter;
    }

    public void setJitter(boolean jitter) {
        this.jitter = jitter;
    }

    /**
     * 创建当前 RedeliveryPolicy 的独立副本（I5 边界不变量）。
     *
     * @return 独立副本
     */
    public RedeliveryPolicy copy() {
        return new RedeliveryPolicy(maxAttempts, backoff, initialDelayMs,
            maxDelayMs, multiplier, jitter);
    }

    @Override
    public String toString() {
        return String.format("RedeliveryPolicy{maxAttempts=%d, backoff=%s, initialDelay=%dms}",
            maxAttempts, backoff, initialDelayMs);
    }
}
