package com.loomq.benchmark;

/** 稳态吞吐测量参数。 */
public record BenchmarkConfig(
    int inFlight,          // 闭环在途数（= 档位 maxConcurrency）
    long delayRangeMs,     // executeAt 延迟上限（1..range 随机）
    long measureWindowMs,  // 测量窗
    long subWindowMs,      // 子窗采样间隔
    long warmupMs          // 预热时长（测量前丢弃）
) {
    public static BenchmarkConfig forTier(int maxConcurrency) {
        return new BenchmarkConfig(maxConcurrency, 5, 3_000, 100, 1_000);
    }
    public BenchmarkConfig {
        if (inFlight <= 0 || delayRangeMs <= 0 || measureWindowMs <= 0
            || subWindowMs <= 0 || warmupMs < 0) {
            throw new IllegalArgumentException("invalid BenchmarkConfig");
        }
    }
}