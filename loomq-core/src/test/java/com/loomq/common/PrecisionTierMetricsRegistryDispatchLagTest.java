package com.loomq.common;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.loomq.domain.intent.PrecisionTier;
import com.loomq.domain.intent.PrecisionTierCatalog;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * R7: due→dispatch lag 直方图单位错误——调度器 {@code recordDispatchQueueLag} 传毫秒
 * ({@code lagMs = nanoTime 差 / 1_000_000}),但 {@link PrecisionTierMetricsRegistry} 用
 * wakeup 延迟的微秒边界({@code LATENCY_BOUNDS})分桶,导致导出指标
 * {@code loomq_dispatch_queue_lag_ms_p95} 比真实值小 1000×。
 *
 * <p>回归测试:记录 7ms 的 lag——修复前 7 被当作微秒落入 0μs 桶(P95=0),
 * 修复后落入 5ms 桶(P95=5)。</p>
 */
class PrecisionTierMetricsRegistryDispatchLagTest {

    @Test
    @DisplayName("due→dispatch lag 用毫秒边界分桶,不被当作微秒")
    void dispatchQueueLagUsesMillisecondBuckets() {
        PrecisionTierMetricsRegistry registry =
            new PrecisionTierMetricsRegistry(PrecisionTierCatalog.defaultCatalog());
        registry.recordDispatchQueueLagByTier(PrecisionTier.ULTRA, 7);

        assertEquals(5, registry.getDispatchQueueLagP95(PrecisionTier.ULTRA),
            "7ms due->dispatch lag must bucket to 5ms, not 0us");
    }
}
