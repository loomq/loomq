package com.loomq.common;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * R21: wakeup 延迟以微秒记录(LATENCY_BOUNDS 为 µs,调度器 recordWakeupLatency 传
 * toNanos()/1000),但导出名/HELP 标称 ms——5ms 唤醒显示为 5000"ms",1000× 错标,
 * 与 due→dispatch lag(ms 真值)单位不一致。修复:导出名/HELP 改为 us。
 */
class BugWakeupLatencyUnitExportTest {

    @Test
    void wakeupLatencyExportsMustClaimMicroseconds() {
        String prom = new MetricsCollector().exportPrometheusMetrics();
        // 修复前:wakeup 导出标称 ms 而值为 µs
        assertFalse(prom.contains("loomq_scheduler_wakeup_latency_ms_p95"),
            "wakeup latency values are microseconds; export must not claim ms");
        assertTrue(prom.contains("loomq_scheduler_wakeup_latency_us_p95"),
            "wakeup latency P95 export must be named in microseconds");
        assertTrue(prom.contains("loomq_scheduler_wakeup_latency_us_p99"));
        assertTrue(prom.contains("loomq_scheduler_wakeup_latency_us_p999"));
        // lag 指标是 ms 真值,不得误改
        assertTrue(prom.contains("loomq_dispatch_queue_lag_ms_p95"),
            "due->dispatch lag stays in ms (true millisecond values)");
    }
}
