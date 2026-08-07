package com.loomq.benchmark;

import com.loomq.LoomqEngine;
import com.loomq.domain.intent.*;
import com.loomq.infrastructure.wheel.WheelConfig;
import com.loomq.spi.DeliveryHandler;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** 创建吞吐稳态基准：闭环在"创建完成"上，executeAt=远未来避免投递干扰。 */
@Tag("benchmark")
class CreateIntentBenchmark {

    private static final DeliveryHandler NOOP = i ->
        CompletableFuture.completedFuture(DeliveryHandler.DeliveryResult.SUCCESS);

    private static final int IN_FLIGHT = 64;
    private static final long WINDOW_MS = 2_000;
    private static final int WINDOWS = 5;

    @Test
    void measureCreateIntentThroughput(@TempDir Path tmp) throws Exception {
        measureCreate(tmp, true);   // 单发
    }

    @Test
    void measureBatchCreateIntentThroughput(@TempDir Path tmp) throws Exception {
        measureCreate(tmp, false);  // 批量
    }

    private void measureCreate(Path tmp, boolean single) throws Exception {
        var wheel = WheelConfig.defaultConfig().withDataDir(tmp.toString()).withSlotsPerBucket(65_536);
        try (LoomqEngine engine = LoomqEngine.builder()
                .wheelConfig(wheel).nodeId("bench-create").deliveryHandler(NOOP).build()) {
            engine.start();
            // 数组盒绕过闭包捕获的 definite-assignment 检查：producer 线程延迟到 run() 才启动，
            // 故盒内引用在回调真正触发前已就绪（与 SteadyStateHarness 构造注释一致）。
            SteadyStateHarness[] ref = new SteadyStateHarness[1];
            SteadyStateHarness harness = new SteadyStateHarness(IN_FLIGHT, () -> {
                Intent intent = new Intent();
                intent.setExecuteAt(Instant.now().plusSeconds(30));  // 远未来，不投递
                intent.setPrecisionTier(PrecisionTier.STANDARD);
                if (single) {
                    engine.createIntent(intent, AckMode.DURABLE)
                        .whenComplete((seq, err) -> ref[0].onComplete(0));
                } else {
                    engine.createIntents(List.of(intent), AckMode.DURABLE)
                        .whenComplete((seq, err) -> ref[0].onComplete(0));
                }
            });
            ref[0] = harness;
            var r = harness.run(1_000, WINDOW_MS, WINDOWS);
            System.out.printf("RESULT|create|batch=%s|qps_median=%.0f|qps_iqr=%.0f|samples=%d%n",
                single ? "single" : "batch", r.qpsMedian(), r.qpsIqr(), r.qpsSamples());
        }
    }
}