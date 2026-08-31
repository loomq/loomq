package com.loomq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.domain.intent.AckMode;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.spi.DeliveryHandler;
import com.loomq.spi.DeliveryHandler.DeliveryResult;
import com.loomq.tracing.IntentTrace;
import com.loomq.tracing.IntentTraceStore;
import java.nio.file.Path;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * R21: trace 生命周期缺环——(a) 冷 create(>hotBoundaryMs 走 promotionDaemon.register)
 * 从未 schedule,冷 intent 无 trace 条目,排查无迹可循;(b) cancelIntent 成功路径零
 * trace 调用,取消后 trace 恒显 SCHEDULED/DUE。修复:命令服务注入 traceStore——冷
 * create 补 recordCreated,取消(热/冷)补 updateStatus(CANCELED),fireNow 补 DUE 标记。
 */
class BugColdCreateAndCancelTraceTest {

    @TempDir
    Path tempDir;

    private final DeliveryHandler mockHandler =
        intent -> CompletableFuture.completedFuture(DeliveryResult.DEAD_LETTER);

    @Test
    void coldCreateMustProduceTrace() throws Exception {
        IntentTraceStore traceStore = new IntentTraceStore();
        try (LoomqEngine engine = LoomqEngine.builder()
            .walDir(tempDir)
            .nodeId("r21-cold-trace")
            .deliveryHandler(mockHandler)
            .intentTraceStore(traceStore)
            .build()) {
            engine.start();

            Intent cold = new Intent("r21-cold-trace-0001");
            cold.setExecuteAt(Instant.ofEpochMilli(System.currentTimeMillis() + 2L * 60 * 60 * 1000)); // +2h > hotBoundary
            engine.createIntent(cold, AckMode.DURABLE).join();

            IntentTrace trace = traceStore.get("r21-cold-trace-0001");
            assertNotNull(trace, "cold creates must record a trace entry (were: no trace until promotion)");
            assertEquals(IntentStatus.CREATED, trace.status());
        }
    }

    @Test
    void cancelMustMarkTraceCanceled() throws Exception {
        IntentTraceStore traceStore = new IntentTraceStore();
        try (LoomqEngine engine = LoomqEngine.builder()
            .walDir(tempDir)
            .nodeId("r21-cancel-trace")
            .deliveryHandler(mockHandler)
            .intentTraceStore(traceStore)
            .build()) {
            engine.start();

            Intent hot = new Intent("r21-cancel-trace-0001");
            hot.setExecuteAt(Instant.ofEpochMilli(System.currentTimeMillis() + 60_000));
            engine.createIntent(hot, AckMode.DURABLE).join();

            assertTrue(engine.cancelIntent("r21-cancel-trace-0001"), "cancel must succeed");
            IntentTrace trace = traceStore.get("r21-cancel-trace-0001");
            assertNotNull(trace, "hot create must have a trace");
            assertEquals(IntentStatus.CANCELED, trace.status(),
                "canceled intent's trace must show CANCELED, not the stale SCHEDULED");
        }
    }
}
