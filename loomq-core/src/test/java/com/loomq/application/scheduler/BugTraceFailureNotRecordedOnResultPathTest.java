package com.loomq.application.scheduler;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.spi.DeliveryHandler;
import com.loomq.spi.DeliveryHandler.DeliveryResult;
import com.loomq.store.ConcurrentIntentStore;
import com.loomq.tracing.IntentTraceStore;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * DeliveryResult.RETRY 等结果路径也应记录 trace failure，不能只在异常路径记录。
 */
class BugTraceFailureNotRecordedOnResultPathTest {

    @Test
    void retryResultMustRecordTraceFailure() throws Exception {
        IntentTraceStore traceStore = new IntentTraceStore();
        ConcurrentIntentStore store = new ConcurrentIntentStore();
        DeliveryHandler handler = i -> CompletableFuture.completedFuture(DeliveryResult.RETRY);

        PrecisionScheduler scheduler = new PrecisionScheduler(store, handler, null, null, new com.loomq.common.MetricsCollector(), traceStore);
        scheduler.start();

        Intent intent = new Intent("trace-retry-0001");
        intent.setExecuteAt(Instant.now());
        intent.setPrecisionTier(PrecisionTier.ULTRA);
        store.save(intent);
        scheduler.schedule(intent);

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (traceStore.get("trace-retry-0001") == null
                || traceStore.get("trace-retry-0001").failureReason() == null) {
            if (System.nanoTime() > deadline) {
                break;
            }
            Thread.sleep(10);
        }

        assertNotNull(traceStore.get("trace-retry-0001").failureReason(),
            "RETRY result path must record a trace failure reason");
        scheduler.stop();
    }
}
