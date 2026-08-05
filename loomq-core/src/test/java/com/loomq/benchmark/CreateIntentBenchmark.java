package com.loomq.benchmark;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.LoomqEngine;
import com.loomq.domain.intent.AckMode;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.spi.DeliveryHandler;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * createIntent 吞吐基准:测量批量创建 + DURABLE 落盘的 QPS。
 *
 * <p>纳入 full-tests profile(@Tag("benchmark")),不阻塞 CI fast/slow 门禁。
 */
@Tag("benchmark")
class CreateIntentBenchmark {

    private static final DeliveryHandler NOOP = i ->
        CompletableFuture.completedFuture(DeliveryHandler.DeliveryResult.SUCCESS);

    @Test
    void measureCreateIntentThroughput(@TempDir Path tmp) throws Exception {
        int count = 500;
        try (LoomqEngine engine = LoomqEngine.builder()
                .dataDir(tmp).nodeId("bench-1").deliveryHandler(NOOP).build()) {
            engine.start();

            List<CompletableFuture<Long>> futures = new ArrayList<>(count);
            long startNs = System.nanoTime();

            for (int i = 0; i < count; i++) {
                Intent intent = new Intent();
                intent.setExecuteAt(Instant.now().plusSeconds(30));
                intent.setPrecisionTier(PrecisionTier.STANDARD);
                futures.add(engine.createIntent(intent, AckMode.DURABLE));
            }

            // Wait for all
            for (var f : futures) f.get();

            long elapsedMs = (System.nanoTime() - startNs) / 1_000_000;
            double qps = count * 1000.0 / elapsedMs;

            System.out.printf("[Benchmark] createIntent DURABLE: %d intents in %dms (%.1f QPS)%n",
                count, elapsedMs, qps);
            System.out.printf("RESULT|create|batch=single|count=%d|ms=%d|qps=%.0f%n",
                count, elapsedMs, qps);

            assertTrue(elapsedMs > 0, "elapsed time must be positive");
            assertTrue(qps > 0, "QPS must be positive");
        }
    }

    @Test
    void measureBatchCreateIntentThroughput(@TempDir Path tmp) throws Exception {
        int count = 500;
        try (LoomqEngine engine = LoomqEngine.builder()
                .dataDir(tmp).nodeId("bench-2").deliveryHandler(NOOP).build()) {
            engine.start();

            List<Intent> intents = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                Intent intent = new Intent();
                intent.setExecuteAt(Instant.now().plusSeconds(30));
                intent.setPrecisionTier(PrecisionTier.STANDARD);
                intents.add(intent);
            }

            long startNs = System.nanoTime();
            engine.createIntents(intents, AckMode.DURABLE).get();
            long elapsedMs = (System.nanoTime() - startNs) / 1_000_000;
            double qps = count * 1000.0 / elapsedMs;

            System.out.printf("[Benchmark] createIntents batch DURABLE: %d intents in %dms (%.1f QPS)%n",
                count, elapsedMs, qps);
            System.out.printf("RESULT|create|batch=batch|count=%d|ms=%d|qps=%.0f%n",
                count, elapsedMs, qps);

            assertTrue(elapsedMs > 0);
            assertTrue(qps > 0);
        }
    }
}
