package com.loomq;

import static org.junit.jupiter.api.Assertions.*;

import com.loomq.domain.intent.AckMode;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.spi.DeliveryHandler;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * P1-1 验证:终态(SUCCESS/DEAD_LETTER)落盘后,崩溃恢复不再将已终结 Intent
 * 误标为 overdue EXPIRED/DEAD_LETTERED。
 */
class TerminalStateCrashRecoveryTest {

    @Test
    void ackedIntentSurvivesCrashRecovery(@TempDir Path tmp) throws Exception {
        DeliveryHandler successHandler = i ->
            CompletableFuture.completedFuture(DeliveryHandler.DeliveryResult.SUCCESS);

        String intentId;
        try (LoomqEngine engine = LoomqEngine.builder()
                .dataDir(tmp).nodeId("crash-1").deliveryHandler(successHandler).build()) {
            engine.start();

            Intent intent = new Intent();
            intentId = intent.getIntentId();
            intent.setExecuteAt(Instant.now().plusSeconds(1));
            intent.setPrecisionTier(PrecisionTier.STANDARD);
            engine.createIntent(intent, AckMode.DURABLE).get();

            // Wait for delivery to reach terminal state
            awaitTerminal(engine, intentId, Duration.ofSeconds(10));
            Optional<Intent> live = engine.getIntent(intentId);
            assertTrue(live.isPresent());
            assertEquals(IntentStatus.ACKED, live.get().getStatus(),
                "intent must be ACKED before crash");
        }

        // Reopen: recovery must NOT mark already-ACKED intent as overdue
        try (LoomqEngine engine2 = LoomqEngine.builder()
                .dataDir(tmp).nodeId("crash-2").deliveryHandler(successHandler).build()) {
            engine2.start();

            assertEquals(0, engine2.getMetricsCollector().getRecoveryOverdueTotal(),
                "already-ACKED intent must not be marked overdue by recovery");

            assertTrue(engine2.getIntent(intentId).isEmpty(),
                "terminal intent should not be loaded into memory by recovery");
        }
    }

    @Test
    void deadLetteredIntentSurvivesCrashRecovery(@TempDir Path tmp) throws Exception {
        DeliveryHandler deadLetterHandler = i ->
            CompletableFuture.completedFuture(DeliveryHandler.DeliveryResult.DEAD_LETTER);

        String intentId;
        try (LoomqEngine engine = LoomqEngine.builder()
                .dataDir(tmp).nodeId("crash-1").deliveryHandler(deadLetterHandler).build()) {
            engine.start();

            Intent intent = new Intent();
            intentId = intent.getIntentId();
            intent.setExecuteAt(Instant.now().plusSeconds(1));
            intent.setPrecisionTier(PrecisionTier.STANDARD);
            engine.createIntent(intent, AckMode.DURABLE).get();

            awaitTerminal(engine, intentId, Duration.ofSeconds(10));
            Optional<Intent> live = engine.getIntent(intentId);
            assertTrue(live.isPresent());
            assertEquals(IntentStatus.DEAD_LETTERED, live.get().getStatus(),
                "intent must be DEAD_LETTERED before crash");
        }

        try (LoomqEngine engine2 = LoomqEngine.builder()
                .dataDir(tmp).nodeId("crash-2").deliveryHandler(deadLetterHandler).build()) {
            engine2.start();

            assertEquals(0, engine2.getMetricsCollector().getRecoveryOverdueTotal(),
                "already-DEAD_LETTERED intent must not be marked overdue by recovery");
        }
    }

    private void awaitTerminal(LoomqEngine engine, String intentId, Duration timeout)
            throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            Optional<Intent> opt = engine.getIntent(intentId);
            if (opt.isPresent() && opt.get().getStatus().isTerminal()) {
                return;
            }
            Thread.sleep(50);
        }
        fail("Intent " + intentId + " did not reach terminal state within " + timeout);
    }
}
