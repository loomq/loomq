package com.loomq;

import static org.junit.jupiter.api.Assertions.*;

import com.loomq.domain.intent.AckMode;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.spi.DeliveryHandler;
import com.loomq.testutil.TestEngines;
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
            TestEngines.awaitTerminal(engine, intentId, Duration.ofSeconds(10));
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

            TestEngines.awaitTerminal(engine, intentId, Duration.ofSeconds(10));
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

    @Test
    void ackedIntentSurvivesWithoutClose(@TempDir Path tmp) throws Exception {
        DeliveryHandler successHandler = i ->
            CompletableFuture.completedFuture(DeliveryHandler.DeliveryResult.SUCCESS);

        String intentId;
        // 1. 启动引擎、创建 intent、等待终态
        LoomqEngine engine = LoomqEngine.builder()
            .dataDir(tmp).nodeId("crash-noclose").deliveryHandler(successHandler).build();
        engine.start();

        Intent intent = new Intent();
        intentId = intent.getIntentId();
        intent.setExecuteAt(Instant.now().plusSeconds(1));
        intent.setPrecisionTier(PrecisionTier.STANDARD);
        engine.createIntent(intent, AckMode.DURABLE).get();

        TestEngines.awaitTerminal(engine, intentId, Duration.ofSeconds(10));
        Optional<Intent> live = engine.getIntent(intentId);
        assertTrue(live.isPresent());
        assertEquals(IntentStatus.ACKED, live.get().getStatus(),
            "intent must be ACKED before simulated crash");

        // 2. 模拟崩溃（不调用 close，不 forceDirty）
        engine.simulateCrash();

        // Windows 下旧 mmap 段可能在重开前仍被引用，sleep+gc 加速段释放；
        // 若重开失败属测试环境问题而非被测语义问题。
        Thread.sleep(500);
        System.gc();

        // 3. 同目录重开，验证终态在盘上
        try (LoomqEngine engine2 = LoomqEngine.builder()
                .dataDir(tmp).nodeId("recover").deliveryHandler(successHandler).build()) {
            engine2.start();

            assertEquals(0, engine2.getMetricsCollector().getRecoveryOverdueTotal(),
                "already-ACKED intent must survive without close() - terminal write must reach the wheel without close() force");

            assertTrue(engine2.getIntent(intentId).isEmpty(),
                "terminal intent should not be loaded into memory by recovery");
        }
    }

}
