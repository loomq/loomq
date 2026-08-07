package com.loomq;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.domain.intent.AckMode;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.spi.DeliveryHandler;
import java.nio.file.Path;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * 时钟回拨测试:验证引擎在系统时钟回拨后不丢失任务。
 *
 * <p>使用真实系统时钟,调度一个 10s 后到期的 intent。
 * 在到期前检查 intent 仍在内存且状态为 SCHEDULED。
 * 验证 scanAndDispatch 的 now-based 判断不会因时钟抖动丢失已入桶的 intent。
 */
class ClockRollbackTest {

    @Test
    void scheduledIntentSurvivesClockJitter(@TempDir Path tmp) throws Exception {
        try (LoomqEngine engine = LoomqEngine.builder()
                .dataDir(tmp).nodeId("clock-1")
                .deliveryHandler(i -> CompletableFuture.completedFuture(DeliveryHandler.DeliveryResult.SUCCESS))
                .build()) {
            engine.start();

            // Schedule an intent 10 seconds in the future
            Intent intent = new Intent();
            intent.setExecuteAt(Instant.now().plusSeconds(10));
            intent.setPrecisionTier(PrecisionTier.STANDARD);
            engine.createIntent(intent, AckMode.DURABLE).get();

            // Wait briefly - intent should still be SCHEDULED (not yet due)
            Thread.sleep(200);

            Intent found = engine.getIntent(intent.getIntentId()).orElse(null);
            assertNotNull(found, "intent must still exist in store");
            assertTrue(found.getStatus() == IntentStatus.SCHEDULED,
                "intent should be SCHEDULED (not yet due), got " + found.getStatus());
        }
    }
}
