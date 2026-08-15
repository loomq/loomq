package com.loomq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.loomq.domain.intent.AckMode;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.domain.intent.RedeliveryPolicy;
import com.loomq.spi.DeliveryHandler;
import com.loomq.spi.DeliveryHandler.DeliveryResult;
import java.nio.file.Path;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * updateIntent 应像 createIntent 一样在持久化前校验更新后的 Intent。
 * 当前缺失校验，非法 deadline / redelivery 会被静默接受。
 */
class BugUpdateIntentValidationTest {

    @TempDir Path tmp;

    private static final DeliveryHandler SUCCESS =
        i -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS);

    @Test
    void updateMustRejectDeadlineNotAfterExecuteAt() throws Exception {
        String id = "upd-valid-deadline-01";
        try (LoomqEngine engine = LoomqEngine.builder()
                .dataDir(tmp.resolve("d")).nodeId("d1")
                .deliveryHandler(SUCCESS)
                .build()) {
            engine.start();
            long t0 = System.currentTimeMillis();
            Intent it = new Intent(id);
            it.setExecuteAt(Instant.ofEpochMilli(t0 + 60_000));
            it.setDeadline(Instant.ofEpochMilli(t0 + 120_000));
            it.setPrecisionTier(PrecisionTier.ULTRA);
            engine.createIntent(it, AckMode.DURABLE).get();

            Instant original = engine.getIntent(id).orElseThrow().getExecuteAt();
            assertThrows(IllegalArgumentException.class,
                () -> engine.updateIntent(id, i -> i.setDeadline(Instant.ofEpochMilli(t0 + 30_000))),
                "deadline must remain after executeAt after update");
            assertEquals(original, engine.getIntent(id).orElseThrow().getExecuteAt(),
                "failed update must keep the original schedule");
        }
    }

    @Test
    void updateMustRejectInvalidRedeliveryPolicy() throws Exception {
        String id = "upd-valid-redelivery-01";
        try (LoomqEngine engine = LoomqEngine.builder()
                .dataDir(tmp.resolve("r")).nodeId("r1")
                .deliveryHandler(SUCCESS)
                .build()) {
            engine.start();
            long t0 = System.currentTimeMillis();
            Intent it = new Intent(id);
            it.setExecuteAt(Instant.ofEpochMilli(t0 + 60_000));
            it.setPrecisionTier(PrecisionTier.ULTRA);
            engine.createIntent(it, AckMode.DURABLE).get();

            Instant original = engine.getIntent(id).orElseThrow().getExecuteAt();
            assertThrows(IllegalArgumentException.class,
                () -> engine.updateIntent(id, i -> i.setRedelivery(new RedeliveryPolicy(0, "exponential", 1000, 60000, 2.0, false))),
                "redelivery.maxAttempts must remain >= 1 after update");
            assertEquals(original, engine.getIntent(id).orElseThrow().getExecuteAt(),
                "failed update must keep the original schedule");
        }
    }
}
