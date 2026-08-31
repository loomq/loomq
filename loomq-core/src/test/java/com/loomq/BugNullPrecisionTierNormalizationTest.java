package com.loomq;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.loomq.domain.intent.AckMode;
import com.loomq.domain.intent.Intent;
import com.loomq.spi.DeliveryHandler.DeliveryResult;
import java.nio.file.Path;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * 内部 live Intent 不允许 precisionTier 为 null：创建入口应归一化为默认档，
 * 否则 trace/内部逻辑与外部 copy 看到的状态不一致。
 */
class BugNullPrecisionTierNormalizationTest {

    @TempDir Path tmp;

    @Test
    void createMustNormalizeNullPrecisionTier() throws Exception {
        try (LoomqEngine engine = LoomqEngine.builder()
                .dataDir(tmp.resolve("n")).nodeId("n1")
                .deliveryHandler(i -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS))
                .build()) {
            engine.start();
            Intent it = new Intent("null-tier-0003");
            it.setExecuteAt(Instant.ofEpochMilli(System.currentTimeMillis() + 60_000));
            it.setPrecisionTier(null);
            engine.createIntent(it, AckMode.DURABLE).get();

            Intent internal = engine.getIntentStoreInternal().findByIdInternal("null-tier-0003");
            assertNotNull(internal.getPrecisionTier(),
                "internal live Intent must not keep null precisionTier after create");
        }
    }
}
