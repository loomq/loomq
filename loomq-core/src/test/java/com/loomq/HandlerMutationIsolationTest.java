package com.loomq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.domain.intent.AckMode;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.spi.DeliveryHandler;
import com.loomq.testutil.TestEngines;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * I5/G2 固化测试：DeliveryHandler 收到的是防御性快照，handler 内调用 setter
 * 不得影响内核活对象状态。
 *
 * <p>覆盖单发路径（ULTRA）和批量路径（STANDARD），验证：</p>
 * <ul>
 *   <li>handler 对 snapshot 调用 setExecuteAt / setDeadline / setTags 不改写活对象</li>
 *   <li>intent 正常 ACKED（活对象状态机不受 snapshot 变更影响）</li>
 * </ul>
 */
@Tag("integration")
class HandlerMutationIsolationTest {

    /**
     * Handler that aggressively mutates the received Intent (snapshot).
     * If the kernel passes the live object, these mutations corrupt state.
     */
    private static final class MutatingHandler implements DeliveryHandler {
        final CountDownLatch delivered = new CountDownLatch(1);

        @Override
        public CompletableFuture<DeliveryResult> deliverAsync(Intent intent) {
            // Attempt to corrupt kernel state via the received object
            intent.setExecuteAt(Instant.now().plusSeconds(999));
            intent.setDeadline(Instant.now().plusSeconds(999));
            intent.setTags(Map.of("mutated", "true"));
            delivered.countDown();
            return CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        }
    }

    @Test
    void handlerMutationDoesNotAffectKernelUltra(@TempDir Path tmp) throws Exception {
        handlerMutationDoesNotAffectKernel(tmp, "mut-ultra", PrecisionTier.ULTRA);
    }

    @Test
    void handlerMutationDoesNotAffectKernelStandard(@TempDir Path tmp) throws Exception {
        handlerMutationDoesNotAffectKernel(tmp, "mut-std", PrecisionTier.STANDARD);
    }

    private void handlerMutationDoesNotAffectKernel(Path tmp, String nodeId,
                                                     PrecisionTier tier) throws Exception {
        MutatingHandler handler = new MutatingHandler();
        try (LoomqEngine engine = LoomqEngine.builder()
                .dataDir(tmp).nodeId(nodeId).deliveryHandler(handler).build()) {
            engine.start();

            Intent intent = new Intent();
            String id = intent.getIntentId();
            Instant originalExecuteAt = Instant.now().plusMillis(200);
            Instant originalDeadline = Instant.now().plusSeconds(60);
            intent.setExecuteAt(originalExecuteAt);
            intent.setDeadline(originalDeadline);
            intent.setPrecisionTier(tier);
            intent.setTags(Map.of("original", "true"));

            engine.createIntent(intent, AckMode.DURABLE).get();

            assertTrue(handler.delivered.await(10, TimeUnit.SECONDS),
                "handler must be invoked");
            TestEngines.awaitTerminal(engine, id, Duration.ofSeconds(10));

            Optional<Intent> result = engine.getIntent(id);
            assertTrue(result.isPresent(), "intent must still be in store after ACKED");
            Intent stored = result.get();

            // executeAt must be original (handler tried to set 999s -- if live object leaked, this fails)
            assertEquals(originalExecuteAt, stored.getExecuteAt(),
                "executeAt must be original value (SUCCESS path doesn't change it)");

            // deadline must not be the 999s value
            assertEquals(originalDeadline, stored.getDeadline(),
                "handler mutation must not affect kernel deadline");

            // tags must not be {"mutated": "true"}
            assertTrue(stored.getTags() != null && stored.getTags().containsKey("original"),
                "handler mutation must not affect kernel tags");
            assertTrue(!stored.getTags().containsKey("mutated"),
                "handler's mutated tag must not appear in kernel state");
        }
        System.gc();
        Thread.sleep(200);
    }

}
