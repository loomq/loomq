package com.loomq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.domain.intent.AckMode;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.spi.CallbackHandler;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * I5/G2 固化测试：CallbackHandler 收到的是防御性快照，handler 内调用 setter
 * 不得影响内核活对象状态。
 *
 * <p>覆盖 cancelIntent 的 CANCELLED 回调路径。</p>
 */
@Tag("integration")
class CallbackMutationIsolationTest {

    private static final class MutatingCallbackHandler implements CallbackHandler {
        final CountDownLatch invoked = new CountDownLatch(1);

        @Override
        public void onIntentEvent(Intent intent, EventType type, Throwable error) {
            // Attempt to corrupt kernel state via the received object
            intent.setExecuteAt(Instant.now().plusSeconds(999));
            intent.setTags(Map.of("mutated", "true"));
            invoked.countDown();
        }
    }

    @Test
    void callbackMutationDoesNotAffectKernel(@TempDir Path tmp) throws Exception {
        MutatingCallbackHandler handler = new MutatingCallbackHandler();
        try (LoomqEngine engine = LoomqEngine.builder()
                .dataDir(tmp).nodeId("cb-mut").build()) {
            engine.registerCallbackHandler(handler);
            engine.start();

            Intent intent = new Intent();
            String id = intent.getIntentId();
            Instant originalExecuteAt = Instant.now().plusMillis(500);
            intent.setExecuteAt(originalExecuteAt);
            intent.setPrecisionTier(PrecisionTier.STANDARD);
            intent.setTags(Map.of("original", "true"));

            engine.createIntent(intent, AckMode.DURABLE).get();

            assertTrue(engine.cancelIntent(id), "cancel must succeed");
            assertTrue(handler.invoked.await(10, TimeUnit.SECONDS),
                "callback must be invoked");

            Optional<Intent> result = engine.getIntent(id);
            assertTrue(result.isPresent(), "intent must still be in store after CANCELED");
            Intent stored = result.get();

            assertEquals(originalExecuteAt, stored.getExecuteAt(),
                "executeAt must be original (callback tried to mutate)");
            assertTrue(stored.getTags() != null && stored.getTags().containsKey("original"),
                "tags must contain original key");
            assertTrue(!stored.getTags().containsKey("mutated"),
                "mutated tag must not appear in kernel state");
        }
        System.gc();
        Thread.sleep(200);
    }
}
