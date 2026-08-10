package com.loomq;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.domain.intent.AckMode;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.spi.DeliveryHandler;
import com.loomq.store.ConcurrentIntentStore;
import com.loomq.store.IdempotencyResult;
import com.loomq.store.IntentStore;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * 崩溃注入测试:模拟 createIntent 中途失败(store.save 抛异常),
 * 验证补偿取消写入 + 重启 recovery 不 ghost 投递。
 */
class CrashInjectionTest {

    private static final DeliveryHandler DEAD_LETTER =
        i -> CompletableFuture.completedFuture(DeliveryHandler.DeliveryResult.DEAD_LETTER);

    static final class FailingStore implements IntentStore {
        private final IntentStore delegate = new ConcurrentIntentStore();
        private volatile boolean failOnSave = false;

        @Override public void save(Intent intent) {
            if (failOnSave) throw new RuntimeException("injected save() failure");
            delegate.save(intent);
        }
        @Override public void update(Intent intent) { delegate.update(intent); }
        @Override public Intent findById(String intentId) { return delegate.findById(intentId); }
        @Override public Intent findByIdInternal(String intentId) { return delegate.findByIdInternal(intentId); }
        @Override public void delete(String intentId) { delegate.delete(intentId); }
        @Override public void upsert(Intent intent) { delegate.upsert(intent); }
        @Override public Map<String, Intent> getAllIntents() { return delegate.getAllIntents(); }
        @Override public long countByStatus(IntentStatus s) { return delegate.countByStatus(s); }
        @Override public IdempotencyResult checkIdempotency(String k) { return delegate.checkIdempotency(k); }
        @Override public long getPendingCount() { return delegate.getPendingCount(); }
        @Override public void shutdown() { delegate.shutdown(); }
    }

    @Test
    void createIntentFailureCompensatesAndPreventsGhostDelivery(@TempDir Path tmp) throws Exception {
        FailingStore failingStore = new FailingStore();
        String intentId;

        try (LoomqEngine engine = LoomqEngine.builder()
                .dataDir(tmp).nodeId("crash-1").deliveryHandler(DEAD_LETTER)
                .intentStore(failingStore).build()) {
            engine.start();
            failingStore.failOnSave = true;

            Intent intent = new Intent();
            intentId = intent.getIntentId();
            intent.setExecuteAt(Instant.now().plusSeconds(2));
            intent.setPrecisionTier(PrecisionTier.STANDARD);

            assertThrows(Exception.class,
                () -> engine.createIntent(intent, AckMode.DURABLE).get(),
                "createIntent should propagate the injected failure");

            Optional<Intent> live = engine.getIntent(intentId);
            assertTrue(live.isEmpty(), "failed intent must be absent from in-memory store");
        }

        // Fresh engine on same data dir: compensation cancel wrote CANCELED terminal revision
        try (LoomqEngine engine2 = LoomqEngine.builder()
                .dataDir(tmp).nodeId("crash-2").deliveryHandler(DEAD_LETTER).build()) {
            engine2.start();
            assertFalse(engine2.getIntent(intentId).isPresent(),
                "recovery must not resurrect a compensated-cancelled intent (no ghost delivery)");
        }
    }
}
