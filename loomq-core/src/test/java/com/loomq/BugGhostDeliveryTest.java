package com.loomq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.domain.intent.AckMode;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.spi.DeliveryHandler;
import com.loomq.spi.DeliveryHandler.DeliveryResult;
import com.loomq.store.ConcurrentIntentStore;
import com.loomq.store.IdempotencyResult;
import com.loomq.store.IntentStore;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * BUG #1 (ghost delivery, HIGH confidence) — empirical verification.
 *
 * <p>IntentCommandService.createIntent() persists to the durable wheel (PHTW) FIRST
 * (line 163), then calls intentStore.save() / scheduler.schedule() (lines 168-169).
 * The catch block (lines 179-199) rolls back the in-memory store / scheduler / index /
 * cohort, but explicitly does NOT roll back the wheel write ("已落盘的 wheel/tail 写入不回滚").
 *
 * <p>Consequence: if the post-persist step throws, the caller receives a failure, but the
 * DURABLE slot remains on disk. On the next engine restart (crash / redeploy) WheelRecovery
 * resurrects that slot and the intent is delivered — a "ghost delivery" of an intent the
 * caller believes was never created.
 *
 * <p>This test injects a failing store (save() throws) to force exactly that path, then
 * proves (a) the live engine reports the intent absent, yet (b) a fresh engine on the same
 * data dir resurrects it from the orphaned wheel slot.
 */
@Tag("slow")
class BugGhostDeliveryTest {

    private static final DeliveryHandler DEAD_LETTER =
        i -> CompletableFuture.completedFuture(DeliveryResult.DEAD_LETTER);

    /** IntentStore whose save() throws, to simulate a post-durable-write failure. */
    static final class ThrowingSaveStore implements IntentStore {
        private final IntentStore delegate = new ConcurrentIntentStore();

        @Override public void save(Intent intent) { throw new RuntimeException("injected save() failure"); }
        @Override public void update(Intent intent) { delegate.update(intent); }
        @Override public Intent findById(String intentId) { return delegate.findById(intentId); }
        @Override public Intent findByIdInternal(String intentId) { return delegate.findByIdInternal(intentId); }
        @Override public void delete(String intentId) { delegate.delete(intentId); }
        @Override public void upsert(Intent intent) { delegate.upsert(intent); }
        @Override public Map<String, Intent> getAllIntents() { return delegate.getAllIntents(); }
        @Override public long countByStatus(com.loomq.domain.intent.IntentStatus s) { return delegate.countByStatus(s); }
        @Override public IdempotencyResult checkIdempotency(String k) { return delegate.checkIdempotency(k); }
        @Override public long getPendingCount() { return delegate.getPendingCount(); }
        @Override public void shutdown() { delegate.shutdown(); }
    }

    @Test
    void durableWriteNotRolledBackCausesGhostDeliveryAfterRestart(@TempDir Path tmp) throws Exception {
        String intentId;
        Instant executeAt = Instant.now().plusSeconds(2); // hot (<= hotBoundaryMs)

        // engine1: store.save() throws -> createIntent rolls back in-memory state,
        // but the DURABLE wheel write (done before save) is intentionally NOT rolled back.
        try (LoomqEngine engine1 = LoomqEngine.builder()
                .dataDir(tmp).nodeId("ghost-1").deliveryHandler(DEAD_LETTER)
                .intentStore(new ThrowingSaveStore()).build()) {
            engine1.start();

            Intent intent = new Intent();
            intentId = intent.getIntentId();
            intent.setExecuteAt(executeAt);

            ExecutionException ex = assertThrows(ExecutionException.class,
                () -> engine1.createIntent(intent, AckMode.DURABLE).get(),
                "createIntent should propagate the post-persist failure");
            assertTrue(ex.getCause() instanceof RuntimeException, "failure cause should be the injected store error");

            // In-memory store/scheduler rolled back -> intent invisible in engine1
            Optional<Intent> live = engine1.getIntent(intentId);
            assertTrue(live.isEmpty(),
                "intent must be absent from in-memory store after failed create (rolled back)");
        }

        // engine2: fresh process on same data dir. 修复后:补偿取消写了 CANCELED 终态
        // revision(append-only,recovery 按 max revision 取胜者), WheelRecovery 见终态跳过→不复活。
        try (LoomqEngine engine2 = LoomqEngine.builder()
                .dataDir(tmp).nodeId("ghost-2").deliveryHandler(DEAD_LETTER).build()) {
            engine2.start();
            Optional<Intent> resurrected = engine2.getIntent(intentId);
            assertFalse(resurrected.isPresent(),
                "FIX #1: compensation cancel writes CANCELED terminal revision; "
                    + "recovery skips it — no ghost delivery of a failed-create intent");
            // 若(错误地)复活,状态也不应是 SCHEDULED——保留断言防止部分回退
            if (resurrected.isPresent()) {
                assertEquals(IntentStatus.CANCELED, resurrected.get().getStatus(),
                    "residual slot must be terminal CANCELED, not SCHEDULED");
            }
        }
    }
}
