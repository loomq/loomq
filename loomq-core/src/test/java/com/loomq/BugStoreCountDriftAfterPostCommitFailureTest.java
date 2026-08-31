package com.loomq;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.loomq.domain.intent.AckMode;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.spi.DeliveryHandler.DeliveryResult;
import com.loomq.testutil.TestStores;
import java.nio.file.Path;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * R22 提交后保护让 updateIntent 在 store.update 失败时仍返回成功，
 * 但 ConcurrentIntentStore 的 statusCounts 没有同步，导致计数漂移。
 */
class BugStoreCountDriftAfterPostCommitFailureTest {

    @TempDir Path tmp;

    @Test
    void statusCountsMustFollowCommittedUpdateEvenWhenStoreUpdateThrows() throws Exception {
        try (LoomqEngine engine = LoomqEngine.builder()
                .dataDir(tmp.resolve("c")).nodeId("c1")
                .intentStore(new TestStores.UpdateThrowingStore())
                .deliveryHandler(i -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS))
                .build()) {
            engine.start();
            Intent it = new Intent("count-drift-001");
            it.setExecuteAt(Instant.ofEpochMilli(System.currentTimeMillis() + 60_000));
            it.setPrecisionTier(PrecisionTier.ULTRA);
            engine.createIntent(it, AckMode.DURABLE).get();

            engine.updateIntent("count-drift-001", i -> i.transitionTo(IntentStatus.DUE), null);

            assertEquals(IntentStatus.DUE, engine.getIntent("count-drift-001").orElseThrow().getStatus());
            assertEquals(0, engine.getIntentStore().countByStatus(IntentStatus.SCHEDULED),
                "SCHEDULED count should drop after committed update to DUE");
            assertEquals(1, engine.getIntentStore().countByStatus(IntentStatus.DUE),
                "DUE count should reflect the committed update even if store.update threw");
        }
    }
}
