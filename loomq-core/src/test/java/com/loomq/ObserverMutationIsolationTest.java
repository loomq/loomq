package com.loomq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.domain.intent.AckMode;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.spi.DeliveryHandler;
import com.loomq.spi.DeliveryHandler.DeliveryResult;
import com.loomq.spi.IntentObserver;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * I5/G2 固化测试：IntentObserver 收到的是防御性快照，observer 内调用 setter
 * 不得影响内核活对象状态。
 *
 * <p>覆盖 onScheduled（schedule 路径）和 onDelivered（finalizeIntent 终态路径）。</p>
 */
@Tag("integration")
class ObserverMutationIsolationTest {

    private static final class MutatingObserver implements IntentObserver {
        final CountDownLatch scheduled = new CountDownLatch(1);
        final CountDownLatch delivered = new CountDownLatch(1);
        final AtomicInteger scheduledCount = new AtomicInteger();

        @Override
        public void onScheduled(Intent intent) {
            // Attempt to corrupt kernel state via the received object
            intent.setExecuteAt(Instant.now().plusSeconds(999));
            intent.setTags(Map.of("mutated", "true"));
            scheduledCount.incrementAndGet();
            scheduled.countDown();
        }

        @Override
        public void onDelivered(Intent intent, DeliveryResult result) {
            intent.setExecuteAt(Instant.now().plusSeconds(999));
            intent.setDeadline(Instant.now().plusSeconds(999));
            intent.setTags(Map.of("mutated", "true"));
            delivered.countDown();
        }

        @Override
        public void onDeadLettered(Intent intent) {}

        @Override
        public void onExpired(Intent intent) {}

        @Override
        public void onDeliveryFailed(Intent intent, Throwable error) {}
    }

    @Test
    void observerMutationDoesNotAffectKernel(@TempDir Path tmp) throws Exception {
        MutatingObserver observer = new MutatingObserver();
        DeliveryHandler handler = intent ->
            CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        try (LoomqEngine engine = LoomqEngine.builder()
                .dataDir(tmp).nodeId("obs-mut").deliveryHandler(handler).build()) {
            engine.registerObserver(observer);
            engine.start();

            Intent intent = new Intent();
            String id = intent.getIntentId();
            Instant originalExecuteAt = Instant.now().plusMillis(200);
            Instant originalDeadline = Instant.now().plusSeconds(60);
            intent.setExecuteAt(originalExecuteAt);
            intent.setDeadline(originalDeadline);
            intent.setPrecisionTier(PrecisionTier.STANDARD);
            intent.setTags(Map.of("original", "true"));

            engine.createIntent(intent, AckMode.DURABLE).get();

            assertTrue(observer.scheduled.await(10, TimeUnit.SECONDS),
                "onScheduled must be invoked");
            assertTrue(observer.delivered.await(10, TimeUnit.SECONDS),
                "onDelivered must be invoked");
            awaitTerminal(engine, id, Duration.ofSeconds(10));

            Optional<Intent> result = engine.getIntent(id);
            assertTrue(result.isPresent(), "intent must still be in store after ACKED");
            Intent stored = result.get();

            assertEquals(originalExecuteAt, stored.getExecuteAt(),
                "executeAt must be original (observer tried to mutate via onScheduled/onDelivered)");
            assertEquals(originalDeadline, stored.getDeadline(),
                "deadline must be original (observer mutation must not affect kernel)");
            assertTrue(stored.getTags() != null && stored.getTags().containsKey("original"),
                "tags must contain original key");
            assertTrue(!stored.getTags().containsKey("mutated"),
                "mutated tag must not appear in kernel state");
        }
        System.gc();
        Thread.sleep(200);
    }

    private void awaitTerminal(LoomqEngine engine, String id, Duration timeout)
            throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            Optional<Intent> cur = engine.getIntent(id);
            if (cur.isPresent() && cur.get().getStatus().isTerminal()) {
                return;
            }
            Thread.sleep(20);
        }
        throw new AssertionError("intent " + id + " did not reach terminal within " + timeout);
    }
}
