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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * I5/G1 固化测试：投递内容确定化。
 *
 * <p>沿用 {@link ClaimedInFlightRaceTest} 的 GateHandler 模式：在途期间 update 内容，
 * 断言首轮投递为更新前快照、RETRY 重投为更新后内容。</p>
 *
 * <p>语义可陈述为："首轮投递携带派发时刻的内容，重试携带最新内容"。</p>
 *
 * <p><b>确定性设计：</b>首轮 handler 在读取 tags 前等待 updateIntent 完成（updateCompleted
 * latch）。若实现正确（传快照），快照在 update 之前已取，tags 为 v1；若实现错误（传活对象），
 * update 已改写活对象 tags，handler 读到 v2。二选一，无撕裂。</p>
 */
@Tag("integration")
class UpdateContentDeterminismTest {

    /**
     * Handler that records tags from each received snapshot.
     *
     * First delivery: waits for updateCompleted latch before reading tags,
     * then returns RETRY (via uncompleted future that the test completes).
     * Second delivery: reads tags immediately, returns SUCCESS.
     */
    private static final class VersionTrackingHandler implements DeliveryHandler {
        final CountDownLatch firstInvoked = new CountDownLatch(1);
        final CountDownLatch updateCompleted = new CountDownLatch(1);
        final CompletableFuture<DeliveryResult> firstOutcome = new CompletableFuture<>();
        final List<Map<String, String>> receivedTags = new CopyOnWriteArrayList<>();
        final AtomicInteger calls = new AtomicInteger();

        @Override
        public CompletableFuture<DeliveryResult> deliverAsync(Intent intent) {
            int n = calls.incrementAndGet();
            if (n == 1) {
                // Signal that first delivery has started (snapshot already taken)
                firstInvoked.countDown();
                // Wait for the test thread to complete updateIntent before reading tags.
                // With snapshot: tags are v1 (snapshot taken before update).
                // With live object: tags are v2 (update mutated the live object).
                // RuntimeException (not AssertionError) so the batch consumer's catch block
                // catches it and routes to handleDeliveryException -> terminal state.
                try {
                    if (!updateCompleted.await(10, TimeUnit.SECONDS)) {
                        throw new RuntimeException("updateCompleted latch not counted down within 10s");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
                Map<String, String> tags = intent.getTags();
                receivedTags.add(tags != null ? Map.copyOf(tags) : Map.of());
                return firstOutcome;  // stay in-flight until test completes it
            }
            // Second delivery (retry): read tags immediately
            Map<String, String> tags = intent.getTags();
            receivedTags.add(tags != null ? Map.copyOf(tags) : Map.of());
            return CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        }
    }

    @Test
    void firstDeliveryCarriesPreUpdateSnapshot_retryCarriesUpdatedContent(
            @TempDir Path tmp) throws Exception {
        VersionTrackingHandler handler = new VersionTrackingHandler();
        try (LoomqEngine engine = LoomqEngine.builder()
                .dataDir(tmp).nodeId("det-1").deliveryHandler(handler).build()) {
            engine.start();

            Intent intent = new Intent();
            String id = intent.getIntentId();
            intent.setExecuteAt(Instant.now().plusMillis(100));
            intent.setPrecisionTier(PrecisionTier.STANDARD);
            intent.setTags(Map.of("version", "v1"));

            engine.createIntent(intent, AckMode.DURABLE).get();

            // Wait for first delivery to start (scanDue has claimed, snapshot taken)
            assertTrue(handler.firstInvoked.await(10, TimeUnit.SECONDS),
                "first delivery must be invoked");

            // While first delivery is in-flight (handler waiting on updateCompleted),
            // update content on the live object
            engine.updateIntent(id,
                i -> i.setTags(Map.of("version", "v2")),
                null);

            // Release the handler to read tags and complete
            handler.updateCompleted.countDown();

            // Complete first delivery as RETRY -> triggers reschedule
            handler.firstOutcome.complete(DeliveryHandler.DeliveryResult.RETRY);

            // Wait for second delivery (redelivery) + terminal state
            awaitCalls(handler, 2, Duration.ofSeconds(15));
            TestEngines.awaitTerminal(engine, id, Duration.ofSeconds(10));

            // Assertions
            assertEquals(2, handler.calls.get(),
                "exactly 2 deliveries: first (pre-update) + retry (post-update)");

            // First delivery must carry pre-update snapshot (v1)
            assertEquals("v1", handler.receivedTags.get(0).get("version"),
                "first delivery must carry pre-update snapshot (v1)");

            // Retry must carry post-update content (v2)
            assertEquals("v2", handler.receivedTags.get(1).get("version"),
                "retry must carry updated content (v2)");
        }
        System.gc();
        Thread.sleep(200);
    }

    private void awaitCalls(VersionTrackingHandler handler, int expected, Duration timeout)
            throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (handler.calls.get() < expected && System.nanoTime() < deadline) {
            Thread.sleep(50);
        }
        assertEquals(expected, handler.calls.get(),
            "expected " + expected + " deliveries within " + timeout);
    }

}
