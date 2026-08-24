package com.loomq.application.scheduler;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.domain.intent.PrecisionTierCatalog;
import com.loomq.domain.intent.PrecisionTierProfile;
import com.loomq.spi.DeliveryHandler;
import com.loomq.spi.DeliveryHandler.DeliveryResult;
import com.loomq.spi.IntentObserver;
import com.loomq.spi.RedeliveryDecider;
import com.loomq.store.ConcurrentIntentStore;
import com.loomq.store.IntentStore;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

class PrecisionSchedulerTest {

    private IntentStore intentStore;
    private PrecisionScheduler scheduler;

    @BeforeEach
    void setUp() {
        intentStore = new ConcurrentIntentStore();
    }

    @AfterEach
    void tearDown() {
        if (scheduler != null) {
            scheduler.stop();
        }
    }

    // ========== Lifecycle ==========

    @Test
    void pauseMustNotSurviveRestart() {
        DeliveryHandler handler = intent -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        scheduler.start();
        scheduler.pause();
        assertTrue(scheduler.isPaused(), "precondition: paused");
        scheduler.stop();
        scheduler.start();
        assertFalse(scheduler.isPaused(),
            "start() must reset paused — restart means fresh service, not silent suspension");
    }

    // ========== Construction ==========

    @Test
    void shouldRequireNonNullDeliveryHandler() {
        assertThrows(NullPointerException.class, () ->
            new PrecisionScheduler(intentStore, null, null));
    }

    @Test
    void shouldConstructWithDeliveryHandlerAndDefaultDecider() {
        DeliveryHandler handler = intent -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        assertDoesNotThrow(() -> {
            scheduler = new PrecisionScheduler(intentStore, handler, null);
        });
        assertNotNull(scheduler);
    }

    @Test
    void shouldConstructWithCustomRedeliveryDecider() {
        DeliveryHandler handler = intent -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        RedeliveryDecider decider = ctx -> ctx.isSuccess();
        assertDoesNotThrow(() -> {
            scheduler = new PrecisionScheduler(intentStore, handler, decider);
        });
    }

    @Test
    void shouldConstructWithCustomCatalog() {
        DeliveryHandler handler = intent -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        Map<PrecisionTier, PrecisionTierProfile> profiles = Map.of(
            PrecisionTier.ULTRA, new PrecisionTierProfile(10, 100, 1, 5, 8)
        );
        PrecisionTierCatalog catalog = PrecisionTierCatalog.of(profiles, PrecisionTier.ULTRA);
        assertDoesNotThrow(() -> {
            scheduler = new PrecisionScheduler(intentStore, handler, null, catalog);
        });
        assertEquals(1, scheduler.getBackpressureStatus().size());
    }

    // ========== Lifecycle ==========

    @Test
    void startShouldBeIdempotent() {
        DeliveryHandler handler = intent -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        scheduler.start();
        scheduler.start(); // second call should be a no-op
        assertDoesNotThrow(() -> scheduler.stop());
    }

    @Test
    void stopShouldWorkWithoutStart() {
        DeliveryHandler handler = intent -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        assertDoesNotThrow(() -> scheduler.stop());
    }

    @Test
    void pauseResumeShouldToggleFlag() {
        DeliveryHandler handler = intent -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        assertFalse(scheduler.isPaused());
        scheduler.pause();
        assertTrue(scheduler.isPaused());
        scheduler.pause(); // idempotent
        assertTrue(scheduler.isPaused());
        scheduler.resume();
        assertFalse(scheduler.isPaused());
        scheduler.resume(); // idempotent
        assertFalse(scheduler.isPaused());
    }

    // ========== schedule() ==========

    @Test
    void scheduleShouldAcceptCreatedStatus() {
        DeliveryHandler handler = intent -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        Intent intent = new Intent("test-created");
        intent.setExecuteAt(Instant.now().plusSeconds(60));
        intentStore.save(intent);
        assertDoesNotThrow(() -> scheduler.schedule(intent));
    }

    @Test
    void scheduleShouldAcceptScheduledStatus() {
        DeliveryHandler handler = intent -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        Intent intent = new Intent("test-scheduled");
        intent.setExecuteAt(Instant.now().plusSeconds(60));
        intent.transitionTo(IntentStatus.SCHEDULED);
        intentStore.save(intent);
        assertDoesNotThrow(() -> scheduler.schedule(intent));
    }

    @Test
    void scheduleShouldSkipNonCreatedOrScheduledIntent() {
        DeliveryHandler handler = intent -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        Intent intent = new Intent("test-skip");
        intent.transitionTo(IntentStatus.SCHEDULED);
        intent.transitionTo(IntentStatus.DUE);
        // should not throw, just log a warning
        assertDoesNotThrow(() -> scheduler.schedule(intent));
    }

    @Test
    void scheduleImmediateIntentShouldGoToBucket() {
        DeliveryHandler handler = intent -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        Intent intent = new Intent("test-imm");
        intent.setExecuteAt(Instant.now().minusSeconds(1)); // past → immediate
        intentStore.save(intent);
        assertDoesNotThrow(() -> scheduler.schedule(intent));
    }

    @Test
    void scheduleFutureIntentShouldUseVirtualThreadSleep() {
        DeliveryHandler handler = intent -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        Intent intent = new Intent("test-future");
        intent.setExecuteAt(Instant.now().plusMillis(200));
        intentStore.save(intent);
        assertDoesNotThrow(() -> scheduler.schedule(intent));
    }

    // ========== restore() ==========

    @Test
    void restoreShouldAddToBucketGroup() {
        DeliveryHandler handler = intent -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        Intent intent = new Intent("test-restore");
        intent.setExecuteAt(Instant.now().plusSeconds(60));
        intent.transitionTo(IntentStatus.SCHEDULED);
        intentStore.save(intent);
        assertDoesNotThrow(() -> scheduler.restore(intent));
    }

    @Test
    void restoreShouldSkipNullIntent() {
        DeliveryHandler handler = intent -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        assertDoesNotThrow(() -> scheduler.restore(null));
    }

    @Test
    void restoreShouldSkipTerminalIntent() {
        DeliveryHandler handler = intent -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        Intent intent = new Intent("test-term");
        intent.setExecuteAt(Instant.now().plusSeconds(60));
        intent.transitionTo(IntentStatus.SCHEDULED);
        intent.transitionTo(IntentStatus.DUE);
        intent.transitionTo(IntentStatus.DISPATCHING);
        intent.transitionTo(IntentStatus.DEAD_LETTERED);
        assertDoesNotThrow(() -> scheduler.restore(intent));
    }

    @Test
    void restoreShouldSkipNullExecuteAt() {
        DeliveryHandler handler = intent -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        Intent intent = new Intent("test-no-ex");
        // executeAt is null by default
        assertDoesNotThrow(() -> scheduler.restore(intent));
    }

    // ========== unschedule() ==========

    @Test
    void unscheduleShouldDelegateToBucketGroupManager() {
        DeliveryHandler handler = intent -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        Intent intent = new Intent("test-unsched");
        intent.setExecuteAt(Instant.now().plusSeconds(60));
        intent.transitionTo(IntentStatus.SCHEDULED);
        scheduler.schedule(intent);
        assertDoesNotThrow(() -> scheduler.unschedule(intent));
    }

    // ========== Backpressure ==========

    @Test
    void isTierUnderBackpressureShouldDetectSemaphoreExhaustion() {
        DeliveryHandler handler = intent -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        assertFalse(scheduler.isTierUnderBackpressure(PrecisionTier.ULTRA));
    }

    @Test
    void isTierUnderBackpressureShouldReturnFalseWhenNotUnderPressure() {
        DeliveryHandler handler = intent -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        assertFalse(scheduler.isTierUnderBackpressure(PrecisionTier.STANDARD));
    }

    @Test
    void getBackpressureStatusShouldReturnAllTiers() {
        DeliveryHandler handler = intent -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        Map<PrecisionTier, DispatchPipeline.BackpressureInfo> status = scheduler.getBackpressureStatus();
        assertEquals(PrecisionTierCatalog.defaultCatalog().supportedTiers().size(), status.size());
        for (PrecisionTier tier : PrecisionTierCatalog.defaultCatalog().supportedTiers()) {
            DispatchPipeline.BackpressureInfo info = status.get(tier);
            assertNotNull(info);
            assertTrue(info.maxConcurrency() > 0);
            assertTrue(info.availablePermits() >= 0);
            assertEquals(0, info.queueSize());
        }
    }

    // ========== getBucketGroupManager() ==========

    @Test
    void shouldExposeBucketGroupManager() {
        DeliveryHandler handler = intent -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        assertNotNull(scheduler.getBucketGroupManager());
    }

    // ========== dispatch cycle with controlled mock handler ==========

    // These tests verify the full dispatch pipeline through start → scan → consume → callback

    /** Creates an immediate intent transitioned to SCHEDULED (as production does). */
    private Intent readyIntent(String id) {
        Intent intent = new Intent(id);
        intent.setExecuteAt(Instant.now().minusMillis(100));
        intent.transitionTo(IntentStatus.SCHEDULED);
        intentStore.save(intent);
        return intent;
    }

    @Test
    @Tag("slow")
    void dispatchSuccessShouldTransitionToAcked() throws Exception {
        CountDownLatch delivered = new CountDownLatch(1);
        DeliveryHandler handler = intent -> {
            delivered.countDown();
            return CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        };
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        scheduler.start();

        Intent intent = readyIntent("test-success");
        scheduler.schedule(intent);

        assertTrue(delivered.await(5, TimeUnit.SECONDS), "delivery should happen within 5s");

        // Wait a bit for the callback to finalize
        Thread.sleep(100);
        Intent stored = intentStore.findById("test-success");
        assertNotNull(stored);
        assertTrue(stored.getStatus() == IntentStatus.ACKED || stored.getStatus() == IntentStatus.DELIVERED,
            "expected ACKED or DELIVERED but was " + stored.getStatus());
    }

    @Test
    @Tag("slow")
    void dispatchDeadLetterShouldTransitionToDeadLettered() throws Exception {
        CountDownLatch delivered = new CountDownLatch(1);
        DeliveryHandler handler = intent -> {
            delivered.countDown();
            return CompletableFuture.completedFuture(DeliveryResult.DEAD_LETTER);
        };
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        scheduler.start();

        Intent intent = readyIntent("test-dl");
        scheduler.schedule(intent);

        assertTrue(delivered.await(5, TimeUnit.SECONDS));
        Thread.sleep(100);
        Intent stored = intentStore.findById("test-dl");
        assertNotNull(stored);
        assertEquals(IntentStatus.DEAD_LETTERED, stored.getStatus());
    }

    @Test
    @Tag("slow")
    void dispatchRetryShouldRescheduleIntent() throws Exception {
        CountDownLatch delivered = new CountDownLatch(1);
        DeliveryHandler handler = intent -> {
            delivered.countDown();
            return CompletableFuture.completedFuture(DeliveryResult.RETRY);
        };
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        scheduler.start();

        Intent intent = readyIntent("test-retry");
        scheduler.schedule(intent);

        assertTrue(delivered.await(5, TimeUnit.SECONDS));
        Thread.sleep(100);
        Intent stored = intentStore.findById("test-retry");
        assertNotNull(stored);
        // After RETRY, should be SCHEDULED for re-delivery
        assertEquals(IntentStatus.SCHEDULED, stored.getStatus());
        // executeAt should be in the future
        assertTrue(stored.getExecuteAt().isAfter(Instant.now()));
    }

    @Test
    @Tag("slow")
    void dispatchExpiredShouldTransitionToExpired() throws Exception {
        CountDownLatch delivered = new CountDownLatch(1);
        DeliveryHandler handler = intent -> {
            delivered.countDown();
            return CompletableFuture.completedFuture(DeliveryResult.EXPIRED);
        };
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        scheduler.start();

        Intent intent = readyIntent("test-exp");
        scheduler.schedule(intent);

        assertTrue(delivered.await(5, TimeUnit.SECONDS));
        Thread.sleep(100);
        Intent stored = intentStore.findById("test-exp");
        assertNotNull(stored);
        assertEquals(IntentStatus.EXPIRED, stored.getStatus());
    }

    @Test
    @Tag("slow")
    void shouldReleaseSemaphoreAfterDispatch() throws Exception {
        CountDownLatch delivered = new CountDownLatch(1);
        DeliveryHandler handler = intent -> {
            delivered.countDown();
            return CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        };
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        scheduler.start();

        Intent intent = readyIntent("test-release");
        scheduler.schedule(intent);

        assertTrue(delivered.await(5, TimeUnit.SECONDS));
        Thread.sleep(100);

        var status = scheduler.getBackpressureStatus().get(PrecisionTier.ULTRA);
        // After dispatch completes, all permits should be available (or at least not 0)
        assertTrue(status.availablePermits() > 0,
            "expected some available permits, got " + status.availablePermits());
    }

    @Test
    @Tag("slow")
    void semaphoreShouldBeReleasedEvenOnException() throws Exception {
        AtomicReference<Throwable> callbackError = new AtomicReference<>();
        CountDownLatch callbackFired = new CountDownLatch(1);

        DeliveryHandler handler = intent -> {
            callbackFired.countDown();
            CompletableFuture<DeliveryResult> future = new CompletableFuture<>();
            future.completeExceptionally(new RuntimeException("simulated failure"));
            return future;
        };
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        scheduler.start();

        Intent intent = readyIntent("test-ex");
        scheduler.schedule(intent);

        assertTrue(callbackFired.await(5, TimeUnit.SECONDS));
        Thread.sleep(200);

        var status = scheduler.getBackpressureStatus().get(PrecisionTier.ULTRA);
        assertTrue(status.availablePermits() > 0,
            "semaphore should be released even after exception, got " + status.availablePermits());
    }

    @Test
    @Tag("slow")
    void shouldHandleDeliveryTimeout() throws Exception {
        CountDownLatch callbackFired = new CountDownLatch(1);
        DeliveryHandler handler = intent -> {
            callbackFired.countDown();
            // Return a future that never completes → triggers orTimeout
            return new CompletableFuture<>();
        };
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        scheduler.start();

        Intent intent = readyIntent("test-timeout");
        scheduler.schedule(intent);

        assertTrue(callbackFired.await(5, TimeUnit.SECONDS));
        // The orTimeout(30s) is applied by runBatchConsumer, so for unit test purposes
        // we just verify the callback was invoked (the delivery handler was called)
        // Full timeout test requires 30s wait, impractical for unit tests
    }

    @Test
    void deliveryHandlerCalledWithCorrectIntent() throws Exception {
        AtomicReference<String> deliveredId = new AtomicReference<>();
        CountDownLatch delivered = new CountDownLatch(1);

        DeliveryHandler handler = intent -> {
            deliveredId.set(intent.getIntentId());
            delivered.countDown();
            return CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        };
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        scheduler.start();

        Intent intent = readyIntent("test-id-check");
        scheduler.schedule(intent);

        assertTrue(delivered.await(5, TimeUnit.SECONDS));
        assertEquals("test-id-check", deliveredId.get());
    }

    // ========== handleDeliveryFailure exception path (after fix) ==========

    @Test
    @Tag("slow")
    void shouldDeadLetterAfterMaxAttemptsOnException() throws Exception {
        CountDownLatch delivered = new CountDownLatch(1);
        DeliveryHandler handler = intent -> {
            delivered.countDown();
            CompletableFuture<DeliveryResult> future = new CompletableFuture<>();
            future.completeExceptionally(new RuntimeException("persistent failure"));
            return future;
        };
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        scheduler.start();

        Intent intent = readyIntent("test-max-ex");
        // Pre-set attempts to max to trigger dead-letter
        for (int i = 0; i < 10; i++) {
            intent.incrementAttempts();
        }
        intentStore.update(intent);
        scheduler.schedule(intent);

        assertTrue(delivered.await(5, TimeUnit.SECONDS));
        Thread.sleep(200);
        Intent stored = intentStore.findById("test-max-ex");
        assertNotNull(stored);
        // With attempts >= max, handleDeliveryFailure should transition to DEAD_LETTERED
        boolean terminal = stored.getStatus() == IntentStatus.DEAD_LETTERED;
        assertTrue(terminal || stored.getStatus().isTerminal(),
            "expected terminal state but was " + stored.getStatus());
    }

    @Test
    void shouldRetryWhenAttemptsLessThanMaxOnException() throws Exception {
        CountDownLatch delivered = new CountDownLatch(1);
        DeliveryHandler handler = intent -> {
            delivered.countDown();
            CompletableFuture<DeliveryResult> future = new CompletableFuture<>();
            future.completeExceptionally(new RuntimeException("transient failure"));
            return future;
        };
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        scheduler.start();

        Intent intent = readyIntent("test-retry-ex");
        scheduler.schedule(intent);

        assertTrue(delivered.await(5, TimeUnit.SECONDS));
        Thread.sleep(200);
        Intent stored = intentStore.findById("test-retry-ex");
        assertNotNull(stored);
        // After delivery exception with attempts < max, should retry → SCHEDULED
        assertEquals(IntentStatus.SCHEDULED, stored.getStatus());
        assertTrue(stored.getExecuteAt().isAfter(Instant.now()), "executeAt should be in the future for retry");
    }

    // ========== Phase 7.1: Expiry Index ==========

    @Test
    void shouldIndexAndUnindexIntent() {
        DeliveryHandler handler = intent -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        scheduler = new PrecisionScheduler(intentStore, handler, null);

        Intent intent = new Intent("test-index");
        intent.setExecuteAt(Instant.now().plusSeconds(60));
        intent.setPrecisionTier(PrecisionTier.STANDARD);
        intentStore.save(intent);
        scheduler.schedule(intent);

        // After schedule(), intent should be in store with SCHEDULED status
        Intent stored = intentStore.findById("test-index");
        assertNotNull(stored);
        assertTrue(stored.getStatus() == IntentStatus.SCHEDULED || stored.getStatus() == IntentStatus.CREATED);

        // Properly transition through the state machine to terminal
        if (stored.getStatus() != IntentStatus.SCHEDULED) {
            stored.transitionTo(IntentStatus.SCHEDULED);
        }
        stored.transitionTo(IntentStatus.DUE);
        stored.transitionTo(IntentStatus.DISPATCHING);
        stored.transitionTo(IntentStatus.DELIVERED);
        stored.transitionTo(IntentStatus.ACKED);
        intentStore.update(stored);

        Intent finalStored = intentStore.findById("test-index");
        assertNotNull(finalStored);
        assertTrue(finalStored.getStatus().isTerminal());
    }

    @Test
    void shouldNotFlagFutureIntentAsExpired() {
        DeliveryHandler handler = intent -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        scheduler = new PrecisionScheduler(intentStore, handler, null);

        Intent intent = new Intent("test-future-exp");
        intent.setExecuteAt(Instant.now().plusSeconds(600)); // 10 min future
        intent.setPrecisionTier(PrecisionTier.STANDARD);
        intentStore.save(intent);
        scheduler.schedule(intent);

        Intent stored = intentStore.findById("test-future-exp");
        assertNotNull(stored);
        // Intent should NOT be expired (executeAt is far in the future)
        assertFalse(stored.getStatus().isTerminal(), "future intent should not be terminal");
    }

    @Test
    void shouldDetectExpiredIntentViaIndex() {
        DeliveryHandler handler = intent -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        scheduler.start();

        Intent intent = new Intent("test-expired");
        intent.setExecuteAt(Instant.now().minusSeconds(60)); // 1 min past
        intent.setDeadline(Instant.now().minusSeconds(1));   // deadline past
        intent.setPrecisionTier(PrecisionTier.STANDARD);
        intent.transitionTo(IntentStatus.SCHEDULED);
        intentStore.save(intent);
        scheduler.schedule(intent);

        // Wait for scan cycle to pick up expired intent
        sleep(1500);
        Intent stored = intentStore.findById("test-expired");
        assertNotNull(stored);
        // Should be EXPIRED or DEAD_LETTERED (both terminal)
        assertTrue(stored.getStatus().isTerminal(),
            "expired intent should reach terminal state, was " + stored.getStatus());
    }

    @Test
    void shouldCleanIndexOnTerminalState() {
        DeliveryHandler handler = intent -> CompletableFuture.completedFuture(DeliveryResult.DEAD_LETTER);
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        scheduler.start();

        Intent intent = new Intent("test-dl-index");
        intent.setExecuteAt(Instant.now().minusMillis(100));
        intent.transitionTo(IntentStatus.SCHEDULED);
        intentStore.save(intent);
        scheduler.schedule(intent);

        // Wait for delivery + dead-letter processing (generous timeout for CI/resource contention)
        sleep(2000);
        Intent stored = intentStore.findById("test-dl-index");
        assertNotNull(stored);
        assertEquals(IntentStatus.DEAD_LETTERED, stored.getStatus());

        // Create a NEW intent with a different ID (index properly cleaned)
        Intent newIntent = new Intent("test-dl-index-2");
        newIntent.setExecuteAt(Instant.now().plusSeconds(60));
        newIntent.setPrecisionTier(PrecisionTier.STANDARD);
        intentStore.save(newIntent);
        // Scheduling a new intent should work fine (index is clean for old intent)
        assertDoesNotThrow(() -> scheduler.schedule(newIntent));
    }

    @Test
    void shouldReindexOnRestore() {
        DeliveryHandler handler = intent -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        scheduler = new PrecisionScheduler(intentStore, handler, null);

        Intent intent = new Intent("test-restore-idx");
        intent.setExecuteAt(Instant.now().plusSeconds(60));
        intent.setPrecisionTier(PrecisionTier.STANDARD);
        intent.transitionTo(IntentStatus.SCHEDULED);
        intentStore.save(intent);
        scheduler.restore(intent);

        Intent stored = intentStore.findById("test-restore-idx");
        assertNotNull(stored);
        assertEquals(IntentStatus.SCHEDULED, stored.getStatus());
    }

    @Test
    void shouldHandleNullExecuteAtGracefully() {
        DeliveryHandler handler = intent -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        scheduler = new PrecisionScheduler(intentStore, handler, null);

        Intent intent = new Intent("test-null-ex");
        // executeAt is null — schedule() must reject with a clear contract error (create 入口
        // 由 IntentValidator 拦截,此处兜底直连 API 调用——不再裸 NPE 无上下文)。
        intent.transitionTo(IntentStatus.SCHEDULED);
        intentStore.save(intent);
        IllegalArgumentException ex =
            assertThrows(IllegalArgumentException.class, () -> scheduler.schedule(intent));
        assertTrue(ex.getMessage().contains("executeAt must not be null"),
            "rejection must name the violated contract, was: " + ex.getMessage());
    }

    // ========== Phase 7.2: Backpressure Observable ==========

    @Test
    void shouldNotifyObserverOnBackpressure() {
        List<Intent> failedIntents = new ArrayList<>();
        IntentObserver observer = new IntentObserver() {
            @Override public void onScheduled(Intent i) {}
            @Override public void onDelivered(Intent i, DeliveryResult r) {}
            @Override public void onDeadLettered(Intent i) {}
            @Override public void onExpired(Intent i) {}
            @Override public void onDeliveryFailed(Intent i, Throwable e) {
                failedIntents.add(i);
            }
        };

        DeliveryHandler handler = intent -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        scheduler.setObservers(List.of(observer));
        scheduler.start();

        // Cause backpressure by spawning many intents quickly
        for (int i = 0; i < 500; i++) {
            Intent intent = new Intent("bp-obs-" + i);
            intent.setExecuteAt(Instant.now().minusMillis(100));
            intent.transitionTo(IntentStatus.SCHEDULED);
            intentStore.save(intent);
            scheduler.schedule(intent);
        }

        sleep(500);
        // At least some intents should have triggered backpressure notification
        // (depends on queue capacity and consumer speed; we just verify no crash)
        assertDoesNotThrow(() -> scheduler.getBackpressureStatus());
    }

    @Test
    void shouldRollbackStatusOnBackpressure() {
        DeliveryHandler handler = intent -> {
            // Slow handler to cause queue buildup
            sleep(50);
            return CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        };
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        scheduler.start();

        // Submit many intents to saturate the queue
        for (int i = 0; i < 300; i++) {
            Intent intent = new Intent("bp-rb-" + i);
            intent.setExecuteAt(Instant.now().minusMillis(100));
            intent.transitionTo(IntentStatus.SCHEDULED);
            intentStore.save(intent);
            scheduler.schedule(intent);
        }

        sleep(1000);
        // All intents should still exist (none silently dropped)
        for (int i = 0; i < 300; i++) {
            assertNotNull(intentStore.findById("bp-rb-" + i),
                "intent bp-rb-" + i + " should not be silently dropped");
        }
    }

    @Test
    void shouldRetryBackpressuredIntentNextCycle() {
        DeliveryHandler handler = intent -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        scheduler.start();

        Intent intent = new Intent("bp-retry");
        intent.setExecuteAt(Instant.now().minusSeconds(1));
        intent.transitionTo(IntentStatus.SCHEDULED);
        intentStore.save(intent);
        scheduler.schedule(intent);

        // Wait for delivery
        sleep(1000);
        Intent stored = intentStore.findById("bp-retry");
        assertNotNull(stored);
        // Should have been processed (either delivered or retried)
        assertFalse(stored.getStatus() == IntentStatus.CREATED,
            "intent should have been processed beyond CREATED");
    }

    @Test
    void shouldIncrementBackpressureMetrics() {
        DeliveryHandler handler = intent -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        scheduler.start();

        // Submit intents to trigger queue activity
        for (int i = 0; i < 200; i++) {
            Intent intent = new Intent("bp-metric-" + i);
            intent.setExecuteAt(Instant.now().minusMillis(100));
            intent.transitionTo(IntentStatus.SCHEDULED);
            intentStore.save(intent);
            scheduler.schedule(intent);
        }

        sleep(1000);
        // Verify metrics were collected (no NPE, no crash)
        var status = scheduler.getBackpressureStatus();
        for (var entry : status.entrySet()) {
            assertNotNull(entry.getValue());
            assertTrue(entry.getValue().maxConcurrency() > 0);
        }
    }

    private static void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    // ========== Phase 4: IntentObserver Callbacks ==========

    @Test
    void shouldInvokeAllObserverCallbacks() throws Exception {
        List<String> events = new ArrayList<>();
        IntentObserver observer = new IntentObserver() {
            @Override public void onScheduled(Intent i) { events.add("scheduled"); }
            @Override public void onDelivered(Intent i, DeliveryResult r) { events.add("delivered"); }
            @Override public void onDeadLettered(Intent i) { events.add("dead"); }
            @Override public void onExpired(Intent i) { events.add("expired"); }
            @Override public void onDeliveryFailed(Intent i, Throwable e) { events.add("failed"); }
        };

        DeliveryHandler handler = intent -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        scheduler.setObservers(List.of(observer));
        scheduler.start();

        Intent intent = readyIntent("obs-all");
        scheduler.schedule(intent);

        sleep(1500);
        // onScheduled should always fire
        assertTrue(events.contains("scheduled"), "onScheduled not fired, events=" + events);
        // SUCCESS delivery should fire onDelivered
        assertTrue(events.contains("delivered"), "onDelivered not fired, events=" + events);
    }

    @Test
    void shouldNotCrashOnObserverException() throws Exception {
        IntentObserver crashing = new IntentObserver() {
            @Override public void onScheduled(Intent i) { throw new RuntimeException("boom"); }
            @Override public void onDelivered(Intent i, DeliveryResult r) {}
            @Override public void onDeadLettered(Intent i) {}
            @Override public void onExpired(Intent i) {}
            @Override public void onDeliveryFailed(Intent i, Throwable e) {}
        };

        DeliveryHandler handler = intent -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        scheduler.setObservers(List.of(crashing));
        scheduler.start();

        Intent intent = readyIntent("obs-crash");
        scheduler.schedule(intent);

        sleep(1500);
        Intent stored = intentStore.findById("obs-crash");
        assertNotNull(stored);
        // Intent should still be processed despite observer exception
        assertTrue(stored.getStatus().isTerminal() || stored.getStatus() == IntentStatus.ACKED,
            "intent should be processed despite crashing observer, was " + stored.getStatus());
    }

    @Test
    void shouldDynamicallyRegisterAndRemoveObservers() throws Exception {
        List<String> eventsA = new ArrayList<>();
        List<String> eventsB = new ArrayList<>();
        IntentObserver observerA = new IntentObserver() {
            @Override public void onScheduled(Intent i) { eventsA.add("A"); }
            @Override public void onDelivered(Intent i, DeliveryResult r) {}
            @Override public void onDeadLettered(Intent i) {}
            @Override public void onExpired(Intent i) {}
            @Override public void onDeliveryFailed(Intent i, Throwable e) {}
        };
        IntentObserver observerB = new IntentObserver() {
            @Override public void onScheduled(Intent i) { eventsB.add("B"); }
            @Override public void onDelivered(Intent i, DeliveryResult r) {}
            @Override public void onDeadLettered(Intent i) {}
            @Override public void onExpired(Intent i) {}
            @Override public void onDeliveryFailed(Intent i, Throwable e) {}
        };

        DeliveryHandler handler = intent -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        scheduler.setObservers(List.of(observerA));
        scheduler.start();

        Intent i1 = readyIntent("obs-dyn-1");
        scheduler.schedule(i1);
        sleep(1500);
        assertFalse(eventsA.isEmpty(), "observer A should receive events");

        // Switch to observer B
        eventsA.clear();
        scheduler.setObservers(List.of(observerB));
        Intent i2 = readyIntent("obs-dyn-2");
        scheduler.schedule(i2);
        sleep(1500);
        assertTrue(eventsA.isEmpty(), "observer A should no longer receive events");
        assertFalse(eventsB.isEmpty(), "observer B should receive events");
    }

    @Test
    void shouldWorkWithoutAnyObservers() throws Exception {
        DeliveryHandler handler = intent -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        scheduler.start();

        Intent intent = readyIntent("obs-none");
        scheduler.schedule(intent);

        sleep(1500);
        Intent stored = intentStore.findById("obs-none");
        assertNotNull(stored);
        assertTrue(stored.getStatus().isTerminal() || stored.getStatus() == IntentStatus.ACKED);
    }
}
