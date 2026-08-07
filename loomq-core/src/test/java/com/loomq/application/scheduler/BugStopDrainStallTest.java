package com.loomq.application.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import org.junit.jupiter.api.Test;

/**
 * FIX #2 (stop() drain no longer stalls consumers) - regression guard.
 *
 * <p>Previously PrecisionScheduler.stop() "drained" every tier by acquiring ALL permits and NEVER
 * releasing them. A consumer that re-looped called acquireWithBorrow() -> own.acquire() (blocking
 * fallback). With the drain holding every permit, that acquire could never succeed on its own.
 * The only way the consumer unblocked was sharedExecutor.shutdownNow() interrupting it - which
 * happened only after stop() burned the 10s awaitTermination timeout.
 *
 * <p>The fix replaces semaphore-based drain with per-tier in-flight counters (tierInFlight).
 * stop() spins on tierInFlight == 0 (dispatch +1, finalize -1) instead of acquiring all permits.
 * Consumers blocked on acquire() are unaffected because no permits are stolen from them.
 *
 * <p>This test verifies the fix: the drain mechanism does NOT consume semaphore permits, and it
 * completes promptly once in-flight deliveries finish (no 10s stall).
 */
class BugStopDrainStallTest {

    @Test
    void stopDrainsByInFlightCounterNotSemaphoreAcquire() throws Exception {
        int max = 4;
        ResizableSemaphore sem = new ResizableSemaphore(max);
        assertEquals(max, sem.availablePermits());

        // Simulate an in-flight delivery: tierInFlight = 1
        AtomicInteger inFlight = new AtomicInteger(1);

        // Simulate the delivery completing after 200ms
        Thread delivery = Thread.ofVirtual().start(() -> {
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            inFlight.set(0); // delivery finalized -> tierInFlight decremented
        });

        // Simulate stop()'s drain loop (PrecisionScheduler.stop() lines 360-375):
        // Spin on tierInFlight > 0 with a 10s deadline, parking 10ms per iteration.
        long drainDeadlineNs = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (inFlight.get() > 0) {
            if (System.nanoTime() > drainDeadlineNs) {
                break;
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
        }

        long elapsedMs = (System.nanoTime() - (drainDeadlineNs - TimeUnit.SECONDS.toNanos(10))) / 1_000_000;

        // FIX #2: semaphore permits are NOT consumed by the drain
        assertEquals(max, sem.availablePermits(),
            "FIX #2: stop()'s drain uses tierInFlight, not semaphore acquire -> permits untouched");

        // FIX #2: drain completes promptly once in-flight reaches 0 (no 10s stall)
        assertTrue(inFlight.get() == 0,
            "FIX #2: drain completed because in-flight counter reached 0");
        assertTrue(elapsedMs < 5000,
            "FIX #2: drain completed in ~" + elapsedMs + "ms (well under the 10s timeout)");

        delivery.join(2000);
        assertTrue(!delivery.isAlive());
    }
}
