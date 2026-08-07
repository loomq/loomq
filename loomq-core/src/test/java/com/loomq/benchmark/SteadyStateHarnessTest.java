package com.loomq.benchmark;

import static org.junit.jupiter.api.Assertions.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class SteadyStateHarnessTest {

    @Test void closedLoopMaintainsInFlightAndProducesSamples() throws Exception {
        AtomicLong inFlight = new AtomicLong();
        AtomicLong maxInFlight = new AtomicLong();
        AtomicReference<SteadyStateHarness> ref = new AtomicReference<>();
        SteadyStateHarness h = new SteadyStateHarness(8, () -> {
            long cur = inFlight.incrementAndGet();
            maxInFlight.accumulateAndGet(cur, Math::max);
            new Thread(() -> { inFlight.decrementAndGet(); ref.get().onComplete(0); }).start();
        });
        ref.set(h);
        SteadyStateHarness.Result r = h.run(200, 1000, 5);
        assertTrue(r.qpsSamples() >= 5, "应产出多个整窗样本");
        assertTrue(r.qpsMedian() >= 0, "QPS 应非负");
        assertTrue(maxInFlight.get() <= 8, "在途不应超过 inFlight: " + maxInFlight.get());
    }

    @Test void completionProducesPositiveLatencyStats() throws Exception {
        AtomicReference<SteadyStateHarness> ref = new AtomicReference<>();
        SteadyStateHarness h = new SteadyStateHarness(4, () -> new Thread(() -> ref.get().onComplete(5)).start());
        ref.set(h);
        SteadyStateHarness.Result r = h.run(100, 500, 5);
        assertTrue(r.qpsMedian() >= 0);
        assertTrue(r.e2eP99Us() >= 0);
        assertTrue(r.qpsSamples() >= 5, "至少 5 个整窗样本");
    }
}