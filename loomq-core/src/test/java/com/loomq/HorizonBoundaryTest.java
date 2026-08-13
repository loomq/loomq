package com.loomq;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.domain.intent.AckMode;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.infrastructure.wheel.SlotLocation;
import com.loomq.infrastructure.wheel.WheelConfig;
import com.loomq.infrastructure.wheel.WheelStore;
import com.loomq.spi.DeliveryHandler;
import com.loomq.testutil.TestWheelConfigs;
import java.nio.file.Path;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * 视界边界测试:executeAt 恰好在 horizon 边界的 Intent 的 tail/wheel 路由。
 *
 * <p>WheelStore.locate() 用 delta = executeAt - now 判断:
 * delta > horizonMs -> tail; 否则 -> wheel。
 * 边界值(delta == horizonMs)应落入 wheel(<= 判断)。
 */
class HorizonBoundaryTest {

    @Test
    void exactHorizonGoesToWheel(@TempDir Path tmp) {
        AtomicLong clock = new AtomicLong(Instant.parse("2026-07-30T12:00:00Z").toEpochMilli());
        WheelConfig cfg = TestWheelConfigs.defaults(tmp);
        try (WheelStore store = new WheelStore(cfg, clock::get)) {
            long horizonMs = cfg.horizonDays() * 24L * 60 * 60 * 1000L;

            // executeAt = now + horizonMs exactly (boundary)
            Intent boundary = new Intent("intent_horizon_boundary");
            boundary.setExecuteAt(Instant.ofEpochMilli(clock.get() + horizonMs));
            boundary.setPrecisionTier(PrecisionTier.STANDARD);
            SlotLocation loc = store.locate(boundary.getExecuteAt());

            assertTrue(!loc.inTail(),
                "executeAt = now + horizonMs should go to wheel (not tail), got: " + loc);

            // executeAt = now + horizonMs + 1ms -> tail
            Intent beyond = new Intent("intent_horizon_beyond");
            beyond.setExecuteAt(Instant.ofEpochMilli(clock.get() + horizonMs + 1));
            SlotLocation loc2 = store.locate(beyond.getExecuteAt());

            assertTrue(loc2.inTail(),
                "executeAt = now + horizonMs + 1 should go to tail");
        }
    }

    @Test
    void intentAtHorizonBoundaryRoundTrips(@TempDir Path tmp) throws Exception {
        try (LoomqEngine engine = LoomqEngine.builder()
                .dataDir(tmp).nodeId("horizon-1")
                .deliveryHandler(i -> CompletableFuture.completedFuture(DeliveryHandler.DeliveryResult.SUCCESS))
                .build()) {
            engine.start();

            // Create an intent just inside the hot boundary
            Intent intent = new Intent();
            intent.setExecuteAt(Instant.now().plusSeconds(2));
            intent.setPrecisionTier(PrecisionTier.STANDARD);
            Long seq = engine.createIntent(intent, AckMode.DURABLE).get();
            assertNotNull(seq);
            assertTrue(engine.getIntent(intent.getIntentId()).isPresent(),
                "hot intent should be in memory store");
        }
    }
}
