package com.loomq.infrastructure.wheel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.loomq.application.recovery.WheelRecovery;
import com.loomq.application.recovery.WheelRecoveryReport;
import com.loomq.application.scheduler.PrecisionScheduler;
import com.loomq.common.MetricsCollector;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.RedeliveryPolicy;
import com.loomq.spi.DeliveryHandler;
import com.loomq.store.ConcurrentIntentStore;
import java.nio.file.Path;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * R20: RedeliveryPolicy 不进 PHTW 槽持久化——SlotCodec 只编码 attempts(0x0B)却丢策略本身。
 * 崩溃恢复后,自定义重试语义(maxAttempts/退避/抖动)静默回退为默认值(5000ms/5 次):
 * maxAttempts=2 的契约在重启后变成 5 次(超契约投递),maxAttempts=100 的重试链在 5 次后
 * 提前死信。重试重排程的 DURABLE 落盘(StateChangeSink)同样经此编码,重启即丢策略。
 */
class SlotCodecRedeliveryPersistenceTest {

    @Test
    void redeliveryPolicyMustSurviveSlotRoundTrip() {
        Intent intent = new Intent("r20-codec-rd-0001");
        intent.setExecuteAt(Instant.now().plusSeconds(60));
        intent.transitionTo(IntentStatus.SCHEDULED);
        intent.incrementRevision();
        intent.setRedelivery(new RedeliveryPolicy(3, "exponential", 250, 10_000, 1.5, false));

        Intent decoded = SlotCodec.decode(SlotCodec.encode(intent));

        assertNotNull(decoded.getRedelivery(),
            "redelivery policy must survive the slot round-trip (custom retry contract is durable)");
        assertEquals(3, decoded.getRedelivery().getMaxAttempts());
        assertEquals("exponential", decoded.getRedelivery().getBackoff());
        assertEquals(250, decoded.getRedelivery().getInitialDelayMs());
        assertEquals(10_000, decoded.getRedelivery().getMaxDelayMs());
        assertEquals(1.5, decoded.getRedelivery().getMultiplier(), 1e-9);
        assertFalse(decoded.getRedelivery().isJitter());
    }

    @Test
    void redeliveryPolicyMustSurviveWheelRecovery() {
        AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        WheelConfig cfg = new WheelConfig(tmp.toString(), "t", 30, 16, 1, 10_000L, 60L * 60_000L, 60_000L, null);
        String id = "r20-recover-rd-0001";

        try (WheelStore store = new WheelStore(cfg, clock::get);
             TailIndex tail = new TailIndex(tmp, clock::get)) {
            Intent intent = new Intent(id);
            intent.setExecuteAt(Instant.ofEpochMilli(clock.get() + 60_000));
            intent.transitionTo(IntentStatus.SCHEDULED);
            intent.incrementRevision();
            intent.setRedelivery(new RedeliveryPolicy(2, "fixed", 100, 500, 1.0, false));
            store.put(intent);
        }

        try (WheelStore store2 = new WheelStore(cfg, clock::get);
             TailIndex tail2 = new TailIndex(tmp, clock::get);
             ConcurrentIntentStore mem = new ConcurrentIntentStore();
             IntentLocationIndex idx = new IntentLocationIndex();
             PromotionDaemon daemon = new PromotionDaemon(store2, tail2, idx, clock::get, (i, loc) -> {}, 60_000L)) {
            PrecisionScheduler scheduler = new PrecisionScheduler(
                mem, i -> CompletableFuture.completedFuture(DeliveryHandler.DeliveryResult.SUCCESS), null);
            WheelRecoveryReport rpt = new WheelRecovery(store2, tail2, 60L * 60_000L, new MetricsCollector())
                .recover(mem, scheduler, idx, daemon);

            assertNotNull(rpt.maxRevisions().get(id), "intent must be recovered");
            RedeliveryPolicy recovered = mem.findById(id).getRedelivery();
            assertNotNull(recovered,
                "redelivery policy must survive crash recovery (recovered intent lost it)");
            assertEquals(2, recovered.getMaxAttempts());
            assertEquals("fixed", recovered.getBackoff());
            assertEquals(100, recovered.getInitialDelayMs());
            assertEquals(500, recovered.getMaxDelayMs());
            assertFalse(recovered.isJitter());
        }
    }

    @TempDir
    Path tmp;
}
