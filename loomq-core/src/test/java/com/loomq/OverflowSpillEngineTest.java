package com.loomq;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.loomq.domain.intent.AckMode;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.domain.intent.PrecisionTierCatalog;
import com.loomq.infrastructure.wheel.WheelConfig;
import com.loomq.spi.DeliveryHandler;
import com.loomq.spi.IntentObserver;
import java.nio.file.Path;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * 方向 C 溢出 spill 的引擎级验证：同秒突发超过 SEC 桶容量时，
 * createIntent 不再抛 SlotOverflowException（spill 到 MIN 吸收），全部投递、零持久化失败。
 */
@Tag("integration")
class OverflowSpillEngineTest {

    private static final DeliveryHandler SUCCESS = intent ->
        CompletableFuture.completedFuture(DeliveryHandler.DeliveryResult.SUCCESS);

    @Test
    void burstExceedingSecondBucketCapacityDeliversWithSpill(@TempDir Path tmp) throws Exception {
        var base = PrecisionTierCatalog.defaultCatalog();
        // SEC 桶容量 64：100 个同秒 intent → 64 落 SEC，36 spill 到 MIN（MIN 容量 64 ≥ 36）
        var wheel = WheelConfig.defaultConfig().withDataDir(tmp.toString()).withSlotsPerBucket(64);

        try (LoomqEngine engine = LoomqEngine.builder()
                .wheelConfig(wheel).nodeId("spill-t")
                .catalog(base)
                .deliveryHandler(SUCCESS).build()) {
            engine.start();

            AtomicLong delivered = new AtomicLong();
            engine.registerObserver(new IntentObserver() {
                @Override public void onDelivered(Intent i, DeliveryHandler.DeliveryResult r) { delivered.incrementAndGet(); }
                @Override public void onScheduled(Intent i) {}
                @Override public void onDeadLettered(Intent i) {}
                @Override public void onExpired(Intent i) {}
                @Override public void onDeliveryFailed(Intent i, Throwable e) {}
            });

            long t0 = System.currentTimeMillis();
            // 100 个同一秒 executeAt：SEC 桶容量 64 → 第 65 个起必须 spill 到 MIN 才能落盘
            for (int i = 0; i < 100; i++) {
                Intent it = new Intent();
                it.setExecuteAt(Instant.ofEpochMilli(t0 + 5_000));
                it.setPrecisionTier(PrecisionTier.STANDARD);
                engine.createIntent(it, AckMode.DURABLE);   // 无 spill 时第 65 个抛 SlotOverflowException
            }

            long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(30);
            while (delivered.get() < 100 && System.currentTimeMillis() < deadline) Thread.sleep(50);
            assertEquals(100, delivered.get(), "spill 吸收溢出：同秒 100 突发全部投递");
            assertEquals(0, engine.getScheduler().getPersistFailures(),
                "溢出被 spill 吸收，无持久化失败（无 SlotOverflowException 冒泡到 finalize）");
        }
    }
}
