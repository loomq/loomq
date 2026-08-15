package com.loomq;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.loomq.domain.intent.AckMode;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.spi.DeliveryHandler;
import com.loomq.spi.DeliveryHandler.DeliveryResult;
import com.loomq.spi.IntentObserver;
import com.loomq.testutil.TestStores;
import java.nio.file.Path;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * 调度器结算路径的 I6 容错应同样覆盖 IntentStore.update() 失败：
 * 内存镜像更新失败不能吞掉 onDelivered / 终态持久化，否则已投递 Intent 重启后可能重复投递。
 */
class BugSchedulerFinalizeStoreUpdateFailureTest {

    @TempDir Path tmp;

    private static final DeliveryHandler SUCCESS =
        i -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS);

    @Test
    void onDeliveredMustStillFireWhenStoreUpdateFails() throws Exception {
        String id = "sched-finalize-01";
        AtomicInteger delivered = new AtomicInteger();
        try (LoomqEngine engine = LoomqEngine.builder()
                .dataDir(tmp.resolve("s")).nodeId("s1")
                .intentStore(new TestStores.UpdateThrowingStore())
                .deliveryHandler(SUCCESS)
                .build()) {
            engine.start();
            engine.registerObserver(new IntentObserver() {
                @Override public void onScheduled(Intent intent) { }
                @Override public void onDelivered(Intent intent, DeliveryResult result) { delivered.incrementAndGet(); }
                @Override public void onDeadLettered(Intent intent) { }
                @Override public void onExpired(Intent intent) { }
                @Override public void onDeliveryFailed(Intent intent, Throwable error) { }
            });

            Intent it = new Intent(id);
            it.setExecuteAt(Instant.ofEpochMilli(System.currentTimeMillis() + 50));
            it.setPrecisionTier(PrecisionTier.ULTRA);
            engine.createIntent(it, AckMode.DURABLE).get();

            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (delivered.get() == 0 && System.nanoTime() < deadline) {
                Thread.sleep(20);
            }
            assertEquals(1, delivered.get(),
                "onDelivered must still fire even if IntentStore.update fails after durable delivery");
        }
    }
}
