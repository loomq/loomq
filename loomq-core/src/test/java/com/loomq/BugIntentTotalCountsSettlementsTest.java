package com.loomq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.common.MetricsCollector;
import com.loomq.domain.intent.AckMode;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.RedeliveryPolicy;
import com.loomq.spi.DeliveryHandler;
import com.loomq.spi.DeliveryHandler.DeliveryResult;
import java.nio.file.Path;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * R21: incrementIntentByTier 的唯一调用点在 finalizeIntent 的 finally——计数器统计的是
 * "投递结算次数"而非 HELP 声称的"创建数":重试风暴虚增(每次 RETRY 再 +1)、从未到期的
 * 冷 intent 完全不计,按创建量做的容量/告警判断全部失真。修复:计数移到 createIntent/
 * createIntents 的成功路径(热/冷均计),结算路径不再 +1。
 */
class BugIntentTotalCountsSettlementsTest {

    @TempDir
    Path tempDir;

    @Test
    void intentTotalMustCountCreationsNotSettlements() throws Exception {
        AtomicInteger deliveries = new AtomicInteger();
        DeliveryHandler handler = intent -> {
            int n = deliveries.incrementAndGet();
            // 前两次 RETRY,第三次 SUCCESS → 3 次结算
            return CompletableFuture.completedFuture(n <= 2 ? DeliveryResult.RETRY : DeliveryResult.SUCCESS);
        };
        MetricsCollector mc = new MetricsCollector();
        try (LoomqEngine engine = LoomqEngine.builder()
            .walDir(tempDir)
            .nodeId("r21-intent-total")
            .deliveryHandler(handler)
            .metricsCollector(mc)
            .build()) {
            engine.start();

            Intent intent = new Intent("r21-intent-total-0001");
            intent.setExecuteAt(Instant.now().minusMillis(50));
            intent.setRedelivery(new RedeliveryPolicy(5, "fixed", 1, 1, 1.0, false));
            engine.createIntent(intent, AckMode.DURABLE).join();

            // 等 3 次结算完成 → ACKED
            IntentStatus st = null;
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (System.nanoTime() < deadline) {
                st = engine.getIntent("r21-intent-total-0001").map(Intent::getStatus).orElse(null);
                if (st == IntentStatus.ACKED) break;
                Thread.sleep(10);
            }
            assertEquals(IntentStatus.ACKED, st, "intent must settle via 3 attempts");
            assertEquals(3, deliveries.get(), "sanity: exactly three settlement attempts");
            assertEquals(1L, mc.getIntentCountsByTier().get(intent.getPrecisionTier()),
                "counter claims 'Total intents created': 1 creation must count 1, not 3 settlements");
            assertTrue(mc.getIntentCountsByTier().values().stream().mapToLong(Long::longValue).sum() <= 2,
                "total across tiers must be ~1 (one creation), not inflated by retries");
        }
    }
}
