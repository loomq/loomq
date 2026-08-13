package com.loomq.application.command;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.LoomqEngine;
import com.loomq.domain.intent.AckMode;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.spi.DeliveryHandler;
import com.loomq.spi.DeliveryHandler.DeliveryResult;
import java.nio.file.Path;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * R16: updateIntent 的用户 updater 可置空 executeAt——persistToWheel.locate(null) 抛 NPE,
 * 且 scanDue 的 intent.getExecuteAt().isAfter(now) 持续 NPE 使该档扫描永久卡死(同桶及后续
 * intent 全部饿死)。修复:updater 后 executeAt 空值复查,回滚并抛 IllegalArgumentException。
 */
class BugUpdateNullExecuteAtTest {

    @TempDir Path tmp;

    @Test
    void updaterMustNotNullExecuteAt() throws Exception {
        AtomicInteger deliveries = new AtomicInteger();
        DeliveryHandler handler = intent -> {
            deliveries.incrementAndGet();
            return CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        };

        try (LoomqEngine engine = LoomqEngine.builder()
                .dataDir(tmp).nodeId("upd-null-exec").deliveryHandler(handler).build()) {
            engine.start();

            long t0 = System.currentTimeMillis();
            Intent it = new Intent("upd-null-exec-0001");
            it.setExecuteAt(Instant.ofEpochMilli(t0 + 300));
            it.setPrecisionTier(PrecisionTier.ULTRA);
            engine.createIntent(it, AckMode.DURABLE).get();

            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> engine.updateIntent("upd-null-exec-0001", i -> i.setExecuteAt(null)),
                "updater 置空 executeAt 必须被拒绝");
            assertTrue(ex.getMessage().contains("executeAt"),
                "错误信息应指明 executeAt 空值,got: " + ex.getMessage());

            // 修复后:executeAt 回滚到旧值,intent 仍按原计划投递;修复前:NPE 且扫描卡死。
            Intent after = engine.getIntent("upd-null-exec-0001").orElseThrow();
            assertEquals(Instant.ofEpochMilli(t0 + 300), after.getExecuteAt(),
                "拒绝后 executeAt 必须回滚到原值");
            assertEquals(IntentStatus.SCHEDULED, after.getStatus(),
                "拒绝后 intent 必须保持 SCHEDULED");

            long deadline = System.currentTimeMillis() + 5_000;
            while (deliveries.get() == 0 && System.currentTimeMillis() < deadline) {
                Thread.sleep(20);
            }
            assertEquals(1, deliveries.get(), "拒绝空 executeAt 后 intent 必须仍按原计划投递");
        }
    }
}
