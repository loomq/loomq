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
 * R11: updateIntent 的用户 updater 可调用 {@code intent.transitionTo(终态)}，绕过 cancelIntent/
 * finalize/handleExpired 专用生命周期路径——终态经 append 持久化，但 removeFromSchedule /
 * locationIndex.remove / reclaimTerminal 清理全部缺失，导致 locationIndex/intentExpiryIndex/
 * multiSlot 泄漏直到重启。生命周期变更应由专用 API 承担，updater 仅允许变更内容/改期，
 * 终态转换必须拒绝。
 */
class BugUpdateTerminalViaUpdaterTest {

    @TempDir Path tmp;

    @Test
    void updaterMustNotTransitionIntentToTerminalState() throws Exception {
        AtomicInteger deliveries = new AtomicInteger();
        DeliveryHandler handler = intent -> {
            deliveries.incrementAndGet();
            return CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        };

        try (LoomqEngine engine = LoomqEngine.builder()
                .dataDir(tmp).nodeId("upd-terminal").deliveryHandler(handler).build()) {
            engine.start();

            long t0 = System.currentTimeMillis();
            Intent it = new Intent("upd-terminal-0001");
            it.setExecuteAt(Instant.ofEpochMilli(t0 + 300));
            it.setPrecisionTier(PrecisionTier.ULTRA);
            engine.createIntent(it, AckMode.DURABLE).get();

            // 修复前：updater 的 transitionTo(CANCELED) 静默生效，intent 变为终态、跳过投递，
            // 且 locationIndex/intentExpiryIndex/multiSlot 泄漏。修复后：必须拒绝并保持 SCHEDULED。
            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> engine.updateIntent("upd-terminal-0001", i -> i.transitionTo(IntentStatus.CANCELED)),
                "updater 转终态必须被拒绝，生命周期变更应走 cancelIntent");

            assertTrue(ex.getMessage().contains("cancelIntent"),
                "错误信息应指引调用方使用 cancelIntent，got: " + ex.getMessage());

            Intent after = engine.getIntent("upd-terminal-0001").orElseThrow();
            assertEquals(IntentStatus.SCHEDULED, after.getStatus(),
                "拒绝后 intent 必须保持 SCHEDULED（可继续投递），不得残留终态");

            // intent 仍按原计划投递（证明未被破坏）
            long deadline = System.currentTimeMillis() + 5_000;
            while (deliveries.get() == 0 && System.currentTimeMillis() < deadline) {
                Thread.sleep(20);
            }
            assertEquals(1, deliveries.get(), "拒绝终态转换后 intent 必须仍按原计划投递");
        }
    }
}
