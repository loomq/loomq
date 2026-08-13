package com.loomq.application.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.LoomqEngine;
import com.loomq.domain.intent.AckMode;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.RedeliveryPolicy;
import com.loomq.spi.DeliveryHandler;
import com.loomq.spi.DeliveryHandler.DeliveryResult;
import java.nio.file.Path;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * R21: updateIntent 的 updater 可合法两步迁移 SCHEDULED→DUE→DISPATCHING(validateTransition
 * 均放行,R11 只拦终态、R16 只拦 executeAt=null)——意图被持久化为"无投递在途"的
 * DISPATCHING:内存态永久卡死;finalize 的 R6 DUE 起步守卫从 DISPATCHING 转 DUE 抛 ISE
 * 被 runFinalizeTask 吞 → ACK 不落盘/onDelivered 不通知;重启后 recovery 把非终态
 * DISPATCHING 当活 intent 重新投递 → 与已发生投递形成重复投递。
 *
 * <p>修复:updater 后只允许停留 SCHEDULED/DUE,其余状态回滚并抛 IllegalArgumentException。</p>
 */
class BugUpdaterDispatchingStuckTest {

    @TempDir
    Path tempDir;

    private final DeliveryHandler mockHandler =
        intent -> CompletableFuture.completedFuture(DeliveryResult.DEAD_LETTER);

    @Test
    void updaterMustNotPersistDispatchingWithoutInflightDelivery() throws Exception {
        try (LoomqEngine engine = LoomqEngine.builder()
            .walDir(tempDir)
            .nodeId("r21-dispatching-node")
            .deliveryHandler(mockHandler)
            .build()) {
            engine.start();

            Intent intent = new Intent("r21-dispatching-0001");
            intent.setExecuteAt(Instant.ofEpochMilli(System.currentTimeMillis() + 30_000));
            intent.setRedelivery(new RedeliveryPolicy(2, "fixed", 1000, 1000, 1.0, false));
            engine.createIntent(intent, AckMode.DURABLE).join();
            assertEquals(IntentStatus.SCHEDULED, engine.getIntent("r21-dispatching-0001").get().getStatus());

            // 修复前:updater 合法两步迁移通过所有守卫,意图持久化为 DISPATCHING 卡死;
            // 修复后:拒绝并回滚到 SCHEDULED。
            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> engine.updateIntent("r21-dispatching-0001", i -> {
                    i.transitionTo(IntentStatus.DUE);
                    i.transitionTo(IntentStatus.DISPATCHING);
                }),
                "updater must not move an intent to DISPATCHING without an in-flight delivery");
            assertTrue(ex.getMessage().contains("DISPATCHING"),
                "error must name the offending state, got: " + ex.getMessage());

            Intent after = engine.getIntent("r21-dispatching-0001").get();
            assertEquals(IntentStatus.SCHEDULED, after.getStatus(),
                "intent must be rolled back to SCHEDULED and remain scheduled");
            assertEquals(30_000L, after.getExecuteAt().toEpochMilli() - System.currentTimeMillis(),
                5_000L, "original schedule must be preserved");
        }
    }
}
