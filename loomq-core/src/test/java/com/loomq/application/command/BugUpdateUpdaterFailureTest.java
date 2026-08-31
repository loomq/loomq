package com.loomq.application.command;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.loomq.LoomqEngine;
import com.loomq.domain.intent.AckMode;
import com.loomq.domain.intent.Intent;
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
 * updateIntent 用户 updater 异常回归：updater 抛异常时不得把 intent 留在"已摘除调度结构"状态。
 *
 * <p>时序：updateIntent(id, updater, newExecuteAt) → reschedule 分支先 removeFromSchedule
 * 摘除桶/cohort 条目 → updater.accept 抛异常（用户代码 NPE 等）→ 异常上抛，但 intent 已不在
 * 任何调度结构——store 中是 SCHEDULED、磁盘已持久化，却永不投递，直到重启恢复才被重新调度
 * （静默延迟丢失）。修复：updater 抛异常或持久化失败时，若已摘除调度结构则按当前 executeAt
 * 重新 schedule()，保证投递不丢。</p>
 */
class BugUpdateUpdaterFailureTest {

    @Test
    void updaterFailureMustNotLeaveIntentUnscheduled(@TempDir Path tmp) throws Exception {
        AtomicInteger deliveries = new AtomicInteger();
        DeliveryHandler handler = intent -> {
            deliveries.incrementAndGet();
            return CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        };

        try (LoomqEngine engine = LoomqEngine.builder()
                .dataDir(tmp).nodeId("upd-fail").deliveryHandler(handler).build()) {
            engine.start();

            long t0 = System.currentTimeMillis();
            Intent it = new Intent("upd-fail-1");
            it.setExecuteAt(Instant.ofEpochMilli(t0 + 400));
            it.setPrecisionTier(PrecisionTier.ULTRA);
            engine.createIntent(it, AckMode.DURABLE).get();

            // updater 抛异常 + 改期 → removeFromSchedule 已执行（修复前：intent 从此消失）
            assertThrows(RuntimeException.class, () -> engine.updateIntent(
                "upd-fail-1",
                i -> { throw new IllegalStateException("updater boom"); },
                Instant.ofEpochMilli(t0 + 60_000)),
                "updater 异常必须上抛给调用方");

            // 修复后：按原 executeAt 重新调度 → 仍投递；修复前：永不投递（断言超时失败）
            long deadline = System.currentTimeMillis() + 5_000;
            while (deliveries.get() == 0 && System.currentTimeMillis() < deadline) {
                Thread.sleep(20);
            }
            assertEquals(1, deliveries.get(), "updater 失败后 intent 必须仍按原计划投递");
        }
    }
}
