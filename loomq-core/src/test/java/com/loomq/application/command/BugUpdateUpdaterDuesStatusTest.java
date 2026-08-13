package com.loomq.application.command;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
 * updateIntent 用户 updater 把状态置为 DUE 后抛异常回归：updater 异常不得把 intent 留在
 * "已摘除调度结构"的 DUE 状态。
 *
 * <p>时序：updateIntent(id, updater, newExecuteAt) → reschedule 分支先 removeFromSchedule
 * 摘除桶/cohort 条目 → updater 内部 transitionTo(DUE) 后抛异常 → 异常上抛。R5/R6 的
 * "DUE 结算容错"只覆盖了 finalizeIntent / handleDeliveryFailure 的 DUE 前奏，但 updateIntent
 * 的失败回滚仍写死 {@code status == SCHEDULED} 才重新调度——updater 置 DUE 后抛异常时，
 * intent 已不在任何调度结构、store 中是 DUE、磁盘仍是旧 SCHEDULED 槽，却永不投递，直到
 * 重启恢复才被重新调度（静默延迟丢失）。回滚必须镜像正常 reschedule 分支：DUE 走
 * restore()、SCHEDULED 走 schedule()。</p>
 */
class BugUpdateUpdaterDuesStatusTest {

    @Test
    void updaterSettingDueThenThrowingMustNotLeaveIntentUnscheduled(@TempDir Path tmp) throws Exception {
        AtomicInteger deliveries = new AtomicInteger();
        DeliveryHandler handler = intent -> {
            deliveries.incrementAndGet();
            return CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        };

        try (LoomqEngine engine = LoomqEngine.builder()
                .dataDir(tmp).nodeId("upd-due-fail").deliveryHandler(handler).build()) {
            engine.start();

            long t0 = System.currentTimeMillis();
            Intent it = new Intent("upd-due-fail-1");
            it.setExecuteAt(Instant.ofEpochMilli(t0 + 400));
            it.setPrecisionTier(PrecisionTier.ULTRA);
            engine.createIntent(it, AckMode.DURABLE).get();

            // updater 把状态推进到 DUE（R5/R6 结算容错覆盖的中间态）后抛异常 + 改期 →
            // removeFromSchedule 已执行，修复前：intent 卡在 DUE、不在任何调度结构、永不投递。
            assertThrows(RuntimeException.class, () -> engine.updateIntent(
                "upd-due-fail-1",
                i -> {
                    i.transitionTo(IntentStatus.DUE);
                    throw new IllegalStateException("updater boom after DUE");
                },
                Instant.ofEpochMilli(t0 + 60_000)),
                "updater 异常必须上抛给调用方");

            // 修复后：按原 executeAt 重新调度 → 仍投递并结算；修复前：永不投递（断言超时失败）
            long deadline = System.currentTimeMillis() + 5_000;
            while (deliveries.get() == 0 && System.currentTimeMillis() < deadline) {
                Thread.sleep(20);
            }
            assertEquals(1, deliveries.get(), "updater 置 DUE 后失败，intent 必须仍按原计划投递");
        }
    }
}
