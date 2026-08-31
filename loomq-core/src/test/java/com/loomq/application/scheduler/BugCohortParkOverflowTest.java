package com.loomq.application.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.spi.DeliveryHandler;
import com.loomq.spi.DeliveryHandler.DeliveryResult;
import com.loomq.store.ConcurrentIntentStore;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * CohortManager park 溢出回归：远期 intent（executeAt &gt; ~292 年）不得让 wakeLoop 空转报错。
 *
 * <p>wakeLoop 用 {@code Duration.ofMillis(sleepMs).toNanos()} 计算 park 时长，&gt;292 年
 * 跨度 multiplyExact 溢出抛 ArithmeticException → 每 100ms 重复报错（日志风暴）；
 * 若该 cohort 是 earliest，后续更晚 cohort 全部饿死。PromotionDaemon 已有
 * MAX_PARK_MS=24h 钳制（round 1 修复），CohortManager 漏修。断言 wakeLoop 错误计数为 0。</p>
 */
class BugCohortParkOverflowTest {

    private PrecisionScheduler scheduler;

    @AfterEach
    void tearDown() {
        if (scheduler != null) scheduler.stop();
    }

    @Test
    void farFutureCohortMustNotSpinWakeLoop() throws Exception {
        ConcurrentIntentStore intentStore = new ConcurrentIntentStore();
        DeliveryHandler handler = intent -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        scheduler.start();

        Intent far = new Intent("far-future");
        // >292 年：Duration.ofMillis(...).toNanos() multiplyExact 溢出抛异常
        far.setExecuteAt(Instant.now().plusMillis(400L * 365 * 24 * 3600 * 1000L));
        far.setPrecisionTier(PrecisionTier.STANDARD);
        far.transitionTo(IntentStatus.SCHEDULED);
        intentStore.save(far);
        scheduler.schedule(far);

        Thread.sleep(600);   // 修复前：wakeLoop 每 ~100ms 报错一次（wakeLoopErrors>=1）

        assertEquals(0, scheduler.getCohortManager().getWakeLoopErrors(),
            "wakeLoop 不得因 park 时长溢出而错误空转");
    }
}
