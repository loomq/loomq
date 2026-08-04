package com.loomq.application.scheduler;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.spi.DeliveryHandler;
import com.loomq.spi.DeliveryHandler.DeliveryResult;
import com.loomq.store.ConcurrentIntentStore;
import com.loomq.store.IntentStore;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * AdaptiveTierScanner 事件驱动测试（默认目录 ULTRA 已启用 adaptiveScan=true）。
 *
 * <p>用真实 PrecisionScheduler + 真实时钟验证 adaptive 扫描路径：
 * 1) 未来桶 intent 由新桶监听 unpark 触发投递（早于保底 tick）；
 * 2) 已到期 intent 持续 scan 直至 drain；
 * 3) 空表保底 tick 下循环不崩溃、可正常停止。</p>
 *
 * <p><b>保底 tick 语义</b>：ULTRA 的 fallback tick = {@code scanIntervalMs × 100 = 10 × 100 = 1000ms}。
 * 当一个近期的 intent 插入时，若它由新桶监听 unpark 触发投递，将在远小于 1000ms 内完成；
 * 若监听器缺失（仅靠保底 tick），则要等 scanner 下一次 park 到期（约 1000ms）才被扫描到。
 * 因此把"近期的 intent"投递时间限制在 500ms 内，即可把 unpark 路径与保底 tick 路径区分开。</p>
 */
class AdaptiveScannerTest {

    /** ULTRA fallback tick = scanIntervalMs × 100 = 10 × 100 = 1000ms。 */
    private static final long ULTRA_FALLBACK_TICK_MS = 1_000L;
    /** 事件驱动投递必须远小于保底 tick，否则说明走的是保底 tick 而非 unpark。 */
    private static final long EVENT_DRIVEN_BOUND_MS = 500L;

    private IntentStore intentStore;
    private PrecisionScheduler scheduler;

    @AfterEach
    void tearDown() {
        if (scheduler != null) {
            scheduler.stop();
        }
    }

    private static Intent ultraIntent(String id, Instant executeAt) {
        Intent intent = new Intent(id);
        intent.setExecuteAt(executeAt);
        intent.setPrecisionTier(PrecisionTier.ULTRA);
        intent.transitionTo(IntentStatus.SCHEDULED);
        return intent;
    }

    @Test
    @DisplayName("adaptive 事件驱动: 未来桶 intent 由新桶监听 unpark 触发投递(早于保底 tick)")
    void eventDrivenFutureDelivery() throws Exception {
        intentStore = new ConcurrentIntentStore();
        CountDownLatch delivered = new CountDownLatch(1);
        DeliveryHandler handler = intent -> {
            delivered.countDown();
            return CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        };
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        scheduler.start();

        // 等 scanner 先进入空表 park（保底 tick 1000ms），确保后续投递由 unpark 触发而非首次扫描。
        // 若不加此等待，scanner 首次扫描可能直接命中已到期 intent，测不出 unpark 路径。
        Thread.sleep(100);

        // ULTRA precision window = 10ms；未来 5ms 内入桶（非 cohort），且作为新桶触发监听 unpark。
        long t0 = System.nanoTime();
        Intent intent = ultraIntent("adaptive-future", Instant.now().plusMillis(5));
        scheduler.schedule(intent);

        // 必须远小于保底 tick（1000ms）内投递——只有 unpark 能做到。
        // 若 listener 被移除，该 intent 要等 ~1000ms 保底 tick 才被投递 → 断言失败。
        assertTrue(delivered.await(2, TimeUnit.SECONDS),
            "adaptive scanner should deliver future-dated intent via event-driven wakeup");
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
        assertTrue(elapsedMs < EVENT_DRIVEN_BOUND_MS,
            "intent delivered after " + elapsedMs + "ms — via fallback tick, not event-driven unpark");
    }

    @Test
    @DisplayName("adaptive 扫描: 已到期 intent 持续 scan 直至 drain")
    void immediateDueDrain() throws Exception {
        intentStore = new ConcurrentIntentStore();
        CountDownLatch delivered = new CountDownLatch(2);
        DeliveryHandler handler = intent -> {
            delivered.countDown();
            return CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        };
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        scheduler.start();

        scheduler.schedule(ultraIntent("due-1", Instant.now().minusMillis(5)));
        scheduler.schedule(ultraIntent("due-2", Instant.now().minusMillis(3)));

        assertTrue(delivered.await(5, TimeUnit.SECONDS),
            "adaptive scanner should drain already-due intents");
    }

    @Test
    @DisplayName("更早 key 插入触发 unpark，多个 intent 均被投递")
    void earlierKeyWakesScanner() throws Exception {
        intentStore = new ConcurrentIntentStore();
        CountDownLatch earlierDelivered = new CountDownLatch(1);
        CountDownLatch laterDelivered = new CountDownLatch(1);
        DeliveryHandler handler = intent -> {
            if (intent.getIntentId().equals("earlier")) earlierDelivered.countDown();
            if (intent.getIntentId().equals("later")) laterDelivered.countDown();
            return CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        };
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        scheduler.start();

        // 先放一个远在保底 tick(1000ms)之外的 later 桶(+3000ms)，让 scanner 以此 key 为 park 目标
        // 并进入 park。经 bucketGroupManager 直接入桶，绕开 schedule() 的 cohort 路由(>10ms 走 cohort)，
        // 保证该 far-future 桶驻留桶组、成为 scanner 的 park 目标。
        Intent later = ultraIntent("later", Instant.now().plusMillis(3000));
        scheduler.getBucketGroupManager().add(later);

        // 等 scanner 完成 park 到 later 桶（保底 tick 1000ms，这里只等 100ms 便已进入 park）。
        scannerToPark();

        // 再放一个近期的 earlier 桶(+2ms)：新桶 key 更早，触发 setBucketAddListener unpark。
        long t0 = System.nanoTime();
        scheduler.schedule(ultraIntent("earlier", Instant.now().plusMillis(2)));

        // earlier 必须由 unpark 立即投递(远小于 1000ms 保底 tick)。
        // 若 listener 被移除，earlier 只能等 ~1000ms 保底 tick 才被投递 → 断言失败。
        assertTrue(earlierDelivered.await(2, TimeUnit.SECONDS),
            "earlier intent should be delivered promptly via event-driven wakeup");
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
        assertTrue(elapsedMs < EVENT_DRIVEN_BOUND_MS,
            "earlier intent took " + elapsedMs + "ms — via fallback tick, not event-driven unpark");

        // 收尾：等 later 在其到期时间投递（有界超时），再停。
        assertTrue(laterDelivered.await(5, TimeUnit.SECONDS),
            "later intent should be delivered at its due time");
    }

    @Test
    @DisplayName("空表保底 tick: adaptive 循环空转不崩溃、可正常停止")
    void emptyTableFallbackTick() throws Exception {
        intentStore = new ConcurrentIntentStore();
        DeliveryHandler handler = intent -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        scheduler = new PrecisionScheduler(intentStore, handler, null);
        scheduler.start();

        // 空表下 adaptive 循环应走保底 tick 空转，不抛异常
        Thread.sleep(50);
        assertDoesNotThrow(() -> scheduler.stop());
    }

    /**
     * 留出时间让 scanner 进入 park 状态（空表或首个 far-future 桶）。
     * 100ms 远大于 scanner 单次循环耗时，且远小于 1000ms 保底 tick，
     * 因此能保证 scanner 已 park 而不会在插入前主动到期。
     */
    private static void scannerToPark() throws InterruptedException {
        Thread.sleep(100);
    }
}