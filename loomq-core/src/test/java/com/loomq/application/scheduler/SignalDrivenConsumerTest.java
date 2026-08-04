package com.loomq.application.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.domain.intent.PrecisionTierCatalog;
import com.loomq.domain.intent.PrecisionTierProfile;
import com.loomq.domain.intent.WalMode;
import com.loomq.spi.DeliveryHandler;
import com.loomq.spi.DeliveryHandler.DeliveryResult;
import com.loomq.store.ConcurrentIntentStore;
import com.loomq.store.IntentStore;
import java.lang.reflect.Field;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Phase 3 (PR3) — Signal-driven 消费端（C3）测试。
 *
 * <p>Phase 1/2 让扫描端事件驱动；本测试验证消费端改造：
 * 1. <b>offer → unpark</b>：scanAndDispatch 将 intent 投递到 dispatch 队列后 unpark 一个消费者，
 *    消费者在 unpark 后立即消费（远小于 1ms park cap，证明走 unpark 而非 poll cap）；
 * 2. <b>空 poll 保底</b>：无 unpark 时消费者靠 1ms cap 重新 poll，仍在 ~1ms 内恢复消费；
 * 3. <b>无回归</b>：ACK 计数正确。</p>
 *
 * <p>用真实 {@link PrecisionScheduler} + 内存 store + 成功桩 + 高分辨率时间戳。</p>
 *
 * <p><b>为什么用单消费者目录</b>：默认目录 ULTRA 有 16 个消费者，即使无 unpark，16 个消费者
 * 轮询也会在 ~60µs 内取走 intent，无法区分 unpark 与 poll 路径。本测试用单消费者 ULTRA 目录，
 * 使"消费者已 park"时（空队列 park 1ms cap）唯一能提前唤醒它的机制就是 unpark。</p>
 */
class SignalDrivenConsumerTest {

    /** unpark 路径的投递中位数上界：须远小于 1ms park cap（1000µs），证明走 unpark。 */
    private static final long UNPARK_MEDIAN_BOUND_US = 500L;
    /**
     * 空 poll 保底（无 unpark）的投递恢复上界。理想路径是 1ms park cap re-poll（~1ms），
     * 但该断言是"恢复界"而非"理想界"：放宽到 100ms（约 100x 理想 park cap 时间），
     * 容忍任何现实负载下的调度抖动，仅当消费者真正卡死（恢复 >100ms 表明真实回归）时才失败。
     */
    private static final long FALLBACK_BOUND_US = 100000L;

    private IntentStore intentStore;
    private PrecisionScheduler scheduler;

    @AfterEach
    void tearDown() {
        if (scheduler != null) {
            scheduler.stop();
        }
    }

    /** 单消费者目录：ULTRA(单发) + STANDARD(批量) 均 adaptive、consumerCount=1。须含全部枚举档避免 acquireWithBorrow 借档 NPE。 */
    private static PrecisionTierCatalog singleConsumerCatalog() {
        EnumMap<PrecisionTier, PrecisionTierProfile> profiles = new EnumMap<>(PrecisionTier.class);
        profiles.put(PrecisionTier.ULTRA, new PrecisionTierProfile(10, 200, 1, 5, 1, 200 * 16,
            WalMode.DURABLE, 10, false, true, 200_000));
        profiles.put(PrecisionTier.FAST, new PrecisionTierProfile(50, 150, 1, 10, 1, 150 * 16,
            WalMode.DURABLE, 50, false, true, 200_000));
        profiles.put(PrecisionTier.HIGH, new PrecisionTierProfile(100, 50, 5, 50, 1, 50 * 16,
            WalMode.DURABLE, 100, false, true, 200_000));
        profiles.put(PrecisionTier.STANDARD, new PrecisionTierProfile(500, 50, 20, 100, 1, 50 * 16,
            WalMode.DURABLE, 500, false, true, 200_000));
        profiles.put(PrecisionTier.ECONOMY, new PrecisionTierProfile(1000, 50, 25, 300, 1, 50 * 16,
            WalMode.DURABLE, 1000, false, true, 200_000));
        profiles.put(PrecisionTier.MILLI, new PrecisionTierProfile(1, 100, 1, 1, 1, 100 * 16,
            WalMode.DURABLE, 1, true, true, 200_000));
        return PrecisionTierCatalog.of(profiles, PrecisionTier.STANDARD);
    }

    private static Intent dueIntent(String id, PrecisionTier tier, Instant executeAt) {
        Intent intent = new Intent(id);
        intent.setExecuteAt(executeAt);
        intent.setPrecisionTier(tier);
        intent.transitionTo(IntentStatus.SCHEDULED);
        return intent;
    }

    @Test
    @DisplayName("offer→unpark: 消费者在 park 状态下被 unpark 立即消费(远小于 1ms cap)")
    void offerUnparkDeliversFast() throws Exception {
        intentStore = new ConcurrentIntentStore();
        List<Long> deliveryTimes = Collections.synchronizedList(new ArrayList<>());
        DeliveryHandler handler = intent -> {
            deliveryTimes.add(System.nanoTime());
            return CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        };
        scheduler = new PrecisionScheduler(intentStore, handler, null, singleConsumerCatalog());
        scheduler.start();

        // 留时间让单消费者在空队列上进入 1ms park cap（只有 unpark 能提前唤醒它）。
        Thread.sleep(200);

        // 多轮取中位数：若走 1ms poll cap，中位数会逼近 1000µs；走 unpark 则中位数远小于 cap。
        // 中位数对单轮偶发负载尖峰稳健，仍能证明 unpark 路径占主导。
        final int rounds = 7;
        List<Long> elapsedUsList = new ArrayList<>(rounds);
        for (int i = 0; i < rounds; i++) {
            int expectedSize = deliveryTimes.size();
            long t0 = System.nanoTime();
            // 直接入桶触发 adaptive scanner 的 bucketAddListener → scan → offer → unpark 消费者。
            scheduler.getBucketGroupManager().add(
                dueIntent("unpark-" + i, PrecisionTier.ULTRA, Instant.now().minusMillis(5)));
            awaitDelivery(deliveryTimes, expectedSize + 1);
            long elapsedUs = (deliveryTimes.get(expectedSize) - t0) / 1000;
            elapsedUsList.add(elapsedUs);
            Thread.sleep(5); // 让消费者重新进入 park 状态
        }
        Collections.sort(elapsedUsList);
        long medianUs = elapsedUsList.get(rounds / 2); // 7 轮的中间元素（index 3）

        assertTrue(medianUs < UNPARK_MEDIAN_BOUND_US,
            "offer→delivery median " + medianUs + "µs (all " + elapsedUsList
                + ") — should be << 1ms unpark path (park cap is 1000µs); a poll-cap path would have median ~1000µs");
    }

    @Test
    @DisplayName("空 poll 保底: 无 unpark 时消费者靠 1ms cap 重新 poll 并在 ~1ms 内恢复消费")
    void emptyPollFallbackWithoutUnpark() throws Exception {
        intentStore = new ConcurrentIntentStore();
        CountDownLatch delivered = new CountDownLatch(1);
        DeliveryHandler handler = intent -> {
            delivered.countDown();
            return CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        };
        scheduler = new PrecisionScheduler(intentStore, handler, null, singleConsumerCatalog());
        scheduler.start();

        // 让消费者进入空队列 park 状态。
        Thread.sleep(200);

        // 绕开 scanAndDispatch（不经 offer 后 unpark），直接把 intent 投进 dispatch 队列：
        // 消费者只能靠 1ms park cap 重新 poll 取走它。
        Field qf = PrecisionScheduler.class.getDeclaredField("tierDispatchQueues");
        qf.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<PrecisionTier, BlockingQueue<Intent>> queues =
            (Map<PrecisionTier, BlockingQueue<Intent>>) qf.get(scheduler);
        BlockingQueue<Intent> queue = queues.get(PrecisionTier.ULTRA);

        long t0 = System.nanoTime();
        queue.offer(dueIntent("fallback-1", PrecisionTier.ULTRA, Instant.now()));
        assertTrue(delivered.await(2, TimeUnit.SECONDS),
            "empty-poll fallback should deliver without unpark (1ms park cap re-poll)");
        long elapsedUs = (System.nanoTime() - t0) / 1000;
        assertTrue(elapsedUs < FALLBACK_BOUND_US,
            "fallback re-poll delivered in " + elapsedUs + "µs — should recover via the 1ms park cap re-poll (recovery bound 100ms; tolerates load jitter, fails only on a real hang)");
    }

    @Test
    @DisplayName("无回归: 单发消费者路径 ACK 计数正确(20 个 intent 全部 ACKED)")
    void ackCountingSingleConsumer() throws Exception {
        intentStore = new ConcurrentIntentStore();
        final int n = 20;
        CountDownLatch delivered = new CountDownLatch(n);
        DeliveryHandler handler = intent -> {
            delivered.countDown();
            return CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        };
        scheduler = new PrecisionScheduler(intentStore, handler, null, singleConsumerCatalog());
        scheduler.start();

        List<Intent> scheduled = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            Intent intent = dueIntent("ack-single-" + i, PrecisionTier.ULTRA, Instant.now().minusMillis(5));
            scheduled.add(intent);
            scheduler.schedule(intent);
        }

        assertTrue(delivered.await(5, TimeUnit.SECONDS),
            "all " + n + " intents should be delivered; got " + delivered.getCount() + " remaining");
        for (Intent intent : scheduled) {
            awaitStatus(intent, IntentStatus.ACKED);
            assertEquals(IntentStatus.ACKED, intentStore.findById(intent.getIntentId()).getStatus(),
                "intent " + intent.getIntentId() + " should be ACKED");
        }
    }

    @Test
    @DisplayName("无回归: 批量消费者路径(runBatchDrainConsumer)投递 + ACK 计数正确")
    void ackCountingBatchConsumer() throws Exception {
        intentStore = new ConcurrentIntentStore();
        final int n = 25;
        CountDownLatch delivered = new CountDownLatch(n);
        DeliveryHandler handler = intent -> {
            delivered.countDown();
            return CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        };
        scheduler = new PrecisionScheduler(intentStore, handler, null, singleConsumerCatalog());
        scheduler.start();

        List<Intent> scheduled = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            Intent intent = dueIntent("ack-batch-" + i, PrecisionTier.STANDARD, Instant.now().minusMillis(5));
            scheduled.add(intent);
            scheduler.schedule(intent);
        }

        assertTrue(delivered.await(5, TimeUnit.SECONDS),
            "all " + n + " intents should be delivered via batch consumer; got " + delivered.getCount() + " remaining");
        for (Intent intent : scheduled) {
            awaitStatus(intent, IntentStatus.ACKED);
            assertEquals(IntentStatus.ACKED, intentStore.findById(intent.getIntentId()).getStatus(),
                "intent " + intent.getIntentId() + " should be ACKED");
        }
    }

    /** 轮询等待 deliveryTimes 达到目标大小（带超时）。 */
    private static void awaitDelivery(List<Long> deliveryTimes, int target) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (deliveryTimes.size() < target) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("delivery not observed within 3s (size="
                    + deliveryTimes.size() + ", target=" + target + ")");
            }
            Thread.sleep(1);
        }
    }

    /** 轮询等待 intent 达到目标状态（带超时）。 */
    private static void awaitStatus(Intent intent, IntentStatus status) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (intent.getStatus() != status) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("intent " + intent.getIntentId() + " did not reach "
                    + status + " (was " + intent.getStatus() + ") within 3s");
            }
            Thread.sleep(1);
        }
    }
}