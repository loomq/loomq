package com.loomq.application.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.domain.intent.RedeliveryPolicy;
import com.loomq.spi.DeliveryHandler;
import com.loomq.spi.DeliveryHandler.DeliveryResult;
import com.loomq.store.ConcurrentIntentStore;
import com.loomq.store.IntentStore;
import com.loomq.testutil.TestIntents;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * R19: 在途投递期间 updateIntent(newExecuteAt) 的改期被重试路径静默覆写。
 *
 * <p>复现链：scanDue 已 CAS 认领 Intent（索引条目已消耗）后，updateIntent 的
 * removeFromSchedule 返回 false → 不重排程，但 updater/executeAt 仍会应用到共享活对象
 * 并 DURABLE 持久化（updateIntent 契约："更新在重试路径…确定生效"）。首轮投递失败进入
 * 重试分支时，旧代码用 backoff（now + delayMs）覆写 executeAt —— 用户显式改期被丢弃，
 * Intent 在数秒内被重投，与用户的"延迟到 +30s"意图相悖。</p>
 *
 * <p>修复：重试分支发现 executeAt 显著晚于当前（超过一档精度窗口，覆盖桶粒度
 * ≤window 的前沿偏差——未被改期的在途 Intent 不可能超出该偏差）时，尊重用户改期，
 * 按新 executeAt 重排程而非套用 backoff。</p>
 */
@Tag("slow")
class BugMidFlightRescheduleClobberedByRetryTest {

    /** 固定 1s 无抖动退避：若无修复，失败后 ~1s 内就会重投，便于断言区分。 */
    private static final RedeliveryPolicy FIXED_1S =
        new RedeliveryPolicy(5, "fixed", 1000, 1000, 1.0, false);

    /** 首次投递：阻塞至测试线程改期完成，然后返回 RETRY（结果路径）。 */
    private static DeliveryHandler blockingFirstThenRetry(AtomicInteger deliveries,
                                                          CountDownLatch firstStarted,
                                                          CountDownLatch releaseFirst) {
        return intent -> {
            int n = deliveries.incrementAndGet();
            if (n == 1) {
                firstStarted.countDown();
                try {
                    releaseFirst.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return CompletableFuture.completedFuture(DeliveryResult.RETRY);
            }
            return CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        };
    }

    /** 首次投递：阻塞至测试线程改期完成，然后以异常结束（异常路径）。 */
    private static DeliveryHandler blockingFirstThenThrow(AtomicInteger deliveries,
                                                          CountDownLatch firstStarted,
                                                          CountDownLatch releaseFirst) {
        return intent -> {
            int n = deliveries.incrementAndGet();
            if (n == 1) {
                firstStarted.countDown();
                try {
                    releaseFirst.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                CompletableFuture<DeliveryResult> future = new CompletableFuture<>();
                future.completeExceptionally(new RuntimeException("transient failure"));
                return future;
            }
            return CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        };
    }

    @Test
    void midFlightRescheduleMustBeHonoredOnRetryResult() throws Exception {
        IntentStore store = new ConcurrentIntentStore();
        AtomicInteger deliveries = new AtomicInteger();
        CountDownLatch firstStarted = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        PrecisionScheduler scheduler = new PrecisionScheduler(
            store, blockingFirstThenRetry(deliveries, firstStarted, releaseFirst), null);
        scheduler.start();
        try {
            Intent intent = TestIntents.due("r19-midflight-retry-0001", PrecisionTier.STANDARD, Instant.now().minusMillis(50));
            intent.setRedelivery(FIXED_1S);
            store.save(intent);
            scheduler.schedule(intent);

            assertTrue(firstStarted.await(5, TimeUnit.SECONDS), "first attempt must start");
            Instant rescheduledTo = Instant.now().plusSeconds(30);
            try {
                // 镜像 updateIntent 在锁内的语义：claimed 后不重排程，但 executeAt
                // 仍被改为用户新值并持久化（共享活对象，重试结算读的就是它）。
                synchronized (intent) {
                    intent.setExecuteAt(rescheduledTo);
                }
            } finally {
                releaseFirst.countDown(); // 首轮投递返回 RETRY → 重试结算
            }

            // 修复前：backoff(1s)覆写 executeAt → ~1s 内第二次投递（attempt 2 成功 → ACKED），
            // 用户改期静默丢失。修复后：尊重改期，重排程到 +30s，backoff 窗口内无重投。
            Thread.sleep(2500);
            assertEquals(1, deliveries.get(),
                "mid-flight reschedule must be honored: no backoff redelivery within the retry window");
            Intent cur = store.findById("r19-midflight-retry-0001");
            assertTrue(cur.getExecuteAt().isAfter(Instant.now().plusSeconds(20)),
                "executeAt must keep the user's rescheduled time, was " + cur.getExecuteAt());
            assertEquals(IntentStatus.SCHEDULED, cur.getStatus(),
                "intent must stay scheduled at the user's new time, not be ACKED by an early retry");
        } finally {
            scheduler.stop();
        }
    }

    @Test
    void midFlightRescheduleMustBeHonoredOnDeliveryException() throws Exception {
        IntentStore store = new ConcurrentIntentStore();
        AtomicInteger deliveries = new AtomicInteger();
        CountDownLatch firstStarted = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        PrecisionScheduler scheduler = new PrecisionScheduler(
            store, blockingFirstThenThrow(deliveries, firstStarted, releaseFirst), null);
        scheduler.start();
        try {
            Intent intent = TestIntents.due("r19-midflight-ex-0001", PrecisionTier.STANDARD, Instant.now().minusMillis(50));
            intent.setRedelivery(FIXED_1S);
            store.save(intent);
            scheduler.schedule(intent);

            assertTrue(firstStarted.await(5, TimeUnit.SECONDS), "first attempt must start");
            Instant rescheduledTo = Instant.now().plusSeconds(30);
            try {
                synchronized (intent) {
                    intent.setExecuteAt(rescheduledTo);
                }
            } finally {
                releaseFirst.countDown(); // 首轮投递异常结束 → handleDeliveryFailure 重试分支
            }

            Thread.sleep(2500);
            assertEquals(1, deliveries.get(),
                "mid-flight reschedule must be honored on the exception path too");
            Intent cur = store.findById("r19-midflight-ex-0001");
            assertTrue(cur.getExecuteAt().isAfter(Instant.now().plusSeconds(20)),
                "executeAt must keep the user's rescheduled time, was " + cur.getExecuteAt());
            assertEquals(IntentStatus.SCHEDULED, cur.getStatus());
        } finally {
            scheduler.stop();
        }
    }
}
