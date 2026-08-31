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
 * R19 边界:在途改期落在"一档精度窗口内"(now, now+window] 时同样必须被尊重。
 *
 * <p>scheduleRetryOrHonorReschedule 的 honor 判定阈值是 {@code executeAt.isAfter(now + window)};
 * 未被改期的在途 Intent executeAt 恒 ≤ now(scanDue 只摘到期条目),"将来时刻"本身就等价于
 * "用户改过期"——窗口只是制造死区:改期到 (now, now+window] 的 Intent 被 backoff 以更高
 * revision 覆写,用户显式 DURABLE 改期持久性丢失(重启按 max-revision 取 backoff 时刻)。</p>
 *
 * <p>修复:阈值收紧为 {@code isAfter(now)}。本测试断言窗口内改期(now+100ms,STANDARD
 * 500ms 窗口内)在重试结算后不被 backoff 覆写,并按用户时刻投递。</p>
 */
@Tag("slow")
class BugMidFlightRescheduleWithinWindowTest {

    /** 固定 1s 无抖动退避:若无修复,结算后 executeAt 被改写为 now+1s(可区分)。 */
    private static final RedeliveryPolicy FIXED_1S =
        new RedeliveryPolicy(5, "fixed", 1000, 1000, 1.0, false);

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

    /** 窗口内改期(now+100ms,< STANDARD 500ms 窗口)在 RETRY 结果路径必须被尊重。 */
    @Test
    void withinWindowRescheduleMustBeHonoredOnRetryResult() throws Exception {
        assertWithinWindowHonored("r19-within-retry-0001",
            BugMidFlightRescheduleWithinWindowTest::blockingFirstThenRetry);
    }

    /** 窗口内改期在投递异常路径必须被尊重。 */
    @Test
    void withinWindowRescheduleMustBeHonoredOnDeliveryException() throws Exception {
        assertWithinWindowHonored("r19-within-ex-0001",
            BugMidFlightRescheduleWithinWindowTest::blockingFirstThenThrow);
    }

    private void assertWithinWindowHonored(String id, HandlerFactory factory) throws Exception {
        IntentStore store = new ConcurrentIntentStore();
        AtomicInteger deliveries = new AtomicInteger();
        CountDownLatch firstStarted = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        PrecisionScheduler scheduler = new PrecisionScheduler(
            store, factory.build(deliveries, firstStarted, releaseFirst), null);
        scheduler.start();
        try {
            Intent intent = TestIntents.due(id, PrecisionTier.STANDARD, Instant.now().minusMillis(50));
            intent.setRedelivery(FIXED_1S);
            store.save(intent);
            scheduler.schedule(intent);

            assertTrue(firstStarted.await(5, TimeUnit.SECONDS), "first attempt must start");
            Instant rescheduledTo = Instant.now().plusMillis(100); // STANDARD 500ms 窗口内
            try {
                // 镜像 updateIntent 在锁内的语义:claimed 后不重排程,executeAt 仍被应用并持久化。
                synchronized (intent) {
                    intent.setExecuteAt(rescheduledTo);
                }
            } finally {
                releaseFirst.countDown(); // 首轮投递结束 → 重试结算
            }

            // 等待结算完成:要么按用户时刻重投成功(deliveries==2),要么 executeAt 被 backoff
            // 改写为 now+1s(修复前)——后者立即暴露,不必等 backoff 投递。
            long deadline = System.currentTimeMillis() + 3000;
            boolean deliveredAtRescheduled = false;
            while (System.currentTimeMillis() < deadline) {
                if (deliveries.get() >= 2) {
                    deliveredAtRescheduled = true;
                    break;
                }
                Intent cur = store.findById(id);
                if (cur != null && cur.getExecuteAt() != null
                        && !cur.getExecuteAt().equals(rescheduledTo)
                        && cur.getExecuteAt().isAfter(Instant.now().minusSeconds(1))) {
                    break; // executeAt 被改写 → backoff 分支(修复前行为)
                }
                Thread.sleep(20);
            }
            assertTrue(deliveredAtRescheduled,
                "within-window reschedule must be honored: delivery at user's time, not overwritten by backoff");
            Intent cur = store.findById(id);
            assertEquals(IntentStatus.ACKED, cur.getStatus(),
                "second delivery must succeed and ack at the user's rescheduled time");
        } finally {
            scheduler.stop();
        }
    }

    private interface HandlerFactory {
        DeliveryHandler build(AtomicInteger deliveries, CountDownLatch firstStarted, CountDownLatch releaseFirst);
    }
}
