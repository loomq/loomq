package com.loomq.application.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.loomq.common.MetricsCollector;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.spi.DeliveryHandler;
import com.loomq.spi.DeliveryHandler.DeliveryResult;
import com.loomq.store.ConcurrentIntentStore;
import com.loomq.store.IntentStore;
import com.loomq.tracing.IntentTraceStore;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import org.junit.jupiter.api.Test;

/**
 * R6: updateIntent 的 updater 无状态白名单校验，可把 intent 置为结算链中间态（DUE）。
 * 此后投递成功进入 finalizeIntent：终态守卫放行（DUE 非终态），但 transitionTo(DUE)
 * 从 DUE 起步抛 ISE → 被 runFinalizeTask 吞掉 → ACK 未落盘、onDelivered 丢失、intent
 * 卡死 DUE（索引已被认领消耗，不再有任何路径投递）；重启按磁盘旧 SCHEDULED 槽重复投递。
 * handleDeliveryFailure 的 DUE→DISPATCHING 前奏同款问题。
 *
 * <p>回归测试：内存态为 DUE 的 intent（updater 置位后的等价状态）投递 SUCCESS 后
 * 必须沿结算链走到 ACKED，不得抛异常被吞。
 */
class FinalizeDueStateRegressionTest {

    @Test
    void dueStateMustSettleToAcked() throws Exception {
        IntentStore store = new ConcurrentIntentStore();
        AtomicInteger deliveries = new AtomicInteger();
        DeliveryHandler handler = intent -> {
            deliveries.incrementAndGet();
            return CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        };
        MetricsCollector metrics = new MetricsCollector();
        PrecisionScheduler scheduler = new PrecisionScheduler(store, handler, null, null, metrics, new IntentTraceStore());
        scheduler.start();
        try {
            Intent intent = new Intent("r6-due-settle-0001");
            intent.setExecuteAt(Instant.now().plusMillis(50));
            store.save(intent);
            scheduler.schedule(intent); // SCHEDULED → 入桶
            // updateIntent updater 的等效副作用：状态被置为 DUE（命令层不拦截）
            intent.transitionTo(IntentStatus.DUE);
            store.update(intent);

            IntentStatus st = null;
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (System.nanoTime() < deadline) {
                Intent cur = store.findById("r6-due-settle-0001");
                st = cur != null ? cur.getStatus() : null;
                if (st == IntentStatus.ACKED) break;
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(5));
            }
            assertEquals(IntentStatus.ACKED, st,
                "DUE-status intent must settle through the dispatch chain to ACKED, not crash the settlement");
            assertEquals(1, deliveries.get(), "exactly one delivery");
            assertEquals(0, metrics.getFinalizeTaskExceptionsTotal(),
                "no swallowed finalize exceptions (pre-fix: ISE from DUE->DUE transition)");
        } finally {
            scheduler.stop();
        }
    }
}
