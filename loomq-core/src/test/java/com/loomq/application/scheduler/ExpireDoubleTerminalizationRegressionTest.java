package com.loomq.application.scheduler;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.spi.DeliveryHandler;
import com.loomq.store.ConcurrentIntentStore;
import com.loomq.store.IntentStore;
import java.lang.reflect.Method;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

/**
 * R6: handleExpired 缺少终态守卫——consumer 过期闸门（投递前最后一道闸）与扫描线程的
 * checkExpiredIntents 可对同一 intent 并发调用 handleExpired：败者进入锁后从终态二次
 * transitionTo(EXPIRED) 抛 ISE。consumer 路径（runSingleIntentConsumer 964 行 /
 * runBatchDrainConsumer 1068 行）无 try/catch，ISE 直接杀死消费者 VT（consumerThreads
 * 是固定 Thread[]，无监督）→ 档位投递容量静默永久退化；扫描路径有 try/catch 吞掉，
 * 故败者归属决定后果。触发场景是确定性的（deadline 在入队后越过），败者归属是竞态。
 *
 * <p>回归测试：同一 intent 连续两次 handleExpired，第二次必须幂等跳过（不得抛 ISE）。
 */
class ExpireDoubleTerminalizationRegressionTest {

    @Test
    void secondHandleExpiredMustBeNoop() throws Exception {
        IntentStore store = new ConcurrentIntentStore();
        DeliveryHandler handler = intent -> CompletableFuture.completedFuture(DeliveryHandler.DeliveryResult.SUCCESS);
        PrecisionScheduler scheduler = new PrecisionScheduler(store, handler, null);
        scheduler.start();
        try {
            Intent intent = new Intent("r6-expire-guard-0001");
            intent.setExecuteAt(Instant.now().plusSeconds(300)); // 远期：扫描/投递不干扰
            intent.setDeadline(Instant.now().plusSeconds(600));
            store.save(intent);
            scheduler.schedule(intent);

            Method m = PrecisionScheduler.class.getDeclaredMethod("handleExpired", Intent.class);
            m.setAccessible(true);

            // 第一次终态化：EXPIRED 落内存（sink 为 null 时持久化跳过，语义不变）
            m.invoke(scheduler, intent);
            assertEquals(IntentStatus.EXPIRED, intent.getStatus());

            // 第二次（双路径竞态的败者）：必须幂等跳过，不得从终态二次 transitionTo 抛 ISE。
            // 修复前：transitionTo(EXPIRED) 从 EXPIRED → IllegalStateException 穿透调用栈。
            assertDoesNotThrow(() -> m.invoke(scheduler, intent));
            assertEquals(IntentStatus.EXPIRED, intent.getStatus());
        } finally {
            scheduler.stop();
        }
    }
}
