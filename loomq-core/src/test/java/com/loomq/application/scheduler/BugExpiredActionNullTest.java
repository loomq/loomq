package com.loomq.application.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.loomq.LoomqEngine;
import com.loomq.domain.intent.AckMode;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.spi.DeliveryHandler;
import com.loomq.spi.DeliveryHandler.DeliveryResult;
import com.loomq.store.ConcurrentIntentStore;
import com.loomq.store.IntentStore;
import java.nio.file.Path;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * R18: {@code setExpiredAction(null)} 使 handleExpired 的 switch 抛 NPE——consumer 路径无
 * try/catch,NPE 杀死消费者 VT(consumerThreads 固定数组无监督),该档投递容量永久退化;
 * scanner 路径吞掉 NPE 反复重试,intent 永不终态化(日志风暴)。Intent.setExpiredAction 无
 * null 守卫,IntentValidator 也不校验——null 是可达的用户输入。
 *
 * <p>回归测试:(1) null expiredAction 的过期 intent 必须按默认 DISCARD 终态化为 EXPIRED;
 * (2) 消费者存活,后续 intent 仍能投递;创建入口拒绝 null expiredAction。</p>
 */
class BugExpiredActionNullTest {

    @Test
    void nullExpiredActionMustTerminalizeAsDiscardAndKeepConsumerAlive() throws Exception {
        IntentStore store = new ConcurrentIntentStore();
        AtomicInteger deliveries = new AtomicInteger();
        DeliveryHandler handler = intent -> {
            deliveries.incrementAndGet();
            return CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        };
        PrecisionScheduler scheduler = new PrecisionScheduler(store, handler, null);
        scheduler.start();
        try {
            // 已过期的 intent,expiredAction 为 null(用户 setter 可达状态)
            Intent intent = new Intent("r18-null-action-0001");
            intent.setExecuteAt(Instant.now().minusMillis(100));
            intent.setDeadline(Instant.now().minusMillis(50));
            intent.setExpiredAction(null);
            store.save(intent);
            scheduler.schedule(intent);

            // 修复前:NPE 使 intent 永不终态化(consumer 死 / scanner 吞异常重试)→ 轮询超时
            IntentStatus st = null;
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
            while (System.nanoTime() < deadline) {
                Intent cur = store.findById("r18-null-action-0001");
                st = cur != null ? cur.getStatus() : null;
                if (st == IntentStatus.EXPIRED) break;
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
            }
            assertEquals(IntentStatus.EXPIRED, st,
                "null expiredAction must default to DISCARD -> EXPIRED (not NPE)");

            // 消费者必须存活:后续 intent 仍能投递(修复前 consumer 线程已死)
            Intent second = new Intent("r18-null-action-0002");
            second.setExecuteAt(Instant.now().minusMillis(50));
            second.setPrecisionTier(PrecisionTier.ULTRA);
            store.save(second);
            scheduler.schedule(second);
            long d2 = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
            while (deliveries.get() == 0 && System.nanoTime() < d2) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
            }
            assertEquals(1, deliveries.get(),
                "consumer must survive the null-expiredAction intent and still deliver");
        } finally {
            scheduler.stop();
        }
    }

    @Test
    void createIntentMustRejectNullExpiredAction(@TempDir Path tmp) throws Exception {
        try (LoomqEngine engine = LoomqEngine.builder().dataDir(tmp).nodeId("null-action").build()) {
            engine.start();
            Intent intent = new Intent("r18-null-create-001");
            intent.setExecuteAt(Instant.now().plusSeconds(300));
            intent.setExpiredAction(null);
            assertThrows(RuntimeException.class,
                () -> engine.createIntent(intent, AckMode.ASYNC).join(),
                "createIntent must reject null expiredAction");
        }
    }
}
