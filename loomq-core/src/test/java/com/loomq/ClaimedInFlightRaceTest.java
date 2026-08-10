package com.loomq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.loomq.domain.intent.AckMode;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.spi.DeliveryHandler;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * P1-2 认领窗口端到端测试：投递在途期间 fireNow/updateIntent 不得引发第二次投递。
 *
 * <p>确定性设计：等 GateHandler 被调用即意味着 scanDue 的 CAS 已消耗索引（认领完成、
 * 投递在途），此时再调 fireNow/updateIntent 必然走认领分支。之后手动完成投递 Future，
 * 跨过多个扫描周期断言投递次数恒为 1。若认领分支被改回"无条件持久化 + restore"，
 * 第二次 scanDue 会再次投递 → {@code handler.calls} 断言失败。</p>
 *
 * <p>注意：STANDARD 档批量模式经 {@link DeliveryHandler#deliverBatchAsync} 默认实现
 * 回落逐个 {@link DeliveryHandler#deliverAsync}，故只需实现单条 SPI。</p>
 *
 * <p>集成测试（启动完整引擎 + mmap 磁盘 I/O + 多扫描周期等待）：按仓库规范打
 * {@code @Tag("integration")}，默认/CI fast-tests 排除，经 {@code -Pintegration-tests}
 * 或 {@code -Pfull-tests} 运行（同 LoomqEngineRecoveryTest 约定）。</p>
 */
@Tag("integration")
class ClaimedInFlightRaceTest {

    private static final class GateHandler implements DeliveryHandler {
        final CountDownLatch invoked = new CountDownLatch(1);
        final CompletableFuture<DeliveryResult> outcome = new CompletableFuture<>();
        final AtomicInteger calls = new AtomicInteger();

        @Override
        public CompletableFuture<DeliveryResult> deliverAsync(Intent intent) {
            calls.incrementAndGet();
            invoked.countDown();
            return outcome;
        }
    }

    @Test
    void fireNowWhileClaimed_deliversExactlyOnce(@TempDir Path tmp) throws Exception {
        GateHandler handler = new GateHandler();
        try (LoomqEngine engine = LoomqEngine.builder()
                .dataDir(tmp).nodeId("race-firenow").deliveryHandler(handler).build()) {
            engine.start();

            Intent intent = new Intent();
            String id = intent.getIntentId();
            intent.setExecuteAt(Instant.now().plusMillis(100));
            intent.setPrecisionTier(PrecisionTier.STANDARD);
            engine.createIntent(intent, AckMode.DURABLE).get();

            // 等消费者真正开始投递：此刻 scanDue CAS 已消耗索引，认领窗口关闭
            assertTrue(handler.invoked.await(10, TimeUnit.SECONDS), "intent must be in-flight");

            // 在途期间 fireNow：认领分支，不得产生第二次投递
            assertTrue(engine.fireNow(id), "fireNow should succeed (in-flight serves as fire-now)");

            handler.outcome.complete(DeliveryHandler.DeliveryResult.SUCCESS);
            awaitTerminal(engine, id, Duration.ofSeconds(10));
            Thread.sleep(1500);  // 跨过至少 2 个 STANDARD 扫描周期

            assertEquals(1, handler.calls.get(), "claimed intent must be delivered exactly once");
        }
        // Windows：@TempDir 清理可能与 engine.close() 后的 mmap 段释放竞态；
        // 给 JVM 一个 GC 机会再返回，避免目录删除失败（同 ackedIntentSurvivesWithoutClose 先例）。
        System.gc();
        Thread.sleep(200);
    }

    @Test
    void updateIntentWhileClaimed_deliversExactlyOnce(@TempDir Path tmp) throws Exception {
        GateHandler handler = new GateHandler();
        try (LoomqEngine engine = LoomqEngine.builder()
                .dataDir(tmp).nodeId("race-update").deliveryHandler(handler).build()) {
            engine.start();

            Intent intent = new Intent();
            String id = intent.getIntentId();
            intent.setExecuteAt(Instant.now().plusMillis(100));
            intent.setPrecisionTier(PrecisionTier.STANDARD);
            engine.createIntent(intent, AckMode.DURABLE).get();

            assertTrue(handler.invoked.await(10, TimeUnit.SECONDS), "intent must be in-flight");

            // 在途期间 updateIntent：newExecuteAt != old，但已认领 -> 不重排，不得二次投递
            Optional<Intent> updated = engine.updateIntent(id, i -> { /* no-op updater */ },
                Instant.now().plusSeconds(60));
            assertTrue(updated.isPresent(), "update accepted (claimed path)");

            handler.outcome.complete(DeliveryHandler.DeliveryResult.SUCCESS);
            awaitTerminal(engine, id, Duration.ofSeconds(10));
            Thread.sleep(1500);

            assertEquals(1, handler.calls.get(), "claimed intent must be delivered exactly once");
        }
        System.gc();
        Thread.sleep(200);
    }

    private void awaitTerminal(LoomqEngine engine, String id, Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            Optional<Intent> cur = engine.getIntent(id);
            if (cur.isPresent() && cur.get().getStatus().isTerminal()) {
                return;
            }
            Thread.sleep(20);
        }
        fail("intent " + id + " did not reach terminal state within " + timeout);
    }
}
