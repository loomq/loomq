package com.loomq.application.command;

import static org.junit.jupiter.api.Assertions.*;

import com.loomq.domain.intent.Intent;
import java.nio.file.Path;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CancelColdConcurrencyTest {
    @TempDir Path tmp;

    /**
     * H2:同一冷 Intent 并发取消必须串行化 —— 第二个取消者重读槽已是 CANCELED(terminal),
     * transitionTo 抛 ISE 返回 false,不 double-write、不 double 计数。
     */
    @Test
    void concurrentColdCancelDoesNotDoubleWrite() throws Exception {
        AtomicLong clock = new AtomicLong(Instant.parse("2026-06-30T00:00:00Z").toEpochMilli());
        try (CommandStackFx fx = new CommandStackFx(tmp,
                new CommandStackFx.Options(clock::get, null, false, true))) {

            // 冷 Intent:executeAt 远超 60min → 落 wheel(非 tail),不在内存 store
            Intent cold = fx.plantColdWheel("intent_cld00000001", null);

            long beforeCancelled = fx.mc().getIntentsCancelledTotal();

            int n = 8;
            CountDownLatch ready = new CountDownLatch(n);
            CountDownLatch fire = new CountDownLatch(1);
            AtomicInteger successes = new AtomicInteger();
            AtomicInteger failures = new AtomicInteger();
            try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
                for (int i = 0; i < n; i++) {
                    pool.submit(() -> {
                        ready.countDown();
                        try { fire.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); return; }
                        if (fx.svc().cancelIntent(cold.getIntentId())) successes.incrementAndGet();
                        else failures.incrementAndGet();
                    });
                }
                ready.await();
                fire.countDown();
                pool.shutdown();
                assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
            }

            assertEquals(1, successes.get(), "exactly one concurrent cold cancel must succeed");
            assertEquals(n - 1, failures.get(), "others must see terminal state and return false");
            assertEquals(1L, fx.mc().getIntentsCancelledTotal() - beforeCancelled,
                "cancel counter must increment exactly once (no double-count)");
        }
    }

    /**
     * H2 tail 路径:超出 day 视界(>30d)的冷 Intent 落 TailIndex,并发取消必须恰好一个成功。
     * TailIndex.remove 返回 false 表示已并发取消者移除 → 第二取消者须返回 false,不 double 计数。
     * (appendLock 仅串行单次 append,不串行 remove→awaitCommit→metric++ 序列,故须布尔门控。)
     */
    @Test
    void concurrentTailColdCancelDoesNotDoubleCount() throws Exception {
        AtomicLong clock = new AtomicLong(Instant.parse("2026-06-30T00:00:00Z").toEpochMilli());
        try (CommandStackFx fx = new CommandStackFx(tmp,
                new CommandStackFx.Options(clock::get, null, false, true))) {

            // 远期冷 Intent:executeAt > 30d 视界 → 落 tail(非 wheel),不在内存 store。
            Intent cold = fx.plantColdTail("intent_ctl00000001", null);

            long beforeCancelled = fx.mc().getIntentsCancelledTotal();

            int n = 8;
            CountDownLatch ready = new CountDownLatch(n);
            CountDownLatch fire = new CountDownLatch(1);
            AtomicInteger successes = new AtomicInteger();
            AtomicInteger failures = new AtomicInteger();
            try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
                for (int i = 0; i < n; i++) {
                    pool.submit(() -> {
                        ready.countDown();
                        try { fire.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); return; }
                        if (fx.svc().cancelIntent(cold.getIntentId())) successes.incrementAndGet();
                        else failures.incrementAndGet();
                    });
                }
                ready.await();
                fire.countDown();
                pool.shutdown();
                assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
            }

            assertEquals(1, successes.get(), "exactly one concurrent tail cold cancel must succeed");
            assertEquals(n - 1, failures.get(),
                "others' tailIndex.remove must return false (already cancelled) → return false");
            assertEquals(1L, fx.mc().getIntentsCancelledTotal() - beforeCancelled,
                "cancel counter must increment exactly once on tail path (no double-count)");
        }
    }
}
