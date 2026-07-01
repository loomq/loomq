package com.loomq.infrastructure.wheel;

import static org.junit.jupiter.api.Assertions.*;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import java.nio.file.Path;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class GroupCommitBarrierTest {
    @TempDir Path tmp;

    @Test
    void awaitCommitBlocksUntilForce() throws Exception {
        AtomicLong clock = new AtomicLong(Instant.parse("2026-06-30T00:00:00Z").toEpochMilli());
        WheelConfig cfg = new WheelConfig(tmp.toString(), "t", 30, 16, 1, null);
        try (WheelStore store = new WheelStore(cfg, clock::get);
             TailIndex tail = new TailIndex(tmp, clock::get);
             GroupCommitBarrier barrier = new GroupCommitBarrier(store, tail, 5)) {
            Intent it = new Intent("intent_bar0000000001");
            it.setExecuteAt(Instant.ofEpochMilli(clock.get() + 1_000));
            it.transitionTo(IntentStatus.SCHEDULED);
            store.put(it);

            long genBefore = barrier.currentGeneration();
            barrier.start();
            long gen = barrier.awaitCommit();
            assertTrue(gen > genBefore);
        }
    }

    /**
     * B2:close() 必须执行终态 force + 发布 flushedTicket,使在途 DURABLE 写者排空(返回成功)。
     * 否则 close() 停止循环却不推进 frontier → 写者只能经 5s 超时退出 → createIntent 抛 FAILED
     * 并仅回滚内存,而 wheelStore.close() 其后仍把槽位刷盘 → 重启 recovery 恢复+调度 →
     * 调用方以为"创建失败"的 Intent 被 ghost 投递。
     *
     * <p>用 60s 长间隔确保循环在测试期间不会自然 force,故排空只能来自 close()。</p>
     */
    @Test
    void closeDrainsInFlightAwaitCommit() throws Exception {
        AtomicLong clock = new AtomicLong(Instant.parse("2026-06-30T00:00:00Z").toEpochMilli());
        WheelConfig cfg = new WheelConfig(tmp.toString(), "t", 30, 16, 60_000L, null);
        try (WheelStore store = new WheelStore(cfg, clock::get);
             TailIndex tail = new TailIndex(tmp, clock::get);
             GroupCommitBarrier barrier = new GroupCommitBarrier(store, tail, 60_000)) {
            barrier.start();
            // 让循环跑完首次(no-op,writeTicket=0)force 迭代并 park 60s,使测试期间不再自然 force
            // —— 排空只能来自 close()。
            Thread.sleep(200);

            Intent it = new Intent("intent_drn00000001");
            it.setExecuteAt(Instant.ofEpochMilli(clock.get() + 1_000));
            it.transitionTo(IntentStatus.SCHEDULED);
            store.put(it);                       // 字节进 mmap,尚未 force

            // 在途 DURABLE 写者:领取 ticket=1,阻塞在 awaitCommit(循环已 park 60s)。
            CompletableFuture<Long> writer = CompletableFuture.supplyAsync(barrier::awaitCommit);
            // 确保写者已领取 ticket 并 park,先于 close() 快照 writeTicket。
            Thread.sleep(200);
            assertFalse(writer.isDone(), "writer should still be blocked (loop interval is 60s)");

            // close() 必须排空:终态 force 覆盖在途写入 + 发布 flushedTicket=1。
            barrier.close();

            // 修复后写者立即返回(被排空);未修复则写者阻塞到 5s 超时,get(2s) 抛
            // TimeoutException → 测试失败(捕获 B2)。
            assertEquals(1L, writer.get(2, TimeUnit.SECONDS),
                "in-flight awaitCommit must be drained by close(), not time out against a dead frontier");
            assertFalse(writer.isCompletedExceptionally(),
                "drained writer must return success, not throw FAILED/timeout");
        }
    }
}
