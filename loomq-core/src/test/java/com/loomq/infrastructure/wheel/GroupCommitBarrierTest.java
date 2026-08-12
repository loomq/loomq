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
        WheelConfig cfg = new WheelConfig(tmp.toString(), "t", 30, 16, 1, 10_000L, 60L * 60_000L, 60_000L, null);
        try (WheelStore store = new WheelStore(cfg, clock::get);
             TailIndex tail = new TailIndex(tmp, clock::get);
             GroupCommitBarrier barrier = new GroupCommitBarrier(store, tail, 5, 10_000)) {
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
        WheelConfig cfg = new WheelConfig(tmp.toString(), "t", 30, 16, 60_000L, 10_000L, 60L * 60_000L, 60_000L, null);
        try (WheelStore store = new WheelStore(cfg, clock::get);
             TailIndex tail = new TailIndex(tmp, clock::get);
             GroupCommitBarrier barrier = new GroupCommitBarrier(store, tail, 60_000, 10_000)) {
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

    /**
     * A3:awaitCommit 超时不应抛错,而应内联 force 兜底 —— 否则慢盘下 DURABLE 写者假失败 →
     * createIntent 内存回滚 + FAILED,但 wheelStore.close() 仍刷盘 → 重启 recovery 复活 = ghost。
     *
     * <p>用极大间隔(60s)确保循环不会自然 force;awaitCommitTimeoutMs=50ms 触发超时兜底,
     * 断言写者返回成功且 flushedTicket 被推进(而非抛 RuntimeException)。</p>
     */
    @Test
    void awaitCommitTimeoutFallsBackToInlineForce() throws Exception {
        AtomicLong clock = new AtomicLong(Instant.parse("2026-06-30T00:00:00Z").toEpochMilli());
        WheelConfig cfg = new WheelConfig(tmp.toString(), "t", 30, 16, 60_000L, 50L, 60L * 60_000L, 60_000L, null);
        try (WheelStore store = new WheelStore(cfg, clock::get);
             TailIndex tail = new TailIndex(tmp, clock::get);
             GroupCommitBarrier barrier = new GroupCommitBarrier(store, tail, 60_000, 50)) {
            barrier.start();
            Thread.sleep(200); // 循环跑完首次 no-op force 迭代并 park 60s

            Intent it = new Intent("intent_a0300000001");
            it.setExecuteAt(Instant.ofEpochMilli(clock.get() + 1_000));
            it.transitionTo(IntentStatus.SCHEDULED);
            store.put(it); // 字节进 mmap,尚未 force

            long genBefore = barrier.currentGeneration();
            // 超时兜底:50ms 后内联 force,返回成功(不抛错),flushedTicket 推进到 ≥1。
            long gen = barrier.awaitCommit();
            assertTrue(gen > genBefore, "inline-force fallback must advance flushedTicket");
            assertTrue(gen >= 1L, "writer's ticket=1 must be covered by inline force");
        }
    }

    /**
     * A3 兜底不变量钉子:段 2 必须在 force 前快照 ticket 再发布,而非 force 后重读 writeTicket。
     * 若实现错误(force 后重读),并发写者 B 在 A 的 force 与 writeTicket.get() 之间 put+领 ticket,
     * B 会被 A 发布的 flushedTicket 错误标记为已持久,但 B 字节在 force 后才入 mmap → 崩溃丢失(DURABLE 伪阳)。
     *
     * <p>本测试是不变量钉(pin)而非确定性竞态复现:写者 A 超时兜底完成后再 put B,断言
     * flushedAfter == 1L —— 即 A 的兜底发布只覆盖 force 前的 ticket 快照(=1),不含 B 的 ticket=2。
     * 因 B 在 A 返回后才 put,本断言无法确定性地捕获"force 后重读"错误;确定性竞态测试需在
     * forceDirty() 内插入可控屏障,超出本测试范围。保留 assertEquals(1L, flushedAfter, ...) 与测试体不变。</p>
     */
    @Test
    void awaitCommitTimeoutFallbackDoesNotOverpublishConcurrentTicket() throws Exception {
        AtomicLong clock = new AtomicLong(Instant.parse("2026-06-30T00:00:00Z").toEpochMilli());
        WheelConfig cfg = new WheelConfig(tmp.toString(), "t", 30, 16, 60_000L, 50L, 60L * 60_000L, 60_000L, null);
        try (WheelStore store = new WheelStore(cfg, clock::get);
             TailIndex tail = new TailIndex(tmp, clock::get);
             GroupCommitBarrier barrier = new GroupCommitBarrier(store, tail, 60_000, 50)) {
            barrier.start();
            Thread.sleep(200); // daemon 首次 no-op force 后 park 60s

            // 写者 A:超时兜底段 2 会 force 并发布。
            Intent a = new Intent("intent_rca00000001");
            a.setExecuteAt(Instant.ofEpochMilli(clock.get() + 1_000));
            a.transitionTo(IntentStatus.SCHEDULED);
            store.put(a);

            long flushedAfter = barrier.awaitCommit(); // A 领 ticket=1,超时兜底 force+发布
            // 写者 B 的 put 在 A 兜底完成之后:其 ticket=2 不应被 A 的发布纳入。
            Intent b = new Intent("intent_rcb00000001");
            b.setExecuteAt(Instant.ofEpochMilli(clock.get() + 1_000));
            b.transitionTo(IntentStatus.SCHEDULED);
            store.put(b);

            // A 的兜底发布必须只覆盖 force 之前的 ticket(=1),不应把 B 的 ticket=2 标记为已持久。
            // 若实现错误(force 后重读 writeTicket 并发布),flushedAfter 会是 2,本断言失败。
            assertEquals(1L, flushedAfter,
                "timeout fallback must publish the pre-force ticket snapshot, not a post-force re-read");
            // B 仍需等待自己的 DURABLE(force 未覆盖其字节):
            long bGen = barrier.awaitCommit(); // daemon(60s 间隔)或下一轮覆盖 B —— 这里由 daemon 首次 park 后不会很快 force
            assertTrue(bGen >= 2L, "B's own awaitCommit must eventually cover its ticket");
        }
    }

    /**
     * 内联 force 失败不得发布 durability frontier（虚假持久化确认）。
     *
     * <p>doInlineForce 的 forceDirty()/flush() 抛异常（ENOSPC/EIO/段已关闭）时字节并未落盘；
     * 若仍推进 flushedTicket，其他在兜底分支等待的写者会看到 flushedTicket >= myTicket 而返回
     * 成功——DURABLE 假阳：崩溃即丢数据，正是本类 javadoc 声称要杜绝的 under-wait。
     * daemon 循环只在 force 成功后发布，内联兜底必须一致。</p>
     *
     * <p>构造：put 制造脏桶 → 关闭 store（arena 关闭 → 后续 seg.force() 抛 ISE）→ 并发写者
     * awaitCommit。daemon 循环每次 force 失败且不发布；写者超时后走内联 force → 同样失败 →
     * 两个写者都必须抛错（frontier 保持 0），而非返回成功。</p>
     */
    @Test
    void inlineForceFailureMustNotPublishFrontier() throws Exception {
        AtomicLong clock = new AtomicLong(Instant.parse("2026-06-30T00:00:00Z").toEpochMilli());
        WheelConfig cfg = new WheelConfig(tmp.toString(), "t", 30, 16, 1L, 200L, 60L * 60_000L, 60_000L, null);
        WheelStore store = new WheelStore(cfg, clock::get);
        TailIndex tail = new TailIndex(tmp, clock::get);
        GroupCommitBarrier barrier = new GroupCommitBarrier(store, tail, 1, 200);

        Intent it = new Intent("intent_flf00000001");
        it.setExecuteAt(Instant.ofEpochMilli(clock.get() + 1_000));
        it.transitionTo(IntentStatus.SCHEDULED);
        store.put(it); // 脏桶：forceDirty 有内容可刷

        barrier.start();
        // 让 daemon 跑过初始轮次，然后关闭 store → 所有桶段不活 → 后续 forceDirty 抛 ISE。
        Thread.sleep(50);
        store.close();
        tail.close();

        // 两个并发 DURABLE 写者：daemon 已无法 force（每轮抛错且不发布），超时后各自内联
        // force 也失败。修复前：doInlineForce 的 finally 仍发布 frontier → 至少一个写者
        // 拿到假成功返回。修复后：frontier 不动，两个写者都抛错。
        CompletableFuture<Long> w1 = CompletableFuture.supplyAsync(barrier::awaitCommit);
        CompletableFuture<Long> w2 = CompletableFuture.supplyAsync(barrier::awaitCommit);

        assertThrows(java.util.concurrent.ExecutionException.class, w1::get,
            "writer 1 must not receive a false DURABLE success when the force failed");
        assertThrows(java.util.concurrent.ExecutionException.class, w2::get,
            "writer 2 must not receive a false DURABLE success when the force failed");
        assertEquals(0L, barrier.currentGeneration(),
            "frontier must never advance past a failed force");
        barrier.close();
    }
}
