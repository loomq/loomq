package com.loomq.infrastructure.wheel;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.testutil.TestWheelConfigs;
import java.nio.file.Path;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * R21: close() 关闭 inline force 执行器后,段 2 超时兜底的
 * {@code inlineForceExecutor.submit(...).get()} 抛 RejectedExecutionException(未被
 * ExecutionException/InterruptedException 捕获)——DURABLE 写者的字节已入 mmap,调用方
 * 却按失败回滚(createIntent 补偿 CANCELED 同样落不了盘),重启后 SCHEDULED 槽复活 =
 * 幽灵投递。正是段 2 兜底设计要防的窗口,在 shutdown 交错时重开。
 *
 * <p>修复:RejectedExecutionException → 调用线程同步内联 force(shutdown 期间接受短暂
 * pin,正确性优先——force 成功即发布 frontier,写者按成功返回,无幽灵窗口)。</p>
 */
class BugBarrierRejectedExecutionOnShutdownTest {

    @TempDir
    Path tmp;

    @Test
    void awaitCommitMustSurviveExecutorShutdown() throws Exception {
        AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        WheelConfig cfg = TestWheelConfigs.defaults(tmp);
        try (WheelStore store = new WheelStore(cfg, clock::get);
             TailIndex tail = new TailIndex(tmp, clock::get)) {
            GroupCommitBarrier barrier = new GroupCommitBarrier(store, tail, 1000, 100); // 100ms 超时
            barrier.start(); // daemon 运行中;close() 才会真正执行 drain + 关执行器

            Intent intent = new Intent("r21-barrier-0001");
            intent.setExecuteAt(Instant.ofEpochMilli(clock.get() + 60_000));
            intent.transitionTo(IntentStatus.SCHEDULED);
            intent.incrementRevision();
            store.put(intent);
            // daemon 覆盖(interval 1000ms)或段 2 内联 force(executor 活着) → 成功
            assertTrue(barrier.awaitCommit() > 0, "inline force must work while the executor is alive");

            // 关闭 barrier:停 daemon + 关 inline force 执行器
            barrier.close();

            // close 的最终 force 只覆盖已 snapshot 的 ticket;新写者的 ticket 更大,
            // 段 1 等不到 daemon(已停)→ 段 2 submit → 修复前 RejectedExecutionException。
            Intent intent2 = new Intent("r21-barrier-0002");
            intent2.setExecuteAt(Instant.ofEpochMilli(clock.get() + 60_000));
            intent2.transitionTo(IntentStatus.SCHEDULED);
            intent2.incrementRevision();
            store.put(intent2);
            long ticket = barrier.awaitCommit();
            assertTrue(ticket > 0,
                "awaitCommit must survive executor shutdown (RejectedExecutionException re-opens the ghost window)");
        }
    }
}
