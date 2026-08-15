package com.loomq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.domain.intent.AckMode;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.spi.DeliveryHandler;
import com.loomq.spi.DeliveryHandler.DeliveryResult;
import com.loomq.testutil.TestStores;
import java.nio.file.Path;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * C4-2/C4-3 回归:命令层"提交后失败"不得回滚已提交的终态/改期。
 *
 * <p>cancelIntent 与 fireNow 的持久化(终态/新 executeAt)在锁内已提交(mmap),锁外的
 * awaitCommit/store 更新等失败若触发无差别回滚:磁盘与内存分歧——cancel 重启后按
 * max-revision 取磁盘 CANCELED(调用方却被告知失败);fireNow 磁盘 SCHEDULED@now 过期后
 * 被 recovery 终态化(内存却以为投递在旧时刻)。注入:store.update 抛错(提交后失败,
 * 持久化已发生)。</p>
 */
class BugPostCommitFailureRollbackTest {

    @TempDir Path tmp;

    private static final DeliveryHandler SUCCESS =
        i -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS);

    @Test
    void cancelCommittedMustNotRollbackOnPostCommitFailure() throws Exception {
        String id = "postcc-cancel-01";
        AtomicInteger deliveries = new AtomicInteger();
        try (LoomqEngine engine = LoomqEngine.builder()
                .dataDir(tmp.resolve("c")).nodeId("c1")
                .intentStore(new TestStores.UpdateThrowingStore())
                .deliveryHandler(i -> { deliveries.incrementAndGet(); return CompletableFuture.completedFuture(DeliveryResult.SUCCESS); })
                .build()) {
            engine.start();
            Intent it = new Intent(id);
            it.setExecuteAt(Instant.ofEpochMilli(System.currentTimeMillis() + 60_000));
            it.setPrecisionTier(PrecisionTier.ULTRA);
            engine.createIntent(it, AckMode.DURABLE).get();

            // 提交后失败:取消已持久化(CANCELED 已入 mmap),store.update 抛错
            boolean cancelled = engine.cancelIntent(id);
            assertTrue(cancelled, "durably committed cancel must report success, not roll back");
            Intent cur = engine.getIntent(id).orElseThrow();
            assertEquals(IntentStatus.CANCELED, cur.getStatus(),
                "committed cancel must NOT be rolled back by a post-commit failure (disk says CANCELED)");
            Thread.sleep(300);
            assertEquals(0, deliveries.get(), "cancelled intent must not be re-scheduled by a false rollback");
        }
    }

    @Test
    void fireNowCommittedMustNotRollbackOnPostCommitFailure() throws Exception {
        String id = "postcc-firenow-01";
        AtomicInteger deliveries = new AtomicInteger();
        try (LoomqEngine engine = LoomqEngine.builder()
                .dataDir(tmp.resolve("f")).nodeId("f1")
                .intentStore(new TestStores.UpdateThrowingStore())
                .deliveryHandler(i -> { deliveries.incrementAndGet(); return CompletableFuture.completedFuture(DeliveryResult.SUCCESS); })
                .build()) {
            engine.start();
            Intent it = new Intent(id);
            it.setExecuteAt(Instant.ofEpochMilli(System.currentTimeMillis() + 60_000));
            it.setPrecisionTier(PrecisionTier.ULTRA);
            engine.createIntent(it, AckMode.DURABLE).get();

            // 提交后失败:SCHEDULED@now 已入 mmap(索引指向新槽),store.update 抛错
            boolean fired = engine.fireNow(id);
            assertTrue(fired, "committed fireNow must report success, not roll back");
            // 修复后:不回滚 executeAt,按 now 投递;修复前:回滚到 +60s,投递窗口外
            long deadline = System.currentTimeMillis() + 5_000;
            while (System.currentTimeMillis() < deadline && deliveries.get() == 0) {
                Thread.sleep(20);
            }
            assertEquals(1, deliveries.get(),
                "committed fireNow must deliver now, not be rolled back to the original schedule");
        }
    }

    /** C4-5:updater 先改 executeAt 再抛异常时,须按更新前的 executeAt 重新调度。 */
    @Test
    void updaterMutatesThenThrowsMustRescheduleAtOriginalTime() throws Exception {
        String id = "upd-mutate-0001";
        AtomicInteger deliveries = new AtomicInteger();
        try (LoomqEngine engine = LoomqEngine.builder()
                .dataDir(tmp.resolve("u")).nodeId("u1")
                .deliveryHandler(i -> { deliveries.incrementAndGet(); return CompletableFuture.completedFuture(DeliveryResult.SUCCESS); })
                .build()) {
            engine.start();
            long t0 = System.currentTimeMillis();
            Intent it = new Intent(id);
            it.setExecuteAt(Instant.ofEpochMilli(t0 + 400));
            it.setPrecisionTier(PrecisionTier.ULTRA);
            engine.createIntent(it, AckMode.DURABLE).get();

            // updater 把 executeAt 改到 +60s 后抛异常;newExecuteAt 分支已把 intent 摘出调度结构
            assertThrows(RuntimeException.class, () -> engine.updateIntent(
                id,
                i -> {
                    i.setExecuteAt(Instant.ofEpochMilli(System.currentTimeMillis() + 60_000));
                    throw new IllegalStateException("updater boom after mutation");
                },
                Instant.ofEpochMilli(t0 + 120_000)),
                "updater 异常必须上抛给调用方");

            // 修复后:按原 executeAt(t0+400) 重新调度 → 投递;修复前:按变异后 +60s 调度 → 不投递
            long deadline = System.currentTimeMillis() + 5_000;
            while (System.currentTimeMillis() < deadline && deliveries.get() == 0) {
                Thread.sleep(20);
            }
            assertEquals(1, deliveries.get(),
                "updater failure must reschedule at the ORIGINAL executeAt, not the mutated one");
        }
    }
}
