package com.loomq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.domain.intent.AckMode;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.spi.DeliveryHandler;
import java.nio.file.Path;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * C18-1(r18): 恢复期磁盘墓碑不注入 tombstoneIds → 同进程重建×终态回收违反 C4-1,
 * 重启被旧墓碑遮蔽(静默丢失)。镜像 RecreateSameProcessThenRestartTest 三段链骨架;
 * wheel 变体与 tail 变体各一条,双红灯(修复前 revision 断言失败)。
 *
 * <p>r18 实测偏差(相对计划原稿):计划 tail 变体以"冷 cancel 留下可扫描 tail 终态墓碑"为前提,
 * 但 {@code TailIndex.remove}(冷取消路径)追加 TOMBSTONE 后即从内存索引摘除,重启重放亦
 * applyRemove——冷取消后的 tail 记录不再可扫描;而恢复期 P0-2 的 tail 分支对健康时钟不可达
 * ({@code promoteInto} 以 now+horizon 为界,past-dated 记录必先被提入 wheel 再终态化)。
 * 引擎级真实可达的 tail 终态墓碑唯一来源:热副本在途改期超视界后热取消
 * ({@code persistTerminalInPlace} tail 分支 {@code tail.put(CANCELED)},C3-3 不回收)。
 * tail 变体据此重设计:create 热载 → 热改期 +40d(tail)→ 热取消留 tail 终态墓碑 →
 * e2 注入臂保护重建 → e3 重启投递。另:重建#2 executeAt 由 +30s 改 +2s——计划原稿 +30s
 * 与 e3 的 5s 投递等待窗自相矛盾(实测修复后 e3 仍 delivered==0,纯因未到期);+2s 与既有
 * RecreateSameProcessThenRestartTest 的 +200ms 先例同型且裕量更大("e2 关闭前不投递"约束仍满足)。</p>
 */
class RecreateTerminalTombstoneAfterRestartTest {

    @TempDir Path tmp;

    /**
     * 重建#2 的 executeAt:+2s——足够远使 e2 关闭前不会投递(e2 在创建后百毫秒级即关闭),
     * 又使 e3 启动后迅速到期,落在 5s 投递等待窗内(r18 实测偏差:计划原稿 +30s 供不了
     * 5s 窗,见类头)。
     */
    private static Instant recreateTwoExecAt() {
        return Instant.ofEpochMilli(System.currentTimeMillis() + 2_000L);
    }

    @Test
    void wheelTombstoneMustSurviveTerminalReclaimAcrossRecreates() throws Exception {
        String id = "r18-tombstone-0001";
        Path dir = tmp.resolve("w");

        // e1:create → reschedule(多槽) → cancel(CANCELED R3 墓碑)——不投递,直接关
        try (LoomqEngine e1 = LoomqEngine.builder()
                .dataDir(dir).nodeId("e1")
                .deliveryHandler(i -> CompletableFuture.completedFuture(DeliveryHandler.DeliveryResult.SUCCESS))
                .build()) {
            e1.start();
            long t0 = System.currentTimeMillis();
            Intent first = new Intent(id);
            first.setExecuteAt(Instant.ofEpochMilli(t0 + 300_000));
            first.setPrecisionTier(PrecisionTier.ULTRA);
            e1.createIntent(first, AckMode.DURABLE).get();
            e1.updateIntent(id, i -> { }, Instant.ofEpochMilli(t0 + 400_000));   // 多槽
            e1.cancelIntent(id);                                                  // CANCELED R3 墓碑
        }

        // e2:重启恢复(terminal 胜者 CANCELED R3,count=2)→ 重建#1(seed R4)→ 投递 ACKED(修复前:
        // 单槽回收 + maxRevisions.remove)→ 同进程重建#2(修复前 R1 → revision 断言失败,红灯一)
        AtomicInteger deliveredE2 = new AtomicInteger();
        try (LoomqEngine e2 = LoomqEngine.builder()
                .dataDir(dir).nodeId("e2")
                .deliveryHandler(i -> {
                    deliveredE2.incrementAndGet();
                    return CompletableFuture.completedFuture(DeliveryHandler.DeliveryResult.SUCCESS);
                })
                .build()) {
            e2.start();
            Intent recreated1 = new Intent(id);
            recreated1.setExecuteAt(Instant.ofEpochMilli(System.currentTimeMillis() + 200));
            recreated1.setPrecisionTier(PrecisionTier.ULTRA);
            e2.createIntent(recreated1, AckMode.DURABLE).get();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (System.nanoTime() < deadline) {
                Intent cur = e2.getIntent(id).orElse(null);
                if (cur != null && cur.getStatus().isTerminal()) break;
                Thread.sleep(20);
            }
            assertEquals(1, deliveredE2.get(), "重建#1 必须正常投递(该断言修复前后均应成立)");

            Intent recreated2 = new Intent(id);
            recreated2.setExecuteAt(recreateTwoExecAt());
            recreated2.setPrecisionTier(PrecisionTier.ULTRA);
            e2.createIntent(recreated2, AckMode.DURABLE).get();
            long rev = e2.getIntent(id).orElseThrow().getRevision();
            assertTrue(rev >= 5,
                "重建#2 必须种子到 ACKED R5 之上(墓碑保护的 maxRevisions 不可被回收);got " + rev);
        }

        // e3:重启——修复前重建#2(R1)被旧墓碑 R3/R4 遮蔽 → delivered==0(红灯二);修复后 ==1
        AtomicInteger deliveredE3 = new AtomicInteger();
        try (LoomqEngine e3 = LoomqEngine.builder()
                .dataDir(dir).nodeId("e3")
                .deliveryHandler(i -> {
                    deliveredE3.incrementAndGet();
                    return CompletableFuture.completedFuture(DeliveryHandler.DeliveryResult.SUCCESS);
                })
                .build()) {
            e3.start();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (System.nanoTime() < deadline && deliveredE3.get() == 0) {
                Thread.sleep(20);
            }
            assertEquals(1, deliveredE3.get(),
                "重建#2 重启后必须投递,不得被旧终态墓碑遮蔽");
        }
    }

    /**
     * tail 变体(r18 重设计,见类头):e1 热载创建 → 热改期 +40d(落 tail,idx 改指)→
     * 热取消经 persistTerminalInPlace tail 分支留 tail 终态墓碑 CANCELED R3(C3-3 不回收);
     * e2 恢复该 tail 终态为 max-revision 胜者(inTail → else 臂)→(修复后)注入
     * tombstonedIds,重建#1 的 ACKED 单槽回收不再移除种子映射 → 重建#2 种子到 R5 之上
     * (修复前 R1,红灯);e3 重启重建#2 投递(修复前 tail R3 墓碑仍为胜者,遮蔽重建#2
     * → delivered==0,红灯二)。
     */
    @Test
    void tailTombstoneMustSurviveRecreateAcrossRestarts() throws Exception {
        String id = "r18-tail-tomb-001";
        Path dir = tmp.resolve("t");

        // e1:create 热载(+2s)→ 热改期 +40d(超视界落 tail,R2,idx 改指)→ 热取消
        // (CANCELED R3 落 tail persistTerminalInPlace tail 分支——热副本在内存,不走
        // cancelCold 的 tailIndex.remove 路径)
        try (LoomqEngine e1 = LoomqEngine.builder()
                .dataDir(dir).nodeId("e1")
                .deliveryHandler(i -> CompletableFuture.completedFuture(DeliveryHandler.DeliveryResult.SUCCESS))
                .build()) {
            e1.start();
            long t0 = System.currentTimeMillis();
            Intent first = new Intent(id);
            first.setExecuteAt(Instant.ofEpochMilli(t0 + 2_000));
            first.setPrecisionTier(PrecisionTier.ULTRA);
            e1.createIntent(first, AckMode.DURABLE).get();
            e1.updateIntent(id, i -> { }, Instant.ofEpochMilli(t0 + 40L * 24 * 3600 * 1000));   // 热改期 → tail
            e1.cancelIntent(id);   // 热取消 → tail CANCELED R3 墓碑(恢复期不回收,C3-3)
        }

        // e2:重启恢复——tail EXPIRED R2 是 max-revision 胜者(count==1 且 inTail → else 臂)
        // →(修复后)注入 tombstonedIds;重建#1(seed R3)→ 投递 ACKED R4 → 单槽回收时
        // 墓碑守卫保住 maxRevisions(修复前 remove)→ 重建#2(seed R5,修复前 R1 → 红灯一)
        AtomicInteger deliveredE2 = new AtomicInteger();
        try (LoomqEngine e2 = LoomqEngine.builder()
                .dataDir(dir).nodeId("e2")
                .deliveryHandler(i -> {
                    deliveredE2.incrementAndGet();
                    return CompletableFuture.completedFuture(DeliveryHandler.DeliveryResult.SUCCESS);
                })
                .build()) {
            e2.start();
            Intent recreated1 = new Intent(id);
            recreated1.setExecuteAt(Instant.ofEpochMilli(System.currentTimeMillis() + 200));
            recreated1.setPrecisionTier(PrecisionTier.ULTRA);
            e2.createIntent(recreated1, AckMode.DURABLE).get();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (System.nanoTime() < deadline) {
                Intent cur = e2.getIntent(id).orElse(null);
                if (cur != null && cur.getStatus().isTerminal()) break;
                Thread.sleep(20);
            }
            assertEquals(1, deliveredE2.get(), "重建#1 必须正常投递(该断言修复前后均应成立)");

            Intent recreated2 = new Intent(id);
            recreated2.setExecuteAt(recreateTwoExecAt());
            recreated2.setPrecisionTier(PrecisionTier.ULTRA);
            e2.createIntent(recreated2, AckMode.DURABLE).get();
            long rev = e2.getIntent(id).orElseThrow().getRevision();
            assertTrue(rev >= 5,
                "重建#2 必须种子到 ACKED R5 之上(tail 墓碑注入臂保护种子映射);got " + rev);
        }

        // e3:重启——修复前 tail R2 墓碑仍是 max-revision 胜者 → 遮蔽重建#2 → delivered==0(红灯二)
        AtomicInteger deliveredE3 = new AtomicInteger();
        try (LoomqEngine e3 = LoomqEngine.builder()
                .dataDir(dir).nodeId("e3")
                .deliveryHandler(i -> {
                    deliveredE3.incrementAndGet();
                    return CompletableFuture.completedFuture(DeliveryHandler.DeliveryResult.SUCCESS);
                })
                .build()) {
            e3.start();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (System.nanoTime() < deadline && deliveredE3.get() == 0) {
                Thread.sleep(20);
            }
            assertEquals(1, deliveredE3.get(),
                "重建#2 重启后必须投递(tail 终态注入臂保护种子映射)");
        }
    }
}
