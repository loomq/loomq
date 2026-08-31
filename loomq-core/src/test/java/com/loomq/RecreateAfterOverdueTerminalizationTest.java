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
 * C18-1 残留(r19 T1): {@code WheelRecovery} overdue 臂(P0-2 补终态化)只做
 * maxRevisions.put 与原地覆写,不补标 multiSlot/tombstonedIds——与 r18 terminal
 * else 臂(C18-1a/b)不对称。同进程 overdue→重建→终态→回收→再重建→重启的遮蔽链
 * 仍开:重建 incarnation 终态被 persistTerminalInPlace 误判 singleSlot 回收,且
 * reclaimTerminal 的 C4-1 守卫(tombstoneIds)失效 → maxRevisions.remove → 再重建
 * 种子回落,重启被磁盘上残留的 overdue 终态墓碑(更高 revision)遮蔽,静默至桶过期。
 *
 * <p>镜像 {@code RecreateTerminalTombstoneAfterRestartTest} 三段引擎链骨架
 * (e1 → e2 → e3),双臂 = 两方法:count>1(双补标,multiSlot+tombstonedIds)与
 * count==1(单 tombstonedIds 补标)。行为红测试:修复前两方法双红灯(重建#2
 * revision 断言 + e3 投递断言),修复后全绿。</p>
 *
 * <p>tail 来源 overdue 臂不设测试:r18 实测偏差已证其对健康时钟不可达
 * ({@code promoteInto} 以 now+horizon 为界,past-dated tail 记录必先提入 wheel
 * 再终态化),不设臆造用例。</p>
 */
class RecreateAfterOverdueTerminalizationTest {

    @TempDir Path tmp;

    /**
     * count>1 双补标臂:e1 create(+2s,slot A R1)→ updateIntent no-op +4s
     * (slot B R2,多槽)→ +2s 前关(零投递)。e2 恢复:P0-2 命中胜者 slot B(R2)
     * → EXPIRED R3 原地覆写(count=2,slot A 陈旧兄弟留盘);修复后 multiSlot+{X}
     * 与 tombstonedIds+{X} 随报告注入。
     *
     * <p>重建#1(+200ms)→ ACKED(≥R5):修复后经 multiSlot 补标判 !singleSlot →
     * 重建槽保留墓碑 + maxRevisions 保留 ACKED revision → 重建#2 种子 ≥R6;修复前
     * singleSlot 误判回收 + tombstonedIds 空 → maxRevisions.remove → 重建#2 R1
     * (红灯一)。e3:修复前磁盘胜者 = EXPIRED R3(R3 > R1),SCHEDULED R1 被遮蔽
     * 静默至桶过期 → delivered==0(红灯二);修复后 ==1。</p>
     */
    @Test
    void overdueMultiSlotTombstoneMustSurviveRecreateAcrossRestarts() throws Exception {
        String id = "r19-overdue-multi-1";
        Path dir = tmp.resolve("m");
        long t0 = System.currentTimeMillis();

        // e1:create(+2s)→ 多槽改期(+4s)→ +2s 前立即 close(零投递)
        try (LoomqEngine e1 = LoomqEngine.builder()
                .dataDir(dir).nodeId("e1")
                .deliveryHandler(i -> CompletableFuture.completedFuture(DeliveryHandler.DeliveryResult.SUCCESS))
                .build()) {
            e1.start();
            Intent first = new Intent(id);
            first.setExecuteAt(Instant.ofEpochMilli(t0 + 2_000));
            first.setPrecisionTier(PrecisionTier.ULTRA);
            e1.createIntent(first, AckMode.DURABLE).get();
            e1.updateIntent(id, i -> { }, Instant.ofEpochMilli(t0 + 4_000));   // 多槽:slot A R1 + slot B R2
        }

        // e2:sleep 至两槽均 overdue(t0+4.5s)后启动——恢复期 P0-2 补 EXPIRED R3
        Thread.sleep(Math.max(0, t0 + 4_500 - System.currentTimeMillis()));
        AtomicInteger deliveredE2 = new AtomicInteger();
        try (LoomqEngine e2 = LoomqEngine.builder()
                .dataDir(dir).nodeId("e2")
                .deliveryHandler(i -> {
                    deliveredE2.incrementAndGet();
                    return CompletableFuture.completedFuture(DeliveryHandler.DeliveryResult.SUCCESS);
                })
                .build()) {
            e2.start();
            // 同进程重建#1(+200ms)→ 投递至终态(种子 R4 起投递链逐态 increment → ACKED ≥R5)
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
            assertEquals(1, deliveredE2.get(), "重建#1 必须正常投递(该断言修复前后均应成立,锚定链路健康)");

            // 同进程重建#2(+2s):修复后种子 ≥R6;修复前 R1(红灯一)
            Intent recreated2 = new Intent(id);
            recreated2.setExecuteAt(Instant.ofEpochMilli(System.currentTimeMillis() + 2_000));
            recreated2.setPrecisionTier(PrecisionTier.ULTRA);
            e2.createIntent(recreated2, AckMode.DURABLE).get();
            long rev = e2.getIntent(id).orElseThrow().getRevision();
            assertTrue(rev >= 6,
                "重建#2 必须种子到重建#1 ACKED revision(≥R5)之上(overdue count>1 双补标臂"
                    + "保护 maxRevisions 不被单槽回收移除);got " + rev);
        }

        // e3:重启——修复前 EXPIRED R3 遮蔽重建#2 → delivered==0(红灯二);修复后 ==1
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
                "重建#2 重启后必须投递,不得被 overdue 终态墓碑(EXPIRED R3)遮蔽");
        }
    }

    /**
     * count==1 单 tombstonedIds 臂:e1 仅 create(+2s)→ 关;e2 恢复 P0-2 →
     * EXPIRED R2 原地覆写(count=1;修复后仅 tombstonedIds+{X},multiSlot 不加)。
     * 重建#1(+200ms)→ ACKED(≥R4)→ 单槽回收(singleSlot=true 在此按 multiSlot
     * 簿记语义正确)但 C4-1 守卫见 tombstonedIds 含 X → maxRevisions 保留 →
     * 重建#2(+2s)种子 ≥R5(修复前 R1,红灯一);e3:修复前 EXPIRED R2 遮蔽
     * → delivered==0(红灯二),修复后 ==1。
     */
    @Test
    void overdueSingleSlotTombstoneMustSurviveRecreateAcrossRestarts() throws Exception {
        String id = "r19-overdue-single-1";
        Path dir = tmp.resolve("s");
        long t0 = System.currentTimeMillis();

        // e1:仅 create(+2s),到期前关(零投递)
        try (LoomqEngine e1 = LoomqEngine.builder()
                .dataDir(dir).nodeId("e1")
                .deliveryHandler(i -> CompletableFuture.completedFuture(DeliveryHandler.DeliveryResult.SUCCESS))
                .build()) {
            e1.start();
            Intent first = new Intent(id);
            first.setExecuteAt(Instant.ofEpochMilli(t0 + 2_000));
            first.setPrecisionTier(PrecisionTier.ULTRA);
            e1.createIntent(first, AckMode.DURABLE).get();
        }

        // e2:sleep 至 overdue(t0+2.5s)后启动——P0-2 补 EXPIRED R2 原地覆写(count=1)
        Thread.sleep(Math.max(0, t0 + 2_500 - System.currentTimeMillis()));
        AtomicInteger deliveredE2 = new AtomicInteger();
        try (LoomqEngine e2 = LoomqEngine.builder()
                .dataDir(dir).nodeId("e2")
                .deliveryHandler(i -> {
                    deliveredE2.incrementAndGet();
                    return CompletableFuture.completedFuture(DeliveryHandler.DeliveryResult.SUCCESS);
                })
                .build()) {
            e2.start();
            // 同进程重建#1(+200ms)→ ACKED(种子 R3 起投递链逐态 increment → ≥R4)
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
            assertEquals(1, deliveredE2.get(), "重建#1 必须正常投递(该断言修复前后均应成立,锚定链路健康)");

            // 同进程重建#2(+2s):修复后种子 ≥R5(ACKED ≥R4 → seedRevision ≥R4 → +1);
            // 修复前 tombstonedIds 空 → C4-1 守卫失效 → maxRevisions.remove → R1(红灯一)
            Intent recreated2 = new Intent(id);
            recreated2.setExecuteAt(Instant.ofEpochMilli(System.currentTimeMillis() + 2_000));
            recreated2.setPrecisionTier(PrecisionTier.ULTRA);
            e2.createIntent(recreated2, AckMode.DURABLE).get();
            long rev = e2.getIntent(id).orElseThrow().getRevision();
            assertTrue(rev >= 5,
                "重建#2 必须种子到重建#1 ACKED revision(≥R4)之上(overdue count==1 单"
                    + " tombstonedIds 补标臂经 C4-1 守卫保护种子映射);got " + rev);
        }

        // e3:重启——修复前 EXPIRED R2 遮蔽重建#2 → delivered==0(红灯二);修复后 ==1
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
                "重建#2 重启后必须投递,不得被 overdue 终态墓碑(EXPIRED R2)遮蔽");
        }
    }
}
