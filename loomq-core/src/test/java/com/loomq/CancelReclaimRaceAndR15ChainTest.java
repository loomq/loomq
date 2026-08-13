package com.loomq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.domain.intent.AckMode;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.spi.DeliveryHandler;
import com.loomq.spi.DeliveryHandler.DeliveryResult;
import java.nio.file.Path;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * C4-1/C2-1 回归:终态回收与并发重建的竞态 + R15 种子映射过早移除。
 *
 * <p><b>竞态(</b>cancelIntent 热路径):终态已提交(索引指向 CANCELED 槽)后、awaitCommit
 * 窗口内,同 id 重建被允许(磁盘终态可重建)——重建把索引指向新 SCHEDULED 槽;随后
 * reclaimTerminal 按 intentId 无条件清索引 → 新 incarnation 的索引被抹,冷 intent 到点
 * 不被 promote,静默不投递直到重启。修复:reclaimTerminal 只做按终态 loc 的定向移除。</p>
 *
 * <p><b>R15 链(</b>同进程重建→ACK 单槽回收→再重建→重启):第一次 incarnation 的
 * 多槽终态墓碑(更高 revision)仍留在磁盘,但第二次 incarnation 单槽 ACK 后
 * maxRevisions.remove 把种子抹掉 → 第三次重建从 rev 1 起步 → 重启被墓碑遮蔽。
 * 修复:tombstoneIds 持久标记"曾保留终态墓碑",有墓碑则不移除种子映射。</p>
 */
@Tag("slow")
class CancelReclaimRaceAndR15ChainTest {

    @TempDir Path tmp;

    private static final DeliveryHandler SUCCESS =
        i -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS);

    @Test
    void reclaimMustNotClobberIndexOfConcurrentRebuild() throws Exception {
        String id = "r15-race-0001";
        Path dir = tmp.resolve("race");
        // 10s 组提交间隔:拉开 awaitCommit 窗口,使重建确定性地落在窗口内
        com.loomq.infrastructure.wheel.WheelConfig cfg = new com.loomq.infrastructure.wheel.WheelConfig(
            dir.toString(), "t", 30, 16, 10_000L, 10_000L, 60L * 60_000L, 60_000L, null);

        try (LoomqEngine engine = LoomqEngine.builder()
                .wheelConfig(cfg).nodeId("e1").deliveryHandler(SUCCESS).build()) {
            engine.start();
            Intent first = new Intent(id);
            first.setExecuteAt(Instant.ofEpochMilli(System.currentTimeMillis() + 300_000));
            first.setPrecisionTier(PrecisionTier.ULTRA);
            engine.createIntent(first, AckMode.DURABLE).get();

            // 取消在独立线程执行(其 awaitCommit 阻塞 ~10s,给出重建窗口)
            Thread canceller = new Thread(() -> engine.cancelIntent(id), "cancel-thread");
            canceller.start();
            // 等取消提交(store 状态已 CANCELED,锁已出,正阻塞在 awaitCommit)
            long deadline = System.currentTimeMillis() + 5_000;
            while (System.currentTimeMillis() < deadline) {
                Intent cur = engine.getIntent(id).orElse(null);
                if (cur != null && cur.getStatus() == IntentStatus.CANCELED) break;
                Thread.sleep(20);
            }
            assertEquals(IntentStatus.CANCELED, engine.getIntent(id).orElseThrow().getStatus(),
                "cancel must have committed before the rebuild");

            // 窗口内重建同 id:磁盘终态允许重建;冷 intent(>60min)依赖索引被 promote
            Intent recreated = new Intent(id);
            recreated.setExecuteAt(Instant.ofEpochMilli(System.currentTimeMillis() + 2L * 60 * 60_000L));
            recreated.setPrecisionTier(PrecisionTier.ULTRA);
            engine.createIntent(recreated, AckMode.DURABLE).get();

            canceller.join(15_000);
            assertTrue(!canceller.isAlive(), "canceller must finish");
            // 修复前:reclaimTerminal 无条件 locationIndex.remove → 重建索引被抹
            assertNotNull(engine.getLocationIndex().get(id),
                "reclaimTerminal must NOT clobber the index of a concurrently rebuilt intent");
        }
    }

    @Test
    void r15SeedMustSurviveSingleSlotAckAfterTombstone() throws Exception {
        String id = "r15-chain-0001";
        Path dir = tmp.resolve("chain");
        AtomicInteger e2Deliveries = new AtomicInteger();

        // e1:create → reschedule(多槽)→ cancel(终态墓碑 rev3,tombstoneIds 标记)
        // → 重建(rev≥4)→ 投递 ACK(单槽回收)→ 再重建(修复后 rev≥5;修复前 rev1)
        try (LoomqEngine e1 = LoomqEngine.builder()
                .dataDir(dir).nodeId("e1").deliveryHandler(SUCCESS).build()) {
            e1.start();
            long t0 = System.currentTimeMillis();
            Intent first = new Intent(id);
            first.setExecuteAt(Instant.ofEpochMilli(t0 + 300_000));
            first.setPrecisionTier(PrecisionTier.ULTRA);
            e1.createIntent(first, AckMode.DURABLE).get();
            e1.updateIntent(id, i -> {}, Instant.ofEpochMilli(t0 + 400_000)); // 多槽
            assertTrue(e1.cancelIntent(id), "cancel must succeed");           // 墓碑 rev3

            Intent recreated = new Intent(id);
            recreated.setExecuteAt(Instant.ofEpochMilli(System.currentTimeMillis() + 200));
            recreated.setPrecisionTier(PrecisionTier.ULTRA);
            e1.createIntent(recreated, AckMode.DURABLE).get();
            // 等第一次重建投递 + ACK 落盘 + reclaimTerminal 执行
            long deadline = System.currentTimeMillis() + 5_000;
            while (System.currentTimeMillis() < deadline) {
                Intent cur = e1.getIntent(id).orElse(null);
                if (cur != null && cur.getStatus().isTerminal()) break;
                Thread.sleep(20);
            }
            assertEquals(IntentStatus.ACKED, e1.getIntent(id).orElseThrow().getStatus(),
                "first recreated incarnation must deliver and ack");
            Thread.sleep(100); // 让 reclaimTerminal(锁外)跑完

            // 第二次重建:修复前 histMax 已被抹 → rev 1;修复后墓碑守卫 → rev ≥ 5
            Intent again = new Intent(id);
            again.setExecuteAt(Instant.ofEpochMilli(System.currentTimeMillis() + 5_000));
            again.setPrecisionTier(PrecisionTier.ULTRA);
            e1.createIntent(again, AckMode.DURABLE).get();
            long rev = e1.getIntent(id).orElseThrow().getRevision();
            assertTrue(rev >= 5,
                "second recreate must seed above the retained tombstone (rev 4), got " + rev);
        }

        // e2 重启:修复前第二次重建 rev1 被 rev3 墓碑遮蔽 → 永不投递;修复后 rev5 胜出 → 投递
        try (LoomqEngine e2 = LoomqEngine.builder()
                .dataDir(dir).nodeId("e2")
                .deliveryHandler(i -> {
                    e2Deliveries.incrementAndGet();
                    return CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
                }).build()) {
            e2.start();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (System.nanoTime() < deadline && e2Deliveries.get() == 0) {
                Thread.sleep(50);
            }
            assertEquals(1, e2Deliveries.get(),
                "second recreated incarnation must be delivered after restart, not shadowed by the tombstone");
        }
    }
}
