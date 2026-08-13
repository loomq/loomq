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
 * R8: 终态墓碑遮蔽重建的 Intent——createIntent 重建同 intentId 时新 Intent 从 revision 0
 * 起步,recovery 按 max revision 去重时被旧终态墓碑(更高 revision,multiSlot 保留)遮蔽,
 * 新 Intent 静默丢失(永不投递)。
 *
 * <p>时序:create(+reschedule 成多槽)→ cancel(终态墓碑,multiSlot 不回收)→ 重启(recovery
 * 注入磁盘历史最高 revision)→ 重建同 id → 再重启。修复后新 Intent revision 抬升到历史
 * 最高值之上,第二次重启 recovery 按 max revision 选中新 SCHEDULED 槽而非旧终态墓碑,
 * 正常投递;修复前新 Intent rev1 &lt; 墓碑 rev3,被终态墓碑遮蔽永不投递。</p>
 */
class RecreateAfterTerminalTombstoneTest {

    @TempDir Path tmp;

    @Test
    void recreateSameIdAfterTerminalTombstoneMustNotBeGhostLost() throws Exception {
        String id = "r8-recreate-0001";

        // Phase 1: 建立多槽终态墓碑。create → reschedule(追加第 2 槽,multiSlot) → cancel(终态墓碑)。
        try (LoomqEngine e1 = LoomqEngine.builder()
                .dataDir(tmp.resolve("w")).nodeId("e1")
                .deliveryHandler(i -> CompletableFuture.completedFuture(DeliveryHandler.DeliveryResult.SUCCESS))
                .build()) {
            e1.start();
            long t0 = System.currentTimeMillis();
            Intent it = new Intent(id);
            it.setExecuteAt(Instant.ofEpochMilli(t0 + 300_000));
            it.setPrecisionTier(PrecisionTier.ULTRA);
            e1.createIntent(it, AckMode.DURABLE).get();
            // reschedule → 追加新槽,markMultiSlot
            e1.updateIntent(id, i -> {}, Instant.ofEpochMilli(t0 + 400_000));
            // cancel → 终态墓碑(rev3),multiSlot 不回收
            assertTrue(e1.cancelIntent(id), "cancel must succeed");
        }

        // Phase 2: 重启 → recovery 注入历史最高 revision(rev3);重建同 id 时 revision 必须抬升。
        long recreatedRevision;
        try (LoomqEngine e2 = LoomqEngine.builder()
                .dataDir(tmp.resolve("w")).nodeId("e2")
                .deliveryHandler(i -> CompletableFuture.completedFuture(DeliveryHandler.DeliveryResult.SUCCESS))
                .build()) {
            e2.start();
            long t0 = System.currentTimeMillis();
            Intent it = new Intent(id);
            it.setExecuteAt(Instant.ofEpochMilli(t0 + 200));
            it.setPrecisionTier(PrecisionTier.ULTRA);
            e2.createIntent(it, AckMode.DURABLE).get();
            recreatedRevision = e2.getIntent(id).orElseThrow().getRevision();
        }
        assertTrue(recreatedRevision >= 4,
            "recreated intent revision must be seeded above historical max (3); got " + recreatedRevision);

        // Phase 3: 再重启 → recovery 必须选中新的 SCHEDULED 槽(rev≥4)而非旧终态墓碑(rev3)。
        AtomicInteger delivered = new AtomicInteger();
        try (LoomqEngine e3 = LoomqEngine.builder()
                .dataDir(tmp.resolve("w")).nodeId("e3")
                .deliveryHandler(i -> {
                    delivered.incrementAndGet();
                    return CompletableFuture.completedFuture(DeliveryHandler.DeliveryResult.SUCCESS);
                })
                .build()) {
            e3.start();

            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (System.nanoTime() < deadline) {
                Intent cur = e3.getIntent(id).orElse(null);
                if (cur != null && cur.getStatus().isTerminal()) break;
                Thread.sleep(20);
            }
            assertEquals(1, delivered.get(),
                "recreated intent must be delivered after second restart, not shadowed by terminal tombstone");
        }
    }
}
