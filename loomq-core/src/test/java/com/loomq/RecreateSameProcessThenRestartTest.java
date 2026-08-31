package com.loomq;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
 * R9: 同一进程内 create → cancel → 重建同 id → 重启,新 SCHEDULED Intent 被旧终态墓碑遮蔽。
 *
 * <p>与 {@link RecreateAfterTerminalTombstoneTest}(重建前有重启注入历史 revision)不同,
 * 本测试的重建发生在取消之后、任何重启之前:此时 recoveredMaxRevisions 未覆盖该 id(取消
 * 墓碑在本进程内新写),重建的新 Intent 从 revision 0 起步 → 重启 recovery 按 max revision
 * 去重选中旧终态墓碑,新 SCHEDULED 槽被遮蔽、永不投递。修复需在进程内维护
 * intentId → 历史最高 revision 的活映射,持久化写路径持续更新。</p>
 */
class RecreateSameProcessThenRestartTest {

    @TempDir Path tmp;

    @Test
    void recreateSameIdInProcessThenRestartMustDeliver() throws Exception {
        String id = "r9-recreate-0001";
        Path dir = tmp.resolve("w");

        // 单进程内:create → reschedule(多槽) → cancel(终态墓碑) → 重建同 id(尚 SCHEDULED)。
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
            e1.updateIntent(id, i -> {}, Instant.ofEpochMilli(t0 + 400_000)); // 多槽
            e1.cancelIntent(id);                                              // 终态墓碑 rev3

            Intent recreated = new Intent(id);
            recreated.setExecuteAt(Instant.ofEpochMilli(System.currentTimeMillis() + 200));
            recreated.setPrecisionTier(PrecisionTier.ULTRA);
            e1.createIntent(recreated, AckMode.DURABLE).get();
            // 关键断言:进程内重建即应抬升 revision 到墓碑之上,而非等重启
            long rev = e1.getIntent(id).orElseThrow().getRevision();
            assertEquals(true, rev >= 4,
                "in-process recreate must seed revision above terminal tombstone (3); got " + rev);
        }

        // 重启 → recovery 必须选中新的 SCHEDULED 槽而非旧终态墓碑。
        AtomicInteger delivered = new AtomicInteger();
        try (LoomqEngine e2 = LoomqEngine.builder()
                .dataDir(dir).nodeId("e2")
                .deliveryHandler(i -> {
                    delivered.incrementAndGet();
                    return CompletableFuture.completedFuture(DeliveryHandler.DeliveryResult.SUCCESS);
                })
                .build()) {
            e2.start();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (System.nanoTime() < deadline) {
                Intent cur = e2.getIntent(id).orElse(null);
                if (cur != null && cur.getStatus().isTerminal()) break;
                Thread.sleep(20);
            }
            assertEquals(1, delivered.get(),
                "recreated intent must be delivered after restart, not shadowed by terminal tombstone");
        }
    }
}
