package com.loomq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.domain.intent.AckMode;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.spi.DeliveryHandler;
import com.loomq.spi.IntentObserver;
import java.nio.file.Path;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * F1 主回归:重排程过的 Intent 跨两次崩溃不得被幽灵复活重投。
 *
 * <p>无修复时:Engine2 重启后 multiSlotIntents 为空,Intent 被误判单槽,终态槽被回收;
 * Engine3 恢复时只有陈旧 SCHEDULED 兄弟(rev1)幸存,按 max-revision 复活为 SCHEDULED → 重投。</p>
 */
@Tag("integration")
class MultiSlotTombstoneCrashRecoveryTest {

    private static final DeliveryHandler SUCCESS = intent ->
        CompletableFuture.completedFuture(DeliveryHandler.DeliveryResult.SUCCESS);

    private static IntentObserver deliveryCounter(AtomicLong counter) {
        return new IntentObserver() {
            @Override public void onDelivered(Intent i, DeliveryHandler.DeliveryResult r) { counter.incrementAndGet(); }
            @Override public void onScheduled(Intent i) {}
            @Override public void onDeadLettered(Intent i) {}
            @Override public void onExpired(Intent i) {}
            @Override public void onDeliveryFailed(Intent i, Throwable e) {}
        };
    }

    @Test
    void rescheduledIntentIsNotRedeliveredAfterTwoCrashes(@TempDir Path tmp) throws Exception {
        String id = "multi-crash-1";
        long t0 = System.currentTimeMillis();

        // Engine1:创建(rev1 @ +24s)+ 改期到 +12s(rev2,留陈旧 SCHEDULED 兄弟槽),立即关闭
        try (LoomqEngine e1 = LoomqEngine.builder()
                .dataDir(tmp).nodeId("mc-1").deliveryHandler(SUCCESS).build()) {
            e1.start();
            Intent it = new Intent(id);
            it.setExecuteAt(Instant.ofEpochMilli(t0 + 24_000));
            it.setPrecisionTier(PrecisionTier.STANDARD);
            e1.createIntent(it, AckMode.DURABLE).get();
            e1.updateIntent(id, i -> {}, Instant.ofEpochMilli(t0 + 12_000));
        }

        // Engine2:重启,恢复 count=2 → multi 标记;投递 X → ACKED,终态槽保留墓碑
        AtomicLong delivered2 = new AtomicLong();
        try (LoomqEngine e2 = LoomqEngine.builder()
                .dataDir(tmp).nodeId("mc-2").deliveryHandler(SUCCESS).build()) {
            e2.start();
            e2.registerObserver(deliveryCounter(delivered2));
            long deadline = System.currentTimeMillis() + 20_000;
            while (delivered2.get() == 0 && System.currentTimeMillis() < deadline) Thread.sleep(50);
            assertEquals(1, delivered2.get(), "Engine2 应恰好投递一次");
            assertEquals(IntentStatus.ACKED, e2.getIntent(id).orElseThrow().getStatus(),
                "Engine2 中 X 应已 ACKED(终态)");
        }

        // Engine3:再重启,终态胜者(rev3)被跳过 → 不复活、不索引、零投递
        AtomicLong delivered3 = new AtomicLong();
        try (LoomqEngine e3 = LoomqEngine.builder()
                .dataDir(tmp).nodeId("mc-3").deliveryHandler(SUCCESS).build()) {
            e3.start();
            e3.registerObserver(deliveryCounter(delivered3));
            assertTrue(e3.getIntent(id).isEmpty(),
                "终态 intent 不应被恢复载入/索引(否则即幽灵复活)");
            assertEquals(0, e3.getMetricsCollector().getRecoveryOverdueTotal(),
                "X 的槽均在将来,不应有 overdue");
            // 等过陈旧兄弟的 executeAt(+24s),确认零重投
            while (System.currentTimeMillis() < t0 + 24_000 + 1_000) Thread.sleep(50);
            assertEquals(0, delivered3.get(), "已投递 Intent 不得被幽灵复活重投");
        }
    }
}
