package com.loomq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.domain.intent.AckMode;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.spi.DeliveryHandler;
import com.loomq.spi.DeliveryHandler.DeliveryResult;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * PHTW(持久化分层时间轮)崩溃恢复测试。
 *
 * <p>验证引擎关闭后重启,未到期的 DURABLE Intent 能从磁盘时间轮扫描恢复:
 * <ol>
 *   <li>engine1 创建一个 DURABLE Intent(执行时间 +60s,落在 day-wheel 热窗口内)</li>
 *   <li>关闭 engine1(group-commit msync + 强制脏桶落盘)</li>
 *   <li>以同一 walDir 重新打开 engine2 —— 内存 IntentStore 为空,
 *       仅靠 {@code WheelRecovery.recover()} 扫描磁盘槽位重建</li>
 *   <li>断言 Intent 被恢复:存在、状态 SCHEDULED、executeAt 一致、pendingCount ≥ 1</li>
 * </ol>
 *
 * <p>标记 {@code slow}:涉及完整引擎启停与磁盘 mmap 落盘,不纳入 fast-tests CI 门禁。
 */
@Tag("slow")
class LoomqEnginePhtwRecoveryTest {

    @TempDir
    Path tmp;

    /** 投递处理器:全部判为 DEAD_LETTER,避免恢复后意图被实际投递而离开 SCHEDULED 态。 */
    private static final DeliveryHandler DEAD_LETTER_HANDLER =
        intent -> CompletableFuture.completedFuture(DeliveryResult.DEAD_LETTER);

    @Test
    void durableIntentSurvivesRestartViaWheelRecovery() throws Exception {
        String intentId;
        Instant executeAt = Instant.now().plusSeconds(60);

        // ---- engine1: 创建并持久化一个 DURABLE Intent ----
        try (LoomqEngine engine1 = LoomqEngine.builder()
            .walDir(tmp)
            .nodeId("phtw-recovery-node")
            .deliveryHandler(DEAD_LETTER_HANDLER)
            .build()) {
            engine1.start();

            Intent intent = new Intent();
            intentId = intent.getIntentId();
            intent.setExecuteAt(executeAt);

            long seq = engine1.createIntent(intent, AckMode.DURABLE).join();
            assertTrue(seq > 0, "createIntent 应返回正序列号");

            // 创建后立即可见(热路径已载入内存)
            Optional<Intent> before = engine1.getIntent(intentId);
            assertTrue(before.isPresent());
            assertEquals(IntentStatus.SCHEDULED, before.get().getStatus());
        }
        // engine1 已关闭:group-commit 已 msync,wheelStore.close() 强制脏桶落盘

        // ---- engine2: 同一 walDir 重启,内存存储为空,仅靠磁盘恢复 ----
        try (LoomqEngine engine2 = LoomqEngine.builder()
            .walDir(tmp)
            .nodeId("phtw-recovery-node")
            .deliveryHandler(DEAD_LETTER_HANDLER)
            .build()) {
            engine2.start();

            Optional<Intent> restored = engine2.getIntent(intentId);
            assertTrue(restored.isPresent(), "重启后 Intent 应由 WheelRecovery 从磁盘恢复");
            Intent recovered = restored.get();
            assertNotNull(recovered);
            assertEquals(intentId, recovered.getIntentId());
            assertEquals(IntentStatus.SCHEDULED, recovered.getStatus(),
                "恢复后状态应为 SCHEDULED(尚未到期)");
            assertEquals(executeAt, recovered.getExecuteAt(),
                "恢复后 executeAt 应与写入一致");

            assertTrue(engine2.getStats().pendingCount() >= 1,
                "pendingCount 应至少包含该恢复的 Intent");
        }
    }
}
