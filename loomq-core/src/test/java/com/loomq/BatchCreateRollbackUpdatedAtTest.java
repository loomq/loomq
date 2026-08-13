package com.loomq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.loomq.domain.intent.AckMode;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * R14: createIntents 失败回滚把变异后的 {@code intent.getUpdatedAt()} 传给 rollbackStatus,
 * 导致未持久化 intent 回滚后 updatedAt 残留 transitionTo/incrementRevision 的时间戳,
 * 与已还原的 status/revision 不一致。修复:回滚前捕获原始 updatedAt 并还原。
 *
 * <p>触发:批量创建中第 2 条 intent 因 payload 溢出(超 210B)在 persistToWheel 抛
 * SlotOverflowException,第 1 条已写(补偿取消),第 2 条未写(回滚 status/revision/updatedAt)。
 */
class BatchCreateRollbackUpdatedAtTest {

    @TempDir Path tmp;

    @Test
    void rollbackMustRestoreOriginalUpdatedAt() throws Exception {
        try (LoomqEngine engine = LoomqEngine.builder()
                .dataDir(tmp).nodeId("rollback-updated").build()) {
            engine.start();

            Intent first = new Intent("rollback-updated-0001");
            first.setExecuteAt(Instant.now().plusSeconds(300));
            first.setPrecisionTier(PrecisionTier.ULTRA);

            // 第 2 条:超长 idempotencyKey 使 payload 溢出 210B → persistToWheel 抛 SlotOverflow
            Intent second = new Intent("rollback-updated-0002");
            second.setExecuteAt(Instant.now().plusSeconds(300));
            second.setPrecisionTier(PrecisionTier.ULTRA);
            second.setIdempotencyKey("k".repeat(400));

            Instant secondUpdatedAtBefore = second.getUpdatedAt();

            assertThrows(RuntimeException.class,
                () -> engine.createIntents(List.of(first, second), AckMode.ASYNC).join(),
                "batch create must fail on the oversized second intent");

            // 第 2 条未持久化 → 回滚后 status=CREATED、updatedAt 还原到 createIntents 之前的
            // 快照(即第二次 setter 后的值),而非 transitionTo/incrementRevision 变异后的时间戳。
            assertEquals(IntentStatus.CREATED, second.getStatus(),
                "rolled-back intent must return to CREATED");
            assertEquals(secondUpdatedAtBefore, second.getUpdatedAt(),
                "rolled-back updatedAt must be restored to the pre-create snapshot, not the mutated value");
        }
    }
}
