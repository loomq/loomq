package com.loomq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.domain.intent.AckMode;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.spi.DeliveryHandler;
import com.loomq.spi.DeliveryHandler.DeliveryResult;
import com.loomq.testutil.TestStores;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * C4-2/C4-3 同款语义在 updateIntent 上的回归：持久化已提交（mmap + 索引已更新）后，
 * store.update / awaitCommit 失败不得回滚内存调度，否则磁盘与内存分叉，重启后恢复出新调度。
 */
class BugUpdatePostCommitFailureTest {

    @TempDir Path tmp;

    private static final DeliveryHandler SUCCESS =
        i -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS);

    @Test
    void updateCommittedMustNotRollbackOnPostCommitFailure() throws Exception {
        String id = "upd-post-commit-01";
        Path dir = tmp.resolve("u");
        long t0 = System.currentTimeMillis();
        Instant newExecuteAt = Instant.ofEpochMilli(t0 + 120_000);

        try (LoomqEngine engine = LoomqEngine.builder()
                .dataDir(dir).nodeId("u1")
                .intentStore(new TestStores.UpdateThrowingStore())
                .deliveryHandler(SUCCESS)
                .build()) {
            engine.start();
            Intent it = new Intent(id);
            it.setExecuteAt(Instant.ofEpochMilli(t0 + 60_000));
            it.setPrecisionTier(PrecisionTier.ULTRA);
            engine.createIntent(it, AckMode.DURABLE).get();

            // 持久化已提交：新 executeAt 已入 mmap + 索引；store.update 抛错不应让调用方看到回滚。
            Optional<Intent> updated = engine.updateIntent(
                id,
                i -> { },
                newExecuteAt);

            assertTrue(updated.isPresent(), "committed update must report success, not throw");
            assertEquals(newExecuteAt, updated.orElseThrow().getExecuteAt(),
                "committed update must NOT be rolled back by a post-commit failure");
        }

        // 重启后磁盘权威应仍是新调度；若旧实现回滚内存但磁盘已新，这里会读到新时间，
        // 而第一次调用已经抛异常，因此该测试在修复前会失败在 updateIntent 抛异常处。
        try (LoomqEngine engine2 = LoomqEngine.builder()
                .dataDir(dir).nodeId("u2")
                .deliveryHandler(SUCCESS)
                .build()) {
            engine2.start();
            Intent after = engine2.getIntent(id).orElseThrow();
            assertEquals(newExecuteAt, after.getExecuteAt(),
                "restart must observe the committed new schedule, not the rolled-back old one");
        }
    }
}
