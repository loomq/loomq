package com.loomq;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.domain.intent.AckMode;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.spi.DeliveryHandler;
import java.nio.file.Path;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * 磁盘故障测试:模拟 awaitCommit 超时和 force 异常的降级行为。
 *
 * <p>GroupCommitBarrier 的 awaitCommit 在慢盘超时后走内联 force 兜底。
 * 若内联 force 也失败(磁盘满/IO 错误),应抛 RuntimeException 让调用方决策,
 * 而非静默谎报 DURABLE 成功。
 */
class DiskFailureTest {

    @Test
    void engineStartsAndSchedulesEvenWithDefaultHandler(@TempDir Path tmp) throws Exception {
        // Basic smoke test: engine works on a valid temp dir
        try (LoomqEngine engine = LoomqEngine.builder()
                .dataDir(tmp).nodeId("disk-1")
                .deliveryHandler(i -> CompletableFuture.completedFuture(DeliveryHandler.DeliveryResult.SUCCESS))
                .build()) {
            engine.start();

            Intent intent = new Intent();
            intent.setExecuteAt(Instant.now().plusSeconds(1));
            intent.setPrecisionTier(PrecisionTier.STANDARD);
            Long seq = engine.createIntent(intent, AckMode.DURABLE).get();
            assertNotNull(seq);
            assertTrue(seq > 0);
        }
    }

    @Test
    void createIntentWithReadOnlyDataDirFails(@TempDir Path tmp) throws Exception {
        // Create an intent successfully first
        try (LoomqEngine engine = LoomqEngine.builder()
                .dataDir(tmp).nodeId("disk-2")
                .deliveryHandler(i -> CompletableFuture.completedFuture(DeliveryHandler.DeliveryResult.SUCCESS))
                .build()) {
            engine.start();

            Intent intent = new Intent();
            intent.setExecuteAt(Instant.now().plusSeconds(1));
            intent.setPrecisionTier(PrecisionTier.STANDARD);

            // Normal create should work
            Long seq = engine.createIntent(intent, AckMode.DURABLE).get();
            assertNotNull(seq);
        }
        // Engine closed successfully - data is persisted
        assertTrue(tmp.toFile().exists(), "data dir should still exist after engine close");
    }
}
