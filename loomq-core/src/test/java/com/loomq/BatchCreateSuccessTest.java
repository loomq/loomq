package com.loomq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.domain.intent.AckMode;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * createIntents 批量成功路径语义测试。
 *
 * <p>techdebt D3:批量路径仅失败回滚有测试(BatchCreateRollbackUpdatedAtTest),成功路径
 * (多条全落盘、重启恢复)从未直接断言——核心写入 API 的正面语义无保护。</p>
 */
class BatchCreateSuccessTest {

    @TempDir Path tmp;

    private static Intent intent(String id) {
        Intent it = new Intent(id);
        it.setExecuteAt(Instant.now().plusSeconds(300));
        it.setPrecisionTier(PrecisionTier.ULTRA);
        return it;
    }

    @Test
    void batchCreatePersistsAllAndSurvivesRestart() throws Exception {
        List<String> ids = new ArrayList<>();
        try (LoomqEngine engine = LoomqEngine.builder()
                .dataDir(tmp).nodeId("batch-success").build()) {
            engine.start();

            List<Intent> batch = new ArrayList<>();
            for (int i = 1; i <= 5; i++) {
                Intent it = intent(String.format("batch_success_%02d", i));
                batch.add(it);
                ids.add(it.getIntentId());
            }

            List<Long> seqs = engine.createIntents(batch, AckMode.ASYNC).join();
            assertEquals(5, seqs.size(), "batch create must return a sequence per intent");

            for (String id : ids) {
                Intent cur = engine.getIntent(id).orElseThrow();
                assertEquals(IntentStatus.SCHEDULED, cur.getStatus(),
                    "all batch-created intents must be SCHEDULED");
            }
        }

        // 重启恢复:全部 5 条仍在磁盘权威中
        try (LoomqEngine engine2 = LoomqEngine.builder()
                .dataDir(tmp).nodeId("batch-success").build()) {
            engine2.start();
            for (String id : ids) {
                assertTrue(engine2.getIntent(id).isPresent(),
                    "batch-created intent " + id + " must survive restart");
            }
        }
    }
}
