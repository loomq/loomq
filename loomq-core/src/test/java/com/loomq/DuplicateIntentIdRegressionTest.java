package com.loomq;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.domain.intent.AckMode;
import com.loomq.domain.intent.Intent;
import java.nio.file.Path;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * R6: createIntent 不校验 intentId 是否已存在——同 id 第二次创建产生双活副本（I1 破坏）：
 * 两个对象同 id 各占一个 wheel 槽、各自入桶 → 同一 intentId 双投递且内容分叉；store
 * 条目被后写者覆写、结算来回抖动；磁盘残留非终态 stale 槽（recovery 按 max revision
 * 去重，败者槽永不回收，桶容量永久泄漏）。修复：创建前按 locationIndex（活 intent
 * 登记表，终态/取消已移除）预检，重复 id 抛 IllegalArgumentException——检查必须在补偿
 * 路径之外，否则 compensateCancel 会把已存在的 intent 覆写成 CANCELED。
 */
class DuplicateIntentIdRegressionTest {

    @TempDir Path tmp;

    @Test
    void duplicateIntentIdMustBeRejected() throws Exception {
        LoomqEngine engine = LoomqEngine.builder().dataDir(tmp.resolve("dup")).build();
        engine.start();
        try {
            Intent first = new Intent("r6-dup-id-0001");
            first.setExecuteAt(Instant.now().plusSeconds(300));
            engine.createIntent(first, AckMode.ASYNC).join();

            Intent second = new Intent("r6-dup-id-0001");
            second.setExecuteAt(Instant.now().plusSeconds(400));
            RuntimeException ex = assertThrows(RuntimeException.class,
                () -> engine.createIntent(second, AckMode.ASYNC).join(),
                "duplicate intentId must be rejected, not create a second live copy");
            assertTrue(hasCause(ex, IllegalArgumentException.class),
                "rejection must be IllegalArgumentException, got: " + ex);
        } finally {
            engine.close();
        }
    }

    private static boolean hasCause(Throwable t, Class<?> type) {
        for (Throwable cur = t; cur != null; cur = cur.getCause()) {
            if (type.isInstance(cur)) return true;
        }
        return false;
    }
}
