package com.loomq.application.command;

import static org.junit.jupiter.api.Assertions.*;

import com.loomq.domain.intent.AckMode;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.domain.intent.WalMode;
import org.junit.jupiter.api.Test;

class ResolveWalModeTest {
    /**
     * H1:BATCH_DEFERRED 在 group-commit 架构下与 ASYNC 语义重合(周期 fsync 已覆盖批量语义),
     * 必须走非 DURABLE 路径(不 awaitCommit),与 ASYNC 一致。
     */
    @Test
    void batchDeferredIsAliasOfAsync() {
        Intent intent = new Intent("intent_rwm000000001");
        intent.setExecuteAt(java.time.Instant.now().plusSeconds(60));
        intent.setPrecisionTier(PrecisionTier.STANDARD);
        intent.transitionTo(IntentStatus.SCHEDULED);
        intent.setWalMode(WalMode.BATCH_DEFERRED);

        // resolveWalMode 是 IntentCommandService 私有方法;经 package-private 测试入口暴露
        WalMode resolved = IntentCommandService.resolveWalModeForTest(intent, null);
        assertNotSame(WalMode.DURABLE, resolved,
            "BATCH_DEFERRED must not become DURABLE (alias of ASYNC under group-commit)");
    }

    /**
     * H1 path-3(tier-default):HIGH 档位默认 walMode=BATCH_DEFERRED(PrecisionTierCatalog:114),
     * 是生产可达的 load-bearing 路径。null ackMode + null intent.walMode 时,resolveWalMode
     * 必须把 tier-default 的 BATCH_DEFERRED 归一为 ASYNC。严格断言 assertSame(ASYNC) 钉死别名契约。
     */
    @Test
    void highTierDefaultBatchDeferredResolvesToAsync() {
        Intent intent = new Intent("intent_rwm000000003");
        intent.setExecuteAt(java.time.Instant.now().plusSeconds(60));
        intent.setPrecisionTier(PrecisionTier.HIGH);   // tier-default = BATCH_DEFERRED
        intent.transitionTo(IntentStatus.SCHEDULED);
        // 不设 walMode、不传 ackMode → 走 path-3 tier-default

        WalMode resolved = IntentCommandService.resolveWalModeForTest(intent, null);
        assertSame(WalMode.ASYNC, resolved,
            "HIGH tier default BATCH_DEFERRED must normalize to ASYNC under group-commit");
    }

    @Test
    void durableAckModeOverridesIntentWalMode() {
        Intent intent = new Intent("intent_rwm000000002");
        intent.setExecuteAt(java.time.Instant.now().plusSeconds(60));
        intent.setPrecisionTier(PrecisionTier.STANDARD);
        intent.transitionTo(IntentStatus.SCHEDULED);
        intent.setWalMode(WalMode.ASYNC);

        WalMode resolved = IntentCommandService.resolveWalModeForTest(intent, AckMode.DURABLE);
        assertSame(WalMode.DURABLE, resolved, "explicit AckMode.DURABLE must win over intent walMode");
    }
}
