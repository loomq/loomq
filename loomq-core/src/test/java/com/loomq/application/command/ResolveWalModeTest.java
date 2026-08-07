package com.loomq.application.command;

import static org.junit.jupiter.api.Assertions.*;

import com.loomq.domain.intent.AckMode;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.domain.intent.WalMode;
import org.junit.jupiter.api.Test;

class ResolveWalModeTest {
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

    /**
     * path-3(tier-default):null ackMode + null intent.walMode 时回退 tier 默认档。
     * v0.9.x 精简后 WalMode 仅剩 ASYNC/DURABLE,STANDARD 档默认 DURABLE → 严格断言钉死契约。
     */
    @Test
    void tierDefaultWalModeAppliesWhenIntentAndAckModeUnset() {
        Intent intent = new Intent("intent_rwm_default");
        intent.setExecuteAt(java.time.Instant.now().plusSeconds(60));
        intent.setPrecisionTier(PrecisionTier.STANDARD);
        intent.transitionTo(IntentStatus.SCHEDULED);
        // 不设 walMode、不传 ackMode → 走 tier-default 路径
        WalMode resolved = IntentCommandService.resolveWalModeForTest(intent, null);
        assertSame(WalMode.DURABLE, resolved, "STANDARD tier default is DURABLE");
    }

    /**
     * v0.9.x 精简副作用（PrecisionTier.java:96-97 文档化）：存量 "HIGH" 配置经 fromString
     * remap 到 FAST 档，Fast 默认 walMode 为 DURABLE——实际落盘强度从旧 HIGH 的
     * BATCH_DEFERRED(≈ASYNC) 升为持久(方向更安全)。钉死该契约，防止未来把 FAST 默认改回
     * ASYNC 而静默破坏"旧 HIGH 配置变强持久"的语义。
     */
    @Test
    void legacyHighConfigRemapsToFastAndResolvesDurable() {
        Intent intent = new Intent("intent_rwm_legacy_high");
        intent.setExecuteAt(java.time.Instant.now().plusSeconds(60));
        // 存量 "HIGH" 配置经 fromString remap → FAST
        intent.setPrecisionTier(PrecisionTier.fromString("HIGH"));
        intent.transitionTo(IntentStatus.SCHEDULED);
        // 不设 walMode、不传 ackMode → path-3 tier-default
        WalMode resolved = IntentCommandService.resolveWalModeForTest(intent, null);
        assertSame(PrecisionTier.FAST, intent.getPrecisionTier(), "HIGH must remap to FAST");
        assertSame(WalMode.DURABLE, resolved, "remapped FAST tier default is DURABLE");
    }
}
