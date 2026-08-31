package com.loomq.testutil;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import java.time.Instant;

/**
 * 测试共享 Intent 构造夹具。
 *
 * <p>收敛 4 个测试文件各自实现的 dueIntent/pastIntent(executeAt 默认值 -50/-5/-100ms
 * 各自漂移);时间偏移显式传参,消除"哪个偏移是故意的"的歧义。</p>
 */
public final class TestIntents {

    private TestIntents() {
    }

    /** 已到期(executeAt 显式给定)的 SCHEDULED intent,触发即时投递/flush。 */
    public static Intent due(String id, PrecisionTier tier, Instant executeAt) {
        Intent intent = new Intent(id);
        intent.setExecuteAt(executeAt);
        intent.setPrecisionTier(tier);
        intent.transitionTo(IntentStatus.SCHEDULED);
        return intent;
    }

    /** 已到期(now - pastMs)的 SCHEDULED intent,带 deadline。 */
    public static Intent past(String id, PrecisionTier tier, long pastMs) {
        Intent intent = new Intent(id);
        intent.setExecuteAt(Instant.now().minusMillis(pastMs));
        intent.setDeadline(Instant.now().plusSeconds(3600));
        intent.setPrecisionTier(tier);
        intent.transitionTo(IntentStatus.SCHEDULED);
        return intent;
    }
}
