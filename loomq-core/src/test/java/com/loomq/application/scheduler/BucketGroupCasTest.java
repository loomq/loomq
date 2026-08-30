package com.loomq.application.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import java.time.Instant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * scanDue CAS 认领协议机制测试。
 *
 * <p>验证 scanDue 的原子认领:CAS 期望值从 BucketEntry.revisionAtAdd 取得(不可变快照),
 * 而非从可变 Intent 或当前索引状态读取。fireNow 重注册后 revision 递增,
 * 旧桶条目的 CAS 必失败,杜绝"同数值 bucketKey 骗过条件删除"的确定性漏洞。
 */
class BucketGroupCasTest {

    @Test
    @DisplayName("CAS 认领: fireNow 重注册后旧 revision 的 CAS 失败")
    void testCasPreventsFalseClaimAfterReRegister() {
        BucketGroup group = new BucketGroup(PrecisionTier.STANDARD);
        Instant executeAt = Instant.parse("2026-07-31T12:00:00.000Z");
        Intent intent = new Intent("intent_cas_test_0001");
        intent.setExecuteAt(executeAt);
        intent.setPrecisionTier(PrecisionTier.STANDARD);
        intent.transitionTo(IntentStatus.SCHEDULED);

        // Add intent (revision 0)
        group.add(intent, executeAt);
        assertEquals(1, group.getPendingCount());

        // Compute bucketKey (STANDARD precision window = 500ms)
        long bucketKey = (executeAt.toEpochMilli() / 500) * 500;

        // Simulate scanDue's bucket detachment (buckets.remove)
        assertTrue(group.testDetachBucket(executeAt),
            "testDetachBucket should find and remove the bucket");

        // Simulate fireNow: re-register with incremented revision
        intent.incrementRevision();  // revision 1
        group.add(intent, executeAt);  // creates new bucket with revision 1

        // CAS with old revision (0) must fail - this is the bug fix
        assertFalse(group.testClaim(intent.getIntentId(), bucketKey, 0),
            "CAS with stale revision must fail after re-registration");

        // CAS with current revision (1) must succeed - normal claim works
        assertTrue(group.testClaim(intent.getIntentId(), bucketKey, 1),
            "CAS with current revision must succeed");
    }
}
