package com.loomq.application.scheduler;

import static org.junit.jupiter.api.Assertions.*;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

/**
 * P1-2 极端竞态测试:CAS 成功后 fireNow 不再重注册。
 *
 * <p>场景:scanDue 的 CAS 认领成功(旧 revision 匹配),但在加入 dueIntents 前,
 * fireNow 介入。fireNow 的 removeFromSchedule 返回 false（索引条目已被 CAS 消耗），
 * 跳过 restore -- 不产生新桶条目。
 * <ul>
 *   <li>第一次 scanDue 返回 intent(CAS 已成功) -- 投递一次</li>
 *   <li>fireNow 不重注册 -> 第二次 scanDue 返回空</li>
 * </ul>
 * CAS 认领窗口已确定性关闭:唯一的确定性重复投递向量被消除。
 *
 * <p><b>覆盖边界：</b>本测试固化 BucketGroup 契约（认领后 remove 返回 false + 无重注册），
 * 不直接调用 fireNow——fireNow/updateIntent 认领分支的端到端回归由
 * {@code ClaimedInFlightRaceTest} 承担。</p>
 */
class ScanDueFireNowRaceTest {

    @Test
    void casSucceedsThenFireNowSkipsReRegister_noDuplicateDelivery() {
        BucketGroup group = new BucketGroup(PrecisionTier.STANDARD);
        Intent intent = new Intent("intent_race_test_001");
        intent.setExecuteAt(Instant.now().minusMillis(100)); // past = due
        intent.setPrecisionTier(PrecisionTier.STANDARD);
        intent.transitionTo(IntentStatus.SCHEDULED);

        group.add(intent, intent.getExecuteAt());
        assertEquals(1, group.getPendingCount());

        AtomicBoolean removeResult = new AtomicBoolean();
        // Set up hook: after CAS succeeds, simulate fireNow's removeFromSchedule.
        // Index entry has been consumed by CAS -> BucketGroup.remove MUST return false,
        // and fireNow skips restore -> no re-registration.
        group.testScanPostClaimHook = () -> {
            intent.incrementRevision();  // fireNow increments revision
            removeResult.set(group.remove(intent));  // fireNow's removeFromSchedule
        };

        // First scanDue: CAS succeeds, hook fires, intent added to dueIntents
        List<Intent> due1 = group.scanDue(Instant.now());
        assertEquals(1, due1.size(), "intent should be in dueIntents (CAS succeeded)");
        assertEquals("intent_race_test_001", due1.get(0).getIntentId());
        assertFalse(removeResult.get(),
            "claimed intent must report not-scheduled -- fix contract under test");

        // No new bucket entry -- fireNow skipped restore
        assertEquals(0, group.getPendingCount(),
            "fireNow must not re-register a claimed intent");

        // Second scanDue: nothing to pick up -- no duplicate delivery
        List<Intent> due2 = group.scanDue(Instant.now());
        assertTrue(due2.isEmpty(), "second scanDue must return empty -- no duplicate delivery");

        assertEquals(0, group.getPendingCount(),
            "no pending intents after fix");
    }
}
