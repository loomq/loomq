package com.loomq.application.scheduler;

import static org.junit.jupiter.api.Assertions.*;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * P1-2 极端竞态测试:CAS 成功后 fireNow 重注册。
 *
 * <p>场景:scanDue 的 CAS 认领成功(旧 revision 匹配),但在加入 dueIntents 前,
 * fireNow 介入并重新注册了 intent(新 revision)。此时:
 * <ul>
 *   <li>第一次 scanDue 返回 intent(CAS 已成功) -- 投递一次</li>
 *   <li>fireNow 的 add 把 intent 放入新桶 -- 第二次 scanDue 再次投递</li>
 * </ul>
 * 这是 at-least-once 语义的固有边界。防御由消费者的终态守卫
 * (isTerminal check before deliverAsync) + finalizeIntent 的 synchronized(intent)
 * 串行化兜底。本测试固化此行为,确保即使极端竞态发生,系统行为也是可预测的。
 */
class ScanDueFireNowRaceTest {

    @Test
    void casSucceedsThenFireNowReRegisters_intentAppearsTwiceButPredictably() {
        BucketGroup group = new BucketGroup(PrecisionTier.STANDARD);
        Intent intent = new Intent("intent_race_test_001");
        intent.setExecuteAt(Instant.now().minusMillis(100)); // past = due
        intent.setPrecisionTier(PrecisionTier.STANDARD);
        intent.transitionTo(IntentStatus.SCHEDULED);

        group.add(intent, intent.getExecuteAt());
        assertEquals(1, group.getPendingCount());

        // Set up hook: after CAS succeeds, simulate fireNow re-register
        group.testScanPostClaimHook = () -> {
            intent.incrementRevision();  // fireNow increments revision
            group.add(intent, intent.getExecuteAt());  // fireNow re-registers
        };

        // First scanDue: CAS succeeds (old revision matches), hook fires (re-register),
        // intent is added to dueIntents
        List<Intent> due1 = group.scanDue(Instant.now());
        assertEquals(1, due1.size(), "intent should be in dueIntents (CAS succeeded before fireNow)");
        assertEquals("intent_race_test_001", due1.get(0).getIntentId());

        // The intent is ALSO in a new bucket (from fireNow's add in the hook)
        assertEquals(1, group.getPendingCount(),
            "intent should also be in new bucket (fireNow re-registered during scan)");

        // Second scanDue: picks up the re-registered intent from the new bucket
        List<Intent> due2 = group.scanDue(Instant.now());
        assertEquals(1, due2.size(), "re-registered intent should be picked up by second scanDue");

        // Total: intent was returned twice (once from each scanDue)
        // This is the at-least-once scenario -- defense is at consumer level:
        // - Terminal state guard before deliverAsync
        // - synchronized(intent) in finalizeIntent prevents state corruption
        assertEquals(0, group.getPendingCount(),
            "all buckets should be drained after two scanDue calls");
    }
}
