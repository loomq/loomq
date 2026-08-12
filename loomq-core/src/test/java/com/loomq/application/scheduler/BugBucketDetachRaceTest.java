package com.loomq.application.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.domain.intent.PrecisionTierCatalog;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * 摘桶竞态回归：scanDue 摘除到期桶期间，并发 add() 不得把条目写入游离桶。
 *
 * <p>add() 旧实现 computeIfAbsent（取桶）与 put（入桶）两步非原子：scanDue 的
 * buckets.remove 恰在两步之间执行时，条目落入已摘除的游离桶，而 intentIndex 的
 * ClaimEntry 已写入——intent 既不在桶也不可被认领，永不投递（静默丢失，直到重启）。</p>
 *
 * <p>修复：取桶 + put 合并为 buckets.compute 一次原子操作——条目要么在 map 内
 * （下次扫描认领），要么在 scanDue 已摘除并正在迭代的桶内（本 cycle 认领），
 * 不存在"游离"状态。</p>
 *
 * <p>用 testAddPostComputeHook 确定性模拟 scanDue 摘桶窗口（同 testScanPostClaimHook
 * 的测试钩子哲学）。</p>
 */
class BugBucketDetachRaceTest {

    @Test
    void addDuringBucketDetachMustNotLoseIntent() throws Exception {
        BucketGroup group = new BucketGroup(PrecisionTier.ULTRA, PrecisionTierCatalog.defaultCatalog());
        long nowMs = System.currentTimeMillis();
        Instant now = Instant.ofEpochMilli(nowMs);

        // 先入一个 intent 建桶
        Intent a = new Intent("detach-a");
        a.setExecuteAt(now);
        a.transitionTo(IntentStatus.SCHEDULED);
        a.incrementRevision();
        group.add(a, a.getExecuteAt());

        // 钩子：模拟 scanDue 摘桶窗口。修复前（取桶与 put 两步非原子）：b 尚未入桶
        // → 摘除该桶使后续 put 落入游离桶；修复后（取桶+put 原子）：b 已入桶 →
        // 不动，b 留在 map 内被下次扫描认领。条件摘桶使测试对两种实现都确定性判别。
        CountDownLatch hookFired = new CountDownLatch(1);
        group.testAddPostComputeHook = () -> {
            if (!group.testBucketContains(now, "detach-b")) {
                group.testDetachBucket(now);
            }
            hookFired.countDown();
        };
        Intent b = new Intent("detach-b");
        b.setExecuteAt(now);
        b.transitionTo(IntentStatus.SCHEDULED);
        b.incrementRevision();
        BucketGroup.AddResult r = group.add(b, b.getExecuteAt());
        assertTrue(hookFired.await(3, TimeUnit.SECONDS), "hook must fire");
        group.testAddPostComputeHook = null;

        assertEquals(BucketGroup.AddResult.ADDED, r);
        // 修复前：b 落入游离桶 → scanDue 永不返回 b（assertTrue 失败）
        List<Intent> due = group.scanDue(Instant.ofEpochMilli(nowMs + 1));
        assertTrue(due.stream().anyMatch(i -> "detach-b".equals(i.getIntentId())),
            "b 不得因摘桶竞态丢失");
    }
}
