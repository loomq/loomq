package com.loomq.application.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.domain.intent.PrecisionTierCatalog;
import com.loomq.domain.intent.PrecisionTierProfile;
import com.loomq.domain.intent.WalMode;
import java.time.Instant;
import java.util.EnumMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Phase 1 增量 API 测试：earliestBucketKey / BucketAddListener / AddResult 高水位降级。
 *
 * <p>覆盖：空表/单桶/多桶 earliestBucketKey；监听器仅新桶触发；maxBuckets 用尽后
 * 新桶返回 FALLBACK_TO_COHORT；普通 add 返回 ADDED。</p>
 */
class BucketGroupAdaptiveApiTest {

    private static Intent intent(String id, long epochMs) {
        Intent intent = new Intent(id);
        intent.setExecuteAt(Instant.ofEpochMilli(epochMs));
        intent.setPrecisionTier(PrecisionTier.STANDARD);
        intent.transitionTo(IntentStatus.SCHEDULED);
        return intent;
    }

    @Test
    @DisplayName("earliestBucketKey: 空表返回 null")
    void earliestKeyEmptyIsNull() {
        BucketGroup group = new BucketGroup(PrecisionTier.STANDARD);
        assertNull(group.earliestBucketKey());
    }

    @Test
    @DisplayName("earliestBucketKey: 单桶返回该桶 key")
    void earliestKeySingle() {
        BucketGroup group = new BucketGroup(PrecisionTier.STANDARD);
        long epochMs = 1000L;
        group.add(intent("k1", epochMs), Instant.ofEpochMilli(epochMs));
        // STANDARD precision window = 500ms
        assertEquals((epochMs / 500) * 500, group.earliestBucketKey());
    }

    @Test
    @DisplayName("earliestBucketKey: 多桶返回最小 key")
    void earliestKeyMulti() {
        BucketGroup group = new BucketGroup(PrecisionTier.STANDARD);
        group.add(intent("k1", 3000L), Instant.ofEpochMilli(3000L));
        group.add(intent("k2", 1000L), Instant.ofEpochMilli(1000L));
        group.add(intent("k3", 2000L), Instant.ofEpochMilli(2000L));
        assertEquals(1000L, group.earliestBucketKey());
    }

    @Test
    @DisplayName("setBucketAddListener: 仅新桶触发，已有桶加成员不触发")
    void listenerOnlyOnNewBucket() {
        BucketGroup group = new BucketGroup(PrecisionTier.STANDARD);
        AtomicInteger fireCount = new AtomicInteger();
        group.setBucketAddListener(key -> fireCount.incrementAndGet());

        // 首个 add 创建新桶 -> 触发
        group.add(intent("a1", 1000L), Instant.ofEpochMilli(1000L));
        assertEquals(1, fireCount.get());

        // 同桶加成员 -> 不触发
        group.add(intent("a2", 1000L), Instant.ofEpochMilli(1000L));
        assertEquals(1, fireCount.get());

        // 新桶 -> 触发
        group.add(intent("b1", 2000L), Instant.ofEpochMilli(2000L));
        assertEquals(2, fireCount.get());
    }

    @Test
    @DisplayName("add 正常路径返回 ADDED")
    void addReturnsAdded() {
        BucketGroup group = new BucketGroup(PrecisionTier.STANDARD);
        assertEquals(BucketGroup.AddResult.ADDED,
            group.add(intent("a1", 1000L), Instant.ofEpochMilli(1000L)));
    }

    @Test
    @DisplayName("高水位: maxBuckets 用尽后新桶 → FALLBACK_TO_COHORT")
    void highWaterFallback() {
        EnumMap<PrecisionTier, PrecisionTierProfile> profiles = new EnumMap<>(PrecisionTier.class);
        profiles.put(PrecisionTier.STANDARD,
            new PrecisionTierProfile(500, 50, 20, 100, 3, 50 * 16, WalMode.DURABLE, 500,
                false, false, 2));
        PrecisionTierCatalog catalog = PrecisionTierCatalog.of(profiles, PrecisionTier.STANDARD);
        BucketGroup group = new BucketGroup(PrecisionTier.STANDARD, catalog);
        assertEquals(2, group.getMaxBuckets());

        assertEquals(BucketGroup.AddResult.ADDED,
            group.add(intent("a1", 1000L), Instant.ofEpochMilli(1000L)));
        assertEquals(BucketGroup.AddResult.ADDED,
            group.add(intent("b1", 2000L), Instant.ofEpochMilli(2000L)));
        // 第 3 个新桶触发高水位降级
        assertEquals(BucketGroup.AddResult.FALLBACK_TO_COHORT,
            group.add(intent("c1", 3000L), Instant.ofEpochMilli(3000L)));

        // 桶数保持 2，降级 intent 未入桶
        assertEquals(2, group.getBucketCount());
        assertEquals(2, group.getPendingCount());
    }

    @Test
    @DisplayName("P3-2: 高水位降级将 intent 移出旧桶并清空索引（无双注册、无 stale index）")
    void highWaterFallbackMovesIntentOutOfBucket() {
        EnumMap<PrecisionTier, PrecisionTierProfile> profiles = new EnumMap<>(PrecisionTier.class);
        profiles.put(PrecisionTier.STANDARD,
            new PrecisionTierProfile(500, 50, 20, 100, 3, 50 * 16, WalMode.DURABLE, 500,
                false, false, 2));
        PrecisionTierCatalog catalog = PrecisionTierCatalog.of(profiles, PrecisionTier.STANDARD);
        BucketGroup group = new BucketGroup(PrecisionTier.STANDARD, catalog);

        // a1、a2 同桶 1000；b1 桶 2000 → 桶数已满（maxBuckets=2）
        group.add(intent("a1", 1000L), Instant.ofEpochMilli(1000L));
        group.add(intent("a2", 1100L), Instant.ofEpochMilli(1100L));
        group.add(intent("b1", 2000L), Instant.ofEpochMilli(2000L));

        // reschedule a1 到新桶 3000（新桶 + 桶数已满）→ 降级，a1 被移出旧桶 1000（归 cohort）
        assertEquals(BucketGroup.AddResult.FALLBACK_TO_COHORT,
            group.add(intent("a1", 3000L), Instant.ofEpochMilli(3000L)));

        // a1 已移出旧桶 1000：pendingCount=2（a2+b1），桶数仍 2，无双注册
        assertEquals(2, group.getPendingCount());
        assertEquals(2, group.getBucketCount());
    }

    @Test
    @DisplayName("P3-4: earliestExecuteAt 返回最早桶内最小精确 executeAt")
    void earliestExecuteAtReturnsMinExact() {
        BucketGroup group = new BucketGroup(PrecisionTier.STANDARD);
        // 同一桶(500ms 窗口=桶 1000)内两个不同 executeAt
        group.add(intent("a1", 1050L), Instant.ofEpochMilli(1050L));
        group.add(intent("a2", 1020L), Instant.ofEpochMilli(1020L));
        // 更晚桶 → 桶 2000
        group.add(intent("b1", 2000L), Instant.ofEpochMilli(2000L));
        assertEquals(1020L, group.earliestExecuteAt());
    }

    @Test
    @DisplayName("P3-4: earliestExecuteAt 空表返回 null")
    void earliestExecuteAtEmptyIsNull() {
        BucketGroup group = new BucketGroup(PrecisionTier.STANDARD);
        assertNull(group.earliestExecuteAt());
    }
}