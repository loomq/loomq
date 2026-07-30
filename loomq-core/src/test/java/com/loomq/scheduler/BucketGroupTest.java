package com.loomq.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.application.scheduler.BucketGroup;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.PrecisionTier;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * BucketGroup 精度桶组测试
 *
 * @author loomq
 * @since v0.5.1
 */
class BucketGroupTest {

    private BucketGroup standardGroup;
    private BucketGroup economyGroup;

    @BeforeEach
    void setUp() {
        standardGroup = new BucketGroup(PrecisionTier.STANDARD);
        economyGroup = new BucketGroup(PrecisionTier.ECONOMY);
    }

    @Test
    @DisplayName("BucketGroup 初始化正确")
    void testInitialization() {
        assertEquals(PrecisionTier.STANDARD, standardGroup.getTier());
        assertEquals(500, standardGroup.getPrecisionWindowMs());

        assertEquals(PrecisionTier.ECONOMY, economyGroup.getTier());
        assertEquals(1000, economyGroup.getPrecisionWindowMs());

        assertEquals(0, standardGroup.getBucketCount());
        assertEquals(0, standardGroup.getPendingCount());
    }

    @Test
    @DisplayName("添加任务到桶")
    void testAddIntent() {
        Intent intent = createTestIntent(PrecisionTier.STANDARD, Instant.now().plusMillis(5000));
        standardGroup.add(intent, intent.getExecuteAt());

        assertEquals(1, standardGroup.getPendingCount());
        assertTrue(standardGroup.getBucketCount() >= 1);
    }

    @Test
    @DisplayName("扫描到期任务")
    void testScanDueIntents() {
        Instant now = Instant.now();
        Intent intent1 = createTestIntent(PrecisionTier.STANDARD, now.minusMillis(100));
        Intent intent2 = createTestIntent(PrecisionTier.STANDARD, now.plusMillis(1000));

        standardGroup.add(intent1, intent1.getExecuteAt());
        standardGroup.add(intent2, intent2.getExecuteAt());

        List<Intent> dueIntents = standardGroup.scanDue(now);

        assertEquals(1, dueIntents.size());
        assertEquals(intent1.getIntentId(), dueIntents.get(0).getIntentId());
    }

    @Test
    @DisplayName("扫描移除到期桶")
    void testScanRemovesDueBuckets() {
        Instant now = Instant.now();
        Intent intent = createTestIntent(PrecisionTier.STANDARD, now.minusMillis(1000));
        standardGroup.add(intent, intent.getExecuteAt());

        assertEquals(1, standardGroup.getPendingCount());

        standardGroup.scanDue(now);

        assertEquals(0, standardGroup.getPendingCount());
    }

    @Test
    @DisplayName("按 intentId 删除任务")
    void testRemoveIntent() {
        Instant executeAt = Instant.now().plusMillis(1000);
        Intent intent = createTestIntent(PrecisionTier.STANDARD, executeAt);
        standardGroup.add(intent, intent.getExecuteAt());

        assertTrue(standardGroup.remove(intent));
        assertEquals(0, standardGroup.getPendingCount());
        assertEquals(0, standardGroup.getBucketCount());
    }


    @Test
    @DisplayName("清空桶")
    void testClear() {
        Intent intent = createTestIntent(PrecisionTier.STANDARD, Instant.now().plusMillis(5000));
        standardGroup.add(intent, intent.getExecuteAt());

        assertTrue(standardGroup.getPendingCount() > 0);

        standardGroup.clear();

        assertEquals(0, standardGroup.getPendingCount());
        assertEquals(0, standardGroup.getBucketCount());
    }

    @Test
    @DisplayName("多个任务入同一桶")
    void testMultipleIntentsInSameBucket() {
        Instant baseTime = Instant.parse("2026-04-09T12:30:00.000Z");
        Intent intent1 = createTestIntent(PrecisionTier.STANDARD, baseTime.plusMillis(100));
        Intent intent2 = createTestIntent(PrecisionTier.STANDARD, baseTime.plusMillis(400));

        // 两个任务的执行时间在同一个精度窗口内（500ms）
        standardGroup.add(intent1, intent1.getExecuteAt());
        standardGroup.add(intent2, intent2.getExecuteAt());

        // 应该在同一个桶内
        assertEquals(1, standardGroup.getBucketCount());
        assertEquals(2, standardGroup.getPendingCount());
    }

    @Test
    @DisplayName("不同时间桶的任务分离")
    void testDifferentBuckets() {
        Instant baseTime = Instant.parse("2026-04-09T12:30:00.000Z");
        Intent intent1 = createTestIntent(PrecisionTier.STANDARD, baseTime.plusMillis(100));
        Intent intent2 = createTestIntent(PrecisionTier.STANDARD, baseTime.plusMillis(600));

        // 两个任务的执行时间跨越精度窗口边界（500ms）
        standardGroup.add(intent1, intent1.getExecuteAt());
        standardGroup.add(intent2, intent2.getExecuteAt());

        // 应该在不同的桶内
        assertEquals(2, standardGroup.getBucketCount());
        assertEquals(2, standardGroup.getPendingCount());
    }

    // ========== 辅助方法 ==========

    private Intent createTestIntent(PrecisionTier tier, Instant executeAt) {
        Intent intent = new Intent();
        intent.setPrecisionTier(tier);
        intent.setExecuteAt(executeAt);
        return intent;
    }
}
