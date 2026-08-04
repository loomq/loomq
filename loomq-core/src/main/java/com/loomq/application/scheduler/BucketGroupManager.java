package com.loomq.application.scheduler;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.domain.intent.PrecisionTierCatalog;
import java.time.Instant;
import java.util.Collection;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 精度桶组管理器
 *
 * 管理所有精度档位的 BucketGroup，提供统一的任务添加和扫描接口。
 *
 * @author loomq
 * @since v0.5.1
 */
public class BucketGroupManager {

    private static final Logger logger = LoggerFactory.getLogger(BucketGroupManager.class);

    /**
     * 按精度档位分组的 BucketGroup
     */
    private final Map<PrecisionTier, BucketGroup> bucketGroups;

    /**
     * 精度档位目录
     */
    private final PrecisionTierCatalog precisionTierCatalog;

    /**
     * 构造函数
     * 初始化所有精度档位的 BucketGroup
     */
    public BucketGroupManager() {
        this(PrecisionTierCatalog.defaultCatalog());
    }

    /**
     * 构造函数
     *
     * @param precisionTierCatalog 精度档位目录
     */
    public BucketGroupManager(PrecisionTierCatalog precisionTierCatalog) {
        this.precisionTierCatalog = precisionTierCatalog;
        this.bucketGroups = new EnumMap<>(PrecisionTier.class);

        for (PrecisionTier tier : precisionTierCatalog.supportedTiers()) {
            bucketGroups.put(tier, new BucketGroup(tier, precisionTierCatalog));
        }

        logger.info("Initialized {} bucket groups for precision tiers", bucketGroups.size());
    }

    /**
     * 添加 Intent 到对应精度档位的桶
     *
     * @param intent Intent 实例
     */
    public BucketGroup.AddResult add(Intent intent) {
        PrecisionTier tier = intent.getPrecisionTier();
        BucketGroup group = bucketGroups.get(tier);
        if (group == null) {
            logger.warn("Unknown precision tier {}, using default tier {}", tier, precisionTierCatalog.defaultTier());
            group = bucketGroups.get(precisionTierCatalog.defaultTier());
        }
        return group.add(intent, intent.getExecuteAt());
    }

    /**
     * Batch-add intents from a cohort flush. Groups by tier first to avoid
     * redundant catalog lookups per intent.
     *
     * <p>这些 intent 是系统已接受的再入路径（此前已入 cohort），强制入桶
     * （addForced），忽略高水位降级——否则高水位档（如 MILLI）在 cohort flush
     * 时 add() 返回 FALLBACK_TO_COHORT 而结果被丢弃，intent 既不在桶也不在
     * cohort，静默丢失。</p>
     *
     * @param intents intents to add
     */
    public void addAll(Collection<Intent> intents) {
        if (intents.isEmpty()) return;

        for (Intent intent : intents) {
            PrecisionTier tier = intent.getPrecisionTier();
            BucketGroup group = bucketGroups.get(tier);
            if (group == null) {
                group = bucketGroups.get(precisionTierCatalog.defaultTier());
            }
            group.addForced(intent, intent.getExecuteAt());
        }
    }

    /**
     * 从对应精度档位的桶中移除 Intent。
     *
     * @param intent Intent 实例
     * @return true 表示已移除
     */
    public boolean remove(Intent intent) {
        PrecisionTier tier = intent.getPrecisionTier();
        BucketGroup group = bucketGroups.get(tier);

        if (group == null) {
            return false;
        }

        return group.remove(intent);
    }

    /**
     * 扫描指定精度档位的到期任务
     *
     * @param tier 精度档位
     * @param now  当前时间
     * @return 到期的 Intent 列表
     */
    public List<Intent> scanDue(PrecisionTier tier, Instant now) {
        BucketGroup group = bucketGroups.get(tier);
        if (group == null) {
            return List.of();
        }
        return group.scanDue(now);
    }

    /**
     * 获取指定精度档位的 BucketGroup
     *
     * @param tier 精度档位
     * @return BucketGroup
     */
    public BucketGroup getBucketGroup(PrecisionTier tier) {
        return bucketGroups.get(tier);
    }

    /**
     * 获取所有精度档位的等待任务数
     *
     * @return 按精度档位分组的任务数
     */
    public Map<PrecisionTier, Integer> getPendingCounts() {
        Map<PrecisionTier, Integer> counts = new EnumMap<>(PrecisionTier.class);

        for (Map.Entry<PrecisionTier, BucketGroup> entry : bucketGroups.entrySet()) {
            counts.put(entry.getKey(), entry.getValue().getPendingCount());
        }

        return counts;
    }

    /**
     * 获取总等待任务数
     *
     * @return 总任务数
     */
    public int getTotalPendingCount() {
        return bucketGroups.values().stream()
            .mapToInt(BucketGroup::getPendingCount)
            .sum();
    }

    /**
     * 清空所有桶
     */
    public void clear() {
        bucketGroups.values().forEach(BucketGroup::clear);
        logger.info("Cleared all bucket groups");
    }
}
