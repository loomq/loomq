package com.loomq.infrastructure.wheel;

import com.loomq.config.ConfigSupport;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.domain.intent.PrecisionTierCatalog;
import java.util.Objects;
import java.util.Properties;

/**
 * 持久化分层时间轮配置。
 *
 * 四轮视界:sec(60s) / min(60min) / hour(24h) / day({@code horizonDays}d)。
 * 超过 day 视界的 Intent 落 tail。
 */
public record WheelConfig(
    String dataDir,
    String shardId,
    int horizonDays,
    int slotsPerBucket,
    long groupCommitIntervalMs,
    long awaitCommitTimeoutMs,
    long hotBoundaryMs,
    long promotionLeadMs,
    PrecisionTier defaultTier,
    long bucketRetentionMs,
    long compactionThresholdBytes
) {
    public WheelConfig {
        dataDir = requireText(dataDir, "dataDir");
        shardId = requireText(shardId, "shardId");
        requirePositive(horizonDays, "horizonDays");
        requirePositive(slotsPerBucket, "slotsPerBucket");
        requirePositive(groupCommitIntervalMs, "groupCommitIntervalMs");
        requirePositive(awaitCommitTimeoutMs, "awaitCommitTimeoutMs");
        requirePositive(hotBoundaryMs, "hotBoundaryMs");
        requirePositive(promotionLeadMs, "promotionLeadMs");
        requirePositive(bucketRetentionMs, "bucketRetentionMs");
        requirePositive(compactionThresholdBytes, "compactionThresholdBytes");
        defaultTier = defaultTier != null ? defaultTier : PrecisionTierCatalog.defaultCatalog().defaultTier();
    }

    /** Backward-compatible 9-arg constructor: defaults bucketRetentionMs and compactionThresholdBytes. */
    public WheelConfig(String dataDir, String shardId, int horizonDays, int slotsPerBucket,
                       long groupCommitIntervalMs, long awaitCommitTimeoutMs,
                       long hotBoundaryMs, long promotionLeadMs, PrecisionTier defaultTier) {
        this(dataDir, shardId, horizonDays, slotsPerBucket, groupCommitIntervalMs,
             awaitCommitTimeoutMs, hotBoundaryMs, promotionLeadMs, defaultTier,
             (long) horizonDays * 24 * 60 * 60_000L + 24 * 60 * 60_000L,  // horizon + 1 day safety margin
             512L * 1024 * 1024);  // 512 MB
    }

    public static WheelConfig defaultConfig() {
        return new WheelConfig("./data/wheel", "shard-0", 30, 1024, 1, 10_000L,
            60L * 60_000L, 60_000L,
            PrecisionTierCatalog.defaultCatalog().defaultTier());
    }

    public static WheelConfig fromProperties(Properties props) {
        Properties p = props == null ? new Properties() : props;
        return new WheelConfig(
            ConfigSupport.string(p, "./data/wheel", "wheel.data_dir", "wheel.dataDir"),
            ConfigSupport.string(p, "shard-0", "wheel.shard_id", "wheel.shardId"),
            ConfigSupport.intValue(p, 30, "wheel.horizon_days", "wheel.horizonDays"),
            ConfigSupport.intValue(p, 1024, "wheel.slots_per_bucket", "wheel.slotsPerBucket"),
            ConfigSupport.longValue(p, 1, "wheel.group_commit_interval_ms", "wheel.groupCommitIntervalMs"),
            ConfigSupport.longValue(p, 10_000L, "wheel.await_commit_timeout_ms", "wheel.awaitCommitTimeoutMs"),
            ConfigSupport.longValue(p, 60L * 60_000L, "wheel.hot_boundary_ms", "wheel.hotBoundaryMs"),
            ConfigSupport.longValue(p, 60_000L, "wheel.promotion_lead_ms", "wheel.promotionLeadMs"),
            PrecisionTier.fromString(ConfigSupport.string(p, "STANDARD", "wheel.default_tier", "wheel.defaultTier")));
    }

    public WheelConfig withDataDir(String dir) {
        return new WheelConfig(dir, shardId, horizonDays, slotsPerBucket, groupCommitIntervalMs,
            awaitCommitTimeoutMs, hotBoundaryMs, promotionLeadMs, defaultTier,
            bucketRetentionMs, compactionThresholdBytes);
    }

    private static String requireText(String value, String fieldName) {
        String checked = Objects.requireNonNull(value, fieldName + " cannot be null");
        if (checked.isBlank()) {
            throw new IllegalArgumentException(fieldName + " cannot be blank");
        }
        return checked;
    }

    private static void requirePositive(int v, String name) { if (v <= 0) throw new IllegalArgumentException(name + " must be positive"); }
    private static void requirePositive(long v, String name) { if (v <= 0) throw new IllegalArgumentException(name + " must be positive"); }
}
