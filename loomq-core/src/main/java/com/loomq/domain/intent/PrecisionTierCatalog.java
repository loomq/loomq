package com.loomq.domain.intent;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 精度档位目录。
 *
 * 负责集中管理 preset 配置，避免把调度参数直接焊死在 PrecisionTier 枚举里。
 */
public final class PrecisionTierCatalog {

    private static final PrecisionTierCatalog DEFAULT = createDefault();

    private final EnumMap<PrecisionTier, PrecisionTierProfile> profiles;
    private final List<PrecisionTier> supportedTiers;
    private final PrecisionTier defaultTier;

    private PrecisionTierCatalog(EnumMap<PrecisionTier, PrecisionTierProfile> profiles,
                                 PrecisionTier defaultTier) {
        this.profiles = new EnumMap<>(profiles);
        this.supportedTiers = List.copyOf(this.profiles.keySet());
        this.defaultTier = Objects.requireNonNull(defaultTier, "defaultTier");
        if (!this.profiles.containsKey(this.defaultTier)) {
            throw new IllegalArgumentException("defaultTier must exist in catalog");
        }
    }

    public static PrecisionTierCatalog defaultCatalog() {
        return DEFAULT;
    }

    public static PrecisionTierCatalog of(Map<PrecisionTier, PrecisionTierProfile> profiles,
                                          PrecisionTier defaultTier) {
        Objects.requireNonNull(profiles, "profiles");
        Objects.requireNonNull(defaultTier, "defaultTier");

        EnumMap<PrecisionTier, PrecisionTierProfile> entries = new EnumMap<>(PrecisionTier.class);
        entries.putAll(profiles);
        return new PrecisionTierCatalog(entries, defaultTier);
    }

    public PrecisionTierProfile profile(PrecisionTier tier) {
        PrecisionTier resolvedTier = tier != null && profiles.containsKey(tier)
            ? tier
            : defaultTier;
        return profiles.get(resolvedTier);
    }

    public long precisionWindowMs(PrecisionTier tier) {
        return profile(tier).precisionWindowMs();
    }

    public int maxConcurrency(PrecisionTier tier) {
        return profile(tier).maxConcurrency();
    }

    public int batchSize(PrecisionTier tier) {
        return profile(tier).batchSize();
    }

    public int batchWindowMs(PrecisionTier tier) {
        return profile(tier).batchWindowMs();
    }

    public int consumerCount(PrecisionTier tier) {
        return profile(tier).consumerCount();
    }

    public int dispatchQueueCapacity(PrecisionTier tier) {
        return profile(tier).dispatchQueueCapacity();
    }

    public WalMode walMode(PrecisionTier tier) {
        return profile(tier).walMode();
    }

    public long scanIntervalMs(PrecisionTier tier) {
        return profile(tier).scanIntervalMs();
    }

    public boolean isBatchEnabled(PrecisionTier tier) {
        return profile(tier).isBatchEnabled();
    }

    public boolean isDirectBucket(PrecisionTier tier) { return profile(tier).directBucket(); }
    public boolean isAdaptive(PrecisionTier tier) { return profile(tier).adaptiveScan(); }
    public int maxBuckets(PrecisionTier tier) { return profile(tier).maxBuckets(); }

    /**
     * 按枚举 ordinal 解码精度档位(持久化格式写入 {@code tier.ordinal()})。越界或该档不在
     * 本目录内则回退默认档。
     *
     * <p><b>R13 修正</b>:旧实现用 {@code supportedTiers.get(ordinal)} 按"列表下标"取档,与
     * "枚举 ordinal"混淆——当自定义目录为枚举的非连续子集(如仅 {ULTRA, MILLI})时,
     * ordinal 1 被错读成列表第 1 项(MILLI)而非 FAST、ordinal 3(MILLI)被误判越界回退。
     * 改为 {@code PrecisionTier.values()[ordinal]} 直接映射枚举 ordinal,再校验是否受支持。</p>
     *
     * 【边界】此处不应用 PrecisionTier.LEGACY_REMAPS:新格式 ordinal 2 是 STANDARD,旧 HIGH
     * 也是 ordinal 2(碰撞),重映射会窜改合法新数据。remap 仅对 fromString 的 runtime/config
     * 输入生效;旧 wheel 数据按 v0.9.x ordinal 断裂契约清库,不做兼容读取。
     */
    public PrecisionTier tierByOrdinal(int ordinal) {
        PrecisionTier[] all = PrecisionTier.values();
        if (ordinal < 0 || ordinal >= all.length) {
            return defaultTier;
        }
        PrecisionTier tier = all[ordinal];
        return profiles.containsKey(tier) ? tier : defaultTier;
    }

    public List<PrecisionTier> supportedTiers() {
        return supportedTiers;
    }

    public int tierCount() {
        return supportedTiers.size();
    }

    public PrecisionTier defaultTier() {
        return defaultTier;
    }

    private static PrecisionTierCatalog createDefault() {
        // 四档（v0.9.x 精简：HIGH→FAST(50ms)、ECONOMY→STANDARD(500ms)，画像沿用被保留档）
        EnumMap<PrecisionTier, PrecisionTierProfile> profiles = new EnumMap<>(PrecisionTier.class);
        profiles.put(PrecisionTier.ULTRA, new PrecisionTierProfile(10, 200, 1, 5, 16, 200 * 16,
            WalMode.DURABLE, 10, false, true));
        profiles.put(PrecisionTier.FAST, new PrecisionTierProfile(50, 150, 1, 10, 12, 150 * 16,
            WalMode.DURABLE, 50));
        profiles.put(PrecisionTier.STANDARD, new PrecisionTierProfile(500, 50, 20, 100, 3, 50 * 16,
            WalMode.DURABLE, 500));
        profiles.put(PrecisionTier.MILLI, new PrecisionTierProfile(
            1,          // precisionWindowMs
            100,        // maxConcurrency
            1,          // batchSize（单发模式）
            1,          // batchWindowMs（不适用，填 1 满足 record 校验；MILLI 非批量）
            8,          // consumerCount
            100 * 16,   // dispatchQueueCapacity
            WalMode.DURABLE,
            1,          // scanIntervalMs：adaptive 下语义为"保底 tick 上限"
            true,       // directBucket：cohort 旁路直插桶
            true,       // adaptiveScan：事件驱动扫描
            200_000));  // maxBuckets：内存高水位降级阈值
        return new PrecisionTierCatalog(profiles, PrecisionTier.STANDARD);
    }
}
