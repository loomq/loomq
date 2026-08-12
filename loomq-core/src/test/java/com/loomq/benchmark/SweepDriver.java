package com.loomq.benchmark;

import com.loomq.domain.intent.*;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;

/** 扫参驱动器：把 "sweep.consumers=4,8,16,32" 系统属性解析成覆盖版 catalog 序列。 */
public final class SweepDriver {

    /** 解析形如 "4,8,16,32" 的逗号分隔正整数列表。 */
    public static List<Integer> parseValues(String csv) {
        List<Integer> out = new ArrayList<>();
        for (String s : csv.split(",")) {
            int v = Integer.parseInt(s.trim());
            if (v <= 0) throw new IllegalArgumentException("sweep value must be positive: " + v);
            out.add(v);
        }
        if (out.isEmpty()) throw new IllegalArgumentException("empty sweep values");
        return out;
    }

    /** 对指定档位，为每个值构造一个覆盖该参数的 catalog。 */
    public static List<PrecisionTierCatalog> catalogs(PrecisionTierCatalog base, PrecisionTier tier,
                                                      SweepParam param, List<Integer> values) {
        List<PrecisionTierCatalog> out = new ArrayList<>();
        for (int v : values) {
            EnumMap<PrecisionTier, PrecisionTierProfile> profiles = new EnumMap<>(PrecisionTier.class);
            for (PrecisionTier t : base.supportedTiers()) {
                PrecisionTierProfile p = base.profile(t);
                profiles.put(t, t == tier ? param.apply(p, v) : p);
            }
            out.add(PrecisionTierCatalog.of(profiles, base.defaultTier()));
        }
        return out;
    }
}