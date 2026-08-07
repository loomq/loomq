package com.loomq.benchmark;

import java.util.Arrays;

/** 统计工具：对样本计算中位数/四分位/百分位（线性插值）。 */
public final class Stats {
    private Stats() {}

    public static double[] sorted(double[] raw) {
        double[] c = raw.clone();
        Arrays.sort(c);
        return c;
    }

    public static double median(double[] sorted) { return percentile(sorted, 0.50); }
    public static double q1(double[] sorted)      { return percentile(sorted, 0.25); }
    public static double q3(double[] sorted)      { return percentile(sorted, 0.75); }
    public static double iqr(double[] sorted)     { return q3(sorted) - q1(sorted); }
    public static double p95(double[] sorted)     { return percentile(sorted, 0.95); }
    public static double p99(double[] sorted)     { return percentile(sorted, 0.99); }
    public static double p999(double[] sorted)    { return percentile(sorted, 0.999); }

    /** 线性插值百分位；要求 sorted 已升序。 */
    public static double percentile(double[] sorted, double p) {
        if (sorted.length == 0) throw new IllegalArgumentException("empty sample");
        if (sorted.length == 1 || p <= 0) return sorted[0];
        if (p >= 1) return sorted[sorted.length - 1];
        double pos = p * (sorted.length - 1);
        int lo = (int) Math.floor(pos);
        int hi = (int) Math.ceil(pos);
        if (lo == hi) return sorted[lo];
        double frac = pos - lo;
        return sorted[lo] + frac * (sorted[hi] - sorted[lo]);
    }
}