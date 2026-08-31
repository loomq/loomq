package com.loomq.common;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 有界桶直方图(common 包内共享实现):ceil-累积近似百分位——返回累计计数越过
 * ⌈total×p⌉ 的桶下界。历史上 PTMR/LMR 各持一份逐行相同的实现,round 12 起收口为本类;
 * 桶边界与数值单位由构造处显式声明(如 wake 延迟 µs、lag/finalize ms),调用方命名须与单位一致。
 *
 * <p>线程安全模型与历史实现一致:每桶 AtomicLong,record 无锁;percentile/max/mean 为
 * 无锁弱一致读(采样进行中允许微小读数偏差,与既有行为相同)。</p>
 */
final class Histogram {

    private final int[] bounds;
    private final ConcurrentHashMap<Integer, AtomicLong> buckets = new ConcurrentHashMap<>();
    private final AtomicLong sampleCount = new AtomicLong(0);

    Histogram(int[] bounds) {
        this.bounds = bounds.clone();
        for (int i = 0; i < this.bounds.length; i++) {
            buckets.put(i, new AtomicLong(0));
        }
    }

    void record(long value) {
        sampleCount.incrementAndGet();
        buckets.get(findBucket(value)).incrementAndGet();
    }

    long sampleCount() {
        return sampleCount.get();
    }

    long percentile(double p) {
        long total = sampleCount.get();
        if (total == 0) {
            return 0;
        }
        long target = (long) Math.ceil(total * p);
        long cumulative = 0;
        for (int i = 0; i < bounds.length; i++) {
            cumulative += buckets.get(i).get();
            if (cumulative >= target) {
                return bounds[i];
            }
        }
        return bounds[bounds.length - 1];
    }

    /** 最高非空桶下界;空直方图返回 0(与历史实现一致)。 */
    long max() {
        for (int i = bounds.length - 1; i >= 0; i--) {
            if (buckets.get(i).get() > 0) {
                return bounds[i];
            }
        }
        return 0;
    }

    /** 桶中点加权均值;空直方图返回 0。末桶无上界可依,中点取其下界。 */
    long mean() {
        long total = sampleCount.get();
        if (total == 0) {
            return 0;
        }
        long sum = 0;
        for (int i = 0; i < bounds.length; i++) {
            long lower = bounds[i];
            long upper = (i + 1 < bounds.length) ? bounds[i + 1] : lower;
            long midpoint = (lower + upper) / 2;
            sum += midpoint * buckets.get(i).get();
        }
        return sum / total;
    }

    private int findBucket(long value) {
        for (int i = bounds.length - 1; i >= 0; i--) {
            if (value >= bounds[i]) {
                return i;
            }
        }
        return 0;
    }
}
