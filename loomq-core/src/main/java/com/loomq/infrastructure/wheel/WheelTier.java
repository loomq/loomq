package com.loomq.infrastructure.wheel;

/** 四轮 + tail。windowMs 为每槽时间跨度,count 为视界内槽数。 */
public enum WheelTier {
    SEC(1_000L, 60),
    MIN(60_000L, 60),
    HOUR(3_600_000L, 24),
    DAY(86_400_000L, 30); // count 由 horizonDays 覆盖
    final long windowMs; final int count;
    WheelTier(long w, int c) { windowMs = w; count = c; }

    /** 溢出链的下一层更粗档；DAY 为链终点返回 null。 */
    WheelTier nextCoarser() {
        return switch (this) {
            case SEC -> MIN;
            case MIN -> HOUR;
            case HOUR -> DAY;
            case DAY -> null;
        };
    }
}
