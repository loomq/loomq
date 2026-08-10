package com.loomq.infrastructure.wheel;

public record SlotLocation(WheelTier tier, long bucketKey, int slotIndex, boolean inTail) {
    public static SlotLocation tail(long executeAtMs) { return new SlotLocation(null, executeAtMs, 0, true); }
}
