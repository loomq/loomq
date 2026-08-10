package com.loomq.infrastructure.wheel;
/** 冷 Intent 的内存句柄(~40B):仅 id + 磁盘位置,payload 留磁盘。 */
public record ColdHandle(String intentId, SlotLocation loc) {}
