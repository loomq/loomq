package com.loomq.infrastructure.wheel;
import com.loomq.domain.intent.Intent;
/** 扫描产物:槽位置 + 解码后的 Intent(供恢复重建索引)。 */
public record SlotEntry(SlotLocation loc, Intent intent) {}
