package com.loomq.application.recovery;

import java.util.Map;
import java.util.Set;

/**
 * recover() 报告:hot/cold 计数 + multiSlot 恢复标记 + max-revision 种子映射 +
 * 终态墓碑 id 集(C18-1,r18)。构造方仅 WheelRecovery;LoomqEngine.start() 按分量
 * 经命令服务簿记缝注入(markMultiSlot / markMaxRevisions / markTombstones)。
 */
public record WheelRecoveryReport(int hotRestored, int coldRegistered, Set<String> multiSlotIntentIds,
                                  Map<String, Long> maxRevisions, Set<String> tombstonedIds) {
}
