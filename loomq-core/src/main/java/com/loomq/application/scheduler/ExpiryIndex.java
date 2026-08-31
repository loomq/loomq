package com.loomq.application.scheduler;

import com.loomq.domain.intent.Intent;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * 按 executeAt 索引活跃 intent(替代全量扫描)。scanner 遍历、结算路径摘除。
 * 每个唯一毫秒时间戳一个 HashSet;并发安全;unindex 原子(computeIfPresent)。
 */
final class ExpiryIndex {

    private final ConcurrentSkipListMap<Long, Set<String>> index = new ConcurrentSkipListMap<>();

    void index(Intent intent) {
        if (intent.getExecuteAt() == null) return;
        long key = intent.getExecuteAt().toEpochMilli();
        index.computeIfAbsent(key, k -> ConcurrentHashMap.newKeySet()).add(intent.getIntentId());
    }

    /** 从过期索引移除 intent(原子操作,避免并发添加时误删 bucket)。 */
    void unindex(String intentId, long executeAtMs) {
        index.computeIfPresent(executeAtMs, (key, ids) -> {
            ids.remove(intentId);
            return ids.isEmpty() ? null : ids;
        });
    }

    /** 已到期条目视图(含等于 nowMs 的 key);遍历时反映并发修改,惰性清理由调用方承担。 */
    NavigableMap<Long, Set<String>> expiredEntriesUpTo(long nowMs) {
        return index.headMap(nowMs, true);
    }
}
