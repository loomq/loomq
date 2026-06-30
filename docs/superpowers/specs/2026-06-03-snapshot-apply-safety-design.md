# 设计文档：Snapshot 应用安全性修复

> **状态：** 设计完成，待实现
> **日期：** 2026-06-03
> **分支：** dev-xuyuchen

---

## 1. 问题

`handleInstallSnapshot` 中 `store.clear()` 在 `upsert` 循环之前执行。如果 `upsert` 中途抛异常，旧数据已清除，新数据不完整，store 处于不可恢复的损坏状态。

```
当前流程：
1. decoded = decodeStoreSnapshot(data)   ✅ 失败不伤 store
2. store.clear()                         ⚠️ 旧数据永久删除
3. for i : store.upsert(i)              ❌ 中途失败 → 部分数据，无法回滚
```

## 2. 设计

**核心思路**：`decodeStoreSnapshot` 已将完整快照解码到内存。先 upsert（覆盖旧数据），再清除不在快照中的残留 intent。任何步骤失败都不会丢失旧数据。

```
新流程：
1. decoded = decodeStoreSnapshot(data)   ✅ 失败不伤 store
2. for i : store.upsert(i)              ✅ 覆盖写入，旧数据仍在
3. 删除 store 中不在 decoded 里的 intent ✅ 清除残留
```

**失败场景分析**：

| 失败点 | store 状态 | 可恢复性 |
|--------|-----------|---------|
| 步骤 1 失败 | 旧数据完整 | leader 重试 ✅ |
| 步骤 2 中途失败 | 旧数据 + 部分新数据 | leader 重试，upsert 幂等 ✅ |
| 步骤 3 中途失败 | 旧数据 + 全部新数据 + 残留旧数据 | leader 重试，幂等 ✅ |

## 3. 实现

```java
private Long handleInstallSnapshot(RaftTransport.InstallSnapshotRequest request) {
    if (request.epoch() < election.currentEpoch()) {
        return -1L;
    }
    election.onAppendEntries(request.epoch(), request.leaderId());

    try {
        // 1. 解码快照（失败不伤 store）
        java.util.List<com.loomq.domain.intent.Intent> decoded = decodeStoreSnapshot(request.data());

        // 2. Upsert 新数据（覆盖旧数据，不先 clear）
        java.util.Set<String> snapshotIds = new java.util.HashSet<>();
        for (com.loomq.domain.intent.Intent intent : decoded) {
            store.upsert(intent);
            snapshotIds.add(intent.getIntentId());
        }

        // 3. 清除不在快照中的残留 intent
        java.util.Set<String> toRemove = new java.util.HashSet<>(store.getAllIntents().keySet());
        toRemove.removeAll(snapshotIds);
        for (String id : toRemove) {
            store.delete(id);
        }

        raftLog.compactThrough(request.lastIncludedIndex(), request.lastIncludedEpoch());
        replication.resetToSnapshot(request.lastIncludedIndex());
        return request.lastIncludedIndex();
    } catch (Exception e) {
        log.error("Failed to apply InstallSnapshot from {}", request.leaderId(), e);
        return -1L;
    }
}
```

## 4. 测试

- 现有 `RaftNodeTest.snapshotEncodeDecodeShouldRoundTrip` 通过
- 现有 `RaftSoakTest.snapshotCompaction_continuedWrite_noDataLoss` 通过

## 5. 验收标准

- [ ] `store.clear()` 不再在 `upsert` 循环之前调用
- [ ] `upsert` 中途失败不会丢失旧数据
- [ ] 快照应用后，store 中只有快照中的 intent（无残留）
