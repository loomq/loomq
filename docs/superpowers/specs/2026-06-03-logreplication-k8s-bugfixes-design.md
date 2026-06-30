# 设计文档：LogReplication + K8sLeaseElection 并发 Bug 修复

> **状态：** 设计完成，待实现
> **日期：** 2026-06-03
> **分支：** dev-xuyuchen

---

## 1. 问题总览

| # | 文件 | 问题 | 严重性 |
|---|------|------|--------|
| 1 | LogReplication | applyCommitted lastApplied 在 upsert 前设置 | 关键 |
| 2 | K8sLeaseElection | tryAcquireLease 与 onAppendEntries 双 leader | 高 |
| 3 | LogReplication | handleAppendEntries commitIndex TOCTOU | 高 |
| 4 | K8sLeaseElection | stop() 无法阻止 in-flight tryAcquireLease | 高 |
| 5 | RaftNode | compactThrough 阻塞 applyCommitted | 中 |
| 6 | LogReplication | failPendingWaiters 竞态 | 中 |
| 7 | GrpcRaftTransport | totalChunks 未验证 | 中 |
| 8 | RaftNode | readEntryRaw 并发 compactThrough null | 中 |
| 9 | LogReplication | advanceCommitIndex 原地排序 | 低 |

---

## 2. LogReplication 修复

### 2.1 applyCommitted lastApplied 顺序修复

**问题**：`lastApplied.set(applied)` 在 `store.upsert()` 之前执行。upsert 失败时 lastApplied 已推进，entry 永久跳过。

**修复**：将 `lastApplied.set(applied)` 移到 `store.upsert()` 之后：

```java
public synchronized void applyCommitted() {
    long committed = commitIndex.get();
    long applied = lastApplied.get();
    while (applied < committed) {
        applied++;
        byte[] entryPayload = raftLog.readEntry(applied);
        if (entryPayload == null || entryPayload.length == 0) {
            lastApplied.set(applied); // 空 entry 直接跳过
            continue;
        }
        try {
            Intent intent = IntentBinaryCodec.decode(entryPayload);
            boolean isNew = store.findById(intent.getIntentId()) == null;
            store.upsert(intent);
            lastApplied.set(applied); // upsert 成功后才推进
            if (runtimeListener != null) {
                try {
                    runtimeListener.onCommittedIntent(intent, isNew, election.role() == RaftRole.LEADER);
                } catch (Exception listenerEx) {
                    log.error("Runtime listener failed at index {}", applied, listenerEx);
                }
            }
            completeAppliedWaiter(applied);
        } catch (Exception e) {
            log.error("Failed to apply committed entry at index {}", applied, e);
            failAppliedWaiter(applied, e);
            break; // 停止循环，下次重试
        }
    }
}
```

### 2.2 handleAppendEntries commitIndex 原子更新

**问题**：`commitIndex.get()` + check + `commitIndex.set()` 非原子。

**修复**：使用 `updateAndGet` 原子操作：

```java
// 将：
long currentCommit = commitIndex.get();
if (leaderCommit > currentCommit) {
    long newCommit = Math.min(leaderCommit, lastIdx);
    if (newCommit > currentCommit) {
        commitIndex.set(newCommit);
        commitAdvanced = true;
    }
}

// 改为：
long newCommit = Math.min(leaderCommit, lastIdx);
long oldCommit = commitIndex.getAndUpdate(current -> Math.max(current, newCommit));
boolean commitAdvanced = newCommit > oldCommit;
```

### 2.3 failPendingWaiters 竞态修复

**问题**：迭代后 `clear()` 可能移除并发插入的 waiter。

**修复**：使用 `ConcurrentHashMap` 的弱一致性迭代 + 不调用 clear：

```java
public void failPendingWaiters(Throwable cause) {
    if (cause == null) {
        cause = new IllegalStateException("Raft leadership lost");
    }
    for (CompletableFuture<Void> waiter : appliedWaiters.values()) {
        waiter.completeExceptionally(cause);
    }
    // 不调用 clear() — 让 awaitApplied 的超时机制自然清理
    // ConcurrentHashMap 的弱一致性迭代保证已遍历的 waiter 被 complete
}
```

### 2.4 advanceCommitIndex 拷贝数组

**问题**：`Arrays.sort()` 原地排序，破坏调用方的数组。

**修复**：

```java
public void advanceCommitIndex(long[] matchIndices, long currentEpoch) {
    long[] sorted = matchIndices.clone();
    java.util.Arrays.sort(sorted);
    long candidate = sorted[sorted.length / 2];
    // ... 其余不变
}
```

---

## 3. K8sLeaseElection 修复

### 3.1 stopped 标志防止双 leader

**问题**：`tryAcquireLease()` 与 `onAppendEntries()` 竞态，stop() 后仍可 becomeLeader。

**修复**：添加 `volatile boolean stopped` 标志：

```java
private volatile boolean stopped = false;

@Override
public void start() {
    stopped = false;
    // ... 原有逻辑
}

@Override
public void stop() {
    stopped = true;
    // ... 原有逻辑
}

private synchronized void becomeLeader(long epoch) {
    if (stopped) return; // stop() 后不 promote
    // ... 原有逻辑
}

private synchronized void becomeFollower(String leader, long epoch) {
    if (stopped) return; // stop() 后不变更
    // ... 原有逻辑
}
```

### 3.2 tryAcquireLease 增加 stopped 检查

```java
private void tryAcquireLease() {
    if (stopped) return;
    try {
        // ... 原有逻辑
```

---

## 4. 其他修复

### 4.1 GrpcRaftTransport totalChunks 验证

```java
// 在 onNext 开头：
if (totalChunks == -1) {
    // 初始化元数据
    totalChunks = chunk.getTotalChunks();
    if (totalChunks <= 0 || totalChunks > 10000) {
        errored.set(true);
        responseObserver.onError(new IllegalArgumentException("Invalid totalChunks: " + totalChunks));
        return;
    }
    chunks = new byte[totalChunks][];
}
```

### 4.2 RaftNode readEntryRaw null 处理

在 sendHeartbeats 的 entry 读取循环中：

```java
byte[] entry = raftLog.readEntryRaw(nextIdx + i);
if (entry == null) {
    // Entry 可能被并发 compactThrough 删除，触发 snapshot
    sendInstallSnapshot(ps.peerId);
    continue outer; // 跳过此 peer 的剩余 entries
}
entries[i] = entry;
```

### 4.3 RaftNode compactThrough 移出 synchronized

将 `raftLog.compactThrough()` 从 `synchronized(replication)` 块内移到块外（在 resetToSnapshot 之后）：

```java
synchronized (replication) {
    // ... upsert + prune + resetToSnapshot
}
// compactThrough 在锁外执行，不阻塞 applyCommitted
raftLog.compactThrough(request.lastIncludedIndex(), request.lastIncludedEpoch());
```

---

## 5. 验收标准

- [ ] applyCommitted 失败 entry 不跳过
- [ ] handleAppendEntries commitIndex 不回退
- [ ] K8sLeaseElection stop() 后不重新 becomeLeader
- [ ] failPendingWaiters 不丢 waiter
- [ ] advanceCommitIndex 不破坏调用方数组
- [ ] installSnapshot totalChunks 有边界检查
- [ ] readEntryRaw null 触发 snapshot
- [ ] compactThrough 不阻塞 applyCommitted
- [ ] 所有测试通过
