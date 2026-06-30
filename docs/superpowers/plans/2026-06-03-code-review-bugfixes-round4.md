# Code Review Bug Fixes (Round 4) 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 修复第四轮 code review 发现的 6 个 bug，最关键的是 handleInstallSnapshot 与 applyCommitted 的竞态条件。

**Architecture:** 竞态修复通过 synchronized 边界统一；异常处理修复通过 errored 标志和 len 验证。

**Tech Stack:** Java 25, gRPC

---

## Task 1: handleInstallSnapshot 竞态修复 — synchronized 边界统一

**Files:**
- Modify: `loomq-raft/src/main/java/com/loomq/raft/RaftNode.java`
- Modify: `loomq-raft/src/main/java/com/loomq/raft/LogReplication.java`

**问题**：handleInstallSnapshot 在 gRPC 线程上执行，无锁。applyCommitted 在 synchronized(replication) 块内执行。两者并发时：
- prune 步骤可能误删 applyCommitted 刚写入的 intent
- resetToSnapshot 无锁，applyCommitted 的局部 lastApplied 可能回退

**修复方案**：handleInstallSnapshot 的 store 操作和 resetToSnapshot 都在 `synchronized(replication)` 块内执行。

- [ ] **Step 1: LogReplication.resetToSnapshot 加 synchronized**

在 `LogReplication.java` 中，将：

```java
public void resetToSnapshot(long index) {
    commitIndex.set(index);
    lastApplied.set(index);
}
```

改为：

```java
public synchronized void resetToSnapshot(long index) {
    commitIndex.set(index);
    lastApplied.set(index);
}
```

- [ ] **Step 2: handleInstallSnapshot 的 store 操作加 synchronized**

在 `RaftNode.java` 的 `handleInstallSnapshot` 中，将 upsert + prune + resetToSnapshot 包裹在 synchronized 块内：

```java
try {
    java.util.List<com.loomq.domain.intent.Intent> decoded = decodeStoreSnapshot(request.data());

    // Synchronize with applyCommitted to prevent race conditions
    synchronized (replication) {
        // Upsert new data (overwrites existing, no clear needed)
        java.util.Set<String> snapshotIds = new java.util.HashSet<>();
        for (com.loomq.domain.intent.Intent intent : decoded) {
            store.upsert(intent);
            snapshotIds.add(intent.getIntentId());
        }

        // Remove stale intents not in snapshot
        java.util.Set<String> toRemove = new java.util.HashSet<>(store.getAllIntents().keySet());
        toRemove.removeAll(snapshotIds);
        for (String id : toRemove) {
            store.delete(id);
        }

        raftLog.compactThrough(request.lastIncludedIndex(), request.lastIncludedEpoch());
        replication.resetToSnapshot(request.lastIncludedIndex());
    }

    log.info("InstallSnapshot applied: {} intents, index={}, epoch={}",
        decoded.size(), request.lastIncludedIndex(), request.lastIncludedEpoch());
    return request.lastIncludedIndex();
} catch (Exception e) {
    log.error("Failed to apply InstallSnapshot from {}", request.leaderId(), e);
    return -1L;
}
```

- [ ] **Step 3: 运行测试**

Run: `mvn test -pl loomq-raft -am`
Expected: 全部通过

- [ ] **Step 4: Commit**

```bash
git add loomq-raft/src/main/java/com/loomq/raft/RaftNode.java \
        loomq-raft/src/main/java/com/loomq/raft/LogReplication.java
git commit -m "fix: handleInstallSnapshot synchronized 边界统一，修复与 applyCommitted 的竞态"
```

---

## Task 2: GrpcRaftTransport onError errored 标志修复

**Files:**
- Modify: `loomq-raft/src/main/java/com/loomq/raft/GrpcRaftTransport.java`

- [ ] **Step 1: onError 检查 errored 标志**

在 `RaftServiceImpl.installSnapshot` 的 StreamObserver 中，将：

```java
@Override
public void onError(Throwable t) {
    log.warn("InstallSnapshot stream error from {}: {}", leaderId, t.getMessage());
    errored = true;
    responseObserver.onError(t);
}
```

改为：

```java
@Override
public void onError(Throwable t) {
    log.warn("InstallSnapshot stream error from {}: {}", leaderId, t.getMessage());
    if (!errored) {
        errored = true;
        responseObserver.onError(t);
    }
}
```

- [ ] **Step 2: 运行测试**

Run: `mvn test -pl loomq-raft -am`
Expected: 全部通过

- [ ] **Step 3: Commit**

```bash
git add loomq-raft/src/main/java/com/loomq/raft/GrpcRaftTransport.java
git commit -m "fix: installSnapshot onError 检查 errored 标志，防止 double onError"
```

---

## Task 3: decodeStoreSnapshot per-entry len 验证

**Files:**
- Modify: `loomq-raft/src/main/java/com/loomq/raft/RaftNode.java`

- [ ] **Step 1: 增加 per-entry len 验证**

在 `decodeStoreSnapshot` 中，将：

```java
int len = dis.readInt();
byte[] encoded = new byte[len];
dis.readFully(encoded);
```

改为：

```java
int len = dis.readInt();
if (len < 0 || len > 10_000_000) {
    throw new java.io.IOException("Invalid entry length: " + len);
}
byte[] encoded = new byte[len];
dis.readFully(encoded);
```

- [ ] **Step 2: 运行测试**

Run: `mvn test -pl loomq-raft -am`
Expected: 全部通过

- [ ] **Step 3: Commit**

```bash
git add loomq-raft/src/main/java/com/loomq/raft/RaftNode.java
git commit -m "fix: decodeStoreSnapshot per-entry len 验证，防止 OOM"
```

---

## Task 4: sendInstallSnapshot compactThrough 移到发送成功后

**Files:**
- Modify: `loomq-raft/src/main/java/com/loomq/raft/RaftNode.java`

- [ ] **Step 1: 将 compactThrough 移到 thenAccept 回调中**

将 `sendInstallSnapshot` 中的：

```java
byte[] snapshotData = encodeStoreSnapshot();
if (snapshotData.length == 0) {
    log.error("Failed to encode snapshot for peer {}, skipping", peerId);
    return;
}
raftLog.compactThrough(snapshotIndex, snapshotEpoch);
```

改为：

```java
byte[] snapshotData = encodeStoreSnapshot();
if (snapshotData.length == 0) {
    log.error("Failed to encode snapshot for peer {}, skipping", peerId);
    return;
}
// Don't compact until snapshot is confirmed by follower
```

然后在 `thenAccept` 回调中，成功时 compact：

```java
.thenAccept(response -> {
    if (requestGeneration != ps.requestGeneration) return;
    if (response.bytesReceived() >= 0) {
        ps.nextIndex = snapshotIndex + 1;
        ps.matchIndex = snapshotIndex;
        // Compact log now that snapshot is confirmed
        raftLog.compactThrough(snapshotIndex, snapshotEpoch);
        log.info("InstallSnapshot accepted by {} (index={})", peerId, snapshotIndex);
    } else {
        log.warn("InstallSnapshot rejected by {}", peerId);
    }
})
```

- [ ] **Step 2: 运行测试**

Run: `mvn test -pl loomq-raft -am`
Expected: 全部通过

- [ ] **Step 3: Commit**

```bash
git add loomq-raft/src/main/java/com/loomq/raft/RaftNode.java
git commit -m "fix: sendInstallSnapshot compactThrough 移到 follower 确认后，避免失败时强制其他 follower 走 snapshot"
```

---

## Task 5: 全量回归验证

- [ ] **Step 1: Raft 模块测试**

Run: `mvn test -pl loomq-raft -am`
Expected: 全部通过

- [ ] **Step 2: Server 模块测试**

Run: `mvn test -pl loomq-server -am`
Expected: 全部通过

- [ ] **Step 3: Spotless 格式化**

Run: `mvn spotless:apply -pl loomq-raft,loomq-server`

- [ ] **Step 4: Spotless 检查**

Run: `mvn spotless:check -pl loomq-raft,loomq-server`
Expected: 通过
