# Code Review Bug Fixes (Round 5) 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 修复第五轮 code review 发现的 6 个 bug，最关键的是 inflightMaxIndex 永久停滞。

**Tech Stack:** Java 25

---

## Task 1: inflightMaxIndex 异常路径重置

**Files:**
- Modify: `loomq-raft/src/main/java/com/loomq/raft/RaftNode.java`

- [ ] **Step 1: 在 sendHeartbeats 的异常路径重置 inflightMaxIndex**

在 `sendHeartbeats` 的 `whenComplete` 回调中，将：

```java
if (throwable != null) {
    log.error("AppendEntries failed for peer {}: {}", ps.peerId,
        throwable.getMessage(), throwable);
    return;
}
```

改为：

```java
if (throwable != null) {
    log.error("AppendEntries failed for peer {}: {}", ps.peerId,
        throwable.getMessage(), throwable);
    ps.inflightMaxIndex = 0; // Reset so next heartbeat can retry
    return;
}
```

- [ ] **Step 2: 运行测试**

Run: `mvn test -pl loomq-raft -am`
Expected: 全部通过

- [ ] **Step 3: Commit**

```bash
git add loomq-raft/src/main/java/com/loomq/raft/RaftNode.java
git commit -m "fix: sendHeartbeats 异常路径重置 inflightMaxIndex，防止复制永久停滞"
```

---

## Task 2: decodeStoreSnapshot null intent 检查

**Files:**
- Modify: `loomq-raft/src/main/java/com/loomq/raft/RaftNode.java`

- [ ] **Step 1: 在 decodeStoreSnapshot 循环中增加 null 检查**

将：

```java
var intent = com.loomq.infrastructure.wal.IntentBinaryCodec.decode(encoded);
result.add(intent);
```

改为：

```java
var intent = com.loomq.infrastructure.wal.IntentBinaryCodec.decode(encoded);
if (intent == null) {
    throw new java.io.IOException("Decoded intent is null at index " + i);
}
result.add(intent);
```

- [ ] **Step 2: 运行测试**

Run: `mvn test -pl loomq-raft -am`
Expected: 全部通过

- [ ] **Step 3: Commit**

```bash
git add loomq-raft/src/main/java/com/loomq/raft/RaftNode.java
git commit -m "fix: decodeStoreSnapshot null intent 检查，防止 NPE"
```

---

## Task 3: sendInstallSnapshot compactThrough 错误处理

**Files:**
- Modify: `loomq-raft/src/main/java/com/loomq/raft/RaftNode.java`

- [ ] **Step 1: 将 compactThrough 移到 matchIndex 设置之前**

在 `sendInstallSnapshot` 的 `thenAccept` 回调中，将：

```java
if (response.bytesReceived() >= 0) {
    ps.nextIndex = snapshotIndex + 1;
    ps.matchIndex = snapshotIndex;
    raftLog.compactThrough(snapshotIndex, snapshotEpoch);
    log.info("InstallSnapshot accepted by {} (index={})", peerId, snapshotIndex);
}
```

改为：

```java
if (response.bytesReceived() >= 0) {
    // Compact before updating peer state — if compaction fails, peer state is unchanged
    raftLog.compactThrough(snapshotIndex, snapshotEpoch);
    ps.nextIndex = snapshotIndex + 1;
    ps.matchIndex = snapshotIndex;
    log.info("InstallSnapshot accepted by {} (index={})", peerId, snapshotIndex);
}
```

- [ ] **Step 2: 运行测试**

Run: `mvn test -pl loomq-raft -am`
Expected: 全部通过

- [ ] **Step 3: Commit**

```bash
git add loomq-raft/src/main/java/com/loomq/raft/RaftNode.java
git commit -m "fix: sendInstallSnapshot compactThrough 移到 matchIndex 设置之前"
```

---

## Task 4: 全量回归验证

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
