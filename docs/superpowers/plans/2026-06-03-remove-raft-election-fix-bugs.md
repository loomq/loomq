# 移除自研 Raft 选举 + 修复 Code Review Bugs 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 完全移除 RaftElection（自研 Raft 选举），只保留 K8sLeaseElection 和 StandaloneElection，同时修复 code review 发现的 15 个 bug。

**Architecture:** 删除 RaftElection 及其测试，清理 RaftNode 中所有 RaftElection 引用，修复 GrpcRaftTransport/K8sLeaseElection/RaftNode 中的并发、异常处理和资源泄漏问题。

**Tech Stack:** Java 25, gRPC, K8s Lease API, JUnit 5

---

## 文件结构

```
loomq-raft/src/main/java/com/loomq/raft/
├── RaftElection.java              ← 删除
├── TransportException.java        ← 删除（从未使用）
├── LeaderElection.java            ← 清理 javadoc
├── RaftNode.java                  ← 移除 instanceof + onElectionStarted + 修复 close/gracefulShutdown
├── GrpcRaftTransport.java         ← volatile handler + 异常传播 + connect 竞态修复
├── K8sLeaseElection.java          ← isLeader 副作用修复 + stop await + 清理死字段
├── StandaloneElection.java        ← 不变
├── K8sLeaseConfig.java            ← 不变
├── RaftTransport.java             ← 不变
├── InMemoryRaftTransport.java     ← 不变

loomq-raft/src/test/java/com/loomq/raft/
├── LeaderElectionTest.java        ← 删除（测试 RaftElection）
├── RaftNodeTest.java              ← 适配（移除 RaftElection cast）
├── K8sLeaseElectionTest.java      ← 修复 stop() 泄漏
├── RaftSoakTest.java              ← 不变
```

---

## Task 1: 删除 RaftElection + 清理 RaftNode

**Files:**
- Delete: `loomq-raft/src/main/java/com/loomq/raft/RaftElection.java`
- Delete: `loomq-raft/src/main/java/com/loomq/raft/TransportException.java`
- Modify: `loomq-raft/src/main/java/com/loomq/raft/RaftNode.java`
- Modify: `loomq-raft/src/main/java/com/loomq/raft/LeaderElection.java`
- Modify: `loomq-raft/src/test/java/com/loomq/raft/RaftNodeTest.java`

- [ ] **Step 1: 删除 RaftElection.java 和 TransportException.java**

```bash
rm loomq-raft/src/main/java/com/loomq/raft/RaftElection.java
rm loomq-raft/src/main/java/com/loomq/raft/TransportException.java
```

- [ ] **Step 2: 清理 RaftNode 构造器 — 移除 RaftElection 创建**

将 RaftNode 构造器（line 71-138）中的：

```java
if (externalElection != null) {
    this.election = externalElection;
} else {
    RaftElection raftElection = new RaftElection(nodeId, wal, config.peers(),
        config.electionMinMs(), config.electionMaxMs());
    this.election = raftElection;
}
```

改为：

```java
if (externalElection == null) {
    throw new IllegalArgumentException("externalElection is required (K8sLeaseElection or StandaloneElection)");
}
this.election = externalElection;
```

- [ ] **Step 3: 移除 onElectionStarted 回调注册**

删除构造器中的：

```java
// RaftElection 特有的选举开始回调 — 构造时类型判断，不是运行时分发
if (election instanceof RaftElection raftElection) {
    raftElection.setOnElectionStarted(this::onElectionStarted);
}
```

- [ ] **Step 4: 删除 onElectionStarted 方法**

删除整个方法（line 500-508）：

```java
private void onElectionStarted(long epoch) {
    syncRaftMetrics();
    log.info("Election started for epoch {} (voting not supported with current transport)", epoch);
}
```

- [ ] **Step 5: 更新 RaftNode 2-arg 和 3-arg 构造器**

将 2-arg 构造器改为创建 `StandaloneElection`（单机默认）：

```java
public RaftNode(RaftConfig config, WalAccessor wal, IntentStore store, RaftTransport transport) {
    this(config, wal, store, transport, null, new StandaloneElection(wal));
}
```

将 3-arg 构造器同理：

```java
public RaftNode(RaftConfig config, WalAccessor wal, IntentStore store, RaftTransport transport,
                RaftRuntimeListener runtimeListener) {
    this(config, wal, store, transport, runtimeListener, new StandaloneElection(wal));
}
```

- [ ] **Step 6: 修复 RaftNodeTest 中的 RaftElection cast**

`RaftNodeTest.staleAppendEntriesResponsesShouldNotRegressPeerState` 中 line 296：

```java
((RaftElection) node.getElection()).becomeLeader(1);
```

改为使用 `StandaloneElection`（自动成为 leader）或直接用 `StandaloneElection` 构造 RaftNode。由于该测试需要手动控制 leader 状态，改用 `StandaloneElection`：

```java
RaftNode node = new RaftNode(config, wal, store, transport, null,
    new StandaloneElection(wal));
```

`StandaloneElection.start()` 自动成为 leader，不需要手动调 `becomeLeader`。

- [ ] **Step 7: 清理 LeaderElection javadoc**

将 LeaderElection.java 中的：

```java
 *   <li>{@link RaftElection} — 自研 Raft 选举（多节点集群）</li>
```

改为：

```java
 *   <li>{@link K8sLeaseElection} — K8s Lease 选主（分布式部署）</li>
```

- [ ] **Step 8: 运行测试**

Run: `mvn test -pl loomq-raft -am`
Expected: 全部通过

- [ ] **Step 9: Commit**

```bash
git add -A
git commit -m "refactor: 完全移除自研 Raft 选举（RaftElection），只保留 K8s + Standalone"
```

---

## Task 2: 删除 LeaderElectionTest

**Files:**
- Delete: `loomq-raft/src/test/java/com/loomq/raft/LeaderElectionTest.java`

- [ ] **Step 1: 删除 LeaderElectionTest.java**

此文件全部测试 RaftElection 的行为（选举超时、投票、stepDown 等）。RaftElection 已删除，测试无意义。

```bash
rm loomq-raft/src/test/java/com/loomq/raft/LeaderElectionTest.java
```

- [ ] **Step 2: 运行测试**

Run: `mvn test -pl loomq-raft -am`
Expected: 全部通过

- [ ] **Step 3: Commit**

```bash
git add -u
git commit -m "test: 删除 LeaderElectionTest（RaftElection 已移除）"
```

---

## Task 3: GrpcRaftTransport — volatile handler + 异常传播 + connect 竞态

**Files:**
- Modify: `loomq-raft/src/main/java/com/loomq/raft/GrpcRaftTransport.java`

- [ ] **Step 1: handler 字段加 volatile**

```java
// 将：
private Function<RaftTransport.AppendEntriesRequest, RaftTransport.AppendEntriesResponse> appendEntriesHandler;
private Function<RaftTransport.InstallSnapshotRequest, RaftTransport.InstallSnapshotResponse> installSnapshotHandler;
// 改为：
private volatile Function<RaftTransport.AppendEntriesRequest, RaftTransport.AppendEntriesResponse> appendEntriesHandler;
private volatile Function<RaftTransport.InstallSnapshotRequest, RaftTransport.InstallSnapshotResponse> installSnapshotHandler;
```

- [ ] **Step 2: InstallSnapshot 服务端 handler 异常传播**

在 `RaftServiceImpl.installSnapshot` 的 `onNext` 中，将：

```java
} catch (Exception e) {
    log.error("InstallSnapshot chunk handler error", e);
}
```

改为：

```java
} catch (Exception e) {
    log.error("InstallSnapshot chunk handler error", e);
    responseObserver.onError(e);
    return;
}
```

同时在 `onError` 中传播：

```java
@Override
public void onError(Throwable t) {
    log.warn("InstallSnapshot stream error", t);
    responseObserver.onError(t);
}
```

- [ ] **Step 3: connect() 竞态修复**

将 connect() 中的 get-then-put 改为原子操作：

```java
@Override
public CompletableFuture<Void> connect(String peerId, String host, int port) {
    return CompletableFuture.runAsync(() -> {
        ManagedChannel existing = channels.put(peerId,
            ManagedChannelBuilder.forAddress(host, port).usePlaintext().build());
        if (existing != null) {
            existing.shutdown();
        }
        ManagedChannel channel = channels.get(peerId);
        blockingStubs.put(peerId, RaftServiceGrpc.newBlockingStub(channel));
        asyncStubs.put(peerId, RaftServiceGrpc.newStub(channel));
        log.info("GrpcRaftTransport connected to {} at {}:{}", peerId, host, port);
    }, rpcExecutor);
}
```

- [ ] **Step 4: 删除重复的 SNAPSHOT_CHUNK_SIZE 常量**

删除 GrpcRaftTransport 中的：

```java
private static final int SNAPSHOT_CHUNK_SIZE = 256 * 1024; // 256KB per chunk
```

此常量只在 RaftNode 中使用，GrpcRaftTransport 不需要。

- [ ] **Step 5: 运行测试**

Run: `mvn test -pl loomq-raft -am`
Expected: 全部通过

- [ ] **Step 6: Commit**

```bash
git add loomq-raft/src/main/java/com/loomq/raft/GrpcRaftTransport.java
git commit -m "fix: GrpcRaftTransport volatile handler + 异常传播 + connect 竞态修复"
```

---

## Task 4: K8sLeaseElection — isLeader 副作用修复 + stop await + 清理死字段

**Files:**
- Modify: `loomq-raft/src/main/java/com/loomq/raft/K8sLeaseElection.java`

- [ ] **Step 1: isLeader() 移除 stepDown 副作用**

将：

```java
@Override
public boolean isLeader() {
    if (role != RaftRole.LEADER) return false;

    // Monotonic clock check: if we haven't renewed within leaseDuration, step down
    long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - lastRenewNanoTime);
    if (elapsedMs > config.leaseDurationSeconds() * 1000L) {
        log.warn("Lease expired (monotonic clock), elapsed={}ms, stepping down", elapsedMs);
        stepDown(currentEpoch);
        return false;
    }

    return true;
}
```

改为：

```java
@Override
public boolean isLeader() {
    return role == RaftRole.LEADER && !isLeaseExpiredMonotonic();
}

/**
 * 单调时钟检查：距上次续约是否超过 leaseDuration。
 * 不修改状态，仅返回是否过期。
 */
private boolean isLeaseExpiredMonotonic() {
    if (lastRenewNanoTime == 0) return false; // 从未续约，不判断
    long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - lastRenewNanoTime);
    return elapsedMs > config.leaseDurationSeconds() * 1000L;
}
```

同时在 `tryAcquireLease()` 的定期任务中，增加过期检查和 stepDown：

```java
private void tryAcquireLease() {
    try {
        // 如果自己是 leader 但单调时钟过期，主动退位
        if (role == RaftRole.LEADER && isLeaseExpiredMonotonic()) {
            log.warn("Lease expired (monotonic clock), stepping down");
            stepDown(currentEpoch);
        }

        V1Lease existingLease = readLease();
        // ... 其余逻辑不变
```

- [ ] **Step 2: stop() 增加 awaitTermination**

```java
@Override
public void stop() {
    log.info("K8sLeaseElection stopping: pod={}", config.podName());
    if (renewTask != null) {
        renewTask.cancel(false);
    }
    scheduler.shutdown();
    try {
        if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
            scheduler.shutdownNow();
        }
    } catch (InterruptedException e) {
        scheduler.shutdownNow();
        Thread.currentThread().interrupt();
    }
    role = RaftRole.FOLLOWER;
}
```

- [ ] **Step 3: 删除 lastKnownRenewTimeMicros 死字段**

删除字段声明：

```java
private volatile long lastKnownRenewTimeMicros = 0;
```

删除 updateFencingState 中对该字段的写入：

```java
private void updateFencingState(V1Lease lease) {
    // 只保留单调时钟更新
    lastRenewNanoTime = System.nanoTime();
}
```

- [ ] **Step 4: 清理 isLeaseExpired 中的重复 null 检查**

将：

```java
private boolean isLeaseExpired(V1Lease lease) {
    if (lease.getSpec() == null || lease.getSpec().getRenewTime() == null) {
        return true;
    }
    OffsetDateTime renewTime = lease.getSpec().getRenewTime();
    if (renewTime == null) {
        return true;
    }
```

改为：

```java
private boolean isLeaseExpired(V1Lease lease) {
    if (lease.getSpec() == null || lease.getSpec().getRenewTime() == null) {
        return true;
    }
    OffsetDateTime renewTime = lease.getSpec().getRenewTime();
```

- [ ] **Step 5: 运行测试**

Run: `mvn test -pl loomq-raft -am`
Expected: 全部通过

- [ ] **Step 6: Commit**

```bash
git add loomq-raft/src/main/java/com/loomq/raft/K8sLeaseElection.java
git commit -m "fix: K8sLeaseElection isLeader 纯查询 + stop await + 清理死字段"
```

---

## Task 5: RaftNode — close try-finally + gracefulShutdown isLeader 检查

**Files:**
- Modify: `loomq-raft/src/main/java/com/loomq/raft/RaftNode.java`

- [ ] **Step 1: close() 增加 try-finally**

```java
@Override
public void close() {
    try {
        stopHeartbeat();
        if (isLeader() && !peerStates.isEmpty() && shutdownTimeoutMs > 0) {
            gracefulShutdown();
        }
    } catch (Exception e) {
        log.error("Error during graceful shutdown", e);
    } finally {
        heartbeatTimer.shutdown();
        election.stop();
        replication.failPendingWaiters(new IllegalStateException("Raft node closed"));
        notifyRuntimeRole(RaftRole.FOLLOWER);
        metrics.updateRaftRole("OFFLINE");
        metrics.updateRaftLeaderId(null);
        metrics.updateRaftReplicationLag(0);
        metrics.updateRaftConnectedPeers(0);
        metrics.updateRaftTotalPeers(0);
        if (transport != null) {
            try {
                transport.close();
            } catch (Exception e) {
                log.error("Error closing transport", e);
            }
        }
        log.info("RaftNode closed: node={}", nodeId);
    }
}
```

- [ ] **Step 2: gracefulShutdown 增加 isLeader 检查**

```java
while (System.currentTimeMillis() < deadline && commitIdx < lastLogIdx && isLeader()) {
```

- [ ] **Step 3: 运行测试**

Run: `mvn test -pl loomq-raft -am`
Expected: 全部通过

- [ ] **Step 4: Commit**

```bash
git add loomq-raft/src/main/java/com/loomq/raft/RaftNode.java
git commit -m "fix: RaftNode close try-finally + gracefulShutdown isLeader 检查"
```

---

## Task 6: InstallSnapshot 单 shot 路径增加 exceptionally

**Files:**
- Modify: `loomq-raft/src/main/java/com/loomq/raft/RaftNode.java`

- [ ] **Step 1: 单 shot InstallSnapshot 增加 exceptionally**

在 `sendInstallSnapshot` 方法中，小 snapshot 路径（line 326-341）的 `.thenAccept(...)` 后增加：

```java
.exceptionally(ex -> {
    log.error("InstallSnapshot failed for {}: {}", peerId, ex.getMessage(), ex);
    return null;
});
```

- [ ] **Step 2: 运行测试**

Run: `mvn test -pl loomq-raft -am`
Expected: 全部通过

- [ ] **Step 3: Commit**

```bash
git add loomq-raft/src/main/java/com/loomq/raft/RaftNode.java
git commit -m "fix: InstallSnapshot 单 shot 路径增加 exceptionally 错误处理"
```

---

## Task 7: K8sLeaseElectionTest — 修复 stop() 泄漏

**Files:**
- Modify: `loomq-raft/src/test/java/com/loomq/raft/K8sLeaseElectionTest.java`

- [ ] **Step 1: 检查所有测试方法是否有 try-finally 确保 stop()**

读取 K8sLeaseElectionTest.java，检查每个创建 K8sLeaseElection 实例的测试方法是否在 finally 块中调用 `election.stop()`。如果没有，添加。

特别是 `isLeaderShouldReturnFalseWhenMonotonicClockExpired` 测试（line 234 附近）。

- [ ] **Step 2: 运行测试**

Run: `mvn test -pl loomq-raft -am`
Expected: 全部通过

- [ ] **Step 3: Commit**

```bash
git add loomq-raft/src/test/java/com/loomq/raft/K8sLeaseElectionTest.java
git commit -m "test: K8sLeaseElectionTest 修复 stop() 泄漏"
```

---

## Task 8: 全量回归验证

**Files:**
- 全模块

- [ ] **Step 1: Raft 模块测试**

Run: `mvn test -pl loomq-raft -am`
Expected: 全部通过

- [ ] **Step 2: Core 模块测试**

Run: `mvn test -pl loomq-core -am`
Expected: 全部通过

- [ ] **Step 3: Server 模块测试**

Run: `mvn test -pl loomq-server -am`
Expected: 全部通过

- [ ] **Step 4: Spotless 格式化**

Run: `mvn spotless:apply -pl loomq-raft,loomq-server`

- [ ] **Step 5: Spotless 检查**

Run: `mvn spotless:check -pl loomq-raft,loomq-server`
Expected: 通过

- [ ] **Step 6: Commit（如有格式化变更）**

```bash
git add -u
git commit -m "style: Spotless 格式化"
```
