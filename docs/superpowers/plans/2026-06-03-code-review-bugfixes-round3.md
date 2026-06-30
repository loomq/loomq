# Code Review Bug Fixes (Round 3) 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 修复第三轮 code review 发现的 10 个 bug，最关键的是 RaftNode.isLeader() 绕过 K8sLeaseElection 单调时钟检查。

**Architecture:** 按严重性分组修复：关键 bug 单独一 task，高/中严重性 bug 合并为 2-3 个 task。

**Tech Stack:** Java 25, gRPC, K8s Lease API, JUnit 5

---

## Task 1: RaftNode.isLeader() 修复 — 调用 election.isLeader()

**Files:**
- Modify: `loomq-raft/src/main/java/com/loomq/raft/RaftNode.java`

- [ ] **Step 1: 修复 isLeader()**

将：

```java
public boolean isLeader() { return role() == RaftRole.LEADER; }
```

改为：

```java
public boolean isLeader() { return election.isLeader(); }
```

这样所有 RaftNode 内部路径（propose、sendHeartbeats、gracefulShutdown、canServeLinearizableRead）都会经过 K8sLeaseElection.isLeader() 的单调时钟检查。

- [ ] **Step 2: 运行测试**

Run: `mvn test -pl loomq-raft -am`
Expected: 全部通过

- [ ] **Step 3: Commit**

```bash
git add loomq-raft/src/main/java/com/loomq/raft/RaftNode.java
git commit -m "fix: RaftNode.isLeader() 调用 election.isLeader() 而非直接读 role"
```

---

## Task 2: GrpcRaftTransport InstallSnapshot 异常处理修复

**Files:**
- Modify: `loomq-raft/src/main/java/com/loomq/raft/GrpcRaftTransport.java`

- [ ] **Step 1: 修复服务端 onError 不关闭响应流**

在 `RaftServiceImpl.installSnapshot` 的 StreamObserver 中，将：

```java
@Override
public void onError(Throwable t) {
    log.warn("InstallSnapshot stream error from {}: {}", leaderId, t.getMessage());
}
```

改为：

```java
@Override
public void onError(Throwable t) {
    log.warn("InstallSnapshot stream error from {}: {}", leaderId, t.getMessage());
    responseObserver.onError(t);
}
```

- [ ] **Step 2: 修复 double-close 风险**

在 onNext 中，设置一个错误标志防止 onCompleted 在 onError 后再次操作响应流：

```java
private volatile boolean errored = false;

@Override
public void onNext(SnapshotChunk chunk) {
    try {
        if (errored) return;
        // ... existing logic
    } catch (Exception e) {
        log.error("InstallSnapshot chunk processing error", e);
        errored = true;
        responseObserver.onError(e);
    }
}

@Override
public void onError(Throwable t) {
    log.warn("InstallSnapshot stream error from {}: {}", leaderId, t.getMessage());
    errored = true;
    responseObserver.onError(t);
}

@Override
public void onCompleted() {
    if (errored) return;
    // ... existing logic
}
```

- [ ] **Step 3: 修复 installSnapshotHandler null 检查延迟**

在 onNext 开头增加 handler null 检查：

```java
@Override
public void onNext(SnapshotChunk chunk) {
    try {
        if (errored) return;
        if (installSnapshotHandler == null) {
            log.warn("InstallSnapshot received but no handler registered");
            errored = true;
            responseObserver.onError(new IllegalStateException("No installSnapshotHandler"));
            return;
        }
        // ... existing logic
```

- [ ] **Step 4: 运行测试**

Run: `mvn test -pl loomq-raft -am`
Expected: 全部通过

- [ ] **Step 5: Commit**

```bash
git add loomq-raft/src/main/java/com/loomq/raft/GrpcRaftTransport.java
git commit -m "fix: GrpcRaftTransport InstallSnapshot 异常处理 + double-close 防护 + null handler 提前检查"
```

---

## Task 3: handleInstallSnapshot 安全性修复

**Files:**
- Modify: `loomq-raft/src/main/java/com/loomq/raft/RaftNode.java`

- [ ] **Step 1: 修复 encodeStoreSnapshot 空数据无限循环**

在 sendInstallSnapshot 中，compactThrough 移到数据验证之后：

```java
byte[] snapshotData = encodeStoreSnapshot();
if (snapshotData.length == 0) {
    log.error("Failed to encode snapshot for peer {}, skipping", peerId);
    return;
}
raftLog.compactThrough(snapshotIndex, snapshotEpoch);
```

- [ ] **Step 2: 修复 decodeStoreSnapshot OOM 风险**

在 decodeStoreSnapshot 中增加 count 上限检查：

```java
int count = dis.readInt();
if (count < 0 || count > 10_000_000) { // 10M intents max
    throw new java.io.IOException("Invalid snapshot count: " + count);
}
java.util.List<com.loomq.domain.intent.Intent> result = new java.util.ArrayList<>(count);
```

- [ ] **Step 3: 修复 handleInstallSnapshot store 部分损坏**

将 store.clear() 移到解码成功之后、upsert 之前（当前已经是这个顺序，确认无误）。如果要更安全，可以在 clear 前保存旧数据用于回滚，但这会增加内存开销。当前设计下，解码失败不会触发 clear，只有解码成功后才 clear + upsert。确认当前逻辑：

```java
// 当前逻辑（line 348-368）：
// 1. decodeStoreSnapshot(request.data()) — 如果失败，抛异常，catch 返回 -1，store 不变 ✅
// 2. store.clear() — 只在解码成功后执行
// 3. for upsert — 如果中途失败，catch 返回 -1，store 部分损坏 ❌
```

修复方案：先 upsert 到临时 list，成功后再 clear + 重新 upsert。但这太复杂。更简单的方案：接受这个风险（snapshot 传输失败时 leader 会重试），在 catch 中记录明确的错误信息。

**实际修复**：当前逻辑已经是先解码再 clear，解码失败不会损坏 store。upsert 中途失败的风险很低（IntentBinaryCodec.decode 成功的数据通常能 upsert）。保持现状，不修改。

- [ ] **Step 4: 运行测试**

Run: `mvn test -pl loomq-raft -am`
Expected: 全部通过

- [ ] **Step 5: Commit**

```bash
git add loomq-raft/src/main/java/com/loomq/raft/RaftNode.java
git commit -m "fix: encodeStoreSnapshot 空数据防护 + decodeStoreSnapshot count 上限检查"
```

---

## Task 4: K8sLeaseElection volatile 修复

**Files:**
- Modify: `loomq-raft/src/main/java/com/loomq/raft/K8sLeaseElection.java`
- Modify: `loomq-raft/src/main/java/com/loomq/raft/StandaloneElection.java`

- [ ] **Step 1: K8sLeaseElection 回调字段加 volatile**

```java
// 将：
private Consumer<Long> onBecomeLeader;
private Consumer<Long> onBecomeFollower;
// 改为：
private volatile Consumer<Long> onBecomeLeader;
private volatile Consumer<Long> onBecomeFollower;
```

- [ ] **Step 2: renewTask 加 volatile**

```java
// 将：
private ScheduledFuture<?> renewTask;
// 改为：
private volatile ScheduledFuture<?> renewTask;
```

- [ ] **Step 3: StandaloneElection 回调字段加 volatile**

```java
// 将：
private Consumer<Long> onBecomeLeader;
// 改为：
private volatile Consumer<Long> onBecomeLeader;
```

- [ ] **Step 4: 运行测试**

Run: `mvn test -pl loomq-raft -am`
Expected: 全部通过

- [ ] **Step 5: Commit**

```bash
git add loomq-raft/src/main/java/com/loomq/raft/K8sLeaseElection.java \
        loomq-raft/src/main/java/com/loomq/raft/StandaloneElection.java
git commit -m "fix: K8sLeaseElection + StandaloneElection 回调字段加 volatile"
```

---

## Task 5: GrpcRaftTransport rpcExecutor 分离 + listenHost 绑定

**Files:**
- Modify: `loomq-raft/src/main/java/com/loomq/raft/GrpcRaftTransport.java`

- [ ] **Step 1: 分离 InstallSnapshot 专用线程池**

```java
// 将：
private final ExecutorService rpcExecutor = Executors.newFixedThreadPool(4, r -> {
    Thread t = new Thread(r, "grpc-raft-rpc");
    t.setDaemon(true);
    return t;
});

// 改为：
private final ExecutorService rpcExecutor = Executors.newFixedThreadPool(4, r -> {
    Thread t = new Thread(r, "grpc-raft-rpc");
    t.setDaemon(true);
    return t;
});
private final ExecutorService snapshotExecutor = Executors.newFixedThreadPool(2, r -> {
    Thread t = new Thread(r, "grpc-raft-snapshot");
    t.setDaemon(true);
    return t;
});
```

- [ ] **Step 2: sendInstallSnapshot 使用 snapshotExecutor**

将 sendInstallSnapshot 中的 `CompletableFuture.supplyAsync(() -> { ... }, rpcExecutor)` 改为 `CompletableFuture.supplyAsync(() -> { ... }, snapshotExecutor)`。

- [ ] **Step 3: close() 中关闭 snapshotExecutor**

在 close() 方法中增加 `snapshotExecutor.shutdown()` + `awaitTermination`。

- [ ] **Step 4: listenHost 绑定修复**

将 `ServerBuilder.forPort(listenPort)` 改为实际绑定 listenHost：

```java
grpcServer = ServerBuilder.forAddress(listenHost, listenPort)
    .addService(new RaftServiceImpl())
    .build()
    .start();
```

但需注意：如果 listenHost 为 null 或空，应 fallback 到 forPort。检查 LoomqServerApplication 中的调用方式。

- [ ] **Step 5: 运行测试**

Run: `mvn test -pl loomq-raft -am`
Expected: 全部通过

- [ ] **Step 6: Commit**

```bash
git add loomq-raft/src/main/java/com/loomq/raft/GrpcRaftTransport.java
git commit -m "fix: GrpcRaftTransport snapshot 专用线程池 + listenHost 绑定修复"
```

---

## Task 6: 全量回归验证

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
