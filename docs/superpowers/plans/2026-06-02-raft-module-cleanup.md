# Raft 模块三项清理 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 消除 Raft 模块的三架构问题：Transport 死代码、instanceof 泄露抽象、K8s 时钟偏移 split-brain 风险。

**Architecture:** 提取 `RaftTransport` 接口 + 统一 `LeaderElection.stepDown()` + K8s fencing token 纵深防御。全面迁移 gRPC，删除自研 TCP 传输。

**Tech Stack:** Java 25, gRPC, Protobuf, K8s Lease API, JUnit 5

---

## 文件结构

```
loomq-raft/src/main/java/com/loomq/raft/
├── RaftTransport.java           ← 从具体类改为接口（含 DTO + TransportException）
├── GrpcRaftTransport.java       ← 实现 RaftTransport 接口
├── InMemoryRaftTransport.java   ← 新增，单元测试用
├── LeaderElection.java          ← 新增 stepDown(long)
├── RaftElection.java            ← 实现 stepDown()，统一回调
├── K8sLeaseElection.java        ← forceStepDown → stepDown，fencing token + 单调时钟
├── K8sLeaseConfig.java          ← 新增 clockSkewBufferSeconds 字段
├── StandaloneElection.java      ← 实现 stepDown() (no-op)
├── RaftNode.java                ← 删除 4 处 instanceof，依赖接口
├── RaftLog.java                 ← 不变
├── LogReplication.java          ← 不变
├── RaftWriteCoordinator.java    ← 不变
├── AppendEntriesResult.java     ← 不变
└── RaftConfig.java              ← 不变

loomq-raft/src/main/proto/loomq/raft/
└── raft_service.proto           ← 删除 RequestVote RPC 和消息

loomq-raft/src/test/java/com/loomq/raft/
├── LeaderElectionTest.java      ← 适配 stepDown 接口
├── K8sLeaseElectionTest.java    ← 新增 fencing token / 单调时钟测试
├── RaftNodeTest.java            ← ControlledRaftTransport 改为实现接口
├── RaftTransportTest.java       ← 删除（测试已删除的 TCP 编解码）
└── RaftSoakTest.java            ← 不变（不直接依赖 transport）
```

---

## Task 1: LeaderElection 接口新增 stepDown

**Files:**
- Modify: `loomq-raft/src/main/java/com/loomq/raft/LeaderElection.java`
- Modify: `loomq-raft/src/main/java/com/loomq/raft/StandaloneElection.java`
- Modify: `loomq-raft/src/main/java/com/loomq/raft/K8sLeaseElection.java`
- Modify: `loomq-raft/src/main/java/com/loomq/raft/RaftElection.java`

- [ ] **Step 1: 在 LeaderElection 接口添加 stepDown 方法**

```java
// LeaderElection.java — 在 addBecomeFollowerListener 方法后添加
void stepDown(long newEpoch);
```

- [ ] **Step 2: StandaloneElection 实现 stepDown（no-op）**

```java
// StandaloneElection.java — 添加方法
@Override
public void stepDown(long newEpoch) {
    // 单机模式永不退位
}
```

- [ ] **Step 3: K8sLeaseElection 将 forceStepDown 重命名为 stepDown**

```java
// K8sLeaseElection.java
// 将 public void forceStepDown(long newEpoch) 重命名为：
@Override
public void stepDown(long newEpoch) {
    synchronized (this) {
        if (newEpoch > currentEpoch) {
            currentEpoch = newEpoch;
            persistEpoch();
        }
        if (isLeaderRole) {
            becomeFollower();
        }
    }
}
```

同时删除旧的 `forceStepDown` 方法（如果 `stepDown` 签名完全一致则直接重命名）。

- [ ] **Step 4: RaftElection 实现 stepDown**

RaftElection 已有 `stepDown(long epoch)` 方法，确认其签名与接口一致。如已有则无需改动，仅确保加 `@Override` 注解。

- [ ] **Step 5: 运行全部 raft 测试验证无回归**

Run: `mvn test -pl loomq-raft -am`
Expected: 全部通过

- [ ] **Step 6: Commit**

```bash
git add loomq-raft/src/main/java/com/loomq/raft/LeaderElection.java \
        loomq-raft/src/main/java/com/loomq/raft/StandaloneElection.java \
        loomq-raft/src/main/java/com/loomq/raft/K8sLeaseElection.java \
        loomq-raft/src/main/java/com/loomq/raft/RaftElection.java
git commit -m "refactor: LeaderElection 接口新增 stepDown()，统一退位机制"
```

---

## Task 2: RaftNode 消除 instanceof

**Files:**
- Modify: `loomq-raft/src/main/java/com/loomq/raft/RaftNode.java`

- [ ] **Step 1: 消除 instanceof #1 — 构造器 RequestVote handler（line 107）**

删除以下代码块（line 106-114）：

```java
// 删除这段：
if (election instanceof RaftElection raftElection) {
    transport.setOnRequestVote(msg -> {
        boolean granted = raftElection.handleRequestVote(msg.epoch(), msg.candidateId(),
            msg.lastLogIndex(), msg.lastLogEpoch());
        syncRaftMetrics();
        return granted;
    });
}
```

- [ ] **Step 2: 消除 instanceof #2 — 构造器生命周期回调（line 137）**

将：

```java
if (election instanceof RaftElection raftElection) {
    raftElection.setOnElectionStarted(this::onElectionStarted);
    raftElection.setOnBecomeLeader(this::onBecomeLeader);
    raftElection.setOnBecomeFollower(this::onBecomeFollower);
} else {
    // K8s Lease mode — register listeners via interface
    election.addBecomeLeaderListener(this::onBecomeLeader);
    election.addBecomeFollowerListener(this::onBecomeFollower);
}
```

改为：

```java
// 统一用接口 listener
election.addBecomeLeaderListener(this::onBecomeLeader);
election.addBecomeFollowerListener(this::onBecomeFollower);

// RaftElection 特有：选举开始回调（构造器已知具体类型，不需要 instanceof）
if (externalElection == null) {
    ((RaftElection) election).setOnElectionStarted(this::onElectionStarted);
}
```

注意：`externalElection == null` 时 election 一定是 `RaftElection`（line 90-96 的逻辑保证），所以这里用条件判断替代 instanceof，语义更清晰。

- [ ] **Step 3: 消除 instanceof #3 — onElectionStarted guard（line 506）**

删除以下 guard：

```java
// 删除这行：
if (!(election instanceof RaftElection raftElection)) return; // K8s mode: no RequestVote
```

同时将方法体中引用 `raftElection` 的地方改为直接使用 `election`：

```java
// 将：
if (total >= majority && raftElection.role() == RaftRole.CANDIDATE) {
    raftElection.becomeLeader(epoch);
}
// 改为：
if (total >= majority && election.role() == RaftRole.CANDIDATE) {
    ((RaftElection) election).becomeLeader(epoch);
}
```

注意：`onElectionStarted` 只在 `RaftElection` 模式下被调用（通过 `setOnElectionStarted`），所以这里的 cast 是安全的。但为更清晰，可以将 `onElectionStarted` 改为接收 `RaftElection` 参数：

```java
// RaftElection.setOnElectionStarted 的回调签名改为 Consumer<Long>（不变）
// 在方法内部，由于只在 RaftElection 模式下触发，cast 是安全的
```

或者更干净的做法：将 `onElectionStarted` 中的 `raftElection.becomeLeader(epoch)` 调用移到 `RaftElection` 内部。`RaftElection.startElection()` 在单节点模式下已经调用 `becomeLeader`，在多节点模式下触发 `onElectionStarted` 回调。可以让 `RaftElection` 直接处理投票计数，但这会改变架构。**保持当前 cast 方案，因为 `onElectionStarted` 只在 RaftElection 模式下触发。**

- [ ] **Step 4: 消除 instanceof #4 — sendHeartbeats step-down（line 657）**

将：

```java
} else if (result.epoch > currentEpoch) {
    if (election instanceof RaftElection raftElection) {
        raftElection.stepDown(result.epoch);
    } else if (election instanceof K8sLeaseElection k8sElection) {
        log.warn("AppendEntries rejected: follower {} has higher epoch {} > {}, stepping down",
            ps.peerId, result.epoch, currentEpoch);
        k8sElection.forceStepDown(result.epoch);
    }
}
```

改为：

```java
} else if (result.epoch > currentEpoch) {
    log.warn("AppendEntries rejected: follower {} has higher epoch {} > {}, stepping down",
        ps.peerId, result.epoch, currentEpoch);
    election.stepDown(result.epoch);
}
```

- [ ] **Step 5: 运行全部 raft 测试**

Run: `mvn test -pl loomq-raft -am`
Expected: 全部通过

- [ ] **Step 6: Commit**

```bash
git add loomq-raft/src/main/java/com/loomq/raft/RaftNode.java
git commit -m "refactor: RaftNode 消除 4 处 instanceof，统一使用 LeaderElection 接口"
```

---

## Task 3: 删除 RaftTransportTest（测试已删除的 TCP 编解码）

**Files:**
- Delete: `loomq-raft/src/test/java/com/loomq/raft/RaftTransportTest.java`

- [ ] **Step 1: 删除 RaftTransportTest.java**

此文件测试 `RaftTransport.encodeRequestVote` / `decodeRequestVote` 等 TCP 编解码静态方法。这些方法将在 Task 6 中随 TCP 传输一起删除。先删除测试以避免编译失败。

- [ ] **Step 2: 运行测试确认无影响**

Run: `mvn test -pl loomq-raft -am`
Expected: 全部通过（此测试类不被其他测试引用）

- [ ] **Step 3: Commit**

```bash
git add -u loomq-raft/src/test/java/com/loomq/raft/RaftTransportTest.java
git commit -m "test: 删除 RaftTransportTest（TCP 编解码测试，将在 Transport 接口化时移除）"
```

---

## Task 4: 提取 RaftTransport 接口 + TransportException

**Files:**
- Rewrite: `loomq-raft/src/main/java/com/loomq/raft/RaftTransport.java`

- [ ] **Step 1: 将 RaftTransport.java 改为接口**

将整个文件替换为：

```java
package com.loomq.raft;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Raft 节点间传输抽象。
 * DTO 为纯 POJO record，不依赖 protobuf 或 gRPC。
 */
public interface RaftTransport extends AutoCloseable {

    // --- DTO 定义 ---

    record AppendEntriesRequest(
        long epoch, String leaderId, long prevLogIndex, long prevLogEpoch,
        List<byte[]> entries, long leaderCommit
    ) {}

    record AppendEntriesResponse(
        long epoch, boolean success, long matchIndex, long conflictIndex
    ) {}

    record InstallSnapshotRequest(
        long epoch, String leaderId, long lastIncludedIndex,
        long lastIncludedEpoch, int chunkIndex, int totalChunks, byte[] data
    ) {}

    record InstallSnapshotResponse(long epoch, long bytesReceived) {}

    // --- 生命周期 ---

    void start() throws Exception;
    CompletableFuture<Void> connect(String peerId, String host, int port);
    void disconnect(String peerId);

    // --- RPC 方法 ---

    CompletableFuture<AppendEntriesResponse> sendAppendEntries(
        String peerId, AppendEntriesRequest request);
    CompletableFuture<InstallSnapshotResponse> sendInstallSnapshot(
        String peerId, InstallSnapshotRequest request);

    // --- 回调注册 ---

    void setOnAppendEntries(
        Function<AppendEntriesRequest, AppendEntriesResponse> handler);
    void setOnInstallSnapshot(
        Function<InstallSnapshotRequest, InstallSnapshotResponse> handler);

    // --- 关闭 ---

    @Override
    void close();
}
```

- [ ] **Step 2: 创建 TransportException**

```java
// 新文件：loomq-raft/src/main/java/com/loomq/raft/TransportException.java
package com.loomq.raft;

public class TransportException extends RuntimeException {
    private final boolean retriable;

    public TransportException(String message, Throwable cause, boolean retriable) {
        super(message, cause);
        this.retriable = retriable;
    }

    public TransportException(String message, boolean retriable) {
        super(message);
        this.retriable = retriable;
    }

    public boolean isRetriable() { return retriable; }
}
```

- [ ] **Step 3: Commit（接口和异常定义先就位，后续 Task 适配实现）**

```bash
git add loomq-raft/src/main/java/com/loomq/raft/RaftTransport.java \
        loomq-raft/src/main/java/com/loomq/raft/TransportException.java
git commit -m "refactor: RaftTransport 改为接口 + 新增 TransportException"
```

注意：此时编译会失败（GrpcRaftTransport、RaftNode 等还未适配）。这是预期的——后续 Task 逐个修复。

---

## Task 5: InMemoryRaftTransport 实现

**Files:**
- Create: `loomq-raft/src/main/java/com/loomq/raft/InMemoryRaftTransport.java`

- [ ] **Step 1: 创建 InMemoryRaftTransport**

```java
package com.loomq.raft;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * 单元测试用 Transport 实现。直接传递 DTO，无网络。
 */
public class InMemoryRaftTransport implements RaftTransport {

    private Function<AppendEntriesRequest, AppendEntriesResponse> appendEntriesHandler;
    private Function<InstallSnapshotRequest, InstallSnapshotResponse> installSnapshotHandler;

    @Override
    public void start() {
        // no-op
    }

    @Override
    public CompletableFuture<Void> connect(String peerId, String host, int port) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void disconnect(String peerId) {
        // no-op
    }

    @Override
    public CompletableFuture<AppendEntriesResponse> sendAppendEntries(
            String peerId, AppendEntriesRequest request) {
        if (appendEntriesHandler != null) {
            return CompletableFuture.completedFuture(appendEntriesHandler.apply(request));
        }
        return CompletableFuture.completedFuture(
            new AppendEntriesResponse(request.epoch(), false, -1, -1));
    }

    @Override
    public CompletableFuture<InstallSnapshotResponse> sendInstallSnapshot(
            String peerId, InstallSnapshotRequest request) {
        if (installSnapshotHandler != null) {
            return CompletableFuture.completedFuture(installSnapshotHandler.apply(request));
        }
        return CompletableFuture.completedFuture(
            new InstallSnapshotResponse(request.epoch(), 0));
    }

    @Override
    public void setOnAppendEntries(
            Function<AppendEntriesRequest, AppendEntriesResponse> handler) {
        this.appendEntriesHandler = handler;
    }

    @Override
    public void setOnInstallSnapshot(
            Function<InstallSnapshotRequest, InstallSnapshotResponse> handler) {
        this.installSnapshotHandler = handler;
    }

    @Override
    public void close() {
        // no-op
    }
}
```

- [ ] **Step 2: Commit**

```bash
git add loomq-raft/src/main/java/com/loomq/raft/InMemoryRaftTransport.java
git commit -m "feat: 新增 InMemoryRaftTransport（单元测试用）"
```

---

## Task 6: GrpcRaftTransport 适配 RaftTransport 接口

**Files:**
- Modify: `loomq-raft/src/main/java/com/loomq/raft/GrpcRaftTransport.java`

- [ ] **Step 1: 修改类声明为实现接口**

```java
// 将：
public class GrpcRaftTransport implements AutoCloseable {
// 改为：
public class GrpcRaftTransport implements RaftTransport {
```

- [ ] **Step 2: 适配方法签名**

将所有方法签名改为匹配 `RaftTransport` 接口：

1. `listen(host, port)` → `start()`
2. `connect(peerId, host, port)` → 返回 `CompletableFuture<Void>`
3. 新增 `disconnect(peerId)`
4. `sendAppendEntries(...)` 参数改为 DTO，返回 `CompletableFuture<AppendEntriesResponse>`
5. `sendInstallSnapshot(...)` 参数改为 DTO，返回 `CompletableFuture<InstallSnapshotResponse>`
6. `setOnAppendEntries(...)` / `setOnInstallSnapshot(...)` 参数改为 DTO 类型
7. 删除 `setOnRequestVote` / `setOnInstallSnapshotChunk`（接口不要求）
8. 删除 `sendRequestVote` / `sendInstallSnapshotChunk`（接口不要求）

内部实现：DTO ↔ protobuf 转换。例如：

```java
@Override
public CompletableFuture<AppendEntriesResponse> sendAppendEntries(
        String peerId, AppendEntriesRequest request) {
    // DTO → protobuf
    com.loomq.raft.grpc.AppendEntriesRequest protoReq =
        com.loomq.raft.grpc.AppendEntriesRequest.newBuilder()
            .setEpoch(request.epoch())
            .setLeaderId(request.leaderId())
            .setPrevLogIndex(request.prevLogIndex())
            .setPrevLogEpoch(request.prevLogEpoch())
            .addAllEntries(request.entries().stream()
                .map(com.google.protobuf.ByteString::copyFrom)
                .toList())
            .setLeaderCommit(request.leaderCommit())
            .build();

    // gRPC call
    com.loomq.raft.grpc.AppendEntriesResponse protoResp =
        getBlockingStub(peerId).appendEntries(protoReq);

    // protobuf → DTO
    return CompletableFuture.completedFuture(new AppendEntriesResponse(
        protoResp.getEpoch(),
        protoResp.getSuccess(),
        protoResp.getMatchIndex(),
        protoResp.getConflictIndex()));
}
```

- [ ] **Step 3: 处理 gRPC server-side handler 中的 DTO 转换**

`RaftServiceImpl` 收到 protobuf 请求后转换为接口 DTO 调用注册的 handler：

```java
@Override
public void appendEntries(com.loomq.raft.grpc.AppendEntriesRequest request,
        StreamObserver<com.loomq.raft.grpc.AppendEntriesResponse> responseObserver) {
    // protobuf → DTO
    List<byte[]> entries = request.getEntriesList().stream()
        .map(ByteString::toByteArray)
        .toList();
    RaftTransport.AppendEntriesRequest dtoReq = new RaftTransport.AppendEntriesRequest(
        request.getEpoch(), request.getLeaderId(),
        request.getPrevLogIndex(), request.getPrevLogEpoch(),
        entries, request.getLeaderCommit());

    // 调用 handler
    RaftTransport.AppendEntriesResponse dtoResp = appendEntriesHandler.apply(dtoReq);

    // DTO → protobuf
    com.loomq.raft.grpc.AppendEntriesResponse protoResp =
        com.loomq.raft.grpc.AppendEntriesResponse.newBuilder()
            .setSuccess(dtoResp.success())
            .setMatchIndex(dtoResp.matchIndex())
            .setConflictIndex(dtoResp.conflictIndex())
            .setEpoch(dtoResp.epoch())
            .build();
    responseObserver.onNext(protoResp);
    responseObserver.onCompleted();
}
```

- [ ] **Step 4: Commit**

```bash
git add loomq-raft/src/main/java/com/loomq/raft/GrpcRaftTransport.java
git commit -m "refactor: GrpcRaftTransport 实现 RaftTransport 接口"
```

---

## Task 7: RaftNode 适配 RaftTransport 接口

**Files:**
- Modify: `loomq-raft/src/main/java/com/loomq/raft/RaftNode.java`

- [ ] **Step 1: 将 transport 字段类型改为接口**

```java
// 将：
private final RaftTransport transport;
// 无需改动 — RaftTransport 现在是接口
```

字段类型已经是 `RaftTransport`，无需改动（接口和旧类同名）。

- [ ] **Step 2: 修改构造器中的 handler 注册**

将旧的 `setOnRequestVote` / `setOnAppendEntries` / `setOnInstallSnapshot` / `setOnInstallSnapshotChunk` 改为新接口的 `setOnAppendEntries` / `setOnInstallSnapshot`：

```java
// Server-side RPC handlers (transport may be null for single-node/no-network tests)
if (transport != null) {
    transport.setOnAppendEntries(request -> {
        AppendEntriesResult result = replication.handleAppendEntries(
            request.epoch(), request.leaderId(),
            request.prevLogIndex(), request.prevLogEpoch(),
            request.entries().toArray(new byte[0][]),
            request.leaderCommit());
        syncRaftMetrics();
        return new RaftTransport.AppendEntriesResponse(
            result.epoch, result.success, result.matchIndex, result.conflictIndex);
    });

    transport.setOnInstallSnapshot(request -> {
        // 统一处理 InstallSnapshot：根据 totalChunks 区分单块 vs 分块
        Long appliedIndex;
        if (request.totalChunks() <= 1) {
            // 小 snapshot：单块传输
            appliedIndex = handleInstallSnapshot(request);
        } else {
            // 大 snapshot：分块传输，缓冲后重组
            appliedIndex = handleInstallSnapshotChunk(request);
        }
        syncRaftMetrics();
        return new RaftTransport.InstallSnapshotResponse(
            request.epoch(), appliedIndex != null ? appliedIndex : -1);
    });
}
```

注意：需要修改 `handleInstallSnapshot` 和 `handleInstallSnapshotChunk` 方法签名以接受新的 DTO 类型。由于接口合并了 `InstallSnapshotMessage` 和 `InstallSnapshotChunkMessage`，需要在 handler 中区分单块和分块场景。

- [ ] **Step 3: 修改 sendHeartbeats 中的 transport 调用**

将：

```java
transport.sendAppendEntries(ps.peerId, currentEpoch, nodeId,
        prevLogIndex, prevLogEpoch, entries, commitIdx)
    .whenComplete((result, throwable) -> {
        // result 是 AppendEntriesResult
        if (result.success) { ... }
        else if (result.epoch > currentEpoch) { ... }
    });
```

改为：

```java
List<byte[]> entryList = java.util.Arrays.asList(entries);
transport.sendAppendEntries(ps.peerId,
        new RaftTransport.AppendEntriesRequest(
            currentEpoch, nodeId, prevLogIndex, prevLogEpoch, entryList, commitIdx))
    .whenComplete((response, throwable) -> {
        if (throwable != null) { ... }
        if (requestGeneration != ps.requestGeneration) return;
        if (myGeneration != leaderGeneration) return;

        if (response.success()) {
            ps.matchIndex = response.matchIndex();
            ps.nextIndex = response.matchIndex() + 1;
            // ... quorumAcks, leaseRenewed
        } else if (response.epoch() > currentEpoch) {
            election.stepDown(response.epoch());
        } else {
            if (response.conflictIndex() > 0) {
                ps.nextIndex = Math.max(1, response.conflictIndex());
            } else {
                ps.nextIndex = Math.max(1, ps.nextIndex - 1);
            }
            ps.inflightMaxIndex = 0;
        }
    });
```

- [ ] **Step 4: 修改 sendInstallSnapshot 中的 transport 调用**

将旧的 `transport.sendInstallSnapshot(peerId, epoch, nodeId, snapshotIndex, snapshotEpoch, snapshotData)` 和 `transport.sendInstallSnapshotChunk(...)` 统一改为新接口的 `sendInstallSnapshot(peerId, new InstallSnapshotRequest(...))`。

小 snapshot（单块，chunkIndex=0, totalChunks=1）：

```java
transport.sendInstallSnapshot(peerId,
    new RaftTransport.InstallSnapshotRequest(
        epoch, nodeId, snapshotIndex, snapshotEpoch,
        0, 1, snapshotData))
.thenAccept(response -> {
    if (requestGeneration != ps.requestGeneration) return;
    if (response.bytesReceived() >= 0) {
        ps.nextIndex = snapshotIndex + 1;
        ps.matchIndex = snapshotIndex;
    }
});
```

大 snapshot（分块，每块一个 `sendInstallSnapshot` 调用）：

```java
int totalChunks = (snapshotData.length + SNAPSHOT_CHUNK_SIZE - 1) / SNAPSHOT_CHUNK_SIZE;
CompletableFuture<Boolean> chain = CompletableFuture.completedFuture(true);

for (int i = 0; i < totalChunks; i++) {
    final int chunkIndex = i;
    byte[] chunk = java.util.Arrays.copyOfRange(snapshotData,
        i * SNAPSHOT_CHUNK_SIZE, Math.min((i + 1) * SNAPSHOT_CHUNK_SIZE, snapshotData.length));

    chain = chain.thenCompose(ok -> {
        if (requestGeneration != ps.requestGeneration) {
            return CompletableFuture.completedFuture(false);
        }
        return transport.sendInstallSnapshot(peerId,
            new RaftTransport.InstallSnapshotRequest(
                epoch, nodeId, snapshotIndex, snapshotEpoch,
                chunkIndex, totalChunks, chunk))
            .thenApply(resp -> resp.bytesReceived() >= 0);
    });
}

chain.thenAccept(success -> {
    if (requestGeneration != ps.requestGeneration) return;
    if (success) {
        ps.nextIndex = snapshotIndex + 1;
        ps.matchIndex = snapshotIndex;
    }
});
```
```

- [ ] **Step 5: 修改 handleInstallSnapshot / handleInstallSnapshotChunk 签名**

将参数从 `RaftTransport.InstallSnapshotMessage` / `RaftTransport.InstallSnapshotChunkMessage` 统一改为 `RaftTransport.InstallSnapshotRequest`：

```java
// handleInstallSnapshot：处理小 snapshot（totalChunks <= 1）
private Long handleInstallSnapshot(RaftTransport.InstallSnapshotRequest request) {
    if (request.epoch() < election.currentEpoch()) {
        log.debug("Rejecting InstallSnapshot: stale epoch {} < {}", request.epoch(), election.currentEpoch());
        return -1L;
    }
    election.onAppendEntries(request.epoch(), request.leaderId());
    // ... 其余逻辑不变，将 msg.xxx() 改为 request.xxx()
    // request.data() 对应原来的 snapshotData
}

// handleInstallSnapshotChunk：处理大 snapshot 分块（totalChunks > 1）
private Long handleInstallSnapshotChunk(RaftTransport.InstallSnapshotRequest request) {
    // ... 逻辑与原 handleInstallSnapshotChunk 相同
    // request.chunkIndex(), request.totalChunks(), request.data() 对应原 msg 字段
    // reassemblyKey = request.leaderId() + ":" + request.lastIncludedIndex()
}
```

- [ ] **Step 6: 运行测试**

Run: `mvn test -pl loomq-raft -am`
Expected: 全部通过

- [ ] **Step 7: Commit**

```bash
git add loomq-raft/src/main/java/com/loomq/raft/RaftNode.java
git commit -m "refactor: RaftNode 适配 RaftTransport 接口"
```

---

## Task 8: RaftNodeTest.ControlledRaftTransport 适配接口

**Files:**
- Modify: `loomq-raft/src/test/java/com/loomq/raft/RaftNodeTest.java`

- [ ] **Step 1: 将 ControlledRaftTransport 改为实现 RaftTransport 接口**

将：

```java
private static final class ControlledRaftTransport extends RaftTransport {
```

改为：

```java
private static final class ControlledRaftTransport implements RaftTransport {
```

- [ ] **Step 2: 适配方法签名**

```java
private static final class ControlledRaftTransport implements RaftTransport {
    private final CountDownLatch appendRequested = new CountDownLatch(1);
    private final CompletableFuture<RaftTransport.AppendEntriesResponse> appendResult = new CompletableFuture<>();

    @Override
    public void start() { /* no-op */ }

    @Override
    public CompletableFuture<Void> connect(String peerId, String host, int port) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void disconnect(String peerId) { /* no-op */ }

    @Override
    public CompletableFuture<RaftTransport.AppendEntriesResponse> sendAppendEntries(
            String peerId, RaftTransport.AppendEntriesRequest request) {
        appendRequested.countDown();
        return appendResult;
    }

    @Override
    public CompletableFuture<RaftTransport.InstallSnapshotResponse> sendInstallSnapshot(
            String peerId, RaftTransport.InstallSnapshotRequest request) {
        return CompletableFuture.completedFuture(
            new RaftTransport.InstallSnapshotResponse(request.epoch(), -1));
    }

    @Override
    public void setOnAppendEntries(
            java.util.function.Function<RaftTransport.AppendEntriesRequest, RaftTransport.AppendEntriesResponse> handler) {
        // no-op for tests
    }

    @Override
    public void setOnInstallSnapshot(
            java.util.function.Function<RaftTransport.InstallSnapshotRequest, RaftTransport.InstallSnapshotResponse> handler) {
        // no-op for tests
    }

    @Override
    public void close() { /* no-op */ }

    boolean awaitAppendRequest(long timeout, TimeUnit unit) throws InterruptedException {
        return appendRequested.await(timeout, unit);
    }

    void completeAppend(AppendEntriesResult result) {
        appendResult.complete(new RaftTransport.AppendEntriesResponse(
            result.epoch, result.success, result.matchIndex, result.conflictIndex));
    }
}
```

- [ ] **Step 3: 适配测试中的 transport 构造**

将 `new ControlledRaftTransport("node-1")` 改为 `new ControlledRaftTransport()`（不再需要 nodeId 参数，因为不再继承旧类的构造器）。

- [ ] **Step 4: 运行 RaftNodeTest**

Run: `mvn test -pl loomq-raft -am -Dtest=RaftNodeTest`
Expected: 全部通过

- [ ] **Step 5: Commit**

```bash
git add loomq-raft/src/test/java/com/loomq/raft/RaftNodeTest.java
git commit -m "test: RaftNodeTest.ControlledRaftTransport 适配 RaftTransport 接口"
```

---

## Task 9: 删除 TCP 传输和 RequestVote

**Files:**
- Delete: 旧 `RaftTransport` 中的 TCP 实现代码（已改为接口，无需删除）
- Modify: `loomq-raft/src/main/proto/loomq/raft/raft_service.proto`

- [ ] **Step 1: 删除 proto 中的 RequestVote 定义**

从 `raft_service.proto` 中删除：

```protobuf
// 删除这段：
// RequestVote RPC (§5.2)
rpc RequestVote (RequestVoteRequest) returns (RequestVoteResponse);

// 删除这段：
// ========== RequestVote ==========
message RequestVoteRequest {
    int64 epoch = 1;
    string candidate_id = 2;
    int64 last_log_index = 3;
    int64 last_log_epoch = 4;
}

message RequestVoteResponse {
    bool vote_granted = 1;
}
```

- [ ] **Step 2: 重新生成 protobuf Java 代码**

Run: `mvn generate-sources -pl loomq-raft`
Expected: 编译通过，`RequestVoteRequest` / `RequestVoteResponse` 类不再生成

- [ ] **Step 3: 确认 GrpcRaftTransport 中无 RequestVote 引用**

检查 `GrpcRaftTransport.java` 中是否有对 `RequestVoteRequest` / `RequestVoteResponse` / `RaftServiceGrpc.RaftServiceBlockingStub.requestVote` 的引用。如有，删除。

- [ ] **Step 4: 运行全部测试**

Run: `mvn test -pl loomq-raft -am`
Expected: 全部通过

- [ ] **Step 5: Commit**

```bash
git add loomq-raft/src/main/proto/loomq/raft/raft_service.proto \
        loomq-raft/src/main/java/com/loomq/raft/GrpcRaftTransport.java
git commit -m "refactor: 删除 RequestVote RPC（K8s Lease 选主不需要投票）"
```

---

## Task 10: K8sLeaseElection 时钟偏移防护

**Files:**
- Modify: `loomq-raft/src/main/java/com/loomq/raft/K8sLeaseElection.java`
- Modify: `loomq-raft/src/main/java/com/loomq/raft/K8sLeaseConfig.java`

- [ ] **Step 1: K8sLeaseConfig 新增 clockSkewBufferSeconds**

```java
// K8sLeaseConfig.java — 新增字段
private final long clockSkewBufferSeconds;

// 更新构造器和验证逻辑
public K8sLeaseConfig(int leaseDurationSeconds, int renewIntervalSeconds,
                      String namespace, String leaseName, long clockSkewBufferSeconds) {
    // ... 现有验证 ...
    if (clockSkewBufferSeconds < 0) {
        throw new IllegalArgumentException("clockSkewBufferSeconds must be >= 0");
    }
    if (leaseDurationSeconds <= renewIntervalSeconds + clockSkewBufferSeconds) {
        throw new IllegalArgumentException(
            "leaseDurationSeconds must be > renewIntervalSeconds + clockSkewBufferSeconds");
    }
    this.clockSkewBufferSeconds = clockSkewBufferSeconds;
}

// 默认值
public K8sLeaseConfig(int leaseDurationSeconds, int renewIntervalSeconds,
                      String namespace, String leaseName) {
    this(leaseDurationSeconds, renewIntervalSeconds, namespace, leaseName, 5);
}
```

- [ ] **Step 2: K8sLeaseElection 新增 fencing token 和单调时钟字段**

```java
// K8sLeaseElection.java — 新增字段
private volatile long lastKnownRenewTimeMicros = 0;
private volatile long lastRenewNanoTime = 0;
private final long clockSkewBufferSeconds;

// 构造器中初始化
this.clockSkewBufferSeconds = config.clockSkewBufferSeconds();
```

- [ ] **Step 3: 新增 updateFencingState 方法**

```java
private void updateFencingState(V1Lease lease) {
    OffsetDateTime renewTime = lease.getSpec().getRenewTime();
    if (renewTime != null) {
        lastKnownRenewTimeMicros = renewTime.toInstant().toEpochMilli() * 1000;
    }
    lastRenewNanoTime = System.nanoTime();
}
```

- [ ] **Step 4: 修改 isLeaseExpired 增加安全缓冲**

```java
private boolean isLeaseExpired(V1Lease lease) {
    OffsetDateTime renewTime = lease.getSpec().getRenewTime();
    if (renewTime == null) return true;

    int durationSeconds = lease.getSpec().getLeaseDurationSeconds() != null
        ? lease.getSpec().getLeaseDurationSeconds()
        : config.leaseDurationSeconds();

    // 墙钟比较 + 安全缓冲
    OffsetDateTime expiryTime = renewTime.plusSeconds(durationSeconds)
                                         .minusSeconds(clockSkewBufferSeconds);
    return OffsetDateTime.now().isAfter(expiryTime);
}
```

- [ ] **Step 5: 修改 isLeader 增加单调时钟自检**

```java
@Override
public boolean isLeader() {
    if (!isLeaderRole) return false;

    // 单调时钟检查：距上次续约是否超过 leaseDuration
    long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - lastRenewNanoTime);
    if (elapsedMs > config.leaseDurationSeconds() * 1000L) {
        log.warn("Lease expired (monotonic clock), elapsed={}ms, stepping down", elapsedMs);
        stepDown(currentEpoch);
        return false;
    }

    return true;
}
```

- [ ] **Step 6: 在 tryAcquireLease 和 renewLease 后调用 updateFencingState**

在 `tryAcquireLease()` 中，每次成功创建/续约/获取 Lease 后调用 `updateFencingState(lease)`。

- [ ] **Step 7: 运行 K8sLeaseElectionTest**

Run: `mvn test -pl loomq-raft -am -Dtest=K8sLeaseElectionTest`
Expected: 全部通过

- [ ] **Step 8: Commit**

```bash
git add loomq-raft/src/main/java/com/loomq/raft/K8sLeaseElection.java \
        loomq-raft/src/main/java/com/loomq/raft/K8sLeaseConfig.java
git commit -m "feat: K8sLeaseElection 时钟偏移防护 - fencing token + 单调时钟自检 + 安全缓冲"
```

---

## Task 11: 新增 K8sLeaseElection 时钟偏移测试

**Files:**
- Modify: `loomq-raft/src/test/java/com/loomq/raft/K8sLeaseElectionTest.java`

- [ ] **Step 1: 测试单调时钟过期自动 stepDown**

```java
@Test
void isLeaderShouldReturnFalseWhenMonotonicClockExpired() throws Exception {
    // 构造 K8sLeaseElection，模拟成为 Leader
    // 手动设置 lastRenewNanoTime 为很久以前
    // 验证 isLeader() 返回 false
}
```

- [ ] **Step 2: 测试安全缓冲配置生效**

```java
@Test
void configValidationShouldRejectInvalidClockSkewBuffer() {
    // leaseDuration=10, renewInterval=3, buffer=8 → 10 <= 3+8 → 应抛异常
    assertThrows(IllegalArgumentException.class, () ->
        new K8sLeaseConfig(10, 3, "default", "loomq-leader", 8));
}
```

- [ ] **Step 3: 运行测试**

Run: `mvn test -pl loomq-raft -am -Dtest=K8sLeaseElectionTest`
Expected: 全部通过

- [ ] **Step 4: Commit**

```bash
git add loomq-raft/src/test/java/com/loomq/raft/K8sLeaseElectionTest.java
git commit -m "test: K8sLeaseElection 时钟偏移防护单元测试"
```

---

## Task 12: 全量回归验证 + 格式化

**Files:**
- 全模块

- [ ] **Step 1: 运行全量 raft 测试**

Run: `mvn test -pl loomq-raft -am`
Expected: 全部通过

- [ ] **Step 2: 运行 core 模块测试（确认无传递影响）**

Run: `mvn test -pl loomq-core -am`
Expected: 全部通过

- [ ] **Step 3: Spotless 格式化**

Run: `mvn spotless:apply -pl loomq-raft`
Expected: 格式化完成

- [ ] **Step 4: 格式检查**

Run: `mvn spotless:check -pl loomq-raft`
Expected: 通过

- [ ] **Step 5: Commit**

```bash
git add -u
git commit -m "style: Spotless 格式化"
```

- [ ] **Step 6: 最终验证**

Run: `make check` 或 `mvn spotless:check test -pl loomq-raft -am`
Expected: 全部通过
