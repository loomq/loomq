# InstallSnapshot 分块下沉到 Transport 层 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 InstallSnapshot 的分块/重组逻辑从 RaftNode 下沉到 GrpcRaftTransport，每个快照只创建 1 个 gRPC stream。

**Architecture:** RaftNode 传完整快照数据给 Transport，GrpcRaftTransport 内部拆分并通过单个 client-streaming gRPC stream 发送。服务端收齐所有 chunks 后重组为完整快照再回调 RaftNode。删除 RaftNode 中的 handleInstallSnapshotChunk、pendingChunks、ChunkReassembly。

**Tech Stack:** Java 25, gRPC client-streaming RPC, Protobuf

---

## 文件结构

```
loomq-raft/src/main/java/com/loomq/raft/
├── GrpcRaftTransport.java    ← 重写 sendInstallSnapshot + RaftServiceImpl.installSnapshot
├── RaftNode.java             ← 简化 sendInstallSnapshot + 删除 chunk 相关代码
└── RaftTransport.java        ← 不变（接口 DTO 不变）
```

---

## Task 1: GrpcRaftTransport — 重写 sendInstallSnapshot（发送端）

**Files:**
- Modify: `loomq-raft/src/main/java/com/loomq/raft/GrpcRaftTransport.java`

- [ ] **Step 1: 添加 SNAPSHOT_CHUNK_SIZE 常量**

在 GrpcRaftTransport 类中添加（RaftNode 中已有同名常量，Transport 层需要自己的）：

```java
private static final int SNAPSHOT_CHUNK_SIZE = 256 * 1024; // 256KB per chunk
```

- [ ] **Step 2: 重写 sendInstallSnapshot 方法**

将当前的"每 chunk 开新 stream"实现替换为"单 stream 发送所有 chunks"：

```java
@Override
public CompletableFuture<RaftTransport.InstallSnapshotResponse> sendInstallSnapshot(
        String peerId, RaftTransport.InstallSnapshotRequest request) {
    return CompletableFuture.supplyAsync(() -> {
        RaftServiceGrpc.RaftServiceStub asyncStub = asyncStubs.get(peerId);
        if (asyncStub == null) {
            return new RaftTransport.InstallSnapshotResponse(request.epoch(), -1);
        }
        try {
            byte[] data = request.data();
            int totalChunks = Math.max(1, (data.length + SNAPSHOT_CHUNK_SIZE - 1) / SNAPSHOT_CHUNK_SIZE);

            CountDownLatch latch = new CountDownLatch(1);
            final long[] result = {-1L};

            StreamObserver<SnapshotResponse> responseObserver = new StreamObserver<>() {
                @Override
                public void onNext(SnapshotResponse response) {
                    result[0] = response.getLastIncludedIndex();
                }

                @Override
                public void onError(Throwable t) {
                    log.warn("InstallSnapshot to {} failed: {}", peerId, t.getMessage());
                    latch.countDown();
                }

                @Override
                public void onCompleted() {
                    latch.countDown();
                }
            };

            // 一个 stream 发送所有 chunks
            long deadlineSeconds = Math.max(60, totalChunks * 5L);
            StreamObserver<SnapshotChunk> requestObserver = asyncStub
                .withDeadlineAfter(deadlineSeconds, TimeUnit.SECONDS)
                .installSnapshot(responseObserver);

            for (int i = 0; i < totalChunks; i++) {
                int start = i * SNAPSHOT_CHUNK_SIZE;
                int end = Math.min(start + SNAPSHOT_CHUNK_SIZE, data.length);
                byte[] chunk = java.util.Arrays.copyOfRange(data, start, end);

                requestObserver.onNext(SnapshotChunk.newBuilder()
                    .setEpoch(request.epoch())
                    .setLeaderId(request.leaderId())
                    .setLastIncludedIndex(request.lastIncludedIndex())
                    .setLastIncludedEpoch(request.lastIncludedEpoch())
                    .setChunkIndex(i)
                    .setTotalChunks(totalChunks)
                    .setData(ByteString.copyFrom(chunk))
                    .build());
            }
            requestObserver.onCompleted();

            latch.await(deadlineSeconds, TimeUnit.SECONDS);
            return new RaftTransport.InstallSnapshotResponse(request.epoch(), result[0]);
        } catch (Exception e) {
            log.warn("InstallSnapshot to {} failed: {}", peerId, e.getMessage());
            return new RaftTransport.InstallSnapshotResponse(request.epoch(), -1);
        }
    }, rpcExecutor);
}
```

- [ ] **Step 3: 重写 RaftServiceImpl.installSnapshot（接收端）**

将当前的"每 chunk 直接调用 handler"改为"缓冲所有 chunks，重组后调用 handler"：

```java
@Override
public StreamObserver<SnapshotChunk> installSnapshot(StreamObserver<SnapshotResponse> responseObserver) {
    return new StreamObserver<>() {
        private long epoch;
        private String leaderId;
        private long lastIncludedIndex;
        private long lastIncludedEpoch;
        private int totalChunks = -1;
        private byte[][] chunks;
        private int receivedCount = 0;

        @Override
        public void onNext(SnapshotChunk chunk) {
            try {
                if (totalChunks == -1) {
                    // 第一个 chunk 初始化元数据
                    epoch = chunk.getEpoch();
                    leaderId = chunk.getLeaderId();
                    lastIncludedIndex = chunk.getLastIncludedIndex();
                    lastIncludedEpoch = chunk.getLastIncludedEpoch();
                    totalChunks = chunk.getTotalChunks();
                    chunks = new byte[totalChunks][];
                }

                int idx = chunk.getChunkIndex();
                if (idx >= 0 && idx < totalChunks && chunks[idx] == null) {
                    chunks[idx] = chunk.getData().toByteArray();
                    receivedCount++;
                }
            } catch (Exception e) {
                log.error("InstallSnapshot chunk processing error", e);
                responseObserver.onError(e);
            }
        }

        @Override
        public void onError(Throwable t) {
            log.warn("InstallSnapshot stream error from {}: {}", leaderId, t.getMessage());
        }

        @Override
        public void onCompleted() {
            try {
                if (receivedCount != totalChunks || installSnapshotHandler == null) {
                    log.warn("InstallSnapshot incomplete: received {}/{} chunks", receivedCount, totalChunks);
                    responseObserver.onNext(SnapshotResponse.newBuilder()
                        .setLastIncludedIndex(-1)
                        .build());
                    responseObserver.onCompleted();
                    return;
                }

                // 重组所有 chunks
                int totalSize = 0;
                for (byte[] c : chunks) {
                    totalSize += c != null ? c.length : 0;
                }
                byte[] fullData = new byte[totalSize];
                int offset = 0;
                for (byte[] c : chunks) {
                    if (c != null) {
                        System.arraycopy(c, 0, fullData, offset, c.length);
                        offset += c.length;
                    }
                }

                // 构造 DTO（完整快照，totalChunks=1）
                RaftTransport.InstallSnapshotRequest dtoReq = new RaftTransport.InstallSnapshotRequest(
                    epoch, leaderId, lastIncludedIndex, lastIncludedEpoch,
                    0, 1, fullData);

                RaftTransport.InstallSnapshotResponse dtoResp =
                    installSnapshotHandler.apply(dtoReq);

                responseObserver.onNext(SnapshotResponse.newBuilder()
                    .setLastIncludedIndex(dtoResp.bytesReceived())
                    .build());
                responseObserver.onCompleted();
            } catch (Exception e) {
                log.error("InstallSnapshot completion error", e);
                responseObserver.onError(e);
            }
        }
    };
}
```

- [ ] **Step 4: 运行测试**

Run: `mvn test -pl loomq-raft -am`
Expected: 全部通过

- [ ] **Step 5: Commit**

```bash
git add loomq-raft/src/main/java/com/loomq/raft/GrpcRaftTransport.java
git commit -m "refactor: GrpcRaftTransport InstallSnapshot 单 stream 分块传输"
```

---

## Task 2: RaftNode — 简化 sendInstallSnapshot + 删除 chunk 代码

**Files:**
- Modify: `loomq-raft/src/main/java/com/loomq/raft/RaftNode.java`

- [ ] **Step 1: 简化 sendInstallSnapshot — 移除 chunk 拆分逻辑**

将当前的 if/else（小 snapshot vs 大 snapshot chunked）替换为统一路径：

```java
public void sendInstallSnapshot(String peerId) {
    PeerReplicationState ps = peerStates.get(peerId);
    if (ps == null) {
        return;
    }

    long epoch = election.currentEpoch();
    long snapshotIndex = replication.lastApplied();
    long snapshotEpoch = snapshotIndex > 0 ? raftLog.readEntryEpoch(snapshotIndex) : 0;
    long requestGeneration = ++ps.requestGeneration;

    byte[] snapshotData = encodeStoreSnapshot();
    raftLog.compactThrough(snapshotIndex, snapshotEpoch);

    log.info("Sending snapshot to {}: {} bytes (index={})", peerId, snapshotData.length, snapshotIndex);

    // Transport 层负责分块，RaftNode 传完整数据
    transport.sendInstallSnapshot(peerId,
        new RaftTransport.InstallSnapshotRequest(
            epoch, nodeId, snapshotIndex, snapshotEpoch,
            0, 1, snapshotData))
        .thenAccept(response -> {
            if (requestGeneration != ps.requestGeneration) return;
            if (response.bytesReceived() >= 0) {
                ps.nextIndex = snapshotIndex + 1;
                ps.matchIndex = snapshotIndex;
                log.info("InstallSnapshot accepted by {} (index={})", peerId, snapshotIndex);
            } else {
                log.warn("InstallSnapshot rejected by {}", peerId);
            }
        })
        .exceptionally(ex -> {
            log.error("InstallSnapshot failed for {}: {}", peerId, ex.getMessage(), ex);
            return null;
        });
}
```

- [ ] **Step 2: 简化 setOnInstallSnapshot handler**

将当前的 if/else（totalChunks <= 1 vs > 1）替换为统一调用 handleInstallSnapshot：

```java
transport.setOnInstallSnapshot(request -> {
    // Transport 层已处理分块重组，这里只接收完整快照
    Long appliedIndex = handleInstallSnapshot(request);
    syncRaftMetrics();
    return new RaftTransport.InstallSnapshotResponse(
        request.epoch(), appliedIndex != null ? appliedIndex : -1);
});
```

- [ ] **Step 3: 删除 handleInstallSnapshotChunk 方法**

删除整个方法（当前 line 424-465）。

- [ ] **Step 4: 删除 pendingChunks 字段**

删除声明（当前 line 48）：

```java
private final ConcurrentMap<String, ChunkReassembly> pendingChunks = new ConcurrentHashMap<>();
```

- [ ] **Step 5: 删除 ChunkReassembly 内部类**

删除整个内部类（当前 line 773-826）。

- [ ] **Step 6: 删除 SNAPSHOT_CHUNK_SIZE 常量**

RaftNode 不再需要分块，删除：

```java
private static final int SNAPSHOT_CHUNK_SIZE = 256 * 1024; // 256KB per chunk
```

- [ ] **Step 7: 清理未使用的 import**

删除不再需要的 `ConcurrentHashMap` import（如果无其他使用）。

- [ ] **Step 8: 运行测试**

Run: `mvn test -pl loomq-raft -am`
Expected: 全部通过

- [ ] **Step 9: Commit**

```bash
git add loomq-raft/src/main/java/com/loomq/raft/RaftNode.java
git commit -m "refactor: RaftNode 删除 InstallSnapshot 分块逻辑，Transport 层透明处理"
```

---

## Task 3: 全量回归验证

- [ ] **Step 1: Raft 模块测试**

Run: `mvn test -pl loomq-raft -am`
Expected: 全部通过

- [ ] **Step 2: Server 模块测试**

Run: `mvn test -pl loomq-server -am`
Expected: 全部通过

- [ ] **Step 3: Spotless 格式化**

Run: `mvn spotless:apply -pl loomq-raft`

- [ ] **Step 4: Spotless 检查**

Run: `mvn spotless:check -pl loomq-raft`
Expected: 通过
