# PR3: GrpcRaftTransport Fixes — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix 3 bugs in GrpcRaftTransport (InstallSnapshot memory exhaustion, sendInstallSnapshot backpressure, connect stub-channel race) and extract graceful shutdown helpers.

**Architecture:** Add `currentEpochSupplier` field for per-chunk epoch validation. Use `isReady()` + `setOnReadyHandler` for backpressure-safe chunk sending. Make connect() atomic with `synchronized(connectionLock)`. Extract `gracefulShutdown()` helper for close().

**Tech Stack:** Java 25, gRPC Java, JUnit 5

**Files to modify:**
- `loomq-raft/src/main/java/com/loomq/raft/GrpcRaftTransport.java`
- `loomq-raft/src/main/java/com/loomq/raft/RaftTransport.java` (add setCurrentEpochSupplier)
- `loomq-raft/src/main/java/com/loomq/raft/RaftNode.java` (call setCurrentEpochSupplier)
- `loomq-raft/src/test/java/com/loomq/raft/RaftNodeTest.java` or new test file

---

### Task 1: Add currentEpochSupplier to RaftTransport interface + GrpcRaftTransport

**Files:**
- Modify: `loomq-raft/src/main/java/com/loomq/raft/RaftTransport.java`
- Modify: `loomq-raft/src/main/java/com/loomq/raft/GrpcRaftTransport.java`
- Modify: `loomq-raft/src/main/java/com/loomq/raft/RaftNode.java`

- [ ] **Step 1: Add setCurrentEpochSupplier to RaftTransport interface**

```java
// In RaftTransport.java, add to the 回调注册 section:
void setCurrentEpochSupplier(java.util.function.Supplier<Long> supplier);
```

- [ ] **Step 2: Implement in GrpcRaftTransport**

```java
// New field:
private volatile java.util.function.Supplier<Long> currentEpochSupplier = () -> 0L;

// Implement interface method:
@Override
public void setCurrentEpochSupplier(java.util.function.Supplier<Long> supplier) {
    this.currentEpochSupplier = supplier;
}
```

- [ ] **Step 3: Call from RaftNode**

In `RaftNode.java`, after `transport.setOnInstallSnapshot(...)`:
```java
transport.setCurrentEpochSupplier(election::currentEpoch);
```

- [ ] **Step 4: Add per-chunk epoch check in onNext**

In `GrpcRaftTransport.RaftServiceImpl.installSnapshot().onNext()`, in the `if (totalChunks == -1)` block, after setting epoch:
```java
// Early epoch check — reject stale snapshot before buffering all chunks
if (currentEpochSupplier.get() > chunk.getEpoch()) {
    log.warn("Rejecting stale InstallSnapshot: epoch {} < current epoch {}",
        chunk.getEpoch(), currentEpochSupplier.get());
    errored.set(true);
    responseObserver.onError(new io.grpc.StatusRuntimeException(
        io.grpc.Status.FAILED_PRECONDITION.withDescription("Stale snapshot epoch")));
    return;
}
```

- [ ] **Step 5: Add MAX_SNAPSHOT_BYTES constant**

```java
private static final long MAX_SNAPSHOT_BYTES = 256L * 1024 * 1024; // 256MB max snapshot
```

- [ ] **Step 6: Add total size check in onNext**

In the same `if (totalChunks == -1)` block, after the totalChunks validation:
```java
long estimatedSize = (long) remoteTotalChunks * SNAPSHOT_CHUNK_SIZE;
if (estimatedSize > MAX_SNAPSHOT_BYTES) {
    log.warn("Rejecting InstallSnapshot: estimated size {} > max {}", estimatedSize, MAX_SNAPSHOT_BYTES);
    errored.set(true);
    responseObserver.onError(new IllegalArgumentException(
        "Snapshot too large: " + estimatedSize + " > " + MAX_SNAPSHOT_BYTES));
    return;
}
```

- [ ] **Step 7: Run tests + commit**

```bash
mvn test -pl loomq-raft -am -Pfast-tests
git add -A
git commit -m "fix: InstallSnapshot early epoch check + max size limit (#3)"
```

---

### Task 2: Fix #9 — sendInstallSnapshot backpressure with isReady()

**Files:**
- Modify: `loomq-raft/src/main/java/com/loomq/raft/GrpcRaftTransport.java`

- [ ] **Step 1: Replace tight for-loop with isReady() + setOnReadyHandler**

Replace lines 235-250 (the `for` loop + `requestObserver.onCompleted()`) in `sendInstallSnapshot()` with:

```java
// Use isReady() + setOnReadyHandler for gRPC backpressure
java.util.concurrent.atomic.AtomicInteger chunkIdx = new java.util.concurrent.atomic.AtomicInteger(0);

// Cast to CallStreamObserver to access isReady() and setOnReadyHandler
// Note: The StreamObserver returned by stub.installSnapshot() is a CallStreamObserver
io.grpc.stub.CallStreamObserver<SnapshotChunk> callStream =
    (io.grpc.stub.CallStreamObserver<SnapshotChunk>) requestObserver;

callStream.setOnReadyHandler(() -> {
    while (callStream.isReady()) {
        int i = chunkIdx.getAndIncrement();
        if (i >= totalChunks) {
            requestObserver.onCompleted();
            return;
        }
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
});

// Trigger first send
callStream.onReadyHandler();
```

- [ ] **Step 2: Run tests**

```bash
mvn test -pl loomq-raft -am -Pfast-tests
```

- [ ] **Step 3: Commit**

```bash
git add loomq-raft/src/main/java/com/loomq/raft/GrpcRaftTransport.java
git commit -m "fix: sendInstallSnapshot uses isReady()+setOnReadyHandler for backpressure (#9)"
```

---

### Task 3: Fix #14 — connect() atomic channel+stubs update

**Files:**
- Modify: `loomq-raft/src/main/java/com/loomq/raft/GrpcRaftTransport.java`

- [ ] **Step 1: Add connection lock**

```java
private final Object connectionLock = new Object();
```

- [ ] **Step 2: Rewrite connect() to be atomic**

Replace the connect() lambda body with:

```java
@Override
public CompletableFuture<Void> connect(String peerId, String host, int port) {
    return CompletableFuture.runAsync(() -> {
        synchronized (connectionLock) {
            ManagedChannel newChannel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext().build();
            ManagedChannel existing = channels.put(peerId, newChannel);
            if (existing != null) {
                existing.shutdown();
            }
            blockingStubs.put(peerId, RaftServiceGrpc.newBlockingStub(newChannel));
            asyncStubs.put(peerId, RaftServiceGrpc.newStub(newChannel));
        }
        log.info("GrpcRaftTransport connected to {} at {}:{}", peerId, host, port);
    }, rpcExecutor);
}
```

- [ ] **Step 3: Run tests + commit**

```bash
mvn test -pl loomq-raft -am -Pfast-tests
git add loomq-raft/src/main/java/com/loomq/raft/GrpcRaftTransport.java
git commit -m "fix: connect() atomic channel+stubs update prevents TOCTOU race (#14)"
```

---

### Task 4: Refactor — extract gracefulShutdown helpers

**Files:**
- Modify: `loomq-raft/src/main/java/com/loomq/raft/GrpcRaftTransport.java`

- [ ] **Step 1: Add gracefulShutdown helper methods**

```java
private void gracefulShutdown(java.util.concurrent.ExecutorService svc, String name) {
    svc.shutdown();
    try {
        if (!svc.awaitTermination(5, TimeUnit.SECONDS)) {
            log.warn("{} did not terminate gracefully, forcing shutdown", name);
            svc.shutdownNow();
        }
    } catch (InterruptedException e) {
        svc.shutdownNow();
        Thread.currentThread().interrupt();
    }
}

private void gracefulShutdown(io.grpc.Server server) {
    server.shutdown();
    try {
        if (!server.awaitTermination(5, TimeUnit.SECONDS)) {
            server.shutdownNow();
        }
    } catch (InterruptedException e) {
        server.shutdownNow();
        Thread.currentThread().interrupt();
    }

private void gracefulShutdown(ManagedChannel channel) {
    channel.shutdown();
    try {
        if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
            channel.shutdownNow();
        }
    } catch (InterruptedException e) {
        channel.shutdownNow();
        Thread.currentThread().interrupt();
    }
}
```

- [ ] **Step 2: Refactor close() to use helpers**

Replace the close() method body with:
```java
@Override
public void close() {
    if (grpcServer != null) {
        gracefulShutdown(grpcServer);
    }
    channels.values().forEach(this::gracefulShutdown);
    gracefulShutdown(rpcExecutor, "rpcExecutor");
    gracefulShutdown(snapshotExecutor, "snapshotExecutor");
}
```

- [ ] **Step 3: Run tests + commit**

```bash
mvn test -pl loomq-raft -am -Pfast-tests
git add loomq-raft/src/main/java/com/loomq/raft/GrpcRaftTransport.java
git commit -m "refactor: extract gracefulShutdown helpers for close()"
```

---

### Task 5: Format + verify

- [ ] **Step 1: Format**

```bash
make format
make check-format
```

- [ ] **Step 2: Full test suite**

```bash
mvn test -pl loomq-raft -am -Pfast-tests
```

- [ ] **Step 3: Update InMemoryRaftTransport and test mocks**

Since `setCurrentEpochSupplier` was added to the `RaftTransport` interface, all implementing classes need the method. Check:
- `InMemoryRaftTransport.java` — needs the method (no-op is fine)
- Any test mocks — need updating

```bash
mvn test -pl loomq-raft -am -Pslow-tests
```

- [ ] **Step 4: Commit formatting**

```bash
git add -A
git commit -m "style: apply spotless formatting"
```

---

## Summary

| Task | Bug/Fix | What |
|------|---------|------|
| 1 | #3 | Per-chunk epoch check + max size limit + currentEpochSupplier |
| 2 | #9 | isReady() + setOnReadyHandler backpressure |
| 3 | #14 | Atomic connect() with connectionLock |
| 4 | Refactor | gracefulShutdown helpers |
| 5 | Verify | Format + tests + implementor updates |
