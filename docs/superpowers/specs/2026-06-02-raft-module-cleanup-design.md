# 设计文档：Raft 模块三项清理

> **状态：** 设计完成，待实现
> **日期：** 2026-06-02
> **作者：** xuyuchen
> **分支：** dev-xuyuchen

---

## 1. 背景与动机

K8s Lease 选主替换 Raft 选举（Phase 1-3）完成后，遗留三个架构问题：

| 问题 | 影响 |
|------|------|
| `GrpcRaftTransport` 死代码 | 已实现 gRPC 传输但无法注入 `RaftNode`，因 `RaftTransport` 是具体类而非接口 |
| K8s 时钟偏移 split-brain | `isLeaseExpired()` 依赖本地墙钟，时钟偏移 > leaseDuration 时双 Leader |
| `instanceof` 泄露抽象 | `RaftNode` 有 4 处 `instanceof` 检查选举类型，新增选举策略需改 RaftNode |

三个问题互相关联：Transport 抽象和 instanceof 消除是同一个重构的两面，时钟偏移防护则是 K8s 选举的独立加固。

---

## 2. 设计目标

| 目标 | 描述 |
|------|------|
| 全面 gRPC | 删除自研 Netty TCP 传输，`GrpcRaftTransport` 正式接入 |
| 接口隔离 | `RaftNode` 依赖抽象接口，不感知传输协议和选举策略 |
| 纵深防御 | 时钟偏移防护三层：renewTime fencing token → 安全缓冲 → epoch fencing |
| 向后兼容 | WAL 日志格式不变，epoch 语义不变，配置兼容 |

---

## 3. Transport 层重构

### 3.1 现状

`RaftTransport` 是具体类（含自定义 TCP 实现：`ReplicaClient`/`ReplicaServer`），`GrpcRaftTransport` 是独立类，两者 API 完全相同但无共同类型。`RaftNode.transport` 字段类型为 `RaftTransport`，gRPC 实现无法注入。

### 3.2 设计：提取 RaftTransport 接口

全面迁移 gRPC，不再维护自研 Netty TCP 实现。

```java
public interface RaftTransport extends AutoCloseable {

    // --- DTO 定义（纯 POJO，不依赖 protobuf） ---
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
    void start() throws Exception;          // 启动 gRPC 服务端
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

**异常处理**：定义 `TransportException`（运行时异常），统一 gRPC `StatusRuntimeException` 和其他 I/O 异常。`RaftNode` 不需要区分底层传输的异常类型。

```java
public class TransportException extends RuntimeException {
    private final boolean retriable;  // 是否可重试（网络超时 vs 协议错误）

    public TransportException(String message, Throwable cause, boolean retriable) {
        super(message, cause);
        this.retriable = retriable;
    }

    public boolean isRetriable() { return retriable; }
}
```

**设计原则：**

- `RaftNode` 不感知传输协议：依赖接口，不 import 任何 gRPC/protobuf 类
- 接口只用简单 DTO：protobuf 侵入被隔离在 `GrpcRaftTransport` 内部
- `start()` 替代 `listen()`：语义更清晰（启动服务端，准备接收请求）
- 移除 `RequestVote` 相关：K8s Lease 选主不需要投票 RPC

### 3.3 实现

| 实现 | 职责 |
|------|------|
| `GrpcRaftTransport` | 实现 `RaftTransport` 接口。内部使用 protobuf stubs，收到请求后 DTO ↔ protobuf 转换 |
| `InMemoryRaftTransport` | 单元测试用。直接传递 DTO 对象，无网络 |

### 3.4 删除

| 删除项 | 原因 |
|--------|------|
| `RaftTransport` 旧具体类 | 被接口替代 |
| `ReplicaClient` / `ReplicaServer` | 自研 TCP 传输，gRPC 全面替代 |
| `ReplicationRecord` / `Ack` | TCP 协议内部类型 |
| `RaftTransport` 中的 `encode*`/`decode*` 静态方法 | TCP 编解码，gRPC protobuf 替代 |
| proto 中的 `RequestVote` RPC 和消息 | K8s 模式不需要投票 |

---

## 4. instanceof 消除 — LeaderElection 接口统一

### 4.1 现状

`RaftNode` 有 4 处 `instanceof` 检查：

1. **构造器 line 107**：注册 RequestVote handler（仅 `RaftElection` 需要）
2. **构造器 line 137**：`RaftElection` 用 setter-style 回调，其他用 listener-style
3. **`onElectionStarted()` line 506**：guard K8s 模式不发 RequestVote
4. **`sendHeartbeats()` line 657**：`RaftElection.stepDown()` vs `K8sLeaseElection.forceStepDown()`

### 4.2 根因

`LeaderElection` 接口缺少 `stepDown(long newEpoch)` 方法，且 `RaftElection` 使用独有的 setter-style 回调（`setOnElectionStarted`）而非接口的 listener-style。

### 4.3 设计

```java
public interface LeaderElection {
    // 现有方法
    RaftRole role();
    boolean isLeader();
    long currentEpoch();
    String currentLeader();
    void start();
    void stop();
    void onAppendEntries(long leaderEpoch, String leaderId);
    void addBecomeLeaderListener(Consumer<Long> listener);
    void addBecomeFollowerListener(Consumer<Long> listener);

    // 新增：统一退位机制
    void stepDown(long newEpoch);
}
```

**各实现的 `stepDown` 行为：**

| 实现 | stepDown 行为 |
|------|-------------|
| `RaftElection` | 现有逻辑：设为 FOLLOWER，持久化 epoch+votedFor=null，重置选举定时器，触发 listener（仅 RaftElection 内部逻辑，K8s 部署不使用） |
| `K8sLeaseElection` | 现有 `forceStepDown` 逻辑：设为 FOLLOWER，持久化 epoch，触发 listener |
| `StandaloneElection` | no-op（单机永不退位） |

**4 处 instanceof 消除方式：**

| 位置 | 消除方式 |
|------|---------|
| 构造器 RequestVote handler | 删除（K8s 模式不需要，全面 gRPC 后无 RequestVote RPC） |
| 构造器生命周期回调 | 统一用 `addBecomeLeaderListener` / `addBecomeFollowerListener` |
| `onElectionStarted` guard | 删除（K8s 模式不会调用此方法） |
| `sendHeartbeats` step-down | 统一调用 `election.stepDown(result.epoch)` |

**`RaftElection` 回调统一：**

- `setOnBecomeLeader` / `setOnBecomeFollower` → 删除，改用接口的 `addBecomeLeaderListener` / `addBecomeFollowerListener`
- `setOnElectionStarted` → 保留为 `RaftElection` 内部方法。`RaftNode` 构造器中直接调用 `raftElection.setOnElectionStarted(this::onElectionStarted)`（构造器已知具体类型，不需要 instanceof）

---

## 5. K8s 时钟偏移防护 — Fencing Token 机制

### 5.1 现状问题

`K8sLeaseElection.isLeaseExpired()` 用 `OffsetDateTime.now()` 比较 `renewTime + leaseDurationSeconds`。如果 Pod A 时钟比 Pod B 快 15 秒，Pod A 会认为 Pod B 的 Lease 已过期并尝试抢占，导致双 Leader。

### 5.2 设计：三道纵深防御

| 层 | 机制 | 保护场景 |
|----|------|---------|
| 第 1 层 | `renewTime` fencing token | 已发生的主权变更 → 快速退位 |
| 第 2 层 | 安全缓冲（`clockSkewBufferSeconds`） | 判断别人 Lease 过期 → 缩小双主窗口 |
| 第 3 层 | epoch fencing（已有） | 最终安全网，旧 Leader 写入被拒绝 |

### 5.3 核心数据结构

```java
public class K8sLeaseElection implements LeaderElection {
    // Fencing token：最近一次观察到的 renewTime（微秒）
    private volatile long lastKnownRenewTimeMicros = 0;

    // 单调时钟：最近一次成功续约的本地纳秒时间（不受 NTP 调整影响）
    private volatile long lastRenewNanoTime = 0;

    // 可配置的安全缓冲（秒），默认 5
    private final long clockSkewBufferSeconds;
}
```

### 5.4 tryAcquireLease() 逻辑

```java
private void tryAcquireLease() {
    V1Lease existingLease = readLease();

    if (existingLease == null) {
        V1Lease created = createLease();
        becomeLeader(created);
        return;
    }

    String holder = existingLease.getSpec().getHolderIdentity();

    if (holder.equals(podName)) {
        // 自己持有 → 续约
        V1Lease renewed = renewLease(existingLease);
        updateFencingState(renewed);
        return;
    }

    // 别人持有 → fencing token + 安全缓冲判断是否可抢
    if (isLeaseExpired(existingLease)) {
        try {
            V1Lease acquired = acquireLease(existingLease);
            becomeLeader(acquired);
        } catch (ApiException e) {
            if (e.getCode() == 409) {
                // 乐观并发冲突，别人先抢到了
                becomeFollower();
            }
        }
    }
}

private void updateFencingState(V1Lease lease) {
    OffsetDateTime renewTime = lease.getSpec().getRenewTime();
    if (renewTime != null) {
        lastKnownRenewTimeMicros = renewTime.toInstant().toEpochMilli() * 1000;
    }
    lastRenewNanoTime = System.nanoTime();
}
```

### 5.5 isLeaseExpired() — 双重检查

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

**安全缓冲的作用**：将双主窗口从 `leaseDurationSeconds` 压缩到 `clockSkewBufferSeconds` 内。在缓冲期间，即使时钟偏移导致误判，epoch fencing 保证数据安全。

### 5.6 isLeader() — 单调时钟自检

```java
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

**关键**：自检用 `System.nanoTime()`（单调时钟），不受 NTP 调整、墙钟跳变影响。

> **注意**：`isLeader()` 自检不使用安全缓冲——自我检查应尽量保守，宁可在真正过期前不退位，也不能过早退位。如果加上缓冲，时钟偏移可能导致自己过早放弃领导权。

### 5.7 onAppendEntries — fencing token 退位

```java
public void onAppendEntries(long leaderEpoch, String leaderId) {
    if (leaderEpoch > currentEpoch) {
        stepDown(leaderEpoch);
    }
}
```

epoch 递增即退位，与 fencing token 配合形成快速响应。

### 5.8 对比表

| 场景 | 改进前 | 改进后 |
|------|--------|--------|
| 自己的 Lease 是否过期 | `OffsetDateTime.now()` 墙钟 | `System.nanoTime()` 单调时钟 |
| 别人的 Lease 是否过期 | 墙钟比较，无缓冲 | 墙钟比较 + `clockSkewBufferSeconds` 安全缓冲 |
| 主权变更退位 | 仅 epoch 检查 | epoch 检查 + renewTime fencing token |
| 最终安全网 | epoch fencing | epoch fencing（不变） |

### 5.9 配置

```yaml
loomq:
  kubernetes:
    lease:
      duration-seconds: 15
      renew-interval-seconds: 4
      clock-skew-buffer-seconds: 5   # 新增：安全缓冲
```

**约束**：`durationSeconds > renewIntervalSeconds + clockSkewBufferSeconds`

> **启动时校验**：`K8sLeaseElection` 构造器必须验证此约束，拒绝非法配置并抛出 `IllegalArgumentException`。

---

## 6. 变更文件清单

### 6.1 删除

| 文件/代码 | 原因 |
|-----------|------|
| `RaftTransport.java`（旧具体类） | 被接口替代 |
| 自研 TCP 代码（ReplicaClient/Server/ReplicationRecord/Ack） | gRPC 全面替代 |
| `RaftTransport` 中的 `encode*`/`decode*` 静态方法 | TCP 编解码 |
| proto 中 `RequestVote` RPC 和消息 | K8s 模式不需要 |
| `RaftNode` 中 4 处 `instanceof` 分支 | 统一接口消除 |
| `RaftElection.setOnBecomeLeader` / `setOnBecomeFollower` | 改用接口 listener |

### 6.2 新增

| 文件 | 职责 |
|------|------|
| `RaftTransport.java`（接口） | Transport 抽象 + DTO 定义 |
| `InMemoryRaftTransport.java` | 单元测试用 |

### 6.3 修改

| 文件 | 变更 |
|------|------|
| `GrpcRaftTransport.java` | 实现 `RaftTransport` 接口，DTO ↔ protobuf 转换 |
| `K8sLeaseElection.java` | fencing token + 单调时钟自检 + 安全缓冲 |
| `LeaderElection.java` | 新增 `stepDown(long newEpoch)` |
| `RaftElection.java` | 实现 `stepDown()`，统一回调为 listener-style |
| `StandaloneElection.java` | 实现 `stepDown()`（no-op） |
| `RaftNode.java` | 删除 instanceof，依赖接口 |
| `RaftNodeTest.java` | `ControlledRaftTransport` 改为实现接口 |
| `RaftTransportTest.java` | 适配新接口 |
| `RaftSoakTest.java` | 适配新接口 |
| `RaftWriteCoordinator.java` | 适配新接口（如有 transport 依赖） |
| `LogReplication.java` | 适配新接口（如有 transport 依赖） |

---

## 7. 测试策略

### 7.1 现有测试适配

- `RaftNodeTest.ControlledRaftTransport` 改为实现 `RaftTransport` 接口
- 所有使用 `RaftTransport` 具体类的测试改为依赖接口
- `InMemoryRaftTransport` 替代网络依赖

### 7.2 新增测试

| 测试 | 覆盖 |
|------|------|
| `K8sLeaseElection` fencing token 逻辑 | renewTime 变化触发退位 |
| `K8sLeaseElection` 单调时钟自检 | nanoTime 过期自动 stepDown |
| `K8sLeaseElection` 安全缓冲 | clockSkewBuffer 配置生效 |
| `LeaderElection.stepDown()` 统一行为 | 各实现 stepDown 的正确性 |
| `GrpcRaftTransport` 接口合规 | 实现 `RaftTransport` 接口所有方法 |
| `InMemoryRaftTransport` | 无网络单元测试基础设施 |

### 7.3 回归验证

- `loomq-raft` 全部现有测试通过
- `RaftSoakTest` 稳定性测试通过
- `K8sLeaseElectionTest` 通过

---

## 8. 风险与缓解

| 风险 | 影响 | 缓解 |
|------|------|------|
| 删除 TCP 传输后 gRPC 性能不如预期 | 复制延迟增加 | gRPC streaming + pipeline + 批量发送优化 |
| fencing token 依赖 renewTime 精度 | 微秒级时钟偏移可能导致误判 | 安全缓冲兜底 + epoch fencing 最终保护 |
| `RaftElection` 回调重构引入回归 | 选举行为变化 | 现有 `LeaderElectionTest` 全量通过 |
| `InMemoryTransport` 无法覆盖网络 edge case | 测试盲区 | `RaftSoakTest` + gRPC 集成测试覆盖 |
| gRPC `ManagedChannel` 生命周期管理 | 重连、空闲回收、关闭 | Phase 3 实现时处理 channel 生命周期策略 |
| fencing token 冷启动（`lastKnownRenewTimeMicros` = 0） | 首次判断可能不准确 | `isLeaseExpired()` 不依赖 fencing token（仅用墙钟+缓冲），冷启动无影响 |

---

## 9. 实施路径

### Phase 1：LeaderElection 接口统一
- `LeaderElection` 新增 `stepDown(long newEpoch)`
- `RaftElection` 实现 `stepDown()`，统一回调为 listener-style
- `K8sLeaseElection` 的 `forceStepDown` 重命名为 `stepDown`
- `StandaloneElection` 实现 `stepDown()`（no-op）
- **验证点：** 所有现有测试通过

### Phase 2：RaftNode instanceof 消除
- 删除 4 处 `instanceof` 分支
- 构造器统一用 listener-style 回调
- `sendHeartbeats` 统一调用 `election.stepDown()`
- **验证点：** `RaftNodeTest` 全部通过

### Phase 3：Transport 接口提取
- 从 `RaftTransport` 提取 `RaftTransport` 接口（含 DTO）
- 旧 `RaftTransport` 重命名为 `TcpRaftTransport`（临时保留，后续删除）
- `GrpcRaftTransport` 实现 `RaftTransport` 接口
- `InMemoryRaftTransport` 实现
- `RaftNode.transport` 字段类型改为接口
- **验证点：** `RaftNodeTest` + `GrpcRaftTransport` 基本连通

### Phase 4：删除 TCP 传输
- 删除 `TcpRaftTransport` 及相关 TCP 代码
- 删除 `ReplicaClient`/`ReplicaServer`/`ReplicationRecord`/`Ack`
- 删除 proto 中 `RequestVote` 定义
- **验证点：** 编译通过，所有测试通过

### Phase 5：K8s 时钟偏移防护
- `K8sLeaseElection` 新增 fencing token（`lastKnownRenewTimeMicros`）
- `isLeader()` 改用单调时钟自检
- `isLeaseExpired()` 增加安全缓冲
- 新增配置 `clock-skew-buffer-seconds`
- 新增单元测试
- **验证点：** `K8sLeaseElectionTest` 通过，时钟偏移场景测试覆盖

### Phase 6：清理与文档
- 更新设计文档（K8s Raft Replacement Design）
- 更新 CLAUDE.md（如有架构变更）
- Spotless 格式化
- **验证点：** `make check` 通过

---

## 10. 验收标准

- [ ] `GrpcRaftTransport` 可直接注入 `RaftNode`，无需类型转换
- [ ] `RaftNode` 中无任何 `instanceof` 检查选举类型
- [ ] `LeaderElection` 接口的 `stepDown()` 三种实现行为正确
- [ ] `K8sLeaseElection.isLeader()` 使用单调时钟自检
- [ ] `K8sLeaseElection.isLeaseExpired()` 包含安全缓冲
- [ ] 时钟偏移 10 秒（< leaseDuration - buffer）不会导致双 Leader
- [ ] `loomq-raft` 全部现有测试通过
- [ ] 新增 fencing token 和单调时钟的单元测试
- [ ] `make check`（格式化 + 测试）通过
