# 设计文档：K8s Lease 选主 + Epoch 日志复制

> **状态：** 设计完成，待实现
> **日期：** 2026-06-01
> **作者：** xuyuchen
> **分支：** dev-xuyuchen

---

## 1. 背景与动机

LoomQ 当前使用自研 Raft 实现（`loomq-raft` 模块）提供分布式一致性。经过生产实践，发现两个核心问题：

1. **维护成本高**：自研 Raft 涉及选举、Term 管理、心跳、日志冲突检测等复杂逻辑，bug 风险大
2. **可用性不够高**：自研实现的 edge case 处理不如成熟方案完善

**决策：** 拆解 Raft，用 Kubernetes Lease API 替代选主，保留日志复制核心机制。

**核心价值：** 用运维复杂度换代码复杂度——K8s 已经是用户的基础设施，不是 LoomQ 额外引入的依赖。

---

## 2. 设计目标与架构取舍

| 目标 | 描述 | 架构取舍 |
|------|------|---------|
| 降低维护成本 | 删除自研选主代码（~380 行），用 K8s Lease 替代 | 分布式能力与 K8s 深度绑定，失去独立部署灵活性 |
| 提高可用性 | K8s Lease 经过大规模验证，edge case 处理成熟 | 依赖 K8s API Server 高可用，API Server 故障会影响选主 |
| 保留数据可靠性 | 核心日志复制机制（AppendEntries + InstallSnapshot）保留 | 保留自研复制代码，仍需维护 |
| 异步复制优先 | 默认异步复制（写 WAL 即返回），同步模式可选 | 异步模式下 Leader 崩溃可能丢失最后一批未复制的日志 |
| 部署环境 | **仅限 Kubernetes**，非 K8s 环境使用单机模式 | 非 K8s 环境无法使用分布式能力 |
| 通信协议 | gRPC 替代自定义 Netty TCP | 生态好，但性能略低于自定义 TCP |

---

## 3. 架构总览

```
┌─────────────────────────────────────────────────────────────┐
│                    Kubernetes Cluster                        │
│                                                             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │   Pod A      │  │   Pod B      │  │   Pod C      │      │
│  │  (Leader)    │  │ (Follower)   │  │ (Follower)   │      │
│  │              │  │              │  │              │      │
│  │ ┌──────────┐ │  │ ┌──────────┐ │  │ ┌──────────┐ │      │
│  │ │K8s Lease │ │  │ │K8s Lease │ │  │ │K8s Lease │ │      │
│  │ │ Election │ │  │ │ Watcher  │ │  │ │ Watcher  │ │      │
│  │ └──────────┘ │  │ └──────────┘ │  │ └──────────┘ │      │
│  │      │       │  │      │       │  │      │       │      │
│  │      ▼       │  │      ▼       │  │      ▼       │      │
│  │ ┌──────────┐ │  │ ┌──────────┐ │  │ ┌──────────┐ │      │
│  │ │  Epoch   │ │  │ │  Epoch   │ │  │ │  Epoch   │ │      │
│  │ │ Manager  │ │  │ │ Manager  │ │  │ │ Manager  │ │      │
│  │ └──────────┘ │  │ └──────────┘ │  │ └──────────┘ │      │
│  │      │       │  │      │       │  │      │       │      │
│  │      ▼       │  │      ▼       │  │      ▼       │      │
│  │ ┌──────────┐ │  │ ┌──────────┐ │  │ ┌──────────┐ │      │
│  │ │   Log    │ │  │ │   Log    │ │  │ │   Log    │ │      │
│  │ │Replicat. │◄├──►│ │Replicat. │◄├──►│ │Replicat. │ │      │
│  │ └──────────┘ │  │ └──────────┘ │  │ └──────────┘ │      │
│  │      │       │  │      │       │  │      │       │      │
│  │      ▼       │  │      ▼       │  │      ▼       │      │
│  │ ┌──────────┐ │  │ ┌──────────┐ │  │ ┌──────────┐ │      │
│  │ │ Intent   │ │  │ │ Intent   │ │  │ │ Intent   │ │      │
│  │ │  Store   │ │  │ │  Store   │ │  │ │  Store   │ │      │
│  │ └──────────┘ │  │ └──────────┘ │  │ └──────────┘ │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
│                                                             │
│  ┌──────────────────────────────────────────────────────┐  │
│  │                  Lease Resource                       │  │
│  │  holder: pod-a                                       │  │
│  │  leaseTransitions: 3  ←── 这就是 epoch               │  │
│  │  acquireTime: 2026-06-01T10:00:00Z                   │  │
│  │  renewTime: 2026-06-01T10:00:05Z                     │  │
│  └──────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

---

## 4. 组件变更清单

### 4.1 删除的组件

| 文件 | 原因 |
|------|------|
| `LeaderElection.java` | 选举逻辑交给 K8s Lease |
| `RaftTransport.java` 中的 RequestVote RPC | 不再需要投票机制 |
| `RaftNode.java` 中的选举相关代码 | 选举回调、term 管理 |

### 4.2 保留并适配的组件

| 文件 | 变更 |
|------|------|
| `LogReplication.java` | `term` → `epoch`，移除选举依赖 |
| `RaftLog.java` | 不变 |
| `RaftWriteCoordinator.java` | `isLeader()` 改为检查 K8s Lease holder + 有效期 |
| `RaftTransport.java` | 改用 gRPC，移除 RequestVote，保留 AppendEntries + InstallSnapshot |

### 4.3 新增的组件

| 文件 | 职责 |
|------|------|
| `K8sLeaseElection.java` | 封装 K8s Lease API，提供 `isLeader()`、`getEpoch()`、`getCurrentLeader()` |
| `LeaderElection.java` (接口) | 抽象选主接口，支持 K8s 和单机模式 |
| `StandaloneElection.java` | 单机模式 no-op 实现，固定返回 Leader |
| gRPC proto 定义 | AppendEntries、InstallSnapshot 的 gRPC 服务定义 |

---

## 5. 运行时行为

### 5.1 Leader 选举流程

```
Pod A 启动
    │
    ▼
尝试创建/更新 Lease
    │
    ├─ 成功 → 成为 Leader，epoch = leaseTransitions
    │
    └─ 失败（已有 holder）→ 成为 Follower，watch Lease
                              │
                              ▼
                    Lease holder 变更？
                        │
                        ├─ 是 → epoch++，尝试抢 Lease
                        │
                        └─ 否 → 继续 watch
```

### 5.2 Follower 转正流程

```
1. K8s Lease 过期，触发新选主
2. Follower 获取 Lease，epoch 递增
3. Follower 变为新 Leader，将 commitIndex 推进到本地 RaftLog 的末尾
4. 新 Leader 开始接收写入请求
5. 新 Leader 向其他 Follower 发送 AppendEntries（携带新 epoch 和新 commitIndex）
   → Follower 发现 epoch 变更，接受新 Leader
   → 可能截断旧 epoch 未提交的日志
```

> **注意：** 步骤 3 中，Follower 本地日志的最后一条 index 可能超过旧 Leader 的 commitIndex（如果 Follower 收到了 AppendEntries 但旧 Leader 未来得及提交）。新 Leader 将 commitIndex 推进到本地末尾，意味着这些日志被"隐式提交"——只要新 Leader 拥有这些日志，它们就不会丢失（因为新 Leader 的日志至少和多数节点一样新，否则无法获取 Lease）。

### 5.3 写入流程（Leader）

```
客户端请求 → IntentHandler
    │
    ▼
RaftWriteCoordinator.submitWrite()
    │
    ├─ 检查 isLeader()（K8s Lease holder + 有效期）
    │
    ├─ 检查 epoch（fencing token）
    │
    ├─ propose 到本地 RaftLog
    │
    ├─ 异步模式（默认）：立即返回成功，后台异步复制
    │
    ├─ 同步模式：等待多数确认后返回
    │
    └─ 返回结果
```

**Leader 主动关机时的处理：**

收到 SIGTERM 信号后，Leader 执行以下步骤：

1. 立即停止接收新的写入请求（`isLeader()` 返回 false 或 HTTP 层拒绝新请求）
2. 等待后台异步复制任务完成（最长 `shutdown-timeout-seconds` 秒）
3. 等待期间，Leader 持续将积压的日志复制到 Followers 并推进 commitIndex
4. 超时后，Leader 强制关闭

这可以最大程度减少因主动关机导致的数据丢失。关键约束：`shutdown-timeout-seconds` 必须小于 K8s 的 `terminationGracePeriodSeconds`，否则 SIGKILL 会在 flush 完成前强杀进程。

### 5.4 读取流程

```
客户端请求 → IntentHandler
    │
    ├─ 强一致读 → 只从 Leader 读（在 lease 有效期内）
    │
    └─ 最终一致读 → 从任意节点读
```

---

## 6. 关键设计细节

### 6.1 Epoch 管理

```java
public interface LeaderElection {
    boolean isLeader();
    long currentEpoch();           // 对应 K8s Lease.leaseTransitions
    String currentLeader();        // 对应 K8s Lease.holderIdentity
    void addLeadershipListener(LeadershipListener listener);
}

public class K8sLeaseElection implements LeaderElection {
    private final String podName;
    private final String leaseName;
    private final String namespace;
    private final KubernetesClient client;
    private volatile long currentEpoch = 0;
    private volatile boolean isLeader = false;

    // 通过 watch Lease 资源感知 leadership 变更
    // 当 holder 变更为自己时，epoch = leaseTransitions
}
```

### 6.2 单机模式（StandaloneElection）

非 K8s 环境下，使用 no-op 实现固定返回 Leader：

```java
public class StandaloneElection implements LeaderElection {
    @Override
    public boolean isLeader() { return true; }

    @Override
    public long currentEpoch() { return 1L; }

    @Override
    public String currentLeader() { return "standalone"; }

    @Override
    public void addLeadershipListener(LeadershipListener listener) {
        // 单机模式下立即通知成为 Leader
        listener.onBecomeLeader(1L);
    }
}
```

### 6.3 Epoch 持久化

epoch 必须持久化——节点重启后如果忘记了自己的 epoch，可能接受旧 Leader 的过期日志。

```java
// 在 K8sLeaseElection 初始化时
long persistedEpoch = walAccessor.readEpoch();  // 从 WAL meta 恢复
if (persistedEpoch > 0) {
    this.currentEpoch = persistedEpoch;
}

// 每次 epoch 变更时持久化
walAccessor.writeEpoch(newEpoch);
```

复用现有 WAL 的 `raft_meta` 文件，将 `term` 替换为 `epoch`。

### 6.4 isLeader() 检查

```java
public boolean isLeader() {
    // 不仅检查 Lease holder，还要检查 Lease 是否仍然有效
    return leaseHolder.equals(podName) && lease.isValid();
}

// Lease 有效性检查
private boolean isLeaseValid() {
    if (renewTime == null) return false;
    long elapsedMs = System.currentTimeMillis() - renewTime.toEpochMilli();
    return elapsedMs < leaseDurationSeconds * 1000L;
}
```

### 6.5 日志条目格式变更

```
旧格式: [8 bytes term][payload]
新格式: [8 bytes epoch][payload]
```

### 6.6 AppendEntries 协议变更

```
旧: AppendEntries(term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit)
新: AppendEntries(epoch, leaderId, prevLogIndex, prevLogEpoch, entries, leaderCommit)
```

### 6.7 Fencing 机制

```java
// Follower 收到 AppendEntries 时
if (msg.epoch < currentEpoch) {
    // 拒绝：这是旧 Leader 的请求
    return AppendEntriesResult.fail(currentEpoch);
}

// Leader 发送 AppendEntries 时
if (!isLeader()) {
    // 已经不是 Leader 了，停止发送
    return;
}
```

### 6.8 复制模式

```java
public enum ReplicationMode {
    ASYNC,   // Leader 写 WAL 即返回，后台异步复制（默认）
    SYNC     // 等待多数确认后返回
}

// RaftWriteCoordinator 中
if (replicationMode == ReplicationMode.ASYNC) {
    // 写入本地 WAL 后立即返回
    long index = raftLog.appendEntry(epoch, payload);
    triggerAsyncReplication();  // 后台异步复制
    return WriteResult.success(index);
} else {
    // 同步模式：等待多数确认
    long index = raftLog.appendEntry(epoch, payload);
    awaitQuorumAck(index, timeoutMs);
    return WriteResult.success(index);
}
```

### 6.9 gRPC InstallSnapshot 分块传输

```protobuf
service RaftService {
    rpc AppendEntries(AppendEntriesRequest) returns (AppendEntriesResponse);
    rpc InstallSnapshot(stream SnapshotChunk) returns (SnapshotResponse);
}

message SnapshotChunk {
    int64 epoch = 1;
    string leader_id = 2;
    int64 last_included_index = 3;
    int64 last_included_epoch = 4;
    int32 chunk_index = 5;
    int32 total_chunks = 6;
    bytes data = 7;
}
```

---

## 7. 配置

### 7.1 K8s Lease 配置

配置路径独立为 `loomq.kubernetes.lease`，与 Core 配置解耦：

```yaml
loomq:
  kubernetes:
    lease:
      duration-seconds: 10      # 过期时间
      renew-interval-seconds: 3  # 每 3 秒续约一次
      namespace: default         # Lease 所在 namespace
      lease-name: loomq-leader   # Lease 资源名称
```

### 7.2 复制模式配置

```yaml
loomq:
  replication:
    mode: ASYNC  # ASYNC（默认）| SYNC
    shutdown-timeout-seconds: 25  # 等待异步复制完成的最大时间，必须小于 terminationGracePeriodSeconds
```

### 7.3 单机模式配置

```yaml
loomq:
  mode: standalone  # standalone | distributed
```

---

## 8. 部署

### 8.1 K8s StatefulSet 部署示例

```yaml
apiVersion: v1
kind: Service
metadata:
  name: loomq-headless
spec:
  clusterIP: None
  selector:
    app: loomq
  ports:
  - port: 9090
    name: grpc
  - port: 8080
    name: http
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: loomq
spec:
  serviceName: loomq-headless
  replicas: 3
  selector:
    matchLabels:
      app: loomq
  template:
    metadata:
      labels:
        app: loomq
    spec:
      serviceAccountName: loomq-sa
      terminationGracePeriodSeconds: 30  # 必须大于 loomq.replication.shutdown-timeout-seconds
      containers:
      - name: loomq
        image: loomq:latest
        env:
        - name: POD_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        - name: LOOMQ_MODE
          value: "distributed"
        ports:
        - containerPort: 9090
          name: grpc
        - containerPort: 8080
          name: http
        volumeMounts:
        - name: loomq-data
          mountPath: /data/loomq
  volumeClaimTemplates:
  - metadata:
      name: loomq-data
    spec:
      accessModes: ["ReadWriteOnce"]
      resources:
        requests:
          storage: 10Gi
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: loomq-sa
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: loomq-lease-role
rules:
- apiGroups: ["coordination.k8s.io"]
  resources: ["leases"]
  verbs: ["get", "list", "watch", "create", "update", "patch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: loomq-lease-binding
subjects:
- kind: ServiceAccount
  name: loomq-sa
roleRef:
  kind: Role
  name: loomq-lease-role
  apiGroup: rbac.authorization.k8s.io
```

### 8.2 单机模式部署

非 K8s 环境下，以单机模式运行：

```bash
java -jar loomq-server.jar --loomq.mode=standalone
```

单机模式下：
- `LeaderElection` 接口使用 `StandaloneElection`（no-op，固定返回 Leader）
- 无日志复制，仅使用本地 RocksDB WAL
- 所有功能正常，但无高可用

---

## 9. 兼容性与迁移

### 9.1 不兼容变更

| 变更项 | 旧格式 | 新格式 | 影响 |
|--------|--------|--------|------|
| WAL 日志条目 | `[8 bytes term][payload]` | `[8 bytes epoch][payload]` | 旧 WAL 文件无法直接使用 |
| 通信协议 | 自定义 Netty TCP | gRPC | 与旧版本节点无法通信 |
| 选举机制 | 自研 Raft 选举 | K8s Lease | 旧版本无法与新版本混合部署 |

### 9.2 升级流程

**不支持原地滚动升级**，需遵循以下流程：

1. **停机**：停止所有旧版本节点
2. **数据清理**：删除旧版本的 WAL 文件（保留 RocksDB 数据）
3. **重新部署**：使用新版本镜像部署 StatefulSet
4. **数据恢复**：新版本启动后，从 RocksDB 重建 WAL

**注意**：如果 RocksDB 数据也需要迁移（格式变更），需要额外的数据迁移步骤。

### 9.3 回滚方案

如需回滚到旧版本：

1. 停止所有新版本节点
2. 删除新版本的 WAL 文件
3. 使用旧版本镜像重新部署
4. 从 RocksDB 数据恢复

---

## 10. 风险分析

| 风险 | 影响 | 缓解措施 |
|------|------|---------|
| K8s Lease 超时时间长（默认 10-30s） | 故障切换慢 | 设置较短的 leaseDuration（10s），renewInterval（3s） |
| 网络分区时可能双 Leader | 数据不一致 | 使用 epoch 做 fencing，旧 Leader 的写入会被拒绝 |
| K8s API Server 不可用 | 无法选主 | 这是 K8s 的固有限制，需要保证 API Server 高可用 |
| gRPC 性能低于自定义 TCP | 复制延迟增加 | 使用 streaming、pipeline、批量发送优化 |
| 旧 Leader GC 停顿后自以为是 | 写入过期日志 | isLeader() 同时检查 Lease 有效期，Follower 端 epoch fencing |
| K8s 强制终止 Leader Pod（SIGKILL） | 异步复制日志丢失 | 优雅关机：SIGTERM 后等待异步复制完成（最长 25s），`shutdown-timeout` < `terminationGracePeriodSeconds` |

---

## 11. 代码量预估

| 变更类型 | 预估代码量 |
|---------|-----------|
| 删除 `LeaderElection.java` | -180 行 |
| 删除选举相关代码 | -200 行 |
| 新增 `K8sLeaseElection.java` | +150 行 |
| 新增 `StandaloneElection.java` | +30 行 |
| 新增 gRPC 定义 + 实现 | +300 行 |
| 适配 `LogReplication.java` | ~50 行变更 |
| 适配 `RaftNode.java` | ~100 行变更 |

**净变化：** 删除约 380 行，新增约 630 行，但核心复杂度从"自研选举 + 自研复制"降为"K8s Lease + 自研复制"。

---

## 12. 验收标准

### 12.1 Leader 选举

- [ ] Leader Pod 宕机后，10 秒内有新 Pod 成为 Leader 并开始接收写入
- [ ] 网络分区场景下，不会出现两个 Leader 同时提交数据（epoch fencing 保证）
- [ ] Leader GC 停顿 20 秒后恢复，不会继续提交数据（Lease 过期检查）

### 12.2 数据可靠性

- [ ] 同步复制模式下，Leader 宕机后所有已确认写入的数据零丢失
- [ ] 同步复制模式下，Leader 连续宕机 3 次后，已提交的数据仍然一致且不丢失（跨多次 Leader 切换的一致性）
- [ ] 异步复制模式下，Leader 宕机后未复制的数据丢失量在可接受范围内（文档明确说明）
- [ ] Follower 落后太多时，自动触发 InstallSnapshot 并成功同步
- [ ] Leader 主动关机（SIGTERM）时，在 25 秒内完成积压日志的异步复制

### 12.3 基本功能

- [ ] `loomq-core` 模块所有现有单元测试通过
- [ ] `loomq-raft` 模块所有现有单元测试通过（适配后）
- [ ] 单机模式（StandaloneElection）下所有功能正常

### 12.4 代码质量

- [ ] 删除选举相关代码后，`loomq-raft` 模块代码量减少 25% 以上
- [ ] 新增代码通过 Spotless 格式检查
- [ ] 新增代码有对应的单元测试

### 12.5 部署验证

- [ ] K8s StatefulSet 部署示例可正常运行
- [ ] 单机模式 JAR 可正常启动并提供服务
- [ ] Lease 配置可通过 `application.yml` 正确加载

---

## 13. 实施路径

### 实施原则

**原则 1：先通过测试，再删代码。** 在删除任何代码之前，先定义 `LeaderElection` 接口，并把现有的选举逻辑包装成 `RaftElection implements LeaderElection`。这样可以确保所有现有测试仍然通过，后续的每一步改动都有安全的回退基线。

**原则 2：先验证基础设施，再写业务代码。** 在写 `K8sLeaseElection` 的具体实现之前，先搭建一个简单的集成测试（可以用 microk8s 或 kind），确保 LoomQ 能成功调用 K8s Lease API。这一步单独验证可以避免大量代码写完后才发现基础设施问题。

### Phase 1：接口抽象
- 定义 `LeaderElection` 接口
- 实现 `StandaloneElection`（单机模式）
- 将现有 `LeaderElection` 包装为 `RaftElection implements LeaderElection`（保持现有行为不变）
- 将 `RaftNode`、`LogReplication`、`RaftWriteCoordinator` 中的选举依赖改为接口依赖
- **验证点：** 所有现有单元测试通过

### Phase 2：Epoch 替代 Term
- 将所有 `term` 引用替换为 `epoch`
- 修改 WAL meta 持久化格式
- 修改 AppendEntries 协议格式

### Phase 3：K8s Lease 实现
- 搭建 K8s 集成测试环境（microk8s 或 kind）
- 实现 `K8sLeaseElection`
- 集成 Kubernetes Java Client
- 实现 Lease watch 和 leadership 监听
- 实现 epoch 持久化
- **验证点：** K8s Lease API 调用成功，Leader 选举和 failover 正常工作

### Phase 4：gRPC 迁移
- 定义 gRPC proto 文件
- 实现 gRPC transport
- 保留分块 InstallSnapshot 逻辑

### Phase 5：复制模式
- 实现异步/同步复制模式切换
- 配置化复制模式选择

### Phase 6：清理
- 删除 `LeaderElection.java`（旧实现）
- 删除 RequestVote 相关代码
- 更新测试
- 更新文档

---

## 14. 总结

这个设计砍掉了分布式系统中最容易出 Bug 的部分（选主、Term 管理、心跳），用 K8s 的成熟原语替代，同时保留了日志复制的核心可靠性。

**最大的架构价值：** 用运维复杂度换代码复杂度——而运维复杂度由 K8s 承担。K8s 已经是用户的基础设施，不是 LoomQ 额外引入的依赖。

**明确的边界：** 分布式能力仅限 K8s 环境，非 K8s 环境使用单机模式。这是一个有意识的架构取舍，换取更简洁的代码和更高的可用性。
