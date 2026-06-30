# 设计文档：InstallSnapshot 分块下沉到 Transport 层

> **状态：** 设计完成，待实现
> **日期：** 2026-06-03
> **作者：** xuyuchen
> **分支：** dev-xuyuchen

---

## 1. 背景与动机

当前 `sendInstallSnapshot` 每个 chunk 创建独立 gRPC stream（100MB snapshot = 400 streams）。proto 已定义为客户端流式 RPC（`stream SnapshotChunk → SnapshotResponse`），但实现未利用。

同时 `RaftNode` 中维护了 `handleInstallSnapshotChunk` + `pendingChunks`（`ConcurrentHashMap<Key, ChunkReassembly>`）用于接收端重组，该缓冲区无 TTL/大小限制，存在内存泄漏风险。

**决策：** 将分块/重组逻辑从 `RaftNode` 下沉到 `GrpcRaftTransport`，上层只处理完整快照。

---

## 2. 设计

### 2.1 接口约定变更

`RaftTransport.sendInstallSnapshot` 签名不变。约定：

- `chunkIndex=0, totalChunks=1`：表示完整快照，Transport 自行分块
- `data` 字段携带完整快照数据（不再拆分）

### 2.2 GrpcRaftTransport 发送端

```
sendInstallSnapshot(peerId, InstallSnapshotRequest(epoch, leaderId, idx, epoch, 0, 1, 完整data))
    ├── 拆分 data 为 N 个 chunks（256KB/chunk）
    ├── 开 1 个 client-streaming gRPC stream
    ├── 按序发送 N 个 SnapshotChunk
    ├── onCompleted
    └── 等待 SnapshotResponse
```

- 1 个 stream 携带所有 chunks，无论快照多大
- deadline 按数据量动态调整：`baseTimeout + chunks * perChunkTimeout`

### 2.3 GrpcRaftTransport 接收端（RaftServiceImpl）

```
StreamObserver<SnapshotChunk> installSnapshot(responseObserver)
    onNext(chunk):
        缓存 chunk 到 byte[][] 数组（按 chunkIndex 存放）
    onCompleted():
        重组所有 chunks 为完整 data
        构造 DTO(完整 data, totalChunks=1)
        调用 installSnapshotHandler
        返回 SnapshotResponse
```

- 同一 stream 内消息顺序有保证，无需并发安全
- 重组在 `onCompleted` 中一次性完成

### 2.4 RaftNode 简化

**删除：**
- `handleInstallSnapshotChunk` 方法
- `pendingChunks` 字段（`ConcurrentHashMap`）
- `ChunkReassembly` 内部类

**修改：**
- `setOnInstallSnapshot` handler 只接收完整快照（`totalChunks=1`），直接调用 `handleInstallSnapshot`
- `sendInstallSnapshot` 不再拆分 chunk，直接传完整 data 给 transport

### 2.5 删除的代码

| 删除项 | 原因 |
|--------|------|
| `RaftNode.handleInstallSnapshotChunk` | gRPC 层透明处理 |
| `RaftNode.pendingChunks` | 不再需要接收端缓冲 |
| `RaftNode.ChunkReassembly` 内部类 | 不再需要 |
| `RaftNode.sendInstallSnapshot` 中的 chunk 拆分循环 | Transport 层处理 |
| `GrpcRaftTransport` 中每 chunk 开新 stream 的逻辑 | 改为单 stream |

---

## 3. 错误处理

| 场景 | 处理 |
|------|------|
| stream 中途失败（网络断开） | gRPC 自动关闭 stream，`onError` 触发，返回 `bytesReceived=-1` |
| chunk 缺失（`onCompleted` 时 `receivedCount != totalChunks`） | 返回 `Status.INTERNAL` 错误 |
| handler 异常 | `responseObserver.onError(e)` 传播给客户端 |
| stream 超时 | deadline 按数据量动态调整，避免大快照超时 |

---

## 4. 测试策略

- `RaftNodeTest`：现有 snapshot 测试通过（接口不变）
- `RaftSoakTest`：snapshot round-trip 测试通过
- 新增：大 snapshot（>256KB）分块传输集成测试
- 新增：stream 中途失败恢复测试

---

## 5. 验收标准

- [ ] 100MB snapshot 只创建 1 个 gRPC stream
- [ ] `RaftNode` 中无 `pendingChunks`、`ChunkReassembly`、`handleInstallSnapshotChunk`
- [ ] 现有测试全部通过
- [ ] 大 snapshot 分块传输正确
