# 设计文档：isLeader() 副作用修复

> **状态：** 设计完成，待实现
> **日期：** 2026-06-03
> **分支：** dev-xuyuchen

---

## 1. 问题

`K8sLeaseElection.isLeader()` 在单调时钟过期时调用 `stepDown()`，使查询方法有副作用。`RaftNode.isLeader()` 调用 `election.isLeader()`，所有内部路径（propose、sendHeartbeats、gracefulShutdown）都可能意外触发领导权丢失。

## 2. 设计

将 lease 过期检查从 `isLeader()` 移到 `tryAcquireLease()` 定期任务中。

**isLeader() 变为纯查询**：
```java
public boolean isLeader() {
    return role == RaftRole.LEADER;
}
```

**tryAcquireLease() 增加过期检查**：
```java
private void tryAcquireLease() {
    try {
        // 定期检查 lease 过期
        if (role == RaftRole.LEADER && isLeaseExpiredMonotonic()) {
            log.warn("Lease expired (monotonic clock), stepping down");
            stepDown(currentEpoch);
        }
        // ... 原有逻辑
    } catch (...) { ... }
}
```

**窗口分析**：isLeader() 不再检查单调时钟，lease 过期到下次 tryAcquireLease() 之间有最长 `renewIntervalSeconds`（默认 4 秒）的窗口。此窗口内 epoch fencing 保证旧 leader 的写入被 follower 拒绝。

## 3. 验收标准

- [ ] `isLeader()` 无副作用
- [ ] lease 过期在 `tryAcquireLease()` 中检查
- [ ] 现有测试全部通过
