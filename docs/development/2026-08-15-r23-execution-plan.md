# R23 执行 Plan（基于 `2026-08-15-r23-fix-spec.md`）

- 日期：2026-08-15
- 状态：已完成

## 任务

- [x] 创建失败测试：
  - `BugStoreCountDriftAfterPostCommitFailureTest`
  - `BugGroupCommitBarrierCloseBeforeStartTest`
  - `BugIntentTraceNullTierTest`
- [x] 修复 `updateIntent` / `updateStoreBestEffort` 提交后计数补偿
- [x] 修复 `GroupCommitBarrier.close()` 资源释放
- [x] 修复 `IntentTrace.toJson()` null tier
- [x] 修复 `IntentTraceStore` eviction queue 动态容量
- [x] 修复 `CohortManager` / `PromotionDaemon` 注册幂等
- [x] 接线 `IntentTraceStore.recordFailure`
- [x] 修正 `LoomqEngine.start()` 重试注释
- [x] 回归验证：新增测试 + 默认 fast-tests + spotless

## 验证命令

```bash
mvn -Dtest=BugStoreCountDriftAfterPostCommitFailureTest,BugGroupCommitBarrierCloseBeforeStartTest,BugIntentTraceNullTierTest test
mvn test
mvn spotless:check
```
