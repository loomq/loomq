# R25 执行 Plan（基于 `2026-08-15-r25-fix-spec.md`）

- 日期：2026-08-15
- 状态：已完成

## 任务

- [x] 创建失败测试：
  - `BugBackpressureIgnoresBorrowedTest`
  - `BugPendingScanTriggerNotClearedOnStartTest`
  - `BugTraceFailureNotRecordedOnResultPathTest`
- [x] 修复背压状态/指标使用 `tierInFlight`
- [x] `start()` 清空 `pendingScanTrigger`
- [x] 结果路径记录 trace failure
- [x] `LoomqEngine.start()` daemon 启动后失败禁止重试
- [x] 回归验证：新增测试 + 默认 fast-tests + spotless

## 验证命令

```bash
mvn -Dtest=BugBackpressureIgnoresBorrowedTest,BugPendingScanTriggerNotClearedOnStartTest,BugTraceFailureNotRecordedOnResultPathTest test
mvn test
mvn spotless:check
```
