# R26 执行 Plan（基于 `2026-08-15-r26-fix-spec.md`）

- 日期：2026-08-15
- 状态：已完成

## 任务

- [x] 新增 `BugEngineRetryUnsupportedAfterDaemonStartTest`
- [x] 修复 `LoomqEngine` 启动失败后 `close()` 仍可清理资源
- [x] 修复 `CohortManager` / `PromotionDaemon` flush 条件移除 mapping
- [x] 回归验证：新增测试 + 默认 fast-tests + spotless

## 验证命令

```bash
mvn -Dtest=BugEngineRetryUnsupportedAfterDaemonStartTest test
mvn test
mvn spotless:check
```
