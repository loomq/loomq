# R27 执行 Plan（基于 `2026-08-15-r27-fix-spec.md`）

- 日期：2026-08-15
- 状态：已完成

## 任务

- [x] 新增 `BugRejectedCommandMustNotNormalizeTierTest`
- [x] `createIntent` 归一化移到重复检查后
- [x] `updateIntent` 归一化移到校验后
- [x] `retryUnsupported` 改为 volatile
- [x] 修正 `LoomqEngine` catch 注释
- [x] 回归验证：新增测试 + 默认 fast-tests + spotless

## 验证命令

```bash
mvn -Dtest=BugRejectedCommandMustNotNormalizeTierTest test
mvn test
mvn spotless:check
```
