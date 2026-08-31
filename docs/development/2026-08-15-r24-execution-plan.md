# R24 执行 Plan（基于 `2026-08-15-r24-fix-spec.md`）

- 日期：2026-08-15
- 状态：已完成

## 任务

- [x] 创建失败测试：
  - `BugNullPrecisionTierNormalizationTest`
  - `BugFactoryNullPropertiesTest`
  - `BugReadOnlyViewCloseTest`
- [x] 修复 `IntentCommandService` null `precisionTier` 归一化
- [x] 修复 `IntentCommandService` 构造器 `precisionTierCatalog` 可能为 null
- [x] 修复 `createIntents` 回滚恢复 `precisionTier`
- [x] 修复 `LoomqEngineFactory` null Properties
- [x] 修复 `ReadOnlyIntentStoreView.close()` no-op
- [x] 修正 `IntentTraceStore` FIFO 注释
- [x] 回归验证：新增测试 + 默认 fast-tests + spotless

## 验证命令

```bash
mvn -Dtest=BugNullPrecisionTierNormalizationTest,BugFactoryNullPropertiesTest,BugReadOnlyViewCloseTest,BatchCreateRollbackUpdatedAtTest test
mvn test
mvn spotless:check
```
