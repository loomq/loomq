# R22 执行 Plan（基于 `2026-08-15-r22-fix-spec.md`）

- 日期：2026-08-15
- 状态：进行中 / 可执行
- 目标：按 R22 fix spec 完成 4 类修复，并用失败测试先行验证。

---

## 1. 执行目标

| 编号 | 目标 | 对应 spec 章节 |
|------|------|----------------|
| G1 | `updateIntent` 提交后失败不再回滚，内存/磁盘一致 | 3.1 |
| G2 | `updateIntent` 持久化前执行完整校验 | 3.2 |
| G3 | `triggerScan` 关闭竞态下 pending 标志收敛 | 3.3 |
| G4 | 调度器结算路径对 `IntentStore.update` 失败容错 | 3.4 |

---

## 2. 前置条件

- 基于当前 `dev-xuyuchen` 工作区。
- 已确认默认 fast-tests 通过、spotless 通过。
- 已存在 fix spec：`.claude/plans/2026-08-15-r22-fix-spec.md`。

---

## 3. 任务分解

### Task 1：创建失败测试（先红）

**文件**

- `loomq-core/src/test/java/com/loomq/BugUpdatePostCommitFailureTest.java`
- `loomq-core/src/test/java/com/loomq/BugUpdateIntentValidationTest.java`
- `loomq-core/src/test/java/com/loomq/BugSchedulerFinalizeStoreUpdateFailureTest.java`
- `loomq-core/src/test/java/com/loomq/application/scheduler/BugTriggerScanRejectedExecutionTest.java`

**验收标准**

- 运行：

```bash
mvn -Dtest=BugUpdatePostCommitFailureTest,BugTriggerScanRejectedExecutionTest,BugUpdateIntentValidationTest,BugSchedulerFinalizeStoreUpdateFailureTest test
```

- 修复前应失败：1 error + 3 failures + 1 failure，共 5 个用例未通过。

---

### Task 2：修复 `updateIntent` 提交后保护

**文件**

- `loomq-core/src/main/java/com/loomq/application/command/IntentCommandService.java`

**改动**

- 在 `catch (RuntimeException e)` 开头增加：

```java
if (persisted) {
    logger.warn("Update committed; ignoring post-commit failure: id={}", intentId, e);
    if (reschedule) {
        // 按新状态补做 scheduler.restore/schedule
    }
    return Optional.of(intent.copy());
}
```

- 保持 `persisted == false` 时原有回滚逻辑不变。

**验收标准**

- `BugUpdatePostCommitFailureTest` 通过。
- `BugUpdateUpdaterFailureTest` 等既有 updater 失败测试仍通过。

---

### Task 3：`updateIntent` 增加更新后校验

**文件**

- `loomq-core/src/main/java/com/loomq/application/command/IntentCommandService.java`

**改动**

- 在 `newExecuteAt` 应用后、`incrementRevision()` 前调用：

```java
IntentValidator.validate(intent);
```

**验收标准**

- `BugUpdateIntentValidationTest` 两个用例通过。
- 非法 deadline / redelivery 在持久化前被拒绝并走回滚重排。

---

### Task 4：修复 `triggerScan` 关闭竞态

**文件**

- `loomq-core/src/main/java/com/loomq/application/scheduler/PrecisionScheduler.java`

**改动**

- 在 `scanScheduler.submit(...)` 外层捕获 `RejectedExecutionException`：

```java
try {
    scanScheduler.submit(() -> { pending.set(false); scanAndDispatch(tier); });
} catch (RejectedExecutionException e) {
    pending.set(false);
    logger.warn("triggerScan rejected for tier {} during shutdown; resetting pending", tier);
}
```

**验收标准**

- `BugTriggerScanRejectedExecutionTest` 通过。
- 既有 `BugTriggerScanStuckFlagTest` 仍通过。

---

### Task 5：调度器 `IntentStore.update` best-effort

**文件**

- `loomq-core/src/main/java/com/loomq/application/scheduler/PrecisionScheduler.java`

**改动**

- 新增私有方法 `updateStoreBestEffort(Intent intent)`。
- 将调度器内全部 6 处 `intentStore.update(intent)` 替换为 `updateStoreBestEffort(intent)`。

**验收标准**

- `BugSchedulerFinalizeStoreUpdateFailureTest` 通过。
- 既有 `FinalizePersistFailureRegressionTest` 等持久化容错测试仍通过。

---

### Task 6：回归验证

**命令**

```bash
# 新增回归
mvn -Dtest=BugUpdatePostCommitFailureTest,BugTriggerScanRejectedExecutionTest,BugUpdateIntentValidationTest,BugSchedulerFinalizeStoreUpdateFailureTest test

# 默认 fast-tests
mvn test

# 格式
mvn spotless:check
```

**验收标准**

- 新增 5 个用例全部通过。
- 默认 fast-tests 全部通过。
- spotless 通过。

---

### Task 7：更新 spec / 自 review

**文件**

- `.claude/plans/2026-08-15-r22-fix-spec.md`

**改动**

- 已补充：
  - `reschedule == true` 时才补做调度；
  - 验证范围说明（未跑 full-tests）；
  - 已知覆盖缺口；
  - 实现与 spec 的小偏差。

**验收标准**

- spec 与最终实现一致。
- 自 review 记录已写入 spec 第 7 节。

---

## 4. 风险与控制

- `updateIntent` 返回语义变化：提交后失败从“抛异常”变为“返回成功”，需与 cancel/fireNow 保持一致；相关文档/调用方需知晓。
- `updateStoreBestEffort` 复用 `persistFailures` 计数器，可能造成指标语义混杂；当前接受，后续可拆独立计数器。
- `BugTriggerScanRejectedExecutionTest` 使用反射，属于白盒测试；字段重构时需同步更新。

---

## 5. 完成定义（DoD）

- [x] 所有新增失败测试在修复前确实失败（已确认）。
- [x] 所有新增测试在修复后通过。
- [x] 默认 fast-tests 通过。
- [x] spotless 通过。
- [x] fix spec 与执行 plan 已同步。
