# LoomQ Core Model

LoomQ is a durable time kernel for distributed systems.

It focuses on one thing: making future events happen reliably, with persistence, recovery, retry orchestration, and deadline handling built in.

## Glossary

| Term | Meaning in LoomQ |
|------|------------------|
| `Intent` | The durable scheduling record. This is the public model used by the API and the engine. |
| `Legacy term` | A legacy/public-facing synonym in older docs. Prefer `Intent` for new material. |
| `Job` | A single execution attempt or delivery action derived from an `Intent`. |
| `Deadline` | The latest acceptable completion time for an `Intent`. |
| `Schedule` | The initial placement of an `Intent` into the time kernel. |
| `Reschedule` | Updating the execution time of an existing `Intent`. |
| `Expire` | Transitioning an `Intent` past its allowed time window. |
| `Recover` | Rebuilding pending work after restart by scanning the persistent timing wheel (PHTW). |
| `Retry` | Re-queuing an `Intent` after a failed delivery attempt. |

## Model Shape

```mermaid
flowchart LR
    API["Intent API"] --> Intent["Durable Intent"]
    Intent --> Schedule["schedule"]
    Intent --> Reschedule["reschedule"]
    Intent --> Expire["expire"]
    Intent --> Recover["recover"]
    Intent --> Retry["retry"]
    Intent --> Deadline["deadline guard"]

    Schedule --> Scheduler["PrecisionScheduler"]
    Reschedule --> Scheduler
    Expire --> Scheduler
    Recover --> Recovery["WheelRecovery"]
    Retry --> Delivery["DeliveryHandler / RedeliveryDecider"]
    Deadline --> Delivery
```

## State Machine

```mermaid
stateDiagram-v2
    [*] --> CREATED
    CREATED --> SCHEDULED
    SCHEDULED --> DUE
    SCHEDULED --> CANCELED
    DUE --> DISPATCHING
    DUE --> CANCELED
    DISPATCHING --> DELIVERED
    DISPATCHING --> DEAD_LETTERED
    DISPATCHING --> EXPIRED
    DELIVERED --> ACKED
    DELIVERED --> EXPIRED
    SCHEDULED --> EXPIRED
    SCHEDULED --> DEAD_LETTERED
    DUE --> EXPIRED
    DUE --> DEAD_LETTERED
    DISPATCHING --> SCHEDULED : RETRY (重试退避后重排)
    DEAD_LETTERED --> SCHEDULED : revive
    CANCELED --> [*]
    ACKED --> [*]
    EXPIRED --> [*]
    DEAD_LETTERED --> [*]
```

> 注:本图为可读性投影(简化图);唯一权威是 `Intent.validateTransition`(含 DEAD_LETTERED→SCHEDULED revive 与全部终态入口)。图与 switch 不一致时以 switch 为准,修图不改 switch。

The current code defines the lifecycle in [`IntentStatus`](../../loomq-core/src/main/java/com/loomq/domain/intent/IntentStatus.java).

## Correctness Boundaries

LoomQ core is responsible for:

- durable scheduling
- rescheduling
- expiration handling
- recovery after restart
- retry orchestration
- time-driven execution hooks

LoomQ core is not responsible for:

- lock semantics
- lease ownership rules
- fencing token policy
- leader election
- business-specific reservation rules

Those belong to a future shell such as `loomqex`, which should build on the kernel instead of being embedded into it.

## Durability Semantics

LoomQ 的持久性保证取决于创建 Intent 时选择的 `AckMode`。理解这个区别对于正确使用 LoomQ 至关重要。

### AckMode 对照表

| AckMode | 持久化写入 | API 返回时机 | 崩溃窗口 | 适用场景 |
|:--------|:---------|:------------|:---------|:---------|
| `ASYNC` | 写入 WheelStore/TailIndex 的 mmap，不等 msync | 写入内存后立即返回 | **有 — API 返回成功不等于数据已持久化，进程崩溃后 Intent 可能丢失** | 低延迟、明确接受 at-most-once 语义 |
| `DURABLE`（默认） | 阻塞至 group-commit msync 完成 | 等待 group-commit msync 落盘 | 无 — PHTW 已落盘 | 大多数业务场景 |
| `REPLICATED` | **预留（当前映射 DURABLE）**：多副本确认未实现，持久化写入与 DURABLE 同臂（`IntentCommandService` 守卫 switch 实证） | 等同 DURABLE | 等同 DURABLE — **无**多副本保障 | 金融、交易等零丢失场景（待未来集群能力落地后另行实现） |

### 崩溃窗口详解（ASYNC 模式）

`createIntent()` 在 `ASYNC` 模式下的执行流程（PHTW 持久化栈）：

```
1. wheelStore.put(intent) 或 tailIndex.put(intent)   ← 写入 mmap 槽位 / tail run 文件
2. locationIndex.put(intentId, loc)                  ← 更新 intentId→槽位索引
3. scheduler.schedule(intent) 或 promotionDaemon.register(...)  ← 调度入队（热 ≤60min）/ 注册冷提升 cohort
4. return seq                                        ← API 返回 201
   ↕ 崩溃窗口：mmap 写入尚未被 group-commit daemon msync 落盘
5. group-commit daemon 周期性 msync(forceDirty)      ← 数据落盘
```

步骤 4 到步骤 5 之间存在**崩溃窗口**：API 已返回成功，但 mmap 写入尚未被 group-commit daemon 刷盘。如果进程在此窗口内崩溃，重启后该 Intent 不会被恢复。

**这不是 bug，而是设计选择**：`ASYNC` 模式优先吞吐（<1ms 延迟），接受极小概率的数据丢失。`DURABLE` 模式则在写入后阻塞于 `commitBarrier.awaitCommit()`，等待覆盖本次写入的 group-commit msync 完成才返回，故无崩溃窗口。

### 对比：状态变更操作的持久性

与 `createIntent` 不同，**状态变更操作始终使用 `DURABLE` 模式**：

| 操作 | 持久化模式 | 说明 |
|:-----|:---------|:-----|
| `createIntent` | 由 `ackMode` 参数决定 | 可选 ASYNC/DURABLE（REPLICATED 预留，当前映射 DURABLE） |
| `updateIntent` | 始终 DURABLE | 硬编码 `AckMode.DURABLE` |
| `cancelIntent` | 始终 DURABLE | 硬编码 `AckMode.DURABLE` |
| `fireNow` | 始终 DURABLE | 硬编码 `AckMode.DURABLE` |

这意味着：即使 Intent 以 `ASYNC` 模式创建，后续的取消、更新、立即触发等操作都保证 PHTW 落盘（group-commit msync）后才返回。

### 选择建议

```
                        ┌─────────────────────┐
                        │  需要零丢失保证？    │
                        └──────────┬──────────┘
                                   │
                    ┌──────────────┴──────────────┐
                    ▼                              ▼
                  是                              否
                    │                              │
        ┌───────────┴───────────┐      ┌──────────┴──────────┐
        │  需要多节点容灾？     │      │  延迟敏感 (<1ms)？   │
        └───────────┬───────────┘      └──────────┬──────────┘
                    │                              │
          ┌─────────┴─────────┐          ┌────────┴────────┐
          ▼                   ▼          ▼                 ▼
        是                  否         是                否
          │                   │          │                 │
     REPLICATED           DURABLE      ASYNC           DURABLE
```

> **注意**：`REPLICATED` 模式为未来集群/多副本能力预留（当前实现映射为 `DURABLE`）。单节点部署下，`REPLICATED` 等同于 `DURABLE`。

## Public Hooks

The codebase already exposes shell-oriented extension points:

- `CallbackHandler`
- `DeliveryHandler`
- `RedeliveryDecider`
- `IntentObserver`

That means the kernel can stay focused while higher-level products define their own behavior on top.
