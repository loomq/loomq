# Issue B 根因报告：引擎投递停摆（SEC 桶容量溢出轰炸 finalize 路径）

- 日期：2026-08-07
- 状态：根因已确认（决策门产出）
- 触发：`StallDetectionTest` 可靠复现（多轮 100% 停摆）
- 原始假设→证伪：交接文档 §4 假设"adaptive/cohort 派发链路竞态"（8a5bedd 相关）——**已被本报告证伪**

## 1. 结论（一句话）

Issue B 的根因**不是调度器并发竞态**，而是 **SEC 时间轮桶容量溢出（Issue A）在 `finalizeIntent` 的持久化路径上抛 `SlotOverflowException`**，导致 intent 已 ACKED 但 `onDelivered` 观察器永不触发 → 基准 harness 槽位永久泄漏 → producer 死锁 → 引擎整体停摆。

## 2. 可靠复现

`StallDetectionTest`（ULTRA 闭环稳态，`SUCCESS` 即时完成 handler，`withSlotsPerBucket(65536)`）多轮 100% 复现停摆。每轮停摆特征完全一致：

```
STALL_DETECTED|idleMs≈3000|submitted≈58k|delivered≈58k|expired=0|deadLettered=0|deliveryFailed=0|outstanding=200
```

- `outstanding = submitted - delivered = 200`（精确等于 inFlight/maxConcurrency）：producer 占满全部槽位，等 `onDelivered` 永不释放。
- `expired/deadLettered/deliveryFailed = 0`：200 个在途 intent 未过期、未死信、未投递失败。

## 3. 可证伪假设逐条判定

| 假设 | 判定 | 证据 |
|------|------|------|
| **H1** finalize 卡在 `awaitStateChangeCommit` | **证伪** | `tierInFlight=0`（finalize 任务未卡在途）；`awaitCommitTimeoutMs=10s` 而停摆在 3s（不可能已超时抛错） |
| **H2** adaptive 扫描器 lost-wakeup | **证伪** | 扫描器处于空桶分支是因桶已空（后续确认 200 个在途均被扫出），非漏唤醒；`scanIntervalMs=10` → park 上限 1s 不自愈 |
| **H3** 消费者 lost-wakeup / permit 耗尽 | **证伪** | `activeDispatch=0`、`queue=0`、permits 全满、`tierInFlight=0`——派发路径完全闲置 |
| **H4** 多竞态交互 | **证伪**（根因单一） | 见 §4：单一机制（SEC 桶溢出）解释了全部现象 |
| **追加** scanDue CAS 静默丢弃 | **证伪** | `scanDueCasDrop=0` |
| **追加** intent 过期/死信 | **证伪** | `expired=0, deadLettered=0` |

**关键正证**：`finalizeSuccessObserverNotified = delivered`（=58k）——所有 finalize SUCCESS 都到达了 `onDelivered` 派发点且被 harness 收到。而 `finalizeTaskExceptions = 118`，全部为：

```
SlotOverflowException: bucket overflow: SEC/1786102645 (slotsPerBucket=65536)
```

## 4. 根因机制（精确到环节）

```
producer createIntent(executeAt=now+1-5ms)  ──每次写 1 个槽到"当前秒"SEC 桶
   ↓
intent 被 scan→dispatch→deliverAsync(即时完成)
   ↓
finalizeIntent SUCCESS：
   transitionTo(ACKED)            ← 内存已 ACKED
   intentStore.update(intent)     ← 热存储已 ACKED
   persistStateChange(intent)     ← persistToWheel 写终态到同一 SEC 桶 ⚠️
       └─ SEC 桶 append-only，无 compaction，65536 槽在 ~40k/s×2 写入下 ~0.8-1.6s 填满
       └─ 桶满 → 抛 SlotOverflowException（在 synchronized 块内）
   ↓ （异常传播，退出 synchronized）
onDelivered 观察器 永不触发        ← 槽位泄漏
   ↓
harness slots 永不释放 → producer 死锁 → 引擎停摆（扫描空桶 park、全档位闲置）
```

**为什么 createIntent 不抛、finalize 抛**：每个 intent 写两次（create 的 SCHEDULED + finalize 的 ACKED）到同一 executeAt 秒桶。create 先写（producer 快）、finalize 后写。桶被 create 阶段填满后，finalize 的终态写撞上满桶。一旦 finalize 开始抛（onDelivered 不触发）→ producer 阻塞 → 不再 createIntent → 所以 createIntent 无错误，错误集中在 finalize。

**为什么 `withSlotsPerBucket(65536)` 解阻塞失败**：65536 槽 ≈ 当前秒桶在 ~40k/s 下的 ~1.6s 累计写入量，测量窗（3s）内必然填满。交接文档的"解阻塞后创建吞吐 40k/s 正常"是在 `CreateIntentBenchmark`（executeAt=now+30s 远未来，落不同秒桶）下测的；投递路径（executeAt=now+1-5ms 落当前秒桶）必然溢出。

## 5. 根因归属

- 这是 **Issue A（SEC 桶容量）的另一个表现形态**，不是独立的调度器 bug。
- 交接文档 §4 的调度器竞态假设（8a5bedd）**不成立**——8a5bedd 的 persist-split 改动本身无并发缺陷。
- 引擎存在一个**独立于容量**的真实缺陷：finalize 的终态持久化失败会**静默吞掉 `onDelivered` 通知并泄漏槽位**，造成不可恢复死锁。即使容量充足，持久化失败也不应阻止已投递 intent 的观察器通知。

## 6. 候选修复方案（决策门需定）

| 方案 | 做法 | 解决 | 代价 |
|------|------|------|------|
| **A. 解耦 onDelivered 与终态持久化**（首推，Issue B 本体） | finalize 的 `onDelivered` 观察器通知不依赖 `persistStateChange` 成功。终态持久化失败时：仍通知 onDelivered（投递确实发生）、记录持久化失败（log/指标）、不泄漏槽位。见方向 C 的关系 | 死锁（槽位泄漏、观察器被吞） | 小、局部；但容量问题仍在（终态可能未落盘） |
| **B. 终态槽 compaction（Issue A Step 2）** | SEC 桶回收终态槽，容量跟随活跃在途而非吞吐史 | 容量根本问题（Issue A+B 一起） | 大：mmap 桶内回收 + 高水位/恢复语义 + 崩溃一致性，需独立 spec |
| **C. 溢出 spill** | 满桶溢出到下一层/TailIndex | 容量 | 大：持久层布局改动，影响 recovery 去重 |
| **D. 基准工件规避** | 投递基准 `executeAt` 加宽（如 1-100ms）分散秒桶，或大幅调大 `slotsPerBucket` | 让基准数字可测 | 非引擎修复；Issue A 延后；掩盖真实容量边界 |
| **E. 接受 + 文档化** | 声明持续 DURABLE 吞吐上限 | 诚实呈现 | 上限偏低 |

**建议组合**：A（修死锁本体，引擎健壮性）+ D（基准解阻塞，立刻拿到可靠投递数字），B/C（容量根治）作为后续 Issue A spec 评估。A 独立于容量，无论容量如何都该修；D 让基准立即可用。

## 7. 决策门

请用户定夺：
1. 是否按 **方案 A** 修 finalize 死锁（onDelivered 与终态持久化解耦）？
2. 基准解阻塞用 **D**（加宽 executeAt / 调大桶）还是等 **B**（compaction）？
3. 修复范围是否仍限定"连续 N 轮零停摆 + fast 全过 + README 更新"（本 spec 原验收）？

## 8. 关键改动索引

- 诊断插桩（已提交）：`PrecisionScheduler.getTierInFlight` / `getFinalizeSuccessObserverNotified` / `getFinalizeTaskExceptions` / `getFinalizeExceptionSamples`；`BucketGroup.getScanDueCasDropCount`
- 复现测试：`loomq-core/src/test/java/com/loomq/benchmark/StallDetectionTest.java`
- 转储取证：`docs/development/stall-dumps/stall-round-0-dump.txt`（及 `loomq-core/target/stall-dumps/`）
- 涉及源码：`PrecisionScheduler.finalizeIntent`（SUCCESS 路径）、`WheelStore` SEC 桶（容量）、`IntentCommandService.persistToWheel`