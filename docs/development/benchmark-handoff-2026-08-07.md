# LoomQ 基准测量重做 交接文档（含两个延后引擎问题）

> **用途**：上下文压缩后新开 spec 的冷启动交接文档。新会话请先读本文档，再决定要立项的内容。
> **基准 HEAD**：`c403a02`（`dev-xuyuchen` 分支）
> **日期**：2026-08-07

---

## 1. 一句话现状

基准测量逻辑已全面重做完成（持续稳态 + 纳秒精度 + 整窗中位数/IQR + 扫参），**工作区干净、fast 测试 305 全过**；过程中**暴露了两个真实的引擎问题**，均已延后，本文档是取证与后续立项的依据。

---

## 2. 已完成：基准测量重做（12 个提交）

### 2.1 动机

旧基准用整数毫秒 + 500 样本 → QPS 被量化到离散点（`113,636` 假象），无法区分真实性能差异（如消费者 8 vs 16）。

### 2.2 交付物（`loomq-core/src/test/java/com/loomq/benchmark/`）

| 文件 | 职责 |
|------|------|
| `Stats.java` | 中位数 / Q1/Q3 / IQR / p95/p99/p999，线性插值百分位 |
| `SteadyStateHarness.java` | 闭环稳态吞吐：`Semaphore(inFlight)` 维持固定在途，`onComplete()` 数完成，纳秒计时，**整窗总吞吐 × 重复 N 窗 → median/IQR** |
| `BenchmarkConfig.java` | `inFlight / delayRangeMs / measureWindowMs / warmupMs` 等参数 |
| `SweepParam.java` / `SweepDriver.java` | 可扫参数枚举 + 解析 `sweep.consumers=4,8,16,32` → 覆盖版 catalog 序列 |
| `CreateIntentBenchmark.java` | 闭环落在"创建完成"，`executeAt=now+30s` 远未来避免投递干扰，测创建吞吐 |
| `DeliveryPathBenchmark.java` | 稳态投递吞吐 + e2e 延迟 + 扫参触发（读 `-Dsweep.consumers`） |
| `PrecisionLatencyBenchmark.java` | **未改**（本就准确，µs 直方图） |

### 2.3 支撑生产改动（小而干净，已提交）

- `PrecisionTierProfile`：加 `withConsumerCount/withMaxConcurrency/withBatchSize/withDispatchQueueCapacity` wither（record，走全字段构造器触发校验）
- `LoomqEngine.Builder.catalog(PrecisionTierCatalog)`：注入自定义档位目录（原传 `null` → `defaultCatalog()`，无注入点）
- `PrecisionScheduler.getPrecisionTierCatalog()` getter
- `WheelConfig.withSlotsPerBucket(int)`：解桶容量阻塞

### 2.4 脚本（`benchmark/scripts/benchmark.sh` / `.ps1`）

- 新 RESULT schema：`qps_median/qps_iqr/samples` 取代 `qps_mean`
- 报告列：`QPS(median) / QPS(IQR)`；投递表 `wake p50/p99`、`e2e p50/p99`、`overhead p99`
- 新增 `--sweep-consumers` / `-SweepConsumers` 参数 + "消费者数扫参"报告表
- 已修 bug：PS1 空 `-Dsweep.consumers=` 不再注入（benchmark 侧也加了 `!sweepProp.isBlank()` 防线）

### 2.5 验证结果（当前 HEAD）

- fast 测试：**305 通过 / 0 失败**（含新框架单测 StatsTest/SteadyStateHarnessTest/SweepDriverTest 等）
- 创建吞吐：单发 ~39–40k QPS，批量 ~40k QPS（Windows 22 核，JDK 25.0.4）
- 触发精度：MILLI/ULTRA p50=0, p99=1ms；STANDARD/FAST 与窗口一致
- 投递吞吐：**可出表，但受下面 Issue B（引擎停摆）影响，数字不可靠**

### 2.6 关键设计决策（实现过程中修正）

- **吞吐统计**：从"100ms 子窗中位数"改为"**整窗总吞吐 × 重复 N 窗 → median/IQR**"。因引擎投递是突发式的（adaptive 扫描器批量派发 + 空闲间隔），100ms 子窗大多为空 → median=0 假象。整窗总量免疫窗内突发性。
- **闭包捕获**：`new SteadyStateHarness(...)` 的 lambda 里引用 `harness` 本身会触发 Java 定值分析错误，需用 `AtomicReference`/数组 holder 绕开（多处）。

---

## 3. 延后问题 A：SEC 轮桶容量限制持续 DURABLE 吞吐

### 3.1 现象

持续稳态下命中 `SlotOverflowException: bucket overflow: SEC/<second> (slotsPerBucket=1024)`，`createIntent` 抛异常 → 该 intent 永不投递 → 在途信号量永不释放 → 管道卡死 → `qps_median=0`。

### 3.2 根因（已确诊）

PHTW 的 SEC 轮（1s×60）每桶固定 **1024 个 256B 定长槽**，**append-only**；投递完成的 intent **不回收槽**（旧槽残留，靠 recovery 按 max revision 去重）。持续速率 R/秒下，每个秒桶累计 R 个槽 → `R > 1024` 溢出。

> **推论**：引擎对"同一秒的持续 DURABLE 创建"有 **~1024/秒硬顶**，与消费者数/并发/扫描线程无关。这是持久层固有约束，CLAUDE.md §10.1 已记载（"旧槽残留由 recovery 去重、无 compaction"）。

### 3.3 突发基准为何没暴露

突发式每次 500 个、`executeAt` 随机散布 4.7–5.3s，单秒桶从未超 1024，故旧基准测出 100k+ 峰值。

### 3.4 已做：Step 1 解阻塞（当前 HEAD 已含）

- `WheelConfig.withSlotsPerBucket(int)` 已加
- 基准引擎用 `WheelConfig.defaultConfig().withSlotsPerBucket(65_536)`（16MB/桶，基准可接受；ULTRA inFlight=200 远低于此）
- 解阻塞后创建吞吐 ~40k/s 正常

### 3.5 彻底修：单槽 compaction（已实现）

采用方向 B（终态槽回收 compaction）的**单槽 compaction** 实现，突破 SEC 桶容量上限并消除大桶内存浪费。提交范围：`ab289a6`（wheel free-list）→ `2509419`（double-free 守卫 + 测试）→ `9222547`/`3936c84`（commandService 终态回收 + 热取消）→ `f0f8d8b`/`4929945`（scheduler 终态原地覆写 + engine 接线）→ `b1b0b42`（验证：回退 1024 桶，零溢出）。

**机制**：
- 终态（ACKED/DEAD_LETTER/EXPIRED/CANCELED）经 `persistTerminalInPlace` **原地覆写** `locationIndex` 指向的最新槽（不追加）；落盘后 `reclaimTerminal` 清空该槽并加入 `Bucket` 的 free-list 复用。
- **从未重排程**（无 RETRY/改期）的 Intent 从 create 到终态只占一个槽 → 回收安全，崩溃一致性自动成立（终态槽在 → skip / 空槽 → skip）。
- **重排程过的多槽 Intent** 只覆写、不回收，保留终态槽作 tombstone，抑制跨桶 stale sibling（与现状 append + max-revision 去重一致）。
- **顺带收益**：`slotsPerBucket` 回退默认 **1024**，基准 / `StallDetectionTest` 不再用 262144 大桶规避。

**验证**：`StallDetectionTest` 连续 **10 轮零停摆** @默认 1024 桶；`DeliveryPathBenchmark` 各档 `qps_median>0`；fast 测试 **311 通过**。

> 其余方向（见 `.claude/plans/2026-08-07-wheel-bucket-throughput-ceiling.md`）：**C. 溢出 spill**（满桶溢出到下一层/tail）**仍未做**，是唯一遗留的未来方向；D. 接受限制文档化、基准退回突发 已不再需要。

---

## 4. 问题 B：引擎投递偶发停顿 —— 已修复（2026-08-07）

> **状态变更**：Issue B 已定位并修复。根因**不是**调度器并发竞态（下述 §4.3 原假设被证伪），而是 SEC 轮桶容量溢出打在 finalize 持久化路径上。完整证据链见 `docs/development/issue-b-rootcause-2026-08-07.md`。

### 4.1 现象（原始记录）

持续闭环负载下，引擎**先投递一批后卡死**，不再派发在途 intent → 死锁。可复现（约 **10/11 次停顿**；有 1 次持续跑 ~21k QPS 证明引擎本身能跑）。

### 4.2 取证（原始记录）

- 单窗 0 = 16,193 完成；窗 1–4 = 0（`e2e_p50=2ms, p999=37ms` 证明窗 0 确有真实投递突发）
- producer 累计提交 **48,405** 个 intent（全部 `DURABLE createIntent` 成功返回，`submitErrors=0`），但只有 ~16k 被投递
- 之后 producer 占满全部 `inFlight`（ULTRA=200）信号量等完成事件，永不释放 → 死锁

### 4.3 根因（2026-08-07 已确诊，原"疑似位置"已推翻）

**根因 = SEC 时间轮桶容量溢出（Issue A）在 `finalizeIntent` 的 `persistStateChange` 路径上抛 `SlotOverflowException`。**

机制（`StallDetectionTest` 100% 复现 + `finalizeExceptionSamples` 全部为 `SlotOverflowException: bucket overflow: SEC/...`）：
1. 每个 intent 写 SEC 桶**两次**（create 的 SCHEDULED + finalize 的 ACKED 终态），append-only 无 compaction。
2. 投递基准 `executeAt=now+1-5ms` 落**当前秒桶**，~40k/s×2 在 ~0.8-1.6s 填满 65536 槽。
3. 桶满后 `persistStateChange` 在 synchronized 块内抛异常 → intent 已 ACKED 但 `onDelivered` 观察器被吞 → 基准 harness 槽位永久泄漏 → producer 死锁 → 引擎整体停摆（扫描空桶 park、全档位闲置）。
4. `withSlotsPerBucket(65536)` 解阻塞**不足**：它的"够用"只在 `CreateIntentBenchmark`（executeAt=now+30s，落远未来秒桶）下成立；投递路径落当前秒桶必然溢出。

原 §4.3 假设（adaptive/cohort 派发链路竞态、关联 `8a5bedd`）**不成立**——8a5bedd 的 persist-split 改动本身无并发缺陷。

### 4.4 修复（2026-08-07，方案 A+D）

- **A. 解耦 onDelivered 与终态持久化（I6 容错）**：`PrecisionScheduler.persistStateChange` 吞掉持久化失败并记录 `persistFailures` 计数，调度流程继续，`onDelivered` 必须仍触发、槽位不泄漏。独立于容量，无论容量如何都该修。
- **D. 基准解阻塞**：`StallDetectionTest` / `DeliveryPathBenchmark` `withSlotsPerBucket` 65536→262144（当前秒桶 ~2× 余量；桶为 mmap 惰性创建，仅触碰的秒桶耗内存）。
- **回归测试**：`FinalizePersistFailureRegressionTest`（fast-tag，进 CI）——注入恒定失败的终态持久化，断言 `onDelivered` 仍触发 + `persistFailures` 记录。
- **验证**：`StallDetectionTest` 连续 **10 轮零停摆** + fast 306 通过；ULTRA 投递 `qps_median=24k, e2e_p50=4ms, wake_p99=2ms`（此前 qps_median=0）。

### 4.5 遗留（仅剩溢出 spill 方向）

容量根治（方向 B 单槽 compaction）**已完成**（见 §3.5）。唯一遗留的未来方向：
- **C. 溢出 spill**（满桶溢出到下一层/TailIndex）—— 未做，作为后续候选（见 §3.5 与 §5）。

---

## 5. 决策与遗留项（2026-08-07 已定）

1. **引擎投递停顿（Issue B）**：✅ **已修复**（方案 A 解耦 onDelivered 与终态持久化 + D 基准解阻塞）。见 §4.4。
2. **容量根治（Issue A）**：✅ **已修复**（方向 B 单槽 compaction）。见 §3.5。`slotsPerBucket` 回退默认 1024，基准不再用 262144 规避。唯一遗留未来方向：**C. 溢出 spill**。
3. **基准投递数字**：✅ 修复后可用（ULTRA 24k QPS 等），README 投递列可更新。
4. **消费者扫参甜点**：桶容量已解（默认 1024 桶 + compaction），扫参可测；但注意持续吞吐仍受活跃在途 ≤ 桶容量约束（非消费者），甜点解读需谨慎。

---

## 6. 关键文件与提交索引

- 基准设计规格：`.claude/plans/2026-08-07-benchmark-measurement-design.md`
- 基准实现计划：`docs/superpowers/plans/2026-08-07-benchmark-measurement-implementation.md`
- 桶容量问题专档：`.claude/plans/2026-08-07-wheel-bucket-throughput-ceiling.md`
- 基准类：`loomq-core/src/test/java/com/loomq/benchmark/`
- 支撑改动：`PrecisionTierProfile.java`、`LoomqEngine.java`、`PrecisionScheduler.java`、`WheelConfig.java`
- 脚本：`benchmark/scripts/benchmark.sh`、`benchmark.ps1`
- 最近提交：`c403a02`（基准收尾）→ `7d027b4`（脚本）→ `0711115`（Create 基准）→ `a684542`（整窗中位数）→ `2cd652c`/`f4d1ee2`（解桶阻塞）→ `96921a3`（Delivery 重写）→ `932d630`/`846ef86`/`165e5b4`（Sweep/Harness/Stats）→ `a2c68f4`/`bc74fac`（catalog/wither）
- 用户此前调度器改动：`8a5bedd`（与 Issue B 疑似相关）