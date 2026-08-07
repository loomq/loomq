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

### 3.5 待评估：彻底修（Step 2，未做）

**思路**：终态槽回收（compaction），让桶容量 ∝ 活跃在途数而非吞吐史。投递/取消/过期的终态槽被回收，桶里只留 pending 槽。好处：突破单桶上限 + 消内存浪费（不用 16MB 大桶）。代价：持久层改动，涉及 mmap 桶内回收 + 高水位/恢复语义 + 与 `BucketReclaimer`（现在只回收过期桶文件，不回收桶内槽）协调；有崩溃一致性风险。**需独立 spec + plan。**

其他方向（见 `.claude/plans/2026-08-07-wheel-bucket-throughput-ceiling.md`）：B. 溢出 spill 到下一层/tail；C. 接受限制文档化；D. 基准退回突发。

---

## 4. 延后问题 B：引擎投递偶发停顿（更紧急，建议优先）

### 4.1 现象

持续闭环负载下，引擎**先投递一批后卡死**，不再派发在途 intent → 死锁。可复现（约 **10/11 次停顿**；有 1 次持续跑 ~21k QPS 证明引擎本身能跑）。

### 4.2 取证（本次交付基准已含的实测）

- 单窗 0 = 16,193 完成；窗 1–4 = 0（`e2e_p50=2ms, p999=37ms` 证明窗 0 确有真实投递突发）
- producer 累计提交 **48,405** 个 intent（全部 `DURABLE createIntent` 成功返回，`submitErrors=0`），但只有 ~16k 被投递
- 之后 producer 占满全部 `inFlight`（ULTRA=200）信号量等完成事件，永不释放 → 死锁
- 最近一次 delivery 全量：MILLI=4913、FAST=2986、STANDARD=99 正常；**ULTRA=0（本次中招）**

### 4.3 疑似位置

`PrecisionScheduler` 的 **adaptive/cohort 派发链路**（`adaptiveScanLoop` / `scanAndDispatch` / cohort flush → triggerScan）。**可能与 `8a5bedd refactor(scheduler): 优化调度器持久化机制避免虚拟线程挂起` 相关**（该提交是用户此前的调度器优化，涉及 finalize/awaitCommit 移出 synchronized 块）。

引擎工作区干净（用户改动已提交为 8a5bedd），停摆在**已提交**的引擎代码里。

### 4.4 建议排查路径

用 systematic-debugging：
1. 复现：`DeliveryPathBenchmark#measureDeliveryThroughput_Ultra`（`mvn test -pl loomq-core "-Dtest=DeliveryPathBenchmark#measureDeliveryThroughput_Ultra" -Dtest.excludedGroups=""`）
2. 聚焦 adaptive 扫描器 park/unpark 与 cohort triggerScan 的竞态（lost-wakeup / 派发遗漏）
3. 检查 `inFlight` 信号量在停摆时是否被某条路径吞掉 release（死锁判据）

---

## 5. 待决策项（新 spec 需定）

1. **引擎投递停顿（Issue B）**：是否立即立项排查修复？（用户正在做调度器优化领域，建议优先）
2. **终态槽 compaction（Issue A Step 2）**：是否单独立项评估彻底修？
3. **基准投递数字不可靠**：Issue B 修复前，README 基准表投递列暂不更新（创建/精度列可用）。
4. **消费者扫参甜点**：Issue B + A 修复前，持续吞吐甜点测不到（真正瓶颈是引擎派发/桶容量，非消费者）。

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