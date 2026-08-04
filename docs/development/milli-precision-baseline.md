# MILLI 精度档位 — 改造前后基准对比

> 本文档记录 Phase 0（PR0）测得的 ULTRA 档位触发精度基线，以及 Phase 4（PR4）引入
> MILLI 1ms 档 + 事件驱动扫描改造后的对比结果，用于验证 MILLI 档可用且 ULTRA/FAST 不劣化。

## 测量信息

| 项 | 值 |
|----|----|
| 基址 commit | `152cb74`（`docs(agents): 同步 CI 章节与 CLAUDE.md 一致`） |
| 环境 | Windows 11 Home China（10.0.26200） |
| Java | Oracle JDK 25.0.4（Maven `JAVA_HOME`，`D:\Development\JDKs\jdk-25.0.4`） |
| Maven | 3.9.1 |
| 模块 | `loomq-core` 0.9.2 |

## 测量方法

- 基准测试类：`loomq-core/src/test/java/com/loomq/benchmark/PrecisionLatencyBenchmark.java`
  （`@Tag("benchmark")`，PR4 起含 ULTRA、MILLI 与 FAST 三档）。
- 注入 1000 个 Intent，`executeAt = now + random(100ms .. 60s)`，`AckMode.DURABLE`。
- 通过 `MetricsCollector.getWakeupLatencySnapshot(tier)` 读取唤醒延迟直方图（微秒），
  除以 1000 转为毫秒。
- 输出 `RESULT|precision|...` 标记，供 PR4 对比；同时后台线程采样 `getCpuLoad()` 输出
  `CPUSAMPLE|...` 空闲 CPU 采样。

---

## PR0 基线（改造前，仅 ULTRA）

命令：

```
mvn test -pl loomq-core -Pfull-tests -Dtest=PrecisionLatencyBenchmark#measurePrecision_Ultra
```

RESULT 标记：

```
RESULT|precision|tier=ULTRA|count=1000|p50_ms=5|p99_ms=10|p999_ms=25
[Benchmark] ULTRA precision: p50=5ms p99=10ms p999=25ms
Tests run: 1, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 60.21 s
```

| 指标 | 值 |
|------|----|
| ULTRA p50（唤醒延迟） | 5 ms |
| ULTRA p99（唤醒延迟） | 10 ms |
| ULTRA p999（唤醒延迟） | 25 ms |
| 空闲 CPU 采样（load） | 0.12 ~ 0.25（系统整体负载，非引擎独占） |

---

## PR4 改造后（现网重跑）

命令：

```
mvn test -pl loomq-core -Pfull-tests -Dtest=PrecisionLatencyBenchmark
```

RESULT 标记：

```
RESULT|precision|tier=MILLI|count=1000|p50_ms=2|p99_ms=10|p999_ms=10
RESULT|precision|tier=ULTRA|count=1000|p50_ms=1|p99_ms=10|p999_ms=10
RESULT|precision|tier=FAST|count=1000|p50_ms=10|p99_ms=50|p999_ms=50
```

### 结果对比

| 档位 | 指标 | PR0 基线 | PR4 改造后 | 对比 |
|------|------|---------|-----------|------|
| MILLI | p50 | —（新增） | 2 ms | 新增档 |
| MILLI | p99 | — | 10 ms | 新增档 |
| MILLI | p999 | — | 10 ms | 新增档 |
| ULTRA | p50 | 5 ms | 1 ms | 不劣化（改善） |
| ULTRA | p99 | 10 ms | 10 ms | 持平 |
| ULTRA | p999 | 25 ms | 10 ms | 不劣化（改善） |
| FAST | p50 | —（PR0 未记录） | 10 ms | 见下方 FAST 说明 |
| FAST | p99 | — | 50 ms | 见下方 FAST 说明 |
| FAST | p999 | — | 50 ms | 见下方 FAST 说明 |

### 空闲 CPU 对比

| 档位 | PR0 基线 | PR4 改造后 |
|------|---------|-----------|
| ULTRA | 0.12 ~ 0.25 | avg 0.063（min 0.004 / max 0.290，n=1053） |
| MILLI | — | avg 0.047（min 0.000 / max 0.334，n=1071） |
| FAST | — | avg 0.118（min 0.000 / max 0.387，n=965） |

空闲 CPU 未因新增 MILLI 档而劣化（事件驱动扫描 + 信号驱动消费使空闲时无轮询开销）。

### 说明

- **ULTRA/FAST 回归检查**：ULTRA p50/p99/p999 均不劣于 PR0 基线（p50 1ms vs 5ms、
  p999 10ms vs 25ms），事件驱动改造未引入回归。FAST 档本次补测（`measurePrecision_Fast`，
  count=1000、注入 delay 100ms..60s），p50=10ms / p99=50ms / p999=50ms——与 FAST 档
  50ms 精度窗口一致（触发误差上界 ≈ 窗口 = 50ms），无异常；**PR0 未记录 FAST 档基线**，
  故 FAST 仅提供改造后实测值，无 PR0 对照（不虚构基线）。
- **Windows 计时器粒度制约**：Windows 默认计时器分辨率约 15.6ms，导致 MILLI 1ms 档的
  `p99/p999` 被系统计时粒度钳制在 ~10ms，无法体现 1ms 级触发精度。**Linux（高精度
  `hrtimer`）是精度目标平台**；Windows 上 MILLI 档仅验证"可用、不报错、不劣化 ULTRA"。
- **10 万散落桶内存拟合（实测）**：临时探针（已删除，不留在仓库）构建 `BucketGroup(MILLI)`
  灌入 100_000 个、每个唯一毫秒 `executeAt` 的 Intent（落 100_000 个独立 1ms 桶），
  用 `Runtime.totalMemory()-freeMemory()` 前后差测量堆增量。两次运行稳定：
  **heap_delta ≈ 57.6 MB（100_000 桶 / 100_000 pending，约 604 B/桶）**。该值为
  **真实测量**（非估算），覆盖热态完整链路：CSLM 桶 + 100_000 个 CHM 桶头 + `intentIndex`
  CHM 条目 + `ClaimEntry`/`BucketEntry` + 100_000 个 `Intent` 对象与 `String` intentId。
  高于计划 §4.3 的 30–40MB 估算（该估算仅计 CHM 桶头 + 索引条目，未含完整 `Intent` 对象
  与桶条目），但绝对值合理，且 100k 散落桶远低于 MILLI 高水位（`maxBuckets=200_000`），
  内存可控。
- **首次运行抖动**：单次运行因最长 Intent 延迟 60s，运行时长为 120s（两档各一轮），属正常；
  对比时建议先预热一次再取数。
- **消费端空 poll park 上限提升（范围扩大说明）**：单发消费者空 poll 后 park 从
  100µs(ULTRA)/200µs(FAST) 统一升到 1ms。信号驱动设计下 1ms 仅为 lost-wakeup 保底
  （中位路径走 offer→unpark），基准证据（ULTRA p50 5ms→1ms）支持该决策；但非 MILLI
  档 signal-miss 场景最坏等待确实变长（1ms vs 原 100µs/200µs）。

## 结论

- MILLI 1ms 档在 Windows 上可正常触发（p50=2ms），受计时器粒度制约 p99 钳制在 ~10ms，
  面向 Linux 的 1ms 级精度目标平台可用。
- ULTRA 无回归，空闲 CPU 反而下降（0.12~0.25 → avg 0.047~0.063），事件驱动改造符合预期。
- FAST 档补测 p50=10ms / p99=50ms / p999=50ms，与 50ms 精度窗口一致，无回归（PR0 未记录
  FAST 基线，无对照）。
- 10 万散落桶内存实测 ≈ 57.6 MB（约 604 B/桶），低于 MILLI 高水位阈值，内存可控。